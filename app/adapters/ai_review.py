"""One review method, two providers.

``AI_REVIEW_PROVIDER`` in config selects the host and model. Call sites only
use ``generate_token_review``.

Groq is the default. The model id comes from ``AI_REVIEW_MODEL`` (default
``openai/gpt-oss-120b``). Its published free-tier row is 30 RPM, 1_000 RPD,
8_000 TPM and 200_000 TPD. The token cap is enforced here with a chars/4
estimate and ``AI_REVIEW_DAILY_TOKEN_BUDGET``. ``grok`` points at xAI's
OpenAI-compatible chat endpoint for a later switch.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from app.config import AI_REVIEW_MODEL, AI_REVIEW_PROVIDER, Settings

logger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 20.0
MAX_ATTEMPTS = 3
_MAX_REVIEW_CHARS = 1500
# GPT-OSS accepts low, medium, or high. none is documented for Qwen only.
# https://console.groq.com/docs/reasoning
_GROQ_REASONING_EFFORT = "low"
_RETRYABLE_STATUS = frozenset({408, 500, 502, 503, 504})
_MAX_RETRY_AFTER_SECONDS = 60
Sleep = Callable[[float], Awaitable[None]]

# Official error object is {"error": {"message", "type"}} from
# https://console.groq.com/docs/errors. Rate-limit headers from
# https://console.groq.com/docs/rate-limits: retry-after is seconds and is
# set only on 429; x-ratelimit-remaining-tokens is tokens per minute, not
# tokens per day. The daily dimension is named in error.message, for example
# "on tokens per day (TPD): Limit 100000, Used 99787, Requested 1616".
_QUOTA_FIELD = re.compile(r"\b(Limit|Used|Requested)\s+([\d,]+)", re.IGNORECASE)

_SYSTEM = (
    "You write a short English risk note for a Solana memecoin monitor. "
    "Use only the fields in the user message. Do not tell the reader to buy "
    "or sell. Do not invent liquidity, holders, or social proof. "
    "Four sentences at most. Plain text only: no Markdown, no asterisks."
)


class AiReviewError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        retryable: bool,
        budget_exceeded: bool = False,
        limit_kind: str | None = None,
        quota_limit: int | None = None,
        quota_used: int | None = None,
        quota_remaining: int | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
        self.budget_exceeded = budget_exceeded
        self.limit_kind = limit_kind
        self.quota_limit = quota_limit
        self.quota_used = quota_used
        self.quota_remaining = quota_remaining


@dataclass(frozen=True)
class TokenReviewInput:
    mint: str
    symbol: str | None
    name: str | None
    risk_score: str | None
    risk_flags: tuple[str, ...]
    liquidity_usd: str | None
    composite_score: str | None
    # The card's checklist as "pass|fail|no data: text" lines. When present it
    # replaces the older score fields, so the note cannot contradict the card.
    checklist: tuple[str, ...] = ()


@dataclass(frozen=True)
class TokenReview:
    text: str
    total_tokens: int | None = None
    finish_reason: str | None = None
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    reasoning_tokens: int | None = None


@dataclass(frozen=True)
class _Quota:
    kind: str
    retryable: bool
    limit: int | None
    used: int | None
    requested: int | None
    remaining: int | None
    retry_after_seconds: int | None


@dataclass(frozen=True)
class _Provider:
    url: str
    model: str


def provider_for(name: str, *, model: str | None = None) -> _Provider:
    if name == "groq":
        chosen = AI_REVIEW_MODEL if model is None else model.strip()
        if not chosen:
            raise ValueError("AI_REVIEW_MODEL must not be empty")
        return _Provider(
            "https://api.groq.com/openai/v1/chat/completions",
            chosen,
        )
    if name == "grok":
        return _Provider(
            "https://api.x.ai/v1/chat/completions",
            "grok-4",
        )
    raise ValueError(f"unknown AI review provider {name}")


def review_api_key(settings: Settings, provider: str | None = None) -> str:
    selected = provider if provider is not None else AI_REVIEW_PROVIDER
    secret = settings.groq_api_key if selected == "groq" else settings.xai_api_key
    return secret.get_secret_value().strip()


def ai_review_configured(settings: Settings) -> bool:
    key = review_api_key(settings)
    return bool(key) and key != "replace-me"


class AiReviewClient:
    def __init__(
        self,
        *,
        provider: str,
        api_key: str,
        model: str | None = None,
        client: httpx.AsyncClient | None = None,
        sleep: Sleep | None = None,
    ) -> None:
        self._provider = provider_for(provider, model=model)
        self._provider_name = provider
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS)
        self._sleep = sleep or _async_sleep

    @classmethod
    def from_settings(cls, settings: Settings) -> AiReviewClient:
        return cls(
            provider=AI_REVIEW_PROVIDER,
            api_key=review_api_key(settings),
            model=settings.ai_review_model if AI_REVIEW_PROVIDER == "groq" else None,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def generate_token_review(self, token: TokenReviewInput) -> TokenReview:
        if not self._api_key or self._api_key == "replace-me":
            raise AiReviewError(
                f"{self._provider_name} API key is not configured",
                status_code=0,
                retryable=False,
            )
        body: dict[str, Any] = {
            "model": self._provider.model,
            "temperature": 0.2,
            "max_tokens": 400,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": _user_prompt(token)},
            ],
        }
        if self._provider_name == "groq":
            body["reasoning_effort"] = _GROQ_REASONING_EFFORT
        response = await self._post(body)
        payload = response.json()
        return TokenReview(
            text=_review_text(payload),
            total_tokens=_total_tokens(payload),
            finish_reason=_finish_reason(payload),
            prompt_tokens=_usage_int(payload, "prompt_tokens"),
            completion_tokens=_usage_int(payload, "completion_tokens"),
            reasoning_tokens=_reasoning_tokens(payload),
        )

    async def _post(self, body: dict[str, Any]) -> httpx.Response:
        last_status = 0
        for attempt in range(MAX_ATTEMPTS):
            try:
                response = await self._client.post(
                    self._provider.url,
                    headers={"Authorization": f"Bearer {self._api_key}"},
                    json=body,
                )
            except httpx.TimeoutException as exc:
                last_status = 0
                logger.warning(
                    "ai_review_timeout",
                    provider=self._provider_name,
                    attempt=attempt + 1,
                    error_type=type(exc).__name__,
                )
                continue
            except httpx.HTTPError as exc:
                raise AiReviewError(
                    f"{self._provider_name} request failed",
                    status_code=0,
                    retryable=True,
                ) from exc
            if response.status_code == 200:
                return response
            last_status = response.status_code
            if response.status_code == 429:
                quota = classify_rate_limit(response)
                if not quota.retryable:
                    logger.warning(
                        "ai_review_budget_exceeded",
                        provider=self._provider_name,
                        limit_kind=quota.kind,
                        quota_limit=quota.limit,
                        quota_used=quota.used,
                        quota_requested=quota.requested,
                        quota_remaining=quota.remaining,
                    )
                    raise AiReviewError(
                        f"{self._provider_name} daily quota is exhausted",
                        status_code=429,
                        retryable=False,
                        budget_exceeded=True,
                        limit_kind=quota.kind,
                        quota_limit=quota.limit,
                        quota_used=quota.used,
                        quota_remaining=quota.remaining,
                    )
                if attempt + 1 < MAX_ATTEMPTS:
                    delay = _backoff_seconds(quota.retry_after_seconds, attempt)
                    logger.warning(
                        "ai_review_rate_limited",
                        provider=self._provider_name,
                        limit_kind=quota.kind,
                        retry_after_seconds=delay,
                        attempt=attempt + 1,
                    )
                    await self._sleep(delay)
                continue
            logger.warning(
                "ai_review_http_error",
                provider=self._provider_name,
                status_code=response.status_code,
                attempt=attempt + 1,
            )
            if response.status_code not in _RETRYABLE_STATUS:
                raise AiReviewError(
                    f"{self._provider_name} rejected the review",
                    status_code=response.status_code,
                    retryable=False,
                )
        raise AiReviewError(
            f"{self._provider_name} review failed after retries",
            status_code=last_status,
            retryable=True,
        )


def estimate_review_tokens(token: TokenReviewInput) -> int:
    """chars/4 of the system prompt plus the user prompt. Not a tokenizer."""
    return (len(_SYSTEM) + len(_user_prompt(token))) // 4


def _user_prompt(token: TokenReviewInput) -> str:
    if token.checklist:
        return _checklist_prompt(token)
    flags = ", ".join(token.risk_flags) if token.risk_flags else "none"
    return "\n".join(
        [
            f"mint: {token.mint}",
            f"symbol: {token.symbol or 'unknown'}",
            f"name: {token.name or 'unknown'}",
            f"checks_passed_score: {token.risk_score or 'unknown'} out of 100",
            f"failed_checks: {flags}",
            f"liquidity_usd: {token.liquidity_usd or 'unknown'}",
            f"composite_score: {token.composite_score or 'unknown'}",
            # The stored column is named risk_score, but it is the weight of the
            # checks that passed. A live review read 85 as "high risk".
            "checks_passed_score is the weight of the known checks that passed: "
            "100 means every check passed, and a lower value means a failed or "
            "unknown check. Higher is better. It is not a claim the token is safe. "
            "Describe each failed check by name.",
        ]
    )


def _checklist_prompt(token: TokenReviewInput) -> str:
    passed = sum(1 for line in token.checklist if line.startswith("pass:"))
    return "\n".join(
        [
            f"mint: {token.mint}",
            f"symbol: {token.symbol or 'unknown'}",
            f"name: {token.name or 'unknown'}",
            f"checks passed: {passed} of {len(token.checklist)}",
            "checklist (status: reading):",
            *(f"- {line}" for line in token.checklist),
            "These lines are already shown to the reader above your note. "
            "Do not repeat them one by one. Say in plain words what the failed and "
            "no-data lines mean together for the risk of this token. Use only the "
            "numbers in these lines. A passed line is not proof the token is safe.",
        ]
    )


def classify_rate_limit(response: httpx.Response) -> _Quota:
    """Split a Groq 429 into a short-window retry or a daily quota stop.

    ``x-ratelimit-remaining-tokens`` is tokens per minute, so it is not the
    daily remainder. Daily remainder is ``Limit - Used`` from ``error.message``
    when that message names tokens per day or requests per day.
    """
    message = _error_message(response)
    kind = _limit_kind(message)
    counts = _quota_counts(message)
    limit = counts.get("limit")
    used = counts.get("used")
    requested = counts.get("requested")
    remaining = None
    if limit is not None and used is not None:
        remaining = max(0, limit - used)
    daily = kind in {"tpd", "rpd"}
    return _Quota(
        kind=kind,
        retryable=not daily,
        limit=limit,
        used=used,
        requested=requested,
        remaining=remaining,
        retry_after_seconds=_retry_after_seconds(response),
    )


def _error_message(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return ""
    if not isinstance(payload, dict):
        return ""
    error = payload.get("error")
    if not isinstance(error, dict):
        return ""
    message = error.get("message")
    return message if isinstance(message, str) else ""


def _limit_kind(message: str) -> str:
    text = message.lower()
    if "tokens per day" in text or "(tpd)" in text:
        return "tpd"
    if "requests per day" in text or "(rpd)" in text:
        return "rpd"
    if "requests per minute" in text or "(rpm)" in text:
        return "rpm"
    if "tokens per minute" in text or "(tpm)" in text:
        return "tpm"
    return "unknown"


def _quota_counts(message: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for match in _QUOTA_FIELD.finditer(message):
        counts[match.group(1).lower()] = int(match.group(2).replace(",", ""))
    return counts


def _retry_after_seconds(response: httpx.Response) -> int | None:
    raw = response.headers.get("retry-after")
    if raw is None or not raw.strip():
        return None
    try:
        return max(0, int(float(raw)))
    except ValueError:
        return None


def _backoff_seconds(retry_after: int | None, attempt: int) -> float:
    if retry_after is None:
        return float(min(2**attempt, _MAX_RETRY_AFTER_SECONDS))
    return float(min(retry_after, _MAX_RETRY_AFTER_SECONDS))


def _usage(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    usage = payload.get("usage")
    return usage if isinstance(usage, dict) else None


def _nonneg_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value >= 0 else None


def _total_tokens(payload: Any) -> int | None:
    usage = _usage(payload)
    if usage is None:
        return None
    return _nonneg_int(usage.get("total_tokens"))


def _usage_int(payload: Any, name: str) -> int | None:
    usage = _usage(payload)
    if usage is None:
        return None
    return _nonneg_int(usage.get(name))


def _reasoning_tokens(payload: Any) -> int | None:
    usage = _usage(payload)
    if usage is None:
        return None
    details = usage.get("completion_tokens_details")
    if not isinstance(details, dict):
        return None
    return _nonneg_int(details.get("reasoning_tokens"))


def _finish_reason(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return None
    reason = choices[0].get("finish_reason")
    return reason if isinstance(reason, str) else None


async def _async_sleep(delay: float) -> None:
    import asyncio

    await asyncio.sleep(delay)


def _review_text(payload: Any) -> str:
    if not isinstance(payload, dict):
        raise AiReviewError("review response was not an object", status_code=200, retryable=False)
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        raise AiReviewError("review response had no choices", status_code=200, retryable=False)
    message = choices[0].get("message")
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        raise AiReviewError("review response was empty", status_code=200, retryable=False)
    return content.strip()[:_MAX_REVIEW_CHARS]
