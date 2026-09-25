"""One review method, two providers.

``AI_REVIEW_PROVIDER`` in config selects the host and model. Call sites only
use ``generate_token_review``.

Groq is the default because its free tier allows on the order of a thousand
Llama requests per day, which covers ``AI_REVIEW_DAILY_BUDGET`` (150).
Gemini's free tier is tens of requests per day and cannot serve that cap.
``grok`` points at xAI's OpenAI-compatible chat endpoint for a later switch.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import structlog

from app.config import AI_REVIEW_PROVIDER, Settings

logger = structlog.get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 20.0
MAX_ATTEMPTS = 3
_MAX_REVIEW_CHARS = 1500
_RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})

_SYSTEM = (
    "You write a short English risk note for a Solana memecoin monitor. "
    "Use only the fields in the user message. Do not tell the reader to buy "
    "or sell. Do not invent liquidity, holders, or social proof. "
    "Four sentences at most."
)


class AiReviewError(Exception):
    def __init__(self, message: str, *, status_code: int, retryable: bool) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable


@dataclass(frozen=True)
class TokenReviewInput:
    mint: str
    symbol: str | None
    name: str | None
    risk_score: str | None
    risk_flags: tuple[str, ...]
    liquidity_usd: str | None
    composite_score: str | None


@dataclass(frozen=True)
class _Provider:
    url: str
    model: str


def provider_for(name: str) -> _Provider:
    if name == "groq":
        return _Provider(
            "https://api.groq.com/openai/v1/chat/completions",
            "llama-3.3-70b-versatile",
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
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._provider = provider_for(provider)
        self._provider_name = provider
        self._api_key = api_key
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS)

    @classmethod
    def from_settings(cls, settings: Settings) -> AiReviewClient:
        return cls(
            provider=AI_REVIEW_PROVIDER,
            api_key=review_api_key(settings),
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def generate_token_review(self, token: TokenReviewInput) -> str:
        if not self._api_key or self._api_key == "replace-me":
            raise AiReviewError(
                f"{self._provider_name} API key is not configured",
                status_code=0,
                retryable=False,
            )
        body = {
            "model": self._provider.model,
            "temperature": 0.2,
            "max_tokens": 400,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": _user_prompt(token)},
            ],
        }
        response = await self._post(body)
        return _review_text(response.json())

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


def _user_prompt(token: TokenReviewInput) -> str:
    flags = ", ".join(token.risk_flags) if token.risk_flags else "none"
    return "\n".join(
        [
            f"mint: {token.mint}",
            f"symbol: {token.symbol or 'unknown'}",
            f"name: {token.name or 'unknown'}",
            f"risk_score: {token.risk_score or 'unknown'}",
            f"risk_flags: {flags}",
            f"liquidity_usd: {token.liquidity_usd or 'unknown'}",
            f"composite_score: {token.composite_score or 'unknown'}",
            "risk_score is a known-check score, not a claim the token is safe.",
        ]
    )


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
