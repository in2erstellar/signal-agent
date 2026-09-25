"""Provider switch is one config value. The method name stays generate_token_review."""

from __future__ import annotations

import json

import httpx
import pytest

from app.adapters.ai_review import (
    AiReviewClient,
    AiReviewError,
    TokenReviewInput,
    ai_review_configured,
    provider_for,
)
from app.config import Settings

SAMPLE = TokenReviewInput(
    mint="mint",
    symbol="ELIZABETH",
    name="The Amazon Monkey",
    risk_score="80",
    risk_flags=("check_lp_lock",),
    liquidity_usd="12000",
    composite_score="57.14",
)


def test_groq_and_grok_are_selected_by_name() -> None:
    groq = provider_for("groq")
    grok = provider_for("grok")
    assert groq.url == "https://api.groq.com/openai/v1/chat/completions"
    assert groq.model == "openai/gpt-oss-120b"
    assert grok.url == "https://api.x.ai/v1/chat/completions"
    assert grok.model == "grok-4"
    with pytest.raises(ValueError):
        provider_for("gemini")


def test_configured_rejects_a_placeholder(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://signalforge:signalforge@db:5432/signalforge",
    )
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")
    monkeypatch.setenv("GROQ_API_KEY", "")
    assert ai_review_configured(Settings(_env_file=None)) is False
    monkeypatch.setenv("GROQ_API_KEY", "replace-me")
    assert ai_review_configured(Settings(_env_file=None)) is False
    monkeypatch.setenv("GROQ_API_KEY", "gsk-test")
    assert ai_review_configured(Settings(_env_file=None)) is True


def test_groq_model_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+asyncpg://signalforge:signalforge@db:5432/signalforge",
    )
    monkeypatch.setenv("REDIS_URL", "redis://cache:6379/0")
    monkeypatch.setenv("AI_REVIEW_MODEL", "openai/gpt-oss-20b")
    settings = Settings(_env_file=None)
    assert settings.ai_review_model == "openai/gpt-oss-20b"
    assert provider_for("groq", model=settings.ai_review_model).model == "openai/gpt-oss-20b"


async def test_generate_token_review_posts_to_the_selected_host() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"choices": [{"message": {"content": "  Four facts.  "}}]})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http:
        groq = AiReviewClient(provider="groq", api_key="gsk-test", client=http)
        text = await groq.generate_token_review(SAMPLE)
        grok = AiReviewClient(provider="grok", api_key="xai-test", client=http)
        await grok.generate_token_review(SAMPLE)
    assert text.text == "Four facts."
    assert text.total_tokens is None
    assert [str(request.url) for request in seen] == [
        "https://api.groq.com/openai/v1/chat/completions",
        "https://api.x.ai/v1/chat/completions",
    ]
    assert seen[0].headers["Authorization"] == "Bearer gsk-test"
    body = json.loads(seen[0].content)
    assert body["model"] == "openai/gpt-oss-120b"
    assert body["reasoning_effort"] == "low"
    assert "Do not tell the reader to buy" in body["messages"][0]["content"]
    grok_body = json.loads(seen[1].content)
    assert grok_body["model"] == "grok-4"
    assert "reasoning_effort" not in grok_body


async def test_missing_key_is_not_retried() -> None:
    client = AiReviewClient(provider="groq", api_key="")
    try:
        with pytest.raises(AiReviewError) as raised:
            await client.generate_token_review(SAMPLE)
        assert raised.value.retryable is False
    finally:
        await client.aclose()
