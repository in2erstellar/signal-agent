"""The public page is the link a channel subscriber opens. It is not a feed."""

from __future__ import annotations

import os

from httpx import ASGITransport, AsyncClient

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://signalforge:signalforge@localhost:5432/signalforge",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ.setdefault("ENVIRONMENT", "dev")
os.environ.setdefault("LOG_LEVEL", "INFO")

from app.adapters.telegram_bot import bot_public_url, channel_target  # noqa: E402
from app.main import app  # noqa: E402
from app.templates import DISCLAIMER, channel_page  # noqa: E402


def test_channel_target_accepts_an_id_or_a_public_name() -> None:
    assert channel_target("") is None
    assert channel_target("replace-me") is None
    assert channel_target("-100123") == -100123
    assert channel_target("@SignalForgeFeed") == "@SignalForgeFeed"
    assert channel_target("SignalForgeFeed") == "@SignalForgeFeed"
    assert channel_target("bad name") is None
    assert bot_public_url("SignalForgeBot") == "https://t.me/SignalForgeBot"
    assert bot_public_url("") is None


def test_channel_page_states_the_limit_and_links_the_bot() -> None:
    html = channel_page(bot_url="https://t.me/SignalForgeBot")
    assert "не покупает" in html
    assert DISCLAIMER in html
    assert "https://t.me/SignalForgeBot" in html
    assert "иллюстрация" in html
    bare = channel_page(bot_url=None)
    assert "TELEGRAM_BOT_USERNAME" in bare
    assert "t.me/" not in bare


async def test_home_page_is_served_without_an_api_key() -> None:
    async with app.router.lifespan_context(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "SignalForge" in response.text
    assert "не финансовый совет" in response.text.lower() or "Not financial advice" in response.text
