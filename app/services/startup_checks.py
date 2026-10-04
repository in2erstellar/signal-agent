"""Refuse to scan when the process would only produce unavailable reads.

Settings are loaded once, at process start. A later edit of ``.env`` does
nothing until the worker is started again.

The schema head is read from the Alembic scripts in this tree. It is not a
revision id written into this file, so a new migration does not leave the
check pointing at an old parent.
"""

from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

_PLACEHOLDERS = frozenset({"", "replace-me"})
_ROOT = Path(__file__).resolve().parents[2]


class StartupConfigurationError(RuntimeError):
    """The worker must exit. Continuing would hide a broken configuration."""


def require_helius_api_key(value: str) -> None:
    if value.strip() in _PLACEHOLDERS:
        raise StartupConfigurationError(
            "HELIUS_API_KEY is empty or replace-me. "
            "The worker will not start: every holder lookup would fail "
            "and no signal would be published."
        )


def code_schema_head() -> str:
    config = Config(str(_ROOT / "alembic.ini"))
    config.set_main_option("script_location", str(_ROOT / "alembic"))
    script = ScriptDirectory.from_config(config)
    heads = script.get_heads()
    if len(heads) != 1:
        raise StartupConfigurationError(
            f"Alembic has {len(heads)} heads ({', '.join(heads)}). "
            "The worker will not start until the migration graph has one head."
        )
    return heads[0]


def require_schema_revision(actual: str | None, expected: str) -> None:
    if actual != expected:
        raise StartupConfigurationError(
            f"Database alembic_version is {actual!r}, code head is {expected!r}. "
            "The worker will not start on a schema it was not built for."
        )


async def assert_worker_ready(engine: AsyncEngine, helius_api_key: str) -> None:
    """Check the Helius key and the schema together. Either failure stops startup."""
    require_helius_api_key(helius_api_key)
    expected = code_schema_head()
    async with engine.connect() as connection:
        rows = list(await connection.scalars(text("SELECT version_num FROM alembic_version")))
    actual = rows[0] if len(rows) == 1 else None
    require_schema_revision(actual, expected)
