"""The worker must refuse a placeholder Helius key or a schema it was not built for."""

from pathlib import Path

import pytest

from app.services.startup_checks import (
    StartupConfigurationError,
    code_schema_head,
    require_helius_api_key,
    require_schema_revision,
)

ROOT = Path(__file__).resolve().parents[1]


def test_placeholder_helius_key_refuses_start() -> None:
    with pytest.raises(StartupConfigurationError):
        require_helius_api_key("replace-me")
    with pytest.raises(StartupConfigurationError):
        require_helius_api_key("  ")
    require_helius_api_key("a-real-key")


def test_schema_mismatch_refuses_start() -> None:
    with pytest.raises(StartupConfigurationError):
        require_schema_revision("0014_funder_cache_signature", code_schema_head())
    with pytest.raises(StartupConfigurationError):
        require_schema_revision(None, code_schema_head())
    require_schema_revision(code_schema_head(), code_schema_head())


def test_schema_head_is_read_from_the_migration_scripts() -> None:
    head = code_schema_head()
    versions = list((ROOT / "alembic" / "versions").glob("*.py"))
    assert any(head in path.read_text(encoding="utf-8") for path in versions)
    assert head not in Path("app/services/startup_checks.py").read_text(encoding="utf-8")
