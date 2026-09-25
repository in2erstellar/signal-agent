"""Live funder-graph check against Helius. Not for pytest.

Usage:
    uv run python scripts/manual_test_funder_graph.py <TOKEN_MINT>

Reads HELIUS_API_KEY, HELIUS_API_BASE_URL, HELIUS_RPC_URL and DATABASE_URL
from the environment or .env. The default lookup uses standard RPC.
``FUNDER_LOOKUP_METHOD = helius_funded_by`` calls the paid Wallet API.

This script performs real network calls and writes rows to the database.
"""

from __future__ import annotations

import asyncio
import sys

from app.adapters.helius import HeliusClient
from app.config import get_settings
from app.db.base import create_engine, create_sessionmaker
from app.services.funder_graph import FunderGraphBuilder


async def _run(mint: str) -> None:
    settings = get_settings()
    api_key = settings.helius_api_key.get_secret_value()
    if not api_key or api_key == "replace-me":
        raise SystemExit("Set HELIUS_API_KEY in .env before running this script.")

    engine = create_engine(settings)
    session_factory = create_sessionmaker(engine)
    try:
        async with HeliusClient.from_settings(settings) as helius:
            async with session_factory() as session:
                clusters = await FunderGraphBuilder(session, helius).build_graph_for_token(mint)
                await session.commit()
                if not clusters:
                    print(f"No significant cluster for {mint}")
                    return
                for cluster in clusters:
                    print(
                        f"root={cluster.root_funder} size={cluster.cluster_size} "
                        f"confidence={cluster.confidence} members={cluster.member_wallets}"
                    )
    finally:
        await engine.dispose()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/manual_test_funder_graph.py <TOKEN_MINT>")
    asyncio.run(_run(sys.argv[1]))


if __name__ == "__main__":
    main()
