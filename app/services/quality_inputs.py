"""Readings for the checklist lines that need a call outside the database.

Each reading fails on its own. A failure is logged and becomes ``None``
(unavailable on the card), never a pass. Cost per candidate: one DexScreener
call, one ``getAccountInfo``, a few ``getSignaturesForAddress`` pages on the
bonding curve, and one metadata fetch through the IPFS gateway.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

import structlog

from app.adapters.dexscreener import DexQuote
from app.adapters.pumpswap import bonding_curve_address
from app.adapters.token_metadata import MetadataSocials
from app.adapters.token_security import MintView
from app.config import QUALITY_LAUNCH_SLOTS, QUALITY_MAX_CURVE_SIGNATURES
from app.db.models import Token
from app.services.quality_checks import ExternalReadings

logger = structlog.get_logger(__name__)


class PriceSource(Protocol):
    async def token_quote(self, mint: str) -> DexQuote | None: ...


class MintSource(Protocol):
    async def mint_view(self, mint: str) -> MintView: ...


class HistorySource(Protocol):
    async def earliest_signatures(
        self, address: str, *, max_signatures: int
    ) -> list[Mapping[str, Any]]: ...


class MetadataSource(Protocol):
    async def socials(self, uri: str) -> MetadataSocials | None: ...


def launch_trades(rows: list[Mapping[str, Any]]) -> int | None:
    """Successful curve transactions in the creation slot window, the create excluded.

    ``rows`` are oldest first. The first successful row is taken as the create.
    """
    ok = [row for row in rows if row.get("err") is None and isinstance(row.get("slot"), int)]
    if not ok:
        return None
    first_slot = int(ok[0]["slot"])
    in_window = [row for row in ok if int(row["slot"]) < first_slot + QUALITY_LAUNCH_SLOTS]
    return len(in_window) - 1


class QualityReader:
    def __init__(
        self,
        prices: PriceSource,
        mints: MintSource,
        history: HistorySource,
        metadata: MetadataSource,
    ) -> None:
        self._prices = prices
        self._mints = mints
        self._history = history
        self._metadata = metadata

    async def read(self, token: Token) -> ExternalReadings:
        dex = await self._guard("dexscreener", token.mint, self._prices.token_quote(token.mint))
        view = await self._guard("mint_view", token.mint, self._mints.mint_view(token.mint))
        trades = await self._guard("launch_trades", token.mint, self._launch(token))
        links = await self._guard("metadata", token.mint, self._links(view))
        return ExternalReadings(
            dex=dex,
            mint_view=view,
            launch_trades=trades,
            metadata_links=links,
        )

    async def _launch(self, token: Token) -> int | None:
        curve = token.bonding_curve_key or bonding_curve_address(token.mint)
        rows = await self._history.earliest_signatures(
            curve, max_signatures=QUALITY_MAX_CURVE_SIGNATURES
        )
        return launch_trades(rows)

    async def _links(self, view: MintView | None) -> dict[str, str] | None:
        if view is None or view.metadata_uri is None:
            return None
        socials = await self._metadata.socials(view.metadata_uri)
        return None if socials is None else socials.links

    async def _guard(self, reading: str, mint: str, call: Any) -> Any:
        try:
            return await call
        except Exception as exc:
            logger.warning(
                "quality_reading_unavailable",
                reading=reading,
                mint=mint,
                error_type=type(exc).__name__,
                error=str(exc)[:200],
            )
            return None
