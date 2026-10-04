"""Creator-cluster fields for a signal card.

These numbers are not inputs to ``composite_score``. A missing funder or a
missing price is ``unavailable`` with a reason. Zero is only a counted result.

Previous launches are mints already stored for this creator. The card says
"since tracking started" so the count is not read as a full wallet history.

The dev root is this creator's own funding edge (hop 1, then hop 2). It is
not the ``funder_clusters`` row built from the token's top holders.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import OutcomeCheck, Token, WalletFunder

logger = structlog.get_logger(__name__)

DROP_LIMIT = Decimal("0.1")

# Finished limit of the median-sell line. Not a TODO and not a chain walk.
# A sell would have to already sit in wallet_txs for a wallet this process
# tracks. Discovery tokens do not put the creator's sells there, and this
# field does not read the ledger or the wider network to invent one.
MEDIAN_SELL_REASON = "not_in_tracked_ledger"
MEDIAN_SELL_SHOWN = "unavailable (not in the tracked-wallet ledger)"


@dataclass(frozen=True)
class HolderStake:
    owner: str
    amount_raw: int
    excluded: bool
    root_funder: str | None


@dataclass(frozen=True)
class Distribution:
    status: str
    share: Decimal | None
    reason: str | None


@dataclass(frozen=True)
class HistoryLine:
    status: str
    shown: str
    reason: str | None = None


def distribution_of(stakes: list[HolderStake], supply_raw: int, dev_root: str | None) -> Distribution:
    """Share of mint supply on scanned holders with the creator's funder root.

    A holder we scanned and could not classify makes the whole check
    unavailable. Holders we never received are not treated as zero.
    """
    if dev_root is None:
        return Distribution("unavailable", None, "creator_funder_missing")
    if supply_raw <= 0:
        return Distribution("unavailable", None, "no_holder_balances")
    scanned = [stake for stake in stakes if not stake.excluded]
    if not scanned:
        return Distribution("unavailable", None, "no_holder_balances")
    if any(stake.root_funder is None for stake in scanned):
        return Distribution("unavailable", None, "holder_funder_missing")
    matched = sum(stake.amount_raw for stake in scanned if stake.root_funder == dev_root)
    return Distribution("measured", Decimal(matched) / Decimal(supply_raw), None)


def share_above_limit(context: dict[str, Any] | None, limit: Decimal) -> bool:
    """True only for a measured share. Unavailable is not treated as zero or as over."""
    if not context or context.get("distribution_status") != "measured":
        return False
    raw = context.get("distribution_share")
    if raw is None:
        return False
    return Decimal(str(raw)) > limit


def first_hour_drop(listing: Decimal, later: Decimal) -> bool:
    """True when the later price is at most 10% of the listing price."""
    return later <= listing * DROP_LIMIT


def summarize_price_drops(
    rows: list[tuple[str | None, Decimal | None, str | None, Decimal | None]],
) -> HistoryLine:
    """Each row is listing reason, listing price, +1h reason, +1h price.

    An empty list means this creator has no earlier mint in the database.
    """
    if not rows:
        return HistoryLine("unavailable", "unavailable (no earlier launches)", "no_earlier_launches")
    compared: list[tuple[Decimal, Decimal]] = []
    saw_lookup_failure = False
    saw_no_pool = False
    for listing_reason, listing_price, later_reason, later_price in rows:
        if listing_price is not None and later_price is not None:
            compared.append((listing_price, later_price))
            continue
        if listing_reason == "price_lookup_failed" or later_reason == "price_lookup_failed":
            saw_lookup_failure = True
        if listing_reason == "no_pool" or later_reason == "no_pool":
            saw_no_pool = True
    if compared:
        dropped = sum(1 for listing, later in compared if first_hour_drop(listing, later))
        shown = f"{dropped} of {len(compared)}"
        return HistoryLine("measured", shown, None)
    if saw_lookup_failure:
        return HistoryLine("unavailable", "unavailable (price lookup failed)", "price_lookup_failed")
    if saw_no_pool and not saw_lookup_failure:
        return HistoryLine("unavailable", "unavailable (no pool yet)", "no_pool_yet")
    return HistoryLine("unavailable", "unavailable (no follow-up yet)", "no_follow_up_yet")


def summarize_lp_stayed(flags: list[bool | None]) -> HistoryLine:
    """``None`` means that earlier mint has no follow-up LP observation."""
    if not flags:
        return HistoryLine("unavailable", "unavailable (no earlier launches)", "no_earlier_launches")
    known = [flag for flag in flags if flag is not None]
    if not known:
        return HistoryLine("unavailable", "unavailable (no follow-up yet)", "no_follow_up_yet")
    stayed = sum(1 for flag in known if flag)
    return HistoryLine("measured", f"{stayed} of {len(known)}", None)


def creator_context_dict(
    *,
    creator: str | None,
    launches: int,
    distribution: Distribution,
    drops: HistoryLine,
    lp_stayed: HistoryLine,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "creator": creator,
        "launches_since_tracking": launches,
        "distribution_status": distribution.status,
        "distribution_share": None if distribution.share is None else str(distribution.share),
        "distribution_reason": distribution.reason,
        "price_drop_status": drops.status,
        "price_drop_shown": drops.shown,
        "price_drop_reason": drops.reason,
        "lp_stayed_status": lp_stayed.status,
        "lp_stayed_shown": lp_stayed.shown,
        "lp_stayed_reason": lp_stayed.reason,
        "median_sell_status": "unavailable",
        "median_sell_shown": MEDIAN_SELL_SHOWN,
        "median_sell_reason": MEDIAN_SELL_REASON,
    }
    return payload


async def stored_root(session: AsyncSession, wallet: str) -> str | None:
    """Hop-2 funder when that edge was stored, otherwise the hop-1 funder."""
    direct = await session.scalar(
        select(WalletFunder).where(
            WalletFunder.wallet_address == wallet,
            WalletFunder.hop_depth == 1,
        )
    )
    if direct is None:
        return None
    parent = await session.scalar(
        select(WalletFunder).where(
            WalletFunder.wallet_address == direct.funder_address,
            WalletFunder.hop_depth == 2,
        )
    )
    if parent is None:
        return direct.funder_address
    return parent.funder_address


async def measure_distribution(
    session: AsyncSession,
    token: Token,
    book: Any,
    *,
    dev_root: str | None,
) -> None:
    """Store the scanned-holder share. Does not change risk_score."""
    if token.creator_address is None:
        _store_distribution(token, Distribution("unavailable", None, "creator_missing"))
        return
    if book is None:
        _store_distribution(token, Distribution("unavailable", None, "holder_book_missing"))
        return
    stakes: list[HolderStake] = []
    curve = token.bonding_curve_key
    for row in book.rows:
        excluded = bool(row.is_lp_pool) or (curve is not None and row.owner == curve)
        root = None
        if not excluded:
            root = await stored_root(session, row.owner)
        stakes.append(
            HolderStake(
                owner=row.owner,
                amount_raw=int(row.amount_raw),
                excluded=excluded,
                root_funder=root,
            )
        )
    _store_distribution(token, distribution_of(stakes, int(book.supply_raw), dev_root))


def _store_distribution(token: Token, result: Distribution) -> None:
    token.dev_distribution_status = result.status
    token.dev_distribution_share = result.share
    token.dev_distribution_reason = result.reason
    if result.status != "measured":
        logger.warning("distribution_unavailable", mint=token.mint, reason=result.reason)


async def load_creator_context(session: AsyncSession, token: Token) -> dict[str, Any]:
    creator = token.creator_address
    if not creator:
        empty = Distribution("unavailable", None, "creator_missing")
        none = HistoryLine("unavailable", "unavailable (creator missing)", "creator_missing")
        return creator_context_dict(
            creator=None,
            launches=0,
            distribution=empty,
            drops=none,
            lp_stayed=none,
        )
    earlier_ids = list(
        await session.scalars(
            select(Token.id).where(Token.creator_address == creator, Token.id != token.id)
        )
    )
    launches = len(earlier_ids)
    share = _decimal_or_none(token.dev_distribution_share)
    if token.dev_distribution_status in {"measured", "unavailable"}:
        distribution = Distribution(
            token.dev_distribution_status or "unavailable",
            share,
            token.dev_distribution_reason,
        )
    else:
        distribution = Distribution("unavailable", None, "distribution_not_run")
    drops, stayed = await _history(session, earlier_ids)
    return creator_context_dict(
        creator=creator,
        launches=launches,
        distribution=distribution,
        drops=drops,
        lp_stayed=stayed,
    )


async def _history(
    session: AsyncSession,
    earlier_ids: list[Any],
) -> tuple[HistoryLine, HistoryLine]:
    if not earlier_ids:
        return summarize_price_drops([]), summarize_lp_stayed([])
    rows = list(
        await session.scalars(
            select(OutcomeCheck).where(
                OutcomeCheck.token_id.in_(earlier_ids),
                OutcomeCheck.horizon.in_(("listing", "1h")),
            )
        )
    )
    by_token: dict[Any, dict[str, OutcomeCheck]] = {}
    for row in rows:
        by_token.setdefault(row.token_id, {})[row.horizon] = row
    price_rows: list[tuple[str | None, Decimal | None, str | None, Decimal | None]] = []
    lp_flags: list[bool | None] = []
    for token_id in earlier_ids:
        pair = by_token.get(token_id, {})
        listing = pair.get("listing")
        later = pair.get("1h")
        price_rows.append(
            (
                None if listing is None else listing.price_reason,
                None if listing is None else listing.price_usd,
                None if later is None else later.price_reason,
                None if later is None else later.price_usd,
            )
        )
        lp_flags.append(None if later is None else later.lp_locked)
    return summarize_price_drops(price_rows), summarize_lp_stayed(lp_flags)


def _decimal_or_none(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(value)
