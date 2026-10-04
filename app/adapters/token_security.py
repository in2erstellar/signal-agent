"""On-chain facts for the risk scanner. No safety verdicts live here.

Mint and freeze authorities come from ``getAccountInfo`` (jsonParsed).
Holder rows come from ``getTokenLargestAccounts`` plus the owner program of
each holder, so pool vaults can be excluded later.

LP custody is read the same way on the pool LP mint from ``RaydiumPools``.
A balance counts as locked only when its owner is the incinerator burn
wallet or an account owned by Raydium's Burn & Earn / LP Lock program.
SPL ``burn`` that only shrinks supply is not visible here.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from app.adapters.helius import HeliusClient, HeliusResponseError
from app.adapters.raydium import RaydiumPools

# https://docs.raydium.io/reference/program-addresses (mainnet-beta).
RAYDIUM_AMM_V4 = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
RAYDIUM_CPMM = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
RAYDIUM_CLMM = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
# Owner of AMM v4 pool vault token accounts.
RAYDIUM_AMM_V4_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
# Burn & Earn / LP Lock program. LP in its custody is escrowed, not destroyed.
RAYDIUM_LP_LOCK = "LockrWmn6K5twhz3y9w1dQERbmgSaRkfnTeTKbpofwE"
# Widely used incinerator wallet. Sending LP here is the burn-address pattern.
INCINERATOR = "1nc1nerator11111111111111111111111111111111"

LP_POOL_PROGRAMS = frozenset({RAYDIUM_AMM_V4, RAYDIUM_CPMM, RAYDIUM_CLMM})
LP_POOL_OWNERS = frozenset({RAYDIUM_AMM_V4_AUTHORITY})
BURN_ADDRESSES = frozenset({INCINERATOR})
LOCK_PROGRAMS = frozenset({RAYDIUM_LP_LOCK})


@dataclass(frozen=True)
class MintView:
    mint_authority: str | None
    freeze_authority: str | None
    supply_raw: int


@dataclass(frozen=True)
class HolderRow:
    owner: str
    amount_raw: int
    is_lp_pool: bool


@dataclass(frozen=True)
class HolderBook:
    supply_raw: int
    rows: list[HolderRow]


@dataclass(frozen=True)
class LpHolderRow:
    owner: str
    amount_raw: int
    is_burn_address: bool
    is_lock_program: bool


@dataclass(frozen=True)
class LpBook:
    lp_mint: str | None
    supply_raw: int | None
    rows: list[LpHolderRow]
    liquidity_usd: Decimal | None = None
    price_usd: Decimal | None = None


class HeliusTokenSecurity:
    """Reads mint, holder, and LP-custody facts. The scanner decides what they mean."""

    def __init__(self, helius: HeliusClient, raydium: RaydiumPools) -> None:
        self._helius = helius
        self._raydium = raydium

    async def mint_view(self, mint: str) -> MintView:
        return _mint_view(await self._parsed_account(mint))

    async def holder_book(self, mint: str) -> HolderBook:
        mint_view = await self.mint_view(mint)
        largest = await self._helius.rpc("getTokenLargestAccounts", [mint])
        accounts = [
            item
            for item in list(largest.get("value") or [])
            if isinstance(item, dict) and item.get("address") and item.get("amount") is not None
        ]
        if not accounts:
            return HolderBook(supply_raw=mint_view.supply_raw, rows=[])
        addresses = [str(item["address"]) for item in accounts]
        parsed = await self._helius.rpc(
            "getMultipleAccounts",
            [addresses, {"encoding": "jsonParsed"}],
        )
        infos = list(parsed.get("value") or [])
        owners = [_token_owner(info) for info in infos]
        programs = await self._account_owners([owner for owner in owners if owner])
        rows: list[HolderRow] = []
        for item, owner in zip(accounts, owners, strict=False):
            if not owner:
                continue
            program = programs.get(owner)
            rows.append(
                HolderRow(
                    owner=owner,
                    amount_raw=int(item["amount"]),
                    is_lp_pool=owner in LP_POOL_OWNERS or program in LP_POOL_PROGRAMS,
                )
            )
        rows.sort(key=lambda row: row.amount_raw, reverse=True)
        return HolderBook(supply_raw=mint_view.supply_raw, rows=rows)

    async def lp_book(self, mint: str) -> LpBook:
        quote = await self._raydium.pool_quote(mint)
        if quote.lp_mint is None:
            return LpBook(
                lp_mint=None,
                supply_raw=None,
                rows=[],
                liquidity_usd=quote.tvl_usd,
                price_usd=quote.price_usd,
            )
        custody = await self.lp_custody(quote.lp_mint)
        return LpBook(
            lp_mint=custody.lp_mint,
            supply_raw=custody.supply_raw,
            rows=custody.rows,
            liquidity_usd=quote.tvl_usd,
            price_usd=quote.price_usd,
        )

    async def lp_custody(self, lp_mint: str) -> LpBook:
        """LP holder rows for a mint already known. Does not call Raydium."""
        lp_account = _mint_view(await self._parsed_account(lp_mint))
        largest = await self._helius.rpc("getTokenLargestAccounts", [lp_mint])
        accounts = [
            item
            for item in list(largest.get("value") or [])
            if isinstance(item, dict) and item.get("address") and item.get("amount") is not None
        ]
        if not accounts:
            return LpBook(
                lp_mint=lp_mint,
                supply_raw=lp_account.supply_raw,
                rows=[],
            )
        addresses = [str(item["address"]) for item in accounts]
        parsed = await self._helius.rpc(
            "getMultipleAccounts",
            [addresses, {"encoding": "jsonParsed"}],
        )
        owners = [_token_owner(info) for info in list(parsed.get("value") or [])]
        programs = await self._account_owners([owner for owner in owners if owner])
        rows: list[LpHolderRow] = []
        for item, owner in zip(accounts, owners, strict=False):
            if not owner:
                continue
            program = programs.get(owner)
            rows.append(
                LpHolderRow(
                    owner=owner,
                    amount_raw=int(item["amount"]),
                    is_burn_address=owner in BURN_ADDRESSES,
                    is_lock_program=program in LOCK_PROGRAMS,
                )
            )
        return LpBook(
            lp_mint=lp_mint,
            supply_raw=lp_account.supply_raw,
            rows=rows,
        )

    async def _parsed_account(self, address: str) -> dict[str, Any]:
        result = await self._helius.rpc(
            "getAccountInfo",
            [address, {"encoding": "jsonParsed"}],
        )
        value = result.get("value")
        if not isinstance(value, dict):
            raise HeliusResponseError(200, f"account {address} was not found")
        return value

    async def _account_owners(self, addresses: list[str]) -> dict[str, str | None]:
        unique = list(dict.fromkeys(addresses))
        if not unique:
            return {}
        result = await self._helius.rpc(
            "getMultipleAccounts",
            [unique, {"encoding": "base64"}],
        )
        owners: dict[str, str | None] = {}
        for address, info in zip(unique, list(result.get("value") or []), strict=False):
            if isinstance(info, dict) and isinstance(info.get("owner"), str):
                owners[address] = info["owner"]
            else:
                owners[address] = None
        return owners


def _mint_view(account: dict[str, Any]) -> MintView:
    data = account.get("data")
    parsed = data.get("parsed") if isinstance(data, dict) else None
    info = parsed.get("info") if isinstance(parsed, dict) else None
    if not isinstance(info, dict) or info.get("supply") is None:
        raise HeliusResponseError(200, "parsed mint account is missing supply")
    return MintView(
        mint_authority=_optional_address(info.get("mintAuthority")),
        freeze_authority=_optional_address(info.get("freezeAuthority")),
        supply_raw=int(info["supply"]),
    )


def _optional_address(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    return None


def _token_owner(info: Any) -> str | None:
    if not isinstance(info, dict):
        return None
    data = info.get("data")
    parsed = data.get("parsed") if isinstance(data, dict) else None
    account_info = parsed.get("info") if isinstance(parsed, dict) else None
    owner = account_info.get("owner") if isinstance(account_info, dict) else None
    if isinstance(owner, str) and owner:
        return owner
    return None
