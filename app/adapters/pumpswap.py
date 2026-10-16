"""Canonical PumpSwap pool reader. Local PDA plus one ``getAccountInfo``.

Migrations from the pump.fun program land in a pool whose address is
``["pool", index=0, pool-authority, base mint, WSOL]`` under
``pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA``. Raydium's HTTP index does
not list that pool. This module derives the address and parses the account.
It does not decide whether LP is locked: ``Pool.lp_supply`` ignores user
burns, so lock checks keep using the LP mint's real supply and holders.

A missing account is an empty pool. A present account with the wrong owner,
the wrong discriminator, or fewer than 211 bytes is a parse error.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import Any, Protocol

from solders.pubkey import Pubkey

PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
WSOL_MINT = "So11111111111111111111111111111111111111112"
DEFAULT_PUBKEY = "11111111111111111111111111111111"

# Anchor account discriminator for the Pool type in pump_amm.json.
POOL_DISCRIMINATOR = bytes((241, 154, 109, 4, 17, 177, 109, 188))
# Bytes required to read lp_mint, both vaults, and lp_supply. The tail after
# coin_creator was appended by later program upgrades.
MIN_POOL_ACCOUNT_LENGTH = 211

_CANONICAL_INDEX = (0).to_bytes(2, "little")
_OFF_BUMP = 8
_OFF_INDEX = 9
_OFF_CREATOR = 11
_OFF_BASE_MINT = 43
_OFF_QUOTE_MINT = 75
_OFF_LP_MINT = 107
_OFF_BASE_VAULT = 139
_OFF_QUOTE_VAULT = 171
_OFF_LP_SUPPLY = 203
_OFF_COIN_CREATOR = 211
_OFF_MAYHEM = 243
_OFF_CASHBACK = 244
_OFF_VIRTUAL_QUOTE = 245
_OFF_CREATOR_FEE = 261
_OFF_CAN_EDIT_FEE = 269
_OFF_HOLDER_REWARD = 270


class PumpSwapAccountError(Exception):
    """The account is present but is not a readable PumpSwap pool.

    This is a parse failure. Callers must not treat it as "no pool".
    """


class AccountRpc(Protocol):
    async def rpc(self, method: str, params: list[Any]) -> dict[str, Any]: ...


@dataclass(frozen=True)
class PumpSwapPool:
    """Decoded canonical pool. ``creator`` is the pool-authority PDA.

    ``coin_creator`` is the coin creator wallet. ``lp_supply`` is the pool's
    own counter and does not shrink when a user burns LP, so it is not the
    supply a lock check should use.
    """

    address: str
    pool_bump: int
    index: int
    creator: str
    base_mint: str
    quote_mint: str
    lp_mint: str
    pool_base_token_account: str
    pool_quote_token_account: str
    lp_supply: int
    coin_creator: str
    is_mayhem_mode: bool
    is_cashback_coin: bool
    virtual_quote_reserves: int
    creator_fee_bps: int
    can_edit_creator_fee: bool
    is_holder_reward: bool


@dataclass(frozen=True)
class PumpSwapReserves:
    """Vault balances. Quote includes ``virtual_quote_reserves``.

    Both amounts are raw token atoms. For the canonical WSOL quote that is
    lamports, not a USD price.
    """

    base_raw: int
    quote_raw: int


def bonding_curve_address(mint: str) -> str:
    """PDA of the pump.fun bonding curve for ``mint``."""
    address, _bump = Pubkey.find_program_address(
        [b"bonding-curve", bytes(Pubkey.from_string(mint))],
        Pubkey.from_string(PUMP_PROGRAM_ID),
    )
    return str(address)


def canonical_pool_address(mint: str) -> str:
    """PDA of the index-0 WSOL pool created by pump.fun ``migrate``."""
    mint_key = Pubkey.from_string(mint)
    authority, _bump = Pubkey.find_program_address(
        [b"pool-authority", bytes(mint_key)],
        Pubkey.from_string(PUMP_PROGRAM_ID),
    )
    wsol = Pubkey.from_string(WSOL_MINT)
    pool, _pool_bump = Pubkey.find_program_address(
        [b"pool", _CANONICAL_INDEX, bytes(authority), bytes(mint_key), bytes(wsol)],
        Pubkey.from_string(PUMP_AMM_PROGRAM_ID),
    )
    return str(pool)


def parse_pool_account(raw: bytes, owner: str, *, address: str) -> PumpSwapPool:
    """Decode one account. Owner and discriminator are checked before fields."""
    if owner != PUMP_AMM_PROGRAM_ID:
        raise PumpSwapAccountError("pool account owner is not the PumpSwap program")
    if len(raw) < len(POOL_DISCRIMINATOR) or raw[: len(POOL_DISCRIMINATOR)] != POOL_DISCRIMINATOR:
        raise PumpSwapAccountError("pool account discriminator does not match")
    if len(raw) < MIN_POOL_ACCOUNT_LENGTH:
        raise PumpSwapAccountError("pool account is shorter than 211 bytes")
    return PumpSwapPool(
        address=address,
        pool_bump=_u8(raw, _OFF_BUMP),
        index=_u16(raw, _OFF_INDEX),
        creator=_pubkey(raw, _OFF_CREATOR),
        base_mint=_pubkey(raw, _OFF_BASE_MINT),
        quote_mint=_pubkey(raw, _OFF_QUOTE_MINT),
        lp_mint=_pubkey(raw, _OFF_LP_MINT),
        pool_base_token_account=_pubkey(raw, _OFF_BASE_VAULT),
        pool_quote_token_account=_pubkey(raw, _OFF_QUOTE_VAULT),
        lp_supply=_u64(raw, _OFF_LP_SUPPLY),
        coin_creator=_optional_pubkey(raw, _OFF_COIN_CREATOR),
        is_mayhem_mode=_optional_bool(raw, _OFF_MAYHEM),
        is_cashback_coin=_optional_bool(raw, _OFF_CASHBACK),
        virtual_quote_reserves=_optional_i128(raw, _OFF_VIRTUAL_QUOTE),
        creator_fee_bps=_optional_u64(raw, _OFF_CREATOR_FEE),
        can_edit_creator_fee=_optional_bool(raw, _OFF_CAN_EDIT_FEE),
        is_holder_reward=_optional_bool(raw, _OFF_HOLDER_REWARD),
    )


class PumpSwapPools:
    """Reads the canonical pool through the existing Helius JSON-RPC client."""

    def __init__(self, rpc: AccountRpc) -> None:
        self._rpc = rpc

    async def canonical_pool(self, mint: str) -> PumpSwapPool | None:
        """Return the pool, or ``None`` when the account does not exist.

        A present account that fails the owner, discriminator, or length
        check raises ``PumpSwapAccountError``.
        """
        address = canonical_pool_address(mint)
        result = await self._rpc.rpc(
            "getAccountInfo",
            [address, {"encoding": "base64"}],
        )
        if not isinstance(result, dict) or "value" not in result:
            raise PumpSwapAccountError("getAccountInfo result has no value")
        value = result["value"]
        if value is None:
            return None
        if not isinstance(value, dict):
            raise PumpSwapAccountError("getAccountInfo value is not an account")
        owner = value.get("owner")
        pool = parse_pool_account(
            _decode_account_data(value.get("data")),
            owner if isinstance(owner, str) else "",
            address=address,
        )
        if pool.base_mint != mint:
            raise PumpSwapAccountError("pool base mint does not match the requested mint")
        return pool

    async def reserves(self, pool: PumpSwapPool) -> PumpSwapReserves:
        """Base vault balance, and quote vault balance plus virtual reserves."""
        base = await self._token_amount(pool.pool_base_token_account)
        quote_vault = await self._token_amount(pool.pool_quote_token_account)
        return PumpSwapReserves(
            base_raw=base,
            quote_raw=quote_vault + pool.virtual_quote_reserves,
        )

    async def _token_amount(self, address: str) -> int:
        result = await self._rpc.rpc("getTokenAccountBalance", [address])
        value = result.get("value") if isinstance(result, dict) else None
        amount = value.get("amount") if isinstance(value, dict) else None
        if isinstance(amount, bool) or not isinstance(amount, (int, str)):
            raise PumpSwapAccountError(f"token account {address} has no balance")
        try:
            return int(amount)
        except ValueError as exc:
            raise PumpSwapAccountError(f"token account {address} has no balance") from exc


def _decode_account_data(data: Any) -> bytes:
    if isinstance(data, list) and data and isinstance(data[0], str):
        try:
            return base64.b64decode(data[0], validate=True)
        except Exception as exc:
            raise PumpSwapAccountError("pool account data is not base64") from exc
    raise PumpSwapAccountError("pool account data is missing")


def _u8(raw: bytes, offset: int) -> int:
    return raw[offset]


def _u16(raw: bytes, offset: int) -> int:
    return int.from_bytes(raw[offset : offset + 2], "little")


def _u64(raw: bytes, offset: int) -> int:
    return int.from_bytes(raw[offset : offset + 8], "little")


def _pubkey(raw: bytes, offset: int) -> str:
    return str(Pubkey.from_bytes(raw[offset : offset + 32]))


def _optional_pubkey(raw: bytes, offset: int) -> str:
    if len(raw) < offset + 32:
        return DEFAULT_PUBKEY
    return _pubkey(raw, offset)


def _optional_bool(raw: bytes, offset: int) -> bool:
    if len(raw) <= offset:
        return False
    return raw[offset] != 0


def _optional_u64(raw: bytes, offset: int) -> int:
    if len(raw) < offset + 8:
        return 0
    return _u64(raw, offset)


def _optional_i128(raw: bytes, offset: int) -> int:
    if len(raw) < offset + 16:
        return 0
    return int.from_bytes(raw[offset : offset + 16], "little", signed=True)
