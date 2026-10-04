"""PumpSwap account layout and the canonical-pool RPC read. Fixtures only."""

from __future__ import annotations

import base64

import pytest
from solders.pubkey import Pubkey

from app.adapters.pumpswap import (
    DEFAULT_PUBKEY,
    POOL_DISCRIMINATOR,
    PUMP_AMM_PROGRAM_ID,
    WSOL_MINT,
    PumpSwapAccountError,
    PumpSwapPools,
    canonical_pool_address,
    parse_pool_account,
)
from app.adapters.raydium import PoolQuote
from app.adapters.token_security import HeliusTokenSecurity

# Official PumpSwap README example. pool-authority and the index-0 WSOL pool.
KNOWN_MINT = "7LSsEoJGhLeZzGvDofTdNg7M3JttxQqGWNLo6vWMpump"
KNOWN_POOL = "GseMAnNDvntR5uFePZ51yZBXzNSn7GdFPkfHwfr6d77J"
KNOWN_AUTHORITY = "9XDYTfQKwW8sHPqnFdUreMmtmffmkHVPGTNV2e3LKxNW"

LP_MINT = "6dpnPD6UWDw5hbJEuPQwnCCMba1JYwHANKuL6GQ6otAH"
BASE_VAULT = "5jMpkf4JF4noHftLgNKyPNh6roVfPSGSjuEk3U4eLKRa"
QUOTE_VAULT = "43DVcZR4kQFjh4Xm2i3DcneRxNjZp7HMud8yDrJWrDr8"
COIN_CREATOR = "5L5k7gtNLbeXdzpvNrFshg1E1id1ceUDfc6vPUTxp98q"


class QueueHelius:
    def __init__(self, results: list[dict]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, list]] = []

    async def rpc(self, method: str, params: list) -> dict:
        self.calls.append((method, params))
        if not self._results:
            raise AssertionError(f"unexpected rpc {method}")
        return self._results.pop(0)


class EmptyRaydium:
    async def pool_quote(self, mint: str) -> PoolQuote:
        return PoolQuote(lp_mint=None, tvl_usd=None, price_usd=None)


def _encode_pool(
    *,
    discriminator: bytes = POOL_DISCRIMINATOR,
    bump: int = 254,
    index: int = 0,
    creator: str = KNOWN_AUTHORITY,
    base_mint: str = KNOWN_MINT,
    quote_mint: str = WSOL_MINT,
    lp_mint: str = LP_MINT,
    base_vault: str = BASE_VAULT,
    quote_vault: str = QUOTE_VAULT,
    lp_supply: int = 4_194_352_106_721,
    coin_creator: str = COIN_CREATOR,
    is_mayhem_mode: bool = False,
    is_cashback_coin: bool = False,
    virtual_quote_reserves: int = 0,
    creator_fee_bps: int = 0,
    can_edit_creator_fee: bool = False,
    is_holder_reward: bool = False,
    length: int | None = None,
) -> bytes:
    raw = bytearray(271)
    raw[:8] = discriminator
    raw[8] = bump
    raw[9:11] = index.to_bytes(2, "little")
    raw[11:43] = bytes(Pubkey.from_string(creator))
    raw[43:75] = bytes(Pubkey.from_string(base_mint))
    raw[75:107] = bytes(Pubkey.from_string(quote_mint))
    raw[107:139] = bytes(Pubkey.from_string(lp_mint))
    raw[139:171] = bytes(Pubkey.from_string(base_vault))
    raw[171:203] = bytes(Pubkey.from_string(quote_vault))
    raw[203:211] = lp_supply.to_bytes(8, "little")
    raw[211:243] = bytes(Pubkey.from_string(coin_creator))
    raw[243] = int(is_mayhem_mode)
    raw[244] = int(is_cashback_coin)
    raw[245:261] = virtual_quote_reserves.to_bytes(16, "little", signed=True)
    raw[261:269] = creator_fee_bps.to_bytes(8, "little")
    raw[269] = int(can_edit_creator_fee)
    raw[270] = int(is_holder_reward)
    if length is None:
        return bytes(raw)
    return bytes(raw[:length])


def _account(raw: bytes, *, owner: str = PUMP_AMM_PROGRAM_ID) -> dict:
    return {
        "value": {
            "owner": owner,
            "data": [base64.b64encode(raw).decode(), "base64"],
            "lamports": 1,
            "executable": False,
        }
    }


def _parsed_mint(supply: str) -> dict:
    return {
        "value": {
            "data": {
                "parsed": {
                    "info": {
                        "mintAuthority": None,
                        "freezeAuthority": None,
                        "supply": supply,
                    }
                }
            }
        }
    }


def test_canonical_pool_address_matches_the_pump_readme_example() -> None:
    assert canonical_pool_address(KNOWN_MINT) == KNOWN_POOL


def test_short_pool_account_defaults_the_tail() -> None:
    short = _encode_pool(length=243)
    assert 211 <= len(short) < 301
    pool = parse_pool_account(short, PUMP_AMM_PROGRAM_ID, address=KNOWN_POOL)
    assert pool.lp_mint == LP_MINT
    assert pool.pool_base_token_account == BASE_VAULT
    assert pool.pool_quote_token_account == QUOTE_VAULT
    assert pool.lp_supply == 4_194_352_106_721
    assert pool.coin_creator == COIN_CREATOR
    assert pool.creator == KNOWN_AUTHORITY
    assert pool.is_mayhem_mode is False
    assert pool.is_cashback_coin is False
    assert pool.virtual_quote_reserves == 0
    assert pool.creator_fee_bps == 0
    assert pool.can_edit_creator_fee is False
    assert pool.is_holder_reward is False

    # A partial coin_creator is not a pubkey. The whole tail stays at zero.
    partial = parse_pool_account(
        _encode_pool(length=230),
        PUMP_AMM_PROGRAM_ID,
        address=KNOWN_POOL,
    )
    assert partial.lp_supply == 4_194_352_106_721
    assert partial.coin_creator == DEFAULT_PUBKEY
    assert partial.is_mayhem_mode is False
    assert partial.virtual_quote_reserves == 0
    assert partial.is_holder_reward is False


def test_bad_discriminator_is_a_parse_error() -> None:
    raw = _encode_pool(discriminator=bytes((1, 2, 3, 4, 5, 6, 7, 8)))
    with pytest.raises(PumpSwapAccountError, match="discriminator") as raised:
        parse_pool_account(raw, PUMP_AMM_PROGRAM_ID, address=KNOWN_POOL)
    assert "no pool" not in str(raised.value).lower()


def test_wrong_owner_is_a_parse_error() -> None:
    raw = _encode_pool()
    with pytest.raises(PumpSwapAccountError, match="owner") as raised:
        parse_pool_account(raw, "11111111111111111111111111111111", address=KNOWN_POOL)
    assert "no pool" not in str(raised.value).lower()


def test_account_shorter_than_211_bytes_is_a_parse_error() -> None:
    with pytest.raises(PumpSwapAccountError, match="211"):
        parse_pool_account(_encode_pool(length=210), PUMP_AMM_PROGRAM_ID, address=KNOWN_POOL)


def test_full_pool_account_reads_every_field() -> None:
    virtual = -5
    raw = _encode_pool(
        bump=254,
        index=0,
        is_mayhem_mode=True,
        is_cashback_coin=True,
        virtual_quote_reserves=virtual,
        creator_fee_bps=25,
        can_edit_creator_fee=True,
        is_holder_reward=True,
    ) + bytes(10)
    assert len(raw) >= 271
    pool = parse_pool_account(raw, PUMP_AMM_PROGRAM_ID, address=KNOWN_POOL)
    assert pool.address == KNOWN_POOL
    assert pool.pool_bump == 254
    assert pool.index == 0
    assert pool.creator == KNOWN_AUTHORITY
    assert pool.base_mint == KNOWN_MINT
    assert pool.quote_mint == WSOL_MINT
    assert pool.lp_mint == LP_MINT
    assert pool.pool_base_token_account == BASE_VAULT
    assert pool.pool_quote_token_account == QUOTE_VAULT
    assert pool.lp_supply == 4_194_352_106_721
    assert pool.coin_creator == COIN_CREATOR
    assert pool.is_mayhem_mode is True
    assert pool.is_cashback_coin is True
    assert pool.virtual_quote_reserves == virtual
    assert pool.creator_fee_bps == 25
    assert pool.can_edit_creator_fee is True
    assert pool.is_holder_reward is True


async def test_missing_account_is_no_pool_like_an_empty_raydium_list() -> None:
    helius = QueueHelius([{"value": None}, {"value": None}])
    pools = PumpSwapPools(helius)
    assert await pools.canonical_pool(KNOWN_MINT) is None
    method, params = helius.calls[0]
    assert method == "getAccountInfo"
    assert params == [KNOWN_POOL, {"encoding": "base64"}]

    facts = HeliusTokenSecurity(helius, EmptyRaydium(), pools)  # type: ignore[arg-type]
    book = await facts.lp_book(KNOWN_MINT)
    assert book.lp_mint is None
    assert book.supply_raw is None
    assert book.rows == []
    assert book.liquidity_usd is None
    assert book.price_usd is None


async def test_lp_book_uses_mint_supply_and_not_pool_lp_supply() -> None:
    pool_lp_supply = 999_999
    mint_supply = "1000"
    helius = QueueHelius(
        [
            _account(_encode_pool(lp_supply=pool_lp_supply, virtual_quote_reserves=7)),
            _parsed_mint(mint_supply),
            {"value": [{"address": "burn-ata", "amount": "1000"}]},
            {"value": [{"data": {"parsed": {"info": {"owner": "1nc1nerator11111111111111111111111111111111"}}}}]},
            {"value": [{"owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"}]},
        ]
    )
    facts = HeliusTokenSecurity(helius, EmptyRaydium(), PumpSwapPools(helius))  # type: ignore[arg-type]
    book = await facts.lp_book(KNOWN_MINT)
    assert book.lp_mint == LP_MINT
    assert book.supply_raw == 1000
    assert book.supply_raw != pool_lp_supply
    assert book.price_usd is None
    assert book.rows[0].is_burn_address is True


async def test_bad_pool_account_is_not_reported_as_no_pool() -> None:
    helius = QueueHelius(
        [_account(_encode_pool(discriminator=bytes(8)), owner="11111111111111111111111111111111")]
    )
    facts = HeliusTokenSecurity(helius, EmptyRaydium(), PumpSwapPools(helius))  # type: ignore[arg-type]
    with pytest.raises(PumpSwapAccountError, match="owner") as raised:
        await facts.lp_book(KNOWN_MINT)
    assert "no pool" not in str(raised.value).lower()


async def test_quote_reserve_adds_virtual_quote_reserves() -> None:
    pool = parse_pool_account(
        _encode_pool(virtual_quote_reserves=-5),
        PUMP_AMM_PROGRAM_ID,
        address=KNOWN_POOL,
    )
    helius = QueueHelius(
        [
            {"value": {"amount": "100", "decimals": 6}},
            {"value": {"amount": "40", "decimals": 9}},
        ]
    )
    reserves = await PumpSwapPools(helius).reserves(pool)
    assert reserves.base_raw == 100
    assert reserves.quote_raw == 35
    assert [call[0] for call in helius.calls] == ["getTokenAccountBalance", "getTokenAccountBalance"]
    assert helius.calls[0][1] == [BASE_VAULT]
    assert helius.calls[1][1] == [QUOTE_VAULT]
