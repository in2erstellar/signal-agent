"""The 12-line checklist: line statuses, the signal rule, the card, and fusion."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.dexscreener import DexQuote, DexScreenerPrices
from app.adapters.token_metadata import TokenMetadataClient, TokenMetadataError, ipfs_path
from app.adapters.token_security import MintView, _mint_view
from app.config import QUALITY_MAX_LAUNCH_TRADES, QUALITY_MIN_PASSED, QUALITY_TREND_TICKER_MIN
from app.db.models import Signal, Token
from app.services.quality_checks import (
    FAIL,
    PASS,
    UNAVAILABLE,
    ExternalReadings,
    best_possible,
    build_checklist,
    short_line,
    verdict,
)
from app.services.quality_inputs import launch_trades
from app.services.track_record import horizon_stats
from app.templates import alert_text, checks_explained_text, creator_context
from app.workers.signal_fusion import SignalFusion
from tests.factories import make_token
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

NOW = datetime(2026, 10, 16, 12, 0, tzinfo=timezone.utc)
MINT = "Mint1111111111111111111111111111111111111"
MEASURED = {"distribution_status": "measured", "distribution_share": "0.05"}


def _dex(**overrides: object) -> DexQuote:
    values: dict[str, object] = {
        "pair_address": "pair",
        "dex_id": "pumpswap",
        "price_usd": Decimal("0.0001"),
        "liquidity_usd": Decimal("25000"),
        "buys_5m": 40,
        "sells_5m": 20,
        "social_links": (),
    }
    values.update(overrides)
    return DexQuote(**values)  # type: ignore[arg-type]


def _readings(**overrides: object) -> ExternalReadings:
    values: dict[str, object] = {
        "dex": _dex(),
        "mint_view": MintView(None, None, 1000),
        "launch_trades": 1,
        "metadata_links": {"twitter": "https://x.com/example"},
    }
    values.update(overrides)
    return ExternalReadings(**values)  # type: ignore[arg-type]


def _checks(
    *,
    flags: list[str] | None = None,
    context: dict[str, object] | None = None,
    clones: int | None = 0,
    readings: ExternalReadings | None = None,
):
    return build_checklist(
        risk_flags=flags or [],
        creator_context=MEASURED if context is None else context,
        clone_count=clones,
        symbol="MOO",
        readings=_readings() if readings is None else readings,
    )


def _status(checks, key: str) -> str:
    return next(check.status for check in checks if check.key == key)


def test_clean_token_passes_all_twelve() -> None:
    checks = _checks()
    assert len(checks) == 12
    assert [check.group for check in checks].count("contract") == 4
    result = verdict(checks)
    assert (result.passed, result.total, result.required_failed) == (12, 12, ())
    assert result.emits
    assert result.score == Decimal("100.00")


def test_a_failed_required_line_blocks_even_with_eleven_passes() -> None:
    result = verdict(_checks(flags=["check_mint_authority"]))
    assert result.passed == 11
    assert result.required_failed == ("mint_authority",)
    assert not result.emits


def test_minimum_passed_is_inclusive() -> None:
    three_fails = _checks(
        flags=["check_holder_concentration"],
        clones=2,
        readings=_readings(launch_trades=QUALITY_MAX_LAUNCH_TRADES + 5),
    )
    assert verdict(three_fails).passed == QUALITY_MIN_PASSED
    assert verdict(three_fails).emits
    assert verdict(three_fails).score == Decimal("75.00")
    four_fails = _checks(
        flags=["check_holder_concentration", "check_funder_cluster_exposure"],
        clones=2,
        readings=_readings(launch_trades=QUALITY_MAX_LAUNCH_TRADES + 5),
    )
    assert not verdict(four_fails).emits


def test_unavailable_is_never_a_pass() -> None:
    checks = _checks(
        flags=["check_holder_concentration:unavailable"],
        context={"distribution_status": "unavailable", "distribution_reason": "x"},
        readings=ExternalReadings(),
    )
    assert _status(checks, "holder_concentration") == UNAVAILABLE
    assert _status(checks, "creator_share") == UNAVAILABLE
    assert _status(checks, "liquidity") == UNAVAILABLE
    result = verdict(checks)
    assert result.required_failed == ()
    assert not result.emits


def test_creator_share_is_not_required() -> None:
    no_data = {"distribution_status": "unavailable", "distribution_reason": "x"}
    result = verdict(_checks(context=no_data))
    assert result.required_failed == ()
    assert result.passed == 11
    assert result.emits


def test_best_possible_counts_only_external_lines_still_missing() -> None:
    stored_only = _checks(
        readings=ExternalReadings(), flags=["check_holder_concentration:unavailable"]
    )
    # 6 stored passes (mint, freeze, lp, funder, creator, clone) + 5 external.
    assert best_possible(stored_only) == 11


def test_creator_share_limit_is_inclusive() -> None:
    at_limit = {"distribution_status": "measured", "distribution_share": "0.30"}
    over = {"distribution_status": "measured", "distribution_share": "0.31"}
    assert _status(_checks(context=at_limit), "creator_share") == PASS
    assert _status(_checks(context=over), "creator_share") == FAIL


def test_market_lines() -> None:
    thin = _checks(readings=_readings(dex=_dex(liquidity_usd=Decimal("9999"))))
    assert _status(thin, "liquidity") == FAIL
    even = _checks(readings=_readings(dex=_dex(buys_5m=10, sells_5m=10)))
    assert _status(even, "buy_pressure") == FAIL
    clones = _checks(clones=3)
    clone_line = next(check for check in clones if check.key == "not_a_clone")
    assert clone_line.status == FAIL
    assert "3 other tokens named MOO" in clone_line.text


def test_a_trend_ticker_is_not_a_clone() -> None:
    below = next(c for c in _checks(clones=QUALITY_TREND_TICKER_MIN - 1) if c.key == "not_a_clone")
    assert below.status == FAIL
    trend = next(c for c in _checks(clones=QUALITY_TREND_TICKER_MIN) if c.key == "not_a_clone")
    assert trend.status == PASS
    assert trend.text == f"Trend ticker: {QUALITY_TREND_TICKER_MIN} other tokens named MOO in 24h"
    assert short_line(trend.as_dict()) == f"Trend ticker: {QUALITY_TREND_TICKER_MIN}"


def test_socials_none_listed_is_a_fail_and_no_metadata_is_unavailable() -> None:
    assert _status(_checks(readings=_readings(metadata_links={})), "socials") == FAIL
    assert _status(_checks(readings=_readings(metadata_links=None)), "socials") == UNAVAILABLE
    profile = _readings(metadata_links=None, dex=_dex(social_links=("https://t.me/x",)))
    assert _status(_checks(readings=profile), "socials") == PASS


def test_risky_token_extensions() -> None:
    def status(extensions):
        view = MintView(None, None, 1, extensions=tuple(extensions))
        return _status(_checks(readings=_readings(mint_view=view)), "token_extensions")

    assert status([("metadataPointer", {}), ("tokenMetadata", {"uri": "x"})]) == PASS
    zero_fee = {"newerTransferFee": {"transferFeeBasisPoints": 0}}
    assert status([("transferFeeConfig", zero_fee)]) == PASS
    fee = {"newerTransferFee": {"transferFeeBasisPoints": 100}}
    assert status([("transferFeeConfig", fee)]) == FAIL
    assert status([("permanentDelegate", {"delegate": "Someone"})]) == FAIL
    assert status([("transferHook", {"programId": None, "authority": None})]) == PASS


def test_mint_view_reads_token_2022_metadata_uri() -> None:
    account = {
        "data": {
            "parsed": {
                "info": {
                    "supply": "10",
                    "mintAuthority": None,
                    "freezeAuthority": None,
                    "extensions": [
                        {"extension": "metadataPointer", "state": {"authority": None}},
                        {
                            "extension": "tokenMetadata",
                            "state": {"uri": "https://ipfs.io/ipfs/Qm1"},
                        },
                    ],
                }
            }
        }
    }
    view = _mint_view(account)
    assert view.metadata_uri == "https://ipfs.io/ipfs/Qm1"
    assert [name for name, _state in view.extensions] == ["metadataPointer", "tokenMetadata"]


def test_launch_trades_counts_the_creation_window_without_the_create() -> None:
    rows = [
        {"slot": 100, "err": None},
        {"slot": 100, "err": None},
        {"slot": 101, "err": None},
        {"slot": 101, "err": {"InstructionError": [0, "x"]}},
        {"slot": 102, "err": None},
    ]
    assert launch_trades(rows) == 2
    assert launch_trades([]) is None


CID = "QmPdaSxGscWiAzT5RSEw5Q8vegbRvepT3ervmrefGVjk88"


def test_only_ipfs_uris_are_fetched() -> None:
    assert ipfs_path(f"https://ipfs.io/ipfs/{CID}") == CID
    assert ipfs_path(f"ipfs://{CID}") == CID
    assert ipfs_path("https://evil.example/meta.json") is None
    assert ipfs_path("http://169.254.169.254/latest") is None


async def test_metadata_links_come_through_the_configured_gateway() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(
            200, json={"twitter": "https://x.com/a", "telegram": " ", "website": ""}
        )

    client = TokenMetadataClient(
        "https://gw.test/ipfs", client=httpx.AsyncClient(transport=httpx.MockTransport(handler))
    )
    socials = await client.socials(f"https://ipfs.io/ipfs/{CID}")
    assert seen == [f"https://gw.test/ipfs/{CID}"]
    assert socials is not None and socials.links == {"twitter": "https://x.com/a"}
    assert await client.socials("https://evil.example/meta.json") is None

    failing = TokenMetadataClient(
        "https://gw.test/ipfs",
        client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda request: httpx.Response(403, text="challenge"))
        ),
    )
    with pytest.raises(TokenMetadataError):
        await failing.socials(f"ipfs://{CID}")


async def test_dexscreener_reads_five_minute_trades_and_profile_links() -> None:
    pair = {
        "dexId": "pumpswap",
        "pairAddress": "p",
        "baseToken": {"address": MINT},
        "priceUsd": "0.1",
        "liquidity": {"usd": 100},
        "txns": {"m5": {"buys": 7, "sells": 3}},
        "info": {
            "websites": [{"url": "https://a.example"}],
            "socials": [{"url": "https://x.com/a"}],
        },
    }
    client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(200, json=[pair]))
    )
    quote = await DexScreenerPrices("https://dex.test", client=client).token_quote(MINT)
    assert quote is not None
    assert (quote.buys_5m, quote.sells_5m) == (7, 3)
    assert quote.social_links == ("https://a.example", "https://x.com/a")


def test_track_record_counts_collapses_and_doubles() -> None:
    stats = horizon_stats("1h", [Decimal("0.05"), Decimal("0.5"), Decimal("3")])
    assert (stats.measured, stats.collapsed, stats.doubled) == (3, 1, 1)
    assert stats.median_change == Decimal("-0.5")
    assert horizon_stats("7d", []).median_change is None


def test_card_shows_every_line_with_its_icon_and_the_rule() -> None:
    checks = _checks(
        flags=["check_holder_concentration"],
        readings=_readings(launch_trades=None),
    )
    context = {
        **MEASURED,
        "quality_checks": [check.as_dict() for check in checks],
        "quality_passed": 10,
        "quality_total": 12,
    }
    text = alert_text(
        symbol="MOO<",
        mint=MINT,
        composite_score=Decimal("83.33"),
        risk_score=Decimal("85"),
        risk_flags=["check_holder_concentration"],
        bait_warning=False,
        context=context,
    )
    assert "<b>MOO&lt;</b> — <b>10/12</b> checks passed" in text
    box = text.split("<pre>")[1].split("</pre>")[0]
    assert box.count(" ✓ ") == 10
    assert box.count(" ✗ ") == 1
    assert " ? Launch trades: no data" in box
    assert max(len(line) for line in box.replace("&quot;", '"').split("\n")) <= 32
    for title in ("Contract", "Holders", "Market"):
        assert f"\n{title} " in box
    assert "Signal score" not in text
    assert "It does not predict price" in text
    assert creator_context(context) == MEASURED


def test_explainer_names_every_required_line() -> None:
    text = checks_explained_text()
    assert text.count("Required.") == 3
    assert "Confirmed signal" in text
    assert f"at least {QUALITY_MIN_PASSED} of 12" in text


class FakeReader:
    def __init__(self, readings: ExternalReadings) -> None:
        self.readings = readings
        self.calls = 0

    async def read(self, token: Token) -> ExternalReadings:
        self.calls += 1
        return self.readings


def _token(mint: str, **overrides: object) -> Token:
    values: dict[str, object] = {
        "mint": mint,
        "symbol": "MOO",
        "risk_score": Decimal("100"),
        "risk_flags": [],
        "creator_address": "creator-known",
        "dev_distribution_status": "measured",
        "dev_distribution_share": Decimal("0.1"),
        "dev_distribution_reason": None,
        "updated_at": NOW,
        "first_seen_at": NOW,
    }
    values.update(overrides)
    return make_token(**values)


async def _fuse(pipeline_infra: object, tokens: list[Token], reader: FakeReader) -> list[Signal]:
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    try:
        async with sessions() as session:
            async with session.begin():
                session.add_all(tokens)
        fusion = SignalFusion(sessions, clock=lambda: NOW, quality=reader)  # type: ignore[arg-type]
        await fusion.run_once()
        async with sessions() as session:
            return list(await session.scalars(select(Signal)))
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_fusion_stores_the_checklist_on_the_signal(pipeline_infra: object) -> None:
    reader = FakeReader(_readings())
    signals = await _fuse(pipeline_infra, [_token("mint-clean")], reader)
    assert reader.calls == 1
    assert len(signals) == 1
    signal = signals[0]
    assert signal.composite_score == Decimal("100.00")
    assert signal.context is not None
    assert signal.context["quality_passed"] == 12
    assert signal.context["market"]["liquidity_usd"] == "25000"
    assert signal.context["market"]["buys_5m"] == 40
    assert len(signal.context["quality_checks"]) == 12


async def test_fusion_skips_external_reads_when_a_required_line_failed(
    pipeline_infra: object,
) -> None:
    reader = FakeReader(_readings())
    signals = await _fuse(
        pipeline_infra,
        [_token("mint-lp-open", risk_flags=["check_lp_lock"])],
        reader,
    )
    assert signals == []
    assert reader.calls == 0


async def test_fusion_counts_clones_by_ticker(pipeline_infra: object) -> None:
    reader = FakeReader(_readings(launch_trades=QUALITY_MAX_LAUNCH_TRADES + 1, metadata_links={}))
    clones = [_token(f"mint-clone-{n}", symbol="moo ", risk_score=None) for n in range(2)]
    signals = await _fuse(pipeline_infra, [_token("mint-original"), *clones], reader)
    # Launch trades, socials, and clone fail: 9 of 12 still signals.
    assert len(signals) == 1
    lines = {row["key"]: row for row in signals[0].context["quality_checks"]}  # type: ignore[index]
    assert lines["not_a_clone"]["status"] == FAIL
    assert "2 other tokens" in lines["not_a_clone"]["text"]
