"""Public channel: market-cap readings, ASCII charts, result math, and posting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.adapters.dexscreener import DexQuote
from app.adapters.telegram_bot import parse_signal_link, signal_link
from app.db.models import ChannelPost, MarketPoint, Signal, Token
from app.services.ascii_chart import columns, format_usd, render
from app.services.channel_results import horizon_result, verdict_icon
from app.templates import channel_result_text, channel_signal_text
from app.workers.channel_publisher import ChannelPublisher
from app.workers.market_sampler import MarketSampler, is_due, sample_interval
from tests.factories import make_token
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

T0 = datetime(2026, 10, 16, 14, 0, tzinfo=timezone.utc)
MINT = "Mint1111111111111111111111111111111111111"
CHECKLIST = {
    "quality_checks": [{"key": "k", "group": "contract", "status": "pass", "text": "t"}] * 12,
    "quality_passed": 9,
    "quality_total": 12,
}


def test_usd_labels_stay_short() -> None:
    assert format_usd(Decimal("950")) == "$950"
    assert format_usd(Decimal("69200")) == "$69.2k"
    assert format_usd(Decimal("160000")) == "$160k"
    assert format_usd(Decimal("2670000")) == "$2.7M"


def test_chart_starts_at_the_signal_and_fits_a_phone() -> None:
    points = [(0.0, Decimal(20000)), (60.0, Decimal(25000)), (1440.0, Decimal(160000))]
    assert columns(points, span=1440, width=20)[0] == Decimal(20000)
    chart = render(points, span=1440, x_labels=("0h", "12h", "24h"))
    assert chart is not None
    lines = chart.split("\n")
    assert max(len(line) for line in lines) <= 28
    assert lines[0].startswith("  $160k┤")
    assert "$20.0k┤" in lines[4]
    assert lines[-1].strip().startswith("0h")


def test_one_reading_is_not_a_chart() -> None:
    assert render([(0.0, Decimal(1))], span=60, x_labels=("0m", "30m", "60m")) is None


def test_result_needs_a_baseline_and_a_reading_near_the_horizon() -> None:
    points = [(1.0, Decimal(100)), (12.0, Decimal(150)), (55.0, Decimal(6))]
    result = horizon_result(points, minutes=60, tolerance=10)
    assert result is not None
    assert result.base == Decimal(100)
    assert result.end == Decimal(6)
    assert result.change == Decimal("-0.94")
    assert (result.peak, result.peak_minutes) == (Decimal(150), 12.0)
    assert verdict_icon(result.change) == "💀"
    # The first reading came 8 minutes late: no baseline, no result.
    assert (
        horizon_result([(8.0, Decimal(100)), (58.0, Decimal(6))], minutes=60, tolerance=10) is None
    )
    # The last reading is 30 minutes before the horizon: too old to stand for it.
    assert (
        horizon_result([(1.0, Decimal(100)), (30.0, Decimal(6))], minutes=60, tolerance=10) is None
    )


def test_verdict_icons() -> None:
    assert verdict_icon(Decimal("1")) == "🚀"
    assert verdict_icon(Decimal("0.2")) == "📈"
    assert verdict_icon(Decimal("-0.5")) == "📉"
    assert verdict_icon(Decimal("-0.9")) == "💀"


def test_sampling_schedule() -> None:
    assert sample_interval(timedelta(minutes=30)) == timedelta(minutes=5)
    assert sample_interval(timedelta(hours=5)) == timedelta(hours=1)
    assert sample_interval(timedelta(days=3)) == timedelta(hours=6)
    assert sample_interval(timedelta(days=8)) is None
    assert is_due(timedelta(minutes=10), None)
    assert is_due(timedelta(minutes=10), timedelta(minutes=4, seconds=40))
    assert not is_due(timedelta(minutes=10), timedelta(minutes=2))


def test_deep_link_round_trip() -> None:
    signal_id = uuid4()
    link = signal_link("XparseItBot", signal_id)
    assert link == f"https://t.me/XparseItBot?start=sig_{signal_id.hex}"
    assert parse_signal_link([link.split("start=")[1]]) == signal_id
    assert parse_signal_link(["hello"]) is None
    assert parse_signal_link([]) is None


def test_channel_texts() -> None:
    groups = ["contract"] * 4 + ["holders"] * 4 + ["market"] * 4
    statuses = ["pass"] * 4 + ["pass", "fail", "pass", "fail"] + ["pass", "pass", "fail", "pass"]
    checks = [{"group": g, "status": s, "text": "x"} for g, s in zip(groups, statuses, strict=True)]
    first = channel_signal_text(
        symbol="TWMW<",
        mint=MINT,
        checks=checks,
        triggered_at=T0,
        market_cap=Decimal(69200),
        market={"liquidity_usd": "17800", "buys_5m": 66, "sells_5m": 10},
    )
    assert "<b>TWMW&lt;</b> — 9/12 checks passed" in first
    assert "Signal: 14:00 UTC, 16 Oct" in first
    assert "MCap  $69.2k" in first
    assert "Liq   $17.8k" in first
    assert "5m    66 buy/10 sell" in first
    assert "Safety   ▰▰▰▰▰▰▰▰▰▱▱▱ 9/12" in first
    assert "Contract ████ 4/4" in first
    assert "Holders  ██░░ 2/4" in first
    assert "Market   ███░ 3/4" in first
    assert "wins and losses" in first
    pre = first.split("<pre>")[1].split("</pre>")[0]
    assert max(len(line) for line in pre.replace("&quot;", '"').split("\n")) <= 32
    no_market = channel_signal_text(
        symbol="X", mint=MINT, checks=checks, triggered_at=T0, market_cap=None
    )
    assert "MCap  —" in no_market
    result = horizon_result(
        [(0.0, Decimal(69200)), (30.0, Decimal(20000)), (60.0, Decimal(4100))],
        minutes=60,
        tolerance=10,
    )
    assert result is not None
    text = channel_result_text(symbol="TWMW", mint=MINT, horizon="1h", result=result)
    assert "💀 <b>TWMW</b> · 1h after signal: <b>-94%</b>" in text
    assert "$69.2k → $4.1k" in text
    assert "Never above start" in text
    assert text.count("<pre>") == 2


def test_a_token_keeps_its_mascot_and_only_the_mood_changes() -> None:
    from app.services.mascots import DEAD, DOWN, MASCOTS, UP, beside, mascot_for, mood_for

    assert mascot_for(MINT) == mascot_for(MINT)
    assert len({mascot_for(f"mint-{n}") for n in range(200)}) == len(MASCOTS)
    assert mood_for(Decimal("0.5")) == UP
    assert mood_for(Decimal("-0.3")) == DOWN
    assert mood_for(Decimal("-0.9")) == DEAD
    frame = MASCOTS[mascot_for(MINT)][DEAD]
    block = beside(MINT, DEAD, ["a", "b"]).split("\n")
    assert len(block) == 3
    assert block[0].startswith(frame[0].rstrip()) and block[0].endswith("a")


def _quote(cap: str | None) -> DexQuote:
    return DexQuote(
        "pair",
        "pumpswap",
        Decimal("0.1"),
        Decimal("100"),
        market_cap_usd=None if cap is None else Decimal(cap),
    )


class FakeQuotes:
    def __init__(self, caps: dict[str, str | None]) -> None:
        self.caps = caps
        self.asked: list[list[str]] = []

    async def token_quotes(self, mints: list[str]) -> dict[str, DexQuote]:
        self.asked.append(mints)
        return {mint: _quote(cap) for mint, cap in self.caps.items() if mint in mints}


class FakeChannel:
    def __init__(self, *, fail: bool = False) -> None:
        self.posts: list[tuple[str, int | None, tuple[str, str] | None]] = []
        self.fail = fail

    async def post(self, text: str, *, reply_to=None, link=None) -> int:
        if self.fail:
            raise RuntimeError("telegram down")
        self.posts.append((text, reply_to, link))
        return 1000 + len(self.posts)


async def _setup(pipeline_infra: object, tokens_and_signals: list[tuple[Token, Signal]]):
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    async with sessions() as session:
        async with session.begin():
            for token, signal in tokens_and_signals:
                session.add(token)
                await session.flush()
                signal.token_id = token.id
                session.add(signal)
    return engine, sessions


def _signal(**overrides: object) -> Signal:
    values: dict[str, object] = {
        "composite_score": Decimal("75"),
        "risk_score": Decimal("85"),
        "triggered_at": T0,
        "notified": True,
        "context": dict(CHECKLIST),
    }
    values.update(overrides)
    return Signal(**values)  # type: ignore[arg-type]


async def test_sampler_stores_only_readings_with_a_market_cap(pipeline_infra: object) -> None:
    pairs = [
        (make_token(mint="mint-a", symbol="A"), _signal()),
        (make_token(mint="mint-b", symbol="B"), _signal()),
    ]
    engine, sessions = await _setup(pipeline_infra, pairs)
    try:
        quotes = FakeQuotes({"mint-a": "50000", "mint-b": None})
        sampler = MarketSampler(sessions, quotes, clock=lambda: T0 + timedelta(minutes=1))  # type: ignore[arg-type]
        assert await sampler.run_once() == 1
        assert quotes.asked == [["mint-a", "mint-b"]]
        # Two minutes later neither is due yet (5-minute step in the first hour).
        later = MarketSampler(sessions, quotes, clock=lambda: T0 + timedelta(minutes=3))  # type: ignore[arg-type]
        assert await later.run_once() == 0
        async with sessions() as session:
            caps = list(await session.scalars(select(MarketPoint.market_cap_usd)))
        assert caps == [Decimal("50000")]
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_publisher_posts_once_then_replies_with_the_result(pipeline_infra: object) -> None:
    token = make_token(mint=MINT, symbol="TWMW")
    signal = _signal()
    engine, sessions = await _setup(pipeline_infra, [(token, signal)])
    channel = FakeChannel()
    try:
        async with sessions() as session:
            async with session.begin():
                for minutes, cap in ((1, 69200), (30, 20000), (59, 4100)):
                    session.add(
                        MarketPoint(
                            token_id=token.id,
                            observed_at=T0 + timedelta(minutes=minutes),
                            market_cap_usd=Decimal(cap),
                        )
                    )

        def publisher(minutes: float) -> ChannelPublisher:
            return ChannelPublisher(
                sessions,
                channel,
                card_link=lambda s: f"https://t.me/bot?start=sig_{s.id.hex}",
                clock=lambda: T0 + timedelta(minutes=minutes),
            )

        assert await publisher(2).run_once() == 1
        assert await publisher(3).run_once() == 0
        text, reply_to, link = channel.posts[0]
        assert "MCap  $69.2k" in text
        assert reply_to is None
        assert link is not None and link[1].endswith(signal.id.hex)

        assert await publisher(61).run_once() == 1
        result_text, result_reply_to, _ = channel.posts[1]
        assert result_reply_to == 1001
        assert "1h after signal: <b>-94%</b>" in result_text
        assert await publisher(62).run_once() == 0

        # 24h has no reading: once the grace period passes it is recorded as skipped.
        assert await publisher(24 * 60 + 60).run_once() == 0
        assert await publisher(24 * 60 + 200).run_once() == 0
        async with sessions() as session:
            rows = {row.kind: row.status for row in await session.scalars(select(ChannelPost))}
        assert rows == {"signal": "posted", "1h": "posted", "24h": "skipped"}
        assert len(channel.posts) == 2
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()


async def test_publisher_skips_old_style_signals_and_retries_failed_sends(
    pipeline_infra: object,
) -> None:
    pairs = [
        (make_token(mint="mint-old", symbol="OLD"), _signal(context={"creator": "x"})),
        (make_token(mint="mint-new", symbol="NEW"), _signal()),
    ]
    engine, sessions = await _setup(pipeline_infra, pairs)
    try:
        down = FakeChannel(fail=True)
        failing = ChannelPublisher(
            sessions, down, card_link=lambda s: None, clock=lambda: T0 + timedelta(minutes=3)
        )
        assert await failing.run_once() == 0
        up = FakeChannel()
        working = ChannelPublisher(
            sessions, up, card_link=lambda s: None, clock=lambda: T0 + timedelta(minutes=4)
        )
        assert await working.run_once() == 1
        assert len(up.posts) == 1
        assert "NEW" in up.posts[0][0]
        # No market-cap reading within 5 minutes, so the post has no MCap.
        assert "MCap  —" in up.posts[0][0]
    finally:
        await truncate_pipeline(engine)
        await engine.dispose()
