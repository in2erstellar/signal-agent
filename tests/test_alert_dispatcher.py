"""Subscriber filters and the per-chat send interval.

Postgres and the delivery table are real. Telegram itself is a fake sender.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from telegram.error import Forbidden, NetworkError

from app.adapters.telegram_bot import PerChatRateLimiter
from app.db.models import AlertDelivery, Signal, User
from app.services.subscriber_settings import AlertPrefs, accepts_alert, parse_settings
from app.templates import BAIT_WARNING, DISCLAIMER
from app.workers.alert_dispatcher import AlertDispatcher
from tests.factories import make_bait_flag, make_signal, make_token, make_tracked_wallet, make_user
from tests.pipeline_containers import truncate_pipeline

pytest_plugins = ["tests.pipeline_containers"]

T0 = datetime(2026, 9, 24, 18, 0, tzinfo=timezone.utc)
MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


class RecordingSender:
    def __init__(self, blocked: set[int] | None = None, fail_once: set[int] | None = None) -> None:
        self.sent: list[tuple[int, str]] = []
        self.blocked = blocked or set()
        self.fail_once = set(fail_once or set())

    async def send(self, chat_id: int, text: str) -> None:
        if chat_id in self.blocked:
            raise Forbidden("Forbidden: bot was blocked by the user")
        if chat_id in self.fail_once:
            self.fail_once.remove(chat_id)
            raise NetworkError("temporary")
        self.sent.append((chat_id, text))


@pytest.fixture
async def sessions(pipeline_infra: object):
    engine = create_async_engine(pipeline_infra.database_url)  # type: ignore[attr-defined]
    factory = async_sessionmaker(engine, expire_on_commit=False)
    await truncate_pipeline(engine)
    yield factory
    await truncate_pipeline(engine)
    await engine.dispose()


def test_filter_includes_the_exact_bounds_and_rejects_past_them() -> None:
    composite = Decimal("72.5")
    risk = Decimal("85")
    exact = AlertPrefs(min_score=composite, max_risk=Decimal("15"), active=True)
    assert accepts_alert(exact, composite_score=composite, risk_score=risk) is True
    higher_floor = AlertPrefs(min_score=Decimal("72.6"), max_risk=Decimal("15"), active=True)
    assert accepts_alert(higher_floor, composite_score=composite, risk_score=risk) is False
    tighter_risk = AlertPrefs(min_score=composite, max_risk=Decimal("14.9"), active=True)
    assert accepts_alert(tighter_risk, composite_score=composite, risk_score=risk) is False
    inactive = AlertPrefs(min_score=Decimal("0"), max_risk=Decimal("100"), active=False)
    assert accepts_alert(inactive, composite_score=composite, risk_score=risk) is False
    unknown_risk = AlertPrefs(min_score=Decimal("0"), max_risk=Decimal("99"), active=True)
    assert accepts_alert(unknown_risk, composite_score=composite, risk_score=None) is False


async def test_per_chat_limiter_sleeps_only_for_the_same_chat() -> None:
    sleeps: list[float] = []
    clock = {"t": 0.0}

    def now() -> float:
        return clock["t"]

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock["t"] += delay

    limiter = PerChatRateLimiter(interval_seconds=1, sleep=sleep, clock=now)
    await limiter.wait(10)
    await limiter.wait(11)
    await limiter.wait(10)
    assert sleeps == [1.0]


async def test_dispatcher_sends_only_to_users_inside_both_filters(sessions: object) -> None:
    await _seed(
        sessions,
        users=[
            (101, {"min_score": "50", "max_risk": "20", "active": True}),
            (102, {"min_score": "80", "max_risk": "100", "active": True}),
            (103, {"min_score": "50", "max_risk": "10", "active": True}),
            (104, {"min_score": "0", "max_risk": "100", "active": False}),
        ],
        bait=True,
    )
    sender = RecordingSender()
    dispatcher = AlertDispatcher(sessions, sender, clock=lambda: T0)  # type: ignore[arg-type]
    processed, failed, sent = await dispatcher.run_once()
    assert (processed, failed, sent) == (1, 0, 1)
    assert [chat for chat, _ in sender.sent] == [101]
    text = sender.sent[0][1]
    assert "<b>72.5</b>" in text
    assert f"https://solscan.io/token/{MINT}" in text
    assert "check_funder_cluster_exposure" in text
    assert BAIT_WARNING in text
    assert text.endswith(DISCLAIMER)
    assert "купить" not in text.lower()

    async with sessions() as session:  # type: ignore[operator]
        signal = await session.scalar(select(Signal))
        deliveries = await session.scalar(select(func.count()).select_from(AlertDelivery))
    assert signal is not None and signal.notified is True
    assert deliveries == 1

    again = await dispatcher.run_once()
    assert again == (0, 0, 0)
    assert len(sender.sent) == 1


async def test_owner_filter_skips_other_telegram_accounts(sessions: object) -> None:
    await _seed(
        sessions,
        users=[
            (401, {"min_score": "50", "max_risk": "100", "active": True}),
            (402, {"min_score": "50", "max_risk": "100", "active": True}),
        ],
        bait=False,
    )
    sender = RecordingSender()
    dispatcher = AlertDispatcher(
        sessions,  # type: ignore[arg-type]
        sender,
        clock=lambda: T0,
        owners=frozenset({401}),
    )
    processed, failed, sent = await dispatcher.run_once()
    assert (processed, failed, sent) == (1, 0, 1)
    assert [chat for chat, _ in sender.sent] == [401]


async def test_blocked_user_is_deactivated_and_not_retried(sessions: object) -> None:
    await _seed(
        sessions,
        users=[
            (201, {"min_score": "50", "max_risk": "100", "active": True}),
            (202, {"min_score": "50", "max_risk": "100", "active": True}),
        ],
        bait=False,
    )
    sender = RecordingSender(blocked={202})
    dispatcher = AlertDispatcher(sessions, sender, clock=lambda: T0)  # type: ignore[arg-type]
    processed, failed, sent = await dispatcher.run_once()
    assert (processed, failed, sent) == (1, 0, 1)
    assert [chat for chat, _ in sender.sent] == [201]
    assert BAIT_WARNING not in sender.sent[0][1]

    async with sessions() as session:  # type: ignore[operator]
        blocked = await session.scalar(select(User).where(User.telegram_id == 202))
        signal = await session.scalar(select(Signal))
    assert blocked is not None
    assert parse_settings(blocked.settings).active is False
    assert signal is not None and signal.notified is True

    await dispatcher.run_once()
    assert [chat for chat, _ in sender.sent] == [201]


async def test_transport_error_leaves_the_signal_for_the_next_cycle(sessions: object) -> None:
    await _seed(
        sessions,
        users=[(301, {"min_score": "50", "max_risk": "100", "active": True})],
        bait=False,
    )
    sender = RecordingSender(fail_once={301})
    dispatcher = AlertDispatcher(sessions, sender, clock=lambda: T0)  # type: ignore[arg-type]
    first = await dispatcher.run_once()
    assert first == (1, 1, 0)
    async with sessions() as session:  # type: ignore[operator]
        signal = await session.scalar(select(Signal))
        deliveries = await session.scalar(select(func.count()).select_from(AlertDelivery))
    assert signal is not None and signal.notified is False
    assert deliveries == 0

    second = await dispatcher.run_once()
    assert second == (1, 0, 1)
    assert len(sender.sent) == 1
    async with sessions() as session:  # type: ignore[operator]
        signal = await session.scalar(select(Signal))
        deliveries = await session.scalar(select(func.count()).select_from(AlertDelivery))
    assert signal is not None and signal.notified is True
    assert deliveries == 1


async def _seed(sessions: object, *, users: list[tuple[int, dict]], bait: bool) -> None:
    async with sessions() as session:  # type: ignore[operator]
        async with session.begin():
            token = make_token(
                mint=MINT,
                symbol="BONK",
                risk_score=Decimal("85"),
                risk_flags=["check_funder_cluster_exposure"],
            )
            wallet = make_tracked_wallet()
            session.add_all([token, wallet, *[make_user(telegram_id=tid, settings=settings) for tid, settings in users]])
            await session.flush()
            session.add(
                make_signal(
                    token_id=token.id,
                    composite_score=Decimal("72.5"),
                    risk_score=Decimal("85"),
                    triggered_at=T0,
                    notified=False,
                )
            )
            if bait:
                session.add(make_bait_flag(wallet_id=wallet.id, token_id=token.id))
