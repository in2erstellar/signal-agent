"""Consumer for ``ai_review_queue``.

``generate_token_review`` is the only model call. The provider is selected
in config. A retryable error leaves the message unacked and keeps the
budget reservation. A permanent error refunds that reservation.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, Protocol

import structlog
from redis.asyncio import Redis
from redis.exceptions import ResponseError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.adapters.ai_review import AiReviewError, TokenReview, TokenReviewInput
from app.config import (
    AI_REVIEW_DAILY_BUDGET,
    AI_REVIEW_DAILY_TOKEN_BUDGET,
    AI_REVIEW_GROUP,
    AI_REVIEW_QUEUE,
)
from app.db.models import Signal, Token
from app.observability import bind_trace, new_trace_id
from app.services.ai_budget import (
    raise_call_floor,
    raise_token_floor,
    reconcile_tokens,
    release_review,
)
from app.services.token_store import (
    AI_REVIEW_COMPLETED,
    AI_REVIEW_FAILED,
    AI_REVIEW_SKIPPED_BUDGET,
    AI_REVIEW_TRUNCATED,
)

logger = structlog.get_logger(__name__)

Clock = Callable[[], datetime]


class ReviewClient(Protocol):
    async def generate_token_review(self, token: TokenReviewInput) -> TokenReview: ...


class AiReviewWorker:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        redis: Redis,
        client: ReviewClient,
        *,
        stop: Any | None = None,
        clock: Clock | None = None,
        consumer_name: str = "ai-review",
    ) -> None:
        self._sessions = sessions
        self._redis = redis
        self._client = client
        self._stop = stop
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._consumer = consumer_name
        self._group_ready = False

    async def run_once(self, *, block_ms: int | None = None) -> tuple[int, int]:
        await self._ensure_group()
        messages = await self._read("0", block_ms=None)
        messages.extend(await self._read(">", block_ms=block_ms))
        processed = 0
        failed = 0
        for message_id, fields in messages:
            if self._stopping():
                break
            mint = fields.get("mint") or ""
            trace_id = fields.get("trace_id") or new_trace_id()
            try:
                if not mint:
                    await self._ack(message_id)
                    failed += 1
                    continue
                with bind_trace(trace_id):
                    done = await self._review(mint, fields)
                if done:
                    await self._ack(message_id)
                    processed += 1
                else:
                    failed += 1
            except Exception as exc:
                failed += 1
                with bind_trace(trace_id):
                    logger.warning(
                        "ai_review_failed",
                        mint=mint,
                        message_id=message_id,
                        error_type=type(exc).__name__,
                    )
        logger.info("ai_review_cycle", processed=processed, failed=failed)
        return processed, failed

    async def _review(self, mint: str, fields: dict[str, str]) -> bool:
        """Return True when the message should be acked."""
        async with self._sessions() as session:
            token = await session.scalar(select(Token).where(Token.mint == mint))
            if token is None:
                await self._refund(fields)
                logger.info("ai_review_missing_token", mint=mint)
                return True
            if token.ai_review_status == AI_REVIEW_COMPLETED and token.ai_review:
                logger.info("ai_review_already_stored", mint=mint)
                return True
            if token.ai_review_status == AI_REVIEW_SKIPPED_BUDGET:
                logger.info("ai_review_already_skipped_budget", mint=mint)
                return True
            if token.ai_review_status == AI_REVIEW_TRUNCATED:
                logger.info("ai_review_already_truncated", mint=mint)
                return True
            signal = await session.scalar(select(Signal).where(Signal.token_id == token.id))
            flags = token.risk_flags if isinstance(token.risk_flags, list) else []
            review_input = TokenReviewInput(
                mint=token.mint,
                symbol=token.symbol,
                name=token.name,
                risk_score=None if token.risk_score is None else str(token.risk_score),
                risk_flags=tuple(str(flag) for flag in flags),
                liquidity_usd=None if token.liquidity_usd is None else str(token.liquidity_usd),
                composite_score=None if signal is None else str(signal.composite_score),
            )
        try:
            review = await self._client.generate_token_review(review_input)
        except AiReviewError as exc:
            if exc.budget_exceeded:
                await self._mark_budget_exceeded(mint, fields, exc)
                return True
            if exc.retryable:
                logger.warning(
                    "ai_review_retry",
                    mint=mint,
                    status_code=exc.status_code,
                )
                return False
            await self._mark_failed(mint, fields)
            return True
        if review.finish_reason == "length":
            await self._mark_truncated(mint, fields, review.total_tokens)
            return True
        await self._store(mint, review.text)
        await self._reconcile(fields, review.total_tokens)
        return True

    async def _store(self, mint: str, text: str) -> None:
        now = self._clock()
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is None:
                    return
                token.ai_review = text
                token.ai_review_status = AI_REVIEW_COMPLETED
                token.ai_review_at = now
        logger.info("ai_review_completed", mint=mint)

    async def _reconcile(self, fields: dict[str, str], actual: int | None) -> None:
        if actual is None:
            return
        moment = _reserved_at(fields)
        if moment is None:
            return
        reserved = _reserved_tokens(fields)
        await reconcile_tokens(self._redis, moment, reserved, actual)

    async def _mark_budget_exceeded(self, mint: str, fields: dict[str, str], exc: AiReviewError) -> None:
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is not None:
                    token.ai_review_status = AI_REVIEW_SKIPPED_BUDGET
                    token.ai_review_at = self._clock()
        await self._refund(fields)
        moment = _reserved_at(fields) or self._clock()
        if exc.limit_kind == "tpd":
            floor = exc.quota_used if exc.quota_used is not None else AI_REVIEW_DAILY_TOKEN_BUDGET
            await raise_token_floor(self._redis, moment, floor)
        elif exc.limit_kind == "rpd":
            floor = exc.quota_used if exc.quota_used is not None else AI_REVIEW_DAILY_BUDGET
            await raise_call_floor(self._redis, moment, floor)
        logger.warning(
            "ai_review_skipped_budget",
            mint=mint,
            limit_kind=exc.limit_kind,
            quota_limit=exc.quota_limit,
            quota_used=exc.quota_used,
            quota_remaining=exc.quota_remaining,
        )

    async def _mark_truncated(self, mint: str, fields: dict[str, str], actual: int | None) -> None:
        """A cut-off reply is not a card. The spent call and tokens stay reserved."""
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is not None:
                    token.ai_review = None
                    token.ai_review_status = AI_REVIEW_TRUNCATED
                    token.ai_review_at = self._clock()
        await self._reconcile(fields, actual)
        logger.warning("ai_review_truncated", mint=mint, total_tokens=actual)

    async def _mark_failed(self, mint: str, fields: dict[str, str]) -> None:
        async with self._sessions() as session:
            async with session.begin():
                token = await session.scalar(select(Token).where(Token.mint == mint))
                if token is not None:
                    token.ai_review_status = AI_REVIEW_FAILED
        await self._refund(fields)
        logger.info("ai_review_permanent_failure", mint=mint)

    async def _refund(self, fields: dict[str, str]) -> None:
        moment = _reserved_at(fields)
        if moment is None:
            return
        await release_review(self._redis, moment, tokens=_reserved_tokens(fields))

    async def _ack(self, message_id: str) -> None:
        await self._redis.xack(AI_REVIEW_QUEUE, AI_REVIEW_GROUP, message_id)

    async def _ensure_group(self) -> None:
        if self._group_ready:
            return
        try:
            await self._redis.xgroup_create(
                AI_REVIEW_QUEUE,
                AI_REVIEW_GROUP,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    async def _read(
        self, stream_id: str, *, block_ms: int | None
    ) -> list[tuple[str, dict[str, str]]]:
        rows = await self._redis.xreadgroup(
            groupname=AI_REVIEW_GROUP,
            consumername=self._consumer,
            streams={AI_REVIEW_QUEUE: stream_id},
            count=10,
            block=block_ms,
        )
        messages: list[tuple[str, dict[str, str]]] = []
        for _stream, entries in rows or []:
            for message_id, fields in entries:
                messages.append(
                    (str(message_id), {str(key): str(value) for key, value in fields.items()})
                )
        return messages

    def _stopping(self) -> bool:
        return self._stop is not None and self._stop.is_set()


def _reserved_at(fields: dict[str, str]) -> datetime | None:
    raw = fields.get("reserved_at")
    if not raw:
        return None
    try:
        moment = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if moment.tzinfo is None:
        return moment.replace(tzinfo=timezone.utc)
    return moment


def _reserved_tokens(fields: dict[str, str]) -> int:
    raw = fields.get("reserved_tokens") or "0"
    try:
        return max(0, int(raw))
    except ValueError:
        return 0
