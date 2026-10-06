"""Queue names and payload helpers for the pipeline workers."""

from __future__ import annotations

from datetime import datetime, timezone

from app.config import (
    AI_REVIEW_GROUP,
    AI_REVIEW_QUEUE,
    BAIT_CHECK_QUEUE,
    RISK_HEAVY_GROUP,
    RISK_HEAVY_QUEUE,
    RISK_SCAN_GROUP,
    RISK_SCAN_PRIORITY_GROUP,
    RISK_SCAN_PRIORITY_QUEUE,
    RISK_SCAN_QUEUE,
)
from app.services.bait_detector import PendingBaitCheck

__all__ = [
    "AI_REVIEW_GROUP",
    "AI_REVIEW_QUEUE",
    "BAIT_CHECK_QUEUE",
    "RISK_HEAVY_GROUP",
    "RISK_HEAVY_QUEUE",
    "RISK_SCAN_GROUP",
    "RISK_SCAN_PRIORITY_GROUP",
    "RISK_SCAN_PRIORITY_QUEUE",
    "RISK_SCAN_QUEUE",
    "bait_member",
    "parse_bait_member",
    "score_for",
]


def bait_member(pending: PendingBaitCheck) -> str:
    return pending.model_dump_json()


def parse_bait_member(member: str) -> PendingBaitCheck:
    return PendingBaitCheck.model_validate_json(member)


def score_for(moment: datetime) -> float:
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp()
