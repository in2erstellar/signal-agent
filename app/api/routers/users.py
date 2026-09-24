"""Subscriber settings addressed by Telegram id."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session
from app.api.schemas import SettingsPatch, UserSettingsOut
from app.db.models import User
from app.services.subscriber_settings import parse_settings, with_updates

router = APIRouter(prefix="/users", tags=["users"])


def _out(user: User) -> UserSettingsOut:
    prefs = parse_settings(user.settings)
    return UserSettingsOut(
        telegram_id=user.telegram_id,
        min_score=prefs.min_score,
        max_risk=prefs.max_risk,
        active=prefs.active,
    )


@router.get("/{telegram_id}/settings", response_model=UserSettingsOut)
async def get_settings_for_user(
    telegram_id: int,
    session: AsyncSession = Depends(get_session),
) -> UserSettingsOut:
    user = await session.scalar(select(User).where(User.telegram_id == telegram_id))
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")
    return _out(user)


@router.patch("/{telegram_id}/settings", response_model=UserSettingsOut)
async def patch_settings(
    telegram_id: int,
    body: SettingsPatch,
    session: AsyncSession = Depends(get_session),
) -> UserSettingsOut:
    changes = body.model_dump(exclude_unset=True)
    try:
        async with session.begin():
            user = await session.scalar(select(User).where(User.telegram_id == telegram_id))
            if user is None:
                raise HTTPException(status_code=404, detail="user not found")
            if changes:
                user.settings = with_updates(user.settings, **changes)
    except HTTPException:
        raise
    return _out(user)
