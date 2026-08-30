from __future__ import annotations

import datetime
import json
import os
import sqlite3
import threading
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/members", tags=["members"])
_LOCK = threading.RLock()

PROFILE_VERSION = 1
MAX_CLAIM_EVENT_IDS = 300
MAX_EXP_HISTORY = 40
MAX_OWNED_META = 400
OWNED_LIST_KEYS = (
    "feedColors",
    "feedSkins",
    "feedStickers",
    "feedBackdrops",
    "titles",
    "foxItems",
)


def _db_path() -> Path:
    raw = os.getenv("ALCOVE_STATE_DB_PATH", "").strip()
    if raw:
        return Path(raw).expanduser()
    return Path.cwd() / "alcove_state.db"


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _conn() -> sqlite3.Connection:
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS member_progress (
            user_id TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    con.commit()
    return con


def resolve_telegram_user(init_data: str | None) -> dict:
    from .main import verify_telegram_init_data

    return verify_telegram_init_data(init_data or "")


def _admin(secret: str | None) -> None:
    from .main import verify_admin_secret

    verify_admin_secret(secret)


def _parse_ts(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    try:
        return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _as_dict(value: Any) -> dict:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list:
    return list(value) if isinstance(value, list) else []


def _union_str_list(*groups: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for group in groups:
        for item in _as_list(group):
            key = str(item or "").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(key)
    return out


def _trim_dict(value: Any, limit: int) -> dict:
    data = _as_dict(value)
    if len(data) <= limit:
        return dict(data)
    keys = list(data.keys())[-limit:]
    return {key: data[key] for key in keys}


def normalize_profile(raw: Any) -> dict:
    data = _as_dict(raw)
    owned_in = _as_dict(data.get("owned"))
    owned = {key: _union_str_list(owned_in.get(key)) for key in OWNED_LIST_KEYS}
    level = max(1, int(data.get("level") or 1))
    exp = max(0, int(data.get("exp") or 0))
    exp_view = _as_dict(data.get("expView"))
    stats = _as_dict(data.get("stats"))
    clean_stats = {}
    for key, value in stats.items():
        try:
            clean_stats[str(key)[:80]] = max(0, int(value or 0))
        except (TypeError, ValueError):
            continue
    return {
        "version": PROFILE_VERSION,
        "updated_at": str(data.get("updated_at") or data.get("updatedAt") or "").strip(),
        "level": level,
        "exp": exp,
        "expMax": max(1, int(data.get("expMax") or 200)),
        "expView": {
            "level": max(1, int(exp_view.get("level") or level)),
            "exp": max(0, int(exp_view.get("exp") or exp)),
        },
        "feed": _as_dict(data.get("feed")),
        "title": str(data.get("title") or "none")[:80],
        "fox": _as_dict(data.get("fox")),
        "owned": owned,
        "ownedMeta": _trim_dict(data.get("ownedMeta"), MAX_OWNED_META),
        "newUnlocks": _union_str_list(data.get("newUnlocks"))[:200],
        "pendingRewards": _as_list(data.get("pendingRewards"))[-40:],
        "levelRewardsClaimed": _union_str_list(data.get("levelRewardsClaimed"))[:80],
        "claimReceipts": _trim_dict(data.get("claimReceipts"), MAX_CLAIM_EVENT_IDS),
        "claimEventIds": _trim_dict(data.get("claimEventIds"), MAX_CLAIM_EVENT_IDS),
        "expHistory": _as_list(data.get("expHistory"))[-MAX_EXP_HISTORY:],
        "achievementsClaimed": _union_str_list(data.get("achievementsClaimed"))[:200],
        "achievementsNotified": _union_str_list(data.get("achievementsNotified"))[:200],
        "progressionResetToken": str(data.get("progressionResetToken") or "")[:120],
        "stats": clean_stats,
        "memberSince": str(data.get("memberSince") or "")[:80],
        "equippedAchievements": _public_equipped(data.get("equippedAchievements") or data.get("equipped_achievements")),
    }


def merge_profiles(base: dict | None, incoming: dict | None) -> dict:
    left = normalize_profile(base)
    right = normalize_profile(incoming)
    if not base:
        return right
    if not incoming:
        return left

    left_ts = _parse_ts(left.get("updated_at"))
    right_ts = _parse_ts(right.get("updated_at"))
    newer, older = (right, left) if right_ts >= left_ts else (left, right)

    if (
        newer.get("progressionResetToken")
        and newer.get("progressionResetToken") != older.get("progressionResetToken")
        and right_ts != left_ts
    ):
        merged = dict(newer)
        merged["updated_at"] = newer.get("updated_at") or _now()
        return normalize_profile(merged)

    owned = {key: _union_str_list(left["owned"].get(key), right["owned"].get(key)) for key in OWNED_LIST_KEYS}
    stats = {}
    for key in set(left["stats"]) | set(right["stats"]):
        stats[key] = max(int(left["stats"].get(key) or 0), int(right["stats"].get(key) or 0))

    if left["level"] > right["level"]:
        level, exp, exp_max, exp_view = left["level"], left["exp"], left["expMax"], left["expView"]
    elif right["level"] > left["level"]:
        level, exp, exp_max, exp_view = right["level"], right["exp"], right["expMax"], right["expView"]
    else:
        level = left["level"]
        exp = max(left["exp"], right["exp"])
        exp_max = max(left["expMax"], right["expMax"])
        exp_view = newer["expView"]

    merged = {
        **older,
        **newer,
        "level": level,
        "exp": exp,
        "expMax": exp_max,
        "expView": exp_view,
        "owned": owned,
        "ownedMeta": {**left["ownedMeta"], **right["ownedMeta"]},
        "newUnlocks": _union_str_list(left["newUnlocks"], right["newUnlocks"]),
        "pendingRewards": (_as_list(older.get("pendingRewards")) + _as_list(newer.get("pendingRewards")))[-40:],
        "levelRewardsClaimed": _union_str_list(left["levelRewardsClaimed"], right["levelRewardsClaimed"]),
        "claimReceipts": {**left["claimReceipts"], **right["claimReceipts"]},
        "claimEventIds": {**left["claimEventIds"], **right["claimEventIds"]},
        "expHistory": (_as_list(older.get("expHistory")) + _as_list(newer.get("expHistory")))[-MAX_EXP_HISTORY:],
        "achievementsClaimed": _union_str_list(left["achievementsClaimed"], right["achievementsClaimed"]),
        "achievementsNotified": _union_str_list(left["achievementsNotified"], right["achievementsNotified"]),
        "stats": stats,
        "equippedAchievements": newer.get("equippedAchievements") or older.get("equippedAchievements") or [],
        "updated_at": newer.get("updated_at") or _now(),
    }
    return normalize_profile(merged)


def load_profile(user_id: str) -> dict | None:
    key = str(user_id or "").strip()
    if not key:
        return None
    with _LOCK:
        with _conn() as con:
            row = con.execute(
                "SELECT payload_json FROM member_progress WHERE user_id = ?",
                (key,),
            ).fetchone()
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None
    return normalize_profile(payload) if isinstance(payload, dict) else None


def save_profile(user_id: str, incoming: dict, *, replace: bool = False) -> dict:
    key = str(user_id or "").strip()
    if not key:
        raise HTTPException(status_code=400, detail="Missing Telegram user id")
    payload = normalize_profile(incoming)
    if not payload.get("updated_at"):
        payload["updated_at"] = _now()
    with _LOCK:
        current = None if replace else load_profile(key)
        merged = payload if replace or current is None else merge_profiles(current, payload)
        if not merged.get("updated_at"):
            merged["updated_at"] = _now()
        serialized = json.dumps(merged, separators=(",", ":"), ensure_ascii=False)
        with _conn() as con:
            con.execute(
                """
                INSERT INTO member_progress (user_id, payload_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (key, serialized, merged["updated_at"]),
            )
            con.commit()
    return merged


def _init_data(
    init_data: str | None,
    x_telegram_init_data: str | None,
) -> str:
    return (x_telegram_init_data or init_data or "").strip()


def _public_equipped(raw: Any) -> list[dict]:
    out: list[dict] = []
    for item in _as_list(raw)[:3]:
        if isinstance(item, dict):
            out.append({
                "key": str(item.get("key") or "")[:80],
                "name": str(item.get("name") or item.get("title") or "Achievement")[:120],
                "title": str(item.get("title") or item.get("name") or "")[:120],
                "description": str(item.get("description") or item.get("desc") or "")[:400],
                "image": str(item.get("image") or item.get("src") or item.get("image_url") or "")[:500],
                "art": str(item.get("art") or "")[:8000],
            })
        elif str(item or "").strip():
            key = str(item).strip()[:80]
            out.append({"key": key, "name": key, "title": key, "description": "", "image": "", "art": ""})
    return out


def public_card(user_id: str) -> dict:
    key = str(user_id or "").strip()
    profile = load_profile(key) if key else None
    if not profile:
        return {
            "found": False,
            "user_id": key,
            "profile": None,
            "feed_style": {},
            "equipped_achievements": [],
        }
    return {
        "found": True,
        "user_id": key,
        "profile": {
            "level": profile.get("level") or 1,
            "memberSince": profile.get("memberSince") or "",
            "title": profile.get("title") or "none",
            "feed": profile.get("feed") or {},
        },
        "feed_style": profile.get("feed") or {},
        "equipped_achievements": _public_equipped(profile.get("equippedAchievements")),
    }


def _member_id(init_data: str) -> str:
    user = resolve_telegram_user(init_data)
    user_id = user.get("id")
    if user_id is None:
        raise HTTPException(status_code=400, detail="Telegram Mini App user was not provided")
    return str(user_id)


class MemberProfilePayload(BaseModel):
    init_data: str | None = None
    profile: dict[str, Any] = Field(default_factory=dict)
    replace: bool = False


class AdminMemberProfilePayload(BaseModel):
    admin_secret: str
    profile: dict[str, Any] = Field(default_factory=dict)
    replace: bool = False


@router.get("/profile")
def get_my_profile(
    init_data: str | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user_id = _member_id(_init_data(init_data, x_telegram_init_data))
    profile = load_profile(user_id)
    return {"status": "ok", "user_id": user_id, "found": bool(profile), "profile": profile}


@router.put("/profile")
def put_my_profile(
    payload: MemberProfilePayload,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user_id = _member_id(_init_data(payload.init_data, x_telegram_init_data))
    stored = save_profile(user_id, payload.profile, replace=bool(payload.replace))
    return {"status": "ok", "user_id": user_id, "profile": stored}


@router.get("/public-profile")
def get_public_profile(user_id: str):
    return {"status": "ok", **public_card(user_id)}


@router.get("/{user_id}/profile")
def admin_get_profile(user_id: str, admin_secret: str):
    _admin(admin_secret)
    profile = load_profile(user_id)
    return {"status": "ok", "user_id": str(user_id), "found": bool(profile), "profile": profile}


@router.put("/{user_id}/profile")
def admin_put_profile(user_id: str, payload: AdminMemberProfilePayload):
    _admin(payload.admin_secret)
    stored = save_profile(str(user_id), payload.profile, replace=bool(payload.replace))
    return {"status": "ok", "user_id": str(user_id), "profile": stored}
