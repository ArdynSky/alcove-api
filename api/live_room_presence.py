from __future__ import annotations

from datetime import datetime, timezone
import threading
from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/api/live-room-presence", tags=["live-room-presence"])

_LOCK = threading.RLock()
_PRESENCE: dict[str, dict] = {}
TTL_SECONDS = 18


def _now_ts() -> float:
    return datetime.now(timezone.utc).timestamp()


def _clean(now: Optional[float] = None) -> None:
    cutoff = (now or _now_ts()) - TTL_SECONDS
    stale = [uid for uid, row in _PRESENCE.items() if float(row.get("last_seen", 0)) < cutoff]
    for uid in stale:
        _PRESENCE.pop(uid, None)


class PresencePayload(BaseModel):
    user_id: str
    display_name: str = "Member"
    username: Optional[str] = None


class LeavePayload(BaseModel):
    user_id: str


@router.post("/heartbeat")
def heartbeat(payload: PresencePayload):
    uid = str(payload.user_id or "").strip()[:80]
    if not uid:
        return {"online": [], "count": 0}
    now = _now_ts()
    with _LOCK:
        _clean(now)
        _PRESENCE[uid] = {
            "user_id": uid,
            "display_name": (payload.display_name or "Member")[:80],
            "username": (payload.username or "")[:80],
            "last_seen": now,
        }
        rows = sorted(_PRESENCE.values(), key=lambda row: row.get("display_name", "").lower())
    return {"online": rows, "count": len(rows), "ttl_seconds": TTL_SECONDS}


@router.post("/leave")
def leave(payload: LeavePayload):
    uid = str(payload.user_id or "").strip()[:80]
    with _LOCK:
        if uid:
            _PRESENCE.pop(uid, None)
        _clean()
        rows = sorted(_PRESENCE.values(), key=lambda row: row.get("display_name", "").lower())
    return {"online": rows, "count": len(rows), "ttl_seconds": TTL_SECONDS}


@router.get("")
def online_now():
    with _LOCK:
        _clean()
        rows = sorted(_PRESENCE.values(), key=lambda row: row.get("display_name", "").lower())
    return {"online": rows, "count": len(rows), "ttl_seconds": TTL_SECONDS}
