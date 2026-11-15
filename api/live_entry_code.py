"""Four-digit Live Room entry code — shown on OBS, required before lobby join."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/live-entry", tags=["live-entry"])

SESSION_TTL_HOURS = 8
_LOCK = threading.RLock()


def _data_dir() -> Path:
    for key in ("ALCOVE_STATE_DB_PATH", "ALCOVE_RUNTIME_STATE_PATH", "FOX_LOGS_DB_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw).expanduser().resolve().parent
    return Path.cwd()


DB_PATH = Path(os.getenv("ALCOVE_LIVE_ENTRY_DB", str(_data_dir() / "live_entry_code.sqlite3")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    return (dt or _now()).isoformat()


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _secret() -> str:
    return (
        os.getenv("LIVE_ENTRY_SECRET", "").strip()
        or os.getenv("BOT_SYNC_SECRET", "").strip()
        or "alcove-live-entry-dev"
    )


def _hash_token(token: str) -> str:
    return hashlib.sha256(f"{_secret()}:{token}".encode("utf-8")).hexdigest()


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE IF NOT EXISTS live_entry_code ("
        "id INTEGER PRIMARY KEY CHECK (id = 1), "
        "code TEXT NOT NULL, agenda_id TEXT, updated_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS live_entry_sessions ("
        "token_hash TEXT PRIMARY KEY, agenda_id TEXT, created_at TEXT NOT NULL, expires_at TEXT NOT NULL)"
    )
    columns = {row[1] for row in con.execute("PRAGMA table_info(live_entry_sessions)").fetchall()}
    if "agenda_id" not in columns:
        con.execute("ALTER TABLE live_entry_sessions ADD COLUMN agenda_id TEXT")
    con.commit()
    return con


def _generate_code() -> str:
    return f"{secrets.randbelow(10000):04d}"


def _row_unlocked(con):
    return con.execute("SELECT code, agenda_id, updated_at FROM live_entry_code WHERE id = 1").fetchone()


def _write_code_unlocked(con, code: str, agenda_id: Optional[str]) -> str:
    con.execute(
        "INSERT INTO live_entry_code(id, code, agenda_id, updated_at) VALUES(1, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET code=excluded.code, agenda_id=excluded.agenda_id, "
        "updated_at=excluded.updated_at",
        (code, agenda_id, _iso()),
    )
    return code


def _clear_sessions_unlocked(con, agenda_id: Optional[str] = None) -> None:
    if agenda_id is None:
        con.execute("DELETE FROM live_entry_sessions")
    else:
        con.execute(
            "DELETE FROM live_entry_sessions WHERE agenda_id IS NULL OR agenda_id <> ?",
            (str(agenda_id),),
        )


def clear_code() -> None:
    with _LOCK:
        con = _conn()
        con.execute("DELETE FROM live_entry_code WHERE id = 1")
        _clear_sessions_unlocked(con)
        con.commit()
        con.close()


def ensure_for_agenda(agenda_id: Optional[str]) -> str:
    """Keep the existing code for the same live agenda; mint a new one when it changes."""
    wanted = str(agenda_id or "").strip() or None
    with _LOCK:
        con = _conn()
        row = _row_unlocked(con)
        if (
            row
            and str(row["agenda_id"] or "") == str(wanted or "")
            and str(row["code"] or "").isdigit()
            and len(str(row["code"])) == 4
        ):
            code = str(row["code"])
            con.close()
            return code
        code = _write_code_unlocked(con, _generate_code(), wanted)
        # New agenda (or first mint) invalidates sessions from any previous show.
        _clear_sessions_unlocked(con, None)
        con.commit()
        con.close()
        return code


def rotate_code(agenda_id: Optional[str] = None) -> str:
    with _LOCK:
        con = _conn()
        row = _row_unlocked(con)
        aid = str(agenda_id or (row["agenda_id"] if row else "") or "").strip() or None
        code = _write_code_unlocked(con, _generate_code(), aid)
        con.commit()
        con.close()
        return code


def sync_with_active_agenda(agenda: Optional[dict]) -> Optional[str]:
    """Called from agenda status changes. Returns current code when live, else None."""
    if agenda and str(agenda.get("status") or "") == "active":
        return ensure_for_agenda(agenda.get("id"))
    clear_code()
    return None


def _active_agenda():
    from . import agendas

    return agendas.active_agenda().get("agenda")


def _purge_expired_sessions(con) -> None:
    con.execute("DELETE FROM live_entry_sessions WHERE expires_at < ?", (_iso(),))


def _admin_ok(secret: Optional[str]) -> bool:
    expected = os.getenv("BOT_SYNC_SECRET", "").strip()
    if not expected or not secret:
        return False
    return hmac.compare_digest(str(secret), expected)


def _require_host_or_admin(
    admin_secret: Optional[str] = None,
    x_alcove_admin: Optional[str] = None,
    x_bot_sync_secret: Optional[str] = None,
    x_telegram_init_data: Optional[str] = None,
    authorization: Optional[str] = None,
) -> None:
    if _admin_ok(admin_secret) or _admin_ok(x_alcove_admin) or _admin_ok(x_bot_sync_secret):
        return
    if x_telegram_init_data or authorization:
        from .live_room_test import _is_host
        from . import main

        if x_telegram_init_data:
            user = main.verify_telegram_init_data(x_telegram_init_data)
        else:
            from .live_room_test import identity_from_authorization

            user = identity_from_authorization(authorization)
            if not user:
                raise HTTPException(status_code=401, detail="Host authorization required")
        if _is_host(user.get("id") or user.get("user_id")):
            return
        raise HTTPException(status_code=403, detail="Host authorization required")
    raise HTTPException(status_code=403, detail="Host authorization required")


class VerifyPayload(BaseModel):
    code: str = Field(min_length=4, max_length=8)


class RotatePayload(BaseModel):
    admin_secret: str = ""


@router.get("/code")
def get_code():
    """Public OBS readout — only returns digits while a live agenda is active."""
    agenda = _active_agenda()
    live = bool(agenda)
    code = None
    if live:
        code = ensure_for_agenda(agenda.get("id"))
    return {
        "is_live": live,
        "code": code if live else None,
        "agenda_id": agenda.get("id") if agenda else None,
    }


@router.post("/rotate")
def rotate(
    payload: RotatePayload | None = None,
    x_alcove_admin: Optional[str] = Header(default=None),
    x_bot_sync_secret: Optional[str] = Header(default=None, alias="X-Bot-Sync-Secret"),
    x_telegram_init_data: Optional[str] = Header(default=None, alias="X-Telegram-Init-Data"),
    authorization: Optional[str] = Header(default=None),
):
    _require_host_or_admin(
        admin_secret=(payload.admin_secret if payload else None),
        x_alcove_admin=x_alcove_admin,
        x_bot_sync_secret=x_bot_sync_secret,
        x_telegram_init_data=x_telegram_init_data,
        authorization=authorization,
    )
    agenda = _active_agenda()
    if not agenda:
        raise HTTPException(status_code=409, detail="Turn the Live Room on before rotating the entry code")
    code = rotate_code(agenda.get("id"))
    return {"ok": True, "code": code, "agenda_id": agenda.get("id")}


@router.post("/verify")
def verify(payload: VerifyPayload):
    agenda = _active_agenda()
    if not agenda:
        raise HTTPException(status_code=409, detail="Live Room is not open")
    agenda_id = str(agenda.get("id") or "")
    with _LOCK:
        con = _conn()
        row = _row_unlocked(con)
        offered = "".join(ch for ch in str(payload.code or "") if ch.isdigit())
        expected = str(row["code"] if row else "")
        if len(offered) != 4 or offered != expected or str(row["agenda_id"] if row else "") != agenda_id:
            con.close()
            raise HTTPException(status_code=403, detail="Incorrect code")
        token = secrets.token_urlsafe(24)
        expires = _now() + timedelta(hours=SESSION_TTL_HOURS)
        _purge_expired_sessions(con)
        con.execute(
            "INSERT INTO live_entry_sessions(token_hash, agenda_id, created_at, expires_at) VALUES(?,?,?,?)",
            (_hash_token(token), agenda_id, _iso(), _iso(expires)),
        )
        con.commit()
        con.close()
    return {"ok": True, "token": token, "expires_at": _iso(expires)}


@router.get("/session")
def check_session(token: str = ""):
    raw = str(token or "").strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Missing entry token")
    agenda = _active_agenda()
    if not agenda:
        raise HTTPException(status_code=409, detail="Live Room is not open")
    agenda_id = str(agenda.get("id") or "")
    with _LOCK:
        con = _conn()
        _purge_expired_sessions(con)
        row = con.execute(
            "SELECT expires_at, agenda_id FROM live_entry_sessions WHERE token_hash = ?",
            (_hash_token(raw),),
        ).fetchone()
        con.commit()
        con.close()
    if not row or str(row["agenda_id"] or "") != agenda_id:
        raise HTTPException(status_code=401, detail="Entry token expired or unknown")
    expires = _parse(row["expires_at"])
    if not expires or expires <= _now():
        raise HTTPException(status_code=401, detail="Entry token expired or unknown")
    return {"ok": True, "expires_at": row["expires_at"], "agenda_id": agenda_id}
