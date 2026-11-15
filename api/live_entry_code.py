"""Four-digit Live Room entry code — shown on OBS, required before lobby join."""

from __future__ import annotations

import hashlib
import os
import secrets
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
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
        "token_hash TEXT PRIMARY KEY, created_at TEXT NOT NULL, expires_at TEXT NOT NULL)"
    )
    con.commit()
    return con


def _generate_code() -> str:
    return f"{secrets.randbelow(10000):04d}"


def _read_row():
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT code, agenda_id, updated_at FROM live_entry_code WHERE id = 1").fetchone()
        con.close()
    return row


def _write_code(code: str, agenda_id: Optional[str]) -> str:
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO live_entry_code(id, code, agenda_id, updated_at) VALUES(1, ?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET code=excluded.code, agenda_id=excluded.agenda_id, "
            "updated_at=excluded.updated_at",
            (code, agenda_id, _iso()),
        )
        con.commit()
        con.close()
    return code


def clear_code() -> None:
    with _LOCK:
        con = _conn()
        con.execute("DELETE FROM live_entry_code WHERE id = 1")
        con.commit()
        con.close()


def ensure_for_agenda(agenda_id: Optional[str]) -> str:
    """Keep the existing code for the same live agenda; mint a new one when it changes."""
    wanted = str(agenda_id or "").strip() or None
    row = _read_row()
    if row and str(row["agenda_id"] or "") == str(wanted or "") and str(row["code"] or "").isdigit() and len(str(row["code"])) == 4:
        return str(row["code"])
    return _write_code(_generate_code(), wanted)


def rotate_code(agenda_id: Optional[str] = None) -> str:
    row = _read_row()
    aid = str(agenda_id or (row["agenda_id"] if row else "") or "").strip() or None
    return _write_code(_generate_code(), aid)


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


class VerifyPayload(BaseModel):
    code: str = Field(min_length=4, max_length=8)


@router.get("/code")
def get_code():
    """Public OBS readout — only returns digits while a live agenda is active."""
    agenda = _active_agenda()
    live = bool(agenda)
    row = _read_row()
    code = None
    if live:
        if row and str(row["agenda_id"] or "") == str(agenda.get("id") or ""):
            code = str(row["code"])
        else:
            code = ensure_for_agenda(agenda.get("id"))
    return {
        "is_live": live,
        "code": code if live else None,
        "agenda_id": agenda.get("id") if agenda else None,
    }


@router.post("/rotate")
def rotate():
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
    row = _read_row()
    offered = "".join(ch for ch in str(payload.code or "") if ch.isdigit())
    expected = str(row["code"] if row else "")
    if len(offered) != 4 or offered != expected:
        raise HTTPException(status_code=403, detail="Incorrect code")
    token = secrets.token_urlsafe(24)
    expires = _now() + timedelta(hours=SESSION_TTL_HOURS)
    with _LOCK:
        con = _conn()
        _purge_expired_sessions(con)
        con.execute(
            "INSERT INTO live_entry_sessions(token_hash, created_at, expires_at) VALUES(?,?,?)",
            (_hash_token(token), _iso(), _iso(expires)),
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
    with _LOCK:
        con = _conn()
        _purge_expired_sessions(con)
        row = con.execute(
            "SELECT expires_at FROM live_entry_sessions WHERE token_hash = ?",
            (_hash_token(raw),),
        ).fetchone()
        con.commit()
        con.close()
    if not row:
        raise HTTPException(status_code=401, detail="Entry token expired or unknown")
    expires = _parse(row["expires_at"])
    if not expires or expires <= _now():
        raise HTTPException(status_code=401, detail="Entry token expired or unknown")
    return {"ok": True, "expires_at": row["expires_at"]}
