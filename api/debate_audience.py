from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException
from pydantic import BaseModel

from .debate_games import DATA_DIR, _load, _public, router

DB_PATH = DATA_DIR / "debate_audience.sqlite3"
_LOCK = threading.RLock()


def _iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("""
        CREATE TABLE IF NOT EXISTS debate_audience_settings (
            session_id TEXT PRIMARY KEY,
            submissions_open INTEGER NOT NULL DEFAULT 0,
            overlay_submission_id TEXT,
            updated_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS debate_audience_thoughts (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            display_name TEXT NOT NULL,
            username TEXT,
            side TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(session_id, user_id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS debate_archive (
            session_id TEXT PRIMARY KEY,
            payload TEXT NOT NULL,
            archived_at TEXT NOT NULL
        )
    """)
    con.commit()
    return con


def _session_id() -> str:
    sid = str((_load() or {}).get("session_id") or "").strip()
    if not sid:
        raise HTTPException(status_code=409, detail="No debate session is active")
    return sid


def _ensure_settings(session_id: str):
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT OR IGNORE INTO debate_audience_settings(session_id,submissions_open,overlay_submission_id,updated_at) VALUES(?,0,NULL,?)",
            (session_id, _iso()),
        )
        con.commit()
        row = con.execute("SELECT * FROM debate_audience_settings WHERE session_id=?", (session_id,)).fetchone()
        con.close()
    return dict(row) if row else {"session_id": session_id, "submissions_open": 0, "overlay_submission_id": None}


def _submissions_open(state: dict) -> bool:
    return str(state.get("status") or "") in {
        "pooling", "registration_closed", "selected", "intro_for", "speaker_for",
        "holding_against", "intro_against", "speaker_against", "holding_vote",
    }


def _thoughts(session_id: str):
    with _LOCK:
        con = _conn()
        rows = con.execute(
            "SELECT id,session_id,user_id,display_name,username,side,reason,created_at,updated_at FROM debate_audience_thoughts WHERE session_id=? ORDER BY updated_at DESC",
            (session_id,),
        ).fetchall()
        con.close()
    return [dict(r) for r in rows]


def _overlay(session_id: str, settings: dict):
    oid = str(settings.get("overlay_submission_id") or "")
    if not oid:
        return None
    with _LOCK:
        con = _conn()
        row = con.execute(
            "SELECT id,session_id,user_id,display_name,username,side,reason,created_at,updated_at FROM debate_audience_thoughts WHERE session_id=? AND id=?",
            (session_id, oid),
        ).fetchone()
        con.close()
    return dict(row) if row else None


class AudienceOpenPayload(BaseModel):
    open: bool = True


class AudienceThoughtPayload(BaseModel):
    user_id: str
    display_name: str = "Member"
    username: Optional[str] = None
    side: str
    reason: str


class AudienceOverlayPayload(BaseModel):
    submission_id: Optional[str] = None


@router.get("/audience/state")
def audience_state(user_id: Optional[str] = None):
    state = _load()
    sid = str(state.get("session_id") or "")
    if not sid:
        return {"session_id": None, "submissions_open": False, "count": 0, "mine": None, "overlay": None}
    settings = _ensure_settings(sid)
    thoughts = _thoughts(sid)
    mine = next((x for x in thoughts if str(x.get("user_id")) == str(user_id or "")), None) if user_id else None
    return {
        "session_id": sid,
        "submissions_open": _submissions_open(state),
        "count": len(thoughts),
        "mine": mine,
        "overlay": _overlay(sid, settings),
    }


@router.post("/audience/open")
def audience_open(payload: AudienceOpenPayload):
    sid = _session_id()
    _ensure_settings(sid)
    with _LOCK:
        con = _conn()
        con.execute(
            "UPDATE debate_audience_settings SET submissions_open=?,updated_at=? WHERE session_id=?",
            (1 if payload.open else 0, _iso(), sid),
        )
        con.commit()
        con.close()
    return audience_state()


@router.post("/audience/submit")
def audience_submit(payload: AudienceThoughtPayload):
    state = _load()
    sid = str(state.get("session_id") or "")
    if not sid:
        raise HTTPException(status_code=409, detail="No debate is active")
    _ensure_settings(sid)
    if not _submissions_open(state):
        raise HTTPException(status_code=409, detail="Audience thoughts are currently closed")
    uid = str(payload.user_id or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="Your member identity is unavailable")
    contestants = {str(c.get("user_id")) for c in state.get("contestants", [])}
    if uid in contestants:
        raise HTTPException(status_code=403, detail="Selected speakers cannot submit an audience thought")
    side = str(payload.side or "").strip().upper()
    if side not in {"FOR", "AGAINST"}:
        raise HTTPException(status_code=400, detail="Choose FOR or AGAINST")
    reason = " ".join(str(payload.reason or "").split()).strip()
    if not reason:
        raise HTTPException(status_code=400, detail="Add a short reason")
    if len(reason) > 280:
        raise HTTPException(status_code=400, detail="Keep your reason under 280 characters")
    now = _iso()
    with _LOCK:
        con = _conn()
        existing = con.execute(
            "SELECT id,created_at FROM debate_audience_thoughts WHERE session_id=? AND user_id=?",
            (sid, uid),
        ).fetchone()
        thought_id = str(existing["id"]) if existing else str(uuid.uuid4())
        created = str(existing["created_at"]) if existing else now
        con.execute(
            """
            INSERT INTO debate_audience_thoughts(id,session_id,user_id,display_name,username,side,reason,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?)
            ON CONFLICT(session_id,user_id) DO UPDATE SET
              display_name=excluded.display_name,
              username=excluded.username,
              side=excluded.side,
              reason=excluded.reason,
              updated_at=excluded.updated_at
            """,
            (thought_id, sid, uid[:80], (payload.display_name or "Member")[:80], (payload.username or "")[:80], side, reason, created, now),
        )
        con.commit()
        con.close()
    return audience_state(uid)


@router.get("/audience/submissions")
def audience_submissions():
    sid = _session_id()
    settings = _ensure_settings(sid)
    return {
        "session_id": sid,
        "submissions_open": _submissions_open(_load()),
        "overlay": _overlay(sid, settings),
        "submissions": _thoughts(sid),
    }


@router.post("/audience/overlay")
def audience_overlay(payload: AudienceOverlayPayload):
    sid = _session_id()
    settings = _ensure_settings(sid)
    submission_id = str(payload.submission_id or "").strip() or None
    if submission_id:
        valid = {str(x.get("id")) for x in _thoughts(sid)}
        if submission_id not in valid:
            raise HTTPException(status_code=404, detail="Audience thought not found")
    with _LOCK:
        con = _conn()
        con.execute(
            "UPDATE debate_audience_settings SET overlay_submission_id=?,updated_at=? WHERE session_id=?",
            (submission_id, _iso(), sid),
        )
        con.commit()
        con.close()
    settings["overlay_submission_id"] = submission_id
    return {"session_id": sid, "overlay": _overlay(sid, settings)}


@router.post("/archive/finalize")
def archive_finalize():
    state = _load()
    sid = str(state.get("session_id") or "")
    if not sid:
        raise HTTPException(status_code=409, detail="No debate session is available")
    public_state = _public(state)
    thoughts = _thoughts(sid)
    contestants = []
    percentages = public_state.get("vote_percentages") or {}
    for c in state.get("contestants", []):
        contestants.append({
            "user_id": str(c.get("user_id") or ""),
            "display_name": c.get("display_name") or "Member",
            "username": c.get("username") or "",
            "side": c.get("side") or "",
            "percentage": float(percentages.get(str(c.get("user_id")), 0) or 0),
        })
    winner = None
    if len(contestants) == 2 and contestants[0]["percentage"] != contestants[1]["percentage"]:
        winner = max(contestants, key=lambda x: x["percentage"])["user_id"]
    payload = {
        "session_id": sid,
        "title": state.get("title") or "Live Debate",
        "statement": state.get("statement") or "",
        "description": state.get("description") or "",
        "created_at": state.get("created_at"),
        "archived_at": _iso(),
        "contestants": contestants,
        "winner_user_id": winner,
        "vote_total": int(public_state.get("vote_total") or 0),
        "audience_thoughts": thoughts,
    }
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO debate_archive(session_id,payload,archived_at) VALUES(?,?,?) ON CONFLICT(session_id) DO UPDATE SET payload=excluded.payload,archived_at=excluded.archived_at",
            (sid, json.dumps(payload), payload["archived_at"]),
        )
        con.commit()
        con.close()
    return payload


@router.get("/archive/debates")
def archive_debates():
    with _LOCK:
        con = _conn()
        rows = con.execute("SELECT payload FROM debate_archive ORDER BY archived_at DESC").fetchall()
        con.close()
    items = []
    for row in rows:
        try:
            items.append(json.loads(row["payload"]))
        except Exception:
            pass
    return {"debates": items}


@router.get("/archive/debates/{session_id}")
def archive_debate(session_id: str):
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT payload FROM debate_archive WHERE session_id=?", (session_id,)).fetchone()
        con.close()
    if not row:
        raise HTTPException(status_code=404, detail="Debate archive entry not found")
    return json.loads(row["payload"])
