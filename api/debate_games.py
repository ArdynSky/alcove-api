from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
import json
import os
import random
import sqlite3
import threading
import uuid

from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/debate", tags=["debate"])

DATA_DIR = Path(os.getenv("ALCOVE_DEBATE_DIR", Path.cwd() / "debate_data"))
BACKGROUND_DIR = DATA_DIR / "backgrounds"
DB_PATH = DATA_DIR / "debate.sqlite3"
DATA_DIR.mkdir(parents=True, exist_ok=True)
BACKGROUND_DIR.mkdir(parents=True, exist_ok=True)
_LOCK = threading.RLock()
PRESENCE_TTL_SECONDS = 22


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    return (dt or _now()).isoformat()


def _parse(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE IF NOT EXISTS debate_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, updated_at TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS debate_votes (session_id TEXT NOT NULL, user_id TEXT NOT NULL, contestant_id TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY(session_id,user_id))")
    con.execute("CREATE TABLE IF NOT EXISTS live_room_presence (user_id TEXT PRIMARY KEY, display_name TEXT NOT NULL, username TEXT, last_seen TEXT NOT NULL)")
    con.commit()
    return con


def _default_state():
    return {
        "session_id": None,
        "status": "idle",
        "title": "Live Debate",
        "statement": "",
        "description": "",
        "registration_seconds": 60,
        "registration_ends_at": None,
        "duration_seconds": 120,
        "voting_seconds": 30,
        "created_at": None,
        "participants": [],
        "contestants": [],
        "active_side": None,
        "turn_started_at": None,
        "turn_ends_at": None,
        "voting": {"open": False, "opened_at": None, "ends_at": None},
        "results": {"visible": False, "reveal_started_at": None, "animation_seconds": 7},
        "background_filename": None,
    }


def _raw_load():
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT payload FROM debate_state WHERE id=1").fetchone()
        con.close()
    if not row:
        return _default_state()
    try:
        state = json.loads(row["payload"])
    except Exception:
        return _default_state()
    base = _default_state()
    base.update(state or {})
    return base


def _save(state):
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO debate_state(id,payload,updated_at) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
            (json.dumps(state), _iso()),
        )
        con.commit()
        con.close()
    return state


def _normalize_timers(state):
    changed = False
    now = _now()
    if state.get("status") == "pooling":
        end = _parse(state.get("registration_ends_at"))
        if end and end <= now:
            state["status"] = "registration_closed"
            changed = True
    if state.get("status") == "speaker_for":
        end = _parse(state.get("turn_ends_at"))
        if end and end <= now:
            state["status"] = "holding_against"
            state["active_side"] = None
            changed = True
    if state.get("status") == "speaker_against":
        end = _parse(state.get("turn_ends_at"))
        if end and end <= now:
            state["status"] = "holding_vote"
            state["active_side"] = None
            changed = True
    if state.get("status") == "voting" and state.get("voting", {}).get("open"):
        end = _parse(state.get("voting", {}).get("ends_at"))
        if end and end <= now:
            state["voting"]["open"] = False
            state["status"] = "results_ready"
            changed = True
    if changed:
        _save(state)
    return state


def _load():
    return _normalize_timers(_raw_load())


def _votes(state):
    sid = state.get("session_id") or ""
    with _LOCK:
        con = _conn()
        rows = con.execute("SELECT user_id,contestant_id FROM debate_votes WHERE session_id=?", (sid,)).fetchall()
        con.close()
    counts = {str(c.get("user_id")): 0 for c in state.get("contestants", [])}
    for row in rows:
        cid = str(row["contestant_id"])
        if cid in counts:
            counts[cid] += 1
    total = sum(counts.values())
    percentages = {k: (round((v / total) * 100, 1) if total else 0) for k, v in counts.items()}
    return counts, percentages, total, rows


def _public(state, user_id: Optional[str] = None):
    out = json.loads(json.dumps(state))
    bg = state.get("background_filename")
    out["background_url"] = f"/api/debate/background-media/{bg}" if bg else None
    counts, percentages, total, rows = _votes(state)
    uid = str(user_id or "")
    out["vote_counts"] = counts
    out["vote_percentages"] = percentages
    out["vote_total"] = total
    out["my_vote"] = next((str(r["contestant_id"]) for r in rows if str(r["user_id"]) == uid), None)
    out["joined"] = any(str(p.get("user_id")) == uid for p in state.get("participants", [])) if uid else False
    return out


def _active_presence():
    cutoff = _now() - timedelta(seconds=PRESENCE_TTL_SECONDS)
    with _LOCK:
        con = _conn()
        con.execute("DELETE FROM live_room_presence WHERE last_seen < ?", (_iso(cutoff),))
        rows = con.execute("SELECT user_id,display_name,username,last_seen FROM live_room_presence ORDER BY last_seen DESC").fetchall()
        con.commit()
        con.close()
    return [dict(row) for row in rows]


class StartPayload(BaseModel):
    title: str = "Live Debate"
    statement: str
    description: str = ""
    registration_seconds: int = 60
    duration_seconds: int = 120
    voting_seconds: int = 30


class JoinPayload(BaseModel):
    user_id: str
    display_name: str
    username: Optional[str] = None
    feed_style: Optional[dict] = None


class UserPayload(BaseModel):
    user_id: str


class PresencePayload(BaseModel):
    user_id: str
    display_name: str = "Member"
    username: Optional[str] = None


class VotePayload(BaseModel):
    user_id: str
    contestant_id: str


class ResultsPayload(BaseModel):
    visible: bool = True
    animation_seconds: int = 7


@router.get("/state")
def state(user_id: Optional[str] = None):
    return _public(_load(), user_id)


@router.post("/presence/heartbeat")
def presence_heartbeat(payload: PresencePayload):
    uid = str(payload.user_id or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="user_id is required")
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO live_room_presence(user_id,display_name,username,last_seen) VALUES(?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET display_name=excluded.display_name,username=excluded.username,last_seen=excluded.last_seen",
            (uid[:80], (payload.display_name or "Member")[:80], (payload.username or "")[:80], _iso()),
        )
        con.commit()
        con.close()
    people = _active_presence()
    return {"count": len(people), "people": people, "ttl_seconds": PRESENCE_TTL_SECONDS}


@router.get("/presence")
def presence_list():
    people = _active_presence()
    return {"count": len(people), "people": people, "ttl_seconds": PRESENCE_TTL_SECONDS}


@router.post("/presence/leave")
def presence_leave(payload: UserPayload):
    uid = str(payload.user_id or "").strip()
    if uid:
        with _LOCK:
            con = _conn()
            con.execute("DELETE FROM live_room_presence WHERE user_id=?", (uid[:80],))
            con.commit()
            con.close()
    people = _active_presence()
    return {"count": len(people), "people": people, "ttl_seconds": PRESENCE_TTL_SECONDS}


@router.post("/start")
def start(payload: StartPayload):
    statement = (payload.statement or "").strip()
    if not statement:
        raise HTTPException(status_code=400, detail="Debate statement is required")
    previous = _raw_load()
    now = _now()
    registration_seconds = max(15, min(int(payload.registration_seconds or 60), 600))
    state = _default_state()
    state["background_filename"] = previous.get("background_filename")
    state.update({
        "session_id": str(uuid.uuid4()),
        "status": "pooling",
        "title": (payload.title or "Live Debate")[:120],
        "statement": statement[:300],
        "description": (payload.description or "")[:600],
        "registration_seconds": registration_seconds,
        "registration_ends_at": _iso(now + timedelta(seconds=registration_seconds)),
        "duration_seconds": max(30, min(int(payload.duration_seconds or 120), 600)),
        "voting_seconds": max(10, min(int(payload.voting_seconds or 30), 120)),
        "created_at": _iso(now),
    })
    _save(state)
    return _public(state)


@router.post("/registration/close")
def close_registration():
    state = _load()
    if state.get("status") not in {"pooling", "registration_closed"}:
        raise HTTPException(status_code=409, detail="Registration is not open")
    state["status"] = "registration_closed"
    state["registration_ends_at"] = _iso()
    _save(state)
    return _public(state)


@router.post("/background")
async def background_upload(file: UploadFile = File(...)):
    state = _load()
    content_type = (file.content_type or "").lower()
    ext = Path(file.filename or "background.mp4").suffix.lower()
    if content_type not in {"video/mp4", "video/webm"} and ext not in {".mp4", ".webm"}:
        raise HTTPException(status_code=400, detail="Please upload an MP4 or WebM video")
    data = await file.read()
    if len(data) > 40 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Background video must be under 40 MB")
    if ext not in {".mp4", ".webm"}:
        ext = ".mp4" if "mp4" in content_type else ".webm"
    filename = f"debate-{uuid.uuid4().hex[:12]}{ext}"
    (BACKGROUND_DIR / filename).write_bytes(data)
    state["background_filename"] = filename
    _save(state)
    return _public(state)


@router.delete("/background")
def background_remove():
    state = _load()
    state["background_filename"] = None
    _save(state)
    return _public(state)


@router.get("/background-media/{filename}")
def background_media(filename: str):
    path = BACKGROUND_DIR / Path(filename).name
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Background video not found")
    return FileResponse(path)


@router.post("/join")
def join(payload: JoinPayload):
    state = _load()
    if state.get("status") != "pooling":
        raise HTTPException(status_code=409, detail="Debate registration is closed")
    uid = str(payload.user_id or "").strip()
    if not uid:
        raise HTTPException(status_code=400, detail="user_id is required")
    if not any(str(p.get("user_id")) == uid for p in state.get("participants", [])):
        state.setdefault("participants", []).append({
            "user_id": uid[:80],
            "display_name": (payload.display_name or "Member")[:80],
            "username": (payload.username or "")[:80],
            "feed_style": payload.feed_style or {},
            "joined_at": _iso(),
        })
        _save(state)
    return _public(state, uid)


@router.post("/leave")
def leave(payload: UserPayload):
    state = _load()
    if state.get("status") != "pooling":
        raise HTTPException(status_code=409, detail="You can only leave while registration is open")
    uid = str(payload.user_id or "")
    state["participants"] = [p for p in state.get("participants", []) if str(p.get("user_id")) != uid]
    _save(state)
    return _public(state, uid)


@router.post("/select")
def select_contestants():
    state = _load()
    if state.get("status") not in {"pooling", "registration_closed"}:
        raise HTTPException(status_code=409, detail="Close registration before selecting contestants")
    people = list(state.get("participants", []))
    if len(people) < 2:
        raise HTTPException(status_code=409, detail="At least two people must join")
    chosen = random.sample(people, 2)
    if random.random() < 0.5:
        chosen.reverse()
    # Visual order is always FOR on the left (A), AGAINST on the right (B).
    state["contestants"] = [dict(chosen[0], side="FOR", slot="A"), dict(chosen[1], side="AGAINST", slot="B")]
    state["status"] = "selected"
    state["registration_ends_at"] = _iso()
    _save(state)
    return _public(state)


def _start_turn(side: str):
    state = _load()
    if len(state.get("contestants", [])) != 2:
        raise HTTPException(status_code=409, detail="Select two contestants first")
    side = side.upper()
    if side == "FOR" and state.get("status") not in {"selected", "holding_against"}:
        raise HTTPException(status_code=409, detail="The FOR turn cannot start from the current stage")
    if side == "AGAINST" and state.get("status") not in {"selected", "holding_against"}:
        raise HTTPException(status_code=409, detail="The AGAINST turn cannot start from the current stage")
    now = _now()
    state["status"] = "speaker_for" if side == "FOR" else "speaker_against"
    state["active_side"] = side
    state["turn_started_at"] = _iso(now)
    state["turn_ends_at"] = _iso(now + timedelta(seconds=int(state.get("duration_seconds") or 120)))
    state["voting"] = {"open": False, "opened_at": None, "ends_at": None}
    state["results"] = {"visible": False, "reveal_started_at": None, "animation_seconds": 7}
    _save(state)
    return _public(state)


@router.post("/turn/for")
def start_for_turn():
    return _start_turn("FOR")


@router.post("/turn/against")
def start_against_turn():
    return _start_turn("AGAINST")


# Kept for older host pages: begin now starts the FOR turn.
@router.post("/begin")
def begin():
    return _start_turn("FOR")


@router.post("/holding")
def holding():
    state = _load()
    if state.get("status") in {"speaker_for", "holding_against"}:
        state["status"] = "holding_against"
    elif state.get("status") in {"speaker_against", "holding_vote", "selected"}:
        state["status"] = "holding_vote"
    else:
        raise HTTPException(status_code=409, detail="Cannot move to holding from the current stage")
    state["active_side"] = None
    state["turn_ends_at"] = None
    _save(state)
    return _public(state)


@router.post("/voting/open")
def open_voting():
    state = _load()
    if len(state.get("contestants", [])) != 2:
        raise HTTPException(status_code=409, detail="No contestants selected")
    if state.get("status") not in {"holding_vote", "results_ready"}:
        raise HTTPException(status_code=409, detail="Finish both speaking turns before opening voting")
    now = _now()
    state["status"] = "voting"
    state["voting"] = {"open": True, "opened_at": _iso(now), "ends_at": _iso(now + timedelta(seconds=int(state.get("voting_seconds") or 30)))}
    state["results"] = {"visible": False, "reveal_started_at": None, "animation_seconds": 7}
    _save(state)
    return _public(state)


@router.post("/vote")
def vote(payload: VotePayload):
    state = _load()
    if state.get("status") != "voting" or not state.get("voting", {}).get("open"):
        raise HTTPException(status_code=409, detail="Voting is not open")
    uid = str(payload.user_id or "").strip()
    cid = str(payload.contestant_id or "").strip()
    valid = {str(c.get("user_id")) for c in state.get("contestants", [])}
    if cid not in valid:
        raise HTTPException(status_code=400, detail="Invalid contestant")
    if uid in valid:
        raise HTTPException(status_code=403, detail="Contestants cannot vote in their own debate")
    if not uid:
        raise HTTPException(status_code=400, detail="Your member identity is not available")
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO debate_votes(session_id,user_id,contestant_id,created_at) VALUES(?,?,?,?) ON CONFLICT(session_id,user_id) DO UPDATE SET contestant_id=excluded.contestant_id,created_at=excluded.created_at",
            (state.get("session_id") or "", uid[:80], cid[:80], _iso()),
        )
        con.commit()
        con.close()
    return _public(state, uid)


@router.post("/voting/close")
def close_voting():
    state = _load()
    if state.get("status") not in {"voting", "results_ready"}:
        raise HTTPException(status_code=409, detail="Voting is not open")
    state.setdefault("voting", {})["open"] = False
    state["status"] = "results_ready"
    _save(state)
    return _public(state)


@router.post("/results")
def results(payload: ResultsPayload):
    state = _load()
    if payload.visible:
        state.setdefault("voting", {})["open"] = False
        state["status"] = "results"
        state["results"] = {"visible": True, "reveal_started_at": _iso(), "animation_seconds": max(3, min(int(payload.animation_seconds or 7), 15))}
    else:
        state.setdefault("results", {})["visible"] = False
        state["results"]["reveal_started_at"] = None
        if state.get("status") == "results":
            state["status"] = "results_ready"
    _save(state)
    return _public(state)


@router.post("/end")
def end():
    state = _load()
    state["status"] = "ended"
    state["active_side"] = None
    state["turn_ends_at"] = None
    state.setdefault("voting", {})["open"] = False
    state.setdefault("results", {})["visible"] = False
    _save(state)
    return _public(state)
