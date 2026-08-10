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

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api/team-game", tags=["team-game"])

DATA_DIR = Path(os.getenv("ALCOVE_TEAM_GAME_DIR", Path.cwd() / "team_game_data"))
UPLOAD_DIR = DATA_DIR / "uploads"
DB_PATH = DATA_DIR / "team_game.sqlite3"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
_LOCK = threading.RLock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    return (dt or _utcnow()).isoformat()


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_messages (id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL, team_id TEXT NOT NULL, user_id TEXT NOT NULL, display_name TEXT NOT NULL, body TEXT NOT NULL, created_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_submissions (id TEXT PRIMARY KEY, session_id TEXT NOT NULL, team_id TEXT NOT NULL, user_id TEXT NOT NULL, display_name TEXT NOT NULL, filename TEXT NOT NULL, original_name TEXT, caption TEXT, created_at TEXT NOT NULL)"
    )
    con.commit()
    return con


def _default_state():
    return {
        "session_id": None,
        "game_type": "drawing",
        "title": "Drawing Challenge",
        "prompt": "Draw your partner",
        "status": "idle",
        "duration_seconds": 300,
        "created_at": None,
        "started_at": None,
        "ends_at": None,
        "participants": [],
        "teams": [],
        "reveal": {"visible": False, "submission_id": None},
    }


def _load_state():
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT payload FROM team_game_state WHERE id=1").fetchone()
        con.close()
        if not row:
            return _default_state()
        try:
            state = json.loads(row["payload"])
        except Exception:
            return _default_state()
        return state


def _save_state(state):
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO team_game_state(id,payload,updated_at) VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_at=excluded.updated_at",
            (json.dumps(state), _iso()),
        )
        con.commit()
        con.close()
    return state


def _clean_user_id(value) -> str:
    text = str(value or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="user_id is required")
    return text[:80]


def _find_participant(state, user_id: str):
    uid = str(user_id)
    return next((p for p in state.get("participants", []) if str(p.get("user_id")) == uid), None)


def _find_team_for_user(state, user_id: str):
    uid = str(user_id)
    for team in state.get("teams", []):
        if any(str(m.get("user_id")) == uid for m in team.get("members", [])):
            return team
    return None


def _public_state(state, user_id: Optional[str] = None):
    out = dict(state)
    out["participants"] = list(state.get("participants", []))
    out["teams"] = list(state.get("teams", []))
    uid = str(user_id or "").strip()
    mine = _find_team_for_user(state, uid) if uid else None
    out["joined"] = bool(uid and _find_participant(state, uid))
    out["my_team"] = mine
    out["team_chat_enabled"] = bool(mine and state.get("status") in {"assigned", "running"})
    with _LOCK:
        con = _conn()
        rows = con.execute(
            "SELECT id,team_id,user_id,display_name,filename,original_name,caption,created_at FROM team_game_submissions WHERE session_id=? ORDER BY created_at DESC",
            (state.get("session_id") or "",),
        ).fetchall()
        con.close()
    out["submissions"] = [dict(r) for r in rows]
    return out


class StartPayload(BaseModel):
    title: str = "Drawing Challenge"
    prompt: str = "Draw your partner"
    duration_seconds: int = 300
    game_type: str = "drawing"


class JoinPayload(BaseModel):
    user_id: str
    display_name: str
    username: Optional[str] = None


class UserPayload(BaseModel):
    user_id: str


class ChatPayload(BaseModel):
    user_id: str
    display_name: str
    body: str


class RevealPayload(BaseModel):
    submission_id: Optional[str] = None
    visible: bool = True


@router.get("/state")
def team_game_state(user_id: Optional[str] = None):
    return _public_state(_load_state(), user_id)


@router.post("/start")
def team_game_start(payload: StartPayload):
    duration = max(30, min(int(payload.duration_seconds or 300), 3600))
    state = _default_state()
    state.update(
        {
            "session_id": str(uuid.uuid4()),
            "game_type": (payload.game_type or "drawing")[:40],
            "title": (payload.title or "Drawing Challenge")[:120],
            "prompt": (payload.prompt or "Draw your partner")[:500],
            "status": "pooling",
            "duration_seconds": duration,
            "created_at": _iso(),
        }
    )
    _save_state(state)
    return _public_state(state)


@router.post("/join")
def team_game_join(payload: JoinPayload):
    state = _load_state()
    if state.get("status") != "pooling":
        raise HTTPException(status_code=409, detail="Registration is not open")
    uid = _clean_user_id(payload.user_id)
    if not _find_participant(state, uid):
        state.setdefault("participants", []).append(
            {
                "user_id": uid,
                "display_name": (payload.display_name or "Member")[:80],
                "username": (payload.username or "")[:80],
                "joined_at": _iso(),
            }
        )
        _save_state(state)
    return _public_state(state, uid)


@router.post("/leave")
def team_game_leave(payload: UserPayload):
    state = _load_state()
    if state.get("status") != "pooling":
        raise HTTPException(status_code=409, detail="You can only leave before teams are assigned")
    uid = _clean_user_id(payload.user_id)
    state["participants"] = [p for p in state.get("participants", []) if str(p.get("user_id")) != uid]
    _save_state(state)
    return _public_state(state, uid)


@router.post("/assign")
def team_game_assign():
    state = _load_state()
    if state.get("status") != "pooling":
        raise HTTPException(status_code=409, detail="Registration must be open")
    people = list(state.get("participants", []))
    if len(people) < 2:
        raise HTTPException(status_code=409, detail="At least two players are required")
    random.shuffle(people)
    teams = []
    for i in range(0, len(people), 2):
        members = people[i : i + 2]
        teams.append({"id": str(uuid.uuid4()), "label": f"Pair {len(teams)+1}", "members": members})
    state["teams"] = teams
    state["status"] = "assigned"
    _save_state(state)
    return _public_state(state)


@router.post("/begin")
def team_game_begin():
    state = _load_state()
    if state.get("status") not in {"assigned", "running"}:
        raise HTTPException(status_code=409, detail="Assign teams first")
    start = _utcnow()
    state["status"] = "running"
    state["started_at"] = _iso(start)
    state["ends_at"] = _iso(start + timedelta(seconds=int(state.get("duration_seconds") or 300)))
    _save_state(state)
    return _public_state(state)


@router.post("/end")
def team_game_end():
    state = _load_state()
    state["status"] = "ended"
    state["reveal"] = {"visible": False, "submission_id": None}
    _save_state(state)
    return _public_state(state)


@router.get("/chat")
def team_game_chat(user_id: str, after_id: int = 0):
    state = _load_state()
    uid = _clean_user_id(user_id)
    team = _find_team_for_user(state, uid)
    if not team or state.get("status") not in {"assigned", "running"}:
        raise HTTPException(status_code=403, detail="You are not assigned to an active team")
    with _LOCK:
        con = _conn()
        rows = con.execute(
            "SELECT id,user_id,display_name,body,created_at FROM team_game_messages WHERE session_id=? AND team_id=? AND id>? ORDER BY id ASC LIMIT 250",
            (state.get("session_id"), team.get("id"), max(0, int(after_id or 0))),
        ).fetchall()
        con.close()
    return {"team": team, "messages": [dict(r) for r in rows]}


@router.post("/chat")
def team_game_chat_post(payload: ChatPayload):
    state = _load_state()
    uid = _clean_user_id(payload.user_id)
    team = _find_team_for_user(state, uid)
    if not team or state.get("status") not in {"assigned", "running"}:
        raise HTTPException(status_code=403, detail="You are not assigned to an active team")
    body = (payload.body or "").strip()
    if not body:
        raise HTTPException(status_code=400, detail="Message is empty")
    if len(body) > 500:
        raise HTTPException(status_code=400, detail="Message is too long")
    with _LOCK:
        con = _conn()
        cur = con.execute(
            "INSERT INTO team_game_messages(session_id,team_id,user_id,display_name,body,created_at) VALUES(?,?,?,?,?,?)",
            (state.get("session_id"), team.get("id"), uid, (payload.display_name or "Member")[:80], body, _iso()),
        )
        message_id = cur.lastrowid
        con.commit()
        con.close()
    return {"ok": True, "id": message_id}


@router.post("/submit")
async def team_game_submit(
    user_id: str = Form(...),
    display_name: str = Form(...),
    caption: str = Form(""),
    file: UploadFile = File(...),
):
    state = _load_state()
    uid = _clean_user_id(user_id)
    team = _find_team_for_user(state, uid)
    if not team or state.get("status") != "running":
        raise HTTPException(status_code=403, detail="You are not in a running drawing challenge")
    content_type = (file.content_type or "").lower()
    if not content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="Please upload an image")
    data = await file.read()
    if len(data) > 12 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Image must be under 12 MB")
    ext = Path(file.filename or "image.jpg").suffix.lower()
    if ext not in {".jpg", ".jpeg", ".png", ".webp", ".gif", ".heic"}:
        ext = ".jpg"
    submission_id = str(uuid.uuid4())
    filename = f"{submission_id}{ext}"
    (UPLOAD_DIR / filename).write_bytes(data)
    created_at = _iso()
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO team_game_submissions(id,session_id,team_id,user_id,display_name,filename,original_name,caption,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                submission_id,
                state.get("session_id"),
                team.get("id"),
                uid,
                display_name[:80],
                filename,
                (file.filename or "")[:180],
                caption[:300],
                created_at,
            ),
        )
        con.commit()
        con.close()
    return {"ok": True, "submission_id": submission_id}


@router.get("/media/{filename}")
def team_game_media(filename: str):
    safe = Path(filename).name
    path = UPLOAD_DIR / safe
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Image not found")
    return FileResponse(path)


@router.post("/reveal")
def team_game_reveal(payload: RevealPayload):
    state = _load_state()
    if not payload.visible:
        state["reveal"] = {"visible": False, "submission_id": None}
        _save_state(state)
        return _public_state(state)
    sid = str(payload.submission_id or "").strip()
    if not sid:
        raise HTTPException(status_code=400, detail="submission_id is required")
    with _LOCK:
        con = _conn()
        row = con.execute(
            "SELECT id FROM team_game_submissions WHERE id=? AND session_id=?",
            (sid, state.get("session_id")),
        ).fetchone()
        con.close()
    if not row:
        raise HTTPException(status_code=404, detail="Submission not found")
    state["reveal"] = {"visible": True, "submission_id": sid}
    _save_state(state)
    return _public_state(state)


@router.get("/obs-state")
def team_game_obs_state():
    state = _load_state()
    reveal = state.get("reveal") or {}
    if not reveal.get("visible") or not reveal.get("submission_id"):
        return {"visible": False, "game": {"title": state.get("title"), "prompt": state.get("prompt")}}
    with _LOCK:
        con = _conn()
        row = con.execute(
            "SELECT id,team_id,user_id,display_name,filename,caption,created_at FROM team_game_submissions WHERE id=?",
            (reveal.get("submission_id"),),
        ).fetchone()
        con.close()
    if not row:
        return {"visible": False}
    item = dict(row)
    item["media_url"] = f"/api/team-game/media/{item['filename']}"
    team = next((t for t in state.get("teams", []) if t.get("id") == item.get("team_id")), None)
    item["team_label"] = team.get("label") if team else ""
    item["team_members"] = [m.get("display_name") for m in (team or {}).get("members", [])]
    return {"visible": True, "game": {"title": state.get("title"), "prompt": state.get("prompt")}, "submission": item}
