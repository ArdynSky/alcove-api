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
BACKGROUND_DIR = DATA_DIR / "backgrounds"
DB_PATH = DATA_DIR / "team_game.sqlite3"
DATA_DIR.mkdir(parents=True, exist_ok=True)
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
BACKGROUND_DIR.mkdir(parents=True, exist_ok=True)
_LOCK = threading.RLock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: Optional[datetime] = None) -> str:
    return (dt or _utcnow()).isoformat()


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _timer_expired(state: dict) -> bool:
    ends = _parse_iso(state.get("ends_at"))
    return bool(ends and _utcnow() >= ends)


def _apply_hold_if_expired(state: dict) -> dict:
    if state.get("status") == "running" and _timer_expired(state):
        state["status"] = "holding"
        _save_state(state)
    return state


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
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_votes (session_id TEXT NOT NULL, user_id TEXT NOT NULL, submission_id TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (session_id, user_id))"
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
        "obs_mode": "hidden",
        "voting": {"open": False, "opened_at": None, "closed_at": None},
        "background_filename": None,
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
        state.setdefault("background_filename", None)
        state.setdefault("obs_mode", "hidden")
        state.setdefault("voting", {"open": False, "opened_at": None, "closed_at": None})
    return _apply_hold_if_expired(state)


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


def _ensure_votes_table(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_votes (session_id TEXT NOT NULL, user_id TEXT NOT NULL, submission_id TEXT NOT NULL, created_at TEXT NOT NULL, PRIMARY KEY (session_id, user_id))"
    )
    con.commit()


def _session_submissions(state):
    with _LOCK:
        con = _conn()
        rows = con.execute(
            "SELECT id,team_id,user_id,display_name,filename,original_name,caption,created_at FROM team_game_submissions WHERE session_id=? ORDER BY created_at ASC",
            (state.get("session_id") or "",),
        ).fetchall()
        con.close()
    return [dict(r) for r in rows]


def _session_votes(state):
    with _LOCK:
        con = _conn()
        _ensure_votes_table(con)
        rows = con.execute(
            "SELECT user_id,submission_id FROM team_game_votes WHERE session_id=?",
            (state.get("session_id") or "",),
        ).fetchall()
        con.close()
    return {str(row["user_id"]): str(row["submission_id"]) for row in rows}


def _decorate_submission(item, state):
    out = dict(item)
    out["media_url"] = f"/api/team-game/media/{out['filename']}" if out.get("filename") else None
    team = next((t for t in state.get("teams", []) if t.get("id") == out.get("team_id")), None)
    out["team_label"] = team.get("label") if team else ""
    out["team_members"] = [m.get("display_name") for m in (team or {}).get("members", [])]
    return out


def _public_state(state, user_id: Optional[str] = None):
    out = dict(state)
    out["participants"] = list(state.get("participants", []))
    out["teams"] = list(state.get("teams", []))
    bg = state.get("background_filename")
    out["background_url"] = f"/api/team-game/background-media/{bg}" if bg else None
    uid = str(user_id or "").strip()
    mine = _find_team_for_user(state, uid) if uid else None
    out["joined"] = bool(uid and _find_participant(state, uid))
    out["my_team"] = mine
    out["team_chat_enabled"] = bool(mine and state.get("status") == "running")
    submissions = _session_submissions(state)
    votes = _session_votes(state)
    counts = {}
    for submission_id in votes.values():
        counts[submission_id] = counts.get(submission_id, 0) + 1
    out["submissions"] = submissions
    out["voting"] = dict(state.get("voting") or {"open": False, "opened_at": None, "closed_at": None})
    out["my_vote"] = votes.get(uid)
    out["vote_counts"] = counts
    out["vote_total"] = len(votes)
    out["obs_mode"] = state.get("obs_mode") or "hidden"
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


class VotePayload(BaseModel):
    user_id: str
    submission_id: str


class ObsFocusPayload(BaseModel):
    mode: Optional[str] = None
    submission_id: Optional[str] = None


@router.get("/state")
def team_game_state(user_id: Optional[str] = None):
    return _public_state(_load_state(), user_id)


@router.post("/start")
def team_game_start(payload: StartPayload):
    duration = max(30, min(int(payload.duration_seconds or 300), 3600))
    previous = _load_state()
    state = _default_state()
    if previous.get("game_type") == (payload.game_type or "drawing"):
        state["background_filename"] = previous.get("background_filename")
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


@router.post("/background")
async def team_game_background(file: UploadFile = File(...)):
    state = _load_state()
    content_type = (file.content_type or "").lower()
    ext = Path(file.filename or "background.mp4").suffix.lower()
    if content_type not in {"video/mp4", "video/webm"} and ext not in {".mp4", ".webm"}:
        raise HTTPException(status_code=400, detail="Please upload an MP4 or WebM video")
    data = await file.read()
    if len(data) > 40 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Background video must be under 40 MB")
    if ext not in {".mp4", ".webm"}:
        ext = ".mp4" if "mp4" in content_type else ".webm"
    old = state.get("background_filename")
    filename = f"{state.get('game_type') or 'activity'}-{uuid.uuid4().hex[:12]}{ext}"
    (BACKGROUND_DIR / filename).write_bytes(data)
    state["background_filename"] = filename
    _save_state(state)
    return _public_state(state)


@router.delete("/background")
def team_game_background_remove():
    state = _load_state()
    old = state.get("background_filename")
    state["background_filename"] = None
    _save_state(state)
    return _public_state(state)


@router.get("/background-media/{filename}")
def team_game_background_media(filename: str):
    safe = Path(filename).name
    path = BACKGROUND_DIR / safe
    if not path.exists() or not path.is_file():
        raise HTTPException(status_code=404, detail="Background video not found")
    return FileResponse(path)


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


@router.post("/hold")
def team_game_hold():
    state = _load_state()
    if state.get("status") not in {"running", "assigned", "holding"}:
        raise HTTPException(status_code=409, detail="Start the drawing challenge first")
    state["status"] = "holding"
    _save_state(state)
    return _public_state(state)


@router.post("/end")
def team_game_end():
    state = _load_state()
    state["status"] = "ended"
    state["reveal"] = {"visible": False, "submission_id": None}
    state["obs_mode"] = "hidden"
    state["voting"] = {"open": False, "opened_at": None, "closed_at": None}
    _save_state(state)
    return _public_state(state)


@router.get("/chat")
def team_game_chat(user_id: str, after_id: int = 0):
    state = _load_state()
    uid = _clean_user_id(user_id)
    team = _find_team_for_user(state, uid)
    if not team or state.get("status") != "running":
        raise HTTPException(status_code=403, detail="Private chat opens when the activity starts")
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
    if not team or state.get("status") != "running":
        raise HTTPException(status_code=403, detail="Private chat opens when the activity starts")
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
    if not team or state.get("status") != "running" or _timer_expired(state):
        if state.get("status") == "running" and _timer_expired(state):
            state["status"] = "holding"
            _save_state(state)
        raise HTTPException(status_code=403, detail="Uploads are closed while the host collects artwork")
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
        state["obs_mode"] = "gallery" if state.get("status") == "voting" else "hidden"
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
    state["obs_mode"] = "single"
    _save_state(state)
    return _public_state(state)


@router.post("/voting/open")
def team_game_voting_open():
    state = _load_state()
    if state.get("status") not in {"holding", "voting", "assigned", "running"}:
        raise HTTPException(status_code=409, detail="Collect artwork before opening votes")
    submissions = _session_submissions(state)
    if len(submissions) < 1:
        raise HTTPException(status_code=409, detail="No drawings have been submitted yet")
    now = _iso()
    voting = dict(state.get("voting") or {})
    voting.update({"open": True, "opened_at": now, "closed_at": None})
    state["status"] = "voting"
    state["voting"] = voting
    state["obs_mode"] = "gallery"
    _save_state(state)
    return _public_state(state)


@router.post("/voting/close")
def team_game_voting_close():
    state = _load_state()
    if state.get("status") != "voting":
        raise HTTPException(status_code=409, detail="Voting is not open")
    voting = dict(state.get("voting") or {})
    voting.update({"open": False, "closed_at": _iso()})
    state["voting"] = voting
    _save_state(state)
    return _public_state(state)


@router.post("/vote")
def team_game_vote(payload: VotePayload):
    state = _load_state()
    voting = state.get("voting") or {}
    if state.get("status") != "voting" or not voting.get("open"):
        raise HTTPException(status_code=409, detail="Voting is closed")
    uid = _clean_user_id(payload.user_id)
    sid = str(payload.submission_id or "").strip()
    submissions = _session_submissions(state)
    if not any(str(item.get("id")) == sid for item in submissions):
        raise HTTPException(status_code=404, detail="Drawing not found")
    with _LOCK:
        con = _conn()
        _ensure_votes_table(con)
        con.execute(
            "INSERT INTO team_game_votes(session_id,user_id,submission_id,created_at) VALUES(?,?,?,?) ON CONFLICT(session_id,user_id) DO UPDATE SET submission_id=excluded.submission_id, created_at=excluded.created_at",
            (state.get("session_id"), uid, sid, _iso()),
        )
        con.commit()
        con.close()
    return _public_state(state, uid)


@router.post("/obs-focus")
def team_game_obs_focus(payload: ObsFocusPayload):
    state = _load_state()
    mode = str(payload.mode or "").strip().lower()
    sid = str(payload.submission_id or "").strip()
    if mode not in {"", "hidden", "gallery", "single"}:
        raise HTTPException(status_code=400, detail="mode must be gallery, single, or hidden")
    if mode == "single" or (not mode and sid):
        if not sid:
            raise HTTPException(status_code=400, detail="submission_id is required")
        if not any(str(item.get("id")) == sid for item in _session_submissions(state)):
            raise HTTPException(status_code=404, detail="Drawing not found")
        state["obs_mode"] = "single"
        state["reveal"] = {"visible": True, "submission_id": sid}
    elif mode == "gallery":
        state["obs_mode"] = "gallery"
        state["reveal"] = {"visible": False, "submission_id": None}
    else:
        state["obs_mode"] = "hidden"
        state["reveal"] = {"visible": False, "submission_id": None}
    _save_state(state)
    return _public_state(state)


@router.get("/obs-state")
def team_game_obs_state():
    state = _load_state()
    submissions = [_decorate_submission(item, state) for item in _session_submissions(state)]
    reveal = state.get("reveal") or {}
    mode = str(state.get("obs_mode") or "hidden")
    if mode == "gallery":
        focused = None
    elif reveal.get("visible") and reveal.get("submission_id"):
        mode = "single"
        focused = next((item for item in submissions if str(item.get("id")) == str(reveal.get("submission_id") or "")), None)
    elif state.get("status") == "voting" and mode == "hidden":
        mode = "gallery"
        focused = None
    else:
        focused = next((item for item in submissions if str(item.get("id")) == str(reveal.get("submission_id") or "")), None)
    visible = bool(submissions) and mode in {"gallery", "single"}
    return {
        "visible": visible,
        "mode": mode if visible else "hidden",
        "game": {"title": state.get("title"), "prompt": state.get("prompt")},
        "voting": dict(state.get("voting") or {}),
        "submissions": submissions,
        "submission": focused if mode == "single" else None,
    }


class ActivityTemplatePayload(BaseModel):
    name: str
    title: str = "Drawing Challenge"
    prompt: str = "Draw your partner"
    duration_seconds: int = 300
    game_type: str = "drawing"


def _ensure_template_table(con):
    con.execute(
        "CREATE TABLE IF NOT EXISTS team_game_templates (id TEXT PRIMARY KEY, name TEXT NOT NULL, title TEXT NOT NULL, prompt TEXT NOT NULL, duration_seconds INTEGER NOT NULL, game_type TEXT NOT NULL, background_filename TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    con.commit()


def _template_dict(row):
    item = dict(row)
    bg = item.get("background_filename")
    item["background_url"] = f"/api/team-game/background-media/{bg}" if bg else None
    return item


@router.get("/templates")
def team_game_templates():
    with _LOCK:
        con = _conn()
        _ensure_template_table(con)
        rows = con.execute("SELECT * FROM team_game_templates ORDER BY updated_at DESC, name COLLATE NOCASE").fetchall()
        con.close()
    return {"templates": [_template_dict(r) for r in rows]}


@router.post("/templates")
def team_game_template_create(payload: ActivityTemplatePayload):
    state = _load_state()
    template_id = str(uuid.uuid4())
    now = _iso()
    with _LOCK:
        con = _conn()
        _ensure_template_table(con)
        con.execute(
            "INSERT INTO team_game_templates(id,name,title,prompt,duration_seconds,game_type,background_filename,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                template_id,
                (payload.name or payload.title or "Saved activity")[:100],
                (payload.title or "Drawing Challenge")[:120],
                (payload.prompt or "Draw your partner")[:500],
                max(30, min(int(payload.duration_seconds or 300), 3600)),
                (payload.game_type or "drawing")[:40],
                state.get("background_filename"),
                now,
                now,
            ),
        )
        con.commit()
        row = con.execute("SELECT * FROM team_game_templates WHERE id=?", (template_id,)).fetchone()
        con.close()
    return _template_dict(row)


@router.put("/templates/{template_id}")
def team_game_template_update(template_id: str, payload: ActivityTemplatePayload):
    state = _load_state()
    with _LOCK:
        con = _conn()
        _ensure_template_table(con)
        exists = con.execute("SELECT id FROM team_game_templates WHERE id=?", (template_id,)).fetchone()
        if not exists:
            con.close()
            raise HTTPException(status_code=404, detail="Saved activity not found")
        con.execute(
            "UPDATE team_game_templates SET name=?,title=?,prompt=?,duration_seconds=?,game_type=?,background_filename=?,updated_at=? WHERE id=?",
            (
                (payload.name or payload.title or "Saved activity")[:100],
                (payload.title or "Drawing Challenge")[:120],
                (payload.prompt or "Draw your partner")[:500],
                max(30, min(int(payload.duration_seconds or 300), 3600)),
                (payload.game_type or "drawing")[:40],
                state.get("background_filename"),
                _iso(),
                template_id,
            ),
        )
        con.commit()
        row = con.execute("SELECT * FROM team_game_templates WHERE id=?", (template_id,)).fetchone()
        con.close()
    return _template_dict(row)


@router.delete("/templates/{template_id}")
def team_game_template_delete(template_id: str):
    with _LOCK:
        con = _conn()
        _ensure_template_table(con)
        con.execute("DELETE FROM team_game_templates WHERE id=?", (template_id,))
        con.commit()
        con.close()
    return {"ok": True}


@router.post("/templates/{template_id}/activate")
def team_game_template_activate(template_id: str):
    with _LOCK:
        con = _conn()
        _ensure_template_table(con)
        row = con.execute("SELECT * FROM team_game_templates WHERE id=?", (template_id,)).fetchone()
        con.close()
    if not row:
        raise HTTPException(status_code=404, detail="Saved activity not found")
    item = dict(row)
    state = _default_state()
    state.update(
        {
            "session_id": str(uuid.uuid4()),
            "game_type": item.get("game_type") or "drawing",
            "title": item.get("title") or "Drawing Challenge",
            "prompt": item.get("prompt") or "Draw your partner",
            "status": "pooling",
            "duration_seconds": max(30, min(int(item.get("duration_seconds") or 300), 3600)),
            "created_at": _iso(),
            "background_filename": item.get("background_filename"),
        }
    )
    _save_state(state)
    return _public_state(state)
