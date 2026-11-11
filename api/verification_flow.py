"""Durable, identity-bound onboarding progress; never grants membership itself."""

from __future__ import annotations

from contextlib import contextmanager
import json
import secrets
import sqlite3
import time

from fastapi import HTTPException

from . import member_progress


PROTOCOL_VERSION = 2
SESSION_TTL_SECONDS = 2 * 60 * 60
_REAL_IMAGE = "assets/captcha_images/verify-real-fox.png"
_PACKS = {"biker", "dancer", "daddy", "pup"}


@contextmanager
def _transaction():
    path = member_progress._db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(path), timeout=30)
    con.row_factory = sqlite3.Row
    try:
        con.execute("""CREATE TABLE IF NOT EXISTS verification_sessions (
            session_id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            expires_at REAL NOT NULL,
            payload_json TEXT NOT NULL
        )""")
        con.execute("BEGIN IMMEDIATE")
        yield con
        con.commit()
    except BaseException:
        con.rollback()
        raise
    finally:
        con.close()


def _user_id(user: dict) -> str:
    value = user.get("id") if isinstance(user, dict) else None
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise HTTPException(401, "Signed Telegram identity required")
    raw = str(value)
    if not raw.isascii() or not raw.isdigit() or int(raw) <= 0:
        raise HTTPException(401, "Signed Telegram identity required")
    return str(int(raw))


def _load(con: sqlite3.Connection, user_id: str, session_id: str) -> dict:
    if not isinstance(session_id, str) or not session_id or len(session_id) > 128:
        raise HTTPException(400, "Verification session required")
    row = con.execute("SELECT * FROM verification_sessions WHERE session_id = ?", (session_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "Verification session not found")
    if row["user_id"] != user_id:
        raise HTTPException(403, "Verification session belongs to another user")
    if time.time() >= row["expires_at"]:
        raise HTTPException(410, "Verification session expired; restart verification")
    return json.loads(row["payload_json"])


def _public(state: dict) -> dict:
    result = {key: state[key] for key in (
        "session_id", "protocol_version", "stage", "expires_at", "captcha_tiles",
    )}
    if state["stage"] == "ready":
        result.update(feedback=state["accepted"]["feedback"], selected_pack=state["accepted"]["vote"])
    return result


def start(user: dict) -> dict:
    """Create a fresh challenge for an already authenticated Telegram user."""
    user_id = _user_id(user)
    tiles = [{"id": secrets.token_urlsafe(18), "image": image} for image in (
        _REAL_IMAGE,
        *(f"assets/captcha_images/verify-imposter-{index}.png" for index in range(1, 9)),
    )]
    secrets.SystemRandom().shuffle(tiles)
    state = {
        "session_id": secrets.token_urlsafe(32),
        "protocol_version": PROTOCOL_VERSION,
        "stage": "ethos",
        "expires_at": time.time() + SESSION_TTL_SECONDS,
        "captcha_tiles": tiles,
        "accepted": {},
    }
    with _transaction() as con:
        con.execute('DELETE FROM verification_sessions WHERE expires_at <= ?', (time.time(),))
        con.execute("INSERT INTO verification_sessions VALUES (?, ?, ?, ?)", (
            state["session_id"], user_id, state["expires_at"], json.dumps(state),
        ))
    return _public(state)


def _normalize(step: str, value):
    if step == "ethos":
        if value is not True:
            raise HTTPException(400, "Accept the community ethos to continue")
        return True
    if not isinstance(value, str):
        raise HTTPException(400, "Verification answer must be text")
    if step == "backup":
        if value.strip().upper() != "FOX":
            raise HTTPException(400, "Backup answer is incorrect")
        return "FOX"
    if step == "feedback":
        value = value.strip()
        if not 3 <= len(value) <= 1200:
            raise HTTPException(400, "Feedback must contain 3 to 1200 characters")
    if step == "vote" and value not in _PACKS:
        raise HTTPException(400, "Select a valid pack")
    return value


def advance(user: dict, session_id: str, step: str, value) -> dict:
    """Atomically record one ordered step; accepted answers support retries."""
    user_id = _user_id(user)
    if not isinstance(step, str) or step not in {"ethos", "captcha", "backup", "feedback", "vote"}:
        raise HTTPException(400, "Unknown verification step")
    with _transaction() as con:
        state = _load(con, user_id, session_id)
        normalized = _normalize(step, value)
        accepted = state["accepted"]
        if step in accepted:
            if accepted[step] == normalized:
                return _public(state)
            raise HTTPException(409, "Verification step already recorded")
        if state["stage"] != step:
            raise HTTPException(409, "Complete the current verification step first")
        if step == "captcha":
            tile = next((tile for tile in state["captcha_tiles"] if tile["id"] == normalized), None)
            if tile is None:
                raise HTTPException(400, "Select a tile from this verification challenge")
            next_stage = "feedback" if tile["image"] == _REAL_IMAGE else "backup"
        else:
            next_stage = {"ethos": "captcha", "backup": "feedback", "feedback": "vote", "vote": "ready"}[step]
        accepted[step] = normalized
        state["stage"] = next_stage
        con.execute("UPDATE verification_sessions SET payload_json = ? WHERE session_id = ?", (
            json.dumps(state), session_id,
        ))
        return _public(state)


def completed(user: dict, session_id: str) -> dict:
    """Return trusted final values, allowing final approval delivery to retry."""
    user_id = _user_id(user)
    with _transaction() as con:
        state = _load(con, user_id, session_id)
        accepted = state["accepted"]
        if state["stage"] != "ready" or not {"ethos", "captcha", "feedback", "vote"} <= accepted.keys():
            raise HTTPException(409, "Complete verification before submitting")
        captcha_ok = any(tile["id"] == accepted["captcha"] and tile["image"] == _REAL_IMAGE
                         for tile in state["captcha_tiles"])
        if not captcha_ok and accepted.get("backup") != "FOX":
            raise HTTPException(409, "Complete the backup verification first")
        return {
            "session_id": session_id,
            "protocol_version": PROTOCOL_VERSION,
            "feedback": accepted["feedback"],
            "selected_pack": accepted["vote"],
        }


def invalidate_user(user_id: int | str) -> None:
    """Revoke all sessions when an administrator removes verification."""
    normalized = _user_id({"id": user_id})
    with _transaction() as con:
        con.execute("DELETE FROM verification_sessions WHERE user_id = ?", (normalized,))


def invalidate_session(user_id: int | str, session_id: str) -> None:
    normalized = _user_id({'id': user_id})
    with _transaction() as con:
        con.execute('DELETE FROM verification_sessions WHERE user_id=? AND session_id=?', (normalized, session_id))
