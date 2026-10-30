"""Admin bulk progression resets. Content and moderation queues are never deleted."""
from __future__ import annotations

import json
import time
import uuid
from contextlib import closing, contextmanager
from functools import wraps
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import member_progress as profiles

router = APIRouter(prefix="/api/members/progress-reset", tags=["members"])


@contextmanager
def _conn():
    with closing(profiles._conn()) as con, con:
        _ensure_tables(con)
        yield con


def _ensure_tables(con):
    con.executescript("""
        CREATE TABLE IF NOT EXISTS progression_reset_batches (
            id TEXT PRIMARY KEY, created_at REAL NOT NULL, payload_json TEXT NOT NULL,
            executed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS progression_member_resets (
            user_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
            reset_at TEXT NOT NULL, reset_token TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS progression_reset_backups (
            batch_id TEXT NOT NULL, user_id TEXT NOT NULL, payload_json TEXT NOT NULL,
            PRIMARY KEY(batch_id, user_id)
        );
        CREATE TABLE IF NOT EXISTS progression_reset_approvals (
            batch_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS progression_reset_config (
            key TEXT PRIMARY KEY, value_json TEXT NOT NULL
        );
    """)


def serialized_change(fn):
    @wraps(fn)
    def guarded(*args, **kwargs):
        with profiles._LOCK:
            return fn(*args, **kwargs)
    return guarded


def _directory(con) -> dict[str, dict]:
    from . import main
    members = {}
    for user in main.get_verified_alcove_users():
        uid = str(user.get("user_id") or user.get("id") or "")
        if uid.isdigit():
            members[uid] = {"user_id": uid, "username": str(user.get("username") or "").lstrip("@"),
                            "display_name": str(user.get("display_name") or "")}
    for row in con.execute("SELECT user_id FROM member_progress"):
        uid = str(row[0])
        if uid.isdigit():
            members.setdefault(uid, {"user_id": uid, "username": "", "display_name": ""})
    # Members who submitted content may only have an older local profile.
    for rows, pairs in [(main.pulse_question_suggestions, [("user_id", "username")]),
                        (main.spotlight_entries, [("nominator_user_id", "nominator_username"), ("nominee_user_id", "nominee_username")])]:
        for entry in rows:
            for id_key, name_key in pairs:
                uid = str(entry.get(id_key) or "")
                if uid.isdigit():
                    members.setdefault(uid, {"user_id": uid, "username": str(entry.get(name_key) or "").lstrip("@"), "display_name": ""})
    return members


def _saved_exclusions(con) -> list[dict]:
    row = con.execute("SELECT value_json FROM progression_reset_config WHERE key='exclusions'").fetchone()
    return json.loads(row[0]) if row else []


class PreviewPayload(BaseModel):
    admin_secret: str
    excluded_usernames: list[str] | None = None


class ExecutePayload(BaseModel):
    admin_secret: str
    preview_id: str
    confirmation: str


@router.get("/config")
def reset_config(admin_secret: str):
    profiles._admin(admin_secret)
    with profiles._LOCK, _conn() as con:
        members = _directory(con)
        excluded = [members.get(item["user_id"], item) for item in _saved_exclusions(con)]
    return {"status": "ok", "excluded": excluded}


@router.post("/preview")
def preview_reset(payload: PreviewPayload):
    profiles._admin(payload.admin_secret)
    with profiles._LOCK, _conn() as con:
        members = _directory(con)
        if payload.excluded_usernames is None:
            excluded = [members.get(item["user_id"], item) for item in _saved_exclusions(con)]
        else:
            excluded = []
            unknown = []
            for raw in payload.excluded_usernames:
                name = str(raw).strip().lstrip("@").lower()
                if not name:
                    continue
                matches = [item for item in members.values() if item["username"].lower() == name or item["user_id"] == name]
                if len(matches) != 1:
                    unknown.append(raw)
                elif matches[0] not in excluded:
                    excluded.append(matches[0])
            if unknown:
                raise HTTPException(400, "Could not uniquely identify excluded accounts: " + ", ".join(unknown))
        excluded_ids = {item["user_id"] for item in excluded}
        reset = [members[uid] for uid in sorted(members) if uid not in excluded_ids]
        preview_id = uuid.uuid4().hex
        result = {"status": "ok", "preview_id": preview_id, "reset_count": len(reset),
                  "excluded_count": len(excluded), "members": reset, "excluded": excluded}
        con.execute("DELETE FROM progression_reset_batches WHERE executed_at IS NULL AND created_at < ?", (time.time() - 900,))
        con.execute("INSERT INTO progression_reset_batches VALUES (?, ?, ?, NULL)",
                    (preview_id, time.time(), json.dumps(result)))
        con.commit()
    return result


def fresh_profile(current: dict, reset_at: str, token: str) -> dict:
    return profiles.normalize_profile({
        "level": 1, "exp": 0, "expMax": 200, "expView": {"level": 1, "exp": 0},
        "feed": {"color": "base", "skin": "none", "stamp": "none", "cornerSticker": "none", "backdrop": "none"},
        "title": "none", "fox": current.get("fox") or {},
        "owned": {"feedColors": ["base"], "feedSkins": ["none"], "feedStickers": ["none"],
                  "feedBackdrops": ["none"], "titles": ["none", "night_owl"],
                  "foxItems": [item for item in (current.get("owned") or {}).get("foxItems", []) if "_starter" in str(item)]},
        "stats": {key: 0 for key in (current.get("stats") or {})},
        "memberSince": current.get("memberSince") or "", "updated_at": reset_at,
        "progressionResetAt": reset_at, "progressionResetToken": token,
        "progressionAdminResetToken": token,
    })


def _blocked_approvals(con) -> dict[str, list[str]]:
    from . import main
    blocked = {
        "pulse": {str(e.get("id")) for e in main.pulse_question_suggestions if e.get("status") == "approved" or e.get("progression_approved_at")},
        "spotlight": {str(e.get("id")) for e in main.spotlight_entries if e.get("status") == "approved" or e.get("published_at") or e.get("progression_approved_at")},
    }
    # Older archived approvals may no longer be in the runtime queue.
    for kind, table, column in [("pulse", "pulse_archive_questions", "suggestion_id"), ("spotlight", "spotlight_archive", "spotlight_id")]:
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            where = " WHERE status='approved'" if kind == "pulse" else ""
            blocked[kind].update(str(row[0]) for row in con.execute(f"SELECT {column} FROM {table}{where}"))
    return {key: sorted(ids) for key, ids in blocked.items()}


@router.post("/execute")
def execute_reset(payload: ExecutePayload):
    profiles._admin(payload.admin_secret)
    if payload.confirmation != "RESET PROGRESS":
        raise HTTPException(400, "Type RESET PROGRESS to confirm.")
    with profiles._LOCK, _conn() as con:
        con.execute("BEGIN IMMEDIATE")
        row = con.execute("SELECT created_at, payload_json, executed_at FROM progression_reset_batches WHERE id=?", (payload.preview_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Reset preview not found. Preview again.")
        batch = json.loads(row[1])
        if row[2]:
            return {"status": "ok", "already_done": True, "reset_count": batch["reset_count"], "reset_at": row[2]}
        if row[0] < time.time() - 900:
            raise HTTPException(409, "Reset preview expired. Preview again.")
        members = _directory(con)
        excluded_ids = {item["user_id"] for item in batch["excluded"]}
        if set(members) - excluded_ids != {item["user_id"] for item in batch["members"]}:
            raise HTTPException(409, "Member list changed. Preview again before resetting.")
        reset_at = profiles._now()
        token = f"bulk-{int(profiles._parse_ts(reset_at) * 1000)}-{payload.preview_id}"
        con.execute("INSERT INTO progression_reset_approvals VALUES (?, ?)",
                    (payload.preview_id, json.dumps(_blocked_approvals(con))))
        for member in batch["members"]:
            uid = member["user_id"]
            stored = con.execute("SELECT payload_json FROM member_progress WHERE user_id=?", (uid,)).fetchone()
            current = json.loads(stored[0]) if stored else {}
            con.execute("INSERT INTO progression_reset_backups VALUES (?, ?, ?)", (payload.preview_id, uid, json.dumps(current)))
            fresh = fresh_profile(current, reset_at, token)
            con.execute("INSERT OR REPLACE INTO member_progress VALUES (?, ?, ?)", (uid, json.dumps(fresh), reset_at))
            con.execute("INSERT OR REPLACE INTO progression_member_resets VALUES (?, ?, ?, ?)", (uid, payload.preview_id, reset_at, token))
        con.execute("INSERT OR REPLACE INTO progression_reset_config VALUES ('exclusions', ?)", (json.dumps(batch["excluded"]),))
        con.execute("UPDATE progression_reset_batches SET payload_json=?, executed_at=? WHERE id=?", (json.dumps(batch), reset_at, payload.preview_id))
        con.commit()
    return {"status": "ok", "reset_count": batch["reset_count"], "excluded_count": len(excluded_ids), "reset_at": reset_at}


def reset_checkpoint(user_id: Any) -> dict | None:
    if not str(user_id or "").isdigit():
        return None
    with profiles._LOCK, _conn() as con:
        row = con.execute("""SELECT r.reset_at, r.reset_token, a.payload_json FROM progression_member_resets r
                            JOIN progression_reset_approvals a ON a.batch_id=r.batch_id WHERE r.user_id=?""", (str(user_id),)).fetchone()
    if not row:
        return None
    return {"reset_at": row[0], "reset_token": row[1], "blocked_approvals": {kind: set(ids) for kind, ids in json.loads(row[2]).items()}}


def approval_metadata(user_id: Any, kind: str, entry: dict, *, checkpoint: dict | None | bool = False) -> dict:
    checkpoint = reset_checkpoint(user_id) if checkpoint is False else checkpoint
    approved_at = entry.get("progression_approved_at") or entry.get("reviewed_at") or entry.get("published_at")
    approved = str(entry.get("status") or entry.get("review_status") or "").lower() == "approved"
    eligible = approved
    if checkpoint:
        sid = str(entry.get("suggestion_id") or entry.get("spotlight_id") or entry.get("id"))
        eligible = approved and sid not in checkpoint["blocked_approvals"].get(kind, []) and profiles._parse_ts(approved_at) > profiles._parse_ts(checkpoint["reset_at"])
    return {"progression_eligible": bool(eligible), "progression_approved_at": approved_at}
