"""Signed member access with durable verification evidence, independent of progress."""
from __future__ import annotations

import datetime
import hmac
import json
import os
import sqlite3
import threading
from contextlib import closing, contextmanager
from pathlib import Path

from fastapi import APIRouter, Header, HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

router = APIRouter(prefix="/api/membership", tags=["membership"])
OWNERS = {"8385145826", "5737853594"}
_LOCK = threading.RLock()


@contextmanager
def connection():
    from .member_progress import _db_path
    path = _db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(str(path))) as con, con:
        con.execute("""CREATE TABLE IF NOT EXISTS verified_member_registry (
            user_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, updated_at TEXT NOT NULL
        )""")
        yield con


def remember_member(user, *, source="bot_sync", claimed=False, insert_only=False):
    uid = str(user.get("user_id") or user.get("id") or "")
    if not uid.isdigit():
        return
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    with _LOCK, connection() as con:
        row = con.execute("SELECT payload_json FROM verified_member_registry WHERE user_id=?", (uid,)).fetchone()
        old = json.loads(row[0]) if row else {}
        if row and insert_only:
            changed = False
            if claimed and not old.get('verification_pack_claimed'):
                old['verification_pack_claimed'] = True
                changed = True
            if not old.get('fox_form') and user.get('pack_key'):
                old['fox_form'] = user['pack_key']
                changed = True
            if changed:
                con.execute('UPDATE verified_member_registry SET payload_json=?, updated_at=? WHERE user_id=?', (json.dumps(old), now, uid))
            return
        historical_claim = False
        if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='member_progress'").fetchone():
            stored = con.execute('SELECT payload_json FROM member_progress WHERE user_id=?', (uid,)).fetchone()
            if stored:
                profile = json.loads(stored[0])
                historical_claim = bool((profile.get('claimReceipts') or {}).get('verification_pack')) or any(
                    p.get('opened') and (p.get('sourceKey') or p.get('id')) == 'verification_pack'
                    for p in (profile.get('pendingRewards') or []) if isinstance(p, dict))
        pack = user.get("fox_form") or user.get("selected_pack") or user.get("pack_key") or old.get("fox_form")
        data = {**old, "user_id": int(uid), "source": "verified_member_registry",
                "verification_evidence": old.get("verification_evidence") or source,
                "verification_confirmed_at": old.get("verification_confirmed_at") or now,
                "verified_at": user.get("verified_at") or old.get("verified_at"),
                "username": user.get("username") or old.get("username") or "",
                "display_name": user.get("display_name") or old.get("display_name") or "",
                "fox_form": pack, "verification_pack_claimed": bool(claimed or historical_claim or old.get("verification_pack_claimed"))}
        for key in ("joined_at", "first_seen"):
            data[key] = user.get(key) or old.get(key)
        if data != old:
            con.execute("INSERT OR REPLACE INTO verified_member_registry VALUES (?, ?, ?)", (uid, json.dumps(data), now))


def apply_recoveries():
    raw = os.getenv("ALCOVE_VERIFIED_MEMBER_RECOVERIES", "[]")
    try:
        recoveries = json.loads(raw)
    except (ValueError, TypeError):
        raise HTTPException(503, "Verified-member recovery configuration is invalid")
    for user in recoveries if isinstance(recoveries, list) else []:
        if not isinstance(user, dict) or not str(user.get("reason") or "").strip():
            continue
        remember_member(user, source="owner_confirmed_previous_verification", claimed=user.get("pack_claimed") is True, insert_only=True)


def retain_verified_members(users):
    for user in users:
        if str(user.get("verified_at") or "").strip():
            remember_member(user)


def find_retained_member(user_id=None, username=None):
    from . import main
    apply_recoveries()
    uid = str(user_id or "")
    name = str(username or "").strip().lstrip("@").lower()
    # A completed bot verification is evidence; merely choosing a pack is not.
    for entry in main.miniapp_verifications:
        if entry.get("status") != "completed":
            continue
        if (uid and str(entry.get("user_id")) == uid) or (not uid and name and str(entry.get("username") or "").lower() == name):
            remember_member({**entry, "verified_at": entry.get("completed_at")}, source="completed_verification")
    with _LOCK, connection() as con:
        if uid:
            rows = con.execute("SELECT payload_json FROM verified_member_registry WHERE user_id=?", (uid,)).fetchall()
        elif name:
            rows = con.execute("SELECT payload_json FROM verified_member_registry").fetchall()
        else:
            return None
    matches = [json.loads(row[0]) for row in rows]
    if not uid:
        matches = [u for u in matches if str(u.get("username") or "").lower() == name]
    return matches[0] if len(matches) == 1 else None


def retained_members():
    apply_recoveries()
    with _LOCK, connection() as con:
        return [json.loads(row[0]) for row in con.execute("SELECT payload_json FROM verified_member_registry")]


def authorize_signed(init_data=None, authorization=None):
    from . import main
    if init_data:
        user = main.verify_telegram_init_data(init_data)
    elif authorization:
        from .live_room_test import identity_from_authorization
        user = identity_from_authorization(authorization)
        if not user:
            raise HTTPException(401, "Open the app again from Telegram")
    else:
        raise HTTPException(401, "Signed Telegram membership is required")
    return authorize_identity(user)


def authorize_identity(user):
    from . import main
    uid = str(user.get("id") or user.get("user_id") or "")
    name = str(user.get("username") or "").strip().lstrip("@").lower()
    if uid in OWNERS:
        return user, "owner"
    if name and name in main.load_tester_usernames():
        return user, "tester"
    member = main.find_verified_alcove_user(user_id=int(uid)) if uid.isdigit() else None
    if not member:
        raise HTTPException(403, "Verified Alcove membership required")
    if member.get("source") == "verified_member_registry" and name and member.get("username") != name:
        remember_member({**member, "username": name})
    return user, "verified"


@router.post("/access")
def membership_access(x_telegram_init_data: str | None = Header(default=None), authorization: str | None = Header(default=None)):
    user, reason = authorize_signed(x_telegram_init_data, authorization)
    return {"status": "ok", "allowed": True, "verified": reason == "verified", "access_reason": reason,
            "user_id": str(user.get("id") or user.get("user_id"))}


MEMBER_ROUTES = {
    "/api/alcove-users", "/api/members/public-profile", "/api/verification-pack/eligibility",
    "/api/pulse-status", "/api/pulse-questions", "/api/pulse-question-suggestions", "/api/pulse-question-suggestions/status",
    "/api/pulse-entry", "/api/pulse-red-activate", "/api/spotlight-entry", "/api/spotlight-status",
    "/api/wheel-entry", "/api/review", "/api/stream-comment", "/api/wheel-reaction", "/api/room-qa",
    "/api/room-poll/vote", "/api/room-game/join", "/api/room-game/leave", "/api/room-game/team-message",
    "/api/room-game/team-media", "/api/asmr-entry", "/api/story-entry", "/api/members/profile",
    "/api/custom-packs/pending", "/api/custom-packs/claim", "/api/home-alerts", "/api/home-alerts/consume",
    "/api/exp-grants", "/api/debate/join", "/api/debate/leave", "/api/debate/vote", "/api/debate/audience/submit",
    "/api/debate/presence/heartbeat", "/api/debate/presence/leave", "/api/team-game/join", "/api/team-game/leave",
    "/api/team-game/chat", "/api/team-game/submit", "/api/team-game/vote",
}
MEMBER_PREFIXES = ("/api/daily-checkin", "/api/archive/", "/api/pulse-assignments/", "/api/pulse-receipts/", "/api/exp-grants/")


class MemberAccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path.rstrip("/")
        if request.method == "OPTIONS" or not (path in MEMBER_ROUTES or any(path.startswith(p) for p in MEMBER_PREFIXES)):
            return await call_next(request)
        from . import main
        # Existing authenticated bot and admin workflows retain their permissions.
        secret = request.headers.get("X-Bot-Sync-Secret") or request.headers.get("X-Alcove-Admin")
        if secret and main.BOT_SYNC_SECRET and hmac.compare_digest(secret, main.BOT_SYNC_SECRET):
            return await call_next(request)
        body = {}
        if "application/json" in request.headers.get("content-type", ""):
            try:
                body = await request.json()
                if not isinstance(body, dict): body = {}
            except ValueError:
                pass
        try:
            user, _reason = authorize_signed(request.headers.get("X-Telegram-Init-Data") or body.get("init_data"), request.headers.get("authorization"))
            uid = str(user.get("id") or user.get("user_id"))
            username = str(user.get("username") or "").lstrip("@").lower()
            actor_sources = (request.query_params,) if path == "/api/stream-comment" and request.headers.get("authorization") and not request.headers.get("X-Telegram-Init-Data") else (request.query_params, body)
            if path in {"/api/members/public-profile", "/api/alcove-users"}:
                actor_sources = ()  # These read other community members, not the caller's own records.
            elif isinstance(body.get("data"), dict):
                actor_sources = (*actor_sources, body["data"])
            for data in actor_sources:
                for key in ("user_id", "sender_user_id", "nominator_user_id", "responder_user_id"):
                    if data.get(key) is not None and str(data.get(key)) != uid:
                        raise HTTPException(403, "Member identity does not match signed Telegram user")
                for key in ("username", "sender_username", "nominator_username", "responder_username"):
                    if data.get(key) and str(data.get(key)).lstrip("@").lower() != username:
                        raise HTTPException(403, "Member username does not match signed Telegram user")
        except HTTPException as error:
            return JSONResponse({"detail": error.detail}, status_code=error.status_code)
        return await call_next(request)


def remember_pack_claim(user_id):
    with _LOCK, connection() as con:
        row = con.execute("SELECT payload_json FROM verified_member_registry WHERE user_id=?", (str(user_id),)).fetchone()
        if not row:
            return
        data = json.loads(row[0])
        if not data.get("verification_pack_claimed"):
            data["verification_pack_claimed"] = True
            con.execute("UPDATE verified_member_registry SET payload_json=? WHERE user_id=?", (json.dumps(data), str(user_id)))
