from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/live-room-test", tags=["live-room-test"])

OWNER_USER_IDS = {"8385145826", "5737853594"}
HANDOFF_TTL_SECONDS = 120
SESSION_TTL_SECONDS = 4 * 60 * 60
PURPOSE_HANDOFF = "handoff"
PURPOSE_SESSION = "session"

_LOCK = threading.RLock()


def _data_dir() -> Path:
    for key in ("ALCOVE_STATE_DB_PATH", "ALCOVE_RUNTIME_STATE_PATH", "FOX_LOGS_DB_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw).expanduser().resolve().parent
    return Path.cwd()


DB_PATH = Path(os.getenv("LIVE_ROOM_TEST_DB", str(_data_dir() / "live_room_test.sqlite3")))
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


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE IF NOT EXISTS live_room_test_tokens ("
        "id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE, purpose TEXT NOT NULL, "
        "user_json TEXT NOT NULL, created_at TEXT NOT NULL, expires_at TEXT NOT NULL, used_at TEXT)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS live_room_test_events ("
        "id TEXT PRIMARY KEY, user_id TEXT NOT NULL, username TEXT, event_type TEXT NOT NULL, "
        "created_at TEXT NOT NULL, extra_json TEXT NOT NULL DEFAULT '{}')"
    )
    con.commit()
    return con


def _secret() -> str:
    return (
        os.getenv("LIVE_ROOM_TEST_HANDOFF_SECRET", "").strip()
        or os.getenv("BOT_SYNC_SECRET", "").strip()
        or "alcove-live-room-test-dev"
    )


def _hash_token(token: str) -> str:
    return hashlib.sha256(f"{_secret()}:{token}".encode("utf-8")).hexdigest()


def _host_user_ids() -> set[str]:
    ids = set(OWNER_USER_IDS)
    extra = os.getenv("LIVE_ROOM_TEST_HOST_USER_IDS", "")
    for part in extra.split(","):
        value = part.strip()
        if value:
            ids.add(value)
    return ids


def _is_host(user_id) -> bool:
    return str(user_id or "") in _host_user_ids()


def _participant_url() -> str:
    return os.getenv("WHEREBY_LIVE_ROOM_TEST_PARTICIPANT_URL", "").strip()


def _host_url() -> str:
    return os.getenv("WHEREBY_LIVE_ROOM_TEST_HOST_URL", "").strip()


def _public_base() -> str:
    return (
        os.getenv("LIVE_ROOM_TEST_PUBLIC_BASE", "").strip().rstrip("/")
        or "https://ardyn-alcove.com"
    )


def _telegram_app_url() -> str:
    return os.getenv("LIVE_ROOM_TEST_TELEGRAM_APP_URL", "").strip()


def _display_name(user: dict) -> str:
    first = str(user.get("first_name") or "").strip()
    last = str(user.get("last_name") or "").strip()
    combined = " ".join(part for part in (first, last) if part).strip()
    return (
        combined
        or str(user.get("display_name") or "").strip()
        or str(user.get("username") or "").strip()
        or "Member"
    )


def _public_user(user: dict) -> dict:
    user_id = user.get("id") or user.get("user_id")
    return {
        "id": int(user_id) if str(user_id).isdigit() else user_id,
        "user_id": str(user_id),
        "username": str(user.get("username") or "").lstrip("@") or None,
        "first_name": user.get("first_name") or None,
        "last_name": user.get("last_name") or None,
        "display_name": _display_name(user),
    }


def _role_for(user: dict) -> str:
    return "host" if _is_host(user.get("id") or user.get("user_id")) else "participant"


def _has_room_key(query: dict) -> bool:
    return bool(str(query.get("roomKey") or query.get("roomkey") or "").strip())


def _drop_host_keys(query: dict) -> dict:
    cleaned = dict(query)
    cleaned.pop("roomKey", None)
    cleaned.pop("roomkey", None)
    return cleaned


def _apply_embed_defaults(query: dict, display_name: str, user: dict, *, host: bool) -> dict:
    next_query = dict(query)
    if not host:
        next_query = _drop_host_keys(next_query)
        next_query.setdefault("screenshare", "off")
        next_query.setdefault("locking", "off")
        next_query.setdefault("moreButton", "off")
        next_query.setdefault("people", "off")
        next_query.setdefault("breakout", "off")
        next_query.setdefault("topToolbar", "off")
    next_query.setdefault("displayName", display_name or "Member")
    next_query.setdefault("embed", "")
    next_query.setdefault("skipMediaPermissionPrompt", "")
    next_query.setdefault("precallReview", "off")
    next_query.setdefault("precallCeremony", "off")
    next_query.setdefault("chat", "off")
    next_query.setdefault("logo", "off")
    next_query.setdefault("floatSelf", "")
    next_query.setdefault("pipButton", "off")
    next_query.setdefault("leaveButton", "off")
    user_id = str(user.get("id") or user.get("user_id") or "").strip()
    if user_id.isalnum() and len(user_id) <= 36:
        next_query.setdefault("externalId", user_id)
    next_query.pop("audio", None)
    next_query.pop("video", None)
    return next_query


def _build_embed_url(raw_url: str, display_name: str, user: dict, *, host: bool) -> str:
    raw = str(raw_url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    query = _apply_embed_defaults(
        dict(parse_qsl(parsed.query, keep_blank_values=True)),
        display_name,
        user,
        host=host,
    )
    return urlunparse(parsed._replace(query=urlencode(query)))


def _embed_for(user: dict, role: str) -> dict:
    display_name = _display_name(user)
    raw_host = _host_url()
    raw_participant = _participant_url() or raw_host
    participant_url = _build_embed_url(raw_participant, display_name, user, host=False)
    host_url = _build_embed_url(raw_host, display_name, user, host=True) if raw_host else ""
    is_host = role == "host" and bool(host_url)
    embed_url = host_url if is_host else participant_url
    host_query = dict(parse_qsl(urlparse(embed_url or "").query, keep_blank_values=True))
    return {
        "role": "host" if is_host else "participant",
        "embed_url": embed_url or None,
        "participant_embed_url": participant_url or None,
        "configured": bool(embed_url),
        "host_controls": bool(is_host and _has_room_key(host_query)),
    }


def _verify_init_data(init_data: str) -> dict:
    from .main import verify_telegram_init_data

    return verify_telegram_init_data(init_data)


def _issue_token(purpose: str, user: dict, ttl_seconds: int) -> tuple[str, datetime]:
    token = secrets.token_urlsafe(32)
    expires = _now() + timedelta(seconds=ttl_seconds)
    with _LOCK:
        con = _conn()
        try:
            con.execute(
                "INSERT INTO live_room_test_tokens(id, token_hash, purpose, user_json, created_at, expires_at, used_at) "
                "VALUES(?,?,?,?,?,?,NULL)",
                (
                    secrets.token_hex(16),
                    _hash_token(token),
                    purpose,
                    json.dumps(user, separators=(",", ":")),
                    _iso(),
                    _iso(expires),
                ),
            )
            con.commit()
        finally:
            con.close()
    return token, expires


def _load_token(token: str, purpose: str, consume: bool = False) -> dict:
    raw = str(token or "").strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Missing live room token")
    token_hash = _hash_token(raw)
    with _LOCK:
        con = _conn()
        try:
            row = con.execute(
                "SELECT * FROM live_room_test_tokens WHERE token_hash=? AND purpose=?",
                (token_hash, purpose),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid or expired live room token")
            expires = _parse(row["expires_at"])
            if not expires or expires <= _now():
                raise HTTPException(status_code=401, detail="Live room token expired")
            if row["used_at"]:
                raise HTTPException(status_code=409, detail="Live room token already used")
            if consume:
                con.execute(
                    "UPDATE live_room_test_tokens SET used_at=? WHERE token_hash=?",
                    (_iso(), token_hash),
                )
                con.commit()
            try:
                user = json.loads(row["user_json"] or "{}")
            except json.JSONDecodeError:
                user = {}
            if not user.get("id") and not user.get("user_id"):
                raise HTTPException(status_code=401, detail="Invalid live room session")
            return user
        finally:
            con.close()


def _bearer(authorization: Optional[str]) -> str:
    raw = str(authorization or "").strip()
    if raw.lower().startswith("bearer "):
        return raw[7:].strip()
    return raw


def identity_from_authorization(authorization: Optional[str]) -> Optional[dict]:
    token = _bearer(authorization)
    if not token:
        return None
    try:
        user = _load_token(token, PURPOSE_SESSION, consume=False)
    except HTTPException:
        return None
    public = _public_user(user)
    public["role"] = _role_for(user)
    return public


def apply_authorization_identity(payload, authorization: Optional[str]):
    ident = identity_from_authorization(authorization)
    if not ident:
        return None
    user_id = ident.get("user_id") or ident.get("id")
    if hasattr(payload, "user_id"):
        current = getattr(payload, "user_id")
        if isinstance(current, int) or current is None:
            try:
                payload.user_id = int(user_id)
            except (TypeError, ValueError):
                payload.user_id = user_id
        else:
            payload.user_id = str(user_id)
    if ident.get("username") is not None and hasattr(payload, "username"):
        payload.username = ident.get("username")
    if ident.get("display_name") and hasattr(payload, "display_name"):
        payload.display_name = ident["display_name"]
    return ident


def _session_payload(user: dict, session_token: str, expires_at: datetime) -> dict:
    public = _public_user(user)
    embed = _embed_for(user, _role_for(user))
    return {
        "session_token": session_token,
        "user": public,
        "role": embed["role"],
        "embed_url": embed["embed_url"],
        "participant_embed_url": embed["participant_embed_url"],
        "configured": embed["configured"],
        "host_controls": embed["host_controls"],
        "expires_at": _iso(expires_at),
        "telegram_app_url": _telegram_app_url() or None,
    }


class InitDataPayload(BaseModel):
    init_data: str


class ClaimPayload(BaseModel):
    token: str


class EventPayload(BaseModel):
    type: str
    event_id: Optional[str] = None
    activity: Optional[str] = None


@router.post("/session")
def create_session(payload: InitDataPayload):
    user = _verify_init_data(payload.init_data)
    token, expires = _issue_token(PURPOSE_SESSION, user, SESSION_TTL_SECONDS)
    return _session_payload(user, token, expires)


@router.post("/handoff")
def create_handoff(payload: InitDataPayload):
    user = _verify_init_data(payload.init_data)
    token, expires = _issue_token(PURPOSE_HANDOFF, user, HANDOFF_TTL_SECONDS)
    open_url = f"{_public_base()}/live-room-test.html?handoff={token}"
    return {
        "token": token,
        "open_url": open_url,
        "expires_at": _iso(expires),
        "telegram_app_url": _telegram_app_url() or None,
    }


@router.post("/claim")
def claim_handoff(payload: ClaimPayload):
    user = _load_token(payload.token, PURPOSE_HANDOFF, consume=True)
    token, expires = _issue_token(PURPOSE_SESSION, user, SESSION_TTL_SECONDS)
    return _session_payload(user, token, expires)


@router.get("/me")
def me(authorization: Optional[str] = Header(default=None)):
    ident = identity_from_authorization(authorization)
    if not ident:
        raise HTTPException(
            status_code=401,
            detail="Live room session expired. Re-open this Live Room from Telegram.",
        )
    user = {
        "id": ident.get("id"),
        "user_id": ident.get("user_id"),
        "username": ident.get("username"),
        "first_name": ident.get("first_name"),
        "last_name": ident.get("last_name"),
        "display_name": ident.get("display_name"),
    }
    embed = _embed_for(user, ident.get("role") or "participant")
    return {
        "user": _public_user(user),
        "role": embed["role"],
        "embed_url": embed["embed_url"],
        "participant_embed_url": embed["participant_embed_url"],
        "configured": embed["configured"],
        "host_controls": embed["host_controls"],
        "telegram_app_url": _telegram_app_url() or None,
    }


@router.post("/events")
def record_event(payload: EventPayload, authorization: Optional[str] = Header(default=None)):
    ident = identity_from_authorization(authorization)
    if not ident:
        raise HTTPException(status_code=401, detail="Live room session expired")
    event_type = str(payload.type or "").strip().lower()[:40]
    allowed = {"join", "heartbeat", "leave", "vote", "feed"}
    if event_type not in allowed:
        raise HTTPException(status_code=400, detail="Unsupported live room event")
    extra = {
        "event_id": (payload.event_id or "")[:180],
        "activity": (payload.activity or "live_room")[:80],
    }
    with _LOCK:
        con = _conn()
        try:
            con.execute(
                "INSERT INTO live_room_test_events(id, user_id, username, event_type, created_at, extra_json) "
                "VALUES(?,?,?,?,?,?)",
                (
                    secrets.token_hex(16),
                    str(ident.get("user_id") or ident.get("id")),
                    ident.get("username") or "",
                    event_type,
                    _iso(),
                    json.dumps(extra, separators=(",", ":")),
                ),
            )
            con.commit()
        finally:
            con.close()
    return {"ok": True, "type": event_type, "user_id": str(ident.get("user_id") or ident.get("id"))}


@router.post("/heartbeat")
def heartbeat(authorization: Optional[str] = Header(default=None)):
    return record_event(EventPayload(type="heartbeat"), authorization=authorization)
