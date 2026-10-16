"""Daily Check-In: one post per member per Europe/London community day.

Participation, replies, and EXP grants share that day boundary. Content rows
are soft-deleted so uniqueness and reward markers survive deletion. EXP is
recorded in the existing exp_grants ledger; the mini app applies the amount
to the member profile and claims the grant.
"""

from __future__ import annotations

import datetime
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/daily-checkin", tags=["daily-checkin"])

UK_TZ = ZoneInfo("Europe/London")
FEELINGS = ("good", "between", "hard")
FEELING_LABELS = {
    "good": "Feeling good",
    "between": "In between",
    "hard": "Having a hard day",
}
MESSAGE_LIMIT = 150
REPLY_EXP = 10
REPLY_EXP_LIMIT = 10
CHECKIN_FEELING_EXP = 30
CHECKIN_MESSAGE_EXP = 75
CHECKIN_REASON = "Daily check-in"
REPLY_REASON = "Check-in reply"
PAGE_LIMIT = 30
MAX_PAGE_LIMIT = 50
STYLE_KEYS = {
    "color",
    "skin",
    "stamp",
    "cornerSticker",
    "backdrop",
    "color_swatch",
    "colorSwatch",
    "skin_src",
    "skinSrc",
    "skin_chat",
    "stamp_src",
    "stampSrc",
    "sticker_src",
    "stickerSrc",
    "sticker_emoji",
    "stickerEmoji",
    "backdrop_src",
    "backdropSrc",
    "skin_layout",
    "skinLayout",
    "skin_opacity",
    "stamp_opacity",
}
CARE_REASONS = {
    "harassment",
    "unsafe_solicitation",
    "spam",
    "worried_about_someone",
    "other",
}
OWNER_USER_IDS = {"8385145826", "5737853594"}

_LOCK = threading.RLock()
_CLOCK = None
_HITS: dict[tuple[str, str], list[float]] = {}

_SCHEMA = """
CREATE TABLE IF NOT EXISTS daily_checkins (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    community_date TEXT NOT NULL,
    feeling TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    deleted_at TEXT,
    UNIQUE(user_id, community_date)
);
CREATE TABLE IF NOT EXISTS daily_checkin_replies (
    id TEXT PRIMARY KEY,
    checkin_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    community_date TEXT NOT NULL,
    message TEXT NOT NULL,
    display_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    deleted_at TEXT
);
CREATE TABLE IF NOT EXISTS daily_checkin_reply_slots (
    user_id TEXT NOT NULL,
    checkin_id TEXT NOT NULL,
    community_date TEXT NOT NULL,
    reply_id TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, checkin_id)
);
CREATE TABLE IF NOT EXISTS daily_checkin_reports (
    id TEXT PRIMARY KEY,
    reporter_id TEXT NOT NULL,
    target_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS exp_grants (
    id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    amount INTEGER NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    source_id TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    applied_at TEXT,
    result_json TEXT,
    error TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_exp_grants_source
    ON exp_grants(source, source_id);
"""


def _db_path() -> str:
    raw = os.getenv("ALCOVE_STATE_DB_PATH", "").strip()
    if raw:
        return raw
    return os.path.join(os.getcwd(), "alcove_state.db")


def _connect() -> sqlite3.Connection:
    path = _db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path, timeout=5, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn


def set_clock(fn) -> None:
    """Test hook. Production leaves this unset and uses Europe/London now."""
    global _CLOCK
    _CLOCK = fn


def community_clock(now: datetime.datetime | None = None) -> dict[str, Any]:
    if now is None and _CLOCK is not None:
        now = _CLOCK()
    current = now or datetime.datetime.now(UK_TZ)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UK_TZ)
    else:
        current = current.astimezone(UK_TZ)
    tomorrow = (current + datetime.timedelta(days=1)).date()
    reset_at = datetime.datetime.combine(tomorrow, datetime.time.min, tzinfo=UK_TZ)
    return {
        "server_time": current.isoformat(),
        "community_date": current.date().isoformat(),
        "next_reset_at": reset_at.isoformat(),
    }


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _http(status: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status, detail=detail)


def _user_from_init(init_data: str | None) -> dict:
    from .main import verify_telegram_init_data

    user = verify_telegram_init_data(init_data or "")
    if not user.get("id"):
        raise _http(400, "Telegram Mini App user was not provided")
    return user


def _init_data(body_value: str | None, header_value: str | None) -> str:
    return (header_value or body_value or "").strip()


def _display_name(user: dict) -> str:
    parts = [str(user.get("first_name") or "").strip(), str(user.get("last_name") or "").strip()]
    name = " ".join(part for part in parts if part).strip()
    if name:
        return name[:80]
    username = str(user.get("username") or "").strip()
    return (username or "Member")[:80]


def _user_id(user: dict) -> str:
    return str(user.get("id"))


def _is_moderator(user_id: str) -> bool:
    ids = set(OWNER_USER_IDS)
    extra = os.getenv("DAILY_CHECKIN_MODERATOR_IDS", "")
    for part in extra.split(","):
        value = part.strip()
        if value:
            ids.add(value)
    return str(user_id) in ids


def normalize_message(value: str | None, *, allow_blank: bool) -> str:
    text = str(value or "")
    text = text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    text = text.replace("\u2028", " ").replace("\u2029", " ")
    text = re.sub(r"[ \t\f\v]+", " ", text).strip()
    if len(text) > MESSAGE_LIMIT:
        raise _http(400, f"Messages can be at most {MESSAGE_LIMIT} characters.")
    if not allow_blank and not text:
        raise _http(400, "Reply text is required.")
    return text


def _limit(user_id: str, kind: str, maximum: int) -> None:
    now = time.monotonic()
    key = (str(user_id), kind)
    with _LOCK:
        hits = [stamp for stamp in _HITS.get(key, []) if now - stamp < 60]
        if len(hits) >= maximum:
            raise _http(429, "Too many Daily Check-In requests. Please wait a moment.")
        hits.append(now)
        _HITS[key] = hits


def _public_grant(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    source = row["source"]
    source_id = row["source_id"]
    return {
        "id": row["id"],
        "user_id": row["user_id"],
        "amount": row["amount"],
        "reason": row["reason"],
        "source": source,
        "source_id": source_id,
        "dedupe_key": f"{source}:{source_id}" if source and source_id else None,
        "status": row["status"],
        "created_at": row["created_at"],
    }


def _pending_grant(conn: sqlite3.Connection, source: str, source_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM exp_grants WHERE source = ? AND source_id = ?",
        (source, source_id),
    ).fetchone()
    grant = _public_grant(row)
    if not grant or grant["status"] != "pending" or int(grant["amount"] or 0) <= 0:
        return None
    return grant


def _insert_grant(
    conn: sqlite3.Connection,
    *,
    user_id: str,
    amount: int,
    reason: str,
    source: str,
    source_id: str,
) -> dict | None:
    if amount <= 0:
        return None
    existing = conn.execute(
        "SELECT * FROM exp_grants WHERE source = ? AND source_id = ?",
        (source, source_id),
    ).fetchone()
    if existing:
        return _pending_grant(conn, source, source_id)
    grant_id = uuid.uuid4().hex
    conn.execute(
        """
        INSERT INTO exp_grants (
            id, user_id, amount, reason, source, source_id, status, created_at,
            applied_at, result_json, error
        ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, NULL, NULL, NULL)
        """,
        (grant_id, int(user_id), amount, reason, source, source_id, _now_iso()),
    )
    return _pending_grant(conn, source, source_id)


def _feed_style(conn: sqlite3.Connection, user_id: str) -> dict:
    try:
        row = conn.execute(
            "SELECT payload_json FROM member_progress WHERE user_id = ?",
            (str(user_id),),
        ).fetchone()
    except sqlite3.OperationalError:
        return {}
    if not row:
        return {}
    try:
        payload = json.loads(row["payload_json"] or "{}")
    except json.JSONDecodeError:
        return {}
    feed = payload.get("feed") if isinstance(payload, dict) else None
    if not isinstance(feed, dict):
        return {}
    clean: dict[str, Any] = {}
    for key, value in feed.items():
        if key not in STYLE_KEYS or value is None:
            continue
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)) and key.endswith("opacity"):
            clean[key] = max(0.1, min(0.3, float(value)))
            continue
        text = str(value).strip()
        if not text or len(text) > 500:
            continue
        if text.lower().startswith("javascript:"):
            continue
        clean[key] = text
    return clean


def _checkin_row(conn: sqlite3.Connection, user_id: str, day: str):
    return conn.execute(
        "SELECT * FROM daily_checkins WHERE user_id = ? AND community_date = ?",
        (str(user_id), day),
    ).fetchone()


def _require_participation(conn: sqlite3.Connection, user_id: str, day: str):
    row = _checkin_row(conn, user_id, day)
    if row is None:
        raise _http(403, "Post your check-in before reading today's community.")
    return row


def _public_checkin(conn: sqlite3.Connection, row: sqlite3.Row, viewer_id: str) -> dict:
    checkin_id = row["id"]
    reply_count = conn.execute(
        """
        SELECT COUNT(*) AS n FROM daily_checkin_replies
        WHERE checkin_id = ? AND deleted_at IS NULL
        """,
        (checkin_id,),
    ).fetchone()["n"]
    slot = conn.execute(
        "SELECT 1 FROM daily_checkin_reply_slots WHERE user_id = ? AND checkin_id = ?",
        (str(viewer_id), checkin_id),
    ).fetchone()
    is_self = str(row["user_id"]) == str(viewer_id)
    deleted = bool(row["deleted_at"])
    return {
        "id": checkin_id,
        "display_name": row["display_name"],
        "feeling": row["feeling"],
        "feeling_label": FEELING_LABELS.get(row["feeling"], row["feeling"]),
        "message": "" if deleted else (row["message"] or ""),
        "created_at": row["created_at"],
        "community_date": row["community_date"],
        "deleted": deleted,
        "reply_count": int(reply_count or 0) if not deleted else 0,
        "is_self": is_self,
        "already_replied": bool(slot) and not is_self,
        "can_reply": (not is_self) and (not slot) and (not deleted) and row["community_date"] == community_clock()["community_date"],
        "can_delete": is_self and not deleted,
        "feed_style": {} if deleted else _feed_style(conn, row["user_id"]),
    }


def _own_payload(conn: sqlite3.Connection, row: sqlite3.Row | None, viewer_id: str) -> dict | None:
    if row is None:
        return None
    public = _public_checkin(conn, row, viewer_id)
    if row["deleted_at"]:
        return {
            "id": row["id"],
            "feeling": row["feeling"],
            "feeling_label": FEELING_LABELS.get(row["feeling"], row["feeling"]),
            "deleted": True,
            "community_date": row["community_date"],
        }
    return public


class SubmitPayload(BaseModel):
    feeling: str
    message: str | None = None
    community_date: str | None = None
    init_data: str | None = None


class ReplyPayload(BaseModel):
    message: str
    init_data: str | None = None


class DeletePayload(BaseModel):
    init_data: str | None = None


class ReportPayload(BaseModel):
    target_type: str
    target_id: str
    reason: str = "other"
    init_data: str | None = None


def _assert_same_day(confirmed: str | None, day: str) -> None:
    raw = str(confirmed or "").strip()
    if not raw:
        raise _http(400, "Confirm your check-in again before posting.")
    if raw != day:
        raise _http(
            409,
            "The community day changed. Review your check-in and confirm again.",
        )


def submit_checkin(user: dict, feeling: str, message: str | None, confirmed_day: str | None) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "write", 20)
    mood = str(feeling or "").strip().lower()
    if mood not in FEELINGS:
        raise _http(400, "Choose how you are feeling today.")
    text = normalize_message(message, allow_blank=True)
    clock = community_clock()
    day = clock["community_date"]
    _assert_same_day(confirmed_day, day)
    amount = CHECKIN_MESSAGE_EXP if text else CHECKIN_FEELING_EXP
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        existing = _checkin_row(conn, user_id, day)
        if existing is not None:
            if existing["deleted_at"]:
                conn.rollback()
                raise _http(409, "You've already checked in today.")
            conn.commit()
            grant = _pending_grant(conn, "daily_checkin", existing["id"])
            return {
                "status": "ok",
                "recovered": True,
                "checkin": _public_checkin(conn, existing, user_id),
                "award": grant,
                **clock,
            }
        checkin_id = uuid.uuid4().hex
        conn.execute(
            """
            INSERT INTO daily_checkins (
                id, user_id, community_date, feeling, message, display_name, created_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (checkin_id, user_id, day, mood, text, _display_name(user), _now_iso()),
        )
        award = _insert_grant(
            conn,
            user_id=user_id,
            amount=amount,
            reason=CHECKIN_REASON,
            source="daily_checkin",
            source_id=checkin_id,
        )
        conn.commit()
        row = _checkin_row(conn, user_id, day)
        return {
            "status": "ok",
            "recovered": False,
            "checkin": _public_checkin(conn, row, user_id),
            "award": award,
            **clock,
        }
    except HTTPException:
        raise
    except sqlite3.IntegrityError:
        conn.rollback()
        existing = _checkin_row(conn, user_id, day)
        if existing is None:
            raise _http(409, "You've already checked in today.")
        if existing["deleted_at"]:
            raise _http(409, "You've already checked in today.")
        return {
            "status": "ok",
            "recovered": True,
            "checkin": _public_checkin(conn, existing, user_id),
            "award": _pending_grant(conn, "daily_checkin", existing["id"]),
            **community_clock(),
        }
    finally:
        conn.close()


def delete_checkin(user: dict) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "write", 20)
    clock = community_clock()
    day = clock["community_date"]
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = _checkin_row(conn, user_id, day)
        if row is None or row["deleted_at"]:
            conn.rollback()
            raise _http(404, "There is no check-in to delete today.")
        stamp = _now_iso()
        conn.execute(
            "UPDATE daily_checkins SET deleted_at = ?, message = '' WHERE id = ? AND deleted_at IS NULL",
            (stamp, row["id"]),
        )
        conn.execute(
            """
            UPDATE daily_checkin_replies
            SET deleted_at = ?, message = ''
            WHERE checkin_id = ? AND deleted_at IS NULL
            """,
            (stamp, row["id"]),
        )
        conn.commit()
        return {"status": "ok", "deleted": True, **clock}
    finally:
        conn.close()


def get_state(user: dict) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "read", 60)
    clock = community_clock()
    conn = _connect()
    try:
        row = _checkin_row(conn, user_id, clock["community_date"])
        award = None
        if row is not None and not row["deleted_at"]:
            award = _pending_grant(conn, "daily_checkin", row["id"])
        return {
            "status": "ok",
            "participated": row is not None,
            "checkin": _own_payload(conn, row, user_id),
            "award": award,
            **clock,
        }
    finally:
        conn.close()


def list_feed(user: dict, feeling: str, cursor: str | None, limit: int) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "read", 60)
    mood = str(feeling or "").strip().lower()
    if mood not in FEELINGS:
        raise _http(400, "Choose a feeling to view.")
    clock = community_clock()
    day = clock["community_date"]
    page = max(1, min(int(limit or PAGE_LIMIT), MAX_PAGE_LIMIT))
    created_before = None
    id_before = None
    raw_cursor = str(cursor or "").strip()
    if raw_cursor:
        if "|" not in raw_cursor:
            raise _http(400, "That page cursor is not valid.")
        created_before, id_before = raw_cursor.split("|", 1)
    conn = _connect()
    try:
        _require_participation(conn, user_id, day)
        params: list[Any] = [day, mood]
        clause = "community_date = ? AND feeling = ? AND deleted_at IS NULL"
        if created_before and id_before:
            clause += " AND (created_at < ? OR (created_at = ? AND id < ?))"
            params.extend([created_before, created_before, id_before])
        rows = conn.execute(
            f"""
            SELECT * FROM daily_checkins
            WHERE {clause}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            """,
            (*params, page + 1),
        ).fetchall()
        visible = rows[:page]
        next_cursor = None
        if len(rows) > page and visible:
            last = visible[-1]
            next_cursor = f"{last['created_at']}|{last['id']}"
        return {
            "status": "ok",
            "feeling": mood,
            "feeling_label": FEELING_LABELS[mood],
            "posts": [_public_checkin(conn, row, user_id) for row in visible],
            "next_cursor": next_cursor,
            **clock,
        }
    finally:
        conn.close()


def list_replies(user: dict, checkin_id: str) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "read", 60)
    clock = community_clock()
    conn = _connect()
    try:
        _require_participation(conn, user_id, clock["community_date"])
        parent = conn.execute(
            "SELECT * FROM daily_checkins WHERE id = ?",
            (checkin_id,),
        ).fetchone()
        if parent is None or parent["deleted_at"] or parent["community_date"] != clock["community_date"]:
            raise _http(404, "That check-in is not in today's community.")
        rows = conn.execute(
            """
            SELECT * FROM daily_checkin_replies
            WHERE checkin_id = ? AND deleted_at IS NULL
            ORDER BY created_at ASC, id ASC
            """,
            (checkin_id,),
        ).fetchall()
        parent_public = _public_checkin(conn, parent, user_id)
        replies = []
        for row in rows:
            is_self = str(row["user_id"]) == user_id
            replies.append(
                {
                    "id": row["id"],
                    "display_name": row["display_name"],
                    "message": row["message"],
                    "created_at": row["created_at"],
                    "is_self": is_self,
                    "can_delete": is_self or _is_moderator(user_id),
                }
            )
        return {
            "status": "ok",
            "checkin_id": checkin_id,
            "replies": replies,
            "can_reply": parent_public["can_reply"],
            "already_replied": parent_public["already_replied"],
            "is_self": parent_public["is_self"],
            **clock,
        }
    finally:
        conn.close()


def _rewarded_reply_count(conn: sqlite3.Connection, user_id: str, day: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM exp_grants g
        JOIN daily_checkin_replies r ON r.id = g.source_id
        WHERE g.source = 'daily_checkin_reply'
          AND g.user_id = ?
          AND g.amount > 0
          AND r.community_date = ?
        """,
        (int(user_id), day),
    ).fetchone()
    return int(row["n"] or 0)


def submit_reply(user: dict, checkin_id: str, message: str) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "write", 20)
    text = normalize_message(message, allow_blank=False)
    clock = community_clock()
    day = clock["community_date"]
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _require_participation(conn, user_id, day)
        parent = conn.execute(
            "SELECT * FROM daily_checkins WHERE id = ?",
            (checkin_id,),
        ).fetchone()
        if parent is None or parent["deleted_at"] or parent["community_date"] != day:
            conn.rollback()
            raise _http(404, "That check-in is not open for replies today.")
        if str(parent["user_id"]) == user_id:
            conn.rollback()
            raise _http(403, "You can't reply to your own check-in.")
        slot = conn.execute(
            "SELECT reply_id FROM daily_checkin_reply_slots WHERE user_id = ? AND checkin_id = ?",
            (user_id, checkin_id),
        ).fetchone()
        if slot is not None:
            conn.rollback()
            raise _http(409, "You've already replied to this check-in.")
        reply_id = uuid.uuid4().hex
        created = _now_iso()
        try:
            conn.execute(
                """
                INSERT INTO daily_checkin_reply_slots (
                    user_id, checkin_id, community_date, reply_id, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, checkin_id, day, reply_id, created),
            )
        except sqlite3.IntegrityError:
            conn.rollback()
            raise _http(409, "You've already replied to this check-in.")
        conn.execute(
            """
            INSERT INTO daily_checkin_replies (
                id, checkin_id, user_id, community_date, message, display_name, created_at, deleted_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (reply_id, checkin_id, user_id, day, text, _display_name(user), created),
        )
        award = None
        if _rewarded_reply_count(conn, user_id, day) < REPLY_EXP_LIMIT:
            award = _insert_grant(
                conn,
                user_id=user_id,
                amount=REPLY_EXP,
                reason=REPLY_REASON,
                source="daily_checkin_reply",
                source_id=reply_id,
            )
        conn.commit()
        return {
            "status": "ok",
            "reply": {
                "id": reply_id,
                "display_name": _display_name(user),
                "message": text,
                "created_at": created,
                "is_self": True,
                "can_delete": True,
            },
            "award": award,
            **clock,
        }
    except HTTPException:
        raise
    finally:
        conn.close()


def delete_reply(user: dict, reply_id: str) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "write", 20)
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT * FROM daily_checkin_replies WHERE id = ?",
            (reply_id,),
        ).fetchone()
        if row is None or row["deleted_at"]:
            conn.rollback()
            raise _http(404, "That reply is no longer available.")
        if str(row["user_id"]) != user_id and not _is_moderator(user_id):
            conn.rollback()
            raise _http(403, "You can only delete your own replies.")
        conn.execute(
            "UPDATE daily_checkin_replies SET deleted_at = ?, message = '' WHERE id = ?",
            (_now_iso(), reply_id),
        )
        conn.commit()
        return {"status": "ok", "deleted": True, **community_clock()}
    finally:
        conn.close()


def file_report(user: dict, target_type: str, target_id: str, reason: str) -> dict:
    user_id = _user_id(user)
    _limit(user_id, "write", 20)
    kind = str(target_type or "").strip().lower()
    if kind not in {"checkin", "reply"}:
        raise _http(400, "Choose a check-in or a reply to report.")
    why = str(reason or "other").strip().lower()
    if why not in CARE_REASONS:
        raise _http(400, "That report reason is not supported.")
    clock = community_clock()
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _require_participation(conn, user_id, clock["community_date"])
        if kind == "checkin":
            target = conn.execute(
                "SELECT * FROM daily_checkins WHERE id = ? AND deleted_at IS NULL",
                (target_id,),
            ).fetchone()
            excerpt = ""
            if target is not None:
                excerpt = target["message"] or target["feeling"] or ""
        else:
            target = conn.execute(
                "SELECT * FROM daily_checkin_replies WHERE id = ? AND deleted_at IS NULL",
                (target_id,),
            ).fetchone()
            excerpt = target["message"] if target else ""
        if target is None:
            conn.rollback()
            raise _http(404, "That post is no longer available to report.")
        if str(target["user_id"]) == user_id:
            conn.rollback()
            raise _http(400, "You can't report your own post.")
        report_id = uuid.uuid4().hex
        created = _now_iso()
        try:
            from .safety_enforcement import _ensure as _ensure_safety

            _ensure_safety(conn)
        except Exception:
            pass
        conn.execute(
            """
            INSERT INTO daily_checkin_reports (
                id, reporter_id, target_type, target_id, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (report_id, user_id, kind, target_id, why, created),
        )
        try:
            conn.execute(
                """
                INSERT INTO care_reports (
                    id, reporter_id, target_user_id, chat_id, message_id, reason, note,
                    message_excerpt, created_at, status, resolved_at, admin_user_id, decision, source
                ) VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, 'telegram')
                """,
                (
                    report_id,
                    int(user_id),
                    int(target["user_id"]),
                    why,
                    f"Daily Check-In {kind}",
                    str(excerpt or "")[:500],
                    created,
                ),
            )
        except sqlite3.OperationalError:
            pass
        conn.commit()
        return {"status": "ok", "report_id": report_id}
    finally:
        conn.close()


@router.get("")
def route_state(
    init_data: str | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    return get_state(_user_from_init(_init_data(init_data, x_telegram_init_data)))


@router.post("")
def route_submit(
    payload: SubmitPayload,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user = _user_from_init(_init_data(payload.init_data, x_telegram_init_data))
    return submit_checkin(user, payload.feeling, payload.message, payload.community_date)


@router.delete("")
def route_delete(
    payload: DeletePayload | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    payload = payload or DeletePayload()
    user = _user_from_init(_init_data(payload.init_data, x_telegram_init_data))
    return delete_checkin(user)


@router.get("/feed")
def route_feed(
    feeling: str,
    cursor: str | None = None,
    limit: int = PAGE_LIMIT,
    init_data: str | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user = _user_from_init(_init_data(init_data, x_telegram_init_data))
    return list_feed(user, feeling, cursor, limit)


@router.get("/{checkin_id}/replies")
def route_replies(
    checkin_id: str,
    init_data: str | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user = _user_from_init(_init_data(init_data, x_telegram_init_data))
    return list_replies(user, checkin_id)


@router.post("/{checkin_id}/replies")
def route_reply(
    checkin_id: str,
    payload: ReplyPayload,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user = _user_from_init(_init_data(payload.init_data, x_telegram_init_data))
    return submit_reply(user, checkin_id, payload.message)


@router.delete("/replies/{reply_id}")
def route_delete_reply(
    reply_id: str,
    payload: DeletePayload | None = None,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    payload = payload or DeletePayload()
    user = _user_from_init(_init_data(payload.init_data, x_telegram_init_data))
    return delete_reply(user, reply_id)


@router.post("/reports")
def route_report(
    payload: ReportPayload,
    x_telegram_init_data: str | None = Header(default=None, alias="X-Telegram-Init-Data"),
):
    user = _user_from_init(_init_data(payload.init_data, x_telegram_init_data))
    return file_report(user, payload.target_type, payload.target_id, payload.reason)
