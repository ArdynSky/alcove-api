"""Feature-admin enforcement jobs, per-item safety rows, and care reports.

``admin_jobs`` is the queue F.O.X already polls. It is a name -> singleton map
(today only ``bulk_verify_group``), persisted in runtime state. Concurrent
mute / strike / warn / ban / dismiss requests live in ``admin_jobs["actions"]``
so they share that poll and persistence path.

Care reports and EXP grants are rows in the API state database. They are
durable records (reporter timestamps, a +30 EXP receipt), not bot jobs.
Member EXP in this repo is a profile blob. Care-report grants stay pending until the
miniapp applies them and calls ``POST /api/exp-grants/{id}/claim``. Nothing
here writes the profile blob.
"""

from __future__ import annotations

import datetime
import json
import os
import sqlite3
import threading
import uuid
from typing import Any, Literal

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

router = APIRouter(tags=["safety"])

_LOCK = threading.RLock()

ACTIONS_KEY = "actions"
SAFETY_JOB = "safety_action"
CLAIM_STALE_SECONDS = 15 * 60
CARE_REMOVE_EXP = 30
MAX_TERMINAL_JOBS = 200
MAX_REASON_LEN = 1000
MAX_NOTE_LEN = 2000
MAX_ERROR_LEN = 1000
ROW_LIMIT_PER_KIND = 80

SAFETY_ACTIONS = {
    "mute",
    "strike_add",
    "strike_remove",
    "warn_private",
    "warn_public",
    "ban_request",
    "dismiss",
}
USER_REQUIRED_ACTIONS = SAFETY_ACTIONS - {"dismiss"}
MUTE_HOURS = {1, 6, 12, 24}
ACTION_SOURCES = {"feature_admin", "fox_care", "telegram"}
CARE_REASONS = {
    "harassment",
    "unsafe_solicitation",
    "spam",
    "worried_about_someone",
    "other",
}
CARE_DECISIONS = {
    "keep": "kept",
    "remove": "removed",
    "teach": "taught",
}
TERMINAL_JOB_STATUSES = {"completed", "failed"}
QUEUE_STATUSES = {"open", "acted", "dismissed"}

# TODO(fox-logs): flood_flags has message_excerpt but no message_id or chat_id.
# TODO(fox-logs): link_violations and tone_flags have message_id but no chat_id.
ACTION_QUEUE_ROW_GAPS = [
    "flood_flags has no message_id or chat_id; those fields are null and listed in missing.",
    "link_violations and tone_flags have message_id but no chat_id.",
    "strike_count is null when fox_logs.db is unavailable; 0 means the user has no active strikes.",
    "display_name is empty when neither the source row nor user_profiles has one.",
]

_SCHEMA = """
CREATE TABLE IF NOT EXISTS care_reports (
    id TEXT PRIMARY KEY,
    reporter_id INTEGER NOT NULL,
    target_user_id INTEGER NOT NULL,
    chat_id INTEGER,
    message_id INTEGER,
    reason TEXT NOT NULL,
    note TEXT,
    created_at TEXT NOT NULL,
    status TEXT NOT NULL,
    resolved_at TEXT,
    admin_user_id INTEGER,
    decision TEXT,
    source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_care_reports_reporter_created
    ON care_reports(reporter_id, created_at);
CREATE INDEX IF NOT EXISTS idx_care_reports_status_created
    ON care_reports(status, created_at);

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
CREATE INDEX IF NOT EXISTS idx_exp_grants_status_created
    ON exp_grants(status, created_at);

CREATE TABLE IF NOT EXISTS safety_queue_resolutions (
    queue_item_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    job_id TEXT,
    action TEXT,
    updated_at TEXT NOT NULL
);
"""


def _now() -> str:
    from . import main

    return main.now_iso()


def _parse_utc(value: str | None) -> datetime.datetime | None:
    from . import main

    return main.parse_iso_utc(value)


def _connect() -> sqlite3.Connection:
    from . import main

    path = main.STATE_DB_PATH
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure(conn: sqlite3.Connection) -> None:
    conn.executescript(_SCHEMA)


def _db() -> sqlite3.Connection:
    conn = _connect()
    try:
        _ensure(conn)
        conn.commit()
    except Exception:
        conn.close()
        raise
    return conn


def _http(status_code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=status_code, detail=detail)


def _clean_text(value: str | None, *, limit: int, field: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > limit:
        raise _http(400, f"{field} must be at most {limit} characters")
    return text


def _bounded_int(
    value: int | None,
    *,
    field: str,
    required: bool = False,
    allow_negative: bool = False,
) -> int | None:
    if value is None:
        if required:
            raise _http(400, f"{field} is required")
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise _http(400, f"{field} must be an integer")
    if value == 0 or abs(value) >= 2**63:
        raise _http(400, f"{field} is out of range")
    if not allow_negative and value < 0:
        raise _http(400, f"{field} must be a positive integer")
    return value


def _queue_item_id(value: str | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > 120 or any(char.isspace() or ord(char) < 32 for char in text):
        raise _http(400, "queue_item_id must be a single token up to 120 characters")
    return text


def _record_id(value: str | None, *, field: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or len(text) > 80:
        raise _http(400, f"{field} is invalid")
    return text


def _persist_jobs() -> None:
    from . import main

    main.save_runtime_state(force=True)


def _action_jobs() -> list:
    from . import main

    jobs = main.admin_jobs.get(ACTIONS_KEY)
    if not isinstance(jobs, list):
        jobs = []
        main.admin_jobs[ACTIONS_KEY] = jobs
    return jobs


def _trim_jobs(jobs: list) -> None:
    terminal_indexes = [
        index
        for index, job in enumerate(jobs)
        if isinstance(job, dict) and job.get("status") in TERMINAL_JOB_STATUSES
    ]
    overflow = len(terminal_indexes) - MAX_TERMINAL_JOBS
    if overflow <= 0:
        return
    drop = set(terminal_indexes[:overflow])
    jobs[:] = [job for index, job in enumerate(jobs) if index not in drop]


def _public_job(job: dict, *, reclaimable: bool = False) -> dict:
    payload = {
        "job": SAFETY_JOB,
        "id": job.get("id"),
        "action": job.get("action"),
        "status": job.get("status"),
        "telegram_user_id": job.get("telegram_user_id"),
        "chat_id": job.get("chat_id"),
        "message_id": job.get("message_id"),
        "hours": job.get("hours"),
        "reason": job.get("reason"),
        "source": job.get("source"),
        "queue_item_id": job.get("queue_item_id"),
        "care_report_id": job.get("care_report_id"),
        "admin_user_id": job.get("admin_user_id"),
        "requested_at": job.get("requested_at"),
        "claimed_at": job.get("claimed_at"),
        "completed_at": job.get("completed_at"),
        "result": job.get("result"),
        "error": job.get("error"),
    }
    if reclaimable:
        payload["reclaimable"] = True
    return payload


def _claim_is_stale(job: dict) -> bool:
    claimed_at = _parse_utc(job.get("claimed_at"))
    if claimed_at is None:
        return True
    age = (datetime.datetime.utcnow() - claimed_at).total_seconds()
    return age >= CLAIM_STALE_SECONDS


def pending_bot_action_jobs() -> list[dict]:
    """Pending safety actions, plus claimed jobs whose claim has gone stale."""
    with _LOCK:
        jobs = [job for job in _action_jobs() if isinstance(job, dict)]
        visible = []
        for job in jobs:
            status = job.get("status")
            if status == "pending":
                visible.append(_public_job(job))
            elif status == "claimed" and _claim_is_stale(job):
                visible.append(_public_job(job, reclaimable=True))
        return visible


def _find_job(job_id: str) -> dict | None:
    for job in _action_jobs():
        if isinstance(job, dict) and job.get("id") == job_id:
            return job
    return None


def _jobs_by_id() -> dict[str, dict]:
    with _LOCK:
        return {
            job["id"]: job
            for job in _action_jobs()
            if isinstance(job, dict) and job.get("id")
        }


def _related_queue_ids(job: dict) -> list[str]:
    ids: list[str] = []
    queue_item_id = job.get("queue_item_id")
    if queue_item_id:
        ids.append(queue_item_id)
    care_report_id = job.get("care_report_id")
    if care_report_id:
        care_key = f"care_report:{care_report_id}"
        if care_key not in ids:
            ids.append(care_key)
    return ids


def _set_resolution(
    conn: sqlite3.Connection,
    queue_item_id: str,
    status: str,
    job_id: str | None,
    action: str | None,
    *,
    only_if_job: str | None = None,
) -> None:
    if status not in QUEUE_STATUSES:
        raise _http(400, "queue status is invalid")
    current = conn.execute(
        "SELECT job_id FROM safety_queue_resolutions WHERE queue_item_id = ?",
        (queue_item_id,),
    ).fetchone()
    if only_if_job and current and current["job_id"] and current["job_id"] != only_if_job:
        return
    conn.execute(
        """
        INSERT INTO safety_queue_resolutions (queue_item_id, status, job_id, action, updated_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(queue_item_id) DO UPDATE SET
            status = excluded.status,
            job_id = excluded.job_id,
            action = excluded.action,
            updated_at = excluded.updated_at
        """,
        (queue_item_id, status, job_id, action, _now()),
    )


def _apply_job_resolution(job: dict, status: str, *, only_owner: bool) -> None:
    queue_ids = _related_queue_ids(job)
    if not queue_ids:
        return
    conn = _db()
    try:
        for queue_item_id in queue_ids:
            _set_resolution(
                conn,
                queue_item_id,
                status,
                job.get("id"),
                job.get("action"),
                only_if_job=job.get("id") if only_owner else None,
            )
        conn.commit()
    finally:
        conn.close()


def _matching_open_job(candidate: dict) -> dict | None:
    for job in _action_jobs():
        if not isinstance(job, dict) or job.get("status") not in {"pending", "claimed"}:
            continue
        same = all(
            job.get(key) == candidate.get(key)
            for key in (
                "action",
                "telegram_user_id",
                "chat_id",
                "message_id",
                "hours",
                "queue_item_id",
                "care_report_id",
            )
        )
        if same:
            return job
    return None


class SafetyActionPayload(BaseModel):
    admin_secret: str
    action: Literal[
        "mute",
        "strike_add",
        "strike_remove",
        "warn_private",
        "warn_public",
        "ban_request",
        "dismiss",
    ]
    telegram_user_id: int | None = None
    chat_id: int | None = None
    message_id: int | None = None
    hours: int | None = None
    reason: str | None = None
    source: Literal["feature_admin", "fox_care", "telegram"] = "feature_admin"
    queue_item_id: str | None = None
    care_report_id: str | None = None
    admin_user_id: int | None = None


class SafetyJobCompletePayload(BaseModel):
    status: Literal["completed", "failed"] = "completed"
    result: dict | None = None
    error: str | None = None


class CareReportCreatePayload(BaseModel):
    reporter_id: int
    target_user_id: int
    chat_id: int | None = None
    message_id: int | None = None
    reason: Literal[
        "harassment",
        "unsafe_solicitation",
        "spam",
        "worried_about_someone",
        "other",
    ]
    note: str | None = None
    source: Literal["feature_admin", "fox_care", "telegram"] = "fox_care"


class CareReportResolvePayload(BaseModel):
    admin_secret: str
    decision: Literal["keep", "remove", "teach"]
    admin_user_id: int


class ExpGrantCompletePayload(BaseModel):
    status: Literal["applied", "failed"]
    result: dict | None = None
    error: str | None = None


class ExpGrantClaimPayload(BaseModel):
    user_id: int


def _require_admin(secret: str | None) -> None:
    from . import main

    main.verify_admin_secret(secret)


def _require_bot(secret: str | None) -> None:
    from . import main

    main.verify_bot_sync_secret(secret)


def _load_care_report(conn: sqlite3.Connection, report_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM care_reports WHERE id = ?", (report_id,)).fetchone()
    return dict(row) if row else None


def _public_care_report(row: dict) -> dict:
    return {
        "id": row.get("id"),
        "reporter_id": row.get("reporter_id"),
        "target_user_id": row.get("target_user_id"),
        "chat_id": row.get("chat_id"),
        "message_id": row.get("message_id"),
        "reason": row.get("reason"),
        "note": row.get("note"),
        "created_at": row.get("created_at"),
        "status": row.get("status"),
        "resolved_at": row.get("resolved_at"),
        "admin_user_id": row.get("admin_user_id"),
        "decision": row.get("decision"),
        "source": row.get("source"),
    }


def _public_grant(row: dict) -> dict:
    result = None
    raw_result = row.get("result_json")
    if raw_result:
        try:
            parsed = json.loads(raw_result)
        except (TypeError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, dict):
            result = parsed
    source = row.get("source")
    source_id = row.get("source_id")
    return {
        "id": row.get("id"),
        "user_id": row.get("user_id"),
        "amount": row.get("amount"),
        "reason": row.get("reason"),
        "source": source,
        "source_id": source_id,
        "dedupe_key": f"{source}:{source_id}" if source and source_id else None,
        "status": row.get("status"),
        "created_at": row.get("created_at"),
        "applied_at": row.get("applied_at"),
        "result": result,
        "error": row.get("error"),
    }


def _reporter_activity(conn: sqlite3.Connection, reporter_id: int) -> dict:
    now = datetime.datetime.utcnow()
    day_ago = (now - datetime.timedelta(hours=24)).isoformat() + "Z"
    week_ago = (now - datetime.timedelta(days=7)).isoformat() + "Z"
    count_24h = conn.execute(
        "SELECT COUNT(*) AS count FROM care_reports WHERE reporter_id = ? AND created_at >= ?",
        (reporter_id, day_ago),
    ).fetchone()["count"]
    count_7d = conn.execute(
        "SELECT COUNT(*) AS count FROM care_reports WHERE reporter_id = ? AND created_at >= ?",
        (reporter_id, week_ago),
    ).fetchone()["count"]
    last = conn.execute(
        """
        SELECT created_at FROM care_reports
        WHERE reporter_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (reporter_id,),
    ).fetchone()
    return {
        "reporter_id": reporter_id,
        "count_24h": int(count_24h or 0),
        "count_7d": int(count_7d or 0),
        "last_created_at": last["created_at"] if last else None,
    }


def _fox_logs_available() -> bool:
    from . import main

    path = main.FOX_LOGS_DB_PATH
    return bool(path) and os.path.exists(path)


def _profile_names(user_ids: list[int]) -> dict[int, dict]:
    if not user_ids or not _fox_logs_available():
        return {}
    from . import main

    unique = sorted({int(user_id) for user_id in user_ids})
    placeholders = ",".join("?" * len(unique))
    rows = main.fox_db_rows(
        f"""
        SELECT user_id, username, display_name
        FROM user_profiles
        WHERE user_id IN ({placeholders})
        """,
        tuple(unique),
    )
    names = {}
    for row in rows:
        if row.get("user_id") is None:
            continue
        names[int(row["user_id"])] = row
    return names


def _strike_counts(user_ids: list[int]) -> dict[int, int] | None:
    if not _fox_logs_available():
        return None
    from . import main

    unique = sorted({int(user_id) for user_id in user_ids if user_id is not None})
    if not unique:
        return {}
    placeholders = ",".join("?" * len(unique))
    rows = main.fox_db_rows(
        f"""
        SELECT user_id, COUNT(*) AS strike_count
        FROM user_strikes
        WHERE active = 1 AND user_id IN ({placeholders})
        GROUP BY user_id
        """,
        tuple(unique),
    )
    counts = {user_id: 0 for user_id in unique}
    for row in rows:
        if row.get("user_id") is None:
            continue
        counts[int(row["user_id"])] = int(row.get("strike_count") or 0)
    return counts


def _schema_missing(kind: str) -> list[str]:
    if kind == "flood":
        return ["message_id", "chat_id"]
    if kind in {"link", "tone"}:
        return ["chat_id"]
    return []


def _identity(user_id, username, display_name, profiles: dict[int, dict]) -> dict:
    from . import main

    profile = profiles.get(int(user_id)) if user_id is not None else None
    if not username and profile:
        username = profile.get("username") or ""
    if not display_name and profile:
        display_name = profile.get("display_name") or ""
    return main.format_group_user_ref(user_id, username or "", display_name or "")


def _resolutions_for(queue_ids: list[str]) -> dict[str, dict]:
    if not queue_ids:
        return {}
    conn = _db()
    try:
        placeholders = ",".join("?" * len(queue_ids))
        rows = conn.execute(
            f"""
            SELECT queue_item_id, status, job_id, action, updated_at
            FROM safety_queue_resolutions
            WHERE queue_item_id IN ({placeholders})
            """,
            tuple(queue_ids),
        ).fetchall()
    finally:
        conn.close()
    return {row["queue_item_id"]: dict(row) for row in rows}


def _row_status(resolution: dict | None, care_status: str | None = None) -> str:
    if resolution and resolution.get("status") in QUEUE_STATUSES:
        return resolution["status"]
    if care_status and care_status != "pending":
        return "acted"
    return "open"


def _queue_row(
    *,
    row_id: str,
    kind: str,
    user_id,
    username: str,
    display_name: str,
    excerpt: str | None,
    message_id: int | None,
    chat_id: int | None,
    created_at: str | None,
    profiles: dict[int, dict],
    strikes: dict[int, int] | None,
    resolution: dict | None,
    jobs: dict[str, dict],
    care_report_id: str | None = None,
    care_status: str | None = None,
    reporter_id: int | None = None,
    reason: str | None = None,
    detail: dict | None = None,
) -> dict:
    identity = _identity(user_id, username, display_name, profiles)
    missing = list(_schema_missing(kind))
    strike_count = None
    if strikes is None:
        missing.append("strike_count")
    elif user_id is not None:
        strike_count = int(strikes.get(int(user_id), 0))
    job_id = resolution.get("job_id") if resolution else None
    job = jobs.get(job_id) if job_id else None
    message_ids = [int(message_id)] if message_id else []
    return {
        "id": row_id,
        "kind": kind,
        "user_id": identity.get("user_id"),
        "username": identity.get("username") or "",
        "display_name": identity.get("display_name") or "",
        "label": identity.get("label") or "",
        "excerpt": excerpt or None,
        "message_id": int(message_id) if message_id else None,
        "message_ids": message_ids,
        "chat_id": chat_id,
        "strike_count": strike_count,
        "created_at": created_at,
        "status": _row_status(resolution, care_status),
        "queue_item_id": row_id,
        "job_id": job_id,
        "job_status": job.get("status") if job else None,
        "job_action": (resolution.get("action") if resolution else None),
        "care_report_id": care_report_id,
        "care_status": care_status,
        "reporter_id": reporter_id,
        "reason": reason,
        "missing": missing,
        "detail": detail or {},
    }


def build_action_queue_rows(since: str | None) -> list[dict]:
    from . import main

    where, params = main.since_clause("logged_at", since)
    flood_rows = main.fox_db_rows(
        f"""
        SELECT id, user_id, username, display_name, message_count, window_seconds,
               message_excerpt, logged_at
        FROM flood_flags
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (ROW_LIMIT_PER_KIND,),
    )
    link_rows = main.fox_db_rows(
        f"""
        SELECT id, message_id, user_id, username, display_name, message_excerpt,
               link_samples, logged_at
        FROM link_violations
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (ROW_LIMIT_PER_KIND,),
    )
    tone_rows = main.fox_db_rows(
        f"""
        SELECT id, message_id, user_id, username, display_name, categories, severity,
               score, matched_terms, message_excerpt, logged_at
        FROM tone_flags
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (ROW_LIMIT_PER_KIND,),
    )

    care_where = ""
    care_params: tuple = ()
    if since:
        care_where = " WHERE created_at >= ?"
        care_params = (since,)
    conn = _db()
    try:
        care_rows = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT * FROM care_reports
                {care_where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                care_params + (ROW_LIMIT_PER_KIND,),
            ).fetchall()
        ]
    finally:
        conn.close()

    drafts: list[dict] = []
    user_ids: list[int] = []
    queue_ids: list[str] = []

    def remember(user_id, row_id: str, payload: dict) -> None:
        if user_id is not None:
            user_ids.append(int(user_id))
        queue_ids.append(row_id)
        drafts.append(payload)

    for row in flood_rows:
        row_id = f"flood:{row.get('id')}"
        remember(
            row.get("user_id"),
            row_id,
            {
                "row_id": row_id,
                "kind": "flood",
                "user_id": row.get("user_id"),
                "username": row.get("username") or "",
                "display_name": row.get("display_name") or "",
                "excerpt": row.get("message_excerpt"),
                "message_id": None,
                "chat_id": None,
                "created_at": row.get("logged_at"),
                "detail": {
                    "message_count": row.get("message_count"),
                    "window_seconds": row.get("window_seconds"),
                },
            },
        )
    for row in link_rows:
        row_id = f"link:{row.get('id')}"
        remember(
            row.get("user_id"),
            row_id,
            {
                "row_id": row_id,
                "kind": "link",
                "user_id": row.get("user_id"),
                "username": row.get("username") or "",
                "display_name": row.get("display_name") or "",
                "excerpt": row.get("message_excerpt"),
                "message_id": row.get("message_id"),
                "chat_id": None,
                "created_at": row.get("logged_at"),
                "detail": {"link_samples": row.get("link_samples")},
            },
        )
    for row in tone_rows:
        row_id = f"tone:{row.get('id')}"
        remember(
            row.get("user_id"),
            row_id,
            {
                "row_id": row_id,
                "kind": "tone",
                "user_id": row.get("user_id"),
                "username": row.get("username") or "",
                "display_name": row.get("display_name") or "",
                "excerpt": row.get("message_excerpt"),
                "message_id": row.get("message_id"),
                "chat_id": None,
                "created_at": row.get("logged_at"),
                "detail": {
                    "categories": row.get("categories"),
                    "severity": row.get("severity"),
                    "score": row.get("score"),
                    "matched_terms": row.get("matched_terms"),
                },
            },
        )
    for row in care_rows:
        row_id = f"care_report:{row.get('id')}"
        remember(
            row.get("target_user_id"),
            row_id,
            {
                "row_id": row_id,
                "kind": "care_report",
                "user_id": row.get("target_user_id"),
                "username": "",
                "display_name": "",
                "excerpt": row.get("note") or row.get("reason"),
                "message_id": row.get("message_id"),
                "chat_id": row.get("chat_id"),
                "created_at": row.get("created_at"),
                "care_report_id": row.get("id"),
                "care_status": row.get("status"),
                "reporter_id": row.get("reporter_id"),
                "reason": row.get("reason"),
                "detail": {"source": row.get("source"), "decision": row.get("decision")},
            },
        )

    profiles = _profile_names(user_ids)
    strikes = _strike_counts(user_ids)
    resolutions = _resolutions_for(queue_ids)
    jobs = _jobs_by_id()
    rows = []
    for draft in drafts:
        resolution = resolutions.get(draft["row_id"])
        rows.append(
            _queue_row(
                row_id=draft["row_id"],
                kind=draft["kind"],
                user_id=draft.get("user_id"),
                username=draft.get("username") or "",
                display_name=draft.get("display_name") or "",
                excerpt=draft.get("excerpt"),
                message_id=draft.get("message_id"),
                chat_id=draft.get("chat_id"),
                created_at=draft.get("created_at"),
                profiles=profiles,
                strikes=strikes,
                resolution=resolution,
                jobs=jobs,
                care_report_id=draft.get("care_report_id"),
                care_status=draft.get("care_status"),
                reporter_id=draft.get("reporter_id"),
                reason=draft.get("reason"),
                detail=draft.get("detail"),
            )
        )
    rows.sort(key=lambda row: row.get("created_at") or "", reverse=True)
    return rows


def _validate_action(payload: SafetyActionPayload) -> dict:
    action = payload.action
    if action not in SAFETY_ACTIONS:
        raise _http(400, "action is not supported")
    if payload.source not in ACTION_SOURCES:
        raise _http(400, "source is not supported")
    telegram_user_id = _bounded_int(
        payload.telegram_user_id,
        field="telegram_user_id",
        required=action in USER_REQUIRED_ACTIONS,
    )
    queue_item_id = _queue_item_id(payload.queue_item_id)
    care_report_id = _record_id(payload.care_report_id, field="care_report_id")
    if action == "dismiss" and telegram_user_id is None and not queue_item_id and not care_report_id:
        raise _http(400, "dismiss requires telegram_user_id, queue_item_id, or care_report_id")
    hours = payload.hours
    if action == "mute":
        if hours not in MUTE_HOURS:
            raise _http(400, "hours must be 1, 6, 12, or 24 for mute")
    elif hours is not None:
        raise _http(400, "hours is only valid for mute")
    if care_report_id:
        conn = _db()
        try:
            if not _load_care_report(conn, care_report_id):
                raise _http(404, "Care report not found")
        finally:
            conn.close()
    return {
        "action": action,
        "telegram_user_id": telegram_user_id,
        "chat_id": _bounded_int(payload.chat_id, field="chat_id", allow_negative=True),
        "message_id": _bounded_int(payload.message_id, field="message_id"),
        "hours": hours if action == "mute" else None,
        "reason": _clean_text(payload.reason, limit=MAX_REASON_LEN, field="reason"),
        "source": payload.source,
        "queue_item_id": queue_item_id,
        "care_report_id": care_report_id,
        "admin_user_id": _bounded_int(payload.admin_user_id, field="admin_user_id"),
    }


@router.post("/api/admin/safety/actions")
def enqueue_safety_action(payload: SafetyActionPayload):
    _require_admin(payload.admin_secret)
    candidate = _validate_action(payload)
    with _LOCK:
        existing = _matching_open_job(candidate)
        if existing:
            return {"status": "ok", "already_queued": True, "job": _public_job(existing)}
        job = {
            "id": uuid.uuid4().hex,
            "action": candidate["action"],
            "status": "pending",
            "telegram_user_id": candidate["telegram_user_id"],
            "chat_id": candidate["chat_id"],
            "message_id": candidate["message_id"],
            "hours": candidate["hours"],
            "reason": candidate["reason"],
            "source": candidate["source"],
            "queue_item_id": candidate["queue_item_id"],
            "care_report_id": candidate["care_report_id"],
            "admin_user_id": candidate["admin_user_id"],
            "requested_at": _now(),
            "claimed_at": None,
            "completed_at": None,
            "result": None,
            "error": None,
        }
        # ban_request is only a request. F.O.X enforces FINAL_BAN_APPROVER_ID.
        jobs = _action_jobs()
        jobs.append(job)
        _trim_jobs(jobs)
        _persist_jobs()
    resolution_status = "dismissed" if job["action"] == "dismiss" else "acted"
    _apply_job_resolution(job, resolution_status, only_owner=False)
    return {"status": "ok", "already_queued": False, "job": _public_job(job)}


@router.get("/api/admin/safety/actions")
def list_safety_actions(
    admin_secret: str,
    status: str = "pending",
    limit: int = 50,
):
    _require_admin(admin_secret)
    wanted = (status or "pending").strip().lower()
    allowed = {"pending", "claimed", "completed", "failed", "open", "all"}
    if wanted not in allowed:
        raise _http(400, "status must be pending, claimed, completed, failed, open, or all")
    limit = max(1, min(int(limit or 50), 200))
    with _LOCK:
        jobs = [job for job in _action_jobs() if isinstance(job, dict)]
    if wanted == "open":
        jobs = [job for job in jobs if job.get("status") in {"pending", "claimed"}]
    elif wanted != "all":
        jobs = [job for job in jobs if job.get("status") == wanted]
    jobs = list(reversed(jobs))[:limit]
    return {"status": "ok", "jobs": [_public_job(job) for job in jobs]}


@router.get("/api/admin/safety/actions/{job_id}")
def get_safety_action(job_id: str, admin_secret: str):
    _require_admin(admin_secret)
    job_id = _record_id(job_id, field="job_id") or ""
    with _LOCK:
        job = _find_job(job_id)
        if not job:
            raise _http(404, "Admin action job not found")
        return {"status": "ok", "job": _public_job(job)}


@router.post("/api/bot-sync/admin-jobs/{job_id}/claim")
def claim_safety_action(
    job_id: str,
    x_bot_sync_secret: str | None = Header(default=None),
):
    _require_bot(x_bot_sync_secret)
    job_id = _record_id(job_id, field="job_id") or ""
    with _LOCK:
        job = _find_job(job_id)
        if not job:
            raise _http(404, "Admin action job not found")
        status = job.get("status")
        if status in TERMINAL_JOB_STATUSES:
            raise _http(409, "Job is already finished")
        if status == "claimed" and not _claim_is_stale(job):
            raise _http(409, "Job is already claimed")
        job["status"] = "claimed"
        job["claimed_at"] = _now()
        _persist_jobs()
        return {"status": "ok", "job": _public_job(job)}


@router.post("/api/bot-sync/admin-jobs/{job_id}/complete")
def complete_safety_action(
    job_id: str,
    payload: SafetyJobCompletePayload | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    _require_bot(x_bot_sync_secret)
    job_id = _record_id(job_id, field="job_id") or ""
    payload = payload or SafetyJobCompletePayload()
    error = _clean_text(payload.error, limit=MAX_ERROR_LEN, field="error")
    with _LOCK:
        job = _find_job(job_id)
        if not job:
            raise _http(404, "Admin action job not found")
        current = job.get("status")
        if current in TERMINAL_JOB_STATUSES:
            if current == payload.status:
                return {"status": "ok", "already_done": True, "job": _public_job(job)}
            raise _http(409, "Job is already finished")
        if current not in {"pending", "claimed"}:
            raise _http(409, "Job cannot be completed from its current status")
        job["status"] = payload.status
        job["completed_at"] = _now()
        job["result"] = payload.result if isinstance(payload.result, dict) else None
        job["error"] = error if payload.status == "failed" else None
        _persist_jobs()
        finished = _public_job(job)
    if payload.status == "failed":
        _apply_job_resolution(job, "open", only_owner=True)
    else:
        resolution_status = "dismissed" if job.get("action") == "dismiss" else "acted"
        _apply_job_resolution(job, resolution_status, only_owner=False)
    return {"status": "ok", "already_done": False, "job": finished}


@router.post("/api/bot-sync/care-reports")
def create_care_report(
    payload: CareReportCreatePayload,
    x_bot_sync_secret: str | None = Header(default=None),
):
    _require_bot(x_bot_sync_secret)
    if payload.reason not in CARE_REASONS:
        raise _http(400, "reason is not supported")
    if payload.source not in ACTION_SOURCES:
        raise _http(400, "source is not supported")
    reporter_id = _bounded_int(payload.reporter_id, field="reporter_id", required=True)
    target_user_id = _bounded_int(payload.target_user_id, field="target_user_id", required=True)
    report = {
        "id": uuid.uuid4().hex,
        "reporter_id": reporter_id,
        "target_user_id": target_user_id,
        "chat_id": _bounded_int(payload.chat_id, field="chat_id", allow_negative=True),
        "message_id": _bounded_int(payload.message_id, field="message_id"),
        "reason": payload.reason,
        "note": _clean_text(payload.note, limit=MAX_NOTE_LEN, field="note"),
        "created_at": _now(),
        "status": "pending",
        "resolved_at": None,
        "admin_user_id": None,
        "decision": None,
        "source": payload.source,
    }
    conn = _db()
    try:
        conn.execute(
            """
            INSERT INTO care_reports (
                id, reporter_id, target_user_id, chat_id, message_id, reason, note,
                created_at, status, resolved_at, admin_user_id, decision, source
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report["id"],
                report["reporter_id"],
                report["target_user_id"],
                report["chat_id"],
                report["message_id"],
                report["reason"],
                report["note"],
                report["created_at"],
                report["status"],
                report["resolved_at"],
                report["admin_user_id"],
                report["decision"],
                report["source"],
            ),
        )
        activity = _reporter_activity(conn, int(reporter_id))
        conn.commit()
    finally:
        conn.close()
    return {"status": "ok", "report": _public_care_report(report), "reporter_activity": activity}


@router.get("/api/admin/safety/care-reports")
def list_care_reports(
    admin_secret: str,
    status: str = "all",
    period: str | None = None,
    reporter_id: int | None = None,
    limit: int = 80,
):
    _require_admin(admin_secret)
    wanted = (status or "all").strip().lower()
    allowed = {"all", "pending", "kept", "removed", "taught"}
    if wanted not in allowed:
        raise _http(400, "status must be all, pending, kept, removed, or taught")
    since = None
    normalized_period = None
    if period:
        from . import main

        normalized_period = main.group_activity_period(period)
        since = main.group_activity_since(normalized_period)
    reporter = _bounded_int(reporter_id, field="reporter_id")
    limit = max(1, min(int(limit or 80), 200))
    clauses = []
    params: list[Any] = []
    if wanted != "all":
        clauses.append("status = ?")
        params.append(wanted)
    if since:
        clauses.append("created_at >= ?")
        params.append(since)
    if reporter is not None:
        clauses.append("reporter_id = ?")
        params.append(reporter)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    conn = _db()
    try:
        rows = conn.execute(
            f"""
            SELECT * FROM care_reports
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params) + (limit,),
        ).fetchall()
        activity = _reporter_activity(conn, reporter) if reporter is not None else None
    finally:
        conn.close()
    return {
        "status": "ok",
        "period": normalized_period,
        "since": since,
        "reports": [_public_care_report(dict(row)) for row in rows],
        "reporter_activity": activity,
    }


def _insert_remove_grant(conn: sqlite3.Connection, report: dict) -> dict:
    existing = conn.execute(
        "SELECT * FROM exp_grants WHERE source = ? AND source_id = ?",
        ("care_report", report["id"]),
    ).fetchone()
    if existing:
        return dict(existing)
    grant = {
        "id": uuid.uuid4().hex,
        "user_id": report["reporter_id"],
        "amount": CARE_REMOVE_EXP,
        "reason": "care_report_remove",
        "source": "care_report",
        "source_id": report["id"],
        "status": "pending",
        "created_at": _now(),
        "applied_at": None,
        "result_json": None,
        "error": None,
    }
    conn.execute(
        """
        INSERT INTO exp_grants (
            id, user_id, amount, reason, source, source_id, status, created_at,
            applied_at, result_json, error
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            grant["id"],
            grant["user_id"],
            grant["amount"],
            grant["reason"],
            grant["source"],
            grant["source_id"],
            grant["status"],
            grant["created_at"],
            grant["applied_at"],
            grant["result_json"],
            grant["error"],
        ),
    )
    return grant


@router.post("/api/admin/safety/care-reports/{report_id}/resolve")
def resolve_care_report(report_id: str, payload: CareReportResolvePayload):
    _require_admin(payload.admin_secret)
    report_id = _record_id(report_id, field="report_id") or ""
    decision = payload.decision
    if decision not in CARE_DECISIONS:
        raise _http(400, "decision must be keep, remove, or teach")
    admin_user_id = _bounded_int(payload.admin_user_id, field="admin_user_id", required=True)
    new_status = CARE_DECISIONS[decision]
    conn = _db()
    try:
        current = _load_care_report(conn, report_id)
        if not current:
            conn.rollback()
            raise _http(404, "Care report not found")
        if current.get("status") != "pending":
            conn.rollback()
            raise _http(409, "Care report is already resolved")
        resolved_at = _now()
        updated = conn.execute(
            """
            UPDATE care_reports
            SET status = ?, decision = ?, resolved_at = ?, admin_user_id = ?
            WHERE id = ? AND status = 'pending'
            """,
            (new_status, decision, resolved_at, admin_user_id, report_id),
        )
        if updated.rowcount != 1:
            conn.rollback()
            raise _http(409, "Care report is already resolved")
        current["status"] = new_status
        current["decision"] = decision
        current["resolved_at"] = resolved_at
        current["admin_user_id"] = admin_user_id
        grant = _insert_remove_grant(conn, current) if decision == "remove" else None
        _set_resolution(conn, f"care_report:{report_id}", "acted", None, f"care_{decision}")
        conn.commit()
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "status": "ok",
        "report": _public_care_report(current),
        "exp_grant": _public_grant(grant) if grant else None,
    }


@router.get("/api/exp-grants")
def member_pending_exp_grants(user_id: int, limit: int = 100):
    """Pending grants for one member. Open like other member GETs that take user_id."""
    owner = _bounded_int(user_id, field="user_id", required=True)
    limit = max(1, min(int(limit or 100), 200))
    conn = _db()
    try:
        rows = conn.execute(
            """
            SELECT * FROM exp_grants
            WHERE status = 'pending' AND user_id = ?
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (owner, limit),
        ).fetchall()
    finally:
        conn.close()
    return {"status": "ok", "grants": [_public_grant(dict(row)) for row in rows]}


@router.post("/api/exp-grants/{grant_id}/claim")
def claim_exp_grant(grant_id: str, payload: ExpGrantClaimPayload):
    """Mark a grant applied after the member's own profile write succeeds."""
    grant_id = _record_id(grant_id, field="grant_id") or ""
    claimer = _bounded_int(payload.user_id, field="user_id", required=True)
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM exp_grants WHERE id = ?", (grant_id,)).fetchone()
        if not row:
            conn.rollback()
            raise _http(404, "EXP grant not found")
        current = dict(row)
        if int(current.get("user_id") or 0) != int(claimer):
            conn.rollback()
            raise _http(403, "EXP grant belongs to another member")
        if current.get("status") == "applied":
            conn.rollback()
            return {"status": "ok", "already_done": True, "grant": _public_grant(current)}
        if current.get("status") != "pending":
            conn.rollback()
            raise _http(409, "EXP grant is already finished")
        applied_at = _now()
        updated = conn.execute(
            """
            UPDATE exp_grants
            SET status = 'applied', applied_at = ?, error = NULL
            WHERE id = ? AND status = 'pending' AND user_id = ?
            """,
            (applied_at, grant_id, claimer),
        )
        if updated.rowcount != 1:
            conn.rollback()
            raise _http(409, "EXP grant is already finished")
        conn.commit()
        current["status"] = "applied"
        current["applied_at"] = applied_at
        current["error"] = None
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"status": "ok", "already_done": False, "grant": _public_grant(current)}


@router.get("/api/bot-sync/exp-grants")
def bot_pending_exp_grants(
    x_bot_sync_secret: str | None = Header(default=None),
    limit: int = 100,
):
    _require_bot(x_bot_sync_secret)
    limit = max(1, min(int(limit or 100), 200))
    conn = _db()
    try:
        rows = conn.execute(
            """
            SELECT * FROM exp_grants
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    return {"status": "ok", "grants": [_public_grant(dict(row)) for row in rows]}


@router.get("/api/admin/safety/exp-grants")
def admin_exp_grants(admin_secret: str, status: str = "pending", limit: int = 100):
    _require_admin(admin_secret)
    wanted = (status or "pending").strip().lower()
    if wanted not in {"pending", "applied", "failed", "all"}:
        raise _http(400, "status must be pending, applied, failed, or all")
    limit = max(1, min(int(limit or 100), 200))
    clauses = []
    params: list[Any] = []
    if wanted != "all":
        clauses.append("status = ?")
        params.append(wanted)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    conn = _db()
    try:
        rows = conn.execute(
            f"""
            SELECT * FROM exp_grants
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            tuple(params) + (limit,),
        ).fetchall()
    finally:
        conn.close()
    return {"status": "ok", "grants": [_public_grant(dict(row)) for row in rows]}


@router.post("/api/bot-sync/exp-grants/{grant_id}/complete")
def complete_exp_grant(
    grant_id: str,
    payload: ExpGrantCompletePayload,
    x_bot_sync_secret: str | None = Header(default=None),
):
    _require_bot(x_bot_sync_secret)
    grant_id = _record_id(grant_id, field="grant_id") or ""
    error = _clean_text(payload.error, limit=MAX_ERROR_LEN, field="error")
    result_json = json.dumps(payload.result, separators=(",", ":"), sort_keys=True) if payload.result else None
    conn = _db()
    try:
        row = conn.execute("SELECT * FROM exp_grants WHERE id = ?", (grant_id,)).fetchone()
        if not row:
            conn.rollback()
            raise _http(404, "EXP grant not found")
        current = dict(row)
        if current.get("status") in {"applied", "failed"}:
            conn.rollback()
            if current.get("status") == payload.status:
                return {"status": "ok", "already_done": True, "grant": _public_grant(current)}
            raise _http(409, "EXP grant is already finished")
        applied_at = _now()
        stored_error = error if payload.status == "failed" else None
        updated = conn.execute(
            """
            UPDATE exp_grants
            SET status = ?, applied_at = ?, result_json = ?, error = ?
            WHERE id = ? AND status = 'pending'
            """,
            (payload.status, applied_at, result_json, stored_error, grant_id),
        )
        if updated.rowcount != 1:
            conn.rollback()
            raise _http(409, "EXP grant is already finished")
        conn.commit()
        current["status"] = payload.status
        current["applied_at"] = applied_at
        current["result_json"] = result_json
        current["error"] = stored_error
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"status": "ok", "already_done": False, "grant": _public_grant(current)}
