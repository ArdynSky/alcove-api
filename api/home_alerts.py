from __future__ import annotations

import datetime
import os
import sqlite3
from typing import Literal

from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter()

STATE_DB_PATH = os.getenv(
    "ALCOVE_STATE_DB_PATH",
    os.path.join(os.getcwd(), "alcove_state.db"),
)

ALERT_TYPES = {
    "spotlight_awarded": ("connect", "Spotlight awarded!"),
    "pulse_question_answered": ("connect", "Pulse question answered!"),
    "level_up": ("profile", "Level up!"),
    "achievement_unlocked": ("profile", "Achievement unlocked!"),
}


def _now_iso() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


def _ensure_store() -> None:
    directory = os.path.dirname(STATE_DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS home_alerts (
                user_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                alert_type TEXT NOT NULL,
                surface TEXT NOT NULL,
                created_at TEXT NOT NULL,
                consumed_at TEXT,
                PRIMARY KEY (user_id, event_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_home_alerts_active ON home_alerts(user_id, surface, consumed_at)"
        )
        conn.commit()


def _trim_old_rows() -> None:
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=90)).isoformat() + "Z"
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            "DELETE FROM home_alerts WHERE COALESCE(consumed_at, created_at) < ?",
            (cutoff,),
        )
        conn.commit()


class HomeAlertPush(BaseModel):
    user_id: str
    type: str
    event_id: str


class HomeAlertConsume(BaseModel):
    user_id: str
    surface: Literal["connect", "profile"] | None = None


@router.post("/api/home-alerts")
def push_home_alert(payload: HomeAlertPush):
    user_id = str(payload.user_id or "").strip()
    event_id = str(payload.event_id or "").strip()
    alert_type = str(payload.type or "").strip()
    if not user_id or not event_id or alert_type not in ALERT_TYPES:
        return {"status": "error", "error": "invalid_alert"}

    surface, _label = ALERT_TYPES[alert_type]
    _ensure_store()
    created_at = _now_iso()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO home_alerts (user_id, event_id, alert_type, surface, created_at, consumed_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            ON CONFLICT(user_id, event_id) DO NOTHING
            """,
            (user_id, event_id, alert_type, surface, created_at),
        )
        conn.commit()
    _trim_old_rows()
    return {"status": "ok"}


@router.get("/api/home-alerts")
def list_home_alerts(
    user_id: str = Query(...),
    surface: Literal["connect", "profile"] | None = Query(None),
):
    user_id = str(user_id or "").strip()
    if not user_id:
        return {"status": "ok", "alerts": []}

    _ensure_store()
    clauses = ["user_id = ?", "consumed_at IS NULL"]
    params: list[str] = [user_id]
    if surface:
        clauses.append("surface = ?")
        params.append(surface)

    with sqlite3.connect(STATE_DB_PATH) as conn:
        rows = conn.execute(
            f"""
            SELECT event_id, alert_type, surface, created_at
            FROM home_alerts
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC
            LIMIT 100
            """,
            params,
        ).fetchall()

    alerts = []
    for event_id, alert_type, alert_surface, created_at in rows:
        definition = ALERT_TYPES.get(alert_type)
        if not definition:
            continue
        alerts.append(
            {
                "eventId": event_id,
                "type": alert_type,
                "surface": alert_surface,
                "label": definition[1],
                "at": created_at,
            }
        )
    return {"status": "ok", "alerts": alerts}


@router.post("/api/home-alerts/consume")
def consume_home_alerts(payload: HomeAlertConsume):
    user_id = str(payload.user_id or "").strip()
    if not user_id:
        return {"status": "ok", "consumed": 0}

    _ensure_store()
    clauses = ["user_id = ?", "consumed_at IS NULL"]
    params: list[str] = [user_id]
    if payload.surface:
        clauses.append("surface = ?")
        params.append(payload.surface)

    with sqlite3.connect(STATE_DB_PATH) as conn:
        cursor = conn.execute(
            f"UPDATE home_alerts SET consumed_at = ? WHERE {' AND '.join(clauses)}",
            [_now_iso(), *params],
        )
        conn.commit()
        consumed = cursor.rowcount if cursor.rowcount is not None else 0
    return {"status": "ok", "consumed": max(0, int(consumed))}
