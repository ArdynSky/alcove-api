from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import sqlite3
import threading
import uuid
from typing import Any, Literal, Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ConfigDict, Field, model_validator


router = APIRouter(prefix="/api/agendas", tags=["agendas"])


def _data_dir() -> Path:
    for key in ("ALCOVE_STATE_DB_PATH", "ALCOVE_RUNTIME_STATE_PATH", "FOX_LOGS_DB_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw).expanduser().resolve().parent
    return Path.cwd()


DB_PATH = Path(os.getenv("ALCOVE_AGENDA_DB", str(_data_dir() / "agendas.sqlite3")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
_LOCK = threading.RLock()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute(
        "CREATE TABLE IF NOT EXISTS agendas ("
        "id TEXT PRIMARY KEY, title TEXT NOT NULL, event_date TEXT NOT NULL, "
        "start_time TEXT NOT NULL, timezone TEXT NOT NULL, status TEXT NOT NULL, "
        "show_in_app INTEGER NOT NULL DEFAULT 0, items_json TEXT NOT NULL, "
        "created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
    )
    columns = {row[1] for row in con.execute("PRAGMA table_info(agendas)").fetchall()}
    if "current_item_id" not in columns:
        con.execute("ALTER TABLE agendas ADD COLUMN current_item_id TEXT")
    if "completed_item_ids_json" not in columns:
        con.execute("ALTER TABLE agendas ADD COLUMN completed_item_ids_json TEXT NOT NULL DEFAULT '[]'")
    if "session_name" not in columns:
        con.execute("ALTER TABLE agendas ADD COLUMN session_name TEXT NOT NULL DEFAULT ''")
    if "schedule_image_url" not in columns:
        con.execute("ALTER TABLE agendas ADD COLUMN schedule_image_url TEXT NOT NULL DEFAULT ''")
    if "holding_video_url" not in columns:
        con.execute("ALTER TABLE agendas ADD COLUMN holding_video_url TEXT NOT NULL DEFAULT ''")
    con.commit()
    return con


def _clean_url(value: Any) -> str:
    return str(value or "").strip()[:500]


def _first_text(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


class AgendaItem(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    content_id: Optional[str] = None
    content_snapshot: Optional[dict] = None
    kind: str = "activity"
    title: str
    duration_minutes: int = Field(default=10, ge=1, le=480)


class AgendaPayload(BaseModel):
    title: str
    session_name: str = ""
    event_date: str
    start_time: str
    timezone: str = "Europe/London"
    status: Literal["draft", "future", "active", "finished"] = "draft"
    show_in_app: bool = False
    schedule_image_url: str = ""
    holding_video_url: str = ""
    items: list[AgendaItem] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _media_aliases(cls, data: Any):
        if not isinstance(data, dict):
            return data
        payload = dict(data)
        payload["schedule_image_url"] = _clean_url(
            payload.get("schedule_image_url")
            or payload.get("schedule_image")
            or payload.get("holding_backdrop_url")
            or payload.get("backdrop_url")
            or payload.get("image")
        )
        payload["holding_video_url"] = _clean_url(
            payload.get("holding_video_url")
            or payload.get("holdingVideoUrl")
            or payload.get("background_video_url")
            or payload.get("backgroundVideoUrl")
        )
        payload["session_name"] = _first_text(payload.get("session_name"), payload.get("title"))[:160]
        return payload


class AgendaStatusPayload(BaseModel):
    status: Literal["draft", "future", "active", "finished"]


class AgendaProgressPayload(BaseModel):
    action: Literal["activate", "advance", "complete", "reset"]
    item_id: Optional[str] = None


def _validate(payload: AgendaPayload):
    try:
        ZoneInfo(payload.timezone)
    except ZoneInfoNotFoundError:
        raise HTTPException(status_code=400, detail="Use a valid IANA timezone")
    try:
        datetime.strptime(payload.event_date, "%Y-%m-%d")
        datetime.strptime(payload.start_time, "%H:%M")
    except ValueError:
        raise HTTPException(status_code=400, detail="Use YYYY-MM-DD and HH:MM")
    if not payload.title.strip():
        raise HTTPException(status_code=400, detail="Agenda title is required")


def _public(row):
    data = dict(row)
    data["show_in_app"] = bool(data["show_in_app"])
    data["items"] = json.loads(data.pop("items_json") or "[]")
    data["completed_item_ids"] = json.loads(data.pop("completed_item_ids_json", "[]") or "[]")
    start = datetime.strptime(
        f"{data['event_date']} {data['start_time']}", "%Y-%m-%d %H:%M"
    ).replace(tzinfo=ZoneInfo(data["timezone"]))
    cursor = start
    total = 0
    for item in data["items"]:
        item["starts_at"] = cursor.astimezone(timezone.utc).isoformat()
        duration = max(1, int(item.get("duration_minutes") or 1))
        total += duration
        cursor += timedelta(minutes=duration)
    data["starts_at"] = start.astimezone(timezone.utc).isoformat()
    data["finishes_at"] = cursor.astimezone(timezone.utc).isoformat()
    data["total_duration_minutes"] = total
    return data


def _get(agenda_id: str):
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT * FROM agendas WHERE id=?", (agenda_id,)).fetchone()
        con.close()
    if not row:
        raise HTTPException(status_code=404, detail="Agenda not found")
    return row


@router.get("")
def list_agendas():
    with _LOCK:
        con = _conn()
        rows = con.execute(
            "SELECT * FROM agendas ORDER BY event_date DESC,start_time DESC,updated_at DESC"
        ).fetchall()
        con.close()
    return {"agendas": [_public(row) for row in rows]}


@router.get("/active")
def active_agenda():
    with _LOCK:
        con = _conn()
        row = con.execute(
            "SELECT * FROM agendas WHERE status='active' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
        con.close()
    return {"agenda": _public(row) if row else None}


@router.get("/{agenda_id}")
def get_agenda(agenda_id: str):
    return _public(_get(agenda_id))


@router.post("")
def create_agenda(payload: AgendaPayload):
    _validate(payload)
    agenda_id, now = str(uuid.uuid4()), _now()
    if payload.status == "active":
        _deactivate_all()
    with _LOCK:
        con = _conn()
        con.execute(
            "INSERT INTO agendas(id,title,session_name,event_date,start_time,timezone,status,show_in_app,schedule_image_url,holding_video_url,items_json,created_at,updated_at,current_item_id,completed_item_ids_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,'[]')",
            (agenda_id, payload.title.strip()[:160], (payload.session_name or payload.title).strip()[:160],
             payload.event_date, payload.start_time, payload.timezone, payload.status, int(payload.show_in_app),
             _clean_url(payload.schedule_image_url), _clean_url(payload.holding_video_url),
             json.dumps([x.model_dump(exclude={"starts_at"}) for x in payload.items]), now, now),
        )
        con.commit()
        con.close()
    return _public(_get(agenda_id))


@router.put("/{agenda_id}")
def update_agenda(agenda_id: str, payload: AgendaPayload):
    _get(agenda_id)
    _validate(payload)
    if payload.status == "active":
        _deactivate_all(except_id=agenda_id)
    with _LOCK:
        con = _conn()
        con.execute(
            "UPDATE agendas SET title=?,session_name=?,event_date=?,start_time=?,timezone=?,status=?,"
            "show_in_app=?,schedule_image_url=?,holding_video_url=?,items_json=?,updated_at=? WHERE id=?",
            (payload.title.strip()[:160], (payload.session_name or payload.title).strip()[:160],
             payload.event_date, payload.start_time, payload.timezone, payload.status, int(payload.show_in_app),
             _clean_url(payload.schedule_image_url), _clean_url(payload.holding_video_url),
             json.dumps([x.model_dump(exclude={"starts_at"}) for x in payload.items]), _now(), agenda_id),
        )
        con.commit()
        con.close()
    return _public(_get(agenda_id))


def _deactivate_all(except_id: Optional[str] = None):
    with _LOCK:
        con = _conn()
        if except_id:
            con.execute("UPDATE agendas SET status='finished',updated_at=? WHERE status='active' AND id<>?", (_now(), except_id))
        else:
            con.execute("UPDATE agendas SET status='finished',updated_at=? WHERE status='active'", (_now(),))
        con.commit()
        con.close()


@router.post("/{agenda_id}/status")
def set_status(agenda_id: str, payload: AgendaStatusPayload):
    _get(agenda_id)
    if payload.status == "active":
        _deactivate_all(except_id=agenda_id)
    with _LOCK:
        con = _conn()
        con.execute("UPDATE agendas SET status=?,updated_at=? WHERE id=?", (payload.status, _now(), agenda_id))
        con.commit()
        con.close()
    return _public(_get(agenda_id))


@router.post("/{agenda_id}/progress")
def set_progress(agenda_id: str, payload: AgendaProgressPayload):
    agenda = _public(_get(agenda_id))
    if agenda["status"] != "active":
        raise HTTPException(status_code=409, detail="Activate the agenda before running it")
    item_ids = [str(item.get("id")) for item in agenda["items"]]
    item_id = str(payload.item_id or "").strip() or None
    if item_id and item_id not in item_ids:
        raise HTTPException(status_code=404, detail="Agenda item not found")
    current = agenda.get("current_item_id")
    completed = list(dict.fromkeys(str(x) for x in agenda.get("completed_item_ids", [])))
    if payload.action == "reset":
        current, completed = None, []
    elif payload.action == "activate":
        if not item_id:
            raise HTTPException(status_code=400, detail="Choose an agenda item")
        current = item_id
    elif payload.action == "advance":
        if current and current not in completed:
            completed.append(current)
        if not item_id:
            raise HTTPException(status_code=400, detail="Choose the next agenda item")
        current = item_id
    else:
        target = item_id or current
        if target and target not in completed:
            completed.append(target)
        current = None if target == current else current
    with _LOCK:
        con = _conn()
        con.execute(
            "UPDATE agendas SET current_item_id=?,completed_item_ids_json=?,updated_at=? WHERE id=?",
            (current, json.dumps(completed), _now(), agenda_id),
        )
        con.commit()
        con.close()
    return _public(_get(agenda_id))


@router.post("/{agenda_id}/duplicate")
def duplicate_agenda(agenda_id: str):
    source = _public(_get(agenda_id))
    payload = AgendaPayload(
        title=f"{source['title']} Copy",
        session_name=source.get("session_name") or source["title"],
        event_date=source["event_date"],
        start_time=source["start_time"],
        timezone=source["timezone"],
        status="draft",
        show_in_app=False,
        schedule_image_url=source.get("schedule_image_url") or "",
        holding_video_url=source.get("holding_video_url") or "",
        items=[AgendaItem(**{k: v for k, v in item.items() if k != "starts_at"}) for item in source["items"]],
    )
    return create_agenda(payload)


@router.delete("/{agenda_id}")
def delete_agenda(agenda_id: str):
    _get(agenda_id)
    with _LOCK:
        con = _conn()
        con.execute("DELETE FROM agendas WHERE id=?", (agenda_id,))
        con.commit()
        con.close()
    return {"ok": True, "id": agenda_id}
