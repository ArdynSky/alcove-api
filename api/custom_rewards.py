from __future__ import annotations

import datetime
import json
import os
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/custom-packs", tags=["custom-packs"])
_LOCK = threading.RLock()


def _data_dir() -> Path:
    for key in ("ALCOVE_STATE_DB_PATH", "ALCOVE_RUNTIME_STATE_PATH", "FOX_LOGS_DB_PATH"):
        raw = os.getenv(key, "").strip()
        if raw:
            return Path(raw).expanduser().resolve().parent
    return Path.cwd()


DB_PATH = Path(os.getenv("ALCOVE_CUSTOM_PACKS_DB", str(_data_dir() / "custom_reward_packs.sqlite3")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE IF NOT EXISTS custom_pack_templates (id TEXT PRIMARY KEY, name TEXT NOT NULL, description TEXT NOT NULL, items_json TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS custom_pack_grants (id TEXT PRIMARY KEY, template_id TEXT, target_user_id TEXT, target_username TEXT, pack_json TEXT NOT NULL, sent_at TEXT NOT NULL, claimed_at TEXT)")
    con.commit()
    return con


def _admin(secret: str | None):
    expected = os.getenv("BOT_SYNC_SECRET", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Admin secret is not configured")
    if not secret or secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


def _clean_items(items):
    allowed = {"color", "sticker", "skin", "backdrop", "title"}
    out = []
    for raw in items or []:
        if not isinstance(raw, dict):
            continue
        kind = str(raw.get("type") or "").strip().lower()
        item_id = str(raw.get("id") or "").strip()
        if kind not in allowed or not item_id:
            continue
        item = {"type": kind, "id": item_id[:120]}
        for key in ("name", "label", "image", "layout"):
            value = str(raw.get(key) or "").strip()
            if value:
                item[key] = value[:500]
        if kind == "skin":
            try:
                opacity_percent = int(round(float(raw.get("opacity_percent", 20))))
            except (TypeError, ValueError):
                opacity_percent = 20
            item["opacity_percent"] = max(10, min(opacity_percent, 30))
        out.append(item)
    if not out:
        raise HTTPException(status_code=400, detail="Add at least one reward item")
    return out[:30]


def _pack(row):
    item = dict(row)
    item["items"] = json.loads(item.pop("items_json") or "[]")
    return item


class PackPayload(BaseModel):
    admin_secret: str
    name: str
    description: str = ""
    items: list[dict]


class SendPayload(BaseModel):
    admin_secret: str
    template_id: str
    user_id: Optional[str] = None
    username: Optional[str] = None


class ClaimPayload(BaseModel):
    grant_id: str
    user_id: Optional[str] = None
    username: Optional[str] = None


@router.get("/admin/templates")
def list_templates(admin_secret: str):
    _admin(admin_secret)
    with _LOCK:
        con = _conn()
        rows = con.execute("SELECT * FROM custom_pack_templates ORDER BY updated_at DESC, name COLLATE NOCASE").fetchall()
        con.close()
    return {"templates": [_pack(row) for row in rows]}


@router.post("/admin/templates")
def create_template(payload: PackPayload):
    _admin(payload.admin_secret)
    name = (payload.name or "Custom Pack").strip()[:120]
    items = _clean_items(payload.items)
    pack_id = str(uuid.uuid4())
    now = _now()
    with _LOCK:
        con = _conn()
        con.execute("INSERT INTO custom_pack_templates(id,name,description,items_json,created_at,updated_at) VALUES(?,?,?,?,?,?)", (pack_id, name, (payload.description or "")[:500], json.dumps(items), now, now))
        con.commit()
        row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (pack_id,)).fetchone()
        con.close()
    return _pack(row)


@router.put("/admin/templates/{pack_id}")
def update_template(pack_id: str, payload: PackPayload):
    _admin(payload.admin_secret)
    items = _clean_items(payload.items)
    with _LOCK:
        con = _conn()
        exists = con.execute("SELECT id FROM custom_pack_templates WHERE id=?", (pack_id,)).fetchone()
        if not exists:
            con.close()
            raise HTTPException(status_code=404, detail="Custom pack not found")
        con.execute("UPDATE custom_pack_templates SET name=?,description=?,items_json=?,updated_at=? WHERE id=?", ((payload.name or "Custom Pack")[:120], (payload.description or "")[:500], json.dumps(items), _now(), pack_id))
        con.commit()
        row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (pack_id,)).fetchone()
        con.close()
    return _pack(row)


@router.delete("/admin/templates/{pack_id}")
def delete_template(pack_id: str, admin_secret: str):
    _admin(admin_secret)
    with _LOCK:
        con = _conn()
        con.execute("DELETE FROM custom_pack_templates WHERE id=?", (pack_id,))
        con.commit()
        con.close()
    return {"ok": True}


@router.post("/admin/send")
def send_pack(payload: SendPayload):
    _admin(payload.admin_secret)
    user_id = str(payload.user_id or "").strip()
    username = str(payload.username or "").strip().lstrip("@").lower()
    if not user_id and not username:
        raise HTTPException(status_code=400, detail="Choose a Telegram user id or username")
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (payload.template_id,)).fetchone()
        if not row:
            con.close()
            raise HTTPException(status_code=404, detail="Custom pack not found")
        snapshot = _pack(row)
        grant_id = str(uuid.uuid4())
        con.execute("INSERT INTO custom_pack_grants(id,template_id,target_user_id,target_username,pack_json,sent_at,claimed_at) VALUES(?,?,?,?,?,?,NULL)", (grant_id, payload.template_id, user_id or None, username or None, json.dumps(snapshot), _now()))
        con.commit()
        con.close()
    return {"ok": True, "grant_id": grant_id, "pack": snapshot}


@router.get("/pending")
def pending_packs(user_id: Optional[str] = None, username: Optional[str] = None):
    uid = str(user_id or "").strip()
    uname = str(username or "").strip().lstrip("@").lower()
    if not uid and not uname:
        return {"grants": []}
    clauses = []
    args = []
    if uid:
        clauses.append("target_user_id=?")
        args.append(uid)
    if uname:
        clauses.append("LOWER(target_username)=?")
        args.append(uname)
    with _LOCK:
        con = _conn()
        rows = con.execute(f"SELECT * FROM custom_pack_grants WHERE claimed_at IS NULL AND ({' OR '.join(clauses)}) ORDER BY sent_at ASC", args).fetchall()
        con.close()
    grants = []
    for row in rows:
        item = dict(row)
        item["pack"] = json.loads(item.pop("pack_json") or "{}")
        grants.append(item)
    return {"grants": grants}


@router.post("/claim")
def claim_pack(payload: ClaimPayload):
    uid = str(payload.user_id or "").strip()
    uname = str(payload.username or "").strip().lstrip("@").lower()
    with _LOCK:
        con = _conn()
        row = con.execute("SELECT * FROM custom_pack_grants WHERE id=?", (payload.grant_id,)).fetchone()
        if not row:
            con.close()
            raise HTTPException(status_code=404, detail="Grant not found")
        if row["claimed_at"]:
            con.close()
            return {"ok": True, "already_claimed": True}
        matches = bool(uid and row["target_user_id"] and uid == str(row["target_user_id"])) or bool(uname and row["target_username"] and uname == str(row["target_username"]).lower())
        if not matches:
            con.close()
            raise HTTPException(status_code=403, detail="This pack belongs to another member")
        con.execute("UPDATE custom_pack_grants SET claimed_at=? WHERE id=?", (_now(), payload.grant_id))
        con.commit()
        con.close()
    return {"ok": True}
