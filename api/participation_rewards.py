from __future__ import annotations

import json
import random
import sqlite3
import uuid
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from . import custom_rewards as cr

router = APIRouter(prefix="/participation", tags=["participation-rewards"])


class ParticipationAwardPayload(BaseModel):
    user_id: Optional[str] = None
    username: Optional[str] = None
    event_id: str
    activity: str = "live_room"


def _ensure_tables(con: sqlite3.Connection):
    con.execute(
        "CREATE TABLE IF NOT EXISTS participation_reward_config ("
        "id INTEGER PRIMARY KEY CHECK(id=1), template_id TEXT, popup_title TEXT NOT NULL DEFAULT 'Participation Pack', "
        "popup_copy TEXT NOT NULL DEFAULT 'Thanks for taking part! You unlocked a new customisation.', updated_at TEXT NOT NULL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS participation_reward_awards ("
        "id TEXT PRIMARY KEY, user_key TEXT NOT NULL, event_id TEXT NOT NULL, activity TEXT NOT NULL, "
        "item_key TEXT NOT NULL, grant_id TEXT NOT NULL, awarded_at TEXT NOT NULL, "
        "UNIQUE(user_key,event_id), UNIQUE(user_key,item_key))"
    )
    con.commit()


def _user_key(user_id: str, username: str) -> str:
    if user_id:
        return f"id:{user_id}"
    if username:
        return f"username:{username.lower()}"
    return ""


def _participation_template(con: sqlite3.Connection):
    _ensure_tables(con)
    cfg = con.execute("SELECT * FROM participation_reward_config WHERE id=1").fetchone()
    row = None
    if cfg and cfg["template_id"]:
        row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (cfg["template_id"],)).fetchone()
    if not row:
        row = con.execute(
            "SELECT * FROM custom_pack_templates WHERE LOWER(name)='participation pack' ORDER BY updated_at DESC LIMIT 1"
        ).fetchone()
    return cfg, row


def _public_config(con: sqlite3.Connection):
    cfg, row = _participation_template(con)
    if not row:
        return {
            "enabled": False,
            "template_id": None,
            "popup_title": "Participation Pack",
            "popup_copy": "Thanks for taking part! You unlocked a new customisation.",
            "image": "",
            "item_count": 0,
        }
    pack = cr._pack(row)
    return {
        "enabled": bool(pack.get("items")),
        "template_id": pack.get("id"),
        "popup_title": (cfg["popup_title"] if cfg else None) or "Participation Pack",
        "popup_copy": (cfg["popup_copy"] if cfg else None) or pack.get("description") or "Thanks for taking part! You unlocked a new customisation.",
        "image": pack.get("image") or "",
        "item_count": len(pack.get("items") or []),
    }


@router.get("/config")
def participation_config():
    with cr._LOCK:
        con = cr._conn()
        try:
            return _public_config(con)
        finally:
            con.close()


@router.get("/admin")
def participation_admin(admin_secret: str):
    cr._admin(admin_secret)
    with cr._LOCK:
        con = cr._conn()
        try:
            _ensure_tables(con)
            cfg, row = _participation_template(con)
            templates = con.execute("SELECT id,name,description,image,updated_at FROM custom_pack_templates ORDER BY updated_at DESC").fetchall()
            public = _public_config(con)
            public["templates"] = [dict(x) for x in templates]
            public["selected_template_id"] = (cfg["template_id"] if cfg else None) or (row["id"] if row else None)
            return public
        finally:
            con.close()


class ParticipationConfigPayload(BaseModel):
    admin_secret: str
    template_id: str
    popup_title: str = "Participation Pack"
    popup_copy: str = "Thanks for taking part! You unlocked a new customisation."


@router.put("/admin")
def save_participation_admin(payload: ParticipationConfigPayload):
    cr._admin(payload.admin_secret)
    with cr._LOCK:
        con = cr._conn()
        try:
            _ensure_tables(con)
            row = con.execute("SELECT id FROM custom_pack_templates WHERE id=?", (payload.template_id,)).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Choose a saved Custom Pack first")
            con.execute(
                "INSERT INTO participation_reward_config(id,template_id,popup_title,popup_copy,updated_at) VALUES(1,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET template_id=excluded.template_id,popup_title=excluded.popup_title,popup_copy=excluded.popup_copy,updated_at=excluded.updated_at",
                (payload.template_id, (payload.popup_title or "Participation Pack")[:120], (payload.popup_copy or "")[:700], cr._now()),
            )
            con.commit()
            return _public_config(con)
        finally:
            con.close()


@router.post("/award")
def award_participation(payload: ParticipationAwardPayload):
    uid = str(payload.user_id or "").strip()[:80]
    uname = str(payload.username or "").strip().lstrip("@").lower()[:80]
    event_id = str(payload.event_id or "").strip()[:180]
    activity = str(payload.activity or "live_room").strip()[:80]
    key = _user_key(uid, uname)
    if not key:
        raise HTTPException(status_code=400, detail="Member identity is required")
    if not event_id:
        raise HTTPException(status_code=400, detail="event_id is required")

    with cr._LOCK:
        con = cr._conn()
        try:
            _ensure_tables(con)
            cfg, row = _participation_template(con)
            if not row:
                return {"ok": True, "awarded": False, "reason": "participation_pack_not_configured"}
            existing = con.execute(
                "SELECT * FROM participation_reward_awards WHERE user_key=? AND event_id=?",
                (key, event_id),
            ).fetchone()
            if existing:
                return {"ok": True, "awarded": False, "already_awarded": True, "grant_id": existing["grant_id"]}

            pack = cr._pack(row)
            items = list(pack.get("items") or [])
            owned_rows = con.execute(
                "SELECT item_key FROM participation_reward_awards WHERE user_key=?",
                (key,),
            ).fetchall()
            owned = {str(r["item_key"]) for r in owned_rows}
            available = []
            for item in items:
                item_key = f"{item.get('type','')}:{item.get('id','')}"
                if item.get("type") and item.get("id") and item_key not in owned:
                    available.append((item_key, item))
            if not available:
                return {"ok": True, "awarded": False, "collection_complete": True, "item_count": len(items)}

            item_key, chosen = random.choice(available)
            grant_id = str(uuid.uuid4())
            popup_title = (cfg["popup_title"] if cfg else None) or "Participation Pack"
            popup_copy = (cfg["popup_copy"] if cfg else None) or pack.get("description") or "Thanks for taking part! You unlocked a new customisation."
            reward_pack = {
                "id": f"participation-{grant_id}",
                "name": popup_title,
                "description": popup_copy,
                "image": pack.get("image") or "",
                "participation_reward": True,
                "activity": activity,
                "items": [chosen],
            }
            con.execute(
                "INSERT INTO custom_pack_grants(id,template_id,target_user_id,target_username,pack_json,sent_at,claimed_at) VALUES(?,?,?,?,?,?,NULL)",
                (grant_id, row["id"], uid or None, uname or None, json.dumps(reward_pack), cr._now()),
            )
            con.execute(
                "INSERT INTO participation_reward_awards(id,user_key,event_id,activity,item_key,grant_id,awarded_at) VALUES(?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), key, event_id, activity, item_key, grant_id, cr._now()),
            )
            con.commit()
            return {
                "ok": True,
                "awarded": True,
                "grant_id": grant_id,
                "pack": reward_pack,
                "item_key": item_key,
                "remaining": max(0, len(available) - 1),
            }
        finally:
            con.close()
