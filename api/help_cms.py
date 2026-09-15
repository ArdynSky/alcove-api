from __future__ import annotations

import datetime as dt
import hashlib
import os
import re
import sqlite3
import threading
import uuid
from pathlib import Path
from typing import Literal, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

router = APIRouter(prefix="/api", tags=["help"])
_LOCK = threading.RLock()
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
VIDEO_EXTENSIONS = {".mp4"}
IMAGE_MAX_BYTES = 5 * 1024 * 1024
VIDEO_MAX_BYTES = 100 * 1024 * 1024

LEGACY_ITEMS = (
    ("legacy-rules", "Group Rules", "GROUP RULES", "🔵 ┃ Group Rules", "rules", "assets/help_rules_banner.png"),
    ("legacy-ardyn", "Message from Ardyn", "MESSAGE FROM ARDYN", "🟣 ┃ Message from Ardyn", "ardyn", "assets/help_ardyn_banner.png"),
    ("legacy-guide", "Mini App Guide", "MINI APP GUIDE", "🟢 ┃ Mini App Guide", "guide", "assets/help_guide_banner.png"),
    ("legacy-faq", "F.A.Q.'s", "F.A.Q.'S", "🟠 ┃ F.A.Q.'s", "faq", "assets/help_faq_banner.png"),
)
APP_ROOTS = ("LIVE ROOM", "PROFILE", "CONNECT", "ARCHIVE")

LEGACY_BODY = {
    "rules": "🌿 Welcome to The Alcove\n\nA calmer space for wellbeing, respect, and real conversation — seeing each other as whole people, not just profiles.\n\nBy joining, you agree to:\n\n**Group Rules**\nRespect all members. Harassment, hate speech, bullying, or intimidation may lead to removal.\n• No spam, adverts, promo posts, group links, or text-bombing. F.O.X will remove these.\n• No illegal, violent, or pornographic content. You are responsible for what you post.\n• Do not dox, expose, or share anyone's personal details.\n• Ardyn and F.O.X may remove content or members to keep the space safe.\n\n**Video Chat Rules**\n• Cameras and mics should be on where possible.\n• Let everyone speak. Don't dominate the conversation.\n• Take part; calls are interactive and inclusive.\n• Respect the host. Rudeness or disruption may result in being muted.\n\n💚 Have fun, be kind, and look after each other.",
    "ardyn": "Hey everyone, Ardyn Sky here.\n\nI created The Alcove because I wanted to build something different from the usual Telegram spaces — somewhere warmer, safer, more thoughtful, and more human.\n\nIt's a place where people can still laugh, flirt, be cheeky, and have fun, but where connection, care, and respect sit at the heart of everything.\n\nThe Alcove is for people who want to be seen as more than a profile picture, a body, or a quick message in a busy chat. It's about creating space for real conversation, honest questions, mutual support, and deeper connections.\n\nF.O.X is our little guardian, here to welcome new residents, protect the vibe, guide the space, and add a bit of whimsy along the way.\n\nThe Alcove is still growing, and it will continue to be shaped by the people who join it. My hope is that it becomes a place where people can show up as they are, feel they belong, and connect without pressure or judgement.",
    "guide": "Tap **LAUNCH APP** below or type `/app` to open The Alcove inside Telegram.\n\n**What's included**\n• **Pulse** — daily questions and anonymous reflections\n• **Daily Check-In** — quick wellbeing check-ins\n• **Spotlight** — celebrate and recognise members\n• **Connect** — your home hub for community tools\n• **VC Companion** — video calls made interactive\n• **Profile** — customise your experience, and earn achievements\n• **Archive** — your Alcove journey, chronicled for you\n\n_More features are added as The Alcove grows._",
    "faq": "**The Alcove Group**\nQ: What is this group?\nA: A calmer space for wellbeing, respect, and real conversation.\n\nQ: Why were my links removed?\nA: The group is link-free. F.O.X removes links, adverts, promos, and group invites.\n\nQ: Where are the full rules?\nA: Tap **Group Rules** in this menu.\n\n**F.O.X**\nQ: What does F.O.X do?\nA: Welcomes members, verifies newcomers, removes spam/links, and keeps the space safe.\n\nQ: What can I type?\nA: `/help` — this menu · `/app` — Mini App\nDuring a live call, message F.O.X privately: `/draw`, `/debate`, or `/discuss`.\n\nQ: Why did F.O.X DM me?\nA: New members verify privately before posting freely in the group.\n\n**Mini App**\nQ: How do I open it?\nA: Tap **LAUNCH APP** below or type `/app`.\n\nQ: What's included?\nA: Pulse, Check-In, Spotlight, Connect, VC Companion, Profile, and Archive.\n\nQ: What is Pulse?\nA: Daily anonymous questions and community reflections.\n\nContact @Ardyn_Sky for help.",
}


def _data_dir() -> Path:
    raw = os.getenv("ALCOVE_STATE_DB_PATH") or os.getenv("FOX_MESSAGES_PATH")
    return Path(raw).expanduser().resolve().parent if raw else Path.cwd()


def _db_path() -> Path:
    return Path(os.getenv("HELP_CMS_DB_PATH", str(_data_dir() / "help_cms.sqlite3"))).expanduser().resolve()


def _media_dir() -> Path:
    path = Path(os.getenv("HELP_MEDIA_DIR", str(_data_dir() / "help-media"))).expanduser().resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _admin(secret: Optional[str]):
    expected = os.getenv("BOT_SYNC_SECRET", "")
    if not expected:
        raise HTTPException(503, "Admin secret is not configured")
    if secret != expected:
        raise HTTPException(403, "Invalid admin secret")


def _conn():
    path = _db_path(); path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("CREATE TABLE IF NOT EXISTS help_settings (id INTEGER PRIMARY KEY CHECK(id=1), terminal_title TEXT NOT NULL, introduction TEXT NOT NULL, default_button_style TEXT NOT NULL, default_back_wording TEXT NOT NULL, default_home_wording TEXT NOT NULL, published INTEGER NOT NULL, root_media_url TEXT NOT NULL, launcher_video_url TEXT NOT NULL, launcher_fallback_image_url TEXT NOT NULL, launcher_video_opacity INTEGER NOT NULL DEFAULT 50, updated_at TEXT NOT NULL)")
    settings_columns = {row[1] for row in con.execute("PRAGMA table_info(help_settings)")}
    if "launcher_video_opacity" not in settings_columns:
        con.execute("ALTER TABLE help_settings ADD COLUMN launcher_video_opacity INTEGER NOT NULL DEFAULT 50")
    con.execute("CREATE TABLE IF NOT EXISTS help_items (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES help_items(id) ON DELETE RESTRICT, type TEXT NOT NULL CHECK(type IN ('container','content')), internal_name TEXT NOT NULL, title TEXT NOT NULL, button_text TEXT NOT NULL, subtitle TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT '', media_type TEXT NOT NULL DEFAULT 'none', media_url TEXT NOT NULL DEFAULT '', skin_media_url TEXT NOT NULL DEFAULT '', image_position TEXT NOT NULL DEFAULT '50% 0%', overlay_strength INTEGER NOT NULL DEFAULT 45, title_color TEXT NOT NULL DEFAULT '#ffffff', theme TEXT NOT NULL DEFAULT '', sort_order INTEGER NOT NULL DEFAULT 0, show_in_app INTEGER NOT NULL DEFAULT 1, show_in_telegram INTEGER NOT NULL DEFAULT 1, show_back INTEGER NOT NULL DEFAULT 1, show_home INTEGER NOT NULL DEFAULT 1, status TEXT NOT NULL DEFAULT 'draft' CHECK(status IN ('draft','published','hidden')), legacy_key TEXT UNIQUE, created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS help_telegram_media_cache (media_url TEXT NOT NULL, bot_identity TEXT NOT NULL, source_sha256 TEXT NOT NULL, telegram_file_id TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(media_url,bot_identity))")
    now = _now()
    con.execute("INSERT OR IGNORE INTO help_settings(id,terminal_title,introduction,default_button_style,default_back_wording,default_home_wording,published,root_media_url,launcher_video_url,launcher_fallback_image_url,launcher_video_opacity,updated_at) VALUES(1,?,?,?,?,?,?,?,?,?,?,?)", ("F.O.X HELP TERMINAL", "Thank you for accessing the terminal.\nHow can I help today?", "primary", "↩ ┃ Back to menu", "HELP HOME", 1, "assets/help_terminal_banner.png", "", "", 50, now))
    count = con.execute("SELECT COUNT(*) FROM help_items").fetchone()[0]
    if count == 0:
        for order, (item_id, name, title, button, key, media) in enumerate(LEGACY_ITEMS):
            con.execute("INSERT INTO help_items(id,parent_id,type,internal_name,title,button_text,body,media_type,media_url,sort_order,show_in_app,show_in_telegram,status,legacy_key,created_at,updated_at) VALUES(?,NULL,'content',?,?,?,?, 'image',?,?,0,1,'published',?,?,?)", (item_id,name,title,button,LEGACY_BODY[key],media,order,key,now,now))
        for order, title in enumerate(APP_ROOTS):
            item_id = "app-" + title.lower().replace(" ", "-")
            con.execute("INSERT INTO help_items(id,parent_id,type,internal_name,title,button_text,sort_order,show_in_app,show_in_telegram,status,created_at,updated_at) VALUES(?,NULL,'container',?,?,?,?,1,0,'published',?,?)", (item_id,title,title,title,order,now,now))
    con.commit()
    return con


def _row(row):
    value = dict(row)
    for key in ("show_in_app","show_in_telegram","show_back","show_home"):
        value[key] = bool(value[key])
    return value


def _tree(rows):
    items = [_row(row) for row in rows]
    by_id = {item["id"]: item for item in items}
    roots = []
    for item in items:
        item["children"] = []
    for item in items:
        parent = by_id.get(item["parent_id"])
        (parent["children"] if parent else roots).append(item)
    return roots


class SettingsPayload(BaseModel):
    admin_secret: str
    terminal_title: str
    introduction: str = ""
    default_button_style: str = "primary"
    default_back_wording: str = "BACK"
    default_home_wording: str = "HELP HOME"
    published: bool = True
    root_media_url: str = ""
    launcher_video_url: str = ""
    launcher_fallback_image_url: str = ""
    launcher_video_opacity: Literal[25, 50, 75] = 50


class ItemPayload(BaseModel):
    admin_secret: str
    parent_id: Optional[str] = None
    type: Literal["container", "content"]
    internal_name: str
    title: str
    button_text: str
    subtitle: str = ""
    body: str = ""
    media_type: Literal["none", "image", "video"] = "none"
    media_url: str = ""
    skin_media_url: str = ""
    image_position: str = "50% 0%"
    overlay_strength: int = 45
    title_color: str = "#ffffff"
    theme: str = ""
    sort_order: Optional[int] = None
    show_in_app: bool = True
    show_in_telegram: bool = True
    show_back: bool = True
    show_home: bool = True
    status: Literal["draft", "published", "hidden"] = "draft"


class PositionPayload(BaseModel):
    admin_secret: str
    parent_id: Optional[str] = None
    sort_order: int = 0


class AdminPayload(BaseModel):
    admin_secret: str


def _validate_parent(con, item_id, parent_id):
    if not parent_id:
        return
    parent = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (parent_id,)).fetchone()
    if not parent or parent["type"] != "container":
        raise HTTPException(400, "Parent must be an existing container")
    cursor = parent
    while cursor:
        if cursor["id"] == item_id:
            raise HTTPException(400, "An item cannot be moved beneath itself or its descendant")
        cursor = con.execute("SELECT id,parent_id,type FROM help_items WHERE id=?", (cursor["parent_id"],)).fetchone() if cursor["parent_id"] else None


@router.get("/help")
def public_help(destination: Literal["app", "telegram"] = "app"):
    with _LOCK:
        con = _conn()
        settings = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone())
        column = "show_in_app" if destination == "app" else "show_in_telegram"
        rows = con.execute(f"SELECT * FROM help_items WHERE status='published' AND {column}=1 ORDER BY parent_id,sort_order,created_at").fetchall()
        con.close()
    settings["published"] = bool(settings["published"])
    return {"settings": settings, "items": _tree(rows) if settings["published"] else []}


@router.get("/admin/help")
def admin_help(admin_secret: str):
    _admin(admin_secret)
    with _LOCK:
        con = _conn(); settings = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone())
        rows = con.execute("SELECT * FROM help_items ORDER BY parent_id,sort_order,created_at").fetchall(); con.close()
    settings["published"] = bool(settings["published"])
    return {"settings": settings, "items": _tree(rows)}


@router.put("/admin/help/settings")
def save_settings(payload: SettingsPayload):
    _admin(payload.admin_secret); data = payload.model_dump(exclude={"admin_secret"}); data["published"] = int(data["published"]); data["updated_at"] = _now()
    with _LOCK:
        con = _conn(); con.execute("UPDATE help_settings SET " + ",".join(f"{key}=?" for key in data) + " WHERE id=1", tuple(data.values())); con.commit()
        row = dict(con.execute("SELECT * FROM help_settings WHERE id=1").fetchone()); con.close()
    row["published"] = bool(row["published"]); return {"settings": row}


@router.post("/admin/help/items")
def create_item(payload: ItemPayload):
    _admin(payload.admin_secret); item_id = uuid.uuid4().hex[:16]; now = _now()
    with _LOCK:
        con = _conn(); _validate_parent(con,item_id,payload.parent_id)
        order = payload.sort_order if payload.sort_order is not None else con.execute("SELECT COALESCE(MAX(sort_order)+1,0) FROM help_items WHERE parent_id IS ?",(payload.parent_id,)).fetchone()[0]
        data = payload.model_dump(exclude={"admin_secret","sort_order"}); data.update(id=item_id,sort_order=order,created_at=now,updated_at=now)
        keys=list(data); con.execute(f"INSERT INTO help_items({','.join(keys)}) VALUES({','.join('?' for _ in keys)})",tuple(data[k] for k in keys)); con.commit()
        row=con.execute("SELECT * FROM help_items WHERE id=?",(item_id,)).fetchone(); con.close()
    return {"item":_row(row)}


@router.patch("/admin/help/items/{item_id}/position")
def move_item(item_id: str, payload: PositionPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con=_conn(); current=con.execute("SELECT id FROM help_items WHERE id=?",(item_id,)).fetchone()
        if not current: con.close(); raise HTTPException(404,"Help item not found")
        _validate_parent(con,item_id,payload.parent_id)
        con.execute("UPDATE help_items SET parent_id=?,sort_order=?,updated_at=? WHERE id=?",(payload.parent_id,max(0,payload.sort_order),_now(),item_id)); con.commit()
        row=con.execute("SELECT * FROM help_items WHERE id=?",(item_id,)).fetchone(); con.close()
    return {"item":_row(row)}


@router.put("/admin/help/items/{item_id}")
def update_item(item_id: str, payload: ItemPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con = _conn()
        if not con.execute("SELECT id FROM help_items WHERE id=?", (item_id,)).fetchone():
            con.close(); raise HTTPException(404, "Help item not found")
        _validate_parent(con, item_id, payload.parent_id)
        data = payload.model_dump(exclude={"admin_secret"})
        if data.pop("sort_order") is None:
            data.pop("sort_order", None)
        data["updated_at"] = _now()
        con.execute("UPDATE help_items SET " + ",".join(f"{key}=?" for key in data) + " WHERE id=?", tuple(data.values()) + (item_id,))
        con.commit(); row = con.execute("SELECT * FROM help_items WHERE id=?", (item_id,)).fetchone(); con.close()
    return {"item": _row(row)}


@router.post("/admin/help/items/{item_id}/duplicate")
def duplicate_item(item_id: str, payload: AdminPayload):
    _admin(payload.admin_secret)
    with _LOCK:
        con = _conn(); source = con.execute("SELECT * FROM help_items WHERE id=?", (item_id,)).fetchone()
        if not source: con.close(); raise HTTPException(404, "Help item not found")
        def copy_node(row, parent_id):
            new_id = uuid.uuid4().hex[:16]; now = _now(); values = dict(row)
            values.update(id=new_id, parent_id=parent_id, internal_name=values["internal_name"] + " — Copy", status="draft", legacy_key=None, created_at=now, updated_at=now)
            keys = list(values); con.execute(f"INSERT INTO help_items({','.join(keys)}) VALUES({','.join('?' for _ in keys)})", tuple(values[k] for k in keys))
            for child in con.execute("SELECT * FROM help_items WHERE parent_id=? ORDER BY sort_order", (row["id"],)).fetchall(): copy_node(child, new_id)
            return new_id
        new_id = copy_node(source, source["parent_id"]); con.commit(); row = con.execute("SELECT * FROM help_items WHERE id=?", (new_id,)).fetchone(); con.close()
    return {"item": _row(row)}


@router.delete("/admin/help/items/{item_id}")
def delete_item(item_id: str, admin_secret: str):
    _admin(admin_secret)
    with _LOCK:
        con = _conn()
        if con.execute("SELECT 1 FROM help_items WHERE parent_id=? LIMIT 1", (item_id,)).fetchone():
            con.close(); raise HTTPException(409, "Move or delete child items first")
        changed = con.execute("DELETE FROM help_items WHERE id=?", (item_id,)).rowcount; con.commit(); con.close()
    if not changed: raise HTTPException(404, "Help item not found")
    return {"ok": True}


@router.post("/admin/help/media")
async def upload_media(admin_secret: str = Form(...), file: UploadFile = File(...)):
    _admin(admin_secret); original=Path(file.filename or "help-media"); ext=original.suffix.lower()
    if ext not in IMAGE_EXTENSIONS | VIDEO_EXTENSIONS: raise HTTPException(400,"Use PNG, JPEG, WebP, GIF, or MP4 media")
    limit=VIDEO_MAX_BYTES if ext in VIDEO_EXTENSIONS else IMAGE_MAX_BYTES
    stem=re.sub(r"[^a-zA-Z0-9_-]+","-",original.stem).strip("-")[:50] or "help"
    filename=f"{stem}-{uuid.uuid4().hex[:10]}{ext}"; target=_media_dir()/filename; total=0; digest=hashlib.sha256()
    try:
        with target.open("wb") as handle:
            while chunk := await file.read(1024*1024):
                total += len(chunk)
                if total > limit: raise HTTPException(413,"Media file is too large")
                digest.update(chunk); handle.write(chunk)
    except Exception:
        target.unlink(missing_ok=True); raise
    if not total: target.unlink(missing_ok=True); raise HTTPException(400,"Media file is empty")
    return {"asset":{"url":f"/api/help/media/{filename}","filename":filename,"size":total,"sha256":digest.hexdigest(),"media_type":"video" if ext in VIDEO_EXTENSIONS else "image"}}


@router.get("/help/media/{filename}")
def serve_media(filename: str):
    clean=Path(filename).name
    if clean != filename: raise HTTPException(404,"Media not found")
    path=(_media_dir()/clean).resolve()
    if path.parent != _media_dir().resolve() or not path.is_file(): raise HTTPException(404,"Media not found")
    return FileResponse(path)
