from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

router = APIRouter(tags=["homepage"])
_LOCK = threading.RLock()

TILE_KEYS = ("live_room", "profile", "connect", "archive")

DEFAULT_TILE_COPY = {
    "live_room": {
        "info_title": "LIVE ROOM",
        "info_description": (
            "View VC schedule, take part in interactive games, debates, discussions and live feed."
        ),
    },
    "profile": {
        "info_title": "PROFILE",
        "info_description": (
            "Access your profile, customise your experience, and exchange EXP for rewards."
        ),
    },
    "connect": {
        "info_title": "CONNECT",
        "info_description": (
            "Ask and answer questions anonymously, celebrate and award fellow Alcove members."
        ),
    },
    "archive": {
        "info_title": "ARCHIVE",
        "info_description": (
            "Access the full Alcove interaction history including Pulse, Spotlight and VC sessions."
        ),
    },
}

IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
VIDEO_EXTENSIONS = {".mp4", ".webm", ".m4v"}
IMAGE_MAX_BYTES = 5 * 1024 * 1024
VIDEO_MAX_BYTES = 40 * 1024 * 1024


def _persistent_data_dir() -> Path:
    for key in (
        "HOMEPAGE_SETTINGS_PATH",
        "HOMEPAGE_MEDIA_DIR",
        "ALCOVE_RUNTIME_STATE_PATH",
        "ALCOVE_STATE_DB_PATH",
        "FEATURE_FLAGS_PATH",
        "FOX_MESSAGES_PATH",
        "SAFETY_SETTINGS_PATH",
    ):
        raw = os.getenv(key, "").strip()
        if not raw:
            continue
        path = Path(raw).expanduser()
        if key.endswith("_DIR"):
            return path.resolve()
        parent = path.parent
        if str(parent):
            return parent.resolve()
    return Path.cwd()


def _settings_path() -> Path:
    configured = os.getenv("HOMEPAGE_SETTINGS_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return _persistent_data_dir() / "homepage_settings.json"


def _media_dir() -> Path:
    configured = os.getenv("HOMEPAGE_MEDIA_DIR", "").strip()
    if configured:
        path = Path(configured).expanduser().resolve()
    else:
        path = _persistent_data_dir() / "homepage-media"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _admin(secret: str | None) -> None:
    expected = os.getenv("BOT_SYNC_SECRET", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Admin secret is not configured")
    if not secret or secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


def _clean_url(value: Any) -> str:
    text = str(value or "").strip().strip("'\"")[:2000]
    if not text:
        return ""
    if text.startswith("//"):
        text = f"https:{text}"
    elif text and not text.startswith(("/", "http://", "https://", "data:", "blob:", "assets")):
        host = text.split("/", 1)[0]
        if "." in host and " " not in host:
            text = f"https://{text}"
    if "drive.google.com/file/d/" in text:
        start = text.find("drive.google.com/file/d/") + len("drive.google.com/file/d/")
        file_id = text[start:].split("/", 1)[0].split("?", 1)[0]
        if file_id:
            return f"https://drive.google.com/uc?export=view&id={file_id}"
    if "dropbox.com/" in text:
        text = (
            text.replace("www.dropbox.com", "dl.dropboxusercontent.com")
            .replace("?dl=0", "")
            .replace("&dl=0", "")
        )
    if "imgur.com/" in text and "i.imgur.com/" not in text:
        slug = text.rstrip("/").rsplit("/", 1)[-1].split("?", 1)[0]
        slug = slug.replace("gallery/", "").replace("a/", "")
        if slug:
            return f"https://i.imgur.com/{slug}.jpg"
    return text


def _default_tile(key: str) -> dict:
    copy = DEFAULT_TILE_COPY[key]
    return {
        "preview_video_url": "",
        "cta": "LET'S GO!",
        "info_title": copy["info_title"],
        "info_description": copy["info_description"],
    }


def default_homepage_settings() -> dict:
    return {
        "background_video_url": "",
        "fallback_image_url": "",
        "logo_url": "",
        "tiles": {key: _default_tile(key) for key in TILE_KEYS},
    }


def _normalize_tile(key: str, raw: Any) -> dict:
    base = _default_tile(key)
    if not isinstance(raw, dict):
        return base
    title = str(raw.get("info_title") or base["info_title"]).strip()[:80] or base["info_title"]
    description = (
        str(raw.get("info_description") or base["info_description"]).strip()[:400]
        or base["info_description"]
    )
    cta = str(raw.get("cta") or base["cta"]).strip()[:40] or base["cta"]
    return {
        "preview_video_url": _clean_url(
            raw.get("preview_video_url")
            or raw.get("previewVideoUrl")
            or raw.get("video_url")
            or raw.get("videoUrl")
            or ""
        ),
        "cta": cta,
        "info_title": title.upper() if title.isascii() else title,
        "info_description": description,
    }


def normalize_homepage_settings(raw: Any) -> dict:
    base = default_homepage_settings()
    if not isinstance(raw, dict):
        return base
    tiles_raw = raw.get("tiles") if isinstance(raw.get("tiles"), dict) else {}
    return {
        "background_video_url": _clean_url(
            raw.get("background_video_url")
            or raw.get("backgroundVideoUrl")
            or raw.get("background_video")
            or ""
        ),
        "fallback_image_url": _clean_url(
            raw.get("fallback_image_url")
            or raw.get("fallbackImageUrl")
            or raw.get("fallback_image")
            or ""
        ),
        "logo_url": _clean_url(raw.get("logo_url") or raw.get("logoUrl") or ""),
        "tiles": {key: _normalize_tile(key, tiles_raw.get(key)) for key in TILE_KEYS},
    }


def load_homepage_settings() -> dict:
    path = _settings_path()
    if not path.exists():
        return default_homepage_settings()
    try:
        with path.open("r", encoding="utf-8") as handle:
            saved = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return default_homepage_settings()
    return normalize_homepage_settings(saved)


def save_homepage_settings(settings: dict) -> dict:
    normalized = normalize_homepage_settings(settings)
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with _LOCK:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(normalized, handle, indent=2, sort_keys=True)
    return normalized


def _sanitize_stem(original_name: str) -> str:
    stem = Path(original_name or "homepage-media").stem
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", stem).strip("-_").lower()
    return (cleaned or "homepage-media")[:48]


def _public_media_url(filename: str) -> str:
    return f"/api/homepage-media/{filename}"


def _resolve_media_file(filename: str) -> Path:
    safe = Path(filename or "").name
    if not safe or safe != filename or ".." in safe:
        raise HTTPException(status_code=404, detail="Media not found")
    path = _media_dir() / safe
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Media not found")
    return path


def _media_type_for(path: Path) -> str:
    ext = path.suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".webp": "image/webp",
        ".gif": "image/gif",
        ".mp4": "video/mp4",
        ".m4v": "video/mp4",
        ".webm": "video/webm",
    }.get(ext, "application/octet-stream")


class HomepageTileUpdate(BaseModel):
    preview_video_url: Optional[str] = ""
    cta: Optional[str] = "LET'S GO!"
    info_title: Optional[str] = ""
    info_description: Optional[str] = ""


class HomepageSettingsUpdate(BaseModel):
    admin_secret: str
    background_video_url: Optional[str] = ""
    fallback_image_url: Optional[str] = ""
    logo_url: Optional[str] = ""
    tiles: dict[str, HomepageTileUpdate] = Field(default_factory=dict)


@router.get("/api/homepage-settings")
def get_homepage_settings():
    return {"status": "ok", "settings": load_homepage_settings()}


@router.post("/api/homepage-settings")
def update_homepage_settings(payload: HomepageSettingsUpdate):
    _admin(payload.admin_secret)
    current = load_homepage_settings()
    tiles: dict[str, dict] = {}
    for key in TILE_KEYS:
        tile_payload = payload.tiles.get(key)
        current_tile = current["tiles"][key]
        if tile_payload is None:
            tiles[key] = current_tile
            continue
        data = tile_payload.model_dump()
        tiles[key] = {
            "preview_video_url": data.get("preview_video_url", ""),
            "cta": data.get("cta") or current_tile["cta"],
            "info_title": data.get("info_title") or current_tile["info_title"],
            "info_description": data.get("info_description") or current_tile["info_description"],
        }
    settings = save_homepage_settings(
        {
            "background_video_url": payload.background_video_url or "",
            "fallback_image_url": payload.fallback_image_url or "",
            "logo_url": payload.logo_url or "",
            "tiles": tiles,
        }
    )
    return {"status": "ok", "settings": settings}


@router.post("/api/admin/homepage-media/upload")
async def upload_homepage_media(
    admin_secret: str = Form(...),
    file: UploadFile = File(...),
    label: str = Form(""),
    slot: str = Form(""),
):
    _admin(admin_secret)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    ext = Path(file.filename or "upload.bin").suffix.lower()
    if ext in IMAGE_EXTENSIONS:
        if len(content) > IMAGE_MAX_BYTES:
            raise HTTPException(status_code=400, detail="Image too large (max 5 MB)")
        kind = "image"
    elif ext in VIDEO_EXTENSIONS:
        if len(content) > VIDEO_MAX_BYTES:
            raise HTTPException(status_code=400, detail="Video too large (max 40 MB)")
        kind = "video"
    else:
        raise HTTPException(status_code=400, detail="Use PNG, JPEG, WebP, GIF, MP4, or WebM")

    stem = _sanitize_stem(label or file.filename or slot or "homepage")
    filename = f"{stem}-{uuid.uuid4().hex[:8]}{ext}"
    path = _media_dir() / filename
    with path.open("wb") as handle:
        handle.write(content)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "filename": filename,
        "label": str(label or stem).strip()[:80] or filename,
        "slot": str(slot or "").strip()[:40],
        "kind": kind,
        "url": _public_media_url(filename),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
        "size_bytes": len(content),
    }
    return {"status": "ok", "asset": entry}


@router.get("/api/homepage-media/{filename}")
def get_homepage_media(filename: str):
    path = _resolve_media_file(filename)
    return FileResponse(path, media_type=_media_type_for(path))


@router.delete("/api/admin/homepage-media/{filename}")
def delete_homepage_media(filename: str, admin_secret: str):
    _admin(admin_secret)
    path = _resolve_media_file(filename)
    try:
        path.unlink()
    except OSError as exc:
        raise HTTPException(status_code=500, detail="Could not delete media") from exc
    return {"status": "ok", "filename": filename}
