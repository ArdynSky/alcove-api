"""Shared Host Control Activities / Discussions library.

Host Control used to keep this library only in each browser's localStorage, so a
phone never saw content saved on a laptop. Persist one shared list behind the
admin secret so every unlocked Host Control device shows the same library.

Writes are revision-checked so a stale phone snapshot cannot wipe newer laptop
edits, and an emptied/deleted remote library is not resurrected from old locals.
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/host-content", tags=["host-content"])
_LOCK = threading.RLock()
MAX_ITEMS = 400
MAX_PAYLOAD_BYTES = 2 * 1024 * 1024


def _persistent_data_dir() -> Path:
    for key in (
        "HOST_CONTENT_PATH",
        "HOMEPAGE_SETTINGS_PATH",
        "ALCOVE_RUNTIME_STATE_PATH",
        "ALCOVE_STATE_DB_PATH",
        "FEATURE_FLAGS_PATH",
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


def _store_path() -> Path:
    configured = os.getenv("HOST_CONTENT_PATH", "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return _persistent_data_dir() / "host_content.json"


def _admin(secret: str | None) -> None:
    expected = os.getenv("BOT_SYNC_SECRET", "")
    if not expected:
        raise HTTPException(status_code=503, detail="Admin secret is not configured")
    if not secret or secret != expected:
        raise HTTPException(status_code=403, detail="Invalid admin secret")


def _normalize_items(raw: Any) -> list[dict]:
    if not isinstance(raw, list):
        return []
    items: list[dict] = []
    seen: set[str] = set()
    for row in raw:
        if not isinstance(row, dict):
            continue
        item_id = str(row.get("id") or "").strip()
        if not item_id or item_id in seen:
            continue
        seen.add(item_id)
        items.append(row)
        if len(items) >= MAX_ITEMS:
            break
    return items


def _normalize_revision(raw: Any) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0
    return value if value > 0 else 0


def load_host_content() -> dict:
    path = _store_path()
    if not path.exists():
        return {"revision": 0, "items": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, json.JSONDecodeError):
        return {"revision": 0, "items": []}
    if not isinstance(payload, dict):
        return {"revision": 0, "items": []}
    return {
        "revision": _normalize_revision(payload.get("revision")),
        "items": _normalize_items(payload.get("items")),
    }


def _write_host_content(revision: int, items: list[dict]) -> dict:
    normalized = {
        "revision": max(0, int(revision)),
        "items": _normalize_items(items),
    }
    encoded = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
    if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Host content library is too large")
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(encoded, encoding="utf-8")
    tmp.replace(path)
    return normalized


def save_host_content(items: list[Any], base_revision: int | None) -> dict:
    with _LOCK:
        current = load_host_content()
        current_revision = int(current["revision"])
        if base_revision is None:
            base_revision = current_revision
        try:
            base = int(base_revision)
        except (TypeError, ValueError):
            base = -1
        if base != current_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Host content changed on another device. Reload and try again.",
                    "revision": current_revision,
                    "items": current["items"],
                },
            )
        return _write_host_content(current_revision + 1, items)


class HostContentPut(BaseModel):
    admin_secret: str
    items: list[Any] = Field(default_factory=list)
    base_revision: int | None = None


class HostContentItemPut(BaseModel):
    admin_secret: str
    item: dict[str, Any]
    base_revision: int | None = None


@router.get("")
def get_host_content(admin_secret: str = ""):
    _admin(admin_secret)
    return {"status": "ok", **load_host_content()}


@router.put("")
def put_host_content(payload: HostContentPut):
    _admin(payload.admin_secret)
    saved = save_host_content(payload.items, payload.base_revision)
    return {"status": "ok", **saved}


@router.put("/items")
def upsert_host_content_item(payload: HostContentItemPut):
    _admin(payload.admin_secret)
    item = payload.item if isinstance(payload.item, dict) else {}
    item_id = str(item.get("id") or "").strip()
    if not item_id:
        raise HTTPException(status_code=400, detail="Activity id is required")
    with _LOCK:
        current = load_host_content()
        current_revision = int(current["revision"])
        base = payload.base_revision if payload.base_revision is not None else current_revision
        try:
            base_revision = int(base)
        except (TypeError, ValueError):
            base_revision = -1
        if base_revision != current_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Host content changed on another device. Reload and try again.",
                    "revision": current_revision,
                    "items": current["items"],
                },
            )
        items = [row for row in current["items"] if str(row.get("id") or "") != item_id]
        items.append(item)
        saved = _write_host_content(current_revision + 1, items)
    return {"status": "ok", **saved}


@router.delete("/items/{item_id}")
def delete_host_content_item(item_id: str, admin_secret: str = "", base_revision: int | None = None):
    _admin(admin_secret)
    cleaned = str(item_id or "").strip()
    if not cleaned:
        raise HTTPException(status_code=400, detail="Activity id is required")
    with _LOCK:
        current = load_host_content()
        current_revision = int(current["revision"])
        base = base_revision if base_revision is not None else current_revision
        try:
            expected = int(base)
        except (TypeError, ValueError):
            expected = -1
        if expected != current_revision:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "Host content changed on another device. Reload and try again.",
                    "revision": current_revision,
                    "items": current["items"],
                },
            )
        items = [row for row in current["items"] if str(row.get("id") or "") != cleaned]
        saved = _write_host_content(current_revision + 1, items)
    return {"status": "ok", **saved}
