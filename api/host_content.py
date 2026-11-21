"""Shared Host Control Activities / Discussions library.

Host Control used to keep this library only in each browser's localStorage, so a
phone never saw content saved on a laptop. Persist one shared list behind the
admin secret so every unlocked Host Control device shows the same library.
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


def load_host_content() -> dict:
    path = _store_path()
    if not path.exists():
        return {"items": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, TypeError, json.JSONDecodeError):
        return {"items": []}
    if not isinstance(payload, dict):
        return {"items": []}
    return {"items": _normalize_items(payload.get("items"))}


def save_host_content(items: list[dict]) -> dict:
    normalized = {"items": _normalize_items(items)}
    encoded = json.dumps(normalized, separators=(",", ":"), ensure_ascii=False)
    if len(encoded.encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise HTTPException(status_code=413, detail="Host content library is too large")
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with _LOCK:
        tmp.write_text(encoded, encoding="utf-8")
        tmp.replace(path)
    return normalized


class HostContentPut(BaseModel):
    admin_secret: str
    items: list[Any] = Field(default_factory=list)


@router.get("")
def get_host_content(admin_secret: str = ""):
    _admin(admin_secret)
    return {"status": "ok", **load_host_content()}


@router.put("")
def put_host_content(payload: HostContentPut):
    _admin(payload.admin_secret)
    saved = save_host_content(payload.items)
    return {"status": "ok", **saved}
