"""Rehearsal-only fake audience identities for closed-room dry runs.

Reserved Telegram-style numeric IDs in 91001–91006 act as Host Control stand-ins
while an agenda is in `rehearsal`. They must never receive packs or affect the
real verified-member roster after a complete bot sync.
"""
from __future__ import annotations

from typing import Any

# Fixed range so Host Control / ops scripts can recognise rehearsal players.
REHEARSAL_UID_MIN = 91001
REHEARSAL_UID_MAX = 91006

ROSTER: list[dict[str, str]] = [
    {"user_id": "91001", "display_name": "Sam · Rehearsal", "username": "rehearse_sam"},
    {"user_id": "91002", "display_name": "Jamie · Rehearsal", "username": "rehearse_jamie"},
    {"user_id": "91003", "display_name": "Riley · Rehearsal", "username": "rehearse_riley"},
    {"user_id": "91004", "display_name": "Morgan · Rehearsal", "username": "rehearse_morgan"},
    {"user_id": "91005", "display_name": "Taylor · Rehearsal", "username": "rehearse_taylor"},
    {"user_id": "91006", "display_name": "Charlie · Rehearsal", "username": "rehearse_charlie"},
]


def is_rehearsal_dummy(user_id: Any) -> bool:
    text = str(user_id or "").strip()
    if not text.isdigit():
        return False
    value = int(text)
    return REHEARSAL_UID_MIN <= value <= REHEARSAL_UID_MAX


def rehearsal_show_active() -> bool:
    """True when Host Control is running a closed-room rehearsal (not audience live)."""
    try:
        from . import agendas
        payload = agendas.running_agenda()
        agenda = payload.get("agenda") if isinstance(payload, dict) else None
        return bool(agenda and agenda.get("status") == "rehearsal")
    except Exception:
        return False


def allow_bot_rehearsal_dummy(user_id: Any) -> bool:
    """Bot-secret callers may drive these IDs only while a rehearsal is running."""
    return is_rehearsal_dummy(user_id) and rehearsal_show_active()
