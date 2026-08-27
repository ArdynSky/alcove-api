from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel
import re
from urllib.parse import parse_qsl, urlparse, unquote
import urllib.error
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo
import datetime
import hashlib
import hmac
import os
import shutil
import asyncio
import csv
import io
import json
import random
import sqlite3
import threading
import uuid
import zipfile
from html import escape
from fastapi import Header, HTTPException, WebSocket, WebSocketDisconnect, File, UploadFile, Form
from .websocket_manager import manager
from .cards_game import (
    CreateRoomPayload,
    JoinRoomPayload,
    LoadoutPayload,
    QueuePayload,
    RoomActionPayload,
    UserIdPayload,
    cards_resolve_player,
    get_cards_service,
)
from .cards_progress import fetch_pending_rewards, mark_rewards_claimed, profile_summary
from .cards_ws_manager import cards_ws_manager
from . import fox_messages as fox_messages_store
from .home_alerts import router as home_alerts_router
from .team_games import router as team_games_router
from .custom_rewards import router as custom_rewards_router
from .debate_games import router as debate_games_router
from .agendas import router as agendas_router
from .live_room_test import apply_authorization_identity, router as live_room_test_router

try:
    from dotenv import load_dotenv

    _repo_root = Path(__file__).resolve().parents[1]
    for _env_path in (
        _repo_root / "Bot-Review" / "ALCOVE_FOX" / ".env",
        _repo_root / ".env",
    ):
        if _env_path.exists():
            load_dotenv(_env_path, override=False)
            break
except ImportError:
    pass

app = FastAPI()
app.include_router(home_alerts_router)
app.include_router(team_games_router)
app.include_router(custom_rewards_router)
app.include_router(debate_games_router)
app.include_router(agendas_router)
app.include_router(live_room_test_router)

CORS_ALLOWED_ORIGINS = [
    "null",
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "http://127.0.0.1:8000",
    "http://localhost:8000",
    "http://127.0.0.1:5173",
    "http://localhost:5173",
    "https://euphonious-banoffee-1c8215.netlify.app",
    "https://thealcove.netlify.app",
    "https://ardyn-alcove.com",
    "https://www.ardyn-alcove.com",
]
_extra_cors = os.getenv("CORS_ALLOWED_ORIGINS", "").strip()
if _extra_cors:
    CORS_ALLOWED_ORIGINS.extend(
        origin.strip()
        for origin in _extra_cors.split(",")
        if origin.strip() and origin.strip() not in CORS_ALLOWED_ORIGINS
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# ---------------------------------


# ---------------------------------
# Paths / config
# ---------------------------------

ALCOVE_ROOT = os.path.expanduser(r"~/Desktop/Alcove")
DOWNLOADS_DIR = os.path.join(ALCOVE_ROOT, "Downloads")
READY_DIR = os.path.join(ALCOVE_ROOT, "Ready")
ARCHIVE_DIR = os.path.join(ALCOVE_ROOT, "Archive")
PLAYOUT_DIR = os.path.join(ALCOVE_ROOT, "Playout")
CURRENT_PICK_PATH = os.path.join(PLAYOUT_DIR, "current_pick.mp4")
FOX_LOGS_DB_PATH = os.getenv(
    "FOX_LOGS_DB_PATH",
    os.path.join(ALCOVE_ROOT, "Bot-Review", "ALCOVE_FOX", "fox_logs.db"),
)
BOT_SYNC_SECRET = os.getenv("BOT_SYNC_SECRET", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("ALCOVE_TELEGRAM_BOT_TOKEN", "")
ALCOVE_ADMIN_GROUP_ID = int(os.getenv("ALCOVE_ADMIN_GROUP_ID", "-1003971041191") or "-1003971041191")
PULSE_QUESTIONS_TOPIC_ID = int(os.getenv("PULSE_QUESTIONS_TOPIC_ID", "289") or "289")
PULSE_REPORTS_TOPIC_ID = int(os.getenv("PULSE_REPORTS_TOPIC_ID", "365") or "365")
SPOTLIGHT_REVIEW_TOPIC_ID = int(os.getenv("SPOTLIGHT_REVIEW_TOPIC_ID", "97") or "97")
SAFETY_SETTINGS_PATH = os.getenv(
    "SAFETY_SETTINGS_PATH",
    os.path.join(os.getcwd(), "safety_settings.json"),
)
RUNTIME_STATE_PATH = os.getenv(
    "ALCOVE_RUNTIME_STATE_PATH",
    os.path.join(os.getcwd(), "alcove_runtime_state.json"),
)


def persistent_data_dir() -> str:
    """Prefer the Render /var/data disk when sibling settings already use it."""
    for path in (
        os.getenv("ALCOVE_RUNTIME_STATE_PATH", ""),
        os.getenv("ALCOVE_STATE_DB_PATH", ""),
        os.getenv("FOX_LOGS_DB_PATH", ""),
        os.getenv("SAFETY_SETTINGS_PATH", ""),
        os.getenv("FOX_MESSAGES_PATH", ""),
    ):
        if path:
            parent = os.path.dirname(path)
            if parent:
                return parent
    return os.getcwd()


_PERSISTENT_DATA_DIR = persistent_data_dir()

FEATURE_FLAGS_PATH = os.getenv(
    "FEATURE_FLAGS_PATH",
    os.path.join(_PERSISTENT_DATA_DIR, "feature_flags.json"),
)
PULSE_SETTINGS_PATH = os.getenv(
    "PULSE_SETTINGS_PATH",
    os.path.join(_PERSISTENT_DATA_DIR, "pulse_settings.json"),
)
VERIFY_FLOW_LOG_PATH = os.getenv(
    "VERIFY_FLOW_LOG_PATH",
    os.path.join(os.getcwd(), "verification_flow_events.jsonl"),
)
STATE_DB_PATH = os.getenv(
    "ALCOVE_STATE_DB_PATH",
    os.path.join(os.getcwd(), "alcove_state.db"),
)

ROOM_MEDIA_DIR = os.path.join(_PERSISTENT_DATA_DIR, "room-media")
REWARD_ASSETS_DIR = os.getenv(
    "REWARD_ASSETS_DIR",
    os.path.join(_PERSISTENT_DATA_DIR, "reward-assets"),
)
REWARD_ASSETS_MANIFEST_PATH = os.getenv(
    "REWARD_ASSETS_MANIFEST_PATH",
    os.path.join(_PERSISTENT_DATA_DIR, "reward_assets.json"),
)
REWARD_CATALOG_PATH = os.getenv(
    "REWARD_CATALOG_PATH",
    os.path.join(_PERSISTENT_DATA_DIR, "reward_catalog.json"),
)
REWARD_ASSET_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp", ".gif"}
REWARD_ASSET_MAX_BYTES = 5 * 1024 * 1024
LIVE_ROOM_STATE_PATH = os.getenv(
    "ALCOVE_LIVE_ROOM_STATE_PATH",
    os.path.join(_PERSISTENT_DATA_DIR, "live_room_state.json"),
)
ROOM_MEDIA_ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".mp4", ".mov", ".webm", ".m4v"}
ROOM_MEDIA_MAX_BYTES = 100 * 1024 * 1024
ROOM_DISCUSSION_DURATIONS = {5, 10, 20, 30, 45, 60}

for path in [DOWNLOADS_DIR, READY_DIR, ARCHIVE_DIR, PLAYOUT_DIR, ROOM_MEDIA_DIR, REWARD_ASSETS_DIR]:
    os.makedirs(path, exist_ok=True)


CONFIG = {

    # ---------------------------------
    # Approved download domains
    # ---------------------------------

    "approved_video_domains": {

        "cockdude.com": {
            "name": "CockDude",
            "auto_download": False
        },

        "gayforit.eu": {
            "name": "GayForIt",
            "auto_download": False
        },

        "gaytube.com": {
            "name": "GayTube",
            "auto_download": False
        }

    },

    # ---------------------------------
    # Folder structure
    # ---------------------------------

    "paths": {

        "base_dir": Path.home() / "Desktop" / "Alcove",

        "downloads_dir": Path.home() / "Desktop" / "Alcove" / "Downloads",

        "ready_dir": Path.home() / "Desktop" / "Alcove" / "Ready",

        "archive_dir": Path.home() / "Desktop" / "Alcove" / "Archive",

        "playout_dir": Path.home() / "Desktop" / "Alcove" / "Playout",

        "current_pick": Path.home() / "Desktop" / "Alcove" / "Playout" / "current_pick.mp4"

    },

    # ---------------------------------
    # Downloader behaviour
    # ---------------------------------

    "download": {

        "timeout_seconds": 120,

        "max_auto_retries": 2,

        "poll_interval_seconds": 5

    }

}

def normalize_domain(url: str) -> str:
    try:
        domain = urlparse(url).netloc.lower().strip()

        if domain.startswith("www."):
            domain = domain[4:]

        return domain

    except Exception:
        return ""


def get_domain_config(url: str):

    domain = normalize_domain(url)

    for approved_domain, config in CONFIG["approved_video_domains"].items():

        if domain == approved_domain or domain.endswith("." + approved_domain):

            return config

    return None


def is_allowed_domain(url: str) -> bool:

    return get_domain_config(url) is not None

# ---------------------------------
# In-memory storage (MVP bridge)
# ---------------------------------

wheel_entries = []
archived_wheel_entries = []
asmr_entries = []
story_entries = []
spotlight_entries = []
spotlight_submit_lock = threading.Lock()
pulse_entries = []
pulse_receipts = []
pulse_red_activations = []
pulse_red_unlock_notifications = []
pulse_question_review_notifications = []
pulse_question_suggestions = []
pulse_daily_summary_posts = []
pulse_disabled_questions = []
miniapp_verifications = []
synced_alcove_users = []
synced_alcove_analytics = {}
last_bot_sync_at = None
admin_jobs: dict = {}

current_now_playing = None
video_reviews = []
current_wheel_reaction = None
wheel_reaction_events = []
latest_review_overlay = None
wheel_reaction_history = []
wheel_review_history = []
wheel_user_engagement = {}

pending_comments = []
approved_comments = []

room_qa_items = []
room_qa_archive = []
poll_history = []
active_poll = None
room_media_submissions = []
now_showing_media = None
room_game = None
team_feeds = {}
_next_room_qa_id_seq = 1
_next_poll_id_seq = 1
_next_media_id_seq = 1
_next_team_msg_id_seq = 1
ROOM_GAME_MAX_SIZES = {2, 3, 4, 5}
ROOM_GAME_MODES = {"display", "collaborate"}

notification_feed = []

wheel_submission_limits = {}
muted_users = set()
current_winner = None
PULSE_DEFAULT_HEAT_THRESHOLD = int(os.getenv("PULSE_HEAT_THRESHOLD", "50"))
PULSE_GREEN_UNLOCK_INTERVAL_HOURS = 4
PULSE_MAX_GREEN_SLOTS = 6
PULSE_TESTING_UNLIMITED = os.getenv("PULSE_TESTING_UNLIMITED", "0").strip().lower() in {"1", "true", "yes", "on"}
LEAN_MODE = os.getenv("LEAN_MODE", "0").strip().lower() in {"1", "true", "yes", "on"}
PULSE_ADMIN_NOTIFY_ENABLED = os.getenv(
    "PULSE_ADMIN_NOTIFY_ENABLED",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}
# Hard-off pulse admin Telegram unless PULSE_ADMIN_TELEGRAM_FORCE=1 is set explicitly.
PULSE_ADMIN_TELEGRAM_SUPPRESSED = os.getenv(
    "PULSE_ADMIN_TELEGRAM_FORCE",
    "",
).strip().lower() not in {"1", "true", "yes", "on"}
PULSE_UNLIMITED_QUESTION_SUBMIT = os.getenv(
    "PULSE_UNLIMITED_QUESTION_SUBMIT",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}
PULSE_DAILY_QUESTION_LIMIT = 2
PULSE_DAILY_REJECTION_REPLACEMENT_LIMIT = 1
PULSE_MAX_QUESTION_LENGTH = 360


def pulse_question_too_long(text: str) -> str | None:
    if len(text) > PULSE_MAX_QUESTION_LENGTH:
        return f"Pulse question must be {PULSE_MAX_QUESTION_LENGTH} characters or fewer."
    return None


PULSE_RETENTION_DAYS = max(
    7,
    int(os.getenv("PULSE_RETENTION_DAYS", "14" if LEAN_MODE else "30") or (14 if LEAN_MODE else 30)),
)
CARDS_API_ENABLED = os.getenv("CARDS_API_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
PULSE_RED_UNLOCK_NOTIFY_USERNAMES_RAW = os.getenv(
    "PULSE_RED_UNLOCK_NOTIFY_USERNAMES",
    "Ardyn_Sky,The_Alcove",
).strip()
PULSE_RED_UNLOCK_DMS_ENABLED = os.getenv(
    "PULSE_RED_UNLOCK_DMS_ENABLED",
    "0",
).strip().lower() in {"1", "true", "yes", "on"}
UK_TZ = ZoneInfo("Europe/London")


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        return default
    return max(minimum, min(value, maximum))


MAX_NOTIFICATION_FEED = env_int("ALCOVE_MAX_NOTIFICATION_FEED", 250, 50, 2000)
MAX_APPROVED_COMMENTS = env_int("ALCOVE_MAX_APPROVED_COMMENTS", 500, 50, 4000)
MAX_PENDING_COMMENTS = env_int("ALCOVE_MAX_PENDING_COMMENTS", 200, 20, 2000)
MAX_VIDEO_REVIEWS = env_int("ALCOVE_MAX_VIDEO_REVIEWS", 2000, 100, 10000)
MAX_SPOTLIGHT_ENTRIES = env_int("ALCOVE_MAX_SPOTLIGHT_ENTRIES", 1500, 100, 10000)
MAX_PULSE_ENTRIES = env_int("ALCOVE_MAX_PULSE_ENTRIES", 5000, 500, 20000)
MAX_PULSE_RECEIPTS = env_int("ALCOVE_MAX_PULSE_RECEIPTS", 5000, 500, 20000)
MAX_PULSE_RED_ACTIVATIONS = env_int("ALCOVE_MAX_PULSE_RED_ACTIVATIONS", 3000, 200, 20000)
MAX_PULSE_QUESTION_SUGGESTIONS = env_int("ALCOVE_MAX_PULSE_QUESTION_SUGGESTIONS", 2000, 100, 10000)
MAX_PULSE_DAILY_SUMMARY_POSTS = env_int("ALCOVE_MAX_PULSE_DAILY_SUMMARY_POSTS", 1000, 100, 5000)
MAX_PULSE_DISABLED_QUESTIONS = env_int("ALCOVE_MAX_PULSE_DISABLED_QUESTIONS", 1000, 100, 5000)
MAX_MINIAPP_VERIFICATIONS = env_int("ALCOVE_MAX_MINIAPP_VERIFICATIONS", 2000, 100, 10000)
MAX_WHEEL_REACTION_HISTORY = env_int("ALCOVE_MAX_WHEEL_REACTION_HISTORY", 2000, 100, 10000)
MAX_WHEEL_REVIEW_HISTORY = env_int("ALCOVE_MAX_WHEEL_REVIEW_HISTORY", 2000, 100, 10000)

_last_saved_runtime_fingerprint: str | None = None
_last_pulse_prune_day: str | None = None

state = {
    "current_round": 1,
    "round_status": "closed",  # closed | open | locked | spinning | playing
    "winner_intro_loaded": False,
    "room_open": True,
    "closing_soon": False,
    "review_prompt_open": False,
    "review_reveal_active": False,
    "review_score_reveal_active": False,
    "modules": {
        "wheel": True,
        "asmr": False,
        "story": False,
        "shoutouts": False,
    },
    "room_discussion": {
        "discussion_id": None,
        "title": "",
        "duration_minutes": None,
        "started_at": None,
        "ends_at": None,
        "status": "idle",
    },
}


def lean_mode_enabled() -> bool:
    return LEAN_MODE


def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat() + "Z"


def parse_iso_utc(value) -> datetime.datetime | None:
    """Parse API timestamps into naive UTC datetimes safe to compare with utcnow()."""
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def runtime_state_payload() -> dict:
    payload = {
        "spotlight_entries": spotlight_entries,
        "pulse_entries": pulse_entries,
        "pulse_receipts": pulse_receipts,
        "pulse_red_activations": pulse_red_activations,
        "pulse_red_unlock_notifications": pulse_red_unlock_notifications,
        "pulse_question_review_notifications": pulse_question_review_notifications,
        "pulse_question_suggestions": pulse_question_suggestions,
        "pulse_daily_summary_posts": pulse_daily_summary_posts,
        "pulse_disabled_questions": pulse_disabled_questions,
        "miniapp_verifications": miniapp_verifications,
        "synced_alcove_users": synced_alcove_users,
        "last_bot_sync_at": last_bot_sync_at,
        "admin_jobs": admin_jobs,
    }
    if lean_mode_enabled():
        return payload
    payload["wheel_reaction_history"] = wheel_reaction_history
    payload["wheel_review_history"] = wheel_review_history
    payload["wheel_user_engagement"] = wheel_user_engagement
    payload["synced_alcove_analytics"] = synced_alcove_analytics
    return payload



def trim_list_in_place(items: list, limit: int) -> None:
    if limit <= 0:
        items.clear()
        return
    overflow = len(items) - limit
    if overflow > 0:
        del items[:overflow]


def trim_runtime_state_collections() -> None:
    trim_list_in_place(spotlight_entries, MAX_SPOTLIGHT_ENTRIES)
    trim_list_in_place(pulse_entries, MAX_PULSE_ENTRIES)
    trim_list_in_place(pulse_receipts, MAX_PULSE_RECEIPTS)
    trim_list_in_place(pulse_red_activations, MAX_PULSE_RED_ACTIVATIONS)
    trim_list_in_place(pulse_question_suggestions, MAX_PULSE_QUESTION_SUGGESTIONS)
    trim_list_in_place(pulse_daily_summary_posts, MAX_PULSE_DAILY_SUMMARY_POSTS)
    trim_list_in_place(pulse_disabled_questions, MAX_PULSE_DISABLED_QUESTIONS)
    trim_list_in_place(miniapp_verifications, MAX_MINIAPP_VERIFICATIONS)
    trim_list_in_place(wheel_reaction_history, MAX_WHEEL_REACTION_HISTORY)
    trim_list_in_place(wheel_review_history, MAX_WHEEL_REVIEW_HISTORY)
    trim_list_in_place(notification_feed, MAX_NOTIFICATION_FEED)
    trim_list_in_place(approved_comments, MAX_APPROVED_COMMENTS)
    trim_list_in_place(pending_comments, MAX_PENDING_COMMENTS)
    trim_list_in_place(video_reviews, MAX_VIDEO_REVIEWS)


def ensure_state_store() -> None:
    directory = os.path.dirname(STATE_DB_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_state_store (
                state_key TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


def apply_runtime_payload(payload: dict) -> None:
    global spotlight_entries, pulse_entries, pulse_receipts, pulse_red_activations, pulse_red_unlock_notifications
    global pulse_question_review_notifications, pulse_question_suggestions, pulse_daily_summary_posts, pulse_disabled_questions
    global miniapp_verifications, wheel_reaction_history, wheel_review_history, wheel_user_engagement
    global synced_alcove_users, synced_alcove_analytics, last_bot_sync_at, admin_jobs

    spotlight_entries = payload.get("spotlight_entries") if isinstance(payload.get("spotlight_entries"), list) else []
    pulse_entries = payload.get("pulse_entries") if isinstance(payload.get("pulse_entries"), list) else []
    pulse_receipts = payload.get("pulse_receipts") if isinstance(payload.get("pulse_receipts"), list) else []
    pulse_red_activations = payload.get("pulse_red_activations") if isinstance(payload.get("pulse_red_activations"), list) else []
    pulse_red_unlock_notifications = (
        payload.get("pulse_red_unlock_notifications")
        if isinstance(payload.get("pulse_red_unlock_notifications"), list)
        else []
    )
    pulse_question_review_notifications = (
        payload.get("pulse_question_review_notifications")
        if isinstance(payload.get("pulse_question_review_notifications"), list)
        else []
    )
    pulse_question_suggestions = payload.get("pulse_question_suggestions") if isinstance(payload.get("pulse_question_suggestions"), list) else []
    pulse_daily_summary_posts = payload.get("pulse_daily_summary_posts") if isinstance(payload.get("pulse_daily_summary_posts"), list) else []
    pulse_disabled_questions = payload.get("pulse_disabled_questions") if isinstance(payload.get("pulse_disabled_questions"), list) else []
    miniapp_verifications = payload.get("miniapp_verifications") if isinstance(payload.get("miniapp_verifications"), list) else []
    if lean_mode_enabled():
        wheel_reaction_history.clear()
        wheel_review_history.clear()
        wheel_user_engagement.clear()
        synced_alcove_analytics = {}
    else:
        wheel_reaction_history = payload.get("wheel_reaction_history") if isinstance(payload.get("wheel_reaction_history"), list) else []
        wheel_review_history = payload.get("wheel_review_history") if isinstance(payload.get("wheel_review_history"), list) else []
        wheel_user_engagement = payload.get("wheel_user_engagement") if isinstance(payload.get("wheel_user_engagement"), dict) else {}
        synced_alcove_analytics = payload.get("synced_alcove_analytics") if isinstance(payload.get("synced_alcove_analytics"), dict) else {}
    synced_alcove_users = payload.get("synced_alcove_users") if isinstance(payload.get("synced_alcove_users"), list) else []
    last_bot_sync_at = payload.get("last_bot_sync_at") if isinstance(payload.get("last_bot_sync_at"), str) else None
    admin_jobs = payload.get("admin_jobs") if isinstance(payload.get("admin_jobs"), dict) else {}

    trim_runtime_state_collections()


def get_admin_job(name: str) -> dict:
    job = admin_jobs.get(name)
    return job if isinstance(job, dict) else {}


def load_runtime_state_from_db() -> dict | None:
    ensure_state_store()
    try:
        with sqlite3.connect(STATE_DB_PATH) as conn:
            row = conn.execute(
                "SELECT payload_json FROM runtime_state_store WHERE state_key = ?",
                ("alcove_runtime",),
            ).fetchone()
    except sqlite3.Error:
        return None
    if not row or not row[0]:
        return None
    try:
        payload = json.loads(row[0])
    except (TypeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def save_runtime_state_to_db(payload: dict) -> None:
    ensure_state_store()
    serialized = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO runtime_state_store (state_key, payload_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(state_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                updated_at = excluded.updated_at
            """,
            ("alcove_runtime", serialized, datetime.datetime.utcnow().isoformat()),
        )
        conn.commit()


def ensure_pulse_archive_store() -> None:
    ensure_state_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pulse_archive_meta (
                meta_key TEXT PRIMARY KEY,
                meta_value TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pulse_archive_questions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                suggestion_id INTEGER UNIQUE,
                pool TEXT NOT NULL,
                question TEXT NOT NULL,
                category TEXT,
                status TEXT,
                active_from_day_key TEXT,
                submitted_at TEXT,
                reviewed_at TEXT,
                user_id INTEGER,
                username TEXT,
                display_name TEXT,
                source TEXT,
                archived_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pulse_archive_answers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                pulse_id INTEGER NOT NULL UNIQUE,
                suggestion_id INTEGER,
                day_key TEXT NOT NULL,
                pool TEXT NOT NULL,
                category TEXT,
                question TEXT NOT NULL,
                answer TEXT NOT NULL,
                responded_at TEXT,
                sent_at TEXT,
                sender_user_id INTEGER,
                sender_username TEXT,
                sender_display_name TEXT,
                question_owner_user_id INTEGER,
                question_owner_username TEXT,
                question_owner_display_name TEXT,
                archived_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_archive_answers_day ON pulse_archive_answers(day_key)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_archive_answers_pool ON pulse_archive_answers(pool)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_archive_answers_responded ON pulse_archive_answers(responded_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_pulse_archive_questions_day ON pulse_archive_questions(active_from_day_key)"
        )
        conn.commit()


def pulse_archive_meta_get(key: str) -> str | None:
    ensure_pulse_archive_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        row = conn.execute(
            "SELECT meta_value FROM pulse_archive_meta WHERE meta_key = ?",
            (key,),
        ).fetchone()
    return row[0] if row else None


def pulse_archive_meta_set(key: str, value: str) -> None:
    ensure_pulse_archive_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO pulse_archive_meta (meta_key, meta_value)
            VALUES (?, ?)
            ON CONFLICT(meta_key) DO UPDATE SET meta_value = excluded.meta_value
            """,
            (key, value),
        )
        conn.commit()


def pulse_suggestion_id_for_question(question: str, pool: str) -> int | None:
    question = (question or "").strip()
    pool = (pool or "green").strip().lower()
    for entry in pulse_question_suggestions:
        if (entry.get("pool") or "green") != pool:
            continue
        if pulse_suggestion_question(entry) == question:
            suggestion_id = entry.get("id")
            if suggestion_id is not None:
                return int(suggestion_id)
    return None


# Only approved/reserved Pulse questions belong in the member-facing archive.
MEMBER_PULSE_ARCHIVE_QUESTION_STATUSES = ("approved", "reserved")


def archive_pulse_question_from_suggestion(entry: dict) -> None:
    if not entry:
        return
    suggestion_id = entry.get("id")
    if suggestion_id is None:
        return
    # Keep pending/rejected/deleted out of the durable archive.
    if (entry.get("status") or "").strip().lower() not in MEMBER_PULSE_ARCHIVE_QUESTION_STATUSES:
        return
    question = pulse_suggestion_question(entry)
    if not question:
        return
    ensure_pulse_archive_store()
    archived_at = now_iso()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO pulse_archive_questions (
                suggestion_id, pool, question, category, status, active_from_day_key,
                submitted_at, reviewed_at, user_id, username, display_name, source, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(suggestion_id) DO UPDATE SET
                pool = excluded.pool,
                question = excluded.question,
                category = excluded.category,
                status = excluded.status,
                active_from_day_key = excluded.active_from_day_key,
                submitted_at = excluded.submitted_at,
                reviewed_at = excluded.reviewed_at,
                user_id = excluded.user_id,
                username = excluded.username,
                display_name = excluded.display_name,
                source = excluded.source,
                archived_at = excluded.archived_at
            """,
            (
                int(suggestion_id),
                (entry.get("pool") or "green").strip().lower(),
                question,
                entry.get("category") or "General",
                entry.get("status"),
                entry.get("active_from_day_key"),
                entry.get("submitted_at"),
                entry.get("reviewed_at"),
                entry.get("user_id"),
                entry.get("username"),
                entry.get("display_name"),
                entry.get("source"),
                archived_at,
            ),
        )
        conn.commit()


def archive_completed_pulse_entry(entry: dict) -> None:
    if not entry or entry.get("status") != "completed":
        return
    pulse_id = entry.get("id")
    if pulse_id is None:
        return
    question = (entry.get("question") or "").strip()
    answer = (entry.get("response_answer") or entry.get("sender_note") or entry.get("answer") or "").strip()
    if not question or not answer:
        return
    pool = (entry.get("pulse_type") or "green").strip().lower()
    suggestion_id = pulse_suggestion_id_for_question(question, pool)
    ensure_pulse_archive_store()
    archived_at = now_iso()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO pulse_archive_answers (
                pulse_id, suggestion_id, day_key, pool, category, question, answer,
                responded_at, sent_at, sender_user_id, sender_username, sender_display_name,
                question_owner_user_id, question_owner_username, question_owner_display_name,
                archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pulse_id) DO UPDATE SET
                suggestion_id = excluded.suggestion_id,
                day_key = excluded.day_key,
                pool = excluded.pool,
                category = excluded.category,
                question = excluded.question,
                answer = excluded.answer,
                responded_at = excluded.responded_at,
                sent_at = excluded.sent_at,
                sender_user_id = excluded.sender_user_id,
                sender_username = excluded.sender_username,
                sender_display_name = excluded.sender_display_name,
                question_owner_user_id = excluded.question_owner_user_id,
                question_owner_username = excluded.question_owner_username,
                question_owner_display_name = excluded.question_owner_display_name,
                archived_at = excluded.archived_at
            """,
            (
                int(pulse_id),
                suggestion_id,
                entry.get("day_key") or pulse_day_key(),
                pool,
                entry.get("category") or pulse_question_category(question, pool),
                question,
                answer,
                entry.get("responded_at") or entry.get("sent_at"),
                entry.get("sent_at"),
                entry.get("sender_user_id") or entry.get("responder_user_id"),
                entry.get("sender_username") or entry.get("responder_username"),
                entry.get("sender_display_name") or entry.get("responder_display_name"),
                entry.get("question_owner_user_id"),
                entry.get("question_owner_username"),
                entry.get("question_owner_display_name"),
                archived_at,
            ),
        )
        conn.commit()


def pulse_archive_backfill() -> dict:
    if pulse_archive_meta_get("backfill_v1") == "done":
        return {"answers": 0, "questions": 0, "skipped": True}
    answers = 0
    questions = 0
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        archive_completed_pulse_entry(entry)
        answers += 1
    for entry in pulse_question_suggestions:
        # Member archive should only keep approved/reserved history, not pending review.
        if entry.get("status") not in {"approved", "reserved"}:
            continue
        archive_pulse_question_from_suggestion(entry)
        questions += 1
    pulse_archive_meta_set("backfill_v1", "done")
    return {"answers": answers, "questions": questions, "skipped": False}


def permanently_delete_pulse_archive_answer(pulse_id: int) -> dict:
    """Hard-delete one archived answer and its live runtime twin (if present)."""
    ensure_pulse_archive_store()
    pid = int(pulse_id)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        cur = conn.execute("DELETE FROM pulse_archive_answers WHERE pulse_id = ?", (pid,))
        archive_deleted = int(cur.rowcount or 0)
        conn.commit()
    runtime_deleted = 0
    kept = []
    for entry in pulse_entries:
        if int(entry.get("id") or 0) == pid:
            runtime_deleted += 1
            continue
        kept.append(entry)
    if runtime_deleted:
        pulse_entries[:] = kept
        save_runtime_state(force=True)
    if archive_deleted <= 0 and runtime_deleted <= 0:
        raise HTTPException(status_code=404, detail="Pulse answer not found")
    return {
        "pulse_id": pid,
        "archive_deleted": archive_deleted,
        "runtime_deleted": runtime_deleted,
    }


def permanently_delete_pulse_archive_question(suggestion_id: int) -> dict:
    """Hard-delete one archived question, cascade its answers, and soft-delete the live suggestion."""
    ensure_pulse_archive_store()
    sid = int(suggestion_id)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        row = conn.execute(
            """
            SELECT suggestion_id, question, pool
            FROM pulse_archive_questions
            WHERE suggestion_id = ?
            """,
            (sid,),
        ).fetchone()
        if not row:
            # Still allow deleting a live suggestion even if it never reached archive.
            question = None
            pool = None
        else:
            question = (row[1] or "").strip()
            pool = (row[2] or "green").strip().lower()
        answers_deleted = 0
        if question:
            cur = conn.execute(
                """
                DELETE FROM pulse_archive_answers
                WHERE suggestion_id = ?
                   OR (COALESCE(suggestion_id, 0) = 0 AND question = ? AND pool = ?)
                """,
                (sid, question, pool or "green"),
            )
            answers_deleted = int(cur.rowcount or 0)
        else:
            cur = conn.execute(
                "DELETE FROM pulse_archive_answers WHERE suggestion_id = ?",
                (sid,),
            )
            answers_deleted = int(cur.rowcount or 0)
        qcur = conn.execute(
            "DELETE FROM pulse_archive_questions WHERE suggestion_id = ?",
            (sid,),
        )
        questions_deleted = int(qcur.rowcount or 0)
        conn.commit()

    runtime_answers_deleted = 0
    if question:
        kept = []
        for entry in pulse_entries:
            entry_question = (entry.get("question") or "").strip()
            entry_pool = (entry.get("pulse_type") or "green").strip().lower()
            if entry_question == question and entry_pool == (pool or "green"):
                runtime_answers_deleted += 1
                continue
            kept.append(entry)
        if runtime_answers_deleted:
            pulse_entries[:] = kept

    suggestion_deleted = False
    for entry in pulse_question_suggestions:
        if int(entry.get("id") or 0) != sid:
            continue
        cancel_pending_pulse_question_review_notifications(sid, reason="deleted")
        entry["status"] = "deleted"
        entry["rejection_reason"] = None
        entry["resubmit_allowed"] = False
        entry["needs_admin_notify"] = False
        entry["reviewed_at"] = now_iso()
        suggestion_deleted = True
        break

    if questions_deleted <= 0 and answers_deleted <= 0 and not suggestion_deleted and runtime_answers_deleted <= 0:
        raise HTTPException(status_code=404, detail="Pulse question not found")

    if suggestion_deleted or runtime_answers_deleted:
        save_runtime_state(force=True)

    return {
        "suggestion_id": sid,
        "questions_deleted": questions_deleted,
        "answers_deleted": answers_deleted,
        "runtime_answers_deleted": runtime_answers_deleted,
        "suggestion_soft_deleted": suggestion_deleted,
    }


def pulse_archive_answer_row_to_payload(row: tuple) -> dict:
    keys = (
        "archive_id", "pulse_id", "suggestion_id", "day_key", "pool", "category",
        "question", "answer", "responded_at", "sent_at", "sender_user_id",
        "sender_username", "sender_display_name", "question_owner_user_id",
        "question_owner_username", "question_owner_display_name", "archived_at",
    )
    payload = dict(zip(keys, row))
    payload["day_label"] = pulse_day_label(payload.get("day_key")) if payload.get("day_key") else None
    return payload


def pulse_archive_question_row_to_payload(row: tuple) -> dict:
    keys = (
        "archive_id", "suggestion_id", "pool", "question", "category", "status",
        "active_from_day_key", "submitted_at", "reviewed_at", "user_id", "username",
        "display_name", "source", "archived_at",
    )
    payload = dict(zip(keys, row))
    payload["active_from_label"] = (
        pulse_day_label(payload.get("active_from_day_key"))
        if payload.get("active_from_day_key")
        else None
    )
    payload["answers_count"] = pulse_archive_answer_count_for_question(
        payload.get("question"),
        payload.get("pool"),
    )
    return payload


def pulse_archive_answer_count_for_question(question: str | None, pool: str | None = None) -> int:
    question = (question or "").strip()
    if not question:
        return 0
    ensure_pulse_archive_store()
    query = "SELECT COUNT(*) FROM pulse_archive_answers WHERE question = ?"
    params: list = [question]
    if pool:
        query += " AND pool = ?"
        params.append((pool or "green").strip().lower())
    with sqlite3.connect(STATE_DB_PATH) as conn:
        row = conn.execute(query, params).fetchone()
    return int(row[0] or 0) if row else 0


def pulse_archive_stats_payload() -> dict:
    ensure_pulse_archive_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        answer_count = int(conn.execute("SELECT COUNT(*) FROM pulse_archive_answers").fetchone()[0] or 0)
        question_count = int(conn.execute("SELECT COUNT(*) FROM pulse_archive_questions").fetchone()[0] or 0)
        first_day = conn.execute(
            "SELECT MIN(day_key) FROM pulse_archive_answers WHERE day_key != ''"
        ).fetchone()[0]
        last_day = conn.execute(
            "SELECT MAX(day_key) FROM pulse_archive_answers WHERE day_key != ''"
        ).fetchone()[0]
    return {
        "answer_count": answer_count,
        "question_count": question_count,
        "first_day_key": first_day,
        "last_day_key": last_day,
        "first_day_label": pulse_day_label(first_day) if first_day else None,
        "last_day_label": pulse_day_label(last_day) if last_day else None,
    }


def parse_archive_pool_filter(pool: str | None) -> list[str] | None:
    if not pool:
        return None
    pools = [item.strip().lower() for item in pool.split(",") if item.strip()]
    return pools or None


def pulse_archive_answer_order_sql(sort: str | None = None) -> str:
    sort_key = (sort or "date_desc").strip().lower()
    if sort_key == "date_asc":
        return "COALESCE(responded_at, sent_at, archived_at) ASC, pulse_id ASC"
    if sort_key == "question_az":
        return "LOWER(question) ASC, COALESCE(responded_at, sent_at, archived_at) DESC, pulse_id DESC"
    if sort_key == "question_za":
        return "LOWER(question) DESC, COALESCE(responded_at, sent_at, archived_at) DESC, pulse_id DESC"
    return "COALESCE(responded_at, sent_at, archived_at) DESC, pulse_id DESC"


def pulse_archive_question_order_sql(sort: str | None = None) -> str:
    sort_key = (sort or "date_desc").strip().lower()
    if sort_key == "date_asc":
        return "COALESCE(active_from_day_key, submitted_at, archived_at) ASC, suggestion_id ASC"
    if sort_key == "question_az":
        return "LOWER(question) ASC, COALESCE(active_from_day_key, submitted_at, archived_at) DESC, suggestion_id DESC"
    if sort_key == "question_za":
        return "LOWER(question) DESC, COALESCE(active_from_day_key, submitted_at, archived_at) DESC, suggestion_id DESC"
    return "COALESCE(active_from_day_key, submitted_at, archived_at) DESC, suggestion_id DESC"


def archive_viewer_identity(user_id: int | None = None, username: str | None = None) -> dict:
    clean_username = (username or "").strip().lstrip("@") or None
    return {
        "user_id": int(user_id) if user_id is not None else None,
        "username": clean_username,
    }


def query_pulse_archive_answers(
    *,
    day: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    pool: str | None = None,
    q: str | None = None,
    username: str | None = None,
    suggestion_id: int | None = None,
    sort: str | None = None,
    mine_only: bool = False,
    viewer_user_id: int | None = None,
    viewer_username: str | None = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    ensure_pulse_archive_store()
    clauses = ["1=1"]
    params: list = []
    if day:
        clauses.append("day_key = ?")
        params.append(day.strip())
    if from_day:
        clauses.append("day_key >= ?")
        params.append(from_day.strip())
    if to_day:
        clauses.append("day_key <= ?")
        params.append(to_day.strip())
    pools = parse_archive_pool_filter(pool)
    if pools:
        if len(pools) == 1:
            clauses.append("pool = ?")
            params.append(pools[0])
        else:
            clauses.append(f"pool IN ({','.join('?' * len(pools))})")
            params.extend(pools)
    if suggestion_id:
        clauses.append("suggestion_id = ?")
        params.append(int(suggestion_id))
    needle = (q or "").strip().lower()
    if needle:
        clauses.append("(LOWER(question) LIKE ? OR LOWER(answer) LIKE ?)")
        params.extend([f"%{needle}%", f"%{needle}%"])
    uname = (username or "").strip().lower().lstrip("@")
    if uname:
        clauses.append("LOWER(COALESCE(sender_username, '')) LIKE ?")
        params.append(f"%{uname}%")
    if mine_only:
        viewer_username_clean = (viewer_username or "").strip().lower().lstrip("@")
        if viewer_user_id is not None:
            clauses.append("(sender_user_id = ? OR question_owner_user_id = ?)")
            params.extend([int(viewer_user_id), int(viewer_user_id)])
        elif viewer_username_clean:
            clauses.append(
                "(LOWER(COALESCE(sender_username, '')) = ? OR LOWER(COALESCE(question_owner_username, '')) = ?)"
            )
            params.extend([viewer_username_clean, viewer_username_clean])
    where_sql = " AND ".join(clauses)
    offset = max(0, (max(1, page) - 1) * max(1, min(limit, 200)))
    row_limit = max(1, min(limit, 200))
    order_sql = pulse_archive_answer_order_sql(sort)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM pulse_archive_answers WHERE {where_sql}",
            params,
        ).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT id, pulse_id, suggestion_id, day_key, pool, category, question, answer,
                   responded_at, sent_at, sender_user_id, sender_username, sender_display_name,
                   question_owner_user_id, question_owner_username, question_owner_display_name,
                   archived_at
            FROM pulse_archive_answers
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, row_limit, offset],
        ).fetchall()
    return {
        "entries": [pulse_archive_answer_row_to_payload(row) for row in rows],
        "total": total,
        "page": max(1, page),
        "limit": row_limit,
        "pages": max(1, (total + row_limit - 1) // row_limit),
    }


def query_pulse_archive_questions(
    *,
    pool: str | None = None,
    status: str | None = None,
    statuses: list[str] | None = None,
    q: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str | None = None,
    mine_only: bool = False,
    viewer_user_id: int | None = None,
    viewer_username: str | None = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    ensure_pulse_archive_store()
    clauses = ["1=1"]
    params: list = []
    pools = parse_archive_pool_filter(pool)
    if pools:
        if len(pools) == 1:
            clauses.append("pool = ?")
            params.append(pools[0])
        else:
            clauses.append(f"pool IN ({','.join('?' * len(pools))})")
            params.extend(pools)
    status_list = [item.strip() for item in (statuses or []) if item and str(item).strip()]
    if status_list:
        clauses.append(f"status IN ({','.join('?' * len(status_list))})")
        params.extend(status_list)
    elif status:
        clauses.append("status = ?")
        params.append(status.strip())
    needle = (q or "").strip().lower()
    if needle:
        clauses.append("LOWER(question) LIKE ?")
        params.append(f"%{needle}%")
    if from_day:
        clauses.append("COALESCE(active_from_day_key, '') >= ?")
        params.append(from_day.strip())
    if to_day:
        clauses.append("COALESCE(active_from_day_key, '') <= ?")
        params.append(to_day.strip())
    if mine_only:
        viewer_username_clean = (viewer_username or "").strip().lower().lstrip("@")
        if viewer_user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(viewer_user_id))
        elif viewer_username_clean:
            clauses.append("LOWER(COALESCE(username, '')) = ?")
            params.append(viewer_username_clean)
    where_sql = " AND ".join(clauses)
    offset = max(0, (max(1, page) - 1) * max(1, min(limit, 200)))
    row_limit = max(1, min(limit, 200))
    order_sql = pulse_archive_question_order_sql(sort)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM pulse_archive_questions WHERE {where_sql}",
            params,
        ).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT id, suggestion_id, pool, question, category, status, active_from_day_key,
                   submitted_at, reviewed_at, user_id, username, display_name, source, archived_at
            FROM pulse_archive_questions
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, row_limit, offset],
        ).fetchall()
    return {
        "entries": [pulse_archive_question_row_to_payload(row) for row in rows],
        "total": total,
        "page": max(1, page),
        "limit": row_limit,
        "pages": max(1, (total + row_limit - 1) // row_limit),
    }


def pulse_archive_answers_csv(filters: dict) -> str:
    result = query_pulse_archive_answers(
        day=filters.get("day"),
        from_day=filters.get("from_day"),
        to_day=filters.get("to_day"),
        pool=filters.get("pool"),
        q=filters.get("q"),
        username=filters.get("username"),
        suggestion_id=filters.get("suggestion_id"),
        page=1,
        limit=200,
    )
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "day_key", "pool", "pulse_id", "suggestion_id", "question", "answer",
        "responded_at", "sender_username", "sender_display_name", "sender_user_id",
        "question_owner_username", "category",
    ])
    total = result["total"]
    writer.writerows([
        [
            row.get("day_key"), row.get("pool"), row.get("pulse_id"), row.get("suggestion_id"),
            row.get("question"), row.get("answer"), row.get("responded_at"),
            row.get("sender_username"), row.get("sender_display_name"), row.get("sender_user_id"),
            row.get("question_owner_username"), row.get("category"),
        ]
        for row in result["entries"]
    ])
    page = 2
    while len(result["entries"]) and (page - 1) * 200 < total:
        result = query_pulse_archive_answers(
            day=filters.get("day"),
            from_day=filters.get("from_day"),
            to_day=filters.get("to_day"),
            pool=filters.get("pool"),
            q=filters.get("q"),
            username=filters.get("username"),
            suggestion_id=filters.get("suggestion_id"),
            page=page,
            limit=200,
        )
        writer.writerows([
            [
                row.get("day_key"), row.get("pool"), row.get("pulse_id"), row.get("suggestion_id"),
                row.get("question"), row.get("answer"), row.get("responded_at"),
                row.get("sender_username"), row.get("sender_display_name"), row.get("sender_user_id"),
                row.get("question_owner_username"), row.get("category"),
            ]
            for row in result["entries"]
        ])
        page += 1
    return buffer.getvalue()


def sanitize_member_pulse_answer(entry: dict, viewer: dict) -> dict:
    owner = {
        "user_id": entry.get("question_owner_user_id"),
        "username": entry.get("question_owner_username"),
    }
    sender = {
        "user_id": entry.get("sender_user_id"),
        "username": entry.get("sender_username"),
    }
    is_my_answer = bool(viewer and pulse_identities_match(viewer, sender))
    is_my_question = bool(viewer and pulse_identities_match(viewer, owner))
    payload = {
        "archive_id": entry.get("archive_id"),
        "pulse_id": entry.get("pulse_id"),
        "suggestion_id": entry.get("suggestion_id"),
        "day_key": entry.get("day_key"),
        "day_label": entry.get("day_label"),
        "pool": entry.get("pool"),
        "category": entry.get("category"),
        "question": entry.get("question"),
        "answer": entry.get("answer"),
        "responded_at": entry.get("responded_at") or entry.get("sent_at"),
        "is_my_answer": is_my_answer,
        "is_my_question": is_my_question,
    }
    return payload


def sanitize_member_pulse_question(entry: dict, viewer: dict, answers: list[dict] | None = None) -> dict:
    owner = {
        "user_id": entry.get("user_id"),
        "username": entry.get("username"),
    }
    is_my_question = bool(viewer and pulse_identities_match(viewer, owner))
    sanitized_answers = [sanitize_member_pulse_answer(answer, viewer) for answer in (answers or [])]
    return {
        "archive_id": entry.get("archive_id"),
        "suggestion_id": entry.get("suggestion_id"),
        "pool": entry.get("pool"),
        "question": entry.get("question"),
        "category": entry.get("category"),
        "status": entry.get("status"),
        "active_from_day_key": entry.get("active_from_day_key"),
        "active_from_label": entry.get("active_from_label"),
        "submitted_at": entry.get("submitted_at"),
        "answers_count": entry.get("answers_count") or len(sanitized_answers),
        "is_my_question": is_my_question,
        "answers": sanitized_answers,
    }


def pulse_archive_answers_for_question(question: str, pool: str | None = None, limit: int = 200) -> list[dict]:
    result = query_pulse_archive_answers(
        q=question,
        pool=pool,
        page=1,
        limit=limit,
        sort="date_desc",
    )
    return result.get("entries") or []


def build_member_pulse_archive_payload(
    *,
    viewer: dict,
    view: str = "questions",
    pool: str | None = None,
    q: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str | None = None,
    mine_only: bool = False,
    page: int = 1,
    limit: int = 50,
) -> dict:
    viewer_user_id = viewer.get("user_id")
    viewer_username = viewer.get("username")
    stats = pulse_archive_stats_payload()
    payload = {
        "stats": {
            "question_count": stats.get("question_count", 0),
            "answer_count": stats.get("answer_count", 0),
            "first_day_label": stats.get("first_day_label"),
            "last_day_label": stats.get("last_day_label"),
        },
    }
    view_key = (view or "questions").strip().lower()
    if view_key in {"questions", "all"}:
        question_result = query_pulse_archive_questions(
            pool=pool,
            # Pending / rejected / deleted questions stay out of the member Archive.
            statuses=list(MEMBER_PULSE_ARCHIVE_QUESTION_STATUSES),
            q=q,
            from_day=from_day,
            to_day=to_day,
            sort=sort,
            mine_only=mine_only,
            viewer_user_id=viewer_user_id,
            viewer_username=viewer_username,
            page=page,
            limit=limit,
        )
        questions = []
        for entry in question_result.get("entries") or []:
            answers = pulse_archive_answers_for_question(entry.get("question") or "", entry.get("pool"))
            if q:
                needle = (q or "").strip().lower()
                answers = [
                    answer for answer in answers
                    if needle in (answer.get("question") or "").lower()
                    or needle in (answer.get("answer") or "").lower()
                ]
            if mine_only:
                answers = [
                    answer for answer in answers
                    if pulse_identities_match(viewer, {
                        "user_id": answer.get("sender_user_id"),
                        "username": answer.get("sender_username"),
                    })
                    or pulse_identities_match(viewer, {
                        "user_id": answer.get("question_owner_user_id"),
                        "username": answer.get("question_owner_username"),
                    })
                ]
            questions.append(sanitize_member_pulse_question(entry, viewer, answers))
        payload["questions"] = questions
        payload["questions_total"] = question_result.get("total", 0)
        payload["questions_page"] = question_result.get("page", 1)
        payload["questions_pages"] = question_result.get("pages", 1)
    if view_key in {"answers", "all"}:
        answer_result = query_pulse_archive_answers(
            pool=pool,
            q=q,
            from_day=from_day,
            to_day=to_day,
            sort=sort,
            mine_only=mine_only,
            viewer_user_id=viewer_user_id,
            viewer_username=viewer_username,
            page=page,
            limit=limit,
        )
        payload["answers"] = [
            sanitize_member_pulse_answer(entry, viewer)
            for entry in (answer_result.get("entries") or [])
        ]
        payload["answers_total"] = answer_result.get("total", 0)
        payload["answers_page"] = answer_result.get("page", 1)
        payload["answers_pages"] = answer_result.get("pages", 1)
    if view_key == "questions":
        payload["entries"] = payload.get("questions", [])
        payload["total"] = payload.get("questions_total", 0)
        payload["page"] = payload.get("questions_page", 1)
        payload["pages"] = payload.get("questions_pages", 1)
    elif view_key == "answers":
        payload["entries"] = payload.get("answers", [])
        payload["total"] = payload.get("answers_total", 0)
        payload["page"] = payload.get("answers_page", 1)
        payload["pages"] = payload.get("answers_pages", 1)
    return payload


def ensure_spotlight_archive_store() -> None:
    ensure_state_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS spotlight_archive (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spotlight_id INTEGER UNIQUE NOT NULL,
                nominee_user_id INTEGER,
                nominee_username TEXT,
                nominee_display_name TEXT,
                nominator_user_id INTEGER,
                nominator_username TEXT,
                nominator_display_name TEXT,
                reason TEXT NOT NULL,
                edited_reason TEXT,
                style TEXT NOT NULL,
                day_key TEXT,
                published_at TEXT NOT NULL,
                archived_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotlight_archive_published ON spotlight_archive(published_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_spotlight_archive_style ON spotlight_archive(style)"
        )
        conn.commit()


def archive_published_spotlight(entry: dict) -> None:
    if not entry or entry.get("status") != "approved" or not entry.get("published_at"):
        return
    spotlight_id = entry.get("id")
    if spotlight_id is None:
        return
    reason = (entry.get("edited_reason") or entry.get("reason") or "").strip()
    if not reason:
        return
    ensure_spotlight_archive_store()
    archived_at = now_iso()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO spotlight_archive (
                spotlight_id, nominee_user_id, nominee_username, nominee_display_name,
                nominator_user_id, nominator_username, nominator_display_name,
                reason, edited_reason, style, day_key, published_at, archived_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(spotlight_id) DO UPDATE SET
                nominee_user_id = excluded.nominee_user_id,
                nominee_username = excluded.nominee_username,
                nominee_display_name = excluded.nominee_display_name,
                nominator_user_id = excluded.nominator_user_id,
                nominator_username = excluded.nominator_username,
                nominator_display_name = excluded.nominator_display_name,
                reason = excluded.reason,
                edited_reason = excluded.edited_reason,
                style = excluded.style,
                day_key = excluded.day_key,
                published_at = excluded.published_at,
                archived_at = excluded.archived_at
            """,
            (
                int(spotlight_id),
                entry.get("nominee_user_id"),
                entry.get("nominee_username"),
                entry.get("nominee_display_name"),
                entry.get("nominator_user_id"),
                entry.get("nominator_username"),
                entry.get("nominator_display_name"),
                reason,
                entry.get("edited_reason"),
                (entry.get("style") or "gold").strip().lower(),
                entry.get("day_key"),
                entry.get("published_at"),
                archived_at,
            ),
        )
        conn.commit()


def backfill_spotlight_archive_from_runtime() -> None:
    for entry in spotlight_entries:
        if entry.get("status") == "approved" and entry.get("published_at"):
            archive_published_spotlight(entry)


def spotlight_archive_row_to_payload(row: tuple) -> dict:
    keys = (
        "archive_id", "spotlight_id", "nominee_user_id", "nominee_username", "nominee_display_name",
        "nominator_user_id", "nominator_username", "nominator_display_name", "reason", "edited_reason",
        "style", "day_key", "published_at", "archived_at",
    )
    payload = dict(zip(keys, row))
    payload["reason"] = (payload.get("edited_reason") or payload.get("reason") or "").strip()
    payload["day_label"] = pulse_day_label(payload.get("day_key")) if payload.get("day_key") else None
    return payload


def query_spotlight_archive(
    *,
    style: str | None = None,
    nominator: str | None = None,
    nominee: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str | None = None,
    mine: str | None = None,
    viewer_user_id: int | None = None,
    viewer_username: str | None = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    backfill_spotlight_archive_from_runtime()
    ensure_spotlight_archive_store()
    clauses = ["1=1"]
    params: list = []
    styles = parse_archive_pool_filter(style)
    if styles:
        if len(styles) == 1:
            clauses.append("LOWER(style) = ?")
            params.append(styles[0])
        else:
            clauses.append(f"LOWER(style) IN ({','.join('?' * len(styles))})")
            params.extend(styles)
    nominator_needle = (nominator or "").strip().lower().lstrip("@")
    if nominator_needle:
        clauses.append(
            "(LOWER(COALESCE(nominator_username, '')) LIKE ? OR LOWER(COALESCE(nominator_display_name, '')) LIKE ?)"
        )
        params.extend([f"%{nominator_needle}%", f"%{nominator_needle}%"])
    nominee_needle = (nominee or "").strip().lower().lstrip("@")
    if nominee_needle:
        clauses.append(
            "(LOWER(COALESCE(nominee_username, '')) LIKE ? OR LOWER(COALESCE(nominee_display_name, '')) LIKE ?)"
        )
        params.extend([f"%{nominee_needle}%", f"%{nominee_needle}%"])
    if from_day:
        clauses.append("COALESCE(day_key, '') >= ?")
        params.append(from_day.strip())
    if to_day:
        clauses.append("COALESCE(day_key, '') <= ?")
        params.append(to_day.strip())
    mine_key = (mine or "").strip().lower()
    viewer_username_clean = (viewer_username or "").strip().lower().lstrip("@")
    if mine_key == "sent":
        if viewer_user_id is not None:
            clauses.append("nominator_user_id = ?")
            params.append(int(viewer_user_id))
        elif viewer_username_clean:
            clauses.append("LOWER(COALESCE(nominator_username, '')) = ?")
            params.append(viewer_username_clean)
    elif mine_key == "awarded":
        if viewer_user_id is not None:
            clauses.append("nominee_user_id = ?")
            params.append(int(viewer_user_id))
        elif viewer_username_clean:
            clauses.append("LOWER(COALESCE(nominee_username, '')) = ?")
            params.append(viewer_username_clean)
    elif mine_key == "any" and (viewer_user_id is not None or viewer_username_clean):
        if viewer_user_id is not None:
            clauses.append("(nominator_user_id = ? OR nominee_user_id = ?)")
            params.extend([int(viewer_user_id), int(viewer_user_id)])
        elif viewer_username_clean:
            clauses.append(
                "(LOWER(COALESCE(nominator_username, '')) = ? OR LOWER(COALESCE(nominee_username, '')) = ?)"
            )
            params.extend([viewer_username_clean, viewer_username_clean])
    where_sql = " AND ".join(clauses)
    sort_key = (sort or "date_desc").strip().lower()
    if sort_key == "date_asc":
        order_sql = "published_at ASC, spotlight_id ASC"
    elif sort_key == "nominee_az":
        order_sql = "LOWER(COALESCE(nominee_display_name, nominee_username, '')) ASC, published_at DESC"
    elif sort_key == "nominee_za":
        order_sql = "LOWER(COALESCE(nominee_display_name, nominee_username, '')) DESC, published_at DESC"
    else:
        order_sql = "published_at DESC, spotlight_id DESC"
    offset = max(0, (max(1, page) - 1) * max(1, min(limit, 200)))
    row_limit = max(1, min(limit, 200))
    with sqlite3.connect(STATE_DB_PATH) as conn:
        total = int(conn.execute(
            f"SELECT COUNT(*) FROM spotlight_archive WHERE {where_sql}",
            params,
        ).fetchone()[0] or 0)
        rows = conn.execute(
            f"""
            SELECT id, spotlight_id, nominee_user_id, nominee_username, nominee_display_name,
                   nominator_user_id, nominator_username, nominator_display_name,
                   reason, edited_reason, style, day_key, published_at, archived_at
            FROM spotlight_archive
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?
            """,
            [*params, row_limit, offset],
        ).fetchall()
    return {
        "entries": [spotlight_archive_row_to_payload(row) for row in rows],
        "total": total,
        "page": max(1, page),
        "limit": row_limit,
        "pages": max(1, (total + row_limit - 1) // row_limit),
    }


def sanitize_member_spotlight_entry(entry: dict, viewer: dict) -> dict:
    nominator = {
        "user_id": entry.get("nominator_user_id"),
        "username": entry.get("nominator_username"),
    }
    nominee = {
        "user_id": entry.get("nominee_user_id"),
        "username": entry.get("nominee_username"),
    }
    return {
        "spotlight_id": entry.get("spotlight_id"),
        "nominee_display_name": entry.get("nominee_display_name") or entry.get("nominee_username"),
        "nominee_username": entry.get("nominee_username"),
        "nominator_display_name": entry.get("nominator_display_name") or entry.get("nominator_username"),
        "nominator_username": entry.get("nominator_username"),
        "reason": entry.get("reason"),
        "style": entry.get("style"),
        "published_at": entry.get("published_at"),
        "day_key": entry.get("day_key"),
        "day_label": entry.get("day_label"),
        "is_sent_by_me": bool(viewer and pulse_identities_match(viewer, nominator)),
        "is_awarded_to_me": bool(viewer and pulse_identities_match(viewer, nominee)),
    }


def spotlight_archive_stats_payload() -> dict:
    ensure_spotlight_archive_store()
    backfill_spotlight_archive_from_runtime()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        count = int(conn.execute("SELECT COUNT(*) FROM spotlight_archive").fetchone()[0] or 0)
        first_day = conn.execute(
            "SELECT MIN(day_key) FROM spotlight_archive WHERE COALESCE(day_key, '') != ''"
        ).fetchone()[0]
        last_day = conn.execute(
            "SELECT MAX(day_key) FROM spotlight_archive WHERE COALESCE(day_key, '') != ''"
        ).fetchone()[0]
    return {
        "entry_count": count,
        "first_day_key": first_day,
        "last_day_key": last_day,
        "first_day_label": pulse_day_label(first_day) if first_day else None,
        "last_day_label": pulse_day_label(last_day) if last_day else None,
    }


def permanently_delete_spotlight_archive_entry(spotlight_id: int) -> dict:
    """Hard-delete one published Spotlight archive row and its runtime twin if present."""
    ensure_spotlight_archive_store()
    sid = int(spotlight_id)
    with sqlite3.connect(STATE_DB_PATH) as conn:
        cur = conn.execute("DELETE FROM spotlight_archive WHERE spotlight_id = ?", (sid,))
        archive_deleted = int(cur.rowcount or 0)
        conn.commit()
    runtime_deleted = 0
    kept = []
    for entry in spotlight_entries:
        if int(entry.get("id") or 0) == sid:
            runtime_deleted += 1
            continue
        kept.append(entry)
    if runtime_deleted:
        spotlight_entries[:] = kept
        save_runtime_state(force=True)
    if archive_deleted <= 0 and runtime_deleted <= 0:
        raise HTTPException(status_code=404, detail="Spotlight archive entry not found")
    return {
        "spotlight_id": sid,
        "archive_deleted": archive_deleted,
        "runtime_deleted": runtime_deleted,
    }


def permanently_delete_spotlight_runtime_entry(spotlight_id: int) -> dict:
    """Permanently remove a runtime Spotlight nomination/entry (pending/rejected/approved)."""
    sid = int(spotlight_id)
    runtime_deleted = 0
    kept = []
    for entry in spotlight_entries:
        if int(entry.get("id") or 0) == sid:
            runtime_deleted += 1
            continue
        kept.append(entry)
    if runtime_deleted <= 0:
        raise HTTPException(status_code=404, detail="Spotlight entry not found")
    spotlight_entries[:] = kept
    # Also clear any published archive twin so member Archive stays consistent.
    ensure_spotlight_archive_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        cur = conn.execute("DELETE FROM spotlight_archive WHERE spotlight_id = ?", (sid,))
        archive_deleted = int(cur.rowcount or 0)
        conn.commit()
    save_runtime_state(force=True)
    return {
        "spotlight_id": sid,
        "runtime_deleted": runtime_deleted,
        "archive_deleted": archive_deleted,
    }


def admin_spotlight_runtime_entries(status: str | None = None) -> list[dict]:
    status_key = (status or "").strip().lower()
    entries = []
    for entry in spotlight_entries:
        current = (entry.get("status") or "").strip().lower()
        if status_key and current != status_key:
            continue
        entries.append({
            "id": entry.get("id"),
            "status": entry.get("status"),
            "style": entry.get("style"),
            "reason": entry.get("edited_reason") or entry.get("reason"),
            "edited_reason": entry.get("edited_reason"),
            "nominee_display_name": entry.get("nominee_display_name"),
            "nominee_username": entry.get("nominee_username"),
            "nominee_user_id": entry.get("nominee_user_id"),
            "nominator_display_name": entry.get("nominator_display_name"),
            "nominator_username": entry.get("nominator_username"),
            "nominator_user_id": entry.get("nominator_user_id"),
            "day_key": entry.get("day_key"),
            "time": entry.get("time"),
            "published_at": entry.get("published_at"),
            "publish_pending": bool(entry.get("publish_pending")),
            "reviewed_at": entry.get("reviewed_at"),
        })
    entries.sort(key=lambda item: int(item.get("id") or 0), reverse=True)
    return entries


def wheel_archive_reviews_for_entry(entry: dict) -> list[dict]:
    entry_id = entry.get("id")
    embedded = entry.get("reviews")
    if isinstance(embedded, list) and embedded:
        return embedded
    reviews = []
    for review in wheel_review_history:
        try:
            if int(review.get("video_entry_id") or 0) != int(entry_id or 0):
                continue
        except Exception:
            continue
        reviews.append(review)
    return reviews


def wheel_archive_day_key(entry: dict) -> str:
    stamp = entry.get("played_at") or entry.get("archived_at") or entry.get("time") or ""
    return str(stamp)[:10]


def normalize_wheel_archive_entry(entry: dict, viewer: dict | None = None) -> dict:
    data = entry.get("data") or {}
    reviews = wheel_archive_reviews_for_entry(entry)
    ratings = [float(review.get("rating") or 0) for review in reviews if review.get("rating")]
    average_rating = entry.get("average_rating")
    if average_rating is None and ratings:
        average_rating = round(sum(ratings) / len(ratings), 2)
    sanitized_reviews = []
    for review in reviews:
        reviewer = {
            "user_id": review.get("user_id"),
            "username": review.get("username"),
        }
        sanitized_reviews.append({
            "rating": review.get("rating"),
            "review": review.get("review"),
            "display_name": review.get("display_name") or "Anonymous",
            "time": review.get("time"),
            "is_my_review": bool(viewer and pulse_identities_match(viewer, reviewer)),
        })
    return {
        "id": entry.get("id"),
        "video_title": data.get("video_title") or data.get("link") or "Untitled video",
        "submitted_url": entry.get("submitted_url") or data.get("link"),
        "submitted_by": data.get("display_name") or "Unknown",
        "source_domain": entry.get("source_domain"),
        "played_at": entry.get("played_at"),
        "archived_at": entry.get("archived_at"),
        "day_key": wheel_archive_day_key(entry),
        "average_rating": float(average_rating or 0),
        "review_count": entry.get("review_count") or len(sanitized_reviews),
        "reviews": sanitized_reviews,
        "is_my_submission": bool(
            viewer and wheel_entry_matches_submitter(
                entry,
                telegram_id=viewer.get("user_id"),
                username=viewer.get("username"),
                display_name=viewer.get("display_name"),
            )
        ),
    }


def query_archive_wheel_entries(
    *,
    q: str | None = None,
    submitted_by: str | None = None,
    min_rating: float | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str | None = None,
    mine_only: bool = False,
    viewer: dict | None = None,
    page: int = 1,
    limit: int = 50,
) -> dict:
    normalized = [normalize_wheel_archive_entry(entry, viewer) for entry in archived_wheel_entries]
    needle = (q or "").strip().lower()
    submitter_needle = (submitted_by or "").strip().lower()
    filtered = []
    for entry in normalized:
        if needle and needle not in (entry.get("video_title") or "").lower():
            continue
        if submitter_needle and submitter_needle not in (entry.get("submitted_by") or "").lower():
            continue
        if min_rating is not None and float(entry.get("average_rating") or 0) < float(min_rating):
            continue
        day_key = entry.get("day_key") or ""
        if from_day and day_key and day_key < from_day.strip():
            continue
        if to_day and day_key and day_key > to_day.strip():
            continue
        if mine_only and not entry.get("is_my_submission"):
            continue
        filtered.append(entry)
    sort_key = (sort or "date_desc").strip().lower()
    if sort_key == "date_asc":
        filtered.sort(key=lambda item: (item.get("archived_at") or item.get("played_at") or "", item.get("id") or 0))
    elif sort_key == "title_az":
        filtered.sort(key=lambda item: ((item.get("video_title") or "").lower(), item.get("archived_at") or ""))
    elif sort_key == "title_za":
        filtered.sort(key=lambda item: ((item.get("video_title") or "").lower(), item.get("archived_at") or ""), reverse=True)
    elif sort_key == "rating_desc":
        filtered.sort(key=lambda item: (float(item.get("average_rating") or 0), item.get("archived_at") or ""), reverse=True)
    elif sort_key == "rating_asc":
        filtered.sort(key=lambda item: (float(item.get("average_rating") or 0), item.get("archived_at") or ""))
    else:
        filtered.sort(key=lambda item: (item.get("archived_at") or item.get("played_at") or "", item.get("id") or 0), reverse=True)
    total = len(filtered)
    row_limit = max(1, min(limit, 200))
    offset = max(0, (max(1, page) - 1) * row_limit)
    page_entries = filtered[offset:offset + row_limit]
    return {
        "entries": page_entries,
        "total": total,
        "page": max(1, page),
        "limit": row_limit,
        "pages": max(1, (total + row_limit - 1) // row_limit),
    }


def archive_summary_payload(viewer: dict | None = None) -> dict:
    pulse_stats = pulse_archive_stats_payload()
    backfill_spotlight_archive_from_runtime()
    ensure_spotlight_archive_store()
    with sqlite3.connect(STATE_DB_PATH) as conn:
        spotlight_count = int(conn.execute("SELECT COUNT(*) FROM spotlight_archive").fetchone()[0] or 0)
    wheel_result = query_archive_wheel_entries(viewer=viewer, page=1, limit=1)
    return {
        "pulse": {
            "question_count": pulse_stats.get("question_count", 0),
            "answer_count": pulse_stats.get("answer_count", 0),
        },
        "spotlight": {"award_count": spotlight_count},
        "wheel": {"entry_count": wheel_result.get("total", 0)},
    }


def runtime_entry_day(entry: dict) -> str:
    return (
        entry.get("day_key")
        or entry.get("submitted_at")
        or entry.get("time")
        or entry.get("created_at")
        or ""
    )[:10]


def prune_pulse_runtime_data(force: bool = False) -> dict:
    global pulse_entries, pulse_receipts, pulse_red_activations, miniapp_verifications
    global pulse_question_suggestions, spotlight_entries, _last_pulse_prune_day

    today = datetime.datetime.now(UK_TZ).strftime("%Y-%m-%d")
    if not force and _last_pulse_prune_day == today:
        return {
            "pruned_entries": 0,
            "pruned_receipts": 0,
            "pruned_activations": 0,
            "pruned_verifications": 0,
            "pruned_suggestions": 0,
            "pruned_spotlights": 0,
        }

    cutoff = (datetime.datetime.now(UK_TZ) - datetime.timedelta(days=PULSE_RETENTION_DAYS)).strftime("%Y-%m-%d")
    before_entries = len(pulse_entries)
    pulse_entries[:] = [
        entry for entry in pulse_entries
        if (entry.get("day_key") or "") >= cutoff
    ]
    kept_pulse_ids = {int(entry.get("id") or 0) for entry in pulse_entries if entry.get("id")}

    before_receipts = len(pulse_receipts)
    pulse_receipts[:] = [
        receipt for receipt in pulse_receipts
        if int(receipt.get("pulse_id") or 0) in kept_pulse_ids
    ]

    before_activations = len(pulse_red_activations)
    pulse_red_activations[:] = [
        entry for entry in pulse_red_activations
        if (entry.get("day_key") or "") >= cutoff
    ]

    before_verifications = len(miniapp_verifications)
    miniapp_verifications[:] = [
        entry for entry in miniapp_verifications
        if entry.get("status") == "pending"
        or runtime_entry_day(entry) >= cutoff
    ]

    before_suggestions = len(pulse_question_suggestions)
    before_spotlights = len(spotlight_entries)
    if lean_mode_enabled():
        pulse_question_suggestions[:] = [
            entry for entry in pulse_question_suggestions
            if entry.get("status") in {"pending_review", "reserved", "approved"}
            or runtime_entry_day(entry) >= cutoff
        ]
        spotlight_entries[:] = [
            entry for entry in spotlight_entries
            if entry.get("status") == "pending_review"
            or runtime_entry_day(entry) >= cutoff
        ]

    _last_pulse_prune_day = today
    return {
        "pruned_entries": before_entries - len(pulse_entries),
        "pruned_receipts": before_receipts - len(pulse_receipts),
        "pruned_activations": before_activations - len(pulse_red_activations),
        "pruned_verifications": before_verifications - len(miniapp_verifications),
        "pruned_suggestions": before_suggestions - len(pulse_question_suggestions),
        "pruned_spotlights": before_spotlights - len(spotlight_entries),
    }


def cards_api_enabled() -> bool:
    return CARDS_API_ENABLED


def ensure_cards_api_enabled():
    if not cards_api_enabled():
        raise HTTPException(status_code=503, detail="Cards is temporarily disabled.")


def save_runtime_state(force: bool = False) -> bool:
    global _last_saved_runtime_fingerprint

    trim_runtime_state_collections()
    prune_stats = prune_pulse_runtime_data()
    payload = runtime_state_payload()
    fingerprint = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    data_changed = any(prune_stats.values())
    if not force and not data_changed and fingerprint == _last_saved_runtime_fingerprint:
        return False

    save_runtime_state_to_db(payload)

    directory = os.path.dirname(RUNTIME_STATE_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{RUNTIME_STATE_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
    os.replace(temp_path, RUNTIME_STATE_PATH)

    _last_saved_runtime_fingerprint = fingerprint
    return True


def load_runtime_state() -> None:
    payload = load_runtime_state_from_db()
    if isinstance(payload, dict):
        apply_runtime_payload(payload)
        return

    if not os.path.exists(RUNTIME_STATE_PATH):
        return
    try:
        with open(RUNTIME_STATE_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return
    if not isinstance(payload, dict):
        return

    apply_runtime_payload(payload)
    try:
        save_runtime_state_to_db(payload)
    except sqlite3.Error:
        pass


load_runtime_state()


def live_room_state_payload() -> dict:
    return {
        "room_discussion": state.get("room_discussion"),
        "active_poll": active_poll,
        "room_qa_items": room_qa_items,
        "room_qa_archive": room_qa_archive,
        "poll_history": poll_history,
        "room_media_submissions": room_media_submissions,
        "now_showing_media": now_showing_media,
        "room_game": room_game,
        "team_feeds": team_feeds,
        "next_room_qa_id_seq": _next_room_qa_id_seq,
        "next_poll_id_seq": _next_poll_id_seq,
        "next_media_id_seq": _next_media_id_seq,
        "next_team_msg_id_seq": _next_team_msg_id_seq,
    }


def apply_live_room_payload(payload: dict) -> None:
    global active_poll, room_qa_items, room_qa_archive, poll_history, room_media_submissions, now_showing_media
    global room_game, team_feeds
    global _next_room_qa_id_seq, _next_poll_id_seq, _next_media_id_seq, _next_team_msg_id_seq

    if not isinstance(payload, dict):
        return
    discussion = payload.get("room_discussion")
    if isinstance(discussion, dict):
        state["room_discussion"] = discussion
    if "active_poll" in payload:
        active_poll = payload.get("active_poll")
    if isinstance(payload.get("room_qa_items"), list):
        room_qa_items = payload["room_qa_items"]
    if isinstance(payload.get("room_qa_archive"), list):
        room_qa_archive = payload["room_qa_archive"]
    if isinstance(payload.get("poll_history"), list):
        poll_history = payload["poll_history"]
    if isinstance(payload.get("room_media_submissions"), list):
        room_media_submissions = payload["room_media_submissions"]
    if "now_showing_media" in payload:
        now_showing_media = payload.get("now_showing_media")
    if "room_game" in payload:
        room_game = payload.get("room_game")
    if isinstance(payload.get("team_feeds"), dict):
        team_feeds = payload["team_feeds"]
    if isinstance(payload.get("next_room_qa_id_seq"), int) and payload["next_room_qa_id_seq"] > 0:
        _next_room_qa_id_seq = payload["next_room_qa_id_seq"]
    if isinstance(payload.get("next_poll_id_seq"), int) and payload["next_poll_id_seq"] > 0:
        _next_poll_id_seq = payload["next_poll_id_seq"]
    if isinstance(payload.get("next_media_id_seq"), int) and payload["next_media_id_seq"] > 0:
        _next_media_id_seq = payload["next_media_id_seq"]
    if isinstance(payload.get("next_team_msg_id_seq"), int) and payload["next_team_msg_id_seq"] > 0:
        _next_team_msg_id_seq = payload["next_team_msg_id_seq"]


def save_live_room_state() -> None:
    payload = live_room_state_payload()
    directory = os.path.dirname(LIVE_ROOM_STATE_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    temp_path = f"{LIVE_ROOM_STATE_PATH}.tmp"
    with open(temp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
    os.replace(temp_path, LIVE_ROOM_STATE_PATH)


def load_live_room_state() -> None:
    if not os.path.exists(LIVE_ROOM_STATE_PATH):
        return
    try:
        with open(LIVE_ROOM_STATE_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return
    apply_live_room_payload(payload)


def persist_live_room() -> None:
    save_live_room_state()
    ws_broadcast_bundle()


load_live_room_state()
prune_pulse_runtime_data(force=True)
save_runtime_state(force=True)
if lean_mode_enabled():
    print(
        f"[{now_iso()}] LEAN_MODE enabled: retention={PULSE_RETENTION_DAYS}d, "
        f"unlimited_question_submit={PULSE_UNLIMITED_QUESTION_SUBMIT}, "
        f"pulse_admin_notify={PULSE_ADMIN_NOTIFY_ENABLED}, "
        "wheel history omitted from runtime state.",
        flush=True,
    )

FEATURE_FLAG_REGISTRY = {
    "pages": {
        "title": "Home launcher buttons",
        "flags": {
            "video_chat": {
                "label": "Video Chat",
                "description": "Show or hide the Video Chat button on home.",
                "path": "video-chat.html",
                "default": True,
            },
            "live_room": {
                "label": "Live Room",
                "description": "Show or hide the Live Room button on home.",
                "path": "live-room.html",
                "default": True,
            },
            "archive": {
                "label": "Archive",
                "description": "Show or hide the Archive button on home.",
                "path": "archive.html",
                "default": True,
            },
            "info": {
                "label": "Info",
                "description": "Show or hide the Info button on home.",
                "path": "info.html",
                "default": True,
            },
            "wellbeing": {
                "label": "Connect",
                "description": "Show or hide the Connect hub on home.",
                "path": "wellbeing-concept-open-stage.html",
                "default": True,
            },
            "profile": {
                "label": "Profile",
                "description": "Show or hide the Profile loadout page on home.",
                "path": "profile.html",
                "default": True,
            },
            "cards": {
                "label": "Alcove Cards",
                "description": "Show or hide the Alcove Cards game on home.",
                "path": "alcove-cards-demo.html",
                "default": False,
            },
            "live_room_test": {
                "label": "Live Room Test",
                "description": "Show or hide the isolated Live Room camera test page on home.",
                "path": "live-room-test.html",
                "default": False,
            },
        },
    },
    "wellbeing": {
        "title": "Connect sections",
        "flags": {
            "daily_checkin": {
                "label": "Daily Check-in",
                "description": "Show or hide Daily Check-in inside Connect.",
                "path": "wellbeing-concept-open-stage.html#checkin",
                "default": True,
            },
            "spotlight": {
                "label": "Spotlight",
                "description": "Show or hide Spotlight inside Connect.",
                "path": "wellbeing-concept-open-stage.html#spotlight",
                "default": True,
            },
            "pulse": {
                "label": "Pulse",
                "description": "Show or hide Pulse inside Connect.",
                "path": "wellbeing-concept-open-stage.html#pulse",
                "default": True,
            },
        },
    },
}

DEFAULT_FEATURE_FLAGS = {
    group: {key: spec["default"] for key, spec in section["flags"].items()}
    for group, section in FEATURE_FLAG_REGISTRY.items()
}


def feature_flag_schema_payload() -> dict:
    return {
        group: {
            "title": section["title"],
            "items": {
                key: {
                    "label": spec["label"],
                    "description": spec.get("description", ""),
                    "path": spec.get("path", ""),
                    "default": bool(spec["default"]),
                }
                for key, spec in section["flags"].items()
            },
        }
        for group, section in FEATURE_FLAG_REGISTRY.items()
    }

PULSE_QUESTIONS = {
    "green": [],
    "red": [],
}

PULSE_RED_DAILY_QUESTION_DEFAULT = (
    "What is the one thing you'd confess to The Alcove if you knew no one could trace it back to you?"
)

if not PULSE_QUESTIONS["red"]:
    PULSE_QUESTIONS["red"] = [PULSE_RED_DAILY_QUESTION_DEFAULT]

PULSE_QUESTION_CATEGORIES = {
    "WhatΓÇÖs been on your mind more than usual lately?": "Mental health",
    "What kind of day have you really been having?": "Mental health",
    "WhatΓÇÖs something youΓÇÖve been overthinking recently?": "Mental health",
    "When youΓÇÖre not feeling great, what usually helps a bit?": "Mental health",
    "WhatΓÇÖs something you wish people asked you more often?": "Mental health",
    "WhatΓÇÖs been draining your energy lately?": "Mental health",
    "What helps you feel a bit more like yourself again?": "Mental health",
    "WhatΓÇÖs one thing you need more of right now?": "Mental health",
    "How has your body been feeling lately?": "Physical health",
    "WhatΓÇÖs your sleep been like recently?": "Physical health",
    "WhatΓÇÖs one small thing that usually makes your body feel better?": "Physical health",
    "Have you been looking after yourself properly lately?": "Physical health",
    "WhatΓÇÖs been affecting your energy the most?": "Physical health",
    "WhatΓÇÖs one healthy habit youΓÇÖre trying to get back into?": "Physical health",
    "When do you feel most relaxed in your body?": "Physical health",
    "WhatΓÇÖs something physical you know you should probably give more attention to?": "Physical health",
    "WhatΓÇÖs something youΓÇÖve been wanting to say out loud?": "General",
    "What kind of connection are you in the mood for lately?": "General",
    "WhatΓÇÖs been making life feel a bit easier recently?": "General",
    "WhatΓÇÖs something small thatΓÇÖs meant a lot to you lately?": "General",
    "WhatΓÇÖs one thing people often get wrong about you?": "General",
    "What have you been craving more of lately?": "General",
    "WhatΓÇÖs something youΓÇÖd love a bit more honesty about?": "General",
    "WhatΓÇÖs been giving you hope lately?": "General",
    "WhatΓÇÖs your hottest forbidden fantasy youΓÇÖve never told anyone?": "General",
    "WhatΓÇÖs the sluttiest thing youΓÇÖve ever done in public or semi-public?": "General",
    "What exact thing during foreplay instantly makes your cock leak and your hole twitch?": "General",
    "Is there a time you hooked up with someone you really shouldnΓÇÖt have ΓÇö who it was and how filthy it got?": "General",
}

# ---------------------------------
# Models
# ---------------------------------

class WheelEntry(BaseModel):
    telegram_id: int | None = None
    username: str | None = None
    display_name: str
    link: str
    note: str | None = None
    video_title: str | None = None
    video_longer_than_5_minutes: bool | None = None
    clip_start_seconds: int | None = None
    clip_start_label: str | None = None


class VideoReview(BaseModel):
    rating: int
    review: str
    display_name: str
    anonymous: bool
    user_id: int | None = None
    username: str | None = None


class StreamComment(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str
    text: str
    feed_style: dict | None = None


class WheelReaction(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str
    reaction_key: str


class RoomDiscussionStart(BaseModel):
    title: str
    duration_minutes: int


class RoomQASubmit(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str
    question: str


class RoomPollCreate(BaseModel):
    question: str
    options: list[str]


class RoomPollVote(BaseModel):
    user_id: int | None = None
    option_id: str


class RoomGameStart(BaseModel):
    title: str = "Teams"
    mode: str = "display"
    max_size: int = 4


class RoomGameJoin(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str


class RoomGameConfig(BaseModel):
    title: str | None = None
    mode: str | None = None
    max_size: int | None = None


class RoomGameOverlay(BaseModel):
    visible: bool = True


class RoomTeamMessage(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str
    team_id: str
    text: str


class ModuleStateUpdate(BaseModel):
    wheel: bool
    asmr: bool
    story: bool
    shoutouts: bool


class DownloadCompletePayload(BaseModel):
    local_filename: str
    local_path: str
    direct_media_url: str | None = None
    video_title: str | None = None
    download_method: str = "auto"


class DownloadProcessedPayload(BaseModel):
    stream_candidate: dict | None = None
    download_candidate: dict | None = None
    video_title: str | None = None
    process_method: str | None = None


class DownloadFailedPayload(BaseModel):
    error: str


class ManualReadyPayload(BaseModel):
    local_filename: str
    local_path: str
    video_title: str | None = None


class ManualReadyByFilenamePayload(BaseModel):
    filename: str
    video_title: str | None = None


class PayoutPayload(BaseModel):
    copy_from_path: str | None = None


class SpotlightEntry(BaseModel):
    nominee_user_id: int | None = None
    nominee_username: str | None = None
    nominee_display_name: str
    reason: str
    style: str
    nominator_user_id: int | None = None
    nominator_username: str | None = None
    nominator_display_name: str | None = None


class PulseEntry(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str | None = None
    pulse_type: str = "green"
    question: str
    answer: str | None = None


class PulseAssignmentResponse(BaseModel):
    user_id: int | None = None
    username: str | None = None
    answer: str


class PulseReceiptAck(BaseModel):
    user_id: int | None = None
    username: str | None = None


class PulseQuestionSuggestion(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str | None = None
    pool: str = "green"
    category: str
    question: str
    schedule_mode: str | None = None


class MiniappVerificationPayload(BaseModel):
    init_data: str | None = None
    selected_pack: str | None = None
    feedback: str | None = None


class VerificationLogPayload(BaseModel):
    session_id: str | None = None
    event: str | None = None
    level: str | None = "info"
    user_id: int | None = None
    username: str | None = None
    display_name: str | None = None
    step_index: int | None = None
    step_title: str | None = None
    step_pose: str | None = None
    action: str | None = None
    selected_pack: str | None = None
    message: str | None = None
    detail: dict | None = None
    user_agent: str | None = None
    url: str | None = None
    client_time: str | None = None


class PulseSettingsUpdate(BaseModel):
    heat_threshold: int
    reset_interval_hours: int | None = None
    admin_secret: str | None = None


class BotSyncPayload(BaseModel):
    users: list[dict] = []
    analytics: dict = {}
    synced_at: str | None = None


class SpotlightReviewUpdate(BaseModel):
    status: str | None = None
    edited_reason: str | None = None
    review_message_sent: bool | None = None
    reviewed_by: int | None = None
    reviewed_at: str | None = None
    publish_pending: bool | None = None
    published_at: str | None = None


class FeatureFlagsUpdate(BaseModel):
    pages: dict[str, bool] | None = None
    wellbeing: dict[str, bool] | None = None
    tester_usernames: list[str] | None = None
    admin_secret: str | None = None


class RewardCatalogUpdate(BaseModel):
    admin_secret: str
    level_packs: dict[str, dict] | None = None
    achievements: list[dict] | None = None
    verification_packs: dict[str, dict] | None = None
    verification_packs_active: bool | None = None


class VerificationPackEligibilityPayload(BaseModel):
    init_data: str | None = None
    user_id: int | None = None
    username: str | None = None


class AdminSecretQuery(BaseModel):
    admin_secret: str


class AdminPulseQuestionAction(BaseModel):
    admin_secret: str
    action: str
    edited_question: str | None = None
    rejection_reason: str | None = None


class AdminPulseQuestionCreate(BaseModel):
    admin_secret: str
    question: str
    pool: str = "green"
    category: str = "General"
    schedule_mode: str = "tomorrow"


class AdminSpotlightAction(BaseModel):
    admin_secret: str
    action: str
    edited_reason: str | None = None


class AdminSafetySettingsPayload(BaseModel):
    admin_secret: str
    flood_message_threshold: int | None = None
    flood_window_seconds: int | None = None
    daily_digest_enabled: bool | None = None
    daily_digest_hour_utc: int | None = None
    keywords: list[dict] | None = None


class AdminFoxBuiltinSettingsPayload(BaseModel):
    admin_secret: str
    self_care: dict | None = None
    templates: dict | None = None


class FoxAuditBatchPayload(BaseModel):
    entries: list[dict]


class AdminFoxScheduledPostPayload(BaseModel):
    admin_secret: str
    title: str | None = None
    enabled: bool | None = None
    schedule_type: str | None = None
    hour_utc: int | None = None
    minute_utc: int | None = None
    interval_hours: int | None = None
    weekday_utc: int | None = None
    run_at: str | None = None
    target: str | None = None
    topic_id: int | None = None
    text: str | None = None
    banner: str | None = None
    link_url: str | None = None
    link_label: str | None = None
    replace_singleton: str | None = None


class FoxMessageDeliveryPayload(BaseModel):
    post_id: str
    message_id: int | None = None
    chat_id: int | None = None
    error: str | None = None


# ---------------------------------
# Helpers
# ---------------------------------

def iso_in_seconds(seconds: int) -> str:
    return (datetime.datetime.utcnow() + datetime.timedelta(seconds=seconds)).isoformat()


def pulse_notification_due_at() -> str:
    return iso_in_seconds(random.randint(120, 180))


def iso_has_passed(value: str | None) -> bool:
    if not value:
        return True
    try:
        return datetime.datetime.fromisoformat(value) <= datetime.datetime.utcnow()
    except ValueError:
        return True


def uk_now() -> datetime.datetime:
    return datetime.datetime.now(UK_TZ)


def pulse_day_key(at: datetime.datetime | None = None) -> str:
    return (at or datetime.datetime.now(UK_TZ)).strftime("%Y-%m-%d")


def normalize_pulse_schedule_mode(raw: str | None) -> str:
    mode = (raw or "tomorrow").strip().lower()
    if mode in {"today", "now", "live"}:
        return "today"
    if mode in {"reserve", "reserved", "hold"}:
        return "reserve"
    return "tomorrow"


def active_from_for_schedule_mode(mode: str | None) -> str | None:
    schedule = normalize_pulse_schedule_mode(mode)
    if schedule == "today":
        return pulse_day_key()
    if schedule == "tomorrow":
        return pulse_next_day_key()
    return None


def apply_pulse_suggestion_schedule(entry: dict, mode: str | None = None, *, approve: bool = False) -> dict:
    schedule = normalize_pulse_schedule_mode(mode or entry.get("schedule_mode"))
    entry["schedule_mode"] = schedule
    if schedule == "reserve":
        entry["status"] = "reserved"
        entry["active_from_day_key"] = None
        return entry
    if approve or schedule == "today":
        entry["status"] = "approved"
        entry["active_from_day_key"] = active_from_for_schedule_mode(schedule)
    return entry


def pulse_next_day_key(at: datetime.datetime | None = None) -> str:
    current = at or uk_now()
    return (current + datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def pulse_day_before(day_key: str) -> str:
    try:
        parsed = datetime.date.fromisoformat(day_key)
    except ValueError:
        return day_key
    return (parsed - datetime.timedelta(days=1)).strftime("%Y-%m-%d")


def demote_other_red_from_day(day_key: str, except_id: int | None = None) -> None:
    for entry in pulse_question_suggestions:
        if entry.get("status") != "approved":
            continue
        if (entry.get("pool") or "green") != "red":
            continue
        if except_id is not None and int(entry.get("id") or 0) == int(except_id):
            continue
        if (entry.get("active_from_day_key") or "").strip() == day_key:
            entry["active_from_day_key"] = pulse_day_before(day_key)


def ensure_single_red_slot(entry: dict) -> None:
    pool = (entry.get("pool") or "green").strip().lower()
    if pool != "red" or entry.get("status") != "approved":
        return
    active_from = (entry.get("active_from_day_key") or "").strip()
    if not active_from:
        return
    demote_other_red_from_day(active_from, except_id=entry.get("id"))


def pulse_day_label(day_key: str | None = None) -> str:
    raw = day_key or pulse_day_key()
    try:
        parsed = datetime.date.fromisoformat(raw)
    except ValueError:
        return raw
    return parsed.strftime("%d %B %Y")


def pulse_day_ordinal(day: int) -> str:
    if 11 <= day <= 13:
        return f"{day}th"
    suffix = {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")
    return f"{day}{suffix}"


def pulse_day_name_with_ordinal(day_key: str | None) -> str:
    if not day_key:
        return "the next Pulse day"
    try:
        parsed = datetime.date.fromisoformat(day_key)
    except ValueError:
        return day_key
    return f"{parsed.strftime('%A')} {pulse_day_ordinal(parsed.day)} {parsed.strftime('%B')}"


def pulse_question_availability_copy(entry: dict) -> str:
    schedule = normalize_pulse_schedule_mode(entry.get("schedule_mode"))
    active_from = (entry.get("active_from_day_key") or "").strip()
    day_label = pulse_day_name_with_ordinal(active_from)
    if schedule == "today" or active_from == pulse_day_key():
        return (
            f"It is live in Pulse now and stays available until midnight UK time tonight "
            f"({pulse_day_name_with_ordinal(pulse_day_key())})."
        )
    if schedule == "reserve":
        return "It has been saved to the reserve pot. F.O.X will schedule it for a future Pulse day."
    return (
        f"It will be added to the next day's Pulse questions and will be available from "
        f"00:00 on {day_label} (UK time) for 24 hours."
    )


def pulse_suggestion_submitter_user_id(entry: dict) -> int | None:
    try:
        user_id = int(entry.get("user_id") or 0)
    except (TypeError, ValueError):
        return None
    return user_id or None


def cancel_pending_pulse_question_review_notifications(
    suggestion_id: int,
    *,
    kinds: set[str] | None = None,
    reason: str = "superseded",
) -> None:
    """Drop unsent review DMs for a suggestion so approve/reject cannot both go out."""
    suggestion_id = int(suggestion_id or 0)
    if not suggestion_id:
        return
    stamped = now_iso()
    for existing in pulse_question_review_notifications:
        if existing.get("notified_at"):
            continue
        if int(existing.get("suggestion_id") or 0) != suggestion_id:
            continue
        if kinds and existing.get("kind") not in kinds:
            continue
        existing["notified_at"] = stamped
        existing["cancelled"] = True
        existing["cancel_reason"] = reason


PULSE_REVIEW_DM_WEBAPP_URL = (
    "https://ardyn-alcove.com/wellbeing-concept-open-stage.html"
    "?open=pulse&v=miniapp-wellbeing-20260807a"
)
PULSE_REVIEW_DM_SUBMIT_WEBAPP_URL = (
    "https://ardyn-alcove.com/wellbeing-concept-open-stage.html"
    "?open=pulse&submit=new&v=miniapp-wellbeing-20260807a"
)


def telegram_bot_send_message(payload: dict) -> tuple[bool, str]:
    if not TELEGRAM_BOT_TOKEN:
        return False, "bot token missing"
    try:
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))
        if result.get("ok"):
            return True, ""
        return False, str(result.get("description") or "sendMessage failed")
    except urllib.error.HTTPError as exc:
        try:
            detail = json.loads(exc.read().decode("utf-8")).get("description") or repr(exc)
        except Exception:
            detail = repr(exc)
        return False, str(detail)
    except Exception as exc:
        return False, repr(exc)


def pulse_question_review_dm_content(notification: dict) -> tuple[str, str, str] | None:
    kind = notification.get("kind")
    question = (notification.get("question") or "").strip()
    availability = (notification.get("availability_copy") or "").strip()
    if kind == "question_approved":
        text = (
            "Good news — your Pulse question has been approved.\n\n"
            f"<b>{escape(question)}</b>\n\n"
            f"{escape(availability)}\n\n"
            "Open Pulse in the Mini App to collect your approval EXP "
            "and see answers in My Activity."
        )
        return text, "Open Pulse", PULSE_REVIEW_DM_WEBAPP_URL
    if kind == "question_rejected":
        reason = (notification.get("rejection_reason") or "").strip() or "It did not meet the Pulse guidelines."
        if notification.get("resubmit_allowed"):
            text = (
                "Your Pulse question was not approved this time.\n\n"
                f"<b>{escape(question)}</b>\n\n"
                f"<b>Reason from F.O.X</b>\n{escape(reason)}\n\n"
                "You get <b>one</b> replacement attempt today — choose your words carefully.\n\n"
                "Tap the button below to open Pulse in the Mini App and submit your new question there."
            )
            return text, "Submit in Pulse", PULSE_REVIEW_DM_SUBMIT_WEBAPP_URL
        text = (
            "Your Pulse question was not approved this time.\n\n"
            f"<b>{escape(question)}</b>\n\n"
            f"<b>Reason from F.O.X</b>\n{escape(reason)}\n\n"
            "You've already used today's replacement attempt. "
            "You can submit fresh Pulse questions again after midnight UK time.\n\n"
            "Tap Open Pulse to check your status in the Mini App."
        )
        return text, "Open Pulse", PULSE_REVIEW_DM_WEBAPP_URL
    return None


def try_send_pulse_question_review_dm_now(notification: dict) -> bool:
    """Send approve/reject DMs immediately from the API so members are not waiting on FOX poll."""
    if not notification or notification.get("notified_at"):
        return False
    if not pulse_question_review_notification_still_valid(notification):
        return False
    user_id = notification.get("recipient_user_id")
    content = pulse_question_review_dm_content(notification)
    if not user_id or not content:
        return False
    # Claim before calling Telegram so the FOX worker cannot also deliver this row.
    notification["notified_at"] = now_iso()
    notification["delivery"] = "api_claim"
    save_runtime_state()
    text, button_text, button_url = content
    base_payload = {
        "chat_id": int(user_id),
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    attempts = [
        {
            **base_payload,
            "reply_markup": {
                "inline_keyboard": [[{"text": button_text, "web_app": {"url": button_url}}]]
            },
        },
        {
            **base_payload,
            "reply_markup": {
                "inline_keyboard": [[{"text": button_text, "url": button_url}]]
            },
        },
    ]
    last_detail = ""
    for payload in attempts:
        ok, detail = telegram_bot_send_message(payload)
        if ok:
            notification["delivery"] = "api_immediate"
            save_runtime_state()
            return True
        last_detail = detail
    lowered = last_detail.lower()
    if any(
        phrase in lowered
        for phrase in (
            "can't initiate conversation",
            "bot was blocked",
            "user is deactivated",
            "chat not found",
        )
    ):
        notification["cancelled"] = True
        notification["cancel_reason"] = "user_unreachable"
        notification["delivery"] = "api_unreachable"
        save_runtime_state()
        print(
            f"[{now_iso()}] pulse review DM unreachable for {user_id}: {last_detail}",
            flush=True,
        )
        return False
    # Transient failure — release the claim so a later retry can send once.
    notification["notified_at"] = None
    notification["delivery"] = None
    save_runtime_state()
    print(
        f"[{now_iso()}] pulse review DM send deferred for {user_id}: {last_detail}",
        flush=True,
    )
    return False


def queue_pulse_question_review_notification(entry: dict, kind: str, *, rejection_reason: str | None = None) -> None:
    user_id = pulse_suggestion_submitter_user_id(entry)
    if not user_id:
        return
    suggestion_id = int(entry.get("id") or 0)
    if not suggestion_id:
        return
    opposite = {
        "question_approved": {"question_rejected"},
        "question_rejected": {"question_approved"},
    }.get(kind)
    if opposite:
        cancel_pending_pulse_question_review_notifications(
            suggestion_id,
            kinds=opposite,
            reason=f"superseded_by_{kind}",
        )
    notification = None
    for existing in pulse_question_review_notifications:
        if (
            existing.get("kind") == kind
            and int(existing.get("suggestion_id") or 0) == suggestion_id
            and not existing.get("notified_at")
        ):
            # Refresh payload on the still-pending duplicate instead of stacking another DM.
            existing["question"] = pulse_suggestion_question(entry)
            existing["schedule_mode"] = entry.get("schedule_mode") or "tomorrow"
            existing["active_from_day_key"] = entry.get("active_from_day_key")
            existing["availability_copy"] = pulse_question_availability_copy(entry)
            existing["rejection_reason"] = (
                (rejection_reason or entry.get("rejection_reason") or "").strip() or None
            )
            existing["resubmit_allowed"] = bool(entry.get("resubmit_allowed"))
            existing["recipient_user_id"] = user_id
            existing["recipient_username"] = entry.get("username")
            existing["recipient_display_name"] = entry.get("display_name")
            notification = existing
            break
    if notification is None:
        notification = {
            "notification_id": f"pulse-review-{kind}-{suggestion_id}-{len(pulse_question_review_notifications) + 1}",
            "kind": kind,
            "suggestion_id": suggestion_id,
            "recipient_user_id": user_id,
            "recipient_username": entry.get("username"),
            "recipient_display_name": entry.get("display_name"),
            "question": pulse_suggestion_question(entry),
            "schedule_mode": entry.get("schedule_mode") or "tomorrow",
            "active_from_day_key": entry.get("active_from_day_key"),
            "availability_copy": pulse_question_availability_copy(entry),
            "rejection_reason": (rejection_reason or entry.get("rejection_reason") or "").strip() or None,
            "resubmit_allowed": bool(entry.get("resubmit_allowed")),
            "created_at": now_iso(),
            "notified_at": None,
        }
        pulse_question_review_notifications.append(notification)
    # Prefer immediate API delivery; FOX poll remains as fallback if this fails.
    try_send_pulse_question_review_dm_now(notification)


def pulse_question_review_notification_still_valid(item: dict) -> bool:
    """Skip DMs that no longer match the suggestion's current review status."""
    suggestion_id = int(item.get("suggestion_id") or 0)
    if not suggestion_id:
        return False
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return False
    status = (entry.get("status") or "").strip().lower()
    kind = item.get("kind")
    if kind == "question_approved":
        return status == "approved"
    if kind == "question_rejected":
        if status != "rejected":
            return False
        # Keep button/resubmit flag in sync with the live row.
        item["resubmit_allowed"] = bool(entry.get("resubmit_allowed"))
        item["rejection_reason"] = (entry.get("rejection_reason") or item.get("rejection_reason") or "").strip() or None
        item["question"] = pulse_suggestion_question(entry)
        return True
    return False


def normalized_pulse_reset_interval(value) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 12
    return parsed if parsed in {1, 3, 4, 6, 12} else 12


def pulse_question_category(question: str | None, pulse_type: str | None = None) -> str:
    question = (question or "").strip()
    if question in PULSE_QUESTION_CATEGORIES:
        return PULSE_QUESTION_CATEGORIES[question]
    if (pulse_type or "").strip().lower() == "red":
        return "General"
    return "Mental health"


def pulse_default_question_entries():
    rows = []
    for pool, questions in PULSE_QUESTIONS.items():
        for question in questions:
            rows.append({
                "source": "default",
                "pool": pool,
                "category": pulse_question_category(question, pool),
                "question": question,
                "active": {"pool": pool, "question": question} not in pulse_disabled_questions,
            })
    return rows


def pulse_approved_question_entries(day_key: str | None = None):
    rows = []
    today = day_key or pulse_day_key()
    for entry in pulse_question_suggestions:
        if entry.get("status") != "approved":
            continue
        active_from = (entry.get("active_from_day_key") or "").strip()
        if active_from != today:
            continue
        rows.append({
            "source": "suggested",
            "suggestion_id": entry.get("id"),
            "pool": entry.get("pool") or "green",
            "category": entry.get("category") or "General",
            "question": entry.get("edited_question") or entry.get("question") or "",
            "active": True,
            "active_from_day_key": active_from,
        })
    return rows


def pulse_active_questions(pool: str):
    active = []
    for entry in pulse_default_question_entries() + pulse_approved_question_entries():
        if entry.get("pool") != pool:
            continue
        if not entry.get("active", True):
            continue
        question = (entry.get("question") or "").strip()
        if question:
            active.append(question)
    return active


def pulse_question_answer_count(question: str, pool: str | None = None):
    count = 0
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if (entry.get("question") or "").strip() != (question or "").strip():
            continue
        if pool and (entry.get("pulse_type") or "green") != pool:
            continue
        count += 1
    return count


def pulse_question_answer_count_for_day(question: str, pool: str | None = None, day_key: str | None = None):
    day = day_key or pulse_day_key()
    count = 0
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if entry.get("day_key") != day:
            continue
        if (entry.get("question") or "").strip() != (question or "").strip():
            continue
        if pool and (entry.get("pulse_type") or "green") != pool:
            continue
        count += 1
    return count


def pulse_daily_spread_report(day_key: str | None = None):
    day = day_key or pulse_day_key()
    questions = []
    for row in pulse_question_roster():
        today_count = pulse_question_answer_count_for_day(row.get("question"), row.get("pool"), day)
        questions.append({
            **row,
            "answers_today": today_count,
            "answers_all_time": int(row.get("answers_count") or 0),
        })
    questions.sort(key=lambda item: (item.get("answers_today") or 0, (item.get("question") or "").lower()))
    total_answers = sum(int(item.get("answers_today") or 0) for item in questions)
    lowest = min((int(item.get("answers_today") or 0) for item in questions), default=0) if questions else 0
    underserved = [item for item in questions if int(item.get("answers_today") or 0) == lowest]
    return {
        "day_key": day,
        "day_label": pulse_day_label(day),
        "question_count": len(questions),
        "total_answers_today": total_answers,
        "lowest_count": lowest,
        "underserved": underserved,
        "questions": questions,
    }


def pulse_question_roster():
    rows = []
    current_id = 1
    all_entries = pulse_default_question_entries() + pulse_approved_question_entries()
    sort_key = {"Mental health": 0, "Physical health": 1, "General": 2}
    all_entries.sort(key=lambda item: (item.get("pool") != "green", sort_key.get(item.get("category"), 99), item.get("question", "").lower()))
    for entry in all_entries:
        if not entry.get("active", True):
            continue
        owner = pulse_question_suggestion_owner_fields(entry.get("suggestion_id"))
        rows.append({
            "roster_id": current_id,
            "source": entry.get("source"),
            "suggestion_id": entry.get("suggestion_id"),
            "pool": entry.get("pool"),
            "category": entry.get("category"),
            "question": entry.get("question"),
            "answers_count": pulse_question_answer_count(entry.get("question"), entry.get("pool")),
            "answers_today": pulse_question_answer_count_for_day(entry.get("question"), entry.get("pool")),
            **owner,
        })
        current_id += 1
    return rows


def clear_pulse_question_roster(reviewed_by=None):
    removed_defaults = 0
    removed_suggested = 0
    reviewed_at = now_iso()
    for row in pulse_question_roster():
        if row.get("source") == "default":
            marker = {"pool": row.get("pool"), "question": row.get("question")}
            if marker not in pulse_disabled_questions:
                pulse_disabled_questions.append(marker)
                removed_defaults += 1
            continue
        suggestion_id = row.get("suggestion_id")
        if not suggestion_id:
            continue
        suggestion = find_pulse_question_suggestion(suggestion_id)
        if not suggestion or suggestion.get("status") != "approved":
            continue
        suggestion["status"] = "deleted"
        suggestion["reviewed_at"] = reviewed_at
        if reviewed_by is not None:
            suggestion["reviewed_by"] = reviewed_by
        removed_suggested += 1
    save_runtime_state()
    return {
        "removed_defaults": removed_defaults,
        "removed_suggested": removed_suggested,
        "remaining": len(pulse_question_roster()),
    }


def find_resubmittable_rejected_pulse_question(user_id: int | None = None, username: str | None = None):
    """Only today's rejected rows can be amended — never recycle old rejects."""
    today = pulse_day_key()
    username = (username or "").lower()
    matches = []
    stale_cleared = False
    for entry in pulse_question_suggestions:
        if entry.get("status") != "rejected":
            continue
        if not entry.get("resubmit_allowed"):
            continue
        # Stale open amend slots from earlier days must not keep accepting DM replies.
        if (entry.get("day_key") or "") != today:
            entry["resubmit_allowed"] = False
            stale_cleared = True
            continue
        same_user = user_id and int(entry.get("user_id") or 0) == int(user_id)
        same_username = username and (entry.get("username") or "").lower() == username
        if same_user or same_username:
            matches.append(entry)
    if stale_cleared:
        save_runtime_state()
    if not matches:
        return None
    matches.sort(key=lambda item: item.get("reviewed_at") or item.get("submitted_at") or "", reverse=True)
    return matches[0]


def resubmit_rejected_pulse_question(user_id: int | None, username: str | None, question: str) -> dict:
    if pulse_rejection_replacement_used_today(user_id, username):
        return {
            "status": "error",
            "message": "You've already used today's one replacement attempt. Try again after midnight UK time.",
        }
    entry = find_resubmittable_rejected_pulse_question(user_id, username)
    if not entry:
        return {"status": "error", "message": "No rejected Pulse question is waiting for a resubmission from you."}
    cleaned = (question or "").strip()
    if len(cleaned) < 8:
        return {"status": "error", "message": "Please add a little more detail before sending your new Pulse question."}
    entry["question"] = cleaned
    entry["edited_question"] = None
    entry["status"] = "pending_review"
    entry["rejection_reason"] = None
    entry["resubmit_allowed"] = False
    entry["resubmitted_at"] = now_iso()
    entry["reviewed_at"] = None
    entry["reviewed_by"] = None
    entry["needs_admin_notify"] = True
    entry["review_message_sent"] = False
    pulse_consume_rejection_replacement(user_id, username, keep_entry_id=entry.get("id"))
    submitter = entry.get("display_name") or entry.get("username") or entry.get("user_id")
    pulse_admin_notify(
        "\n".join([
            "<b>Pulse question resubmitted after rejection</b>",
            f"ID: <code>{entry['id']}</code>",
            f"Pool: <b>{escape((entry.get('pool') or 'green').title())}</b>",
            f"From: <b>{escape(str(submitter))}</b>",
            "",
            f"<code>{escape(cleaned)}</code>",
            "",
            "Review in Feature Admin.",
        ]),
        PULSE_QUESTIONS_TOPIC_ID,
    )
    save_runtime_state()
    return {
        "status": "ok",
        "message": (
            "Thanks — your new Pulse question has been sent to F.O.X for review. "
            "That uses today's one replacement attempt."
        ),
        "entry": entry,
    }


def find_pulse_question_suggestion(suggestion_id: int):
    for entry in pulse_question_suggestions:
        if int(entry.get("id") or 0) == int(suggestion_id):
            return entry
    return None


def next_pulse_entry_id() -> int:
    return max((int(entry.get("id") or 0) for entry in pulse_entries), default=0) + 1


def find_pulse_entry(pulse_id: int, *, status: str | None = None):
    matches = [
        item for item in pulse_entries
        if int(item.get("id") or 0) == int(pulse_id)
    ]
    if not matches:
        return None
    if status:
        filtered = [item for item in matches if item.get("status") == status]
        if filtered:
            return filtered[-1]
    return matches[-1]


def prioritized_random_question(entries: list[dict], seed_value: str = "") -> dict | None:
    if not entries:
        return None
    by_count = sorted(entries, key=lambda item: int(item.get("answers_count") or 0))
    lowest_count = int(by_count[0].get("answers_count") or 0)
    lowest_group = [item for item in by_count if int(item.get("answers_count") or 0) == lowest_count]
    chooser = random.Random(seed_value or pulse_day_key())
    return chooser.choice(lowest_group)


def pulse_question_choices(pool: str, user_id=None, username=None):
    return [entry.get("question") for entry in pulse_question_option_entries(pool, user_id, username)]


def pulse_question_option_entries(pool: str, user_id=None, username=None):
    viewer = {"user_id": user_id, "username": username}
    roster = [row for row in pulse_question_roster() if row.get("pool") == pool]
    entries = []
    seen = set()
    for row in roster:
        question = (row.get("question") or "").strip()
        if not question or question in seen:
            continue
        seen.add(question)
        owner = {
            "user_id": row.get("user_id"),
            "username": row.get("username"),
        }
        owner_from_suggestion = pulse_question_owner(question, pool)
        if owner_from_suggestion:
            owner = owner_from_suggestion
        is_own = bool(
            (owner.get("user_id") or owner.get("username"))
            and pulse_identities_match(viewer, owner)
        )
        answered_today = pulse_user_answered_question_today(viewer, question, pool)
        if answered_today and not is_own:
            continue
        entries.append({
            "question": question,
            "is_own": is_own,
            "answered_today": answered_today,
            "suggestion_id": row.get("suggestion_id"),
            "category": row.get("category"),
        })
    return entries


def pulse_red_daily_question() -> str:
    today = pulse_day_key()
    for entry in pulse_question_suggestions:
        if entry.get("status") != "approved":
            continue
        if (entry.get("pool") or "green") != "red":
            continue
        if (entry.get("active_from_day_key") or "").strip() != today:
            continue
        question = pulse_suggestion_question(entry)
        if question:
            return question
    return PULSE_RED_DAILY_QUESTION_DEFAULT


def pulse_identities_match(left, right):
    if not left or not right:
        return False
    left_id = left.get("user_id")
    right_id = right.get("user_id")
    if left_id is not None and right_id is not None and int(left_id) == int(right_id):
        return True
    left_username = (left.get("username") or "").lower()
    right_username = (right.get("username") or "").lower()
    return bool(left_username and right_username and left_username == right_username)


def pulse_question_owner(question: str, pool: str, day_key: str | None = None):
    question = (question or "").strip()
    pool = (pool or "green").strip().lower()
    today = day_key or pulse_day_key()
    for entry in pulse_question_suggestions:
        if entry.get("status") != "approved":
            continue
        active_from = (entry.get("active_from_day_key") or "").strip()
        if active_from != today:
            continue
        if (entry.get("pool") or "green") != pool:
            continue
        candidate = (entry.get("edited_question") or entry.get("question") or "").strip()
        if candidate == question:
            return {
                "user_id": entry.get("user_id"),
                "username": entry.get("username"),
                "display_name": entry.get("display_name"),
            }
    return None


def pulse_user_answered_question_today(identity, question: str, pulse_type: str, day_key: str | None = None):
    day = day_key or pulse_day_key()
    question = (question or "").strip()
    pulse_type = (pulse_type or "green").strip().lower()
    for entry in pulse_entries_for_day(day):
        if (entry.get("question") or "").strip() != question:
            continue
        if (entry.get("pulse_type") or "green") != pulse_type:
            continue
        if pulse_identities_match(identity, {
            "user_id": entry.get("sender_user_id"),
            "username": entry.get("sender_username"),
        }):
            return True
    return False


def pulse_suggestion_question(entry):
    return (entry.get("edited_question") or entry.get("question") or "").strip()


try:
    _archive_backfill_stats = pulse_archive_backfill()
    if not _archive_backfill_stats.get("skipped"):
        print(
            f"[{now_iso()}] pulse archive backfill: "
            f"{_archive_backfill_stats.get('answers', 0)} answers, "
            f"{_archive_backfill_stats.get('questions', 0)} questions",
            flush=True,
        )
except Exception as exc:
    print(f"[{now_iso()}] pulse archive backfill error: {exc!r}", flush=True)


def pulse_question_suggestion_admin_payload(entry):
    if not entry:
        return None
    question = pulse_suggestion_question(entry)
    pool = entry.get("pool") or "green"
    active_from = (entry.get("active_from_day_key") or "").strip() or None
    return {
        "id": entry.get("id"),
        "pool": pool,
        "category": entry.get("category") or "General",
        "question": entry.get("question") or "",
        "edited_question": entry.get("edited_question"),
        "display_question": question,
        "status": entry.get("status"),
        "submitted_at": entry.get("submitted_at"),
        "day_key": entry.get("day_key"),
        "active_from_day_key": active_from,
        "active_from_label": pulse_day_label(active_from) if active_from else None,
        "user_id": entry.get("user_id"),
        "username": entry.get("username"),
        "display_name": entry.get("display_name"),
        "reviewed_at": entry.get("reviewed_at"),
        "reviewed_by": entry.get("reviewed_by"),
        "review_message_sent": entry.get("review_message_sent"),
        "schedule_mode": entry.get("schedule_mode") or "tomorrow",
        "needs_admin_notify": entry.get("needs_admin_notify"),
        "source": entry.get("source"),
        "answers_count": pulse_question_answer_count(question, pool),
        "answers_today": pulse_question_answer_count_for_day(question, pool),
    }


def pulse_answers_for_suggestion(suggestion_id: int):
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return None, []
    question = pulse_suggestion_question(entry)
    pool = (entry.get("pool") or "green").strip().lower()
    rows = []
    for pulse_entry in pulse_entries:
        if pulse_entry.get("status") != "completed":
            continue
        if (pulse_entry.get("question") or "").strip() != question:
            continue
        if (pulse_entry.get("pulse_type") or "green") != pool:
            continue
        payload = public_pulse_payload(pulse_entry)
        if payload:
            rows.append(payload)
    rows.sort(key=lambda item: item.get("responded_at") or item.get("sent_at") or "", reverse=True)
    return entry, rows


def pulse_pool_schedule_payload(pool: str = "green"):
    pool = (pool or "green").strip().lower()
    today = pulse_day_key()
    tomorrow = pulse_next_day_key()
    today_rows = []
    tomorrow_rows = []
    archive_rows = []
    for entry in pulse_question_suggestions:
        if (entry.get("pool") or "green") != pool:
            continue
        if entry.get("status") != "approved":
            continue
        payload = pulse_question_suggestion_admin_payload(entry)
        if not payload:
            continue
        active_from = (entry.get("active_from_day_key") or "").strip()
        if active_from == today:
            today_rows.append(payload)
        elif active_from == tomorrow:
            tomorrow_rows.append(payload)
        elif active_from and active_from < today:
            archive_rows.append(payload)
    sort_newest = lambda row: row.get("active_from_day_key") or row.get("submitted_at") or ""
    if pool == "red":
        today_rows.sort(key=sort_newest, reverse=True)
        tomorrow_rows.sort(key=sort_newest, reverse=True)
    else:
        today_rows.sort(key=lambda row: row.get("display_question") or "")
        tomorrow_rows.sort(key=lambda row: row.get("display_question") or "")
    archive_rows.sort(key=sort_newest, reverse=True)
    pool_label = "Red" if pool == "red" else "Green"
    return {
        "pool": pool,
        "day_key": today,
        "day_label": pulse_day_label(today),
        "tomorrow_key": tomorrow,
        "tomorrow_label": pulse_day_label(tomorrow),
        "rollover_note": (
            f"At midnight UK time, the {pool_label} Pulse scheduled for Tomorrow becomes Today "
            f"and Today moves to Archive."
        ),
        "today": today_rows,
        "tomorrow": tomorrow_rows,
        "archive": archive_rows,
    }


def pulse_green_schedule_payload():
    return pulse_pool_schedule_payload("green")


def pulse_question_suggestion_owner_fields(suggestion_id: int | None):
    if not suggestion_id:
        return {}
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {}
    return {
        "submitter_user_id": entry.get("user_id"),
        "submitter_username": entry.get("username"),
        "submitter_display_name": entry.get("display_name"),
        "submitted_at": entry.get("submitted_at"),
        "active_from_day_key": entry.get("active_from_day_key"),
    }


def pulse_suggestions_for_user(user_id=None, username=None, *, include_rejected: bool = True):
    identity = {"user_id": user_id, "username": username}
    rows = []
    for entry in pulse_question_suggestions:
        status = (entry.get("status") or "").strip().lower()
        if status == "deleted":
            continue
        if status == "rejected" and not include_rejected:
            continue
        owner = {"user_id": entry.get("user_id"), "username": entry.get("username")}
        if pulse_identities_match(identity, owner):
            rows.append(entry)
    return rows


def pulse_answers_to_suggestion(identity, suggestion):
    question = pulse_suggestion_question(suggestion)
    pool = (suggestion.get("pool") or "green").strip().lower()
    answers = []
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if (entry.get("question") or "").strip() != question:
            continue
        if (entry.get("pulse_type") or "green") != pool:
            continue
        owner = {
            "user_id": entry.get("question_owner_user_id"),
            "username": entry.get("question_owner_username"),
        }
        if owner.get("user_id") or owner.get("username"):
            if not pulse_identities_match(identity, owner):
                continue
        else:
            roster_owner = pulse_question_owner(question, pool)
            if not roster_owner or not pulse_identities_match(identity, roster_owner):
                continue
        answers.append({
            "pulse_id": entry.get("id"),
            "answer": entry.get("response_answer") or entry.get("sender_note") or entry.get("answer"),
            "received_at": entry.get("responded_at") or entry.get("sent_at"),
            "day_key": entry.get("day_key"),
        })
    answers.sort(key=lambda item: item.get("received_at") or "", reverse=True)
    return answers


def pulse_owned_suggestion_payload(identity, suggestion):
    question = pulse_suggestion_question(suggestion)
    answers = pulse_answers_to_suggestion(identity, suggestion)
    active_from = (suggestion.get("active_from_day_key") or "").strip()
    return {
        "suggestion_id": suggestion.get("id"),
        "question": question,
        "pool": suggestion.get("pool") or "green",
        "category": suggestion.get("category") or "General",
        "status": suggestion.get("status"),
        "review_status": suggestion.get("status"),
        "submitted_at": suggestion.get("submitted_at"),
        "submitted_day_key": suggestion.get("day_key"),
        "active_from_day_key": active_from or None,
        "active_from_label": pulse_day_label(active_from) if active_from else None,
        "reviewed_at": suggestion.get("reviewed_at"),
        "rejection_reason": (suggestion.get("rejection_reason") or "").strip() or None,
        "resubmit_allowed": bool(suggestion.get("resubmit_allowed")),
        "answers_count": len(answers),
        "answers": answers,
    }


def pulse_my_pulses_payload(identity):
    today_key = pulse_day_key()
    today_rows = []
    past_rows = []
    for suggestion in pulse_suggestions_for_user(identity.get("user_id"), identity.get("username")):
        row = pulse_owned_suggestion_payload(identity, suggestion)
        status = (suggestion.get("status") or "").strip().lower()
        active_from = (suggestion.get("active_from_day_key") or "").strip()
        submitted_day = (suggestion.get("day_key") or "").strip()
        reviewed_day = (suggestion.get("reviewed_at") or suggestion.get("submitted_at") or "")[:10]

        if status in {"pending_review", "reserved"}:
            today_rows.append(row)
        elif status == "rejected":
            if submitted_day == today_key or reviewed_day == today_key:
                today_rows.append(row)
            else:
                past_rows.append(row)
        elif submitted_day == today_key or (active_from and active_from >= today_key):
            today_rows.append(row)
        elif status == "approved" and active_from and active_from < today_key:
            past_rows.append(row)
        elif status == "approved" and not active_from:
            past_rows.append(row)

    sort_key = lambda row: row.get("reviewed_at") or row.get("submitted_at") or row.get("active_from_day_key") or row.get("submitted_day_key") or ""
    today_rows.sort(key=sort_key, reverse=True)
    past_rows.sort(key=sort_key, reverse=True)
    return {"today": today_rows, "past": past_rows}


def seconds_until_next_uk_midnight() -> int:
    current = uk_now()
    tomorrow = (current + datetime.timedelta(days=1)).date()
    reset_at = datetime.datetime.combine(tomorrow, datetime.time.min, tzinfo=UK_TZ)
    return max(0, int((reset_at - current).total_seconds()))


def pulse_reset_interval_hours() -> int:
    return load_pulse_settings()["reset_interval_hours"]


def pulse_green_unlock_interval_hours() -> int:
    return pulse_reset_interval_hours()


def next_pulse_unlock_at(now: datetime.datetime | None = None, interval_hours: int | None = None) -> datetime.datetime:
    current = now or uk_now()
    interval = normalized_pulse_reset_interval(interval_hours or pulse_reset_interval_hours())
    midnight = datetime.datetime.combine(current.date(), datetime.time.min, tzinfo=UK_TZ)
    elapsed_seconds = max(0, int((current - midnight).total_seconds()))
    interval_seconds = interval * 3600
    next_boundary_seconds = ((elapsed_seconds // interval_seconds) + 1) * interval_seconds
    if next_boundary_seconds >= 24 * 3600:
        return midnight + datetime.timedelta(days=1)
    return midnight + datetime.timedelta(seconds=next_boundary_seconds)


def seconds_until_next_pulse_unlock(now: datetime.datetime | None = None, interval_hours: int | None = None) -> int:
    current = now or uk_now()
    return max(0, int((next_pulse_unlock_at(current, interval_hours) - current).total_seconds()))


def pulse_unlock_label(now: datetime.datetime | None = None, interval_hours: int | None = None) -> str:
    unlock_at = next_pulse_unlock_at(now, interval_hours)
    return unlock_at.astimezone(UK_TZ).strftime("%H:%M")


def verify_bot_sync_secret(x_bot_sync_secret: str | None):
    if not BOT_SYNC_SECRET:
        raise HTTPException(status_code=503, detail="Bot sync secret is not configured")
    if x_bot_sync_secret != BOT_SYNC_SECRET:
        raise HTTPException(status_code=403, detail="Invalid bot sync secret")


def verify_admin_secret(admin_secret: str | None):
    verify_bot_sync_secret(admin_secret)


def load_reward_assets_manifest() -> dict:
    try:
        if os.path.exists(REWARD_ASSETS_MANIFEST_PATH):
            with open(REWARD_ASSETS_MANIFEST_PATH, "r", encoding="utf-8") as handle:
                data = json.load(handle)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {"assets": []}


def save_reward_assets_manifest(data: dict) -> None:
    directory = os.path.dirname(REWARD_ASSETS_MANIFEST_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(REWARD_ASSETS_MANIFEST_PATH, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2)


def sanitize_reward_asset_stem(original_name: str) -> str:
    stem = Path(original_name or "reward").stem
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "-", stem).strip("-_").lower()
    return (cleaned or "reward")[:48]


def reward_asset_public_url(filename: str) -> str:
    return f"/api/reward-assets/{filename}"


def resolve_reward_asset_file(filename: str) -> str:
    name = Path(filename or "").name
    if not name or name != filename or ".." in name:
        raise ValueError("Invalid filename")
    path = os.path.join(REWARD_ASSETS_DIR, name)
    if not os.path.isfile(path):
        raise FileNotFoundError(name)
    return path


REWARD_PACK_ITEM_TYPES = {"color", "sticker", "skin", "backdrop", "title"}


def _normalize_reward_pack_item(item) -> dict | None:
    if not isinstance(item, dict):
        return None
    item_id = str(item.get("id") or "").strip()
    item_type = str(item.get("type") or "").strip().lower()
    if not item_id or item_type not in REWARD_PACK_ITEM_TYPES:
        return None
    entry = {
        "type": item_type,
        "id": item_id,
    }
    name = str(item.get("name") or item.get("label") or "").strip()
    if name:
        entry["name"] = name
    image = str(item.get("image") or item.get("icon") or item.get("src") or "").strip()
    if image:
        entry["image"] = image
    swatch = str(item.get("swatch") or "").strip()
    if swatch:
        entry["swatch"] = swatch
    if item_type == "skin":
        layout = str(item.get("layout") or item.get("skin_layout") or "").strip().lower()
        if layout in {"banner", "fill", "wide", "full"}:
            entry["layout"] = "banner"
        elif layout in {"stamp", "square", "corner", "seal"}:
            entry["layout"] = "stamp"
    return entry


VERIFICATION_PACK_KEYS = ("biker", "dancer", "daddy", "pup")

DEFAULT_VERIFICATION_PACK_IMAGES = {
    "biker": "assets/icons/fox-pack-biker.png",
    "dancer": "assets/icons/fox-pack-dancer.png",
    "daddy": "assets/icons/fox-pack-daddy.png",
    "pup": "assets/icons/fox-pack-pup.png",
}

DEFAULT_VERIFICATION_PACKS = {
    "biker": {
        "id": "biker",
        "title": "Biker Welcome Pack",
        "copy": "Your verification gift for Appearance — two colours, stickers, skins, and backdrops.",
        "image": DEFAULT_VERIFICATION_PACK_IMAGES["biker"],
        "items": [
            {"type": "color", "id": "green"},
            {"type": "color", "id": "blue"},
            {"type": "sticker", "id": "heart"},
            {"type": "sticker", "id": "fox"},
            {"type": "skin", "id": "spotlight"},
            {"type": "skin", "id": "basechat"},
            {"type": "backdrop", "id": "asmr"},
            {"type": "backdrop", "id": "stage"},
        ],
    },
    "dancer": {
        "id": "dancer",
        "title": "Dancer Welcome Pack",
        "copy": "Your verification gift for Appearance — two colours, stickers, skins, and backdrops.",
        "image": DEFAULT_VERIFICATION_PACK_IMAGES["dancer"],
        "items": [
            {"type": "color", "id": "rose"},
            {"type": "color", "id": "violet"},
            {"type": "sticker", "id": "star"},
            {"type": "sticker", "id": "heart"},
            {"type": "skin", "id": "pulse"},
            {"type": "skin", "id": "foxlove"},
            {"type": "backdrop", "id": "pulse"},
            {"type": "backdrop", "id": "spotlight"},
        ],
    },
    "daddy": {
        "id": "daddy",
        "title": "Daddy Welcome Pack",
        "copy": "Your verification gift for Appearance — two colours, stickers, skins, and backdrops.",
        "image": DEFAULT_VERIFICATION_PACK_IMAGES["daddy"],
        "items": [
            {"type": "color", "id": "gold"},
            {"type": "color", "id": "rose"},
            {"type": "sticker", "id": "fox"},
            {"type": "sticker", "id": "star"},
            {"type": "skin", "id": "foxlove"},
            {"type": "skin", "id": "spotlight"},
            {"type": "backdrop", "id": "wheel"},
            {"type": "backdrop", "id": "asmr"},
        ],
    },
    "pup": {
        "id": "pup",
        "title": "Pup Welcome Pack",
        "copy": "Your verification gift for Appearance — two colours, stickers, skins, and backdrops.",
        "image": DEFAULT_VERIFICATION_PACK_IMAGES["pup"],
        "items": [
            {"type": "color", "id": "gold"},
            {"type": "color", "id": "green"},
            {"type": "sticker", "id": "star"},
            {"type": "sticker", "id": "heart"},
            {"type": "skin", "id": "foxlove"},
            {"type": "skin", "id": "pulse"},
            {"type": "backdrop", "id": "wheel"},
            {"type": "backdrop", "id": "pulse"},
        ],
    },
}


def normalize_level_packs(raw) -> dict:
    if not isinstance(raw, dict):
        return {}
    packs: dict[str, dict] = {}
    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        pack_key = str(key or "").strip()
        if not pack_key:
            continue
        if not pack_key.startswith("level_"):
            level_num = str(value.get("level") or "").strip()
            if level_num.isdigit():
                pack_key = f"level_{level_num}"
            else:
                continue
        level_part = pack_key.replace("level_", "", 1)
        if not level_part.isdigit():
            continue
        items = []
        for item in value.get("items") or []:
            normalized = _normalize_reward_pack_item(item)
            if normalized:
                items.append(normalized)
        packs[pack_key] = {
            "id": str(value.get("id") or pack_key).strip() or pack_key,
            "title": str(value.get("title") or f"Level {level_part} Pack").strip(),
            "copy": str(value.get("copy") or "").strip(),
            "image": str(value.get("image") or "").strip(),
            "items": items,
        }
    return packs


def normalize_verification_pack_key(raw) -> str | None:
    key = str(raw or "").strip().lower()
    if key in VERIFICATION_PACK_KEYS:
        return key
    return None


def normalize_verification_packs(raw) -> dict:
    source = raw if isinstance(raw, dict) else {}
    packs: dict[str, dict] = {}
    for key in VERIFICATION_PACK_KEYS:
        defaults = DEFAULT_VERIFICATION_PACKS[key]
        value = source.get(key) if isinstance(source.get(key), dict) else {}
        items = []
        for item in value.get("items") or defaults.get("items") or []:
            normalized = _normalize_reward_pack_item(item)
            if normalized:
                items.append(normalized)
        if not items:
            for item in defaults.get("items") or []:
                normalized = _normalize_reward_pack_item(item)
                if normalized:
                    items.append(normalized)
        packs[key] = {
            "id": key,
            "title": str(value.get("title") or defaults.get("title") or f"{key.title()} Welcome Pack").strip(),
            "copy": str(value.get("copy") or defaults.get("copy") or "").strip(),
            "image": str(
                value.get("image")
                or defaults.get("image")
                or DEFAULT_VERIFICATION_PACK_IMAGES.get(key)
                or ""
            ).strip(),
            "items": items,
        }
    return packs


def normalize_verification_packs_active(raw) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on", "active"}


def find_user_verification_pack_key(user_id: int | None = None, username: str | None = None) -> str | None:
    username_key = (username or "").strip().lstrip("@").lower()
    matches = []
    for entry in miniapp_verifications:
        if not isinstance(entry, dict):
            continue
        same_user = user_id and int(entry.get("user_id") or 0) == int(user_id)
        same_username = username_key and (entry.get("username") or "").strip().lstrip("@").lower() == username_key
        if not (same_user or same_username):
            continue
        pack_key = normalize_verification_pack_key(entry.get("selected_pack"))
        if not pack_key:
            continue
        matches.append((
            1 if (entry.get("status") or "").strip().lower() == "completed" else 0,
            entry.get("completed_at") or entry.get("last_seen_at") or entry.get("requested_at") or "",
            pack_key,
        ))
    if matches:
        matches.sort(reverse=True)
        return matches[0][2]

    if user_id:
        rows = fox_db_rows(
            "SELECT choice_label FROM fox_votes WHERE user_id = ? LIMIT 1",
            (int(user_id),),
        )
        if rows:
            pack_key = normalize_verification_pack_key(rows[0].get("choice_label"))
            if pack_key:
                return pack_key
        reward_rows = fox_db_rows(
            "SELECT reward_label FROM user_rewards WHERE user_id = ? LIMIT 1",
            (int(user_id),),
        )
        if reward_rows:
            pack_key = normalize_verification_pack_key(reward_rows[0].get("reward_label"))
            if pack_key:
                return pack_key
    return None


def verification_pack_eligibility(user_id: int | None = None, username: str | None = None) -> dict:
    catalog = load_reward_catalog()
    active = bool(catalog.get("verification_packs_active"))
    packs = catalog.get("verification_packs") or normalize_verification_packs({})
    pack_key = find_user_verification_pack_key(user_id, username)
    pack = packs.get(pack_key) if pack_key else None
    return {
        "status": "ok",
        "active": active,
        "activated_at": catalog.get("verification_packs_activated_at"),
        "pack_key": pack_key,
        "eligible": bool(active and pack),
        "source_key": "verification_pack",
        "pack": pack,
    }


def _slugify_achievement_id(value: str, fallback: str = "achievement") -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    cleaned = re.sub(r"_+", "_", cleaned).strip("_")
    return cleaned or fallback


def normalize_reward_achievements(raw) -> list[dict]:
    if not isinstance(raw, list):
        return []
    achievements: list[dict] = []
    used_ids: set[str] = set()
    for index, entry in enumerate(raw):
        if not isinstance(entry, dict):
            continue
        items = []
        for item in entry.get("items") or []:
            normalized = _normalize_reward_pack_item(item)
            if normalized:
                items.append(normalized)
        name = str(entry.get("name") or "").strip()
        profile_id = str(entry.get("profileId") or entry.get("profile_id") or "").strip()
        ach_id = str(entry.get("id") or "").strip()
        if not ach_id:
            ach_id = _slugify_achievement_id(profile_id or name or f"achievement_{index + 1}")
        # Keep ids unique within the published catalog.
        base_id = ach_id
        suffix = 2
        while ach_id in used_ids:
            ach_id = f"{base_id}_{suffix}"
            suffix += 1
        used_ids.add(ach_id)

        target_raw = entry.get("target")
        try:
            target = int(target_raw) if target_raw not in (None, "") else 0
        except (TypeError, ValueError):
            target = 0

        achievements.append(
            {
                "id": ach_id,
                "name": name,
                "shortName": str(entry.get("shortName") or entry.get("short_name") or name).strip(),
                "description": str(entry.get("description") or "").strip(),
                "condition": str(entry.get("condition") or "").strip(),
                "stat": str(entry.get("stat") or "").strip(),
                "target": max(0, target),
                "trophyColor": str(entry.get("trophyColor") or entry.get("trophy_color") or "").strip(),
                "progressVerb": str(entry.get("progressVerb") or entry.get("progress_verb") or "").strip(),
                "icon": str(entry.get("icon") or "").strip(),
                "boxArt": str(entry.get("boxArt") or entry.get("box_art") or "").strip(),
                "profileId": profile_id,
                "packTitle": str(entry.get("packTitle") or entry.get("pack_title") or "").strip(),
                "packCopy": str(entry.get("packCopy") or entry.get("pack_copy") or "").strip(),
                "prize": str(entry.get("prize") or "").strip(),
                "prizeLabel": str(entry.get("prizeLabel") or entry.get("prize_label") or "").strip(),
                "items": items,
            }
        )
    return achievements


def empty_reward_catalog() -> dict:
    return {
        "level_packs": {},
        "achievements": [],
        "verification_packs": normalize_verification_packs({}),
        "verification_packs_active": False,
        "verification_packs_activated_at": None,
        "updated_at": None,
    }


def load_reward_catalog() -> dict:
    catalog = empty_reward_catalog()
    try:
        if os.path.exists(REWARD_CATALOG_PATH):
            with open(REWARD_CATALOG_PATH, "r", encoding="utf-8") as handle:
                data = json.load(handle)
                if isinstance(data, dict):
                    catalog["level_packs"] = normalize_level_packs(data.get("level_packs"))
                    catalog["achievements"] = normalize_reward_achievements(
                        data.get("achievements")
                    )
                    catalog["verification_packs"] = normalize_verification_packs(
                        data.get("verification_packs")
                    )
                    catalog["verification_packs_active"] = normalize_verification_packs_active(
                        data.get("verification_packs_active")
                    )
                    activated = data.get("verification_packs_activated_at")
                    catalog["verification_packs_activated_at"] = (
                        str(activated).strip() if activated else None
                    )
                    updated = data.get("updated_at")
                    catalog["updated_at"] = str(updated).strip() if updated else None
    except Exception:
        pass
    return catalog


def save_reward_catalog(catalog: dict) -> dict:
    active = normalize_verification_packs_active(catalog.get("verification_packs_active"))
    activated_at = catalog.get("verification_packs_activated_at")
    if active and not activated_at:
        activated_at = now_iso()
    if not active:
        activated_at = None
    payload = {
        "level_packs": normalize_level_packs(catalog.get("level_packs")),
        "achievements": normalize_reward_achievements(catalog.get("achievements")),
        "verification_packs": normalize_verification_packs(catalog.get("verification_packs")),
        "verification_packs_active": active,
        "verification_packs_activated_at": str(activated_at).strip() if activated_at else None,
        "updated_at": now_iso(),
    }
    directory = os.path.dirname(REWARD_CATALOG_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(REWARD_CATALOG_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    return payload


def telegram_admin_notify(text: str, topic_id: int | None = None) -> bool:
    if not TELEGRAM_BOT_TOKEN or not ALCOVE_ADMIN_GROUP_ID:
        return False
    payload = {
        "chat_id": ALCOVE_ADMIN_GROUP_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if topic_id:
        payload["message_thread_id"] = topic_id
    try:
        body = json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            return 200 <= response.status < 300
    except Exception as exc:
        print(f"[{now_iso()}] telegram admin notify error: {exc!r}", flush=True)
        return False


def pulse_admin_notify(text: str, topic_id: int | None = None) -> bool:
    return False


def drain_pulse_admin_telegram_backlog() -> None:
    if not PULSE_ADMIN_TELEGRAM_SUPPRESSED:
        return
    changed = False
    stamped = now_iso()
    for entry in pulse_question_suggestions:
        if entry.get("status") == "pending_review" and not entry.get("review_message_sent"):
            entry["review_message_sent"] = True
            entry["needs_admin_notify"] = False
            changed = True
    for entry in pulse_entries:
        if entry.get("status") == "completed" and not entry.get("admin_posted_at"):
            entry["admin_posted_at"] = stamped
            changed = True
    for state in pulse_daily_summary_posts:
        if not state.get("admin_posted_at"):
            state["admin_posted_at"] = stamped
            changed = True
    if changed:
        save_runtime_state(force=True)
        print(
            f"[{stamped}] Drained pulse admin Telegram backlog "
            f"({sum(1 for e in pulse_entries if e.get('status') == 'completed')} completed pulses checked).",
            flush=True,
        )


drain_pulse_admin_telegram_backlog()


def apply_admin_pulse_question_action(
    entry: dict,
    action: str,
    edited_question: str | None = None,
    rejection_reason: str | None = None,
) -> None:
    action = (action or "").strip().lower()
    if action == "amend":
        text = (edited_question or "").strip()
        if not text:
            raise HTTPException(status_code=400, detail="Edited question text is required.")
        too_long = pulse_question_too_long(text)
        if too_long:
            raise HTTPException(status_code=400, detail=too_long)
        entry["edited_question"] = text
        entry["status"] = "pending_review"
        entry["needs_admin_notify"] = True
        entry["review_message_sent"] = False
        return
    if action == "reject":
        reason = (rejection_reason or "").strip()
        if not reason:
            raise HTTPException(
                status_code=400,
                detail="A rejection reason is required so F.O.X can message the member.",
            )
        current_status = (entry.get("status") or "").strip().lower()
        if current_status == "rejected" and (entry.get("rejection_reason") or "").strip() == reason:
            # Idempotent re-click — do not queue another conflicting DM.
            return
        entry["status"] = "rejected"
        entry["rejection_reason"] = reason
        entry["resubmit_allowed"] = not pulse_rejection_replacement_used_today(
            entry.get("user_id"),
            entry.get("username"),
        )
        entry["reviewed_at"] = now_iso()
        queue_pulse_question_review_notification(entry, "question_rejected", rejection_reason=reason)
        return
    if action == "delete":
        # Silent remove — no rejection form, no F.O.X DM, hidden from member app.
        cancel_pending_pulse_question_review_notifications(
            int(entry.get("id") or 0),
            reason="deleted",
        )
        entry["status"] = "deleted"
        entry["rejection_reason"] = None
        entry["resubmit_allowed"] = False
        entry["needs_admin_notify"] = False
        entry["reviewed_at"] = now_iso()
        return
    if action in {"today", "tomorrow", "reserve"}:
        pool = (entry.get("pool") or "green").strip().lower()
        if pool == "red" and action == "reserve":
            raise HTTPException(
                status_code=400,
                detail="Red Pulse questions can only be scheduled for Today or Tomorrow.",
            )
        text = (edited_question or "").strip()
        if text and text != (entry.get("question") or "").strip():
            too_long = pulse_question_too_long(text)
            if too_long:
                raise HTTPException(status_code=400, detail=too_long)
            entry["edited_question"] = text
        apply_pulse_suggestion_schedule(entry, action, approve=True)
        ensure_single_red_slot(entry)
        entry["reviewed_at"] = now_iso()
        entry["needs_admin_notify"] = False
        entry["resubmit_allowed"] = False
        archive_pulse_question_from_suggestion(entry)
        if entry.get("status") == "approved":
            queue_pulse_question_review_notification(entry, "question_approved")
        return
    raise HTTPException(status_code=400, detail="Unknown Pulse question action.")


def create_admin_pulse_question(
    question: str,
    pool: str = "green",
    category: str = "General",
    schedule_mode: str = "tomorrow",
    *,
    source: str = "feature_admin",
    display_name: str = "Feature Admin",
) -> dict:
    pool = (pool or "green").strip().lower()
    question = (question or "").strip()
    if pool not in {"green", "red"}:
        raise HTTPException(status_code=400, detail="Pool must be green or red.")
    if len(question) < 8:
        raise HTTPException(status_code=400, detail="Question must be at least 8 characters.")
    too_long = pulse_question_too_long(question)
    if too_long:
        raise HTTPException(status_code=400, detail=too_long)

    schedule = normalize_pulse_schedule_mode(schedule_mode)
    if pool == "red" and schedule == "reserve":
        raise HTTPException(
            status_code=400,
            detail="Red Pulse questions can only be scheduled for Today or Tomorrow.",
        )
    entry = {
        "id": len(pulse_question_suggestions) + 1,
        "pool": pool,
        "category": "General",
        "question": question,
        "edited_question": None,
        "submitted_at": now_iso(),
        "day_key": pulse_day_key(),
        "user_id": None,
        "username": None,
        "display_name": display_name,
        "status": "pending_review",
        "schedule_mode": schedule,
        "needs_admin_notify": False,
        "review_message_sent": True,
        "reviewed_at": None,
        "reviewed_by": None,
        "active_from_day_key": None,
        "source": source,
    }
    apply_pulse_suggestion_schedule(entry, schedule, approve=True)
    ensure_single_red_slot(entry)
    entry["reviewed_at"] = now_iso()
    pulse_question_suggestions.append(entry)
    archive_pulse_question_from_suggestion(entry)
    save_runtime_state()
    return entry


def verify_telegram_init_data(init_data: str) -> dict:
    if not TELEGRAM_BOT_TOKEN:
        raise HTTPException(status_code=503, detail="Telegram bot token is not configured")
    if not init_data:
        raise HTTPException(status_code=400, detail="Missing Telegram Mini App data")

    params = dict(parse_qsl(init_data, keep_blank_values=True))
    received_hash = params.pop("hash", None)
    if not received_hash:
        raise HTTPException(status_code=400, detail="Missing Telegram Mini App hash")

    data_check_string = "\n".join(f"{key}={params[key]}" for key in sorted(params))
    secret_key = hmac.new(b"WebAppData", TELEGRAM_BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
    expected_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected_hash, received_hash):
        raise HTTPException(status_code=403, detail="Invalid Telegram Mini App signature")

    try:
        auth_date = int(params.get("auth_date") or 0)
    except ValueError:
        auth_date = 0
    if auth_date and (datetime.datetime.utcnow().timestamp() - auth_date) > 86400:
        raise HTTPException(status_code=403, detail="Telegram Mini App session expired")

    try:
        user = json.loads(params.get("user") or "{}")
    except json.JSONDecodeError:
        user = {}
    if not isinstance(user, dict) or not user.get("id"):
        raise HTTPException(status_code=400, detail="Telegram Mini App user was not provided")
    return user


def clipped_text(value, limit=500):
    if value is None:
        return None
    text = str(value)
    return text if len(text) <= limit else text[:limit] + "..."


def clipped_detail(value):
    if not isinstance(value, dict):
        return {}
    detail = {}
    for key, item in value.items():
        clean_key = clipped_text(key, 80)
        if clean_key:
            detail[clean_key] = clipped_text(item, 700)
    return detail


def append_verification_flow_log(payload: VerificationLogPayload):
    entry = {
        "received_at": now_iso(),
        "session_id": clipped_text(payload.session_id, 120),
        "event": clipped_text(payload.event, 120) or "unknown",
        "level": clipped_text(payload.level, 20) or "info",
        "user_id": payload.user_id,
        "username": clipped_text(payload.username, 80),
        "display_name": clipped_text(payload.display_name, 120),
        "step_index": payload.step_index,
        "step_title": clipped_text(payload.step_title, 160),
        "step_pose": clipped_text(payload.step_pose, 60),
        "action": clipped_text(payload.action, 120),
        "selected_pack": clipped_text(payload.selected_pack, 60),
        "message": clipped_text(payload.message, 900),
        "detail": clipped_detail(payload.detail),
        "user_agent": clipped_text(payload.user_agent, 300),
        "url": clipped_text(payload.url, 500),
        "client_time": clipped_text(payload.client_time, 80),
    }
    directory = os.path.dirname(VERIFY_FLOW_LOG_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(VERIFY_FLOW_LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    if entry["level"] in {"warn", "error"} or "error" in str(entry["event"]).lower():
        print(
            f"[{entry['received_at']}] verification {entry['level']} "
            f"user_id={entry['user_id']} username={entry['username']!r} "
            f"step={entry['step_index']} event={entry['event']!r} message={entry['message']!r}",
            flush=True,
        )
    return entry


def read_verification_flow_logs(limit=80, user_id=None, username=None, session_id=None):
    limit = max(1, min(int(limit or 80), 500))
    if not os.path.exists(VERIFY_FLOW_LOG_PATH):
        return []
    username = (username or "").strip().lstrip("@").lower()
    session_id = (session_id or "").strip()
    rows = []
    with open(VERIFY_FLOW_LOG_PATH, "r", encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            if user_id is not None and int(entry.get("user_id") or 0) != int(user_id):
                continue
            if username and (entry.get("username") or "").strip().lstrip("@").lower() != username:
                continue
            if session_id and entry.get("session_id") != session_id:
                continue
            rows.append(entry)
    return rows[-limit:]


def miniapp_verification_payload(entry: dict) -> dict:
    return {
        "id": entry.get("id"),
        "user_id": entry.get("user_id"),
        "username": entry.get("username"),
        "first_name": entry.get("first_name"),
        "last_name": entry.get("last_name"),
        "display_name": entry.get("display_name"),
        "selected_pack": entry.get("selected_pack"),
        "feedback": entry.get("feedback"),
        "status": entry.get("status"),
        "requested_at": entry.get("requested_at"),
        "completed_at": entry.get("completed_at"),
        "detail": entry.get("detail"),
    }


def upsert_miniapp_verification(user: dict) -> dict:
    now = now_iso()
    user_id = int(user.get("id"))
    display_name = " ".join(
        part for part in [user.get("first_name"), user.get("last_name")] if part
    ).strip() or user.get("username") or str(user_id)

    existing = next(
        (
            entry for entry in reversed(miniapp_verifications)
            if int(entry.get("user_id") or 0) == user_id
        ),
        None,
    )
    if existing:
        if existing.get("status") == "failed":
            existing["status"] = "pending"
            existing["requested_at"] = now
            existing["completed_at"] = None
            existing.pop("detail", None)
        existing.update({
            "username": user.get("username"),
            "first_name": user.get("first_name"),
            "last_name": user.get("last_name"),
            "display_name": display_name,
            "last_seen_at": now,
        })
        return existing

    entry = {
        "id": (max([int(item.get("id") or 0) for item in miniapp_verifications] or [0]) + 1),
        "user_id": user_id,
        "username": user.get("username"),
        "first_name": user.get("first_name"),
        "last_name": user.get("last_name"),
        "display_name": display_name,
        "status": "pending",
        "requested_at": now,
        "last_seen_at": now,
        "completed_at": None,
    }
    miniapp_verifications.append(entry)
    return entry


def update_miniapp_verification_details(entry: dict, payload: MiniappVerificationPayload) -> dict:
    if payload.selected_pack:
        entry["selected_pack"] = clipped_text(payload.selected_pack, 60)
    if payload.feedback:
        entry["feedback"] = clipped_text(payload.feedback, 1200)
    return entry


def merged_feature_flags(saved: dict | None = None) -> dict:
    flags = {
        group: values.copy()
        for group, values in DEFAULT_FEATURE_FLAGS.items()
    }
    if not isinstance(saved, dict):
        return flags
    for group, values in saved.items():
        if group not in flags or not isinstance(values, dict):
            continue
        for key, value in values.items():
            if key in flags[group]:
                flags[group][key] = bool(value)
    return flags


def load_feature_flags_raw() -> dict:
    if os.path.exists(FEATURE_FLAGS_PATH):
        try:
            with open(FEATURE_FLAGS_PATH, "r", encoding="utf-8") as handle:
                saved = json.load(handle)
                if isinstance(saved, dict):
                    return saved
        except (OSError, json.JSONDecodeError):
            pass
    return {}


def normalize_tester_usernames(values) -> list[str]:
    if not isinstance(values, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        username = str(value or "").strip().lstrip("@").lower()
        if username and username not in seen:
            seen.add(username)
            result.append(username)
    return sorted(result)


def load_tester_usernames() -> list[str]:
    return normalize_tester_usernames(load_feature_flags_raw().get("tester_usernames"))


def load_feature_flags() -> dict:
    saved = load_feature_flags_raw()
    flags = merged_feature_flags(saved)
    saved_pages = saved.get("pages") if isinstance(saved.get("pages"), dict) else None
    saved_wellbeing = saved.get("wellbeing") if isinstance(saved.get("wellbeing"), dict) else None
    needs_save = (
        not isinstance(saved.get("pages"), dict)
        or not isinstance(saved.get("wellbeing"), dict)
        or saved_pages != flags.get("pages")
        or saved_wellbeing != flags.get("wellbeing")
    )
    if needs_save:
        save_feature_flags(flags)
    return flags


def save_feature_flags(flags: dict, tester_usernames: list[str] | None = None) -> None:
    raw = load_feature_flags_raw()
    payload = {
        group: flags[group]
        for group in DEFAULT_FEATURE_FLAGS
        if group in flags
    }
    if tester_usernames is not None:
        payload["tester_usernames"] = normalize_tester_usernames(tester_usernames)
    elif "tester_usernames" in raw:
        payload["tester_usernames"] = normalize_tester_usernames(raw.get("tester_usernames"))
    directory = os.path.dirname(FEATURE_FLAGS_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(FEATURE_FLAGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


DEFAULT_SAFETY_SETTINGS = {
    "flood_message_threshold": 8,
    "flood_window_seconds": 60,
    "daily_digest_enabled": True,
    "daily_digest_hour_utc": 8,
    "keywords": [],
}


def normalized_safety_settings(raw: dict | None = None) -> dict:
    settings = {
        **DEFAULT_SAFETY_SETTINGS,
        "keywords": [],
    }
    if not isinstance(raw, dict):
        return settings
    try:
        settings["flood_message_threshold"] = max(3, min(int(raw.get("flood_message_threshold", 8)), 50))
    except (TypeError, ValueError):
        pass
    try:
        settings["flood_window_seconds"] = max(15, min(int(raw.get("flood_window_seconds", 60)), 600))
    except (TypeError, ValueError):
        pass
    settings["daily_digest_enabled"] = bool(raw.get("daily_digest_enabled", True))
    try:
        settings["daily_digest_hour_utc"] = max(0, min(int(raw.get("daily_digest_hour_utc", 8)), 23))
    except (TypeError, ValueError):
        pass
    keywords = []
    for entry in raw.get("keywords") or []:
        if not isinstance(entry, dict):
            continue
        term = str(entry.get("term") or "").strip().lower()
        if not term:
            continue
        severity = str(entry.get("severity") or "medium").strip().lower()
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        keywords.append(
            {
                "term": term,
                "category": str(entry.get("category") or "custom").strip() or "custom",
                "severity": severity,
                "enabled": bool(entry.get("enabled", True)),
                "added_at": entry.get("added_at") or now_iso(),
                "added_by": entry.get("added_by") or "admin",
            }
        )
    settings["keywords"] = keywords
    return settings


def load_safety_settings() -> dict:
    if not os.path.exists(SAFETY_SETTINGS_PATH):
        return normalized_safety_settings()
    try:
        with open(SAFETY_SETTINGS_PATH, "r", encoding="utf-8") as handle:
            return normalized_safety_settings(json.load(handle))
    except (OSError, json.JSONDecodeError):
        return normalized_safety_settings()


def save_safety_settings(settings: dict) -> dict:
    normalized = normalized_safety_settings(settings)
    directory = os.path.dirname(SAFETY_SETTINGS_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(SAFETY_SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)
    return normalized


def normalized_pulse_threshold(value) -> int:
    try:
        return max(1, min(999, int(value)))
    except (TypeError, ValueError):
        return max(1, min(999, PULSE_DEFAULT_HEAT_THRESHOLD))


def default_pulse_settings() -> dict:
    return {
        "heat_threshold": normalized_pulse_threshold(PULSE_DEFAULT_HEAT_THRESHOLD),
        "reset_interval_hours": 12,
    }


def load_pulse_settings() -> dict:
    settings = default_pulse_settings()
    if not os.path.exists(PULSE_SETTINGS_PATH):
        return settings
    try:
        with open(PULSE_SETTINGS_PATH, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return settings
    if isinstance(saved, dict):
        settings["heat_threshold"] = normalized_pulse_threshold(saved.get("heat_threshold"))
        settings["reset_interval_hours"] = normalized_pulse_reset_interval(saved.get("reset_interval_hours"))
    return settings


def save_pulse_settings(settings: dict) -> None:
    directory = os.path.dirname(PULSE_SETTINGS_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    normalized = {
        "heat_threshold": normalized_pulse_threshold(settings.get("heat_threshold")),
        "reset_interval_hours": normalized_pulse_reset_interval(settings.get("reset_interval_hours")),
    }
    with open(PULSE_SETTINGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(normalized, handle, indent=2, sort_keys=True)


def pulse_heat_threshold() -> int:
    return load_pulse_settings()["heat_threshold"]


def pulse_progress_payload(day_key: str | None = None) -> dict:
    threshold = pulse_heat_threshold()
    sent = pulse_sent_today_count(day_key)
    remaining = max(threshold - sent, 0)
    green_interval_hours = pulse_green_unlock_interval_hours()
    return {
        "heat_threshold": threshold,
        "reset_interval_hours": green_interval_hours,
        "sent_today": sent,
        "remaining_today": remaining,
        "progress_percent": min(100, int((sent / max(threshold, 1)) * 100)),
        "red_unlocked": sent >= threshold,
        "day_key": day_key or pulse_day_key(),
        "day_label": pulse_day_label(day_key),
        "next_unlock_at": next_pulse_unlock_at(interval_hours=green_interval_hours).isoformat(),
        "next_unlock_label": pulse_unlock_label(interval_hours=green_interval_hours),
        "reset_seconds": seconds_until_next_pulse_unlock(interval_hours=green_interval_hours),
    }


def fox_db_rows(query: str, params=()):
    if not os.path.exists(FOX_LOGS_DB_PATH):
        return []

    try:
        with sqlite3.connect(FOX_LOGS_DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            ensure_fox_read_tables(conn)
            return [dict(row) for row in conn.execute(query, params).fetchall()]
    except sqlite3.Error:
        return []


def ensure_fox_read_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            display_name TEXT,
            first_seen TEXT,
            last_seen TEXT,
            verified_at TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS verified_users (
            user_id INTEGER PRIMARY KEY,
            verified_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS messages (
            message_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            timestamp TEXT,
            contains_link INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS link_violations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            user_id INTEGER,
            username TEXT,
            display_name TEXT,
            message_excerpt TEXT,
            link_samples TEXT,
            logged_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tone_flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            user_id INTEGER,
            username TEXT,
            display_name TEXT,
            categories TEXT,
            severity TEXT,
            score INTEGER,
            matched_terms TEXT,
            message_excerpt TEXT,
            logged_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_strikes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            admin_user_id INTEGER,
            reason TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT,
            removed_at TEXT,
            removed_by INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS captcha_attempts (
            user_id INTEGER,
            attempt_time TEXT,
            success INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fox_votes (
            user_id INTEGER PRIMARY KEY,
            choice_number INTEGER,
            choice_label TEXT,
            voted_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS user_rewards (
            user_id INTEGER PRIMARY KEY,
            reward_type TEXT,
            reward_label TEXT,
            reward_link TEXT,
            status TEXT,
            awarded_at TEXT,
            claimed_at TEXT,
            source TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_user_id INTEGER,
            target_user_id INTEGER,
            action_type TEXT,
            delivery TEXT,
            template_id INTEGER,
            reason TEXT,
            logged_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS member_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            display_name TEXT,
            event_type TEXT,
            detail TEXT,
            logged_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS flood_flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            display_name TEXT,
            message_count INTEGER,
            window_seconds INTEGER,
            message_excerpt TEXT,
            logged_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watchlist_keywords (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            term TEXT UNIQUE,
            category TEXT DEFAULT 'custom',
            severity TEXT DEFAULT 'medium',
            enabled INTEGER DEFAULT 1,
            added_at TEXT,
            added_by TEXT
        )
        """
    )
    conn.commit()


def fox_db_value(query: str, params=(), default=0):
    rows = fox_db_rows(query, params)
    if not rows:
        return default
    value = next(iter(rows[0].values()))
    return default if value is None else value


def period_start(period: str):
    now = datetime.datetime.utcnow()
    if period == "today":
        return datetime.datetime(now.year, now.month, now.day).isoformat()
    if period == "week":
        return (now - datetime.timedelta(days=7)).isoformat()
    return None


def since_clause(column: str, since: str | None):
    if not since:
        return "", ()
    return f" WHERE {column} >= ?", (since,)


def is_alcove_verified_user(user):
    return bool(str((user or {}).get("verified_at") or "").strip())


def get_verified_alcove_users():
    if synced_alcove_users:
        return [user for user in synced_alcove_users if is_alcove_verified_user(user)]

    rows = fox_db_rows(
        """
        SELECT
            p.user_id,
            COALESCE(p.username, '') AS username,
            COALESCE(p.display_name, '') AS display_name,
            COALESCE(p.first_name, '') AS first_name,
            COALESCE(p.last_name, '') AS last_name,
            COALESCE(p.first_seen, '') AS first_seen,
            COALESCE(p.last_seen, '') AS last_seen,
            COALESCE(p.verified_at, v.verified_at, '') AS verified_at,
            COALESCE(p.source, '') AS source,
            COALESCE(m.message_count, 0) AS message_count,
            COALESCE(l.link_count, 0) AS link_attempts,
            COALESCE(t.tone_count, 0) AS tone_flags,
            COALESCE(s.active_strikes, 0) AS active_strikes
        FROM user_profiles p
        LEFT JOIN verified_users v ON v.user_id = p.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS message_count
            FROM messages
            GROUP BY user_id
        ) m ON m.user_id = p.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS link_count
            FROM link_violations
            GROUP BY user_id
        ) l ON l.user_id = p.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS tone_count
            FROM tone_flags
            GROUP BY user_id
        ) t ON t.user_id = p.user_id
        LEFT JOIN (
            SELECT user_id, COUNT(*) AS active_strikes
            FROM user_strikes
            WHERE active = 1
            GROUP BY user_id
        ) s ON s.user_id = p.user_id
        ORDER BY lower(COALESCE(p.username, p.display_name, CAST(p.user_id AS TEXT)))
        """
    )

    users = []
    for row in rows:
        username = row.get("username") or ""
        display_name = row.get("display_name") or username or str(row.get("user_id"))
        users.append(
            {
                "user_id": row.get("user_id"),
                "username": username,
                "display_name": display_name,
                "label": f"@{username}" if username else display_name,
                "first_seen": row.get("first_seen") or None,
                "last_seen": row.get("last_seen") or None,
                "verified_at": row.get("verified_at") or None,
                "source": row.get("source") or None,
                "message_count": row.get("message_count") or 0,
                "link_attempts": row.get("link_attempts") or 0,
                "tone_flags": row.get("tone_flags") or 0,
                "active_strikes": row.get("active_strikes") or 0,
            }
        )

    return [user for user in users if is_alcove_verified_user(user)]


def find_verified_alcove_user(user_id=None, username=None):
    username = (username or "").lstrip("@").lower()
    for user in get_verified_alcove_users():
        if not is_alcove_verified_user(user):
            continue
        if user_id is not None and int(user.get("user_id") or 0) == int(user_id):
            return user
        if username and (user.get("username") or "").lower() == username:
            return user
    return None


def clean_username(username: str | None) -> str | None:
    cleaned = (username or "").strip().lstrip("@")
    return cleaned or None


def pulse_user_identity(user_id=None, username=None):
    user = find_verified_alcove_user(user_id, username)
    if user:
        return user
    if not user_id and not username:
        return None
    return {
        "user_id": user_id,
        "username": clean_username(username),
        "display_name": clean_username(username) or str(user_id or "Unknown"),
        "label": f"@{clean_username(username)}" if clean_username(username) else str(user_id or "Unknown"),
    }


def pulse_entries_for_day(day_key: str | None = None):
    day = day_key or pulse_day_key()
    return [entry for entry in pulse_entries if entry.get("day_key") == day]


def pulse_completed_today_count(day_key: str | None = None):
    return len([
        entry for entry in pulse_entries_for_day(day_key)
        if entry.get("status") == "completed"
    ])


def pulse_sent_today_count(day_key: str | None = None):
    return pulse_completed_today_count(day_key)


def pulse_base_green_slots(now: datetime.datetime | None = None):
    current = now or uk_now()
    interval = pulse_green_unlock_interval_hours()
    midnight = datetime.datetime.combine(current.date(), datetime.time.min, tzinfo=UK_TZ)
    elapsed_seconds = max(0, int((current - midnight).total_seconds()))
    unlocked = 1 + (elapsed_seconds // (interval * 3600))
    return max(1, min(PULSE_MAX_GREEN_SLOTS, unlocked))


def pulse_testing_unlimited() -> bool:
    return PULSE_TESTING_UNLIMITED


def pulse_unlimited_question_submit() -> bool:
    return PULSE_UNLIMITED_QUESTION_SUBMIT


def pulse_heat_unlocked(day_key: str | None = None):
    return pulse_sent_today_count(day_key) >= pulse_heat_threshold()


def pulse_red_unlocked_cycles(day_key: str | None = None) -> int:
    threshold = max(1, pulse_heat_threshold())
    return pulse_sent_today_count(day_key) // threshold


def pulse_user_red_answers_today(user_id=None, username=None, day_key: str | None = None) -> int:
    sent = pulse_user_sent_entries(user_id, username, day_key)
    return len([entry for entry in sent if (entry.get("pulse_type") or "green") == "red"])


def pulse_red_unlock_notify_usernames() -> set[str] | None:
    """Return allowlist of usernames, or None to notify everyone."""
    raw = PULSE_RED_UNLOCK_NOTIFY_USERNAMES_RAW
    if not raw or raw == "*":
        return None
    return {
        part.strip().lower().lstrip("@")
        for part in raw.split(",")
        if part.strip()
    }


def pulse_red_unlock_notify_allowed(user: dict) -> bool:
    allowlist = pulse_red_unlock_notify_usernames()
    if allowlist is None:
        return True
    username = (user.get("username") or "").strip().lower().lstrip("@")
    return username in allowlist


def queue_red_pulse_unlock_notifications(day_key: str, cycle_number: int):
    if not PULSE_RED_UNLOCK_DMS_ENABLED:
        return
    users = get_verified_alcove_users()
    if not users:
        return
    for user in users:
        if not pulse_red_unlock_notify_allowed(user):
            continue
        user_id = user.get("user_id")
        if not user_id:
            continue
        dedupe_key = f"{day_key}:{cycle_number}:{int(user_id)}"
        if any(
            item.get("dedupe_key") == dedupe_key
            for item in pulse_red_unlock_notifications
        ):
            continue
        pulse_red_unlock_notifications.append({
            "notification_id": f"red-unlock-{len(pulse_red_unlock_notifications) + 1}",
            "dedupe_key": dedupe_key,
            "kind": "red_pulse_active",
            "day_key": day_key,
            "cycle_number": cycle_number,
            "recipient_user_id": int(user_id),
            "recipient_username": user.get("username"),
            "recipient_display_name": user.get("display_name"),
            "created_at": now_iso(),
            "notified_at": None,
        })


def maybe_queue_red_pulse_unlock_notifications(day_key: str, previous_cycles: int, new_cycles: int):
    if new_cycles <= previous_cycles:
        return
    for cycle_number in range(previous_cycles + 1, new_cycles + 1):
        queue_red_pulse_unlock_notifications(day_key, cycle_number)


def pulse_user_sent_entries(user_id, username=None, day_key: str | None = None):
    day = day_key or pulse_day_key()
    uname = (username or "").lower().lstrip("@")
    rows = []
    for entry in pulse_entries_for_day(day):
        if user_id is not None and int(entry.get("sender_user_id") or 0) == int(user_id):
            rows.append(entry)
        elif uname and (entry.get("sender_username") or "").lower() == uname:
            rows.append(entry)
    return rows


def pulse_red_community_answers_payload(day_key: str | None = None, identity: dict | None = None):
    day = day_key or pulse_day_key()
    question = pulse_red_daily_question().strip()
    rows = []
    for entry in pulse_entries_for_day(day):
        if (entry.get("pulse_type") or "").lower() != "red":
            continue
        if (entry.get("question") or "").strip() != question:
            continue
        answer = (entry.get("response_answer") or entry.get("answer") or entry.get("sender_note") or "").strip()
        if not answer:
            continue
        owner = {
            "user_id": entry.get("sender_user_id") or entry.get("responder_user_id"),
            "username": entry.get("sender_username") or entry.get("responder_username"),
        }
        rows.append({
            "answer": answer,
            "received_at": entry.get("responded_at") or entry.get("sent_at"),
            "is_mine": bool(identity and pulse_identities_match(identity, owner)),
        })
    rows.sort(key=lambda row: row.get("received_at") or "")
    return rows


def pulse_red_activations_for_user(user_id=None, username=None, day_key: str | None = None):
    day = day_key or pulse_day_key()
    uname = (username or "").lower().lstrip("@")
    matches = []
    for entry in pulse_red_activations:
        if entry.get("day_key") != day:
            continue
        if user_id is not None and int(entry.get("user_id") or 0) == int(user_id):
            matches.append(entry)
        elif uname and (entry.get("username") or "").lower() == uname:
            matches.append(entry)
    return matches


def pulse_red_activation_for_user(user_id=None, username=None, day_key: str | None = None, cycle_number: int | None = None):
    for entry in pulse_red_activations_for_user(user_id, username, day_key):
        if cycle_number is None or int(entry.get("cycle_number") or 0) == int(cycle_number):
            return entry
    return None


def derive_video_title_from_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        slug = (parsed.path or "").rstrip("/").split("/")[-1]
        if not slug:
            return None
        slug = unquote(slug)
        slug = re.sub(r"\.[a-z0-9]{2,5}$", "", slug, flags=re.IGNORECASE)
        slug = re.sub(r"^[0-9]+[._-]*", "", slug)
        slug = re.sub(r"^(watch|video|videos|clip|clips)[-_ ]+", "", slug, flags=re.IGNORECASE)
        slug = re.sub(r"[-_]+", " ", slug)
        slug = re.sub(r"\b(cockdude|seancody|gayporn|porn)\b", "", slug, flags=re.IGNORECASE)
        slug = re.sub(r"\s+", " ", slug).strip()
        if not slug:
            return None
        return slug[:120].title()
    except Exception:
        return None


def pulse_red_is_activated(user_id=None, username=None, day_key: str | None = None) -> bool:
    return pulse_red_activation_for_user(user_id, username, day_key) is not None


def activate_red_pulse_for_user(identity: dict, day_key: str | None = None):
    day = day_key or pulse_day_key()
    cycle_number = pulse_red_unlocked_cycles(day)
    existing = pulse_red_activation_for_user(identity.get("user_id"), identity.get("username"), day, cycle_number)
    if existing:
        return existing
    entry = {
        "id": len(pulse_red_activations) + 1,
        "day_key": day,
        "cycle_number": cycle_number,
        "user_id": identity.get("user_id"),
        "username": identity.get("username"),
        "display_name": identity.get("display_name") or identity.get("label"),
        "activated_at": now_iso(),
    }
    pulse_red_activations.append(entry)
    save_runtime_state()
    return entry


def pulse_slot_state(user_id=None, username=None, now: datetime.datetime | None = None):
    current = now or uk_now()
    day = pulse_day_key(current)
    sent = pulse_user_sent_entries(user_id, username, day)
    green_used = len([entry for entry in sent if entry.get("pulse_type") == "green"])
    red_used = pulse_user_red_answers_today(user_id, username, day)
    green_total = pulse_base_green_slots(current)
    sent_today = pulse_sent_today_count(day)
    threshold = pulse_heat_threshold()
    testing = pulse_testing_unlimited()
    green_interval_hours = pulse_green_unlock_interval_hours()
    unlocked_cycles = pulse_red_unlocked_cycles(day)
    community_red_unlocked = unlocked_cycles > 0
    red_available = max(0, unlocked_cycles - red_used)
    red_ready = red_available > 0
    red_unlocked = community_red_unlocked
    red_activated = community_red_unlocked and red_available > 0
    remainder = sent_today % threshold if threshold else 0
    if community_red_unlocked:
        cycle_completed = threshold
    else:
        cycle_completed = min(sent_today, threshold)
    remaining_today = 0 if community_red_unlocked else max(threshold - min(cycle_completed, threshold), 0)
    green_available = 99 if testing else max(green_total - green_used, 0)
    return {
        "day_key": day,
        "day_label": pulse_day_label(day),
        "green_total": green_total,
        "green_max": PULSE_MAX_GREEN_SLOTS,
        "green_used": green_used,
        "green_available": green_available,
        "red_unlocked": red_unlocked,
        "red_ready": red_ready,
        "red_activated": red_activated,
        "red_used": red_used,
        "red_available": red_available,
        "red_unlocked_cycles": unlocked_cycles,
        "red_activated_cycles": red_used,
        "community_red_unlocked": community_red_unlocked,
        "sent_today": sent_today,
        "heat_threshold": threshold,
        "reset_interval_hours": green_interval_hours,
        "cycle_completed": min(cycle_completed, threshold),
        "remaining_today": remaining_today,
        "next_green_unlock_at": pulse_unlock_label(current, green_interval_hours),
        "next_unlock_at": next_pulse_unlock_at(current, green_interval_hours).isoformat(),
        "reset_seconds": seconds_until_next_pulse_unlock(current, green_interval_hours),
        "testing_unlimited": testing,
    }


def public_pulse_payload(entry):
    if not entry:
        return None
    return {
        "pulse_id": entry.get("id"),
        "pulse_type": entry.get("pulse_type"),
        "category": entry.get("category") or pulse_question_category(entry.get("question"), entry.get("pulse_type")),
        "question": entry.get("question"),
        "sender_note": entry.get("sender_note") or entry.get("answer"),
        "answer": entry.get("response_answer") or entry.get("sender_note") or entry.get("answer"),
        "status": entry.get("status"),
        "sent_at": entry.get("sent_at"),
        "delivered_at": entry.get("delivered_at"),
        "responded_at": entry.get("responded_at"),
        "day_key": entry.get("day_key"),
        "delivery_mode": entry.get("delivery_mode") or "chain",
        "question_owner_user_id": entry.get("question_owner_user_id"),
        "question_owner_username": entry.get("question_owner_username"),
        "question_owner_display_name": entry.get("question_owner_display_name"),
        "sender_user_id": entry.get("sender_user_id"),
        "sender_username": entry.get("sender_username"),
        "sender_display_name": entry.get("sender_display_name"),
        "responder_user_id": entry.get("responder_user_id"),
        "responder_username": entry.get("responder_username"),
        "responder_display_name": entry.get("responder_display_name"),
    }


def pulse_assignments_for_user(user_id=None, username=None):
    uname = (username or "").lower().lstrip("@")
    rows = []
    for entry in pulse_entries:
        if entry.get("status") != "awaiting_response":
            continue
        if not iso_has_passed(entry.get("assignment_notify_after")):
            continue
        if user_id is not None and int(entry.get("delivered_to_user_id") or 0) == int(user_id):
            rows.append(entry)
        elif uname and (entry.get("delivered_to_username") or "").lower() == uname:
            rows.append(entry)
    return rows


def pulse_responded_by_user(user_id=None, username=None):
    uname = (username or "").lower().lstrip("@")
    rows = []
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if user_id is not None and int(entry.get("responder_user_id") or 0) == int(user_id):
            rows.append(entry)
        elif uname and (entry.get("responder_username") or "").lower() == uname:
            rows.append(entry)
    return rows


def pulse_receipts_for_user(user_id=None, username=None):
    uname = (username or "").lower().lstrip("@")
    rows = []
    for receipt in pulse_receipts:
        if not receipt.get("acknowledged_at") and not iso_has_passed(receipt.get("notify_after")):
            continue
        if user_id is not None and int(receipt.get("recipient_user_id") or 0) == int(user_id):
            rows.append(receipt)
        elif uname and (receipt.get("recipient_username") or "").lower() == uname:
            rows.append(receipt)
    return rows


def pulse_receipt_payload(receipt):
    entry = next((item for item in pulse_entries if item.get("id") == receipt.get("pulse_id")), None)
    payload = public_pulse_payload(entry)
    if not payload:
        return None
    payload.update({
        "receipt_id": receipt.get("id"),
        "received_at": receipt.get("received_at"),
        "acknowledged_at": receipt.get("acknowledged_at"),
        "notified_at": receipt.get("notified_at"),
        "notify_after": receipt.get("notify_after"),
    })
    return payload


def pulse_match_next_receiver(receiver, exclude_entry_id=None):
    day = pulse_day_key()
    receiver_id = receiver.get("user_id")
    receiver_username = (receiver.get("username") or "").lower()
    for entry in pulse_entries:
        if exclude_entry_id is not None and int(entry.get("id") or 0) == int(exclude_entry_id):
            continue
        if entry.get("day_key") != day or entry.get("status") != "queued":
            continue
        if receiver_id is not None and int(entry.get("sender_user_id") or 0) == int(receiver_id):
            continue
        if receiver_username and (entry.get("sender_username") or "").lower() == receiver_username:
            continue
        entry["status"] = "awaiting_response"
        entry["delivered_to_user_id"] = receiver.get("user_id")
        entry["delivered_to_username"] = receiver.get("username")
        entry["delivered_to_display_name"] = receiver.get("display_name") or receiver.get("label")
        entry["delivered_at"] = now_iso()
        entry["assignment_notified_at"] = None
        entry["assignment_notify_after"] = pulse_notification_due_at()
        save_runtime_state()
        return entry
    return None


def pulse_daily_summary_state(day_key: str, category: str):
    for entry in pulse_daily_summary_posts:
        if entry.get("day_key") == day_key and entry.get("category") == category:
            return entry
    state = {
        "day_key": day_key,
        "category": category,
        "admin_posted_at": None,
        "published_at": None,
    }
    pulse_daily_summary_posts.append(state)
    save_runtime_state()
    return state


def pulse_completed_admin_entries():
    rows = []
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if entry.get("admin_posted_at"):
            continue
        rows.append(public_pulse_payload(entry))
    return rows


def pulse_daily_summary_payload(day_key: str):
    completed = [
        entry for entry in pulse_entries
        if entry.get("status") == "completed" and entry.get("day_key") == day_key
    ]
    grouped = {}
    for entry in completed:
        category = entry.get("category") or pulse_question_category(entry.get("question"), entry.get("pulse_type"))
        grouped.setdefault(category, []).append(public_pulse_payload(entry))
    summaries = []
    for category, entries in grouped.items():
        state = pulse_daily_summary_state(day_key, category)
        summaries.append({
            "day_key": day_key,
            "day_label": pulse_day_label(day_key),
            "category": category,
            "entries": entries,
            "admin_posted_at": state.get("admin_posted_at"),
            "published_at": state.get("published_at"),
        })
    return summaries


def pulse_category_from_slug(category_slug: str) -> str:
    slug = (category_slug or "").strip().lower()
    mapping = {
        "mental-health": "Mental health",
        "physical-health": "Physical health",
        "general": "General",
    }
    return mapping.get(slug, "General")


def spotlight_today_exists(nominator_user_id=None, nominator_username=None):
    today = pulse_day_key()
    nominator_username = (nominator_username or "").lower()
    for entry in spotlight_entries:
        if entry.get("day_key") != today and not str(entry.get("time", "")).startswith(today):
            continue
        if nominator_user_id and entry.get("nominator_user_id") == nominator_user_id:
            return True
        if nominator_username and (entry.get("nominator_username") or "").lower() == nominator_username:
            return True
    return False


def next_spotlight_id() -> int:
    return max((int(entry.get("id") or 0) for entry in spotlight_entries), default=0) + 1


def get_spotlight_entry(entry_id: int):
    for entry in spotlight_entries:
        if int(entry.get("id") or 0) == int(entry_id):
            return entry
    return None


def spotlight_nominations_for_user(user_id=None, username=None):
    """Spotlight nominations submitted by this resident (nominator), newest first."""
    viewer = archive_viewer_identity(user_id, username)
    nominations = []
    for entry in spotlight_entries:
        nominator = {
            "user_id": entry.get("nominator_user_id"),
            "username": entry.get("nominator_username"),
        }
        if not pulse_identities_match(viewer, nominator):
            continue
        sid = entry.get("id")
        if sid is None:
            continue
        reason = (entry.get("edited_reason") or entry.get("reason") or "").strip()
        nominations.append(
            {
                "spotlight_id": sid,
                "status": entry.get("status"),
                "published_at": entry.get("published_at"),
                "day_key": entry.get("day_key"),
                "style": entry.get("style"),
                "nominee_display_name": entry.get("nominee_display_name") or entry.get("nominee_username"),
                "nominator_display_name": entry.get("nominator_display_name") or entry.get("nominator_username"),
                "reason": reason,
            }
        )
    nominations.sort(key=lambda row: int(row.get("spotlight_id") or 0), reverse=True)
    return nominations


def spotlight_awards_for_user(user_id=None, username=None):
    """Published Spotlight awards for this resident (nominee), newest first."""
    viewer = archive_viewer_identity(user_id, username)
    awards = []
    seen = set()

    try:
        archived = query_spotlight_archive(
            mine="awarded",
            viewer_user_id=viewer.get("user_id"),
            viewer_username=viewer.get("username"),
            page=1,
            limit=50,
            sort="date_desc",
        )
    except Exception:
        archived = {"entries": []}

    for entry in archived.get("entries") or []:
        sid = entry.get("spotlight_id")
        if sid is None or sid in seen:
            continue
        seen.add(sid)
        awards.append(
            {
                "spotlight_id": sid,
                "published_at": entry.get("published_at"),
                "style": entry.get("style"),
                "day_key": entry.get("day_key"),
                "day_label": entry.get("day_label"),
                "nominee_display_name": entry.get("nominee_display_name") or entry.get("nominee_username"),
                "nominator_display_name": entry.get("nominator_display_name") or entry.get("nominator_username"),
                "reason": (entry.get("reason") or "").strip(),
                "status": "approved",
            }
        )

    # Also include live published entries (covers brief window before archive sync).
    for entry in spotlight_entries:
        if entry.get("status") != "approved" or not entry.get("published_at"):
            continue
        nominee = {
            "user_id": entry.get("nominee_user_id"),
            "username": entry.get("nominee_username"),
        }
        if not pulse_identities_match(viewer, nominee):
            continue
        sid = entry.get("id")
        if sid is None or sid in seen:
            continue
        seen.add(sid)
        reason = (entry.get("edited_reason") or entry.get("reason") or "").strip()
        awards.append(
            {
                "spotlight_id": sid,
                "published_at": entry.get("published_at"),
                "style": entry.get("style"),
                "day_key": entry.get("day_key"),
                "nominee_display_name": entry.get("nominee_display_name") or entry.get("nominee_username"),
                "nominator_display_name": entry.get("nominator_display_name") or entry.get("nominator_username"),
                "reason": reason,
                "status": "approved",
            }
        )

    awards.sort(key=lambda row: str(row.get("published_at") or ""), reverse=True)
    return awards


def spotlight_status_payload(nominator_user_id=None, nominator_username=None):
    submitted = spotlight_today_exists(nominator_user_id, nominator_username)
    return {
        "submitted_today": submitted,
        "reset_seconds": seconds_until_next_uk_midnight(),
        "reset_label": "midnight UK time",
        "awards": spotlight_awards_for_user(nominator_user_id, nominator_username),
        "nominations": spotlight_nominations_for_user(nominator_user_id, nominator_username),
    }


def build_alcove_analytics(period: str):
    if synced_alcove_analytics and period in synced_alcove_analytics:
        return synced_alcove_analytics[period]

    since = period_start(period)
    message_where, message_params = since_clause("timestamp", since)
    verified_where, verified_params = since_clause("verified_at", since)
    link_where, link_params = since_clause("logged_at", since)
    captcha_where, captcha_params = since_clause("attempt_time", since)

    if since:
        spotlight_count = len([
            entry for entry in spotlight_entries
            if entry.get("time", "") >= since
        ])
    else:
        spotlight_count = len(spotlight_entries)

    return {
        "newResidents": fox_db_value(f"SELECT COUNT(*) FROM verified_users{verified_where}", verified_params),
        "totalResidents": len(get_verified_alcove_users()),
        "posts": fox_db_value(f"SELECT COUNT(*) FROM messages{message_where}", message_params),
        "replies": 0,
        "reactions": 0,
        "botBlocked": fox_db_value(
            f"SELECT COUNT(*) FROM captcha_attempts{captcha_where}" + (" AND success = 0" if captcha_where else " WHERE success = 0"),
            captcha_params,
        ),
        "linksRemoved": fox_db_value(f"SELECT COUNT(*) FROM link_violations{link_where}", link_params),
        "spotlights": spotlight_count,
        "pulses": 0,
        "videosPlayed": len([entry for entry in archived_wheel_entries if not since or entry.get("played_at", entry.get("archived_at", "")) >= since]),
        "storiesActed": len([entry for entry in story_entries if not since or entry.get("time", "") >= since]),
        "audioSessions": len([entry for entry in asmr_entries if not since or entry.get("time", "") >= since]),
    }


def group_activity_period(period: str) -> str:
    allowed = {"today", "week", "all", "allTime"}
    normalized = (period or "today").strip()
    if normalized == "allTime":
        normalized = "all"
    if normalized not in allowed:
        raise HTTPException(status_code=400, detail="period must be today, week, or all")
    return normalized


def group_activity_since(period: str) -> str | None:
    normalized = group_activity_period(period)
    if normalized == "all":
        return None
    return period_start(normalized)


def format_group_user_ref(user_id, username="", display_name=""):
    username = (username or "").strip()
    display_name = (display_name or "").strip()
    if username:
        label = f"@{username.lstrip('@')}"
    elif display_name:
        label = display_name
    else:
        label = str(user_id or "unknown")
    return {
        "user_id": user_id,
        "username": username.lstrip("@") if username else "",
        "display_name": display_name,
        "label": label,
    }


def group_activity_count_rows(rows, label_key="label", count_key="count"):
    formatted = []
    for row in rows:
        if isinstance(row, dict):
            user = format_group_user_ref(
                row.get("user_id"),
                row.get("username"),
                row.get("display_name"),
            )
            formatted.append({**user, "count": row.get(count_key) or 0})
            continue
        user_id, username, display_name, count = row
        formatted.append({**format_group_user_ref(user_id, username, display_name), "count": count})
    return formatted


def build_group_activity_summary(period: str):
    since = group_activity_since(period)
    message_where, message_params = since_clause("timestamp", since)
    verified_where, verified_params = since_clause("verified_at", since)
    link_where, link_params = since_clause("logged_at", since)
    tone_where, tone_params = since_clause("logged_at", since)
    action_where, action_params = since_clause("logged_at", since)
    captcha_where, captcha_params = since_clause("attempt_time", since)

    tone_alert_where = tone_where + (" AND" if tone_where else " WHERE") + " severity IN ('medium', 'high')"
    failed_captcha_where = captcha_where + (" AND" if captcha_where else " WHERE") + " success = 0"

    top_posters = fox_db_rows(
        f"""
        SELECT m.user_id, COALESCE(p.username, ''), COALESCE(p.display_name, ''), COUNT(*) AS count
        FROM messages m
        LEFT JOIN user_profiles p ON p.user_id = m.user_id
        {message_where}{' AND' if message_where else ' WHERE'} m.user_id IS NOT NULL
        GROUP BY m.user_id, p.username, p.display_name
        ORDER BY count DESC
        LIMIT 8
        """,
        message_params,
    )
    top_tone = fox_db_rows(
        f"""
        SELECT user_id, COALESCE(username, ''), COALESCE(display_name, ''), COUNT(*) AS count
        FROM tone_flags
        {tone_where}
        GROUP BY user_id, username, display_name
        ORDER BY count DESC
        LIMIT 8
        """,
        tone_params,
    )
    top_links = fox_db_rows(
        f"""
        SELECT user_id, COALESCE(username, ''), COALESCE(display_name, ''), COUNT(*) AS count
        FROM link_violations
        {link_where}
        GROUP BY user_id, username, display_name
        ORDER BY count DESC
        LIMIT 8
        """,
        link_params,
    )
    users_needing_attention = fox_db_rows(
        """
        SELECT s.user_id, COALESCE(p.username, ''), COALESCE(p.display_name, ''), COUNT(*) AS count
        FROM user_strikes s
        LEFT JOIN user_profiles p ON p.user_id = s.user_id
        WHERE s.active = 1
        GROUP BY s.user_id, p.username, p.display_name
        HAVING count >= 2
        ORDER BY count DESC
        LIMIT 12
        """
    )
    action_breakdown = fox_db_rows(
        f"""
        SELECT action_type, COUNT(*) AS count
        FROM admin_actions
        {action_where}
        GROUP BY action_type
        ORDER BY count DESC
        """,
        action_params,
    )

    return {
        "period": group_activity_period(period),
        "since": since,
        "generated_at": now_iso(),
        "source": "bot_sync" if synced_alcove_analytics else "fox_logs",
        "db_available": os.path.exists(FOX_LOGS_DB_PATH),
        "last_bot_sync_at": last_bot_sync_at,
        "overview": {
            "newResidents": fox_db_value(f"SELECT COUNT(*) FROM verified_users{verified_where}", verified_params),
            "totalResidents": len(get_verified_alcove_users()),
            "messages": fox_db_value(f"SELECT COUNT(*) FROM messages{message_where}", message_params),
            "activeUsers": fox_db_value(
                f"SELECT COUNT(DISTINCT user_id) FROM messages{message_where}"
                + (" AND" if message_where else " WHERE")
                + " user_id IS NOT NULL",
                message_params,
            ),
            "linkAttempts": fox_db_value(f"SELECT COUNT(*) FROM link_violations{link_where}", link_params),
            "toneFlags": fox_db_value(f"SELECT COUNT(*) FROM tone_flags{tone_where}", tone_params),
            "toneAlerts": fox_db_value(tone_alert_where, tone_params),
            "failedCaptcha": fox_db_value(failed_captcha_where, captcha_params),
            "floodAlerts": fox_db_value(f"SELECT COUNT(*) FROM flood_flags{link_where}", link_params),
            "memberEvents": fox_db_value(f"SELECT COUNT(*) FROM member_events{link_where}", link_params),
            "warnings": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'warning'",
                action_params,
            ),
            "mutes": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'mute'",
                action_params,
            ),
            "strikesAdded": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'strike_add'",
                action_params,
            ),
            "strikesRemoved": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'strike_remove'",
                action_params,
            ),
            "bansApproved": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'ban_approved'",
                action_params,
            ),
            "bansRejected": fox_db_value(
                f"SELECT COUNT(*) FROM admin_actions{action_where}"
                + (" AND" if action_where else " WHERE")
                + " action_type = 'ban_rejected'",
                action_params,
            ),
        },
        "topPosters": group_activity_count_rows(top_posters),
        "topToneFlags": group_activity_count_rows(top_tone),
        "topLinkAttempts": group_activity_count_rows(top_links),
        "usersNeedingAttention": group_activity_count_rows(users_needing_attention),
        "actionBreakdown": [
            {"action_type": row.get("action_type") or "unknown", "count": row.get("count") or 0}
            for row in action_breakdown
        ],
    }


def build_group_activity_users(period: str, sort: str = "risk", limit: int = 50):
    since = group_activity_since(period)
    limit = max(1, min(int(limit or 50), 200))
    users = get_verified_alcove_users()

    message_counts = {
        row["user_id"]: row.get("count") or 0
        for row in fox_db_rows(
            f"""
            SELECT user_id, COUNT(*) AS count
            FROM messages
            {since_clause("timestamp", since)[0]}
            GROUP BY user_id
            """,
            since_clause("timestamp", since)[1],
        )
    }
    link_counts = {
        row["user_id"]: row.get("count") or 0
        for row in fox_db_rows(
            f"""
            SELECT user_id, COUNT(*) AS count
            FROM link_violations
            {since_clause("logged_at", since)[0]}
            GROUP BY user_id
            """,
            since_clause("logged_at", since)[1],
        )
    }
    tone_counts = {
        row["user_id"]: row.get("count") or 0
        for row in fox_db_rows(
            f"""
            SELECT user_id, COUNT(*) AS count
            FROM tone_flags
            {since_clause("logged_at", since)[0]}
            GROUP BY user_id
            """,
            since_clause("logged_at", since)[1],
        )
    }

    enriched = []
    for user in users:
        user_id = user.get("user_id")
        period_messages = message_counts.get(user_id, 0)
        period_links = link_counts.get(user_id, 0)
        period_tone = tone_counts.get(user_id, 0)
        active_strikes = user.get("active_strikes") or 0
        risk_score = (active_strikes * 5) + (period_links * 2) + period_tone
        enriched.append(
            {
                **user,
                "period_messages": period_messages,
                "period_link_attempts": period_links,
                "period_tone_flags": period_tone,
                "risk_score": risk_score,
                "needs_attention": active_strikes >= 2 or period_links >= 3 or period_tone >= 3,
            }
        )

    sort_key = (sort or "risk").strip().lower()
    sort_map = {
        "risk": lambda item: (item["risk_score"], item["period_messages"]),
        "messages": lambda item: item["period_messages"],
        "links": lambda item: item["period_link_attempts"],
        "tone": lambda item: item["period_tone_flags"],
        "strikes": lambda item: item["active_strikes"],
    }
    enriched.sort(key=sort_map.get(sort_key, sort_map["risk"]), reverse=True)
    return {
        "period": group_activity_period(period),
        "since": since,
        "sort": sort_key,
        "users": enriched[:limit],
    }


def build_group_activity_violations(violation_type: str, period: str, limit: int = 40):
    since = group_activity_since(period)
    limit = max(1, min(int(limit or 40), 200))
    where, params = since_clause("logged_at", since)

    if violation_type == "links":
        rows = fox_db_rows(
            f"""
            SELECT id, message_id, user_id, username, display_name, message_excerpt, link_samples, logged_at
            FROM link_violations
            {where}
            ORDER BY logged_at DESC
            LIMIT ?
            """,
            params + (limit,),
        )
    elif violation_type == "tone":
        rows = fox_db_rows(
            f"""
            SELECT id, message_id, user_id, username, display_name, categories, severity, score,
                   matched_terms, message_excerpt, logged_at
            FROM tone_flags
            {where}
            ORDER BY logged_at DESC
            LIMIT ?
            """,
            params + (limit,),
        )
    else:
        raise HTTPException(status_code=400, detail="type must be links or tone")

    for row in rows:
        user = format_group_user_ref(row.get("user_id"), row.get("username"), row.get("display_name"))
        row.update(user)
    return {
        "period": group_activity_period(period),
        "since": since,
        "type": violation_type,
        "items": rows,
    }


def build_group_activity_actions(period: str, limit: int = 40):
    since = group_activity_since(period)
    limit = max(1, min(int(limit or 40), 200))
    where, params = since_clause("logged_at", since)
    rows = fox_db_rows(
        f"""
        SELECT id, admin_user_id, target_user_id, action_type, delivery, template_id, reason, logged_at
        FROM admin_actions
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (limit,),
    )
    return {
        "period": group_activity_period(period),
        "since": since,
        "items": rows,
    }


def build_group_activity_user_detail(user_id: int):
    profile_rows = fox_db_rows(
        """
        SELECT user_id, username, first_name, last_name, display_name, first_seen, last_seen, verified_at, source
        FROM user_profiles
        WHERE user_id = ?
        LIMIT 1
        """,
        (user_id,),
    )
    profile = profile_rows[0] if profile_rows else {"user_id": user_id}
    user = format_group_user_ref(
        profile.get("user_id", user_id),
        profile.get("username"),
        profile.get("display_name"),
    )

    return {
        **user,
        "first_seen": profile.get("first_seen"),
        "last_seen": profile.get("last_seen"),
        "verified_at": profile.get("verified_at"),
        "source": profile.get("source"),
        "message_count": fox_db_value("SELECT COUNT(*) FROM messages WHERE user_id = ?", (user_id,)),
        "link_attempts": fox_db_value("SELECT COUNT(*) FROM link_violations WHERE user_id = ?", (user_id,)),
        "tone_flags": fox_db_value("SELECT COUNT(*) FROM tone_flags WHERE user_id = ?", (user_id,)),
        "high_tone_flags": fox_db_value(
            "SELECT COUNT(*) FROM tone_flags WHERE user_id = ? AND severity = 'high'",
            (user_id,),
        ),
        "active_strikes": fox_db_value(
            "SELECT COUNT(*) FROM user_strikes WHERE user_id = ? AND active = 1",
            (user_id,),
        ),
        "total_strikes": fox_db_value("SELECT COUNT(*) FROM user_strikes WHERE user_id = ?", (user_id,)),
        "recent_strikes": fox_db_rows(
            """
            SELECT active, reason, created_at, admin_user_id, removed_at, removed_by
            FROM user_strikes
            WHERE user_id = ?
            ORDER BY created_at DESC
            LIMIT 8
            """,
            (user_id,),
        ),
        "recent_tone_flags": fox_db_rows(
            """
            SELECT categories, severity, score, matched_terms, message_excerpt, logged_at
            FROM tone_flags
            WHERE user_id = ?
            ORDER BY logged_at DESC
            LIMIT 6
            """,
            (user_id,),
        ),
        "recent_link_attempts": fox_db_rows(
            """
            SELECT link_samples, message_excerpt, logged_at
            FROM link_violations
            WHERE user_id = ?
            ORDER BY logged_at DESC
            LIMIT 6
            """,
            (user_id,),
        ),
        "recent_admin_actions": fox_db_rows(
            """
            SELECT admin_user_id, action_type, delivery, reason, logged_at
            FROM admin_actions
            WHERE target_user_id = ?
            ORDER BY logged_at DESC
            LIMIT 8
            """,
            (user_id,),
        ),
    }


def build_group_activity_sheets_feed(period: str):
    summary = build_group_activity_summary(period)
    users = build_group_activity_users(period, sort="risk", limit=500)
    links = build_group_activity_violations("links", period, limit=2000)
    tone = build_group_activity_violations("tone", period, limit=2000)
    actions = build_group_activity_actions(period, limit=2000)
    return {
        "generated_at": now_iso(),
        "period": summary.get("period"),
        "since": summary.get("since"),
        "source": summary.get("source"),
        "db_available": summary.get("db_available"),
        "last_bot_sync_at": summary.get("last_bot_sync_at"),
        "overview": summary.get("overview") or {},
        "top_posters": summary.get("topPosters") or [],
        "top_tone_flags": summary.get("topToneFlags") or [],
        "top_link_attempts": summary.get("topLinkAttempts") or [],
        "users_needing_attention": summary.get("usersNeedingAttention") or [],
        "action_breakdown": summary.get("actionBreakdown") or [],
        "users": users.get("users") or [],
        "link_alerts": links.get("items") or [],
        "tone_flags": tone.get("items") or [],
        "admin_actions": actions.get("items") or [],
        "member_events": build_group_activity_member_events(period, limit=200).get("items") or [],
        "flood_flags": build_group_activity_flood_flags(period, limit=200).get("items") or [],
    }


def _csv_from_rows(fieldnames: list[str], rows: list[dict]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return buffer.getvalue()


def build_group_activity_csv_files(period: str) -> dict[str, str]:
    feed = build_group_activity_sheets_feed(period)
    overview = feed.get("overview") or {}
    overview_rows = [
        {"metric": key, "value": value}
        for key, value in overview.items()
    ]
    overview_rows.extend([
        {"metric": "generated_at", "value": feed.get("generated_at")},
        {"metric": "period", "value": feed.get("period")},
        {"metric": "since", "value": feed.get("since")},
        {"metric": "source", "value": feed.get("source")},
        {"metric": "last_bot_sync_at", "value": feed.get("last_bot_sync_at")},
    ])

    user_rows = [
        {
            "user_id": row.get("user_id"),
            "username": row.get("username"),
            "display_name": row.get("display_name"),
            "label": row.get("label"),
            "period_messages": row.get("period_messages"),
            "period_link_attempts": row.get("period_link_attempts"),
            "period_tone_flags": row.get("period_tone_flags"),
            "active_strikes": row.get("active_strikes"),
            "message_count": row.get("message_count"),
            "link_attempts": row.get("link_attempts"),
            "tone_flags": row.get("tone_flags"),
            "risk_score": row.get("risk_score"),
            "needs_attention": row.get("needs_attention"),
            "first_seen": row.get("first_seen"),
            "last_seen": row.get("last_seen"),
            "verified_at": row.get("verified_at"),
        }
        for row in feed.get("users") or []
    ]

    link_rows = [
        {
            "logged_at": row.get("logged_at"),
            "user_id": row.get("user_id"),
            "username": row.get("username"),
            "display_name": row.get("display_name"),
            "label": row.get("label"),
            "link_samples": row.get("link_samples"),
            "message_excerpt": row.get("message_excerpt"),
            "message_id": row.get("message_id"),
        }
        for row in feed.get("link_alerts") or []
    ]

    tone_rows = [
        {
            "logged_at": row.get("logged_at"),
            "user_id": row.get("user_id"),
            "username": row.get("username"),
            "display_name": row.get("display_name"),
            "label": row.get("label"),
            "severity": row.get("severity"),
            "score": row.get("score"),
            "categories": row.get("categories"),
            "matched_terms": row.get("matched_terms"),
            "message_excerpt": row.get("message_excerpt"),
            "message_id": row.get("message_id"),
        }
        for row in feed.get("tone_flags") or []
    ]

    action_rows = [
        {
            "logged_at": row.get("logged_at"),
            "action_type": row.get("action_type"),
            "target_user_id": row.get("target_user_id"),
            "admin_user_id": row.get("admin_user_id"),
            "delivery": row.get("delivery"),
            "reason": row.get("reason"),
            "template_id": row.get("template_id"),
        }
        for row in feed.get("admin_actions") or []
    ]

    rank_rows = []
    for section, rows in (
        ("top_posters", feed.get("top_posters") or []),
        ("top_tone_flags", feed.get("top_tone_flags") or []),
        ("top_link_attempts", feed.get("top_link_attempts") or []),
        ("users_needing_attention", feed.get("users_needing_attention") or []),
    ):
        for row in rows:
            rank_rows.append(
                {
                    "section": section,
                    "user_id": row.get("user_id"),
                    "username": row.get("username"),
                    "display_name": row.get("display_name"),
                    "label": row.get("label"),
                    "count": row.get("count"),
                }
            )

    return {
        "overview.csv": _csv_from_rows(["metric", "value"], overview_rows),
        "users.csv": _csv_from_rows(
            [
                "user_id", "username", "display_name", "label", "risk_score", "needs_attention",
                "period_messages", "period_link_attempts", "period_tone_flags", "active_strikes",
                "message_count", "link_attempts", "tone_flags", "first_seen", "last_seen", "verified_at",
            ],
            user_rows,
        ),
        "link_alerts.csv": _csv_from_rows(
            ["logged_at", "user_id", "username", "display_name", "label", "link_samples", "message_excerpt", "message_id"],
            link_rows,
        ),
        "tone_flags.csv": _csv_from_rows(
            [
                "logged_at", "user_id", "username", "display_name", "label", "severity", "score",
                "categories", "matched_terms", "message_excerpt", "message_id",
            ],
            tone_rows,
        ),
        "admin_actions.csv": _csv_from_rows(
            ["logged_at", "action_type", "target_user_id", "admin_user_id", "delivery", "reason", "template_id"],
            action_rows,
        ),
        "rankings.csv": _csv_from_rows(
            ["section", "user_id", "username", "display_name", "label", "count"],
            rank_rows,
        ),
    }


def build_group_activity_csv(period: str, view: str = "users") -> str:
    files = build_group_activity_csv_files(period)
    view_map = {
        "overview": "overview.csv",
        "users": "users.csv",
        "links": "link_alerts.csv",
        "tone": "tone_flags.csv",
        "actions": "admin_actions.csv",
        "rankings": "rankings.csv",
    }
    filename = view_map.get((view or "users").strip().lower())
    if not filename:
        raise HTTPException(status_code=400, detail="view must be overview, users, links, tone, actions, rankings, or full")
    return files[filename]


def build_group_activity_export_zip(period: str) -> bytes:
    files = build_group_activity_csv_files(period)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def build_safety_action_queue(period: str = "today"):
    since = group_activity_since(period)
    summary = build_group_activity_summary(period)
    overview = summary.get("overview") or {}
    items = []

    def add_item(kind, title, count, severity, hint=""):
        if not count:
            return
        items.append(
            {
                "kind": kind,
                "title": title,
                "count": count,
                "severity": severity,
                "hint": hint,
            }
        )

    add_item("flood", "Flood alerts", overview.get("floodAlerts", 0), "high", "Users posting too fast")
    add_item("links", "Link violations", overview.get("linkAttempts", 0), "high", "Review link alerts tab")
    add_item("tone", "Tone alerts", overview.get("toneAlerts", 0), "medium", "Review tone flags tab")
    add_item("strikes", "Users at 2+ strikes", len(summary.get("usersNeedingAttention") or []), "high", "Review user profiles")
    add_item("captcha", "Failed verifications", overview.get("failedCaptcha", 0), "medium", "Check join verification flow")

    link_where, link_params = since_clause("logged_at", since)
    recent_flood = fox_db_rows(
        f"""
        SELECT user_id, username, display_name, message_count, window_seconds, message_excerpt, logged_at
        FROM flood_flags
        {link_where}
        ORDER BY logged_at DESC
        LIMIT 5
        """,
        link_params,
    )
    member_where, member_params = since_clause("logged_at", since)
    recent_joins = fox_db_rows(
        f"""
        SELECT user_id, username, display_name, event_type, detail, logged_at
        FROM member_events
        {member_where}
        ORDER BY logged_at DESC
        LIMIT 8
        """,
        member_params,
    )

    pending_pulse = len([
        entry for entry in pulse_question_suggestions
        if entry.get("status") == "pending_review"
    ])
    pending_spotlight = len([
        entry for entry in spotlight_entries
        if entry.get("status") == "pending_review"
    ])
    add_item("pulse", "Pulse questions awaiting review", pending_pulse, "medium", "Open Pulse admin tab")
    add_item("spotlight", "Spotlights awaiting review", pending_spotlight, "medium", "Open Pulse admin tab")

    items.sort(key=lambda row: {"high": 0, "medium": 1, "low": 2}.get(row["severity"], 3))
    return {
        "period": group_activity_period(period),
        "since": since,
        "generated_at": now_iso(),
        "items": items,
        "recent_flood": recent_flood,
        "recent_member_events": recent_joins,
        "settings": load_safety_settings(),
    }


def build_group_activity_member_events(period: str, limit: int = 60):
    since = group_activity_since(period)
    limit = max(1, min(int(limit or 60), 300))
    where, params = since_clause("logged_at", since)
    rows = fox_db_rows(
        f"""
        SELECT id, user_id, username, display_name, event_type, detail, logged_at
        FROM member_events
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (limit,),
    )
    for row in rows:
        row.update(format_group_user_ref(row.get("user_id"), row.get("username"), row.get("display_name")))
    return {"period": group_activity_period(period), "since": since, "items": rows}


def build_group_activity_flood_flags(period: str, limit: int = 60):
    since = group_activity_since(period)
    limit = max(1, min(int(limit or 60), 300))
    where, params = since_clause("logged_at", since)
    rows = fox_db_rows(
        f"""
        SELECT id, user_id, username, display_name, message_count, window_seconds, message_excerpt, logged_at
        FROM flood_flags
        {where}
        ORDER BY logged_at DESC
        LIMIT ?
        """,
        params + (limit,),
    )
    for row in rows:
        row.update(format_group_user_ref(row.get("user_id"), row.get("username"), row.get("display_name")))
    return {"period": group_activity_period(period), "since": since, "items": rows}


def add_notification(kind: str, text: str, public: bool = True):
    next_id = int(notification_feed[-1].get("id") or 0) + 1 if notification_feed else 1
    notification_feed.append(
        {
            "id": next_id,
            "kind": kind,
            "text": text,
            "public": public,
            "time": now_iso(),
        }
    )
    trim_list_in_place(notification_feed, MAX_NOTIFICATION_FEED)
    ws_broadcast("notifications", notification_feed)


def get_next_anonymous_wheel_name():
    count = 0
    for entry in wheel_entries + archived_wheel_entries:
        name = entry["data"].get("display_name", "")
        if name.startswith("Anonymous"):
            count += 1
    return f"Anonymous {count + 1}"


def get_next_comment_id():
    return len(pending_comments) + len(approved_comments) + 1


def get_round_entries(round_number: int):
    return [entry for entry in wheel_entries if entry.get("round_id") == round_number]


def entry_is_approved(entry: dict) -> bool:
    return entry.get("approval_status") == "approved"


def wheel_entry_matches_submitter(entry: dict, telegram_id=None, username=None, display_name=None) -> bool:
    data = entry.get("data") or {}
    if telegram_id is not None and data.get("telegram_id") == telegram_id:
        return True
    if username and (data.get("username") or "").strip().lower() == str(username).strip().lstrip("@").lower():
        return True
    if display_name and (data.get("display_name") or "").strip().lower() == str(display_name).strip().lower():
        return True
    return False


def get_room_users():
    users = {}
    for entry in wheel_entries + archived_wheel_entries:
        data = entry.get("data", {}) or {}
        name = data.get("display_name")
        if not name:
            continue
        key = name.strip().lower()
        users.setdefault(key, {
            "display_name": name,
            "username": data.get("username"),
            "user_id": data.get("telegram_id"),
            "muted": key in muted_users,
            "submission_limit": wheel_submission_limits.get(key, 1),
            "current_round_entries": 0,
            "last_seen": entry.get("time"),
        })
        if data.get("username"):
            users[key]["username"] = data.get("username")
        if data.get("telegram_id") is not None:
            users[key]["user_id"] = data.get("telegram_id")
        if entry.get("round_id") == state["current_round"]:
            users[key]["current_round_entries"] += 1
        if entry.get("time") and (not users[key]["last_seen"] or entry.get("time") > users[key]["last_seen"]):
            users[key]["last_seen"] = entry.get("time")

    for comment in approved_comments + pending_comments:
        name = (comment.get("display_name") or "").strip()
        if not name:
            continue
        key = name.lower()
        users.setdefault(key, {
            "display_name": name,
            "username": comment.get("username"),
            "user_id": comment.get("user_id"),
            "muted": key in muted_users,
            "submission_limit": wheel_submission_limits.get(key, 1),
            "current_round_entries": 0,
            "last_seen": comment.get("time"),
        })
        users[key]["muted"] = key in muted_users
        if comment.get("username"):
            users[key]["username"] = comment.get("username")
        if comment.get("user_id") is not None:
            users[key]["user_id"] = comment.get("user_id")
        if comment.get("time") and (not users[key]["last_seen"] or comment.get("time") > users[key]["last_seen"]):
            users[key]["last_seen"] = comment.get("time")

    for key, user in users.items():
        user["muted"] = key in muted_users
        user["submission_limit"] = wheel_submission_limits.get(key, 1)
        stats = ensure_wheel_user_engagement(
            user_id=user.get("user_id"),
            username=user.get("username"),
            display_name=user.get("display_name"),
        ) or {}
        user["total_reactions"] = int(stats.get("total_reactions") or 0)
        user["total_reviews"] = int(stats.get("total_reviews") or 0)

    reaction_counts = current_video_reaction_counts()
    reviewed_users = current_video_reviewers()
    for key, user in users.items():
        reaction_key = wheel_user_key(
            user_id=user.get("user_id"),
            username=user.get("username"),
            display_name=user.get("display_name"),
        )
        user["current_video_reactions"] = int(reaction_counts.get(reaction_key, 0)) if reaction_key else 0
        user["current_video_reviewed"] = bool(reaction_key and reaction_key in reviewed_users)

    return sorted(users.values(), key=lambda u: (u["display_name"] or "").lower())


def wheel_user_key(user_id=None, username=None, display_name=None) -> str | None:
    if user_id is not None:
        return f"user:{int(user_id)}"
    if username:
        return f"username:{str(username).strip().lower()}"
    if display_name:
        return f"name:{str(display_name).strip().lower()}"
    return None


def ensure_wheel_user_engagement(user_id=None, username=None, display_name=None) -> dict | None:
    global wheel_user_engagement
    key = wheel_user_key(user_id=user_id, username=username, display_name=display_name)
    if not key:
        return None
    existing = wheel_user_engagement.get(key)
    if not isinstance(existing, dict):
        existing = {
            "user_id": user_id,
            "username": username,
            "display_name": display_name or "Unknown",
            "total_reactions": 0,
            "total_reviews": 0,
            "last_active_at": None,
        }
        wheel_user_engagement[key] = existing
    if user_id is not None:
        existing["user_id"] = user_id
    if username:
        existing["username"] = username
    if display_name:
        existing["display_name"] = display_name
    return existing


def current_video_entry_id() -> int | None:
    try:
        if current_now_playing and current_now_playing.get("id") is not None:
            return int(current_now_playing.get("id"))
    except Exception:
        return None
    return None


def current_video_reaction_counts() -> dict:
    current_entry_id = current_video_entry_id()
    if current_entry_id is None:
        return {}
    counts = {}
    for event in wheel_reaction_history:
        try:
            if int(event.get("video_entry_id") or 0) != current_entry_id:
                continue
        except Exception:
            continue
        key = wheel_user_key(
            user_id=event.get("user_id"),
            username=event.get("username"),
            display_name=event.get("display_name"),
        )
        if not key:
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts


def current_video_reviewers() -> set[str]:
    current_entry_id = current_video_entry_id()
    if current_entry_id is None:
        return set()
    reviewers = set()
    for review in wheel_review_history:
        try:
            if int(review.get("video_entry_id") or 0) != current_entry_id:
                continue
        except Exception:
            continue
        key = wheel_user_key(
            user_id=review.get("user_id"),
            username=review.get("username"),
            display_name=review.get("real_display_name") or review.get("display_name"),
        )
        if key:
            reviewers.add(key)
    return reviewers


def current_active_wheel_reaction():
    global current_wheel_reaction
    if not current_wheel_reaction:
        return None
    expires_at = current_wheel_reaction.get("expires_at")
    if expires_at:
        try:
            if datetime.datetime.fromisoformat(expires_at) <= datetime.datetime.utcnow():
                current_wheel_reaction = None
                return None
        except Exception:
            pass
    return current_wheel_reaction


def current_active_wheel_reactions():
    global wheel_reaction_events
    active = []
    for event in wheel_reaction_events:
        expires_at = event.get("expires_at")
        if expires_at:
            try:
                if datetime.datetime.fromisoformat(expires_at) <= datetime.datetime.utcnow():
                    continue
            except Exception:
                pass
        active.append(event)
    wheel_reaction_events = active[-20:]
    return wheel_reaction_events


def current_active_review_overlay():
    global latest_review_overlay
    if not latest_review_overlay:
        return None
    expires_at = latest_review_overlay.get("expires_at")
    if expires_at:
        try:
            if datetime.datetime.fromisoformat(expires_at) <= datetime.datetime.utcnow():
                latest_review_overlay = None
                return None
        except Exception:
            pass
    return latest_review_overlay


def review_average_rating() -> float:
    ratings = [float(review.get("rating") or 0) for review in video_reviews if review.get("rating")]
    if not ratings:
        return 0.0
    return round(sum(ratings) / len(ratings), 2)

def get_wheel_spin_pool(round_number: int):
    return [
        entry
        for entry in wheel_entries
        if entry.get("round_id") == round_number
        and entry_is_approved(entry)
        and not entry.get("played", False)
        and not entry.get("reserved", False)
    ]


def get_next_spin_pool(round_number: int):
    return get_wheel_spin_pool(round_number)


def entry_is_download_ready(entry: dict) -> bool:
    return entry.get("download_status") in {"ready", "manual_ready"}


def clear_processed_candidates(entry: dict) -> None:
    entry["stream_candidate"] = None
    entry["download_candidate"] = None
    entry["processed_at"] = None


def get_ready_unplayed_entries(round_number: int):
    return [
        entry
        for entry in get_wheel_spin_pool(round_number)
        if entry_is_download_ready(entry)
    ]


def reset_wheel_session_state() -> None:
    global current_winner, current_now_playing, current_wheel_reaction, latest_review_overlay
    global active_poll, room_media_submissions, now_showing_media, room_qa_items, room_qa_archive, poll_history
    global room_game, team_feeds

    wheel_entries.clear()
    archived_wheel_entries.clear()
    pending_comments.clear()
    approved_comments.clear()
    notification_feed.clear()
    video_reviews.clear()
    wheel_reaction_events.clear()
    wheel_reaction_history.clear()
    wheel_review_history.clear()
    wheel_user_engagement.clear()
    wheel_submission_limits.clear()
    muted_users.clear()

    current_winner = None
    current_now_playing = None
    current_wheel_reaction = None
    latest_review_overlay = None

    state["current_round"] = 1
    state["round_status"] = "closed"
    state["winner_intro_loaded"] = False
    state["room_open"] = True
    state["closing_soon"] = False
    state["review_prompt_open"] = False
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    state["room_discussion"] = {
        "discussion_id": None,
        "title": "",
        "duration_minutes": None,
        "started_at": None,
        "ends_at": None,
        "status": "idle",
    }
    room_qa_items.clear()
    room_qa_archive.clear()
    poll_history.clear()
    active_poll = None
    room_media_submissions.clear()
    now_showing_media = None
    room_game = None
    team_feeds.clear()
    save_live_room_state()


def find_entry(entry_id: int):
    for entry in wheel_entries:
        if entry["id"] == entry_id:
            return entry
    return None


def source_domain(url: str) -> str | None:
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return None


def sanitize_name(text: str) -> str:
    keep = []
    for ch in text:
        if ch.isalnum():
            keep.append(ch)
        elif ch in {" ", "-", "_"}:
            keep.append("_")
    cleaned = "".join(keep).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned or "Unknown"


VIDEO_EXTENSIONS = {
    ".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi", ".wmv"
}


def is_video_filename(path: str) -> bool:
    return os.path.splitext(path)[1].lower() in VIDEO_EXTENSIONS


def ensure_unique_path(path: str) -> str:
    if not os.path.exists(path):
        return path
    root, ext = os.path.splitext(path)
    index = 2
    while True:
        candidate = f"{root}_{index}{ext}"
        if not os.path.exists(candidate):
            return candidate
        index += 1


def build_ready_filename(entry: dict, source_path: str) -> str:
    title = (entry.get("data") or {}).get("video_title") or entry.get("video_title") or entry["data"].get("display_name", "Unknown")
    safe_name = sanitize_name(title)[:80]
    ext = os.path.splitext(source_path)[1].lower() or ".mp4"
    if ext not in VIDEO_EXTENSIONS:
        ext = ".mp4"
    return f'{entry["id"]:04d}_{safe_name}{ext}'


def assign_local_file_to_entry(entry: dict, source_path: str, video_title: str | None = None) -> dict:
    if not os.path.exists(source_path):
        return {"status": "error", "message": "local file not found"}

    os.makedirs(READY_DIR, exist_ok=True)
    target_name = build_ready_filename(entry, source_path)
    target_path = ensure_unique_path(os.path.join(READY_DIR, target_name))

    if os.path.abspath(source_path) != os.path.abspath(target_path):
        shutil.move(source_path, target_path)
    else:
        target_path = source_path

    entry["download_status"] = "manual_ready"
    entry["download_error"] = None
    entry["local_filename"] = os.path.basename(target_path)
    entry["local_path"] = target_path
    entry["download_method"] = "manual"
    entry["download_completed_at"] = now_iso()
    if video_title:
        entry["data"]["video_title"] = video_title
    clear_processed_candidates(entry)

    ws_broadcast_bundle()
    return {"status": "ok", "entry": entry}


def latest_video_in_downloads() -> str | None:
    if not os.path.exists(DOWNLOADS_DIR):
        return None
    candidates = []
    for name in os.listdir(DOWNLOADS_DIR):
        path = os.path.join(DOWNLOADS_DIR, name)
        if os.path.isfile(path) and is_video_filename(path):
            candidates.append(path)
    if not candidates:
        return None
    candidates.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidates[0]

def ws_broadcast(event: str, data):
    try:
        asyncio.run(manager.broadcast(event, data))
    except RuntimeError:
        pass
    except Exception:
        pass


def ws_broadcast_bundle():
    try:
        payload = {
            "app_state": get_app_state(),
            "ready_entries": current_round_ready_entries(),
            "current_winner": get_current_winner(),
            "notifications": notification_feed,
        }
        ws_broadcast("state_bundle", payload)
    except Exception:
        pass

# ---------------------------------
# Root / state
# ---------------------------------

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)

    try:
        await websocket.send_text(json.dumps({
            "event": "state_bundle",
            "data": {
                "app_state": get_app_state(),
                "ready_entries": current_round_ready_entries(),
                "current_winner": get_current_winner(),
                "notifications": notification_feed,
            }
        }))

        while True:
            await asyncio.sleep(25)
            await websocket.send_text(json.dumps({
                "event": "ping",
                "data": "ok"
            }))

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


@app.websocket("/ws/cards")
async def cards_websocket_endpoint(websocket: WebSocket):
    if not cards_api_enabled():
        await websocket.accept()
        await websocket.close(code=1013, reason="Cards is temporarily disabled.")
        return
    user_id: int | None = None
    room_code: str | None = None
    try:
        await websocket.accept()
        auth_raw = await websocket.receive_text()
        auth_payload = json.loads(auth_raw)
        init_data = auth_payload.get("init_data") or ""
        claimed_user_id = auth_payload.get("user_id")
        claimed_username = auth_payload.get("username")
        room_code = auth_payload.get("room_code")

        if init_data:
            try:
                tg_user = verify_telegram_init_data(init_data)
                user_id = int(tg_user.get("id"))
                claimed_username = tg_user.get("username") or claimed_username
            except HTTPException:
                user_id = None

        if user_id is None and claimed_user_id:
            user_id = int(claimed_user_id)

        if user_id is None:
            await websocket.close(code=4401)
            return

        player, _is_verified = cards_resolve_player(user_id, claimed_username, find_verified_alcove_user)
        if not player:
            await websocket.send_text(json.dumps({
                "event": "error",
                "data": {"message": "Could not identify your Telegram account for Alcove Cards."},
            }))
            await websocket.close(code=4403)
            return

        if not room_code:
            await websocket.send_text(json.dumps({
                "event": "error",
                "data": {"message": "room_code is required."},
            }))
            await websocket.close(code=4400)
            return

        cards_service = get_cards_service(find_verified_alcove_user)
        cards_service.set_event_loop(asyncio.get_running_loop())
        await cards_ws_manager.connect(room_code, user_id, websocket)
        cards_service.attach_user(user_id, True)
        resync = cards_service.resync_room(room_code, user_id)
        if resync:
            await websocket.send_text(json.dumps({"event": "resync", "data": resync}))

        while True:
            raw = await websocket.receive_text()
            message = json.loads(raw)
            action_payload = RoomActionPayload(
                user_id=user_id,
                action=message.get("action") or "",
                payload=message.get("payload") or {},
            )
            result = cards_service.handle_action(room_code, action_payload)
            await websocket.send_text(json.dumps({"event": "action_result", "data": result}))
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if user_id is not None:
            await cards_ws_manager.disconnect(user_id)
            if user_id:
                get_cards_service(find_verified_alcove_user).attach_user(user_id, False)


@app.on_event("startup")
async def cards_startup_tasks():
    if not cards_api_enabled():
        print(f"[{now_iso()}] Cards API disabled; skipping cards cleanup loop.", flush=True)
        return
    loop = asyncio.get_running_loop()
    service = get_cards_service(find_verified_alcove_user)
    service.set_event_loop(loop)

    async def cleanup_loop():
        while True:
            await asyncio.sleep(120)
            try:
                service.cleanup_stale()
            except Exception:
                pass

    asyncio.create_task(cleanup_loop())


@app.get("/")
def root():
    return {
        "status": "Alcove API running",
        "api_revision": "spotlight-archive-admin-20260808",
        "lean_mode": LEAN_MODE,
        "pulse_admin_notify_enabled": PULSE_ADMIN_NOTIFY_ENABLED,
        "pulse_admin_telegram_suppressed": PULSE_ADMIN_TELEGRAM_SUPPRESSED,
    }


def bot_sync_health() -> dict:
    users = get_verified_alcove_users()
    fox_db_exists = os.path.exists(FOX_LOGS_DB_PATH)
    fox_db_count = 0
    if fox_db_exists and not synced_alcove_users:
        fox_db_count = len(users)

    stale_days = None
    if last_bot_sync_at:
        try:
            synced_at = datetime.datetime.fromisoformat(last_bot_sync_at.replace("Z", "+00:00"))
            if synced_at.tzinfo is None:
                synced_at = synced_at.replace(tzinfo=datetime.timezone.utc)
            stale_days = max(0, (datetime.datetime.now(datetime.timezone.utc) - synced_at).days)
        except ValueError:
            stale_days = None

    issues = []
    if not BOT_SYNC_SECRET:
        issues.append("BOT_SYNC_SECRET is not set on alcove-api.")
    if not synced_alcove_users:
        issues.append("No users in API memory; Spotlight reads from bot sync, not the API disk.")
    if not fox_db_exists:
        issues.append("fox_logs.db is missing on alcove-api (expected; F.O.X keeps the real DB on alcove-fox).")
    if stale_days is not None and stale_days >= 1:
        issues.append(f"Last successful bot sync was {stale_days} day(s) ago.")
    if not issues:
        issues.append("none")

    return {
        "bot_sync_secret_configured": bool(BOT_SYNC_SECRET),
        "synced_users_in_memory": len(synced_alcove_users),
        "active_user_count": len(users),
        "fox_logs_db_on_api": fox_db_exists,
        "fox_logs_fallback_count": fox_db_count,
        "last_bot_sync_at": last_bot_sync_at,
        "days_since_last_sync": stale_days,
        "source": "bot_sync" if synced_alcove_users else "fox_logs",
        "likely_issues": issues,
        "fix_steps": [
            "In Render, set the same BOT_SYNC_SECRET on both alcove-fox and alcove-api.",
            "Confirm alcove-fox worker is Live (not crashed).",
            "In Telegram run /syncusers then /syncapi.",
            "Reload /api/alcove-users and expect source=bot_sync with count > 0.",
        ],
    }


@app.get("/api/alcove-users")
def alcove_users():
    users = get_verified_alcove_users()
    health = bot_sync_health()
    return {
        "status": "ok",
        "count": len(users),
        "users": users,
        "source": health["source"],
        "last_bot_sync_at": last_bot_sync_at,
        "db_available": os.path.exists(FOX_LOGS_DB_PATH),
        "sync_health": health,
    }


@app.get("/api/alcove-analytics")
def alcove_analytics():
    return {
        "status": "ok",
        "today": build_alcove_analytics("today"),
        "week": build_alcove_analytics("week"),
        "allTime": build_alcove_analytics("allTime"),
        "source": "bot_sync" if synced_alcove_analytics else "fox_logs",
        "last_bot_sync_at": last_bot_sync_at,
        "db_available": os.path.exists(FOX_LOGS_DB_PATH),
    }


@app.post("/api/verification/miniapp")
def submit_miniapp_verification(payload: MiniappVerificationPayload):
    if not payload.init_data:
        raise HTTPException(status_code=400, detail="Missing Telegram Mini App data")
    user = verify_telegram_init_data(payload.init_data)
    entry = upsert_miniapp_verification(user)
    update_miniapp_verification_details(entry, payload)
    save_runtime_state()
    return {"status": "ok", "verification": miniapp_verification_payload(entry)}


@app.post("/api/verification/miniapp/status")
def miniapp_verification_status(payload: MiniappVerificationPayload):
    if not payload.init_data:
        raise HTTPException(status_code=400, detail="Missing Telegram Mini App data")
    user = verify_telegram_init_data(payload.init_data)
    user_id = int(user.get("id") or 0)
    entry = next(
        (
            item for item in reversed(miniapp_verifications)
            if int(item.get("user_id") or 0) == user_id
        ),
        None,
    )
    if not entry:
        raise HTTPException(status_code=404, detail="Mini App verification not found")
    return {"status": "ok", "verification": miniapp_verification_payload(entry)}


@app.post("/api/verification-log")
def verification_log(payload: VerificationLogPayload):
    entry = append_verification_flow_log(payload)
    return {"status": "ok", "received_at": entry["received_at"]}


@app.post("/api/bot-sync/alcove")
def bot_sync_alcove(payload: BotSyncPayload, x_bot_sync_secret: str | None = Header(default=None)):
    global synced_alcove_users, synced_alcove_analytics, last_bot_sync_at

    verify_bot_sync_secret(x_bot_sync_secret)

    incoming = [user for user in (payload.users or []) if is_alcove_verified_user(user)]
    users_changed = bool(incoming) and incoming != synced_alcove_users
    analytics_changed = payload.analytics is not None and payload.analytics != synced_alcove_analytics
    if incoming:
        synced_alcove_users = incoming
    elif not synced_alcove_users:
        synced_alcove_users = []
    else:
        print(
            f"[{now_iso()}] bot sync ignored empty user payload; "
            f"keeping {len(synced_alcove_users)} synced residents",
            flush=True,
        )
    if payload.analytics is not None:
        synced_alcove_analytics = payload.analytics
    else:
        synced_alcove_analytics = synced_alcove_analytics or {}
    if users_changed or analytics_changed:
        last_bot_sync_at = payload.synced_at or now_iso()
        save_runtime_state()

    return {
        "status": "ok",
        "users": len(synced_alcove_users),
        "synced_at": last_bot_sync_at,
    }


@app.get("/api/bot-sync/verification/pending")
def bot_pending_miniapp_verifications(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        miniapp_verification_payload(entry)
        for entry in miniapp_verifications
        if entry.get("status") == "pending"
    ]
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/verification-logs")
def bot_verification_logs(
    limit: int = 80,
    user_id: int | None = None,
    username: str | None = None,
    session_id: str | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    return {
        "status": "ok",
        "logs": read_verification_flow_logs(
            limit=limit,
            user_id=user_id,
            username=username,
            session_id=session_id,
        ),
    }


@app.post("/api/bot-sync/verification/{verification_id}")
def bot_update_miniapp_verification(
    verification_id: int,
    payload: dict | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = next(
        (item for item in miniapp_verifications if int(item.get("id") or 0) == int(verification_id)),
        None,
    )
    if not entry:
        return {"status": "error", "message": "Mini App verification not found."}
    payload = payload or {}
    status = payload.get("status")
    if status not in {"pending", "completed", "failed"}:
        return {"status": "error", "message": "Invalid Mini App verification status."}
    entry["status"] = status
    entry["completed_at"] = now_iso() if status in {"completed", "failed"} else None
    if payload.get("detail"):
        entry["detail"] = str(payload.get("detail"))[:500]
    save_runtime_state()
    return {"status": "ok", "verification": miniapp_verification_payload(entry)}


@app.get("/api/app-state")
def get_app_state():
    current_round = state["current_round"]
    round_entries = get_round_entries(current_round)
    ready_entries = get_ready_unplayed_entries(current_round)
    played_count = len([e for e in round_entries if e.get("played", False)])
    failed_count = len([e for e in round_entries if e.get("download_status") == "failed"])
    downloading_count = len(
        [
            e
            for e in round_entries
            if e.get("download_status") in {"pending", "extracting", "downloading"}
        ]
    )

    return {
        "current_round": current_round,
        "round_status": state["round_status"],
        "modules": state["modules"],
        "paths": {
            "downloads": DOWNLOADS_DIR,
            "ready": READY_DIR,
            "archive": ARCHIVE_DIR,
            "playout": CURRENT_PICK_PATH,
        },
        "counts": {
            "round_entries": len(round_entries),
            "ready_entries": len(ready_entries),
            "played_entries": played_count,
            "failed_entries": failed_count,
            "downloading_entries": downloading_count,
            "pending_comments": len(pending_comments),
            "approved_comments": len(approved_comments),
            "archived_wheel_entries": len(archived_wheel_entries),
        },
        "entries": round_entries,
        "pending_comments_list": pending_comments,
        "approved_comments_list": approved_comments,
        "notifications": notification_feed,
        "room_users": get_room_users(),
        "current_winner": current_winner,
        "current_now_playing": current_now_playing,
        "room_open": state.get("room_open", True),
        "closing_soon": state.get("closing_soon", False),
        "review_prompt_open": state.get("review_prompt_open", False),
        "review_reveal_active": state.get("review_reveal_active", False),
        "review_score_reveal_active": state.get("review_score_reveal_active", False),
        "winner_intro_loaded": state.get("winner_intro_loaded", False),
        "video_reviews": video_reviews,
        "video_review_average": review_average_rating(),
        "active_wheel_reaction": current_active_wheel_reaction(),
        "active_wheel_reactions": current_active_wheel_reactions(),
        "active_review_overlay": current_active_review_overlay(),
        "room_discussion": state.get("room_discussion") or {
            "discussion_id": None,
            "title": "",
            "duration_minutes": None,
            "started_at": None,
            "ends_at": None,
            "status": "idle",
        },
        "active_poll": active_poll,
        "room_qa_items": [item for item in room_qa_items if item.get("status") != "archived"],
        "room_qa_archive": room_qa_archive,
        "poll_history": poll_history[-20:],
        "media_submissions": room_media_submissions,
        "now_showing_media": now_showing_media,
        "room_game": room_game,
        "team_feeds": team_feeds if (room_game or {}).get("mode") == "collaborate" else {},
    }


@app.post("/api/session/hard-reset")
def hard_reset_session():
    reset_wheel_session_state()
    save_runtime_state()
    ws_broadcast_bundle()
    return {"status": "ok", "message": "Wheel session reset", "current_round": state["current_round"]}


# ---------------------------------
# Round controls
# ---------------------------------

@app.post("/api/round/open")
def open_round():
    current_round = state["current_round"]
    state["round_status"] = "open"
    state["room_open"] = True
    state["closing_soon"] = False
    state["review_prompt_open"] = False
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    current_round_entries = get_round_entries(current_round)
    wheel_submission_limits.clear()
    add_notification("system", f"Round {current_round} submissions open", True)

    ws_broadcast_bundle()
    return {
        "status": "ok",
        "message": f"Round {current_round} opened",
        "entries_in_round": len(current_round_entries),
    }


@app.post("/api/round/lock")
def lock_round():
    current_round = state["current_round"]
    state["round_status"] = "locked"
    add_notification("system", f"Round {current_round} locked", True)

    ws_broadcast_bundle()
    return {"status": "ok", "message": f"Round {current_round} locked"}


@app.post("/api/round/start-spin")
def start_spin():
    global current_winner, current_now_playing
    current_round = state["current_round"]
    pool = get_next_spin_pool(current_round)

    if not pool:
        return {"status": "error", "message": "No ready entries left to spin in this round."}

    state["round_status"] = "spinning"
    state["winner_intro_loaded"] = False
    current_now_playing = None
    chosen = random.choice(pool)
    current_winner = {
        "entry_id": chosen["id"],
        "entrant_name": chosen["data"].get("display_name", "Unknown"),
        "video_title": chosen["data"].get("video_title"),
        "local_filename": chosen.get("local_filename"),
        "local_path": chosen.get("local_path"),
        "submitted_url": chosen.get("submitted_url") or chosen.get("data", {}).get("link"),
        "clip_start_seconds": chosen.get("clip_start_seconds"),
        "clip_end_seconds": chosen.get("clip_end_seconds"),
        "time": now_iso(),
    }
    add_notification("winner", f"Winner: {current_winner['entrant_name']}", True)
    add_notification("system", f"Round {current_round} spinning", True)

    ws_broadcast_bundle()
    return {"status": "ok", "message": f"Round {current_round} spin started", "winner": current_winner}


def move_unplayed_round_entries_to_reserve(round_number: int) -> int:
    """Park pending/approved unplayed videos in Reserve when a round ends."""
    moved = 0
    stamped = now_iso()
    for entry in wheel_entries:
        if entry.get("round_id") != round_number:
            continue
        if entry.get("played", False):
            continue
        if entry.get("approval_status") not in {"pending", "approved"}:
            continue
        if entry.get("reserved"):
            continue
        entry["reserved"] = True
        entry["reserved_at"] = stamped
        moved += 1
    return moved


@app.post("/api/round/end")
def end_round():
    global current_winner, current_now_playing
    current_round = state["current_round"]
    reserved_count = move_unplayed_round_entries_to_reserve(current_round)
    state["round_status"] = "closed"
    state["winner_intro_loaded"] = False
    state["review_prompt_open"] = False
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    current_winner = None
    current_now_playing = None
    video_reviews.clear()
    add_notification("system", f"Round {current_round} ended", True)
    if reserved_count:
        add_notification(
            "system",
            f"{reserved_count} unplayed video{'s' if reserved_count != 1 else ''} moved to Reserve",
            False,
        )
    state["current_round"] += 1
    wheel_submission_limits.clear()
    return {
        "status": "ok",
        "message": f"Round {current_round} ended. Round {state['current_round']} ready.",
        "reserved_count": reserved_count,
    }


# ---------------------------------
# Module toggles
# ---------------------------------

@app.get("/api/modules")
def get_modules():
    return state["modules"]


@app.post("/api/modules")
def update_modules(payload: ModuleStateUpdate):
    state["modules"] = payload.dict()
    return {"status": "ok", "modules": state["modules"]}


@app.get("/api/feature-flags")
def get_feature_flags():
    return {
        "status": "ok",
        "features": load_feature_flags(),
        "tester_usernames": load_tester_usernames(),
        "schema": feature_flag_schema_payload(),
    }


@app.post("/api/feature-flags")
def update_feature_flags(payload: FeatureFlagsUpdate, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret or payload.admin_secret)
    flags = load_feature_flags()
    incoming = payload.dict(exclude_none=True)
    tester_usernames = None
    for group, values in incoming.items():
        if group == "admin_secret":
            continue
        if group == "tester_usernames":
            tester_usernames = normalize_tester_usernames(values)
            continue
        if group not in flags or not isinstance(values, dict):
            continue
        for key, value in values.items():
            if key in flags[group]:
                flags[group][key] = bool(value)
    save_feature_flags(flags, tester_usernames)
    return {
        "status": "ok",
        "features": flags,
        "tester_usernames": load_tester_usernames(),
        "schema": feature_flag_schema_payload(),
    }


@app.get("/api/pulse-settings")
def get_pulse_settings():
    progress = pulse_progress_payload()
    return {"status": "ok", "settings": load_pulse_settings(), "progress": progress}


@app.post("/api/pulse-settings")
def update_pulse_settings(payload: PulseSettingsUpdate, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret or payload.admin_secret)
    settings = {
        "heat_threshold": normalized_pulse_threshold(payload.heat_threshold),
        "reset_interval_hours": normalized_pulse_reset_interval(payload.reset_interval_hours),
    }
    save_pulse_settings(settings)
    return {"status": "ok", "settings": settings, "progress": pulse_progress_payload()}


@app.get("/api/admin/pulse-question-schedule")
def admin_pulse_question_schedule(admin_secret: str):
    verify_admin_secret(admin_secret)
    green = pulse_pool_schedule_payload("green")
    red = pulse_pool_schedule_payload("red")
    return {"status": "ok", "green": green, "red": red, **green}


@app.get("/api/admin/pulse-questions/{suggestion_id}/answers")
def admin_pulse_question_answers(suggestion_id: int, admin_secret: str):
    verify_admin_secret(admin_secret)
    entry, answers = pulse_answers_for_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question not found."}
    return {
        "status": "ok",
        "entry": pulse_question_suggestion_admin_payload(entry),
        "answers": answers,
    }


@app.get("/api/admin/pulse-archive/stats")
def admin_pulse_archive_stats(admin_secret: str):
    verify_admin_secret(admin_secret)
    return {"status": "ok", "stats": pulse_archive_stats_payload()}


@app.get("/api/admin/pulse-archive/answers")
def admin_pulse_archive_answers(
    admin_secret: str,
    day: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    pool: str | None = None,
    q: str | None = None,
    username: str | None = None,
    suggestion_id: int | None = None,
    page: int = 1,
    limit: int = 50,
):
    verify_admin_secret(admin_secret)
    return {
        "status": "ok",
        **query_pulse_archive_answers(
            day=day,
            from_day=from_day,
            to_day=to_day,
            pool=pool,
            q=q,
            username=username,
            suggestion_id=suggestion_id,
            page=page,
            limit=limit,
        ),
    }


@app.get("/api/admin/pulse-archive/questions")
def admin_pulse_archive_questions(
    admin_secret: str,
    pool: str | None = None,
    status: str | None = None,
    q: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    page: int = 1,
    limit: int = 50,
):
    verify_admin_secret(admin_secret)
    return {
        "status": "ok",
        **query_pulse_archive_questions(
            pool=pool,
            status=status,
            q=q,
            from_day=from_day,
            to_day=to_day,
            page=page,
            limit=limit,
        ),
    }


@app.get("/api/admin/pulse-archive/export")
def admin_pulse_archive_export(
    admin_secret: str,
    day: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    pool: str | None = None,
    q: str | None = None,
    username: str | None = None,
    suggestion_id: int | None = None,
):
    verify_admin_secret(admin_secret)
    filters = {
        "day": day,
        "from_day": from_day,
        "to_day": to_day,
        "pool": pool,
        "q": q,
        "username": username,
        "suggestion_id": suggestion_id,
    }
    csv_text = pulse_archive_answers_csv(filters)
    stamp = day or from_day or pulse_day_key()
    filename = f"pulse-archive-{stamp}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.delete("/api/admin/pulse-archive/questions/{suggestion_id}")
def admin_pulse_archive_delete_question(suggestion_id: int, admin_secret: str):
    """Permanently delete an archived Pulse question and cascade its answers."""
    verify_admin_secret(admin_secret)
    deleted = permanently_delete_pulse_archive_question(suggestion_id)
    return {"status": "ok", "deleted": deleted}


@app.post("/api/admin/pulse-archive/questions/{suggestion_id}/delete")
def admin_pulse_archive_delete_question_post(suggestion_id: int, payload: AdminSecretQuery):
    """POST fallback for clients that struggle with DELETE."""
    verify_admin_secret(payload.admin_secret)
    deleted = permanently_delete_pulse_archive_question(suggestion_id)
    return {"status": "ok", "deleted": deleted}


@app.delete("/api/admin/pulse-archive/answers/{pulse_id}")
def admin_pulse_archive_delete_answer(pulse_id: int, admin_secret: str):
    """Permanently delete one archived Pulse answer."""
    verify_admin_secret(admin_secret)
    deleted = permanently_delete_pulse_archive_answer(pulse_id)
    return {"status": "ok", "deleted": deleted}


@app.post("/api/admin/pulse-archive/answers/{pulse_id}/delete")
def admin_pulse_archive_delete_answer_post(pulse_id: int, payload: AdminSecretQuery):
    """POST fallback for clients that struggle with DELETE."""
    verify_admin_secret(payload.admin_secret)
    deleted = permanently_delete_pulse_archive_answer(pulse_id)
    return {"status": "ok", "deleted": deleted}


@app.get("/api/admin/spotlight-archive/stats")
def admin_spotlight_archive_stats(admin_secret: str):
    verify_admin_secret(admin_secret)
    return {"status": "ok", "stats": spotlight_archive_stats_payload()}


@app.get("/api/admin/spotlight-archive")
def admin_spotlight_archive_list(
    admin_secret: str,
    style: str | None = None,
    nominator: str | None = None,
    nominee: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str = "date_desc",
    page: int = 1,
    limit: int = 50,
):
    verify_admin_secret(admin_secret)
    return {
        "status": "ok",
        **query_spotlight_archive(
            style=style,
            nominator=nominator,
            nominee=nominee,
            from_day=from_day,
            to_day=to_day,
            sort=sort,
            page=page,
            limit=limit,
        ),
    }


@app.delete("/api/admin/spotlight-archive/{spotlight_id}")
def admin_spotlight_archive_delete(spotlight_id: int, admin_secret: str):
    verify_admin_secret(admin_secret)
    deleted = permanently_delete_spotlight_archive_entry(spotlight_id)
    return {"status": "ok", "deleted": deleted}


@app.post("/api/admin/spotlight-archive/{spotlight_id}/delete")
def admin_spotlight_archive_delete_post(spotlight_id: int, payload: AdminSecretQuery):
    verify_admin_secret(payload.admin_secret)
    deleted = permanently_delete_spotlight_archive_entry(spotlight_id)
    return {"status": "ok", "deleted": deleted}


@app.get("/api/admin/spotlights")
def admin_spotlight_runtime_list(admin_secret: str, status: str | None = None):
    """List runtime Spotlight nominations/entries for Feature Admin cleanup."""
    verify_admin_secret(admin_secret)
    entries = admin_spotlight_runtime_entries(status=status)
    return {"status": "ok", "entries": entries, "total": len(entries)}


@app.post("/api/admin/spotlights/{entry_id}/delete")
def admin_spotlight_runtime_delete_post(entry_id: int, payload: AdminSecretQuery):
    """Permanently delete a runtime Spotlight nomination/entry."""
    verify_admin_secret(payload.admin_secret)
    deleted = permanently_delete_spotlight_runtime_entry(entry_id)
    return {"status": "ok", "deleted": deleted}


@app.get("/api/archive/summary")
def member_archive_summary(user_id: int | None = None, username: str | None = None):
    if not user_id and not username:
        return {"status": "error", "message": "Could not identify this Archive user."}
    viewer = archive_viewer_identity(user_id, username)
    return {"status": "ok", "summary": archive_summary_payload(viewer)}


@app.get("/api/archive/pulse")
def member_archive_pulse(
    user_id: int | None = None,
    username: str | None = None,
    view: str = "questions",
    pool: str | None = None,
    q: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str = "date_desc",
    mine_only: bool = False,
    page: int = 1,
    limit: int = 50,
):
    if not user_id and not username:
        return {"status": "error", "message": "Could not identify this Archive user."}
    viewer = archive_viewer_identity(user_id, username)
    return {
        "status": "ok",
        **build_member_pulse_archive_payload(
            viewer=viewer,
            view=view,
            pool=pool,
            q=q,
            from_day=from_day,
            to_day=to_day,
            sort=sort,
            mine_only=mine_only,
            page=page,
            limit=limit,
        ),
    }


@app.get("/api/archive/spotlight")
def member_archive_spotlight(
    user_id: int | None = None,
    username: str | None = None,
    style: str | None = None,
    nominator: str | None = None,
    nominee: str | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str = "date_desc",
    mine: str | None = None,
    page: int = 1,
    limit: int = 50,
):
    if not user_id and not username:
        return {"status": "error", "message": "Could not identify this Archive user."}
    viewer = archive_viewer_identity(user_id, username)
    result = query_spotlight_archive(
        style=style,
        nominator=nominator,
        nominee=nominee,
        from_day=from_day,
        to_day=to_day,
        sort=sort,
        mine=mine,
        viewer_user_id=viewer.get("user_id"),
        viewer_username=viewer.get("username"),
        page=page,
        limit=limit,
    )
    entries = [sanitize_member_spotlight_entry(entry, viewer) for entry in result.get("entries") or []]
    return {
        "status": "ok",
        "entries": entries,
        "total": result.get("total", 0),
        "page": result.get("page", 1),
        "pages": result.get("pages", 1),
        "limit": result.get("limit", limit),
        "stats": {"award_count": result.get("total", 0)},
    }


@app.get("/api/archive/wheel")
def member_archive_wheel(
    user_id: int | None = None,
    username: str | None = None,
    q: str | None = None,
    submitted_by: str | None = None,
    min_rating: float | None = None,
    from_day: str | None = None,
    to_day: str | None = None,
    sort: str = "date_desc",
    mine_only: bool = False,
    page: int = 1,
    limit: int = 50,
):
    if not user_id and not username:
        return {"status": "error", "message": "Could not identify this Archive user."}
    viewer = archive_viewer_identity(user_id, username)
    result = query_archive_wheel_entries(
        q=q,
        submitted_by=submitted_by,
        min_rating=min_rating,
        from_day=from_day,
        to_day=to_day,
        sort=sort,
        mine_only=mine_only,
        viewer=viewer,
        page=page,
        limit=limit,
    )
    return {
        "status": "ok",
        **result,
        "stats": {"entry_count": result.get("total", 0)},
    }


@app.get("/api/admin/group-activity/summary")
def admin_group_activity_summary(admin_secret: str, period: str = "today"):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_summary(period)}


@app.get("/api/admin/group-activity/users")
def admin_group_activity_users(
    admin_secret: str,
    period: str = "week",
    sort: str = "risk",
    limit: int = 50,
):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_users(period, sort=sort, limit=limit)}


@app.get("/api/admin/group-activity/violations")
def admin_group_activity_violations(
    admin_secret: str,
    type: str = "links",
    period: str = "week",
    limit: int = 40,
):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_violations(type, period, limit=limit)}


@app.get("/api/admin/group-activity/actions")
def admin_group_activity_actions(admin_secret: str, period: str = "week", limit: int = 40):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_actions(period, limit=limit)}


@app.get("/api/admin/group-activity/user/{user_id}")
def admin_group_activity_user_detail(user_id: int, admin_secret: str):
    verify_admin_secret(admin_secret)
    return {"status": "ok", "user": build_group_activity_user_detail(user_id)}


@app.get("/api/admin/safety/action-queue")
def admin_safety_action_queue(admin_secret: str, period: str = "today"):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_safety_action_queue(period)}


@app.get("/api/admin/safety/settings")
def admin_safety_settings_get(admin_secret: str):
    verify_admin_secret(admin_secret)
    return {"status": "ok", "settings": load_safety_settings()}


@app.post("/api/admin/safety/settings")
def admin_safety_settings_save(payload: AdminSafetySettingsPayload):
    verify_admin_secret(payload.admin_secret)
    current = load_safety_settings()
    if payload.flood_message_threshold is not None:
        current["flood_message_threshold"] = payload.flood_message_threshold
    if payload.flood_window_seconds is not None:
        current["flood_window_seconds"] = payload.flood_window_seconds
    if payload.daily_digest_enabled is not None:
        current["daily_digest_enabled"] = payload.daily_digest_enabled
    if payload.daily_digest_hour_utc is not None:
        current["daily_digest_hour_utc"] = payload.daily_digest_hour_utc
    if payload.keywords is not None:
        current["keywords"] = payload.keywords
    saved = save_safety_settings(current)
    return {"status": "ok", "settings": saved}


@app.get("/api/admin/safety/member-events")
def admin_safety_member_events(admin_secret: str, period: str = "week", limit: int = 60):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_member_events(period, limit=limit)}


@app.get("/api/admin/safety/flood-flags")
def admin_safety_flood_flags(admin_secret: str, period: str = "week", limit: int = 60):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_flood_flags(period, limit=limit)}


@app.get("/api/bot-sync/safety-settings")
def bot_sync_safety_settings(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    return {"status": "ok", "settings": load_safety_settings()}


@app.get("/api/admin/fox-messages")
def admin_fox_messages(admin_secret: str):
    verify_admin_secret(admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    return {"status": "ok", **fox_messages_store.admin_payload(state)}


@app.post("/api/admin/fox-messages/builtin")
def admin_fox_messages_builtin(payload: AdminFoxBuiltinSettingsPayload):
    verify_admin_secret(payload.admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    builtin = state.setdefault("builtin_settings", {})
    if isinstance(payload.self_care, dict):
        current = builtin.setdefault("self_care", {})
        if "enabled" in payload.self_care:
            current["enabled"] = bool(payload.self_care["enabled"])
        if payload.self_care.get("interval_min_hours") is not None:
            current["interval_min_hours"] = max(1, min(int(payload.self_care["interval_min_hours"]), 168))
        if payload.self_care.get("interval_max_hours") is not None:
            current["interval_max_hours"] = max(1, min(int(payload.self_care["interval_max_hours"]), 168))
        if payload.self_care.get("banner") is not None:
            current["banner"] = str(payload.self_care["banner"]).strip()
        if isinstance(payload.self_care.get("messages"), list):
            messages = [str(line).strip() for line in payload.self_care["messages"] if str(line).strip()]
            if messages:
                current["messages"] = messages[:40]
        if current.get("interval_min_hours", 8) > current.get("interval_max_hours", 12):
            current["interval_max_hours"] = current["interval_min_hours"]
        builtin["self_care"] = current
    if isinstance(payload.templates, dict):
        current_templates = fox_messages_store.normalize_templates(builtin.get("templates"))
        for template_id, patch in payload.templates.items():
            if isinstance(patch, dict):
                current_templates = fox_messages_store.merge_template_update(current_templates, str(template_id), patch)
        builtin["templates"] = current_templates
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", **fox_messages_store.admin_payload(state)}


@app.post("/api/admin/fox-messages/scheduled")
def admin_fox_messages_scheduled_create(payload: AdminFoxScheduledPostPayload):
    verify_admin_secret(payload.admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    post = fox_messages_store.upsert_scheduled_post(state, payload.model_dump(exclude={"admin_secret"}))
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "post": post}


@app.put("/api/admin/fox-messages/scheduled/{post_id}")
def admin_fox_messages_scheduled_update(post_id: str, payload: AdminFoxScheduledPostPayload):
    verify_admin_secret(payload.admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    try:
        post = fox_messages_store.upsert_scheduled_post(
            state,
            payload.model_dump(exclude={"admin_secret"}, exclude_none=True),
            post_id=post_id,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Scheduled post not found")
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "post": post}


@app.delete("/api/admin/fox-messages/scheduled/{post_id}")
def admin_fox_messages_scheduled_delete(post_id: str, admin_secret: str):
    verify_admin_secret(admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    if not fox_messages_store.delete_scheduled_post(state, post_id):
        raise HTTPException(status_code=404, detail="Scheduled post not found")
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "deleted": post_id}


@app.post("/api/admin/fox-messages/scheduled/{post_id}/test")
def admin_fox_messages_scheduled_test(post_id: str, admin_secret: str):
    verify_admin_secret(admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    post = fox_messages_store.queue_test_send(state, post_id)
    if not post:
        raise HTTPException(status_code=404, detail="Scheduled post not found")
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "post": post, "message": "Queued ΓÇö F.O.X will send on the next poll (about 60 seconds)."}


@app.get("/api/bot-sync/fox-messages")
def bot_sync_fox_messages(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    state = fox_messages_store.load_fox_messages_state()
    due = fox_messages_store.posts_due_now(state)
    return {
        "status": "ok",
        "builtin_settings": state.get("builtin_settings") or {},
        "due_posts": due,
        "uploaded_banners": fox_messages_store.banner_manifest(state),
    }


@app.post("/api/admin/fox-messages/banners/upload")
async def admin_fox_banner_upload(
    admin_secret: str = Form(...),
    file: UploadFile = File(...),
    label: str = Form(""),
):
    verify_admin_secret(admin_secret)
    content = await file.read()
    state = fox_messages_store.load_fox_messages_state()
    try:
        entry = fox_messages_store.save_banner_upload(
            content,
            file.filename or "banner.png",
            label,
            state,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "banner": entry, **fox_messages_store.admin_payload(state)}


@app.get("/api/admin/fox-messages/banners/{filename}")
def admin_fox_banner_file(filename: str, admin_secret: str):
    verify_admin_secret(admin_secret)
    try:
        file_path = fox_messages_store.resolve_uploaded_banner_file(filename)
    except (ValueError, FileNotFoundError):
        raise HTTPException(status_code=404, detail="Banner not found")
    media_type = "image/png"
    lower = filename.lower()
    if lower.endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"
    elif lower.endswith(".webp"):
        media_type = "image/webp"
    elif lower.endswith(".gif"):
        media_type = "image/gif"
    return FileResponse(file_path, media_type=media_type)


@app.delete("/api/admin/fox-messages/banners/{banner_id}")
def admin_fox_banner_delete(banner_id: str, admin_secret: str):
    verify_admin_secret(admin_secret)
    state = fox_messages_store.load_fox_messages_state()
    if not fox_messages_store.delete_custom_banner(state, banner_id):
        raise HTTPException(status_code=404, detail="Banner not found")
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", **fox_messages_store.admin_payload(state)}


@app.get("/api/reward-catalog")
def get_reward_catalog():
    catalog = load_reward_catalog()
    return {"status": "ok", **catalog}


@app.post("/api/admin/reward-catalog")
def admin_update_reward_catalog(payload: RewardCatalogUpdate):
    verify_admin_secret(payload.admin_secret)
    current = load_reward_catalog()
    if payload.level_packs is not None:
        current["level_packs"] = normalize_level_packs(payload.level_packs)
    if payload.achievements is not None:
        current["achievements"] = normalize_reward_achievements(payload.achievements)
    if payload.verification_packs is not None:
        current["verification_packs"] = normalize_verification_packs(payload.verification_packs)
    if payload.verification_packs_active is not None:
        was_active = bool(current.get("verification_packs_active"))
        current["verification_packs_active"] = bool(payload.verification_packs_active)
        if current["verification_packs_active"] and not was_active:
            current["verification_packs_activated_at"] = now_iso()
        if not current["verification_packs_active"]:
            current["verification_packs_activated_at"] = None
    saved = save_reward_catalog(current)
    return {"status": "ok", **saved}


@app.get("/api/verification-pack/eligibility")
def get_verification_pack_eligibility(
    user_id: int | None = None,
    username: str | None = None,
):
    return verification_pack_eligibility(user_id, username)


@app.post("/api/verification-pack/eligibility")
def post_verification_pack_eligibility(payload: VerificationPackEligibilityPayload | None = None):
    payload = payload or VerificationPackEligibilityPayload()
    user_id = payload.user_id
    username = payload.username
    if payload.init_data:
        try:
            user = verify_telegram_init_data(payload.init_data)
            user_id = int(user.get("id") or 0) or user_id
            username = user.get("username") or username
        except Exception:
            pass
    return verification_pack_eligibility(user_id, username)


@app.get("/api/reward-assets/{filename}")
def public_reward_asset_file(filename: str):
    try:
        file_path = resolve_reward_asset_file(filename)
    except (ValueError, FileNotFoundError):
        raise HTTPException(status_code=404, detail="Reward asset not found")
    media_type = "image/png"
    lower = filename.lower()
    if lower.endswith((".jpg", ".jpeg")):
        media_type = "image/jpeg"
    elif lower.endswith(".webp"):
        media_type = "image/webp"
    elif lower.endswith(".gif"):
        media_type = "image/gif"
    return FileResponse(file_path, media_type=media_type)


@app.get("/api/admin/reward-assets")
def admin_list_reward_assets(admin_secret: str):
    verify_admin_secret(admin_secret)
    manifest = load_reward_assets_manifest()
    assets = list(reversed(manifest.get("assets") or []))
    return {"status": "ok", "assets": assets}


@app.post("/api/admin/reward-assets/upload")
async def admin_reward_asset_upload(
    admin_secret: str = Form(...),
    file: UploadFile = File(...),
    label: str = Form(""),
    kind: str = Form("icon"),
):
    verify_admin_secret(admin_secret)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > REWARD_ASSET_MAX_BYTES:
        raise HTTPException(status_code=400, detail="File too large (max 5 MB)")
    ext = Path(file.filename or "icon.png").suffix.lower()
    if ext not in REWARD_ASSET_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Use PNG, JPEG, WebP, or GIF")
    stem = sanitize_reward_asset_stem(file.filename or label or "reward")
    filename = f"{stem}-{uuid.uuid4().hex[:8]}{ext}"
    file_path = os.path.join(REWARD_ASSETS_DIR, filename)
    with open(file_path, "wb") as handle:
        handle.write(content)
    entry = {
        "id": uuid.uuid4().hex[:12],
        "filename": filename,
        "label": str(label or stem).strip()[:80] or filename,
        "kind": str(kind or "icon").strip().lower()[:32] or "icon",
        "url": reward_asset_public_url(filename),
        "uploaded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "size_bytes": len(content),
    }
    manifest = load_reward_assets_manifest()
    assets = list(manifest.get("assets") or [])
    assets.append(entry)
    manifest["assets"] = assets[-200:]
    save_reward_assets_manifest(manifest)
    return {"status": "ok", "asset": entry, "assets": list(reversed(manifest["assets"]))}


@app.delete("/api/admin/reward-assets/{asset_id}")
def admin_reward_asset_delete(asset_id: str, admin_secret: str):
    verify_admin_secret(admin_secret)
    manifest = load_reward_assets_manifest()
    assets = list(manifest.get("assets") or [])
    kept = []
    deleted = None
    for entry in assets:
        if entry.get("id") == asset_id:
            deleted = entry
            filename = str(entry.get("filename") or "").strip()
            if filename:
                try:
                    os.remove(os.path.join(REWARD_ASSETS_DIR, Path(filename).name))
                except OSError:
                    pass
            continue
        kept.append(entry)
    if not deleted:
        raise HTTPException(status_code=404, detail="Asset not found")
    manifest["assets"] = kept
    save_reward_assets_manifest(manifest)
    return {"status": "ok", "assets": list(reversed(kept))}


@app.get("/api/bot-sync/fox-banners/{filename}")
def bot_sync_fox_banner_file(filename: str, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    try:
        file_path = fox_messages_store.resolve_uploaded_banner_file(filename)
    except (ValueError, FileNotFoundError):
        raise HTTPException(status_code=404, detail="Banner not found")
    return FileResponse(file_path)


@app.post("/api/bot-sync/fox-messages/delivery")
def bot_sync_fox_messages_delivery(
    payload: FoxMessageDeliveryPayload,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    state = fox_messages_store.load_fox_messages_state()
    fox_messages_store.mark_post_sent(
        state,
        payload.post_id,
        message_id=payload.message_id,
        chat_id=payload.chat_id,
        error=payload.error,
    )
    fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok"}


@app.post("/api/bot-sync/fox-messages/audit")
def bot_sync_fox_messages_audit(
    payload: FoxAuditBatchPayload,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    state = fox_messages_store.load_fox_messages_state()
    added = fox_messages_store.append_audit_log(state, payload.entries or [])
    if added:
        fox_messages_store.save_fox_messages_state(state)
    return {"status": "ok", "added": added, "audit_total": len(state.get("audit_log") or [])}


@app.get("/api/admin/group-activity/sheets-feed")
def admin_group_activity_sheets_feed(admin_secret: str, period: str = "week"):
    verify_admin_secret(admin_secret)
    return {"status": "ok", **build_group_activity_sheets_feed(period)}


@app.get("/api/admin/group-activity/export")
def admin_group_activity_export(
    admin_secret: str,
    period: str = "week",
    view: str = "full",
):
    verify_admin_secret(admin_secret)
    normalized_period = group_activity_period(period)
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d")
    if (view or "full").strip().lower() == "full":
        zip_bytes = build_group_activity_export_zip(normalized_period)
        filename = f"alcove-group-activity-{normalized_period}-{stamp}.zip"
        return Response(
            content=zip_bytes,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    csv_text = build_group_activity_csv(normalized_period, view=view)
    filename = f"alcove-group-activity-{normalized_period}-{view.strip().lower()}-{stamp}.csv"
    return Response(
        content=csv_text,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/admin/review-queue")
def admin_review_queue(admin_secret: str):
    verify_admin_secret(admin_secret)
    pending_questions = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "pending_review"
    ]
    reserved_questions = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "reserved"
    ]
    pending_spotlights = [
        entry for entry in spotlight_entries
        if entry.get("status") == "pending_review"
    ]
    today = pulse_day_key()
    recent_answers = []
    for entry in reversed(pulse_entries):
        if entry.get("status") != "completed":
            continue
        if entry.get("day_key") != today:
            continue
        recent_answers.append(public_pulse_payload(entry))
        if len(recent_answers) >= 40:
            break
    return {
        "status": "ok",
        "pulse_questions_pending": [entry for entry in pending_questions if entry],
        "pulse_questions_reserved": [entry for entry in reserved_questions if entry],
        "spotlights_pending": pending_spotlights,
        "pulse_answers_today": recent_answers,
    }


@app.post("/api/admin/pulse-questions/create")
def admin_create_pulse_question(payload: AdminPulseQuestionCreate):
    verify_admin_secret(payload.admin_secret)
    entry = create_admin_pulse_question(
        payload.question,
        payload.pool,
        payload.category,
        payload.schedule_mode,
    )
    return {"status": "ok", "entry": pulse_question_suggestion_admin_payload(entry)}


@app.post("/api/admin/pulse-questions/{suggestion_id}")
def admin_pulse_question_action(suggestion_id: int, payload: AdminPulseQuestionAction):
    verify_admin_secret(payload.admin_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    apply_admin_pulse_question_action(
        entry,
        payload.action,
        payload.edited_question,
        payload.rejection_reason,
    )
    save_runtime_state()
    return {"status": "ok", "entry": pulse_question_suggestion_admin_payload(entry)}


@app.post("/api/admin/spotlights/{entry_id}")
def admin_spotlight_action(entry_id: int, payload: AdminSpotlightAction):
    verify_admin_secret(payload.admin_secret)
    with spotlight_submit_lock:
        entry = get_spotlight_entry(entry_id)
        if not entry:
            return {"status": "error", "message": "Spotlight not found."}
        action = (payload.action or "").strip().lower()
        current_status = (entry.get("status") or "").strip().lower()
        if action == "amend":
            text = (payload.edited_reason or "").strip()
            if not text:
                raise HTTPException(status_code=400, detail="Edited Spotlight text is required.")
            entry["edited_reason"] = text
            entry["status"] = "pending_review"
            entry["review_message_sent"] = False
        elif action == "reject":
            if current_status == "approved" and entry.get("published_at"):
                return {"status": "error", "message": "Spotlight is already approved and published.", "entry": entry}
            entry["status"] = "rejected"
            entry["reviewed_at"] = now_iso()
            entry["publish_pending"] = False
        elif action == "approve":
            if current_status == "approved":
                return {"status": "ok", "entry": entry, "already_done": True}
            entry["status"] = "approved"
            entry["reviewed_at"] = now_iso()
            entry["publish_pending"] = True
        elif action == "delete":
            deleted = permanently_delete_spotlight_runtime_entry(entry_id)
            return {"status": "ok", "deleted": deleted}
        else:
            raise HTTPException(status_code=400, detail="Unknown Spotlight action.")
        save_runtime_state()
        return {"status": "ok", "entry": entry}


@app.post("/api/admin/verify-all-group-members")
def admin_queue_bulk_verify_group_members(admin_secret: str):
    verify_admin_secret(admin_secret)
    job = get_admin_job("bulk_verify_group")
    if job.get("status") == "pending":
        return {
            "status": "ok",
            "message": "Bulk verify is already queued. F.O.X will run it on the next sync cycle.",
            "job": job,
        }
    admin_jobs["bulk_verify_group"] = {
        "status": "pending",
        "requested_at": now_iso(),
        "source": "feature_admin",
        "completed_at": None,
        "result": None,
    }
    save_runtime_state()
    return {
        "status": "ok",
        "message": "Queued. F.O.X will mark every current group member as verified on the next sync cycle (about 60 seconds).",
        "job": admin_jobs["bulk_verify_group"],
    }


@app.get("/api/admin/verify-all-group-members")
def admin_bulk_verify_group_members_status(admin_secret: str):
    verify_admin_secret(admin_secret)
    return {"status": "ok", "job": get_admin_job("bulk_verify_group")}


@app.get("/api/bot-sync/admin-jobs")
def bot_pending_admin_jobs(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    jobs = []
    bulk_job = get_admin_job("bulk_verify_group")
    if bulk_job.get("status") == "pending":
        jobs.append({"job": "bulk_verify_group", **bulk_job})
    return {"status": "ok", "jobs": jobs}


@app.post("/api/bot-sync/admin-jobs/bulk-verify-group")
def bot_complete_bulk_verify_group_job(payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    current = get_admin_job("bulk_verify_group")
    admin_jobs["bulk_verify_group"] = {
        **current,
        "status": payload.get("status") or "completed",
        "completed_at": now_iso(),
        "result": payload.get("result") if isinstance(payload.get("result"), dict) else {},
        "error": payload.get("error"),
    }
    save_runtime_state()
    return {"status": "ok", "job": admin_jobs["bulk_verify_group"]}


@app.get("/api/debug/domains")
def debug_domains():
    return CONFIG["approved_video_domains"]

@app.get("/api/debug/wheel")
def debug_wheel_entries():
    return wheel_entries

@app.get("/api/debug/paths")
def debug_paths():
    return {
        "downloads_dir": DOWNLOADS_DIR,
        "ready_dir": READY_DIR,
        "archive_dir": ARCHIVE_DIR,
        "playout_file": CURRENT_PICK_PATH
    }

@app.get("/api/debug/downloads")
def debug_downloads():
    return [
        {
            "entry_id": e["id"],
            "name": e["data"].get("display_name"),
            "status": e.get("download_status"),
            "source": e.get("source_domain"),
            "file": e.get("local_filename"),
            "path": e.get("local_path")
        }
        for e in wheel_entries
    ]
# ---------------------------------
# Wheel submissions
# ---------------------------------

@app.post("/api/wheel-entry")
def submit_wheel(entry: WheelEntry):

    if state["round_status"] != "open":
        return {
            "status": "error",
            "message": "Video submissions are not active right now."
        }

    if not state["modules"].get("wheel", False):
        return {
            "status": "error",
            "message": "Wheel of Desire is inactive right now."
        }

    entry_data = entry.dict()

    if entry_data["display_name"].strip().lower() == "anonymous":
        entry_data["display_name"] = get_next_anonymous_wheel_name()

    user_key = entry_data["display_name"].lower()
    allowed = wheel_submission_limits.get(user_key, 1)

    current = sum(
        1
        for e in get_round_entries(state["current_round"])
        if wheel_entry_matches_submitter(
            e,
            telegram_id=entry_data.get("telegram_id"),
            username=entry_data.get("username"),
            display_name=entry_data.get("display_name"),
        )
        and e.get("approval_status") != "rejected"
    )

    if current >= allowed:
        return {"status": "error", "message": "You already have an entry in this round."}

    submitted_url = entry_data["link"]
    domain = normalize_domain(submitted_url)

    if not entry_data.get("video_title"):
        entry_data["video_title"] = derive_video_title_from_url(submitted_url)

    new_entry = {
        "id": len(wheel_entries) + len(archived_wheel_entries) + 1,
        "round_id": state["current_round"],
        "time": now_iso(),
        "played": False,
        "played_at": None,
        "data": entry_data,
        "submitted_url": submitted_url,
        "source_domain": domain,
        "direct_media_url": None,
        "download_status": "pending",
        "download_error": None,
        "download_method": None,
        "local_filename": None,
        "local_path": None,
        "download_started_at": None,
        "download_completed_at": None,
        "stream_candidate": None,
        "download_candidate": None,
        "processed_at": None,
        "clip_start_seconds": entry_data.get("clip_start_seconds"),
        "clip_end_seconds": (
            int(entry_data["clip_start_seconds"]) + 300
            if entry_data.get("clip_start_seconds") is not None
            else None
        ),
        "approval_status": "pending",
        "approval_time": None,
        "rejection_time": None,
        "reserved": False,
        "reserved_at": None,
    }

    wheel_entries.append(new_entry)
    add_notification("submission", f"{entry_data['display_name']} submitted a video", True)

    return {
        "status": "ok",
        "entry_id": new_entry["id"],
        "entries": len(get_round_entries(state["current_round"])),
        "message": "Thank you for submitting your video choice. The wheel will begin shortly. Good luck.",
    }


@app.get("/api/wheel-entries")
def list_wheel_entries():
    return wheel_entries


@app.get("/api/wheel-entries-host")
def list_wheel_entries_host():
    return [
        {
            "entry_id": entry["id"],
            "id": entry["id"],
            "round_id": entry["round_id"],
            "display_name": entry["data"].get("display_name"),
            "username": entry["data"].get("username"),
            "video_title": entry["data"].get("video_title"),
            "submitted_url": entry.get("submitted_url"),
            "source_domain": entry.get("source_domain"),
            "direct_media_url": entry.get("direct_media_url"),
            "download_status": entry.get("download_status"),
            "download_error": entry.get("download_error"),
            "download_method": entry.get("download_method"),
            "local_filename": entry.get("local_filename"),
            "local_path": entry.get("local_path"),
            "stream_candidate": entry.get("stream_candidate"),
            "download_candidate": entry.get("download_candidate"),
            "processed_at": entry.get("processed_at"),
            "clip_start_seconds": entry.get("clip_start_seconds"),
            "clip_end_seconds": entry.get("clip_end_seconds"),
            "played": entry.get("played", False),
            "played_at": entry.get("played_at"),
            "time": entry.get("time"),
            "approval_status": entry.get("approval_status", "pending"),
            "approval_time": entry.get("approval_time"),
            "rejection_time": entry.get("rejection_time"),
            "reserved": bool(entry.get("reserved")),
            "reserved_at": entry.get("reserved_at"),
            "data": entry.get("data"),
        }
        for entry in wheel_entries
    ]


@app.get("/api/wheel-entries-archived")
def list_archived_wheel_entries():
    return archived_wheel_entries


@app.post("/api/wheel-entry/resubmit/{entry_id}")
def resubmit_wheel_entry(entry_id: int):
    """Put a Reserve entry back onto the current wheel round."""
    for entry in wheel_entries:
        if int(entry.get("id") or 0) != int(entry_id):
            continue
        if entry.get("played", False):
            return {"status": "error", "message": "Played videos cannot be resubmitted to the wheel."}
        entry["reserved"] = False
        entry["reserved_at"] = None
        entry["round_id"] = state["current_round"]
        entry["approval_status"] = "approved"
        if not entry.get("approval_time"):
            entry["approval_time"] = now_iso()
        if state["round_status"] == "closed":
            state["round_status"] = "locked"
        ws_broadcast_bundle()
        return {
            "status": "ok",
            "entry_id": entry_id,
            "round_id": entry["round_id"],
            "message": "Entry returned to the current wheel.",
        }
    return {"status": "error", "message": "Entry not found."}


@app.post("/api/wheel-entry/delete/{entry_id}")
def delete_wheel_entry(entry_id: int):
    """Remove an entry completely without archiving it."""
    global current_winner, current_now_playing
    for i, entry in enumerate(wheel_entries):
        if int(entry.get("id") or 0) != int(entry_id):
            continue
        if current_now_playing and int(current_now_playing.get("id") or 0) == int(entry_id):
            current_now_playing = None
            video_reviews.clear()
        if current_winner and int(current_winner.get("entry_id") or 0) == int(entry_id):
            current_winner = None
        del wheel_entries[i]
        ws_broadcast_bundle()
        return {"status": "ok", "entry_id": entry_id, "message": "Entry deleted."}
    return {"status": "error", "message": "Entry not found."}


@app.get("/api/current-round-ready-entries")
def current_round_ready_entries():
    ready_entries = get_ready_unplayed_entries(state["current_round"])
    return [
        {
            "entry_id": entry["id"],
            "entrant_name": entry["data"].get("display_name", "Unknown"),
        }
        for entry in ready_entries
    ]


@app.post("/api/set-video-title/{entry_id}")
def set_video_title(entry_id: int, payload: dict):
    title = payload.get("video_title", "")
    global current_now_playing
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error"}

    entry["data"]["video_title"] = title
    if current_now_playing and current_now_playing["id"] == entry_id:
        current_now_playing = entry

    ws_broadcast_bundle()

    return {"status": "ok"}


# ---------------------------------
# Download queue / worker endpoints
# ---------------------------------

@app.get("/api/downloads/pending")
def list_pending_downloads():
    pending = [
        entry
        for entry in wheel_entries
        if entry.get("approval_status") == "approved"
        and entry.get("download_status") in {"pending", "failed"}
        and not entry.get("played", False)
    ]
    pending.sort(key=lambda e: (e["round_id"], e["id"]))
    return [
        {
            "entry_id": entry["id"],
            "display_name": entry["data"].get("display_name"),
            "video_title": entry["data"].get("video_title"),
            "submitted_url": entry.get("submitted_url"),
            "source_domain": entry.get("source_domain"),
            "download_status": entry.get("download_status"),
            "download_error": entry.get("download_error"),
            "approval_status": entry.get("approval_status", "pending"),
        }
        for entry in pending
    ]


@app.post("/api/entry/approve/{entry_id}")
def approve_entry(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["approval_status"] = "approved"
    entry["approval_time"] = now_iso()
    entry["rejection_time"] = None
    add_notification("system", f"Approved: {entry['data'].get('display_name', 'Unknown')}", False)
    ws_broadcast_bundle()
    return {"status": "ok", "entry_id": entry_id, "approval_status": "approved"}


@app.post("/api/entry/reject/{entry_id}")
def reject_entry(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["approval_status"] = "rejected"
    entry["approval_time"] = None
    entry["rejection_time"] = now_iso()
    entry["download_status"] = "rejected"
    entry["download_error"] = None
    entry["download_started_at"] = None
    entry["download_completed_at"] = None
    entry["direct_media_url"] = None
    entry["local_filename"] = None
    entry["local_path"] = None
    entry["download_method"] = None
    clear_processed_candidates(entry)
    add_notification("system", f"Rejected: {entry['data'].get('display_name', 'Unknown')}", False)
    ws_broadcast_bundle()
    return {"status": "ok", "entry_id": entry_id, "approval_status": "rejected"}


@app.post("/api/downloads/start/{entry_id}")
def mark_download_start(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "extracting"
    entry["download_started_at"] = now_iso()
    entry["download_error"] = None
    clear_processed_candidates(entry)
    return {"status": "ok"}


@app.post("/api/downloads/downloading/{entry_id}")
def mark_downloading(entry_id: int, payload: dict | None = None):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "downloading"
    if payload and payload.get("direct_media_url"):
        entry["direct_media_url"] = payload["direct_media_url"]
    return {"status": "ok"}


@app.post("/api/downloads/complete/{entry_id}")
def mark_download_complete(entry_id: int, payload: DownloadCompletePayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "ready"
    entry["download_error"] = None
    entry["local_filename"] = payload.local_filename
    entry["local_path"] = payload.local_path
    entry["direct_media_url"] = payload.direct_media_url
    entry["download_method"] = payload.download_method
    entry["download_completed_at"] = now_iso()
    if payload.video_title:
        entry["data"]["video_title"] = payload.video_title
    clear_processed_candidates(entry)
    return {"status": "ok"}


@app.post("/api/downloads/processed/{entry_id}")
def mark_download_processed(entry_id: int, payload: DownloadProcessedPayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    stream_candidate = payload.stream_candidate if isinstance(payload.stream_candidate, dict) else None
    download_candidate = payload.download_candidate if isinstance(payload.download_candidate, dict) else None
    if not stream_candidate and not download_candidate:
        return {"status": "error", "message": "no stream or download option was produced"}

    entry["download_status"] = "processed"
    entry["download_error"] = None
    entry["download_method"] = payload.process_method or "process-link"
    entry["stream_candidate"] = stream_candidate
    entry["download_candidate"] = download_candidate
    entry["processed_at"] = now_iso()
    entry["download_completed_at"] = None
    entry["direct_media_url"] = None
    entry["local_filename"] = None
    entry["local_path"] = None
    if payload.video_title:
        entry["data"]["video_title"] = payload.video_title
    ws_broadcast_bundle()
    return {"status": "ok", "entry": entry}


@app.post("/api/downloads/select-stream/{entry_id}")
def select_stream_ready(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}
    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    candidate = entry.get("stream_candidate") or {}
    media_url = str(candidate.get("direct_media_url") or candidate.get("media_url") or "").strip()
    if not media_url:
        return {"status": "error", "message": "no stream option is available"}

    title = candidate.get("video_title") or candidate.get("title")
    clip_start_seconds = candidate.get("clip_start_seconds") if candidate.get("clip_start_seconds") is not None else entry.get("clip_start_seconds")
    clip_end_seconds = candidate.get("clip_end_seconds") if candidate.get("clip_end_seconds") is not None else entry.get("clip_end_seconds")
    entry["download_status"] = "ready"
    entry["download_error"] = None
    entry["direct_media_url"] = media_url
    entry["local_filename"] = ""
    entry["local_path"] = ""
    entry["download_method"] = candidate.get("download_method") or candidate.get("resolve_strategy") or "stream-ready"
    entry["download_completed_at"] = now_iso()
    if title:
        entry["data"]["video_title"] = title
    entry["clip_start_seconds"] = clip_start_seconds
    entry["clip_end_seconds"] = clip_end_seconds
    clear_processed_candidates(entry)
    ws_broadcast_bundle()
    return {"status": "ok", "entry": entry}


@app.post("/api/downloads/select-download/{entry_id}")
def select_download_ready(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}
    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    candidate = entry.get("download_candidate") or {}
    local_path = str(candidate.get("local_path") or "").strip()
    local_filename = str(candidate.get("local_filename") or os.path.basename(local_path)).strip()
    if not local_path:
        return {"status": "error", "message": "no download option is available"}

    title = candidate.get("video_title") or candidate.get("title")
    clip_start_seconds = candidate.get("clip_start_seconds") if candidate.get("clip_start_seconds") is not None else entry.get("clip_start_seconds")
    clip_end_seconds = candidate.get("clip_end_seconds") if candidate.get("clip_end_seconds") is not None else entry.get("clip_end_seconds")
    entry["download_status"] = "manual_ready"
    entry["download_error"] = None
    entry["direct_media_url"] = None
    entry["local_filename"] = local_filename
    entry["local_path"] = local_path
    entry["download_method"] = candidate.get("download_method") or "download-ready"
    entry["download_completed_at"] = now_iso()
    if title:
        entry["data"]["video_title"] = title
    entry["clip_start_seconds"] = clip_start_seconds
    entry["clip_end_seconds"] = clip_end_seconds
    clear_processed_candidates(entry)
    ws_broadcast_bundle()
    return {"status": "ok", "entry": entry}


@app.post("/api/downloads/failed/{entry_id}")
def mark_download_failed(entry_id: int, payload: DownloadFailedPayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "failed"
    entry["download_error"] = payload.error
    entry["download_method"] = "auto"
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/downloads/manual-ready/{entry_id}")
def mark_manual_ready(entry_id: int, payload: ManualReadyPayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "manual_ready"
    entry["download_error"] = None
    entry["local_filename"] = payload.local_filename
    entry["local_path"] = payload.local_path
    entry["download_method"] = "manual"
    entry["download_completed_at"] = now_iso()
    if payload.video_title:
        entry["data"]["video_title"] = payload.video_title
    clear_processed_candidates(entry)
    return {"status": "ok"}


@app.post("/api/downloads/manual-ready-by-filename/{entry_id}")
def mark_manual_ready_by_filename(entry_id: int, payload: ManualReadyByFilenamePayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    raw_name = (payload.filename or "").strip().strip('"')
    if not raw_name:
        return {"status": "error", "message": "filename required"}

    source_path = None
    search_dirs = [READY_DIR, DOWNLOADS_DIR]
    for directory in search_dirs:
        candidate = os.path.join(directory, raw_name)
        if os.path.exists(candidate) and os.path.isfile(candidate):
            source_path = candidate
            break

    if source_path is None:
        return {"status": "error", "message": "file not found in Ready or Downloads"}

    return assign_local_file_to_entry(entry, source_path, payload.video_title)


@app.post("/api/downloads/manual-ready-latest/{entry_id}")
def mark_manual_ready_latest(entry_id: int, payload: dict | None = None):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    latest_path = latest_video_in_downloads()
    if latest_path is None:
        return {"status": "error", "message": "no video files found in Downloads"}

    video_title = None
    if isinstance(payload, dict):
        video_title = payload.get("video_title")

    return assign_local_file_to_entry(entry, latest_path, video_title)


@app.post("/api/downloads/retry/{entry_id}")
def retry_download(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "entry is not approved"}

    entry["download_status"] = "pending"
    entry["download_error"] = None
    entry["direct_media_url"] = None
    entry["download_started_at"] = None
    entry["download_completed_at"] = None
    clear_processed_candidates(entry)
    return {"status": "ok"}


# ---------------------------------
# Winner / spin result
# ---------------------------------

@app.post("/api/spin-result")
def set_spin_result(payload: dict):
    global current_winner
    entry_id = payload.get("entry_id")
    if entry_id is None:
        return {"status": "error", "message": "entry_id required"}

    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "winner entry not found"}

    if not entry_is_approved(entry):
        return {"status": "error", "message": "winner entry is not approved"}

    state["winner_intro_loaded"] = False
    current_winner = {
        "entry_id": entry["id"],
        "entrant_name": entry["data"].get("display_name", "Unknown"),
        "video_title": entry["data"].get("video_title"),
        "local_filename": entry.get("local_filename"),
        "local_path": entry.get("local_path"),
        "submitted_url": entry.get("submitted_url") or entry.get("data", {}).get("link"),
        "clip_start_seconds": entry.get("clip_start_seconds"),
        "clip_end_seconds": entry.get("clip_end_seconds"),
        "time": now_iso(),
    }
    add_notification("winner", f"Winner: {current_winner['entrant_name']}", True)

    ws_broadcast_bundle()
    return {"status": "ok", "winner": current_winner}


@app.post("/api/winner/clear")
def clear_winner():
    global current_winner
    current_winner = None
    state["winner_intro_loaded"] = False
    return {"status": "ok"}


@app.get("/api/current-winner")
def get_current_winner():
    return current_winner


@app.post("/api/winner/intro-loaded/{entry_id}")
def mark_winner_intro_loaded(entry_id: int):
    if not current_winner or int(current_winner.get("entry_id") or 0) != int(entry_id):
        return {"status": "error", "message": "winner does not match"}
    state["winner_intro_loaded"] = True
    ws_broadcast_bundle()
    return {"status": "ok", "entry_id": entry_id, "winner_intro_loaded": True}


# ---------------------------------
# Now playing / played state / playout
# ---------------------------------

@app.post("/api/playout/load/{entry_id}")
def load_for_playout(entry_id: int, payload: PayoutPayload | None = None):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    source_path = payload.copy_from_path if payload and payload.copy_from_path else entry.get("local_path")
    if not source_path or not os.path.exists(source_path):
        return {"status": "error", "message": "local file not found"}

    shutil.copyfile(source_path, CURRENT_PICK_PATH)
    return {"status": "ok", "current_pick_path": CURRENT_PICK_PATH}


@app.post("/api/set-now-playing/{entry_id}")
def set_now_playing(entry_id: int):
    global current_now_playing
    video_reviews.clear()
    state["round_status"] = "playing"
    state["review_prompt_open"] = False
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error"}

    current_now_playing = entry

    ws_broadcast_bundle()

    return {"status": "ok"}


@app.get("/api/now-playing")
def get_now_playing():
    return current_now_playing


@app.post("/api/mark-played/{entry_id}")
def mark_played(entry_id: int):
    global current_now_playing
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error"}

    entry["played"] = True
    entry["played_at"] = now_iso()
    current_now_playing = entry
    state["round_status"] = "playing"
    add_notification("system", f"Played: {entry['data'].get('display_name', 'Unknown')}", False)

    ws_broadcast_bundle()
    return {"status": "ok", "message": "Played saved"}


# ---------------------------------
# Reviews
# ---------------------------------

@app.post("/api/review")
def submit_review(review: VideoReview):
    global current_now_playing, latest_review_overlay
    if current_now_playing is None or "data" not in current_now_playing:
        return {"status": "error", "message": "Reviews are closed until a video is live."}

    current_data = current_now_playing["data"]
    video_title = current_data.get("video_title", "") or "No video title set yet"
    chosen_by = current_data.get("display_name", "") or "Unknown"
    video_entry_id = current_now_playing.get("id")

    # One review per viewer per video — blocks refresh re-submits.
    reviewer_id = review.user_id
    if reviewer_id is not None and video_entry_id is not None:
        for existing in video_reviews:
            if int(existing.get("video_entry_id") or 0) != int(video_entry_id):
                continue
            if int(existing.get("user_id") or 0) == int(reviewer_id):
                return {
                    "status": "error",
                    "message": "You already submitted a review for this video.",
                }

    real_display_name = (review.display_name or "").strip() or "Unknown"
    reviewer_name = "Anonymous" if review.anonymous else real_display_name
    review_record = {
        "video_entry_id": video_entry_id,
        "video_title": video_title,
        "chosen_by": chosen_by,
        "rating": review.rating,
        "review": review.review,
        "display_name": reviewer_name,
        "real_display_name": real_display_name,
        "user_id": review.user_id,
        "username": review.username,
        "anonymous": review.anonymous,
        "time": now_iso(),
    }
    video_reviews.append(review_record)
    wheel_review_history.append(dict(review_record))
    trim_list_in_place(wheel_review_history, MAX_WHEEL_REVIEW_HISTORY)
    stats = ensure_wheel_user_engagement(
        user_id=review.user_id,
        username=review.username,
        display_name=real_display_name,
    )
    if stats is not None:
        stats["total_reviews"] = int(stats.get("total_reviews") or 0) + 1
        stats["last_active_at"] = review_record["time"]
    save_runtime_state()

    latest_review_overlay = None
    ws_broadcast_bundle()

    return {"status": "ok", "reviews": len(video_reviews)}


@app.post("/api/review/open")
def open_review_prompt():
    global current_now_playing
    if current_now_playing is None and current_winner:
        entry_id = current_winner.get("entry_id")
        if entry_id:
            entry = find_entry(entry_id)
            if entry:
                current_now_playing = entry
    state["review_prompt_open"] = True
    state["round_status"] = "reviewing"
    state["review_score_reveal_active"] = False
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/review/close")
def close_review_prompt():
    state["review_prompt_open"] = False
    if current_now_playing:
        state["round_status"] = "reviewing"
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/reviews/reveal/start")
def start_review_reveal():
    state["review_reveal_active"] = True
    state["review_score_reveal_active"] = False
    ws_broadcast_bundle()
    return {"status": "ok", "reviews": len(video_reviews)}


@app.post("/api/reviews/reveal/stop")
def stop_review_reveal():
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/reviews/reveal/score")
def reveal_review_score():
    state["review_reveal_active"] = True
    state["review_score_reveal_active"] = True
    ws_broadcast_bundle()
    return {"status": "ok", "average": review_average_rating(), "reviews": len(video_reviews)}


@app.post("/api/reviews/reveal/hide")
def hide_review_results():
    state["review_reveal_active"] = False
    state["review_score_reveal_active"] = False
    if current_now_playing:
        state["round_status"] = "reviewing"
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.get("/api/reviews")
def list_reviews():
    return video_reviews


# ---------------------------------
# Stream comments moderation
# ---------------------------------

@app.post("/api/stream-comment")
def submit_stream_comment(comment: StreamComment, authorization: str | None = Header(default=None)):
    apply_authorization_identity(comment, authorization)
    # Live Feed copy is single-line and capped to ~3 wrapped lines at max bubble width.
    text = " ".join(str(comment.text or "").split())
    if len(text) == 0:
        return {"status": "error", "message": "Comment cannot be empty."}
    if len(text) > 140:
        return {"status": "error", "message": "Comments must be 140 characters or fewer."}

    display_name = (comment.display_name or "Viewer").strip() or "Viewer"
    if display_name.lower() in muted_users:
        return {"status": "error", "message": "Chat is currently muted for this name."}

    user_identifier = str(comment.user_id or display_name or "").strip()
    if user_identifier:
        recent_comments = [
            c
            for c in approved_comments
            if str(c.get("user_id") or c.get("display_name") or "").strip() == user_identifier
        ]
        if recent_comments:
            latest = sorted(recent_comments, key=lambda x: str(x.get("time") or ""), reverse=True)[0]
            latest_time = parse_iso_utc(latest.get("time"))
            if latest_time is not None:
                seconds_since = (datetime.datetime.utcnow() - latest_time).total_seconds()
                if seconds_since < 4:
                    return {"status": "error", "message": "Please wait a moment before sending another comment."}

    feed_style = comment.feed_style if isinstance(comment.feed_style, dict) else None
    approved_comments.append(
        {
            "comment_id": get_next_comment_id(),
            "user_id": comment.user_id,
            "username": comment.username,
            "display_name": display_name,
            "text": text,
            "time": now_iso(),
            "approved": True,
            "feed_style": feed_style,
        }
    )
    trim_list_in_place(approved_comments, MAX_APPROVED_COMMENTS)
    add_notification("comment", f"{display_name}: {text}", False)
    ws_broadcast_bundle()
    return {"status": "ok", "message": "Message sent."}


@app.get("/api/comments/pending")
def get_pending_comments():
    return pending_comments


@app.get("/api/comments/approved")
def get_approved_comments():
    return approved_comments


@app.post("/api/comments/approve/{comment_id}")
def approve_comment(comment_id: int):
    for index, comment in enumerate(pending_comments):
        if comment["comment_id"] == comment_id:
            approved = dict(comment)
            approved["approved"] = True
            approved["approved_at"] = now_iso()
            approved_comments.append(approved)
            trim_list_in_place(approved_comments, MAX_APPROVED_COMMENTS)
            add_notification("comment", f"{approved['display_name']}: {approved['text']}", True)
            del pending_comments[index]
            return {"status": "ok"}
    return {"status": "error"}


@app.post("/api/comments/reject/{comment_id}")
def reject_comment(comment_id: int):
    for index, comment in enumerate(pending_comments):
        if comment["comment_id"] == comment_id:
            del pending_comments[index]
            return {"status": "ok"}
    return {"status": "error"}


@app.post("/api/wheel-reaction")
def submit_wheel_reaction(payload: WheelReaction):
    global current_wheel_reaction, wheel_reaction_events
    allowed = {"fire", "shock", "hot", "love", "wild"}
    reaction_key = (payload.reaction_key or "").strip().lower()
    if reaction_key not in allowed:
        return {"status": "error", "message": "Unknown reaction."}

    current_wheel_reaction = {
        "reaction_key": reaction_key,
        "display_name": payload.display_name,
        "username": payload.username,
        "user_id": payload.user_id,
        "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(seconds=3)).isoformat(),
        "time": now_iso(),
    }
    wheel_reaction_events.append(dict(current_wheel_reaction))
    wheel_reaction_events = current_active_wheel_reactions()
    current_entry_id = current_video_entry_id()
    stats = ensure_wheel_user_engagement(
        user_id=payload.user_id,
        username=payload.username,
        display_name=payload.display_name,
    )
    if stats is not None:
        stats["total_reactions"] = int(stats.get("total_reactions") or 0) + 1
        stats["last_active_at"] = current_wheel_reaction["time"]
    wheel_reaction_history.append({
        "video_entry_id": current_entry_id,
        "reaction_key": reaction_key,
        "display_name": payload.display_name,
        "username": payload.username,
        "user_id": payload.user_id,
        "time": current_wheel_reaction["time"],
    })
    trim_list_in_place(wheel_reaction_history, MAX_WHEEL_REACTION_HISTORY)
    save_runtime_state()
    ws_broadcast_bundle()
    return {"status": "ok", "reaction": current_wheel_reaction}


# ---------------------------------
# Live Room — discussion, Q&A, polls, media
# ---------------------------------

def _next_room_qa_id() -> int:
    global _next_room_qa_id_seq
    value = _next_room_qa_id_seq
    _next_room_qa_id_seq += 1
    return value


def _next_poll_id() -> int:
    global _next_poll_id_seq
    value = _next_poll_id_seq
    _next_poll_id_seq += 1
    return value


def _next_media_id() -> int:
    global _next_media_id_seq
    value = _next_media_id_seq
    _next_media_id_seq += 1
    return value


def room_discussion_payload() -> dict:
    return state.get("room_discussion") or {
        "discussion_id": None,
        "title": "",
        "duration_minutes": None,
        "started_at": None,
        "ends_at": None,
        "status": "idle",
    }


def current_discussion_id() -> str | None:
    discussion = room_discussion_payload()
    if discussion.get("status") != "active":
        return None
    return discussion.get("discussion_id")


def discussion_meta() -> dict:
    discussion = room_discussion_payload()
    return {
        "discussion_id": discussion.get("discussion_id"),
        "discussion_title": discussion.get("title") or "",
        "discussion_started_at": discussion.get("started_at"),
    }


def archive_active_poll() -> None:
    global active_poll, poll_history
    if not active_poll:
        return
    archived = dict(active_poll)
    archived["archived_at"] = now_iso()
    poll_history.append(archived)
    poll_history[:] = poll_history[-200:]
    active_poll = None


@app.post("/api/room-discussion/start")
def start_room_discussion(payload: RoomDiscussionStart):
    title = (payload.title or "").strip()
    if not title:
        return {"status": "error", "message": "Discussion title is required."}
    duration = int(payload.duration_minutes)
    if duration not in ROOM_DISCUSSION_DURATIONS:
        return {"status": "error", "message": "Duration must be 5, 10, 20, 30, 45, or 60 minutes."}
    started = datetime.datetime.utcnow()
    ends = started + datetime.timedelta(minutes=duration)
    discussion_id = f"disc_{uuid.uuid4().hex[:12]}"
    state["room_discussion"] = {
        "discussion_id": discussion_id,
        "title": title,
        "duration_minutes": duration,
        "started_at": started.isoformat() + "Z",
        "ends_at": ends.isoformat() + "Z",
        "status": "active",
    }
    add_notification("system", f"Discussion started: {title}", True)
    persist_live_room()
    return {"status": "ok", "room_discussion": state["room_discussion"]}


@app.post("/api/room-discussion/end")
def end_room_discussion():
    discussion = room_discussion_payload()
    if discussion.get("status") != "active":
        return {"status": "error", "message": "No active discussion to end."}
    archive_active_poll()
    state["room_discussion"] = {
        **discussion,
        "status": "ended",
        "ends_at": now_iso(),
        "ended_at": now_iso(),
    }
    add_notification("system", "Discussion ended", True)
    persist_live_room()
    return {"status": "ok", "room_discussion": state["room_discussion"]}


@app.get("/api/room-qa")
def list_room_qa(archived: bool = False):
    if archived:
        return room_qa_archive
    return [item for item in room_qa_items if item.get("status") != "archived"]


@app.post("/api/room-qa")
def submit_room_qa(payload: RoomQASubmit):
    global room_qa_items
    discussion_id = current_discussion_id()
    if not discussion_id:
        return {"status": "error", "message": "No active discussion right now."}
    question = (payload.question or "").strip()
    if not question:
        return {"status": "error", "message": "Question cannot be empty."}
    if len(question) > 300:
        return {"status": "error", "message": "Question must be 300 characters or fewer."}
    display_name = (payload.display_name or "Viewer").strip() or "Viewer"
    meta = discussion_meta()
    item = {
        "id": _next_room_qa_id(),
        "discussion_id": discussion_id,
        "discussion_title": meta.get("discussion_title") or "",
        "user_id": payload.user_id,
        "username": payload.username,
        "display_name": display_name,
        "question": question,
        "answer": None,
        "status": "active",
        "created_at": now_iso(),
    }
    room_qa_items.append(item)
    room_qa_items[:] = room_qa_items[-100:]
    persist_live_room()
    return {"status": "ok", "item": item}


@app.post("/api/room-qa/{item_id}/archive")
def archive_room_qa(item_id: int):
    global room_qa_items, room_qa_archive
    for index, item in enumerate(room_qa_items):
        if int(item.get("id") or 0) == int(item_id):
            archived = dict(item)
            archived["status"] = "archived"
            archived["archived_at"] = now_iso()
            room_qa_archive.insert(0, archived)
            room_qa_archive[:] = room_qa_archive[:200]
            del room_qa_items[index]
            persist_live_room()
            return {"status": "ok", "item": archived}
    return {"status": "error", "message": "Question not found."}


@app.post("/api/room-qa/{item_id}/restore")
def restore_room_qa(item_id: int):
    global room_qa_items, room_qa_archive
    for index, item in enumerate(room_qa_archive):
        if int(item.get("id") or 0) == int(item_id):
            restored = dict(item)
            restored["status"] = "active"
            restored["restored_at"] = now_iso()
            room_qa_items.insert(0, restored)
            del room_qa_archive[index]
            persist_live_room()
            return {"status": "ok", "item": restored}
    return {"status": "error", "message": "Archived question not found."}


@app.post("/api/room-qa/{item_id}/answer")
def answer_room_qa(item_id: int, payload: dict | None = None):
    answer = str((payload or {}).get("answer") or "").strip()
    if not answer:
        return {"status": "error", "message": "Answer cannot be empty."}
    for item in room_qa_items:
        if int(item.get("id") or 0) == int(item_id):
            item["answer"] = answer
            item["answered_at"] = now_iso()
            persist_live_room()
            return {"status": "ok", "item": item}
    return {"status": "error", "message": "Question not found."}


@app.post("/api/room-poll/create")
def create_room_poll(payload: RoomPollCreate):
    global active_poll
    discussion_id = current_discussion_id()
    if not discussion_id:
        return {"status": "error", "message": "Start a discussion before creating a poll."}
    question = (payload.question or "").strip()
    options = [str(opt).strip() for opt in (payload.options or []) if str(opt).strip()]
    if not question:
        return {"status": "error", "message": "Poll question is required."}
    if len(options) < 2 or len(options) > 6:
        return {"status": "error", "message": "Polls need between 2 and 6 options."}
    meta = discussion_meta()
    poll_id = str(_next_poll_id())
    active_poll = {
        "id": poll_id,
        "discussion_id": discussion_id,
        "discussion_title": meta.get("discussion_title") or "",
        "question": question,
        "options": [{"id": f"opt_{index + 1}", "text": text} for index, text in enumerate(options)],
        "votes": {},
        "status": "draft",
        "created_at": now_iso(),
    }
    persist_live_room()
    return {"status": "ok", "poll": active_poll}


@app.post("/api/room-poll/open")
def open_room_poll():
    if not active_poll:
        return {"status": "error", "message": "No poll to open."}
    active_poll["status"] = "open"
    active_poll["opened_at"] = now_iso()
    persist_live_room()
    return {"status": "ok", "poll": active_poll}


@app.post("/api/room-poll/close")
def close_room_poll():
    if not active_poll:
        return {"status": "error", "message": "No poll to close."}
    active_poll["status"] = "closed"
    active_poll["closed_at"] = now_iso()
    persist_live_room()
    return {"status": "ok", "poll": active_poll}


@app.post("/api/room-poll/reveal")
def reveal_room_poll():
    if not active_poll:
        return {"status": "error", "message": "No poll to reveal."}
    active_poll["status"] = "revealed"
    active_poll["revealed_at"] = now_iso()
    persist_live_room()
    return {"status": "ok", "poll": active_poll}


@app.post("/api/room-poll/archive")
def archive_room_poll():
    archive_active_poll()
    persist_live_room()
    return {"status": "ok", "poll_history": poll_history[-20:]}


@app.get("/api/room-poll/history")
def list_poll_history():
    return poll_history[-50:]


@app.post("/api/room-poll/vote")
def vote_room_poll(payload: RoomPollVote, authorization: str | None = Header(default=None)):
    apply_authorization_identity(payload, authorization)
    if not active_poll or active_poll.get("status") != "open":
        return {"status": "error", "message": "No open poll right now."}
    option_id = (payload.option_id or "").strip()
    valid_ids = {opt["id"] for opt in active_poll.get("options") or []}
    if option_id not in valid_ids:
        return {"status": "error", "message": "Invalid poll option."}
    user_key = str(payload.user_id or "").strip()
    if not user_key:
        return {"status": "error", "message": "User identity required to vote."}
    votes = active_poll.setdefault("votes", {})
    for voted_option, voters in votes.items():
        if user_key in [str(v) for v in voters]:
            return {"status": "error", "message": "You already voted in this poll."}
    voters = votes.setdefault(option_id, [])
    voters.append(user_key)
    persist_live_room()
    return {"status": "ok", "poll": active_poll}


def find_room_media(media_id: int):
    for item in room_media_submissions:
        if int(item.get("id") or 0) == int(media_id):
            return item
    return None


@app.post("/api/room-media/upload")
async def upload_room_media(
    user_id: int = Form(...),
    display_name: str = Form("Viewer"),
    text: str = Form(""),
    anonymous: str = Form("0"),
    file: UploadFile = File(...),
):
    global room_media_submissions
    original_name = Path(file.filename or "upload.bin").name
    ext = Path(original_name).suffix.lower()
    if ext not in ROOM_MEDIA_ALLOWED_EXTENSIONS:
        return {"status": "error", "message": "Unsupported file type. Use a picture or video."}
    media_id = _next_media_id()
    stored_name = f"{media_id}{ext}"
    stored_path = os.path.join(ROOM_MEDIA_DIR, stored_name)
    total = 0
    try:
        with open(stored_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > ROOM_MEDIA_MAX_BYTES:
                    out.close()
                    os.remove(stored_path)
                    return {"status": "error", "message": "File too large (max 100 MB)."}
                out.write(chunk)
    except OSError:
        return {"status": "error", "message": "Could not save upload."}
    kind = "video" if ext in {".mp4", ".mov", ".webm", ".m4v"} else "image"
    meta = discussion_meta()
    item = {
        "id": media_id,
        "discussion_id": meta.get("discussion_id"),
        "discussion_title": meta.get("discussion_title") or "",
        "user_id": user_id,
        "display_name": (display_name or "Viewer").strip() or "Viewer",
        "anonymous": str(anonymous).strip().lower() in {"1", "true", "yes", "on"},
        "caption": (text or "").strip()[:220],
        "filename": stored_name,
        "kind": kind,
        "status": "pending",
        "created_at": now_iso(),
    }
    room_media_submissions.append(item)
    room_media_submissions[:] = room_media_submissions[-50:]
    add_notification("system", f"Media upload from {item['display_name']}", False)
    persist_live_room()
    return {"status": "ok", "item": item}


@app.post("/api/room-media/approve/{media_id}")
def approve_room_media(media_id: int, payload: dict | None = None):
    item = find_room_media(media_id)
    if not item:
        return {"status": "error", "message": "Upload not found."}
    if payload and "anonymous" in payload:
        item["anonymous"] = bool(payload.get("anonymous"))
    item["status"] = "approved"
    item["approved_at"] = now_iso()
    persist_live_room()
    return {"status": "ok", "item": item}


@app.post("/api/room-media/reject/{media_id}")
def reject_room_media(media_id: int):
    item = find_room_media(media_id)
    if not item:
        return {"status": "error", "message": "Upload not found."}
    item["status"] = "rejected"
    item["rejected_at"] = now_iso()
    persist_live_room()
    return {"status": "ok", "item": item}


@app.post("/api/room-media/show/{media_id}")
def show_room_media(media_id: int, payload: dict | None = None):
    global now_showing_media
    item = find_room_media(media_id)
    if not item or item.get("status") != "approved":
        return {"status": "error", "message": "Approved media not found."}
    payload = payload or {}
    anonymous = payload.get("anonymous") if "anonymous" in payload else item.get("anonymous", False)
    anonymous = bool(anonymous)
    display_name = "Anonymous" if anonymous else (item.get("display_name") or "Viewer")
    now_showing_media = {
        "id": item["id"],
        "url": f"/api/room-media/file/{item['id']}",
        "kind": item.get("kind") or "image",
        "caption": item.get("caption") or "",
        "display_name": display_name,
        "anonymous": anonymous,
        "shown_at": now_iso(),
    }
    persist_live_room()
    return {"status": "ok", "now_showing_media": now_showing_media}


@app.post("/api/room-media/clear")
def clear_room_media():
    global now_showing_media
    now_showing_media = None
    persist_live_room()
    return {"status": "ok"}


@app.get("/api/room-media/file/{media_id}")
def get_room_media_file(media_id: int):
    item = find_room_media(media_id)
    if not item:
        raise HTTPException(status_code=404, detail="File not found")
    path = os.path.join(ROOM_MEDIA_DIR, item.get("filename") or "")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    media_types = {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
        ".mp4": "video/mp4",
        ".mov": "video/quicktime",
        ".webm": "video/webm",
        ".m4v": "video/mp4",
    }
    ext = Path(path).suffix.lower()
    return FileResponse(path, media_type=media_types.get(ext, "application/octet-stream"))


# ---------------------------------
# Live Room — team games
# ---------------------------------

def _next_team_msg_id() -> int:
    global _next_team_msg_id_seq
    value = _next_team_msg_id_seq
    _next_team_msg_id_seq += 1
    return value


def room_game_payload() -> dict | None:
    return room_game


def empty_room_game(title: str, mode: str, max_size: int) -> dict:
    return {
        "game_id": f"game_{uuid.uuid4().hex[:12]}",
        "title": title,
        "mode": mode,
        "max_size": max_size,
        "status": "pooling",
        "pool": [],
        "teams": [],
        "overlay_visible": False,
        "created_at": now_iso(),
        "assigned_at": None,
        "ended_at": None,
    }


def team_label(index: int) -> str:
    letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if index < len(letters):
        return f"Team {letters[index]}"
    return f"Team {index + 1}"


def split_players_into_teams(players: list, max_size: int) -> list[list]:
    """Fill as many max-size groups as possible; remainder becomes a smaller group.
    Avoid leaving a single player alone by borrowing from the previous group."""
    if max_size < 2:
        max_size = 2
    shuffled = list(players)
    random.shuffle(shuffled)
    n = len(shuffled)
    if n == 0:
        return []
    if n <= max_size:
        return [shuffled]

    sizes = []
    remaining = n
    while remaining > max_size:
        sizes.append(max_size)
        remaining -= max_size
    if remaining:
        sizes.append(remaining)

    # Avoid a lonely remainder of 1 when possible
    if len(sizes) >= 2 and sizes[-1] == 1:
        sizes[-2] -= 1
        sizes[-1] = 2

    teams = []
    cursor = 0
    for size in sizes:
        teams.append(shuffled[cursor:cursor + size])
        cursor += size
    return teams


def find_player_team(user_id) -> dict | None:
    if not room_game:
        return None
    key = str(user_id or "")
    for team in room_game.get("teams") or []:
        for member in team.get("members") or []:
            if str(member.get("user_id") or "") == key:
                return team
    return None


@app.post("/api/room-game/start")
def start_room_game(payload: RoomGameStart):
    global room_game, team_feeds
    mode = (payload.mode or "display").strip().lower()
    if mode not in ROOM_GAME_MODES:
        return {"status": "error", "message": "Mode must be display or collaborate."}
    max_size = int(payload.max_size)
    if max_size not in ROOM_GAME_MAX_SIZES:
        return {"status": "error", "message": "Max group size must be 2, 3, 4, or 5."}
    title = (payload.title or "Teams").strip() or "Teams"
    room_game = empty_room_game(title, mode, max_size)
    team_feeds = {}
    add_notification("system", f"Game open: {title}", True)
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/config")
def configure_room_game(payload: RoomGameConfig):
    global room_game
    if not room_game or room_game.get("status") not in {"pooling", "assigned"}:
        return {"status": "error", "message": "Start a game first."}
    if room_game.get("status") == "assigned" and payload.max_size is not None:
        return {"status": "error", "message": "Teams already assigned. End the game to change size."}
    if payload.title is not None:
        room_game["title"] = (payload.title or "").strip() or room_game.get("title") or "Teams"
    if payload.mode is not None:
        mode = payload.mode.strip().lower()
        if mode not in ROOM_GAME_MODES:
            return {"status": "error", "message": "Mode must be display or collaborate."}
        room_game["mode"] = mode
    if payload.max_size is not None:
        max_size = int(payload.max_size)
        if max_size not in ROOM_GAME_MAX_SIZES:
            return {"status": "error", "message": "Max group size must be 2, 3, 4, or 5."}
        room_game["max_size"] = max_size
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/join")
def join_room_game(payload: RoomGameJoin):
    global room_game
    if not room_game or room_game.get("status") != "pooling":
        return {"status": "error", "message": "No open join pool right now."}
    display_name = (payload.display_name or "Viewer").strip() or "Viewer"
    user_key = str(payload.user_id or "").strip()
    if not user_key:
        return {"status": "error", "message": "User identity required."}
    pool = room_game.setdefault("pool", [])
    for player in pool:
        if str(player.get("user_id") or "") == user_key:
            player["display_name"] = display_name
            player["username"] = payload.username
            persist_live_room()
            return {"status": "ok", "room_game": room_game, "already_joined": True}
    pool.append({
        "user_id": payload.user_id,
        "username": payload.username,
        "display_name": display_name,
        "joined_at": now_iso(),
    })
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/leave")
def leave_room_game(payload: RoomGameJoin):
    global room_game
    if not room_game or room_game.get("status") != "pooling":
        return {"status": "error", "message": "No open join pool right now."}
    user_key = str(payload.user_id or "").strip()
    pool = room_game.setdefault("pool", [])
    room_game["pool"] = [player for player in pool if str(player.get("user_id") or "") != user_key]
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/clear-pool")
def clear_room_game_pool():
    global room_game
    if not room_game or room_game.get("status") != "pooling":
        return {"status": "error", "message": "Pool is not open."}
    room_game["pool"] = []
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/assign")
def assign_room_game_teams():
    global room_game, team_feeds
    if not room_game or room_game.get("status") != "pooling":
        return {"status": "error", "message": "Open a join pool and wait for players first."}
    pool = list(room_game.get("pool") or [])
    if len(pool) < 2:
        return {"status": "error", "message": "Need at least 2 people in the pool."}
    max_size = int(room_game.get("max_size") or 4)
    chunks = split_players_into_teams(pool, max_size)
    teams = []
    feeds = {}
    for index, members in enumerate(chunks):
        team_id = f"team_{index + 1}"
        teams.append({
            "id": team_id,
            "label": team_label(index),
            "members": members,
        })
        feeds[team_id] = []
    room_game["teams"] = teams
    room_game["status"] = "assigned"
    room_game["assigned_at"] = now_iso()
    room_game["overlay_visible"] = True
    team_feeds = feeds
    add_notification("system", f"Teams assigned ({len(teams)} groups)", True)
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/overlay")
def set_room_game_overlay(payload: RoomGameOverlay):
    global room_game
    if not room_game or room_game.get("status") != "assigned":
        return {"status": "error", "message": "Assign teams before showing overlay."}
    room_game["overlay_visible"] = bool(payload.visible)
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/end")
def end_room_game():
    global room_game, team_feeds
    if not room_game:
        return {"status": "error", "message": "No active game."}
    room_game = {
        **room_game,
        "status": "ended",
        "overlay_visible": False,
        "ended_at": now_iso(),
    }
    add_notification("system", "Game ended", True)
    persist_live_room()
    return {"status": "ok", "room_game": room_game}


@app.post("/api/room-game/reset")
def reset_room_game():
    global room_game, team_feeds
    room_game = None
    team_feeds = {}
    persist_live_room()
    return {"status": "ok", "room_game": None}


@app.post("/api/room-game/team-message")
def post_team_message(payload: RoomTeamMessage):
    global team_feeds
    if not room_game or room_game.get("status") != "assigned":
        return {"status": "error", "message": "Teams are not active."}
    if room_game.get("mode") != "collaborate":
        return {"status": "error", "message": "This game has no team chat."}
    team_id = (payload.team_id or "").strip()
    team = next((t for t in (room_game.get("teams") or []) if t.get("id") == team_id), None)
    if not team:
        return {"status": "error", "message": "Team not found."}
    user_key = str(payload.user_id or "").strip()
    member_keys = {str(m.get("user_id") or "") for m in team.get("members") or []}
    if user_key not in member_keys:
        return {"status": "error", "message": "You are not in this team."}
    text = (payload.text or "").strip()
    if not text:
        return {"status": "error", "message": "Message cannot be empty."}
    if len(text) > 400:
        return {"status": "error", "message": "Message must be 400 characters or fewer."}
    message = {
        "id": _next_team_msg_id(),
        "team_id": team_id,
        "user_id": payload.user_id,
        "username": payload.username,
        "display_name": (payload.display_name or "Viewer").strip() or "Viewer",
        "text": text,
        "kind": "text",
        "created_at": now_iso(),
    }
    feed = team_feeds.setdefault(team_id, [])
    feed.append(message)
    team_feeds[team_id] = feed[-100:]
    persist_live_room()
    return {"status": "ok", "message": message}


@app.post("/api/room-game/team-media")
async def upload_team_media(
    user_id: int = Form(...),
    display_name: str = Form("Viewer"),
    team_id: str = Form(...),
    text: str = Form(""),
    file: UploadFile = File(...),
):
    global team_feeds, room_media_submissions
    if not room_game or room_game.get("status") != "assigned":
        return {"status": "error", "message": "Teams are not active."}
    if room_game.get("mode") != "collaborate":
        return {"status": "error", "message": "This game has no team chat."}
    team = next((t for t in (room_game.get("teams") or []) if t.get("id") == team_id), None)
    if not team:
        return {"status": "error", "message": "Team not found."}
    user_key = str(user_id or "").strip()
    member_keys = {str(m.get("user_id") or "") for m in team.get("members") or []}
    if user_key not in member_keys:
        return {"status": "error", "message": "You are not in this team."}
    original_name = Path(file.filename or "upload.bin").name
    ext = Path(original_name).suffix.lower()
    if ext not in ROOM_MEDIA_ALLOWED_EXTENSIONS:
        return {"status": "error", "message": "Unsupported file type. Use a picture or video."}
    media_id = _next_media_id()
    stored_name = f"team_{media_id}{ext}"
    stored_path = os.path.join(ROOM_MEDIA_DIR, stored_name)
    total = 0
    try:
        with open(stored_path, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > ROOM_MEDIA_MAX_BYTES:
                    out.close()
                    os.remove(stored_path)
                    return {"status": "error", "message": "File too large (max 100 MB)."}
                out.write(chunk)
    except OSError:
        return {"status": "error", "message": "Could not save upload."}
    kind = "video" if ext in {".mp4", ".mov", ".webm", ".m4v"} else "image"
    message = {
        "id": _next_team_msg_id(),
        "team_id": team_id,
        "user_id": user_id,
        "display_name": (display_name or "Viewer").strip() or "Viewer",
        "text": (text or "").strip()[:220],
        "kind": kind,
        "media_id": media_id,
        "url": f"/api/room-media/file/{media_id}",
        "filename": stored_name,
        "created_at": now_iso(),
    }
    # Ensure file lookup via find_room_media works for team uploads too
    room_media_submissions.append({
        "id": media_id,
        "discussion_id": None,
        "discussion_title": "",
        "user_id": user_id,
        "display_name": message["display_name"],
        "anonymous": False,
        "caption": message["text"],
        "filename": stored_name,
        "kind": kind,
        "status": "team",
        "created_at": now_iso(),
    })
    feed = team_feeds.setdefault(team_id, [])
    feed.append(message)
    team_feeds[team_id] = feed[-100:]
    persist_live_room()
    return {"status": "ok", "message": message}


# ---------------------------------
# Notification feed
# ---------------------------------

@app.get("/api/notifications")
def get_notifications():
    return notification_feed


@app.post("/api/notifications/clear")
def clear_notifications():
    notification_feed.clear()
    return {"status": "ok"}


# ---------------------------------
# Archive
# ---------------------------------

@app.post("/api/wheel-entry/archive/{entry_id}")
def archive_wheel_entry(entry_id: int):
    global current_now_playing, current_winner
    for i, entry in enumerate(wheel_entries):
        if entry["id"] == entry_id:
            archive_path = None
            # move file into archive if it exists
            if entry.get("local_path") and os.path.exists(entry["local_path"]):
                archive_name = entry.get("local_filename") or os.path.basename(entry["local_path"])
                archive_path = os.path.join(ARCHIVE_DIR, archive_name)
                if os.path.abspath(entry["local_path"]) != os.path.abspath(archive_path):
                    shutil.move(entry["local_path"], archive_path)
                entry["local_path"] = archive_path
                entry["download_status"] = "archived"

            archived = dict(entry)
            archived["archived_at"] = now_iso()
            related_reviews = [
                review for review in video_reviews
                if int(review.get("video_entry_id") or 0) == int(entry_id)
            ]
            ratings = [float(review.get("rating") or 0) for review in related_reviews if review.get("rating")]
            archived["reviews"] = related_reviews
            archived["review_count"] = len(related_reviews)
            archived["average_rating"] = round(sum(ratings) / len(ratings), 2) if ratings else 0.0
            archived_wheel_entries.append(archived)

            if archive_path:
                metadata_path = os.path.splitext(archive_path)[0] + ".json"
                metadata = {
                    "entry_id": entry.get("id"),
                    "title": (entry.get("data") or {}).get("video_title"),
                    "submitted_by": (entry.get("data") or {}).get("display_name"),
                    "username": (entry.get("data") or {}).get("username"),
                    "submitted_at": entry.get("time"),
                    "played_at": entry.get("played_at"),
                    "archived_at": archived.get("archived_at"),
                    "local_filename": entry.get("local_filename"),
                    "average_rating": archived.get("average_rating"),
                    "review_count": archived.get("review_count"),
                    "reviews": related_reviews,
                }
                try:
                    with open(metadata_path, "w", encoding="utf-8") as metadata_file:
                        json.dump(metadata, metadata_file, indent=2, ensure_ascii=False)
                except Exception:
                    pass

            if current_now_playing and current_now_playing["id"] == entry_id:
                current_now_playing = None
                video_reviews.clear()

            if current_winner and current_winner["entry_id"] == entry_id:
                current_winner = None

            del wheel_entries[i]
            state["round_status"] = "locked" if get_wheel_spin_pool(state["current_round"]) else "closed"
            ws_broadcast_bundle()
            return {"status": "ok"}

    return {"status": "error"}


# ---------------------------------
# Legacy / extra sections
# ---------------------------------

@app.post("/api/room/close")
def close_room():
    state["closing_soon"] = True
    state["room_open"] = False
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/room/open")
def open_room():
    state["closing_soon"] = False
    state["room_open"] = True
    ws_broadcast_bundle()
    return {"status": "ok"}


@app.post("/api/user/extra-spin")
def user_extra_spin(payload: dict):
    name = (payload.get("display_name") or "").strip().lower()
    if not name:
        return {"status": "error", "message": "display_name required"}
    wheel_submission_limits[name] = wheel_submission_limits.get(name, 1) + 1
    ws_broadcast_bundle()
    return {"status": "ok", "submission_limit": wheel_submission_limits[name]}


@app.post("/api/user/mute")
def user_mute(payload: dict):
    name = (payload.get("display_name") or "").strip().lower()
    if not name:
        return {"status": "error", "message": "display_name required"}
    if payload.get("muted"):
        muted_users.add(name)
    else:
        muted_users.discard(name)
    ws_broadcast_bundle()
    return {"status": "ok", "muted": name in muted_users}

@app.post("/api/wheel-entry/allow-more")
def allow_more(payload: dict):
    name = payload.get("display_name", "").lower()
    limit = wheel_submission_limits.get(name, 1)
    wheel_submission_limits[name] = limit + 1
    return {"status": "ok"}


@app.post("/api/spotlight-entry")
def submit_spotlight(entry: SpotlightEntry):
    print(
        f"[{now_iso()}] spotlight submit attempt "
        f"nominee_user_id={entry.nominee_user_id} nominee_username={entry.nominee_username!r} "
        f"nominator_user_id={entry.nominator_user_id} nominator_username={entry.nominator_username!r} "
        f"style={entry.style!r} reason_len={len((entry.reason or '').strip())}",
        flush=True,
    )
    nominee = find_verified_alcove_user(entry.nominee_user_id, entry.nominee_username)
    if not nominee:
        print(
            f"[{now_iso()}] spotlight submit rejected: nominee not verified "
            f"nominee_user_id={entry.nominee_user_id} nominee_username={entry.nominee_username!r}",
            flush=True,
        )
        return {"status": "error", "message": "That user is not in the Alcove member list."}

    if not entry.nominator_user_id and not entry.nominator_username:
        print(
            f"[{now_iso()}] spotlight submit rejected: missing Telegram identity "
            f"nominee_user_id={entry.nominee_user_id} nominee_username={entry.nominee_username!r}",
            flush=True,
        )
        return {
            "status": "error",
            "message": "Could not identify who submitted this Spotlight. Please open the Mini App from Telegram and try again.",
        }

    nominator = find_verified_alcove_user(entry.nominator_user_id, entry.nominator_username)
    if entry.nominator_user_id and nominee.get("user_id") == entry.nominator_user_id:
        print(
            f"[{now_iso()}] spotlight submit rejected: self nomination by user_id={entry.nominator_user_id}",
            flush=True,
        )
        return {"status": "error", "message": "You cannot nominate yourself."}
    if entry.nominator_username and (nominee.get("username") or "").lower() == entry.nominator_username.lower():
        print(
            f"[{now_iso()}] spotlight submit rejected: self nomination by username={entry.nominator_username!r}",
            flush=True,
        )
        return {"status": "error", "message": "You cannot nominate yourself."}

    with spotlight_submit_lock:
        if spotlight_today_exists(entry.nominator_user_id, entry.nominator_username):
            print(
                f"[{now_iso()}] spotlight submit rejected: already submitted today "
                f"nominator_user_id={entry.nominator_user_id} nominator_username={entry.nominator_username!r}",
                flush=True,
            )
            return {"status": "error", "message": "You have already submitted a Spotlight today."}

        data = entry.dict()
        data["id"] = next_spotlight_id()
        data["time"] = now_iso()
        data["day_key"] = pulse_day_key()
        data["status"] = "pending_review"
        data["edited_reason"] = None
        data["review_message_sent"] = False
        data["reviewed_by"] = None
        data["reviewed_at"] = None
        data["publish_pending"] = False
        data["published_at"] = None
        data["nominee_user_id"] = nominee.get("user_id")
        data["nominee_username"] = nominee.get("username")
        data["nominee_display_name"] = nominee.get("display_name") or nominee.get("label")
        if nominator:
            data["nominator_user_id"] = nominator.get("user_id")
            data["nominator_username"] = nominator.get("username")
            data["nominator_display_name"] = nominator.get("display_name") or nominator.get("label")
        else:
            data["nominator_user_id"] = entry.nominator_user_id
            data["nominator_username"] = (entry.nominator_username or "").lstrip("@") or None
            data["nominator_display_name"] = (
                entry.nominator_display_name
                or (f"@{entry.nominator_username.lstrip('@')}" if entry.nominator_username else None)
            )
        spotlight_entries.append(data)
        save_runtime_state()

    telegram_admin_notify(
        "\n".join([
            "<b>New Spotlight submitted</b>",
            f"ID: <code>{data['id']}</code>",
            f"Nominee: <b>{escape(data.get('nominee_display_name') or data.get('nominee_username') or 'Unknown')}</b>",
            f"Style: <b>{escape(data.get('style') or 'Spotlight')}</b>",
            "",
            f"<code>{escape((entry.reason or '').strip())}</code>",
            "",
            "Review in Feature Admin or wait for F.O.X review buttons.",
        ]),
        SPOTLIGHT_REVIEW_TOPIC_ID,
    )
    print(
        f"[{now_iso()}] spotlight submit success spotlight_id={data['id']} "
        f"nominator_user_id={data.get('nominator_user_id')} nominee_user_id={data.get('nominee_user_id')} "
        f"style={data.get('style')!r}",
        flush=True,
    )
    add_notification("spotlight", f"Spotlight submitted for {entry.nominee_display_name}", False)
    return {
        "status": "ok",
        "spotlight_id": data["id"],
        "spotlights": len(spotlight_entries),
        "nominator_user_id": data.get("nominator_user_id"),
        "nominator_username": data.get("nominator_username"),
        "spotlight_status": spotlight_status_payload(data.get("nominator_user_id"), data.get("nominator_username")),
    }


@app.get("/api/spotlight-status")
def get_spotlight_status(user_id: int | None = None, username: str | None = None):
    if not user_id and not username:
        return {"status": "error", "message": "Could not identify this Spotlight user."}
    return {"status": "ok", **spotlight_status_payload(user_id, username)}


@app.get("/api/spotlight-entries")
def list_spotlights(status: str | None = None):
    entries = spotlight_entries
    if status:
        entries = [entry for entry in entries if entry.get("status") == status]
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/spotlights/pending")
def bot_pending_spotlights(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        entry for entry in spotlight_entries
        if entry.get("status") == "pending_review" and not entry.get("review_message_sent")
    ]
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/spotlights/publish-pending")
def bot_publish_pending_spotlights(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        entry for entry in spotlight_entries
        if entry.get("status") == "approved" and entry.get("publish_pending") and not entry.get("published_at")
    ]
    return {"status": "ok", "entries": entries}


@app.post("/api/bot-sync/spotlights/{entry_id}")
def bot_update_spotlight(entry_id: int, payload: SpotlightReviewUpdate, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    with spotlight_submit_lock:
        entry = get_spotlight_entry(entry_id)
        if not entry:
            return {"status": "error", "message": "Spotlight not found"}

        if payload.status is not None:
            current_status = (entry.get("status") or "").strip().lower()
            next_status = (payload.status or "").strip().lower()
            if current_status in {"approved", "rejected"} and next_status and next_status != current_status:
                return {
                    "status": "error",
                    "message": f"Spotlight is already {current_status}.",
                    "entry": entry,
                }
            if current_status == "approved" and next_status == "approved" and entry.get("published_at"):
                return {"status": "ok", "entry": entry, "already_done": True}
            entry["status"] = payload.status
        if payload.edited_reason is not None:
            entry["edited_reason"] = payload.edited_reason
        if payload.review_message_sent is not None:
            requested_sent = bool(payload.review_message_sent)
            if requested_sent and entry.get("review_message_sent"):
                return {"status": "ok", "entry": entry, "already_done": True}
            entry["review_message_sent"] = requested_sent
        if payload.reviewed_by is not None:
            entry["reviewed_by"] = payload.reviewed_by
        if payload.reviewed_at is not None:
            entry["reviewed_at"] = payload.reviewed_at

        claimed_publish = None
        if payload.publish_pending is not None:
            requested_pending = bool(payload.publish_pending)
            # Atomic claim: first writer to clear publish_pending wins the publish slot.
            if (
                requested_pending is False
                and payload.published_at is None
                and payload.status is None
            ):
                if entry.get("published_at"):
                    return {"status": "ok", "entry": entry, "already_done": True, "claimed": False}
                if not entry.get("publish_pending"):
                    return {"status": "ok", "entry": entry, "already_done": True, "claimed": False}
                entry["publish_pending"] = False
                claimed_publish = True
            else:
                entry["publish_pending"] = requested_pending

        if payload.published_at is not None:
            entry["published_at"] = payload.published_at
        if payload.status == "approved" and not entry.get("published_at"):
            entry["publish_pending"] = True
        if entry.get("published_at") and entry.get("status") == "approved":
            entry["publish_pending"] = False
            archive_published_spotlight(entry)
        save_runtime_state()

        result = {"status": "ok", "entry": entry}
        if claimed_publish is not None:
            result["claimed"] = claimed_publish
        return result


@app.get("/api/pulse-questions")
def get_pulse_questions(user_id: int | None = None, username: str | None = None):
    return {
        "status": "ok",
        "questions": {
            "green": pulse_question_choices("green", user_id, username),
            "red": pulse_question_choices("red", user_id, username),
        },
        "question_options": {
            "green": pulse_question_option_entries("green", user_id, username),
            "red": pulse_question_option_entries("red", user_id, username),
        },
        "red_pulse_question": pulse_red_daily_question(),
        "heat_threshold": pulse_heat_threshold(),
    }


def pulse_question_submissions_for_user_today(user_id=None, username=None) -> list[dict]:
    today = pulse_day_key()
    username = (username or "").lower()
    rows = []
    for existing in pulse_question_suggestions:
        if existing.get("day_key") != today:
            continue
        if (existing.get("status") or "").strip().lower() == "deleted":
            continue
        same_user = user_id and int(existing.get("user_id") or 0) == int(user_id)
        same_username = username and (existing.get("username") or "").lower() == username
        if same_user or same_username:
            rows.append(existing)
    return rows


def pulse_has_pending_rejection_resubmit(user_id=None, username=None) -> bool:
    return any(
        (existing.get("status") or "").strip().lower() == "rejected"
        and existing.get("resubmit_allowed")
        for existing in pulse_question_submissions_for_user_today(user_id, username)
    )


def pulse_rejection_replacement_used_today(user_id=None, username=None) -> bool:
    today = pulse_day_key()
    username_key = (username or "").lower()
    # Count a replacement if it was used today, even when the amended row
    # originally belonged to an earlier day_key (legacy bad amend path).
    for existing in pulse_question_suggestions:
        same_user = user_id and int(existing.get("user_id") or 0) == int(user_id)
        same_username = username_key and (existing.get("username") or "").lower() == username_key
        if not (same_user or same_username):
            continue
        resubmitted_at = (existing.get("resubmitted_at") or "").strip()
        if resubmitted_at.startswith(today):
            return True
    rows = pulse_question_submissions_for_user_today(user_id, username)
    if any(existing.get("resubmitted_at") for existing in rows):
        return True
    return len(rows) > PULSE_DAILY_QUESTION_LIMIT


def pulse_rejection_replacement_available(user_id=None, username=None) -> bool:
    if pulse_rejection_replacement_used_today(user_id, username):
        return False
    return pulse_has_pending_rejection_resubmit(user_id, username)


def pulse_close_other_rejection_resubmits(user_id=None, username=None, keep_entry_id=None) -> None:
    changed = False
    for existing in pulse_question_submissions_for_user_today(user_id, username):
        if keep_entry_id is not None and existing.get("id") == keep_entry_id:
            continue
        if (existing.get("status") or "").strip().lower() != "rejected":
            continue
        if not existing.get("resubmit_allowed"):
            continue
        existing["resubmit_allowed"] = False
        changed = True
    if changed:
        save_runtime_state()


def pulse_consume_rejection_replacement(user_id=None, username=None, *, keep_entry_id=None) -> None:
    """Mark today's one replacement as used and close any other open amend slots."""
    changed = False
    stamped = now_iso()
    today = pulse_day_key()
    username_key = (username or "").lower()
    for existing in pulse_question_suggestions:
        same_user = user_id and int(existing.get("user_id") or 0) == int(user_id)
        same_username = username_key and (existing.get("username") or "").lower() == username_key
        if not (same_user or same_username):
            continue
        if (existing.get("status") or "").strip().lower() != "rejected":
            continue
        if keep_entry_id is not None and existing.get("id") == keep_entry_id:
            if existing.get("resubmit_allowed"):
                existing["resubmit_allowed"] = False
                changed = True
            continue
        if existing.get("resubmit_allowed"):
            existing["resubmit_allowed"] = False
            changed = True
        # Only stamp today's rejected rows — older rejects just lose their amend slot.
        if (existing.get("day_key") or "") == today and not existing.get("resubmitted_at"):
            existing["resubmitted_at"] = stamped
            changed = True
    if changed:
        save_runtime_state()


def pulse_max_submissions_allowed_today(user_id=None, username=None) -> int:
    extra = (
        PULSE_DAILY_REJECTION_REPLACEMENT_LIMIT
        if pulse_rejection_replacement_available(user_id, username)
        else 0
    )
    return PULSE_DAILY_QUESTION_LIMIT + extra


def pulse_question_submissions_today_count(user_id=None, username=None) -> int:
    return pulse_question_submissions_total_today(user_id, username)


def pulse_question_submissions_total_today(user_id=None, username=None) -> int:
    return len(pulse_question_submissions_for_user_today(user_id, username))


def pulse_can_submit_another_question(user_id=None, username=None) -> bool:
    if pulse_unlimited_question_submit():
        return True
    total = pulse_question_submissions_total_today(user_id, username)
    if total < 1:
        return False
    return total < pulse_max_submissions_allowed_today(user_id, username)


@app.post("/api/pulse-question-suggestions")
def submit_pulse_question_suggestion(payload: PulseQuestionSuggestion):
    print(
        f"[{now_iso()}] pulse question submit attempt pool={payload.pool or 'green'} "
        f"category={payload.category!r} user_id={payload.user_id} username={payload.username!r}",
        flush=True,
    )
    identity = pulse_user_identity(payload.user_id, payload.username)
    if not identity:
        print(f"[{now_iso()}] pulse question submit rejected: missing Telegram identity", flush=True)
        return {"status": "error", "message": "Could not identify your Telegram account. Please open the Mini App from Telegram and try again."}

    today = pulse_day_key()
    user_id = identity.get("user_id")
    username = (identity.get("username") or "").lower()
    if not pulse_unlimited_question_submit():
        submissions_today = pulse_question_submissions_total_today(user_id, username)
        max_allowed = pulse_max_submissions_allowed_today(user_id, username)
        if submissions_today >= max_allowed:
            print(
                f"[{now_iso()}] pulse question submit rejected: daily limit reached "
                f"user_id={user_id} username={username!r} count={submissions_today} max={max_allowed}",
                flush=True,
            )
            return {
                "status": "error",
                "message": (
                    "You've already submitted your two Pulse questions for today."
                    if not pulse_rejection_replacement_available(user_id, username)
                    else "You've already used today's one replacement attempt."
                ),
            }

    using_replacement_slot = (
        not pulse_unlimited_question_submit()
        and pulse_rejection_replacement_available(user_id, username)
    )

    pool = (payload.pool or "green").strip().lower()
    category = (payload.category or "").strip()
    question = (payload.question or "").strip()
    allowed_categories = {"Mental health", "Physical health", "General"}

    if pool not in {"green", "red"}:
        print(f"[{now_iso()}] pulse question submit rejected: invalid pool={pool!r}", flush=True)
        return {"status": "error", "message": "Please choose a valid Pulse pool."}
    if category not in allowed_categories:
        print(f"[{now_iso()}] pulse question submit rejected: invalid category={category!r}", flush=True)
        return {"status": "error", "message": "Please choose a valid category."}
    if len(question) < 8:
        print(f"[{now_iso()}] pulse question submit rejected: question too short", flush=True)
        return {"status": "error", "message": "Please add a little more detail before submitting your Pulse."}
    too_long = pulse_question_too_long(question)
    if too_long:
        print(f"[{now_iso()}] pulse question submit rejected: question too long", flush=True)
        return {"status": "error", "message": too_long}

    schedule_mode = normalize_pulse_schedule_mode(payload.schedule_mode) if payload.schedule_mode else None

    # If a rejected question still has today's one replace attempt, amend that row
    # instead of creating another free submission.
    if using_replacement_slot:
        result = resubmit_rejected_pulse_question(user_id, username, question)
        if result.get("status") != "ok":
            return result
        entry = result.get("entry") or {}
        entry["pool"] = pool
        entry["category"] = category
        if schedule_mode:
            entry["schedule_mode"] = schedule_mode
        save_runtime_state()
        submissions_today = pulse_question_submissions_today_count(user_id, username)
        can_submit_another = pulse_can_submit_another_question(user_id, username)
        return {
            "status": "ok",
            "message": (
                "Thanks — your replacement Pulse question is with F.O.X for review. "
                "That uses today's one amendment."
            ),
            "entry": entry,
            "submissions_today": submissions_today,
            "submissions_total_today": pulse_question_submissions_total_today(user_id, username),
            "can_submit_another": can_submit_another,
            "replacement_available": False,
            "replacement_used_today": True,
            "daily_limit": PULSE_DAILY_QUESTION_LIMIT,
        }

    entry = {
        "id": len(pulse_question_suggestions) + 1,
        "pool": pool,
        "category": category,
        "question": question,
        "edited_question": None,
        "submitted_at": now_iso(),
        "day_key": pulse_day_key(),
        "user_id": identity.get("user_id"),
        "username": identity.get("username"),
        "display_name": identity.get("display_name") or identity.get("label"),
        "status": "pending_review",
        "schedule_mode": "tomorrow",
        "needs_admin_notify": True,
        "review_message_sent": False,
        "reviewed_at": None,
        "reviewed_by": None,
        "active_from_day_key": None,
    }
    pulse_question_suggestions.append(entry)
    save_runtime_state()
    submitter = entry.get("display_name") or entry.get("username") or entry.get("user_id")
    pulse_admin_notify(
        "\n".join([
            "<b>New Pulse question submitted</b>",
            f"ID: <code>{entry['id']}</code>",
            f"Pool: <b>{escape(pool.title())}</b>",
            f"Category: <b>{escape(category)}</b>",
            f"From: <b>{escape(str(submitter))}</b>",
            "",
            f"<code>{escape(question)}</code>",
            "",
            "Review in Feature Admin or wait for F.O.X review buttons.",
        ]),
        PULSE_QUESTIONS_TOPIC_ID,
    )
    print(
        f"[{now_iso()}] pulse question submit success suggestion_id={entry['id']} "
        f"pool={pool} category={category!r} status={entry.get('status')} "
        f"user_id={identity.get('user_id')} username={identity.get('username')!r}",
        flush=True,
    )
    submissions_today = pulse_question_submissions_today_count(user_id, username)
    can_submit_another = pulse_can_submit_another_question(user_id, username)
    if can_submit_another:
        success_message = "Pulse sent for admin review. Submit another Pulse question below if you like."
    else:
        success_message = "Both of today's Pulse questions are with F.O.X for review."
    return {
        "status": "ok",
        "message": success_message,
        "submissions_today": submissions_today,
        "submissions_total_today": pulse_question_submissions_total_today(user_id, username),
        "can_submit_another": can_submit_another,
        "daily_limit": PULSE_DAILY_QUESTION_LIMIT,
        "entry": entry,
    }


@app.get("/api/pulse-question-suggestions/status")
def pulse_question_suggestion_status(user_id: int | None = None, username: str | None = None):
    identity = pulse_user_identity(user_id, username)
    if not identity:
        return {"status": "error", "message": "Could not identify this Pulse user."}

    today = pulse_day_key()
    user_id = identity.get("user_id")
    username = (identity.get("username") or "").lower()
    submissions_today = pulse_question_submissions_today_count(user_id, username)
    submissions_total_today = pulse_question_submissions_total_today(user_id, username)
    can_submit_another = pulse_can_submit_another_question(user_id, username)
    if submissions_total_today <= 0:
        return {
            "status": "ok",
            "submitted_today": False,
            "submissions_today": 0,
            "submissions_total_today": 0,
            "can_submit_another": False,
            "replacement_available": False,
            "replacement_used_today": pulse_rejection_replacement_used_today(user_id, username),
            "daily_limit": PULSE_DAILY_QUESTION_LIMIT,
            "message": "Submit today's Pulse question to unlock the game.",
        }

    latest_entry = None
    for existing in pulse_question_submissions_for_user_today(user_id, username):
        if (existing.get("status") or "").strip().lower() == "rejected":
            continue
        latest_entry = existing

    replacement_open = pulse_rejection_replacement_available(user_id, username)
    has_approved_today = any(
        (existing.get("status") or "").strip().lower() == "approved"
        for existing in pulse_question_submissions_for_user_today(user_id, username)
    )
    if not can_submit_another:
        if pulse_rejection_replacement_used_today(user_id, username):
            message = "You've used today's one replacement attempt. New questions unlock at midnight UK time."
        elif submissions_total_today >= PULSE_DAILY_QUESTION_LIMIT:
            message = "Both of today's Pulse questions are already with F.O.X for review."
        else:
            message = "Both of today's Pulse questions are already with F.O.X for review."
    elif replacement_open and has_approved_today:
        # Lead with the approval — do not pair "approved" with "not approved"
        # in one line (that reads like a contradictory F.O.X decision).
        message = (
            "Your Pulse was approved. You still have one replacement attempt today "
            "if you want to rewrite the other question."
        )
    elif replacement_open:
        message = (
            "Your Pulse was not approved. You have one replacement attempt today — "
            "choose your words carefully."
        )
    elif latest_entry and latest_entry.get("status") == "pending_review":
        message = "Your Pulse is with F.O.X for review. You can submit one more today."
    elif latest_entry and latest_entry.get("status") == "approved":
        message = "Your Pulse has been approved. You can submit one more today."
    else:
        message = "You can submit one more Pulse question today."

    return {
        "status": "ok",
        "submitted_today": True,
        "submissions_today": submissions_today,
        "submissions_total_today": submissions_total_today,
        "can_submit_another": can_submit_another,
        "replacement_available": pulse_rejection_replacement_available(user_id, username),
        "replacement_used_today": pulse_rejection_replacement_used_today(user_id, username),
        "daily_limit": PULSE_DAILY_QUESTION_LIMIT,
        "review_status": latest_entry.get("status") if latest_entry else None,
        "message": message,
        "entry": latest_entry,
    }


@app.get("/api/pulse-status")
def get_pulse_status(user_id: int | None = None, username: str | None = None):
    identity = pulse_user_identity(user_id, username)
    if not identity:
        return {"status": "error", "message": "Could not identify this Pulse user."}

    slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    receipts = [
        payload for payload in (pulse_receipt_payload(receipt) for receipt in pulse_receipts_for_user(identity.get("user_id"), identity.get("username")))
        if payload
    ]
    assignments = [
        public_pulse_payload(entry)
        for entry in pulse_assignments_for_user(identity.get("user_id"), identity.get("username"))
    ]
    responded = [
        public_pulse_payload(entry)
        for entry in pulse_responded_by_user(identity.get("user_id"), identity.get("username"))
    ]
    sent = [
        public_pulse_payload(entry)
        for entry in pulse_user_sent_entries(identity.get("user_id"), identity.get("username"))
    ]
    return {
        "status": "ok",
        "user": identity,
        "slots": slots,
        "red_pulse_question": pulse_red_daily_question(),
        "assigned": assignments,
        "received": receipts,
        "responded": responded,
        "sent": sent,
        "my_pulses": pulse_my_pulses_payload(identity),
        "red_community_answers": pulse_red_community_answers_payload(slots["day_key"], identity),
        "pending_queue": len([entry for entry in pulse_entries_for_day() if entry.get("status") == "queued"]),
    }


@app.get("/api/pulse-question-roster")
def get_pulse_question_roster():
    return {"status": "ok", "questions": pulse_question_roster()}


@app.get("/api/pulse-daily-spread")
def get_pulse_daily_spread(day: str | None = None):
    day_key = (day or "").strip() or None
    return {"status": "ok", **pulse_daily_spread_report(day_key)}


@app.post("/api/pulse-red-activate")
def activate_pulse_red(payload: PulseReceiptAck):
    identity = pulse_user_identity(payload.user_id, payload.username)
    if not identity:
        return {"status": "error", "message": "Could not identify this Pulse user."}

    slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    if not slots["red_unlocked"]:
        return {
            "status": "error",
            "message": "Red Pulse has not been unlocked yet.",
            "slots": slots,
        }
    if slots["red_available"] <= 0:
        return {
            "status": "error",
            "message": "You have already answered today's Red Pulse.",
            "slots": slots,
        }

    return {
        "status": "ok",
        "message": "Red Pulse is active for you. Submit your answer when you're ready.",
        "slots": slots,
    }


@app.post("/api/pulse-entry")
def submit_pulse(entry: PulseEntry):
    print(
        f"[{now_iso()}] pulse submit attempt pulse_type={entry.pulse_type or 'green'} "
        f"user_id={entry.user_id} username={entry.username!r} question={((entry.question or '').strip())[:80]!r}",
        flush=True,
    )
    identity = pulse_user_identity(entry.user_id, entry.username)
    if not identity:
        print(f"[{now_iso()}] pulse submit rejected: missing Telegram identity", flush=True)
        return {"status": "error", "message": "Could not identify your Telegram account. Please open the Mini App from Telegram and try again."}

    pulse_type = (entry.pulse_type or "green").strip().lower()
    if pulse_type not in ("green", "red"):
        print(f"[{now_iso()}] pulse submit rejected: unknown pulse type {pulse_type!r}", flush=True)
        return {"status": "error", "message": "Unknown Pulse type."}

    answer_text = (entry.answer or "").strip()
    question = (entry.question or "").strip()
    if len(answer_text) < 3:
        print(f"[{now_iso()}] pulse submit rejected: answer too short", flush=True)
        return {"status": "error", "message": "Please add your anonymous answer before submitting."}
    if question not in pulse_active_questions(pulse_type):
        if pulse_type == "red" and (question or "").strip() == pulse_red_daily_question():
            pass
        else:
            print(f"[{now_iso()}] pulse submit rejected: question not active", flush=True)
            return {"status": "error", "message": "Please choose one of today's Pulses."}

    owner = pulse_question_owner(question, pulse_type)
    if owner and pulse_identities_match(identity, owner):
        print(f"[{now_iso()}] pulse submit rejected: self-answer blocked", flush=True)
        return {"status": "error", "message": "You can't answer your own Pulse."}
    if pulse_user_answered_question_today(identity, question, pulse_type):
        print(f"[{now_iso()}] pulse submit rejected: duplicate answer today", flush=True)
        return {"status": "error", "message": "You already answered that Pulse today."}

    slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    if pulse_type == "green" and slots["green_available"] <= 0:
        print(f"[{now_iso()}] pulse submit rejected: no green slot available", flush=True)
        return {"status": "error", "message": "You do not have a green Pulse available right now."}
    if pulse_type == "red" and not slots["red_unlocked"]:
        print(f"[{now_iso()}] pulse submit rejected: red pulse not unlocked", flush=True)
        return {"status": "error", "message": "Red Pulse has not been unlocked by the community yet."}
    if pulse_type == "red" and slots["red_available"] <= 0:
        print(f"[{now_iso()}] pulse submit rejected: red pulse already used", flush=True)
        return {"status": "error", "message": "You have already answered today's Red Pulse."}

    responded_at = now_iso()
    day = slots["day_key"]
    previous_cycles = pulse_red_unlocked_cycles(day)
    data = {
        "id": next_pulse_entry_id(),
        "day_key": pulse_day_key(),
        "pulse_type": pulse_type,
        "category": pulse_question_category(question, pulse_type),
        "question": question,
        "sender_note": answer_text,
        "answer": answer_text,
        "sender_user_id": identity.get("user_id"),
        "sender_username": identity.get("username"),
        "sender_display_name": identity.get("display_name") or identity.get("label"),
        "sent_at": responded_at,
        "status": "completed",
        "delivery_mode": "question_answer",
        "question_owner_user_id": owner.get("user_id") if owner else None,
        "question_owner_username": owner.get("username") if owner else None,
        "question_owner_display_name": owner.get("display_name") if owner else None,
        "delivered_to_user_id": None,
        "delivered_to_username": None,
        "delivered_to_display_name": None,
        "delivered_at": None,
        "assignment_notified_at": None,
        "assignment_notify_after": None,
        "response_answer": answer_text,
        "responded_at": responded_at,
        "responder_user_id": identity.get("user_id"),
        "responder_username": identity.get("username"),
        "responder_display_name": identity.get("display_name") or identity.get("label"),
        "admin_posted_at": responded_at if pulse_type == "red" else None,
    }
    pulse_entries.append(data)
    archive_completed_pulse_entry(data)
    receipt = None
    if (
        pulse_type != "red"
        and owner
        and not pulse_identities_match(identity, owner)
    ):
        receipt = {
            "id": len(pulse_receipts) + 1,
            "pulse_id": data["id"],
            "recipient_user_id": owner.get("user_id"),
            "recipient_username": owner.get("username"),
            "recipient_display_name": owner.get("display_name"),
            "received_at": responded_at,
            "acknowledged_at": None,
            "notified_at": None,
            "notify_after": pulse_notification_due_at(),
        }
        pulse_receipts.append(receipt)
    new_cycles = pulse_red_unlocked_cycles(day)
    maybe_queue_red_pulse_unlock_notifications(day, previous_cycles, new_cycles)
    save_runtime_state()
    if pulse_type != "red":
        pulse_admin_notify(
            "\n".join([
                "<b>New Pulse answer</b>",
                f"Question: <code>{escape(question)}</code>",
                f"Type: <b>{escape(pulse_type.title())}</b>",
                f"Category: <b>{escape(data.get('category') or 'General')}</b>",
            ]),
            PULSE_REPORTS_TOPIC_ID,
        )
    print(
        f"[{now_iso()}] pulse submit success pulse_id={data['id']} pulse_type={pulse_type} "
        f"sender_user_id={identity.get('user_id')} sender_username={identity.get('username')!r} "
        f"owner_user_id={owner.get('user_id') if owner else None} receipt_id={receipt.get('id') if receipt else None}",
        flush=True,
    )
    updated_slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    add_notification("pulse", "Anonymous Pulse submitted", False)
    message = (
        "Your Red Pulse answer is in. Thanks for contributing to the community heat."
        if pulse_type == "red"
        else (
            "You answered someone's Pulse. If they submitted that Pulse, they'll receive your anonymous answer."
            if receipt
            else "You answered a Pulse."
        )
    )
    return {
        "status": "ok",
        "message": message,
        "pulse_id": data["id"],
        "slots": updated_slots,
        "delivered_to_owner": bool(receipt),
        "receipt": pulse_receipt_payload(receipt) if receipt else None,
    }


@app.post("/api/pulse-assignments/{pulse_id}/respond")
def respond_to_pulse_assignment(pulse_id: int, payload: PulseAssignmentResponse):
    print(
        f"[{now_iso()}] pulse answer attempt pulse_id={pulse_id} user_id={payload.user_id} "
        f"username={payload.username!r}",
        flush=True,
    )
    entry = find_pulse_entry(pulse_id, status="awaiting_response") or find_pulse_entry(pulse_id)
    if not entry:
        print(f"[{now_iso()}] pulse answer rejected: pulse not found", flush=True)
        return {"status": "error", "message": "Pulse assignment not found."}
    if entry.get("status") != "awaiting_response":
        print(f"[{now_iso()}] pulse answer rejected: pulse already answered or not awaiting", flush=True)
        return {"status": "error", "message": "That Pulse has already been answered."}

    identity = pulse_user_identity(payload.user_id, payload.username)
    if not identity:
        print(f"[{now_iso()}] pulse answer rejected: missing Telegram identity", flush=True)
        return {"status": "error", "message": "Could not identify this Pulse user."}
    if identity.get("user_id") is not None and int(entry.get("delivered_to_user_id") or 0) != int(identity.get("user_id")):
        print(f"[{now_iso()}] pulse answer rejected: wrong recipient by user_id", flush=True)
        return {"status": "error", "message": "That Pulse is not assigned to you."}
    if identity.get("user_id") is None and (identity.get("username") or "").lower() != (entry.get("delivered_to_username") or "").lower():
        print(f"[{now_iso()}] pulse answer rejected: wrong recipient by username", flush=True)
        return {"status": "error", "message": "That Pulse is not assigned to you."}

    answer = (payload.answer or "").strip()
    if len(answer) < 3:
        print(f"[{now_iso()}] pulse answer rejected: answer too short", flush=True)
        return {"status": "error", "message": "Please add a little more before sending your answer."}

    day = entry.get("day_key") or pulse_day_key()
    previous_cycles = pulse_red_unlocked_cycles(day)
    entry["status"] = "completed"
    entry["response_answer"] = answer
    entry["responder_user_id"] = identity.get("user_id")
    entry["responder_username"] = identity.get("username")
    entry["responder_display_name"] = identity.get("display_name") or identity.get("label")
    entry["responded_at"] = now_iso()
    receipt = {
        "id": len(pulse_receipts) + 1,
        "pulse_id": entry.get("id"),
        "recipient_user_id": entry.get("sender_user_id"),
        "recipient_username": entry.get("sender_username"),
        "recipient_display_name": entry.get("sender_display_name"),
        "received_at": entry["responded_at"],
        "acknowledged_at": None,
        "notified_at": None,
        "notify_after": pulse_notification_due_at(),
    }
    pulse_receipts.append(receipt)
    archive_completed_pulse_entry(entry)
    new_cycles = pulse_red_unlocked_cycles(day)
    maybe_queue_red_pulse_unlock_notifications(day, previous_cycles, new_cycles)
    save_runtime_state()
    print(
        f"[{now_iso()}] pulse answer success pulse_id={pulse_id} responder_user_id={identity.get('user_id')} "
        f"responder_username={identity.get('username')!r} receipt_id={receipt['id']}",
        flush=True,
    )
    return {"status": "ok", "receipt": pulse_receipt_payload(receipt)}


@app.post("/api/pulse-receipts/{receipt_id}/ack")
def acknowledge_pulse_receipt(receipt_id: int, payload: PulseReceiptAck):
    receipt = next((item for item in pulse_receipts if item.get("id") == receipt_id), None)
    if not receipt:
        return {"status": "error", "message": "Pulse receipt not found."}

    identity = pulse_user_identity(payload.user_id, payload.username)
    if not identity:
        return {"status": "error", "message": "Could not identify this Pulse user."}
    if identity.get("user_id") is not None and int(receipt.get("recipient_user_id") or 0) != int(identity.get("user_id")):
        return {"status": "error", "message": "That Pulse is not assigned to you."}

    receipt["acknowledged_at"] = now_iso()
    save_runtime_state()
    return {"status": "ok", "receipt": pulse_receipt_payload(receipt)}


@app.get("/api/bot-sync/pulses/pending")
def bot_pending_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    queued = [entry for entry in pulse_entries_for_day() if entry.get("status") == "queued"]
    return {"status": "ok", "entries": queued}


@app.get("/api/bot-sync/pulses/outstanding")
def bot_outstanding_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = []
    for entry in pulse_entries_for_day():
        if entry.get("status") != "awaiting_response":
            continue
        payload = public_pulse_payload(entry) or {}
        payload["delivered_to_user_id"] = entry.get("delivered_to_user_id")
        payload["delivered_to_username"] = entry.get("delivered_to_username")
        payload["delivered_to_display_name"] = entry.get("delivered_to_display_name")
        payload["assignment_notified_at"] = entry.get("assignment_notified_at")
        payload["assignment_notify_after"] = entry.get("assignment_notify_after")
        entries.append(payload)
    return {"status": "ok", "entries": entries}


@app.post("/api/bot-sync/pulses/respond/{pulse_id}")
def bot_respond_to_pulse(
    pulse_id: int,
    payload: dict | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    entry = find_pulse_entry(pulse_id, status="awaiting_response") or find_pulse_entry(pulse_id)
    if not entry:
        return {"status": "error", "message": "Pulse assignment not found."}
    if entry.get("status") != "awaiting_response":
        return {"status": "error", "message": "That Pulse has already been answered."}

    answer = (payload.get("answer") or "").strip()
    if len(answer) < 3:
        return {"status": "error", "message": "Please add a little more before sending your answer."}

    responder_user_id = entry.get("delivered_to_user_id")
    responder_username = clean_username(entry.get("delivered_to_username"))
    responder_display_name = (
        entry.get("delivered_to_display_name")
        or responder_username
        or str(responder_user_id or "Anonymous")
    )

    day = entry.get("day_key") or pulse_day_key()
    previous_cycles = pulse_red_unlocked_cycles(day)
    entry["status"] = "completed"
    entry["response_answer"] = answer
    entry["responder_user_id"] = responder_user_id
    entry["responder_username"] = responder_username
    entry["responder_display_name"] = responder_display_name
    entry["responded_at"] = now_iso()
    receipt = {
        "id": len(pulse_receipts) + 1,
        "pulse_id": entry.get("id"),
        "recipient_user_id": entry.get("sender_user_id"),
        "recipient_username": entry.get("sender_username"),
        "recipient_display_name": entry.get("sender_display_name"),
        "received_at": entry["responded_at"],
        "acknowledged_at": None,
        "notified_at": None,
        "notify_after": pulse_notification_due_at(),
    }
    pulse_receipts.append(receipt)
    archive_completed_pulse_entry(entry)
    new_cycles = pulse_red_unlocked_cycles(day)
    maybe_queue_red_pulse_unlock_notifications(day, previous_cycles, new_cycles)
    save_runtime_state()
    print(
        f"[{now_iso()}] bot pulse answer success pulse_id={pulse_id} responder_user_id={responder_user_id} "
        f"responder_username={responder_username!r} receipt_id={receipt['id']}",
        flush=True,
    )
    return {"status": "ok", "receipt": pulse_receipt_payload(receipt), "pulse": public_pulse_payload(entry)}


@app.get("/api/bot-sync/pulses/completed")
def bot_completed_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    if PULSE_ADMIN_TELEGRAM_SUPPRESSED:
        return {"status": "ok", "entries": []}
    return {"status": "ok", "entries": pulse_completed_admin_entries()}


@app.get("/api/bot-sync/pulses/export")
def bot_export_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [public_pulse_payload(entry) for entry in pulse_entries]
    entries = [entry for entry in entries if entry]
    entries.sort(
        key=lambda item: (
            (item.get("question") or "").lower(),
            item.get("sent_at") or "",
            int(item.get("pulse_id") or 0),
        )
    )
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/pulses/answers")
def bot_pulse_answers(
    q: str | None = None,
    day: str | None = None,
    suggestion_id: int | None = None,
    limit: int = 20,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    needle = (q or "").strip().lower()
    day_key = (day or "").strip() or None
    max_rows = max(1, min(int(limit or 20), 100))
    rows = []
    for entry in pulse_entries:
        if entry.get("status") != "completed":
            continue
        if day_key and entry.get("day_key") != day_key:
            continue
        payload = public_pulse_payload(entry) or {}
        question = (payload.get("question") or "").strip()
        if suggestion_id:
            suggestion = find_pulse_question_suggestion(suggestion_id)
            if not suggestion:
                continue
            if pulse_suggestion_question(suggestion) != question:
                continue
        if needle and needle not in question.lower() and needle not in (payload.get("answer") or "").lower():
            continue
        rows.append(payload)
    rows.sort(key=lambda item: item.get("responded_at") or item.get("sent_at") or "", reverse=True)
    return {"status": "ok", "entries": rows[:max_rows], "total": len(rows)}


@app.get("/api/bot-sync/pulse-questions/pending")
def bot_pending_pulse_question_suggestions(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    if PULSE_ADMIN_TELEGRAM_SUPPRESSED:
        return {"status": "ok", "entries": []}
    entries = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "pending_review" and not entry.get("review_message_sent")
    ]
    entries = [entry for entry in entries if entry]
    entries.sort(key=lambda item: item.get("submitted_at") or "", reverse=True)
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/pulse-questions/reserved-list")
def bot_reserved_pulse_question_list(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "reserved"
    ]
    entries = [entry for entry in entries if entry]
    entries.sort(key=lambda item: item.get("submitted_at") or "", reverse=True)
    return {"status": "ok", "entries": entries}


@app.post("/api/bot-sync/pulse-questions/by-id/{suggestion_id}/schedule")
def bot_schedule_reserved_pulse_question(
    suggestion_id: int,
    payload: dict | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    payload = payload or {}
    day_key = (payload.get("active_from_day_key") or payload.get("day") or "").strip()
    if not day_key:
        return {"status": "error", "message": "active_from_day_key is required."}
    entry["status"] = "approved"
    entry["schedule_mode"] = "reserve"
    entry["active_from_day_key"] = day_key
    if payload.get("reviewed_by") is not None:
        entry["reviewed_by"] = payload.get("reviewed_by")
    entry["reviewed_at"] = payload.get("reviewed_at") or now_iso()
    save_runtime_state()
    return {"status": "ok", "entry": pulse_question_suggestion_admin_payload(entry)}


@app.get("/api/bot-sync/pulse-questions/pending-list")
def bot_pending_pulse_question_list(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "pending_review"
    ]
    entries = [entry for entry in entries if entry]
    entries.sort(key=lambda item: item.get("submitted_at") or "", reverse=True)
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/pulse-questions/approved-list")
def bot_approved_pulse_question_list(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "approved"
    ]
    entries = [entry for entry in entries if entry]
    entries.sort(key=lambda item: (item.get("active_from_day_key") or "", item.get("submitted_at") or ""), reverse=True)
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/pulse-questions/rejected-list")
def bot_rejected_pulse_question_list(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        pulse_question_suggestion_admin_payload(entry)
        for entry in pulse_question_suggestions
        if entry.get("status") == "rejected"
    ]
    entries = [entry for entry in entries if entry]
    entries.sort(key=lambda item: item.get("submitted_at") or "", reverse=True)
    return {"status": "ok", "entries": entries}


@app.post("/api/bot-sync/pulse-questions/create")
def bot_create_pulse_question(payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    pool = (payload.get("pool") or "green").strip().lower()
    category = (payload.get("category") or "General").strip()
    question = (payload.get("question") or "").strip()
    allowed_categories = {"Mental health", "Physical health", "General"}
    if pool not in {"green", "red"}:
        return {"status": "error", "message": "Pool must be green or red."}
    if category not in allowed_categories:
        return {"status": "error", "message": "Category must be Mental health, Physical health, or General."}
    if len(question) < 8:
        return {"status": "error", "message": "Question is too short."}

    active_from = (payload.get("active_from_day_key") or pulse_next_day_key()).strip()
    entry = {
        "id": len(pulse_question_suggestions) + 1,
        "pool": pool,
        "category": category,
        "question": question,
        "edited_question": None,
        "submitted_at": now_iso(),
        "day_key": pulse_day_key(),
        "user_id": payload.get("created_by_user_id"),
        "username": payload.get("created_by_username"),
        "display_name": payload.get("created_by_display_name") or "F.O.X admin",
        "status": "approved",
        "review_message_sent": True,
        "reviewed_at": now_iso(),
        "reviewed_by": payload.get("reviewed_by"),
        "active_from_day_key": active_from,
        "source": "admin",
    }
    pulse_question_suggestions.append(entry)
    save_runtime_state()
    return {"status": "ok", "entry": pulse_question_suggestion_admin_payload(entry)}


@app.get("/api/bot-sync/pulse-questions/roster")
def bot_pulse_question_roster(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    return {"status": "ok", "questions": pulse_question_roster()}


@app.get("/api/bot-sync/pulses/daily-spread")
def bot_pulse_daily_spread_alias(day: str | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    day_key = (day or "").strip() or None
    return {"status": "ok", **pulse_daily_spread_report(day_key)}


@app.get("/api/bot-sync/pulse-questions/daily-spread")
def bot_pulse_daily_spread(day: str | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    day_key = (day or "").strip() or None
    return {"status": "ok", **pulse_daily_spread_report(day_key)}


@app.get("/api/bot-sync/pulse-questions/by-id/{suggestion_id}")
def bot_pulse_question_suggestion(suggestion_id: int, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    return {"status": "ok", "entry": entry}


@app.post("/api/bot-sync/pulse-questions/by-id/{suggestion_id}")
def bot_update_pulse_question_suggestion(suggestion_id: int, payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    payload = payload or {}
    if payload.get("reject"):
        reason = (payload.get("rejection_reason") or "").strip()
        if not reason:
            return {"status": "error", "message": "Rejection reason is required."}
        if payload.get("reviewed_by") is not None:
            entry["reviewed_by"] = payload.get("reviewed_by")
        apply_admin_pulse_question_action(entry, "reject", rejection_reason=reason)
        save_runtime_state()
        return {"status": "ok", "entry": entry}
    if "edited_question" in payload:
        entry["edited_question"] = (payload.get("edited_question") or "").strip() or None
    if "category" in payload:
        entry["category"] = payload.get("category") or entry.get("category")
    if "schedule_mode" in payload:
        entry["schedule_mode"] = normalize_pulse_schedule_mode(payload.get("schedule_mode"))
    if "active_from_day_key" in payload:
        entry["active_from_day_key"] = (payload.get("active_from_day_key") or "").strip() or None
    if "needs_admin_notify" in payload:
        entry["needs_admin_notify"] = bool(payload.get("needs_admin_notify"))
    if "status" in payload and not payload.get("approve") and not payload.get("reject"):
        entry["status"] = payload.get("status")
    if "review_message_sent" in payload:
        entry["review_message_sent"] = bool(payload.get("review_message_sent"))
    if "reviewed_by" in payload:
        entry["reviewed_by"] = payload.get("reviewed_by")
    if "reviewed_at" in payload:
        entry["reviewed_at"] = payload.get("reviewed_at")
    if payload.get("approve"):
        schedule_mode = normalize_pulse_schedule_mode(payload.get("schedule_mode") or entry.get("schedule_mode"))
        edited = (payload.get("edited_question") or entry.get("edited_question") or "").strip() or None
        apply_admin_pulse_question_action(entry, schedule_mode, edited)
    elif entry.get("status") == "approved" and not entry.get("active_from_day_key"):
        apply_pulse_suggestion_schedule(entry, entry.get("schedule_mode"), approve=True)
        queue_pulse_question_review_notification(entry, "question_approved")
    save_runtime_state()
    return {"status": "ok", "entry": entry}


@app.get("/api/bot-sync/pulse-questions/review-notifications")
def bot_pending_pulse_question_review_notifications(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    rows = []
    stale_cleared = False
    stamped = now_iso()
    for item in pulse_question_review_notifications:
        if item.get("notified_at"):
            continue
        if not pulse_question_review_notification_still_valid(item):
            item["notified_at"] = stamped
            item["cancelled"] = True
            item["cancel_reason"] = "stale_status"
            stale_cleared = True
            continue
        rows.append({
            "notification_id": item.get("notification_id"),
            "kind": item.get("kind"),
            "suggestion_id": item.get("suggestion_id"),
            "recipient_user_id": item.get("recipient_user_id"),
            "recipient_username": item.get("recipient_username"),
            "recipient_display_name": item.get("recipient_display_name"),
            "question": item.get("question"),
            "schedule_mode": item.get("schedule_mode"),
            "active_from_day_key": item.get("active_from_day_key"),
            "availability_copy": item.get("availability_copy"),
            "rejection_reason": item.get("rejection_reason"),
            "resubmit_allowed": bool(item.get("resubmit_allowed")),
            "created_at": item.get("created_at"),
        })
    if stale_cleared:
        save_runtime_state()
    return {"status": "ok", "notifications": rows}


@app.post("/api/bot-sync/pulse-questions/review-notifications/{notification_id}")
def bot_mark_pulse_question_review_notification_sent(
    notification_id: str,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    alert = next(
        (item for item in pulse_question_review_notifications if item.get("notification_id") == notification_id),
        None,
    )
    if not alert:
        return {"status": "error", "message": "Pulse question review notification not found."}
    alert["notified_at"] = now_iso()
    save_runtime_state()
    return {"status": "ok", "notification": alert}


@app.post("/api/bot-sync/pulse-questions/reset-daily-quota")
def bot_reset_pulse_question_daily_quota(
    payload: dict | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    """Tester helper: clear today's submission quota / replacement-used markers for a member."""
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    try:
        user_id = int(payload.get("user_id") or 0) or None
    except (TypeError, ValueError):
        user_id = None
    username = (payload.get("username") or "").strip().lower() or None
    if not user_id and not username:
        return {"status": "error", "message": "user_id or username is required."}

    today = pulse_day_key()
    cleared_today = 0
    cleared_markers = 0
    for entry in pulse_question_suggestions:
        same_user = user_id and int(entry.get("user_id") or 0) == int(user_id)
        same_username = username and (entry.get("username") or "").lower() == username
        if not (same_user or same_username):
            continue
        if (entry.get("day_key") or "") == today and (entry.get("status") or "").strip().lower() != "deleted":
            entry["status"] = "deleted"
            entry["resubmit_allowed"] = False
            entry["needs_admin_notify"] = False
            cleared_today += 1
        resubmitted_at = (entry.get("resubmitted_at") or "").strip()
        if resubmitted_at.startswith(today):
            entry["resubmitted_at"] = None
            cleared_markers += 1
        if entry.get("resubmit_allowed") and (entry.get("day_key") or "") != today:
            entry["resubmit_allowed"] = False
    save_runtime_state(force=True)
    return {
        "status": "ok",
        "day_key": today,
        "cleared_today_submissions": cleared_today,
        "cleared_replacement_markers": cleared_markers,
        "can_submit_now": True,
    }


@app.get("/api/bot-sync/pulse-questions/resubmit-pending")
def bot_pulse_question_resubmit_pending(
    user_id: int | None = None,
    username: str | None = None,
    x_bot_sync_secret: str | None = Header(default=None),
):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_resubmittable_rejected_pulse_question(user_id, username)
    if not entry:
        return {"status": "ok", "entry_id": None}
    return {
        "status": "ok",
        "entry_id": entry.get("id"),
        "question": pulse_suggestion_question(entry),
        "rejection_reason": entry.get("rejection_reason"),
    }


@app.post("/api/bot-sync/pulse-questions/resubmit")
def bot_pulse_question_resubmit(payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    result = resubmit_rejected_pulse_question(
        payload.get("user_id"),
        payload.get("username"),
        payload.get("question") or "",
    )
    if result.get("status") != "ok":
        return result
    return result


@app.post("/api/bot-sync/pulse-questions/roster/clear")
def bot_clear_pulse_question_roster(payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    payload = payload or {}
    result = clear_pulse_question_roster(payload.get("reviewed_by"))
    return {"status": "ok", **result}


@app.post("/api/bot-sync/pulse-questions/roster/{roster_id}/delete")
def bot_delete_pulse_question(roster_id: int, payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = next((item for item in pulse_question_roster() if int(item.get("roster_id") or 0) == int(roster_id)), None)
    if not entry:
        return {"status": "error", "message": "Question number not found."}

    if entry.get("source") == "default":
        marker = {"pool": entry.get("pool"), "question": entry.get("question")}
        if marker not in pulse_disabled_questions:
            pulse_disabled_questions.append(marker)
    elif entry.get("suggestion_id"):
        suggestion = find_pulse_question_suggestion(entry.get("suggestion_id"))
        if suggestion:
            suggestion["status"] = "deleted"
            suggestion["reviewed_at"] = now_iso()
            if payload and payload.get("reviewed_by") is not None:
                suggestion["reviewed_by"] = payload.get("reviewed_by")
    save_runtime_state()

    return {"status": "ok", "deleted": entry}


@app.post("/api/bot-sync/pulses/completed/mark-all-posted")
def mark_all_completed_pulses_posted(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    stamped = now_iso()
    count = 0
    for entry in pulse_entries:
        if entry.get("status") == "completed" and not entry.get("admin_posted_at"):
            entry["admin_posted_at"] = stamped
            count += 1
    if count:
        save_runtime_state(force=True)
    return {"status": "ok", "count": count}


@app.post("/api/bot-sync/pulses/completed/{pulse_id}")
def mark_completed_pulse_posted(pulse_id: int, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_entry(pulse_id, status="completed") or find_pulse_entry(pulse_id)
    if not entry:
        return {"status": "error", "message": "Pulse not found."}
    entry["admin_posted_at"] = now_iso()
    save_runtime_state()
    return {"status": "ok", "pulse": public_pulse_payload(entry)}


@app.get("/api/bot-sync/pulses/daily-summaries")
def bot_pulse_daily_summaries(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    current_day = pulse_day_key()
    day_keys = sorted({
        entry.get("day_key")
        for entry in pulse_entries
        if entry.get("status") == "completed" and entry.get("day_key") and entry.get("day_key") < current_day
    })
    summaries = []
    for day_key in day_keys:
        for summary in pulse_daily_summary_payload(day_key):
            if not summary.get("admin_posted_at"):
                summaries.append(summary)
    if PULSE_ADMIN_TELEGRAM_SUPPRESSED:
        return {"status": "ok", "summaries": []}
    return {"status": "ok", "summaries": summaries}


@app.get("/api/bot-sync/pulses/daily-summaries/{day_key}/{category_slug}")
def bot_pulse_daily_summary(day_key: str, category_slug: str, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    category = pulse_category_from_slug(category_slug)
    summary = next(
        (item for item in pulse_daily_summary_payload(day_key) if item.get("category") == category),
        None,
    )
    if not summary:
        return {"status": "error", "message": "Pulse summary not found."}
    return {"status": "ok", "summary": summary}


@app.post("/api/bot-sync/pulses/daily-summaries/{day_key}/{category_slug}")
def mark_pulse_daily_summary_posted(day_key: str, category_slug: str, payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    category = pulse_category_from_slug(category_slug)
    state = pulse_daily_summary_state(day_key, category)
    if payload and payload.get("published"):
        state["published_at"] = now_iso()
    else:
        state["admin_posted_at"] = now_iso()
    save_runtime_state()
    return {"status": "ok", "summary": state}


@app.get("/api/bot-sync/pulses/notifications")
def bot_pending_pulse_notifications(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    rows = []
    receipt_handoffs_changed = False
    for entry in pulse_entries:
        if entry.get("status") != "awaiting_response" or entry.get("assignment_notified_at"):
            continue
        if not iso_has_passed(entry.get("assignment_notify_after")):
            continue
        payload = public_pulse_payload(entry)
        if not payload:
            continue
        rows.append({
            "notification_id": f"assignment-{entry.get('id')}",
            "kind": "answer_request",
            "pulse_id": entry.get("id"),
            "recipient_user_id": entry.get("delivered_to_user_id"),
            "recipient_username": entry.get("delivered_to_username"),
            "recipient_display_name": entry.get("delivered_to_display_name"),
            "received_at": entry.get("delivered_at"),
            "notify_after": entry.get("assignment_notify_after"),
            "pulse": payload,
        })
    for receipt in pulse_receipts:
        if (
            receipt.get("notified_at")
            or receipt.get("acknowledged_at")
            or receipt.get("notification_handed_off_at")
            or receipt.get("notification_claim_until")
        ):
            continue
        if not iso_has_passed(receipt.get("notify_after")):
            continue
        payload = pulse_receipt_payload(receipt)
        if not payload:
            continue
        if (payload.get("pulse_type") or "").lower() == "red":
            continue
        delivery_mode = payload.get("delivery_mode") or "chain"
        rows.append({
            "notification_id": f"receipt-{receipt.get('id')}",
            "kind": "question_answered" if delivery_mode == "question_answer" else "reply_received",
            "receipt_id": receipt.get("id"),
            "recipient_user_id": receipt.get("recipient_user_id"),
            "recipient_username": receipt.get("recipient_username"),
            "recipient_display_name": receipt.get("recipient_display_name"),
            "received_at": receipt.get("received_at"),
            "notify_after": receipt.get("notify_after"),
            "pulse": payload,
        })
        # Each receipt represents one specific answer. Permanently record the
        # handoff before returning it so a missing bot acknowledgement cannot
        # put that same answer back into the polling queue later.
        receipt["notification_handed_off_at"] = now_iso()
        receipt_handoffs_changed = True
    if receipt_handoffs_changed:
        save_runtime_state()
    for alert in pulse_red_unlock_notifications if PULSE_RED_UNLOCK_DMS_ENABLED else []:
        if alert.get("notified_at"):
            continue
        if not pulse_red_unlock_notify_allowed({
            "user_id": alert.get("recipient_user_id"),
            "username": alert.get("recipient_username"),
        }):
            continue
        rows.append({
            "notification_id": alert.get("notification_id"),
            "kind": "red_pulse_active",
            "day_key": alert.get("day_key"),
            "cycle_number": alert.get("cycle_number"),
            "recipient_user_id": alert.get("recipient_user_id"),
            "recipient_username": alert.get("recipient_username"),
            "recipient_display_name": alert.get("recipient_display_name"),
            "created_at": alert.get("created_at"),
        })
    return {"status": "ok", "notifications": rows}


@app.post("/api/bot-sync/pulses/notifications/{notification_id}")
def mark_pulse_notification_sent(notification_id: str, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    if notification_id.startswith("assignment-"):
        pulse_id = int(notification_id.split("-", 1)[1])
        entry = next((item for item in pulse_entries if int(item.get("id") or 0) == pulse_id), None)
        if not entry:
            return {"status": "error", "message": "Pulse assignment not found."}
        entry["assignment_notified_at"] = now_iso()
        save_runtime_state()
        return {"status": "ok", "pulse": public_pulse_payload(entry)}

    if notification_id.startswith("red-unlock-"):
        alert = next(
            (item for item in pulse_red_unlock_notifications if item.get("notification_id") == notification_id),
            None,
        )
        if not alert:
            return {"status": "error", "message": "Red Pulse unlock notification not found."}
        alert["notified_at"] = now_iso()
        save_runtime_state()
        return {"status": "ok", "notification": alert}

    receipt_id = int(notification_id.replace("receipt-", "", 1))
    receipt = next((item for item in pulse_receipts if int(item.get("id") or 0) == receipt_id), None)
    if not receipt:
        return {"status": "error", "message": "Pulse receipt not found."}
    receipt["notified_at"] = now_iso()
    save_runtime_state()
    return {"status": "ok", "receipt": pulse_receipt_payload(receipt)}


@app.post("/api/asmr-entry")
def submit_asmr(payload: dict):
    if not state["modules"].get("asmr", False):
        return {"status": "error", "message": "ASMR Requests is inactive right now."}

    asmr_entries.append({"time": now_iso(), "data": payload})
    return {"status": "ok"}


@app.get("/api/asmr-entries")
def list_asmr():
    return asmr_entries


@app.post("/api/story-entry")
def submit_story(payload: dict):
    if not state["modules"].get("story", False):
        return {"status": "error", "message": "Story Game is inactive right now."}

    story_entries.append({"time": now_iso(), "data": payload})
    return {"status": "ok"}


@app.get("/api/story-entries")
def list_story():
    return story_entries


@app.get("/api/membership/status")
def membership_status(user_id: int | None = None, username: str | None = None):
    user = find_verified_alcove_user(user_id, username)
    if not user:
        return {
            "status": "error",
            "verified": False,
            "message": "Verified Alcove membership required.",
        }
    return {
        "status": "ok",
        "verified": True,
        "user": {
            "user_id": user.get("user_id"),
            "username": user.get("username"),
            "display_name": user.get("display_name") or user.get("label"),
        },
    }


# ---------------------------------
# Alcove Cards multiplayer
# ---------------------------------

@app.get("/api/cards/status")
def cards_status(user_id: int | None = None, username: str | None = None):
    if not cards_api_enabled():
        user = find_verified_alcove_user(user_id, username)
        if not user:
            return {
                "status": "error",
                "verified": False,
                "online_allowed": False,
                "cards_disabled": True,
                "message": "Verified Alcove membership required.",
            }
        return {
            "status": "ok",
            "verified": True,
            "online_allowed": False,
            "cards_disabled": True,
            "user": {
                "user_id": user.get("user_id"),
                "username": user.get("username"),
                "display_name": user.get("display_name") or user.get("label"),
            },
        }
    return get_cards_service(find_verified_alcove_user).status(user_id, username)


@app.get("/api/cards/rooms/{code}/sync")
def cards_sync_room(code: str, user_id: int | None = None, username: str | None = None):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).sync_room(code, user_id, username)


@app.get("/api/cards/profile")
def cards_profile(user_id: int | None = None, username: str | None = None):
    ensure_cards_api_enabled()
    user = find_verified_alcove_user(user_id, username)
    if not user:
        return {"status": "error", "message": "Verified Alcove membership required."}
    return {"status": "ok", "profile": profile_summary(int(user["user_id"]))}


@app.get("/api/cards/challenges")
def cards_challenges(user_id: int | None = None, username: str | None = None):
    ensure_cards_api_enabled()
    user = find_verified_alcove_user(user_id, username)
    if not user:
        return {"status": "error", "message": "Verified Alcove membership required."}
    profile = profile_summary(int(user["user_id"]))
    return {"status": "ok", "challenges": profile.get("challenges") or {}}


@app.post("/api/cards/rooms")
def cards_create_room(payload: CreateRoomPayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).create_room(payload)


@app.post("/api/cards/rooms/{code}/join")
def cards_join_room(code: str, payload: JoinRoomPayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).join_room(code, payload)


@app.delete("/api/cards/rooms/{code}")
def cards_leave_room(code: str, user_id: int):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).leave_room(user_id)


@app.post("/api/cards/queue/join")
def cards_join_queue(payload: QueuePayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).join_queue(payload)


@app.post("/api/cards/queue/leave")
def cards_leave_queue(payload: UserIdPayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).leave_queue(payload.user_id)


@app.post("/api/cards/loadout")
def cards_save_loadout(payload: LoadoutPayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).set_loadout(payload)


@app.post("/api/cards/rooms/{code}/action")
def cards_room_action(code: str, payload: RoomActionPayload):
    ensure_cards_api_enabled()
    return get_cards_service(find_verified_alcove_user).handle_action(code, payload)


@app.get("/api/bot-sync/cards/rewards/pending")
def bot_cards_pending_rewards(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    if not cards_api_enabled():
        return {"status": "ok", "rewards": []}
    return {"status": "ok", "rewards": fetch_pending_rewards()}


@app.post("/api/bot-sync/cards/rewards/claim")
def bot_cards_claim_rewards(payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    if not cards_api_enabled():
        return {"status": "ok", "claimed": 0}
    reward_ids = (payload or {}).get("reward_ids") or []
    mark_rewards_claimed([int(item) for item in reward_ids])
    return {"status": "ok", "claimed": len(reward_ids)}
