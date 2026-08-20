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
