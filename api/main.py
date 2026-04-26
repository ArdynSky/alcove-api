from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from urllib.parse import urlparse
from pathlib import Path
from zoneinfo import ZoneInfo
import datetime
import os
import shutil
import asyncio
import json
import random
import sqlite3
from fastapi import Header, HTTPException, WebSocket, WebSocketDisconnect
from .websocket_manager import manager

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:5500",
        "http://localhost:5500",
        "https://euphonious-banoffee-1c8215.netlify.app",
        "https://thealcove.netlify.app",
        "https://ardyn-alcove.com",
        "https://www.ardyn-alcove.com",
    ],
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
FEATURE_FLAGS_PATH = os.getenv(
    "FEATURE_FLAGS_PATH",
    os.path.join(os.getcwd(), "feature_flags.json"),
)
PULSE_SETTINGS_PATH = os.getenv(
    "PULSE_SETTINGS_PATH",
    os.path.join(os.getcwd(), "pulse_settings.json"),
)

for path in [DOWNLOADS_DIR, READY_DIR, ARCHIVE_DIR, PLAYOUT_DIR]:
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
pulse_entries = []
pulse_receipts = []
pulse_red_activations = []
pulse_question_suggestions = []
pulse_daily_summary_posts = []
pulse_disabled_questions = []
synced_alcove_users = []
synced_alcove_analytics = {}
last_bot_sync_at = None

current_now_playing = None
video_reviews = []

pending_comments = []
approved_comments = []

notification_feed = []

wheel_submission_limits = {}
muted_users = set()
current_winner = None
PULSE_DEFAULT_HEAT_THRESHOLD = int(os.getenv("PULSE_HEAT_THRESHOLD", "50"))
PULSE_TESTING_UNLIMITED = os.getenv("PULSE_TESTING_UNLIMITED", "0").strip().lower() in {"1", "true", "yes", "on"}
UK_TZ = ZoneInfo("Europe/London")

state = {
    "current_round": 1,
    "round_status": "closed",  # closed | open | locked | spinning | playing
    "modules": {
        "wheel": True,
        "asmr": False,
        "story": False,
        "shoutouts": False,
    },
}

DEFAULT_FEATURE_FLAGS = {
    "pages": {
        "video_chat": True,
        "archive": True,
        "info": True,
        "wellbeing": True,
        "pulse": False,
        "connect": False,
    },
    "wellbeing": {
        "daily_checkin": True,
        "spotlight": True,
        "pulse": True,
    },
}

PULSE_QUESTIONS = {
    "green": [
        "What has been sitting on your heart lately?",
        "When life feels heavy, what helps you feel held?",
        "What is something you wish people understood about how you feel?",
        "What brings you back to yourself when you start to drift?",
        "What kind of reassurance do you need most these days?",
        "What feeling have you been carrying quietly?",
        "What helps you soften when you have been hard on yourself?",
        "When do you feel most emotionally safe?",
        "What does comfort look like for you right now?",
        "What are you learning about your inner world lately?",
        "What has your body been trying to tell you lately?",
        "When do you feel most at ease in your body?",
        "What helps your body feel comforted?",
        "What kind of rest has been hardest to give yourself?",
        "What physical need have you been neglecting a little?",
        "What helps you feel more grounded in your body?",
        "What does being gentle with your body look like for you?",
        "What part of your physical wellbeing needs more care right now?",
        "What simple act helps your body feel safer or calmer?",
        "What does feeling well in your body mean to you these days?",
        "What part of you do you wish people saw more clearly?",
        "What kind of connection are you craving lately?",
        "What does being understood feel like to you?",
        "What do you long for more of in your life right now?",
        "What kind of presence from another person feels most comforting?",
        "What do you find yourself hoping for quietly?",
        "What helps you feel close to someone?",
        "What do you value most in a meaningful connection?",
        "What part of your story has shaped you the most?",
        "What makes you feel deeply seen?",
    ],
    "red": [
        "What’s your hottest forbidden fantasy you’ve never told anyone?",
        "What’s the sluttiest thing you’ve ever done in public or semi-public?",
        "What exact thing during foreplay instantly makes your cock leak and your hole twitch?",
        "Is there a time you hooked up with someone you really shouldn’t have — who it was and how filthy it got?",
    ],
}

PULSE_QUESTION_CATEGORIES = {
    "What has been sitting on your heart lately?": "Mental health",
    "When life feels heavy, what helps you feel held?": "Mental health",
    "What is something you wish people understood about how you feel?": "Mental health",
    "What brings you back to yourself when you start to drift?": "Mental health",
    "What kind of reassurance do you need most these days?": "Mental health",
    "What feeling have you been carrying quietly?": "Mental health",
    "What helps you soften when you have been hard on yourself?": "Mental health",
    "When do you feel most emotionally safe?": "Mental health",
    "What does comfort look like for you right now?": "Mental health",
    "What are you learning about your inner world lately?": "Mental health",
    "What has your body been trying to tell you lately?": "Physical health",
    "When do you feel most at ease in your body?": "Physical health",
    "What helps your body feel comforted?": "Physical health",
    "What kind of rest has been hardest to give yourself?": "Physical health",
    "What physical need have you been neglecting a little?": "Physical health",
    "What helps you feel more grounded in your body?": "Physical health",
    "What does being gentle with your body look like for you?": "Physical health",
    "What part of your physical wellbeing needs more care right now?": "Physical health",
    "What simple act helps your body feel safer or calmer?": "Physical health",
    "What does feeling well in your body mean to you these days?": "Physical health",
    "What part of you do you wish people saw more clearly?": "General",
    "What kind of connection are you craving lately?": "General",
    "What does being understood feel like to you?": "General",
    "What do you long for more of in your life right now?": "General",
    "What kind of presence from another person feels most comforting?": "General",
    "What do you find yourself hoping for quietly?": "General",
    "What helps you feel close to someone?": "General",
    "What do you value most in a meaningful connection?": "General",
    "What part of your story has shaped you the most?": "General",
    "What makes you feel deeply seen?": "General",
    "What’s your hottest forbidden fantasy you’ve never told anyone?": "General",
    "What’s the sluttiest thing you’ve ever done in public or semi-public?": "General",
    "What exact thing during foreplay instantly makes your cock leak and your hole twitch?": "General",
    "Is there a time you hooked up with someone you really shouldn’t have — who it was and how filthy it got?": "General",
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


class VideoReview(BaseModel):
    rating: int
    review: str
    display_name: str
    anonymous: bool


class StreamComment(BaseModel):
    user_id: int | None = None
    username: str | None = None
    display_name: str
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


class DownloadFailedPayload(BaseModel):
    error: str


class ManualReadyPayload(BaseModel):
    local_filename: str
    local_path: str
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


class FeatureFlagsUpdate(BaseModel):
    pages: dict[str, bool] | None = None
    wellbeing: dict[str, bool] | None = None
    admin_secret: str | None = None


# ---------------------------------
# Helpers
# ---------------------------------

def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


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
    return (at or uk_now()).strftime("%Y-%m-%d")


def pulse_day_label(day_key: str | None = None) -> str:
    raw = day_key or pulse_day_key()
    try:
        parsed = datetime.date.fromisoformat(raw)
    except ValueError:
        return raw
    return parsed.strftime("%d %B %Y")


def normalized_pulse_reset_interval(value) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 12
    return parsed if parsed in {1, 3, 6, 12} else 12


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


def pulse_approved_question_entries():
    rows = []
    for entry in pulse_question_suggestions:
        if entry.get("status") != "approved":
            continue
        rows.append({
            "source": "suggested",
            "suggestion_id": entry.get("id"),
            "pool": entry.get("pool") or "green",
            "category": entry.get("category") or "General",
            "question": entry.get("edited_question") or entry.get("question") or "",
            "active": True,
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


def pulse_question_roster():
    rows = []
    current_id = 1
    all_entries = pulse_default_question_entries() + pulse_approved_question_entries()
    sort_key = {"Mental health": 0, "Physical health": 1, "General": 2}
    all_entries.sort(key=lambda item: (item.get("pool") != "green", sort_key.get(item.get("category"), 99), item.get("question", "").lower()))
    for entry in all_entries:
        if not entry.get("active", True):
            continue
        rows.append({
            "roster_id": current_id,
            "source": entry.get("source"),
            "suggestion_id": entry.get("suggestion_id"),
            "pool": entry.get("pool"),
            "category": entry.get("category"),
            "question": entry.get("question"),
            "answers_count": pulse_question_answer_count(entry.get("question"), entry.get("pool")),
        })
        current_id += 1
    return rows


def find_pulse_question_suggestion(suggestion_id: int):
    for entry in pulse_question_suggestions:
        if int(entry.get("id") or 0) == int(suggestion_id):
            return entry
    return None


def prioritized_random_question(entries: list[dict], seed_value: str = "") -> dict | None:
    if not entries:
        return None
    by_count = sorted(entries, key=lambda item: int(item.get("answers_count") or 0))
    lowest_count = int(by_count[0].get("answers_count") or 0)
    lowest_group = [item for item in by_count if int(item.get("answers_count") or 0) == lowest_count]
    chooser = random.Random(seed_value or pulse_day_key())
    return chooser.choice(lowest_group)


def pulse_question_choices(pool: str, user_id=None, username=None):
    roster = [row for row in pulse_question_roster() if row.get("pool") == pool]
    if pool != "green":
        return [row.get("question") for row in roster if row.get("question")]

    chosen = []
    categories = ("Mental health", "Physical health", "General")
    identity_key = f"{pulse_day_key()}:{user_id or ''}:{(username or '').lower()}"
    for category in categories:
        candidate = prioritized_random_question([
            row for row in roster
            if row.get("category") == category and row.get("question") not in chosen
        ], f"{identity_key}:{category}")
        if candidate:
            chosen.append(candidate.get("question"))

    remaining = [
        row for row in roster
        if row.get("question") not in chosen
    ]
    while len(chosen) < 4 and remaining:
        candidate = prioritized_random_question(remaining, f"{identity_key}:extra:{len(chosen)}")
        if not candidate:
            break
        chosen.append(candidate.get("question"))
        remaining = [row for row in remaining if row.get("question") != candidate.get("question")]

    return [question for question in chosen if question]


def seconds_until_next_uk_midnight() -> int:
    current = uk_now()
    tomorrow = (current + datetime.timedelta(days=1)).date()
    reset_at = datetime.datetime.combine(tomorrow, datetime.time.min, tzinfo=UK_TZ)
    return max(0, int((reset_at - current).total_seconds()))


def pulse_reset_interval_hours() -> int:
    return load_pulse_settings()["reset_interval_hours"]


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


def load_feature_flags() -> dict:
    if not os.path.exists(FEATURE_FLAGS_PATH):
        return merged_feature_flags()
    try:
        with open(FEATURE_FLAGS_PATH, "r", encoding="utf-8") as handle:
            return merged_feature_flags(json.load(handle))
    except (OSError, json.JSONDecodeError):
        return merged_feature_flags()


def save_feature_flags(flags: dict) -> None:
    directory = os.path.dirname(FEATURE_FLAGS_PATH)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(FEATURE_FLAGS_PATH, "w", encoding="utf-8") as handle:
        json.dump(flags, handle, indent=2, sort_keys=True)


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
    interval_hours = pulse_reset_interval_hours()
    return {
        "heat_threshold": threshold,
        "reset_interval_hours": interval_hours,
        "sent_today": sent,
        "remaining_today": remaining,
        "progress_percent": min(100, int((sent / max(threshold, 1)) * 100)),
        "red_unlocked": sent >= threshold,
        "day_key": day_key or pulse_day_key(),
        "day_label": pulse_day_label(day_key),
        "next_unlock_at": next_pulse_unlock_at().isoformat(),
        "next_unlock_label": pulse_unlock_label(),
        "reset_seconds": seconds_until_next_pulse_unlock(),
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


def get_verified_alcove_users():
    if synced_alcove_users:
        return synced_alcove_users

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
        JOIN verified_users v ON v.user_id = p.user_id
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
        WHERE COALESCE(p.verified_at, v.verified_at, '') != ''
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

    return users


def find_verified_alcove_user(user_id=None, username=None):
    username = (username or "").lstrip("@").lower()
    for user in get_verified_alcove_users():
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
    interval = pulse_reset_interval_hours()
    midnight = datetime.datetime.combine(current.date(), datetime.time.min, tzinfo=UK_TZ)
    elapsed_seconds = max(0, int((current - midnight).total_seconds()))
    unlocked = 1 + (elapsed_seconds // (interval * 3600))
    return max(1, min(4, unlocked))


def pulse_testing_unlimited() -> bool:
    return PULSE_TESTING_UNLIMITED


def pulse_heat_unlocked(day_key: str | None = None):
    return pulse_sent_today_count(day_key) >= pulse_heat_threshold()


def pulse_red_unlocked_cycles(day_key: str | None = None) -> int:
    threshold = max(1, pulse_heat_threshold())
    return pulse_sent_today_count(day_key) // threshold


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
    return entry


def pulse_slot_state(user_id=None, username=None, now: datetime.datetime | None = None):
    current = now or uk_now()
    day = pulse_day_key(current)
    sent = pulse_user_sent_entries(user_id, username, day)
    green_used = len([entry for entry in sent if entry.get("pulse_type") == "green"])
    red_used = len([entry for entry in sent if entry.get("pulse_type") == "red"])
    green_total = pulse_base_green_slots(current)
    red_unlocked = pulse_heat_unlocked(day)
    sent_today = pulse_sent_today_count(day)
    threshold = pulse_heat_threshold()
    testing = pulse_testing_unlimited()
    interval_hours = pulse_reset_interval_hours()
    unlocked_cycles = pulse_red_unlocked_cycles(day)
    activated_cycles = len(pulse_red_activations_for_user(user_id, username, day))
    red_ready = unlocked_cycles > activated_cycles
    red_activated = activated_cycles > 0
    red_available = max(activated_cycles - red_used, 0)
    cycle_completed = max(sent_today - (activated_cycles * threshold), 0)
    if red_ready:
        cycle_completed = threshold
    remaining_today = 0 if red_ready else max(threshold - min(cycle_completed, threshold), 0)
    green_available = 99 if testing else max(green_total - green_used, 0)
    return {
        "day_key": day,
        "day_label": pulse_day_label(day),
        "green_total": green_total,
        "green_used": green_used,
        "green_available": green_available,
        "red_unlocked": red_unlocked,
        "red_ready": red_ready,
        "red_activated": red_activated,
        "red_used": red_used,
        "red_available": red_available,
        "red_unlocked_cycles": unlocked_cycles,
        "red_activated_cycles": activated_cycles,
        "sent_today": sent_today,
        "heat_threshold": threshold,
        "reset_interval_hours": interval_hours,
        "cycle_completed": min(cycle_completed, threshold),
        "remaining_today": remaining_today,
        "next_green_unlock_at": pulse_unlock_label(current, interval_hours),
        "next_unlock_at": next_pulse_unlock_at(current, interval_hours).isoformat(),
        "reset_seconds": seconds_until_next_pulse_unlock(current, interval_hours),
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
        "answer": entry.get("response_answer"),
        "status": entry.get("status"),
        "sent_at": entry.get("sent_at"),
        "delivered_at": entry.get("delivered_at"),
        "responded_at": entry.get("responded_at"),
        "day_key": entry.get("day_key"),
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


def get_spotlight_entry(entry_id: int):
    for entry in spotlight_entries:
        if int(entry.get("id") or 0) == int(entry_id):
            return entry
    return None


def spotlight_status_payload(nominator_user_id=None, nominator_username=None):
    submitted = spotlight_today_exists(nominator_user_id, nominator_username)
    return {
        "submitted_today": submitted,
        "reset_seconds": seconds_until_next_uk_midnight(),
        "reset_label": "midnight UK time",
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


def add_notification(kind: str, text: str, public: bool = True):
    notification_feed.append(
        {
            "id": len(notification_feed) + 1,
            "kind": kind,
            "text": text,
            "public": public,
            "time": now_iso(),
        }
    )
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


def get_room_users():
    users = {}
    for entry in wheel_entries + archived_wheel_entries:
        name = (entry.get("data", {}) or {}).get("display_name")
        if not name:
            continue
        key = name.strip().lower()
        users.setdefault(key, {
            "display_name": name,
            "muted": key in muted_users,
            "submission_limit": wheel_submission_limits.get(key, 1),
            "current_round_entries": 0,
            "last_seen": entry.get("time"),
        })
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
            "muted": key in muted_users,
            "submission_limit": wheel_submission_limits.get(key, 1),
            "current_round_entries": 0,
            "last_seen": comment.get("time"),
        })
        users[key]["muted"] = key in muted_users
        if comment.get("time") and (not users[key]["last_seen"] or comment.get("time") > users[key]["last_seen"]):
            users[key]["last_seen"] = comment.get("time")

    for key, user in users.items():
        user["muted"] = key in muted_users
        user["submission_limit"] = wheel_submission_limits.get(key, 1)

    return sorted(users.values(), key=lambda u: (u["display_name"] or "").lower())

def get_next_spin_pool(round_number: int):
    return get_ready_unplayed_entries(round_number)


def entry_is_download_ready(entry: dict) -> bool:
    return entry.get("download_status") in {"ready", "manual_ready"}


def get_ready_unplayed_entries(round_number: int):
    return [
        entry
        for entry in wheel_entries
        if entry.get("round_id") == round_number
        and not entry.get("played", False)
        and entry_is_download_ready(entry)
    ]


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

@app.get("/")
def root():
    return {"status": "Alcove API running"}


@app.get("/api/alcove-users")
def alcove_users():
    users = get_verified_alcove_users()
    return {
        "status": "ok",
        "count": len(users),
        "users": users,
        "source": "bot_sync" if synced_alcove_users else "fox_logs",
        "last_bot_sync_at": last_bot_sync_at,
        "db_available": os.path.exists(FOX_LOGS_DB_PATH),
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


@app.post("/api/bot-sync/alcove")
def bot_sync_alcove(payload: BotSyncPayload, x_bot_sync_secret: str | None = Header(default=None)):
    global synced_alcove_users, synced_alcove_analytics, last_bot_sync_at

    verify_bot_sync_secret(x_bot_sync_secret)

    synced_alcove_users = payload.users or []
    synced_alcove_analytics = payload.analytics or {}
    last_bot_sync_at = payload.synced_at or now_iso()

    return {
        "status": "ok",
        "users": len(synced_alcove_users),
        "synced_at": last_bot_sync_at,
    }


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
    }


# ---------------------------------
# Round controls
# ---------------------------------

@app.post("/api/round/open")
def open_round():
    current_round = state["current_round"]
    state["round_status"] = "open"
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
    current_now_playing = None
    chosen = random.choice(pool)
    current_winner = {
        "entry_id": chosen["id"],
        "entrant_name": chosen["data"].get("display_name", "Unknown"),
        "video_title": chosen["data"].get("video_title"),
        "local_filename": chosen.get("local_filename"),
        "local_path": chosen.get("local_path"),
        "time": now_iso(),
    }
    add_notification("winner", f"Winner: {current_winner['entrant_name']}", True)
    add_notification("system", f"Round {current_round} spinning", True)

    ws_broadcast_bundle()
    return {"status": "ok", "message": f"Round {current_round} spin started", "winner": current_winner}


@app.post("/api/round/end")
def end_round():
    global current_winner, current_now_playing
    current_round = state["current_round"]
    state["round_status"] = "closed"
    current_winner = None
    current_now_playing = None
    video_reviews.clear()
    add_notification("system", f"Round {current_round} ended", True)
    state["current_round"] += 1
    wheel_submission_limits.clear()
    return {
        "status": "ok",
        "message": f"Round {current_round} ended. Round {state['current_round']} ready.",
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
    return {"status": "ok", "features": load_feature_flags()}


@app.post("/api/feature-flags")
def update_feature_flags(payload: FeatureFlagsUpdate, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret or payload.admin_secret)
    flags = load_feature_flags()
    incoming = payload.dict(exclude_none=True)
    for group, values in incoming.items():
        if group not in flags or not isinstance(values, dict):
            continue
        for key, value in values.items():
            if key in flags[group]:
                flags[group][key] = bool(value)
    save_feature_flags(flags)
    return {"status": "ok", "features": flags}


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

    domain_cfg = get_domain_config(entry.link)
    if not domain_cfg:
        return {
            "status": "error",
            "message": "This site is not supported for video downloads."
        }

    entry_data = entry.dict()

    if entry_data["display_name"].strip().lower() == "anonymous":
        entry_data["display_name"] = get_next_anonymous_wheel_name()

    user_key = entry_data["display_name"].lower()
    allowed = wheel_submission_limits.get(user_key, 1)

    current = sum(
        1
        for e in get_round_entries(state["current_round"])
        if e["data"]["display_name"].lower() == user_key
    )

    if current >= allowed:
        return {"status": "error", "message": "You already have an entry in this round."}

    submitted_url = entry_data["link"]
    domain = normalize_domain(submitted_url)

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
            "played": entry.get("played", False),
            "played_at": entry.get("played_at"),
            "time": entry.get("time"),
        }
        for entry in wheel_entries
    ]


@app.get("/api/wheel-entries-archived")
def list_archived_wheel_entries():
    return archived_wheel_entries


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
        if entry.get("download_status") in {"pending", "failed"}
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
        }
        for entry in pending
    ]


@app.post("/api/downloads/start/{entry_id}")
def mark_download_start(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "extracting"
    entry["download_started_at"] = now_iso()
    entry["download_error"] = None
    return {"status": "ok"}


@app.post("/api/downloads/downloading/{entry_id}")
def mark_downloading(entry_id: int, payload: dict | None = None):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "downloading"
    if payload and payload.get("direct_media_url"):
        entry["direct_media_url"] = payload["direct_media_url"]
    return {"status": "ok"}


@app.post("/api/downloads/complete/{entry_id}")
def mark_download_complete(entry_id: int, payload: DownloadCompletePayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "ready"
    entry["download_error"] = None
    entry["local_filename"] = payload.local_filename
    entry["local_path"] = payload.local_path
    entry["direct_media_url"] = payload.direct_media_url
    entry["download_method"] = payload.download_method
    entry["download_completed_at"] = now_iso()
    if payload.video_title:
        entry["data"]["video_title"] = payload.video_title
    return {"status": "ok"}


@app.post("/api/downloads/failed/{entry_id}")
def mark_download_failed(entry_id: int, payload: DownloadFailedPayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "failed"
    entry["download_error"] = payload.error
    entry["download_method"] = "auto"
    return {"status": "ok"}


@app.post("/api/downloads/manual-ready/{entry_id}")
def mark_manual_ready(entry_id: int, payload: ManualReadyPayload):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "manual_ready"
    entry["download_error"] = None
    entry["local_filename"] = payload.local_filename
    entry["local_path"] = payload.local_path
    entry["download_method"] = "manual"
    entry["download_completed_at"] = now_iso()
    if payload.video_title:
        entry["data"]["video_title"] = payload.video_title
    return {"status": "ok"}


@app.post("/api/downloads/retry/{entry_id}")
def retry_download(entry_id: int):
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "entry not found"}

    entry["download_status"] = "pending"
    entry["download_error"] = None
    entry["direct_media_url"] = None
    entry["download_started_at"] = None
    entry["download_completed_at"] = None
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

    if not entry_is_download_ready(entry):
        return {"status": "error", "message": "winner is not download-ready"}

    current_winner = {
        "entry_id": entry["id"],
        "entrant_name": entry["data"].get("display_name", "Unknown"),
        "video_title": entry["data"].get("video_title"),
        "local_filename": entry.get("local_filename"),
        "local_path": entry.get("local_path"),
        "time": now_iso(),
    }
    add_notification("winner", f"Winner: {current_winner['entrant_name']}", True)

    ws_broadcast_bundle()
    return {"status": "ok", "winner": current_winner}


@app.post("/api/winner/clear")
def clear_winner():
    global current_winner
    current_winner = None
    return {"status": "ok"}


@app.get("/api/current-winner")
def get_current_winner():
    return current_winner


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
    global current_now_playing
    if current_now_playing is None or "data" not in current_now_playing:
        return {"status": "error", "message": "Reviews are closed until a video is live."}

    current_data = current_now_playing["data"]
    video_title = current_data.get("video_title", "") or "No video title set yet"
    chosen_by = current_data.get("display_name", "") or "Unknown"

    reviewer_name = review.display_name
    if review.anonymous:
        reviewer_name = "Anonymous"

    video_reviews.append(
        {
            "video_entry_id": current_now_playing.get("id"),
            "video_title": video_title,
            "chosen_by": chosen_by,
            "rating": review.rating,
            "review": review.review,
            "display_name": reviewer_name,
            "time": now_iso(),
        }
    )

    return {"status": "ok", "reviews": len(video_reviews)}


@app.get("/api/reviews")
def list_reviews():
    return video_reviews


# ---------------------------------
# Stream comments moderation
# ---------------------------------

@app.post("/api/stream-comment")
def submit_stream_comment(comment: StreamComment):
    text = comment.text.strip()
    if len(text) == 0:
        return {"status": "error", "message": "Comment cannot be empty."}
    if len(text) > 220:
        return {"status": "error", "message": "Comments must be 220 characters or fewer."}

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
            latest = sorted(recent_comments, key=lambda x: x["time"], reverse=True)[0]
            latest_time = datetime.datetime.fromisoformat(latest["time"])
            seconds_since = (datetime.datetime.utcnow() - latest_time).total_seconds()
            if seconds_since < 4:
                return {"status": "error", "message": "Please wait a moment before sending another comment."}

    approved_comments.append(
        {
            "comment_id": get_next_comment_id(),
            "user_id": comment.user_id,
            "username": comment.username,
            "display_name": display_name,
            "text": text,
            "time": now_iso(),
            "approved": True,
        }
    )
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
            archived_wheel_entries.append(archived)

            if current_now_playing and current_now_playing["id"] == entry_id:
                current_now_playing = None
                video_reviews.clear()

            if current_winner and current_winner["entry_id"] == entry_id:
                current_winner = None

            del wheel_entries[i]
            state["round_status"] = "locked" if get_ready_unplayed_entries(state["current_round"]) else "closed"
            ws_broadcast_bundle()
            return {"status": "ok"}

    return {"status": "error"}


# ---------------------------------
# Legacy / extra sections
# ---------------------------------

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
        f"nominator_user_id={entry.nominator_user_id} nominator_username={entry.nominator_username!r}",
        flush=True,
    )
    nominee = find_verified_alcove_user(entry.nominee_user_id, entry.nominee_username)
    if not nominee:
        print(f"[{now_iso()}] spotlight submit rejected: nominee not verified", flush=True)
        return {"status": "error", "message": "That user is not a verified Alcove resident."}

    if not entry.nominator_user_id and not entry.nominator_username:
        print(f"[{now_iso()}] spotlight submit rejected: missing Telegram identity", flush=True)
        return {
            "status": "error",
            "message": "Could not identify who submitted this Spotlight. Please open the Mini App from Telegram and try again.",
        }

    nominator = find_verified_alcove_user(entry.nominator_user_id, entry.nominator_username)
    if entry.nominator_user_id and nominee.get("user_id") == entry.nominator_user_id:
        print(f"[{now_iso()}] spotlight submit rejected: self nomination by user_id={entry.nominator_user_id}", flush=True)
        return {"status": "error", "message": "You cannot nominate yourself."}
    if entry.nominator_username and (nominee.get("username") or "").lower() == entry.nominator_username.lower():
        print(f"[{now_iso()}] spotlight submit rejected: self nomination by username={entry.nominator_username!r}", flush=True)
        return {"status": "error", "message": "You cannot nominate yourself."}

    if spotlight_today_exists(entry.nominator_user_id, entry.nominator_username):
        print(f"[{now_iso()}] spotlight submit rejected: already submitted today", flush=True)
        return {"status": "error", "message": "You have already submitted a Spotlight today."}

    data = entry.dict()
    data["id"] = len(spotlight_entries) + 1
    data["time"] = now_iso()
    data["day_key"] = pulse_day_key()
    data["status"] = "pending_review"
    data["edited_reason"] = None
    data["review_message_sent"] = False
    data["reviewed_by"] = None
    data["reviewed_at"] = None
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
    print(
        f"[{now_iso()}] spotlight submit success spotlight_id={data['id']} "
        f"nominator_user_id={data.get('nominator_user_id')} nominee_user_id={data.get('nominee_user_id')}",
        flush=True,
    )
    add_notification("spotlight", f"Spotlight submitted for {entry.nominee_display_name}", False)
    return {
        "status": "ok",
        "spotlight_id": data["id"],
        "spotlights": len(spotlight_entries),
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


@app.post("/api/bot-sync/spotlights/{entry_id}")
def bot_update_spotlight(entry_id: int, payload: SpotlightReviewUpdate, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = get_spotlight_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "Spotlight not found"}

    if payload.status is not None:
        entry["status"] = payload.status
    if payload.edited_reason is not None:
        entry["edited_reason"] = payload.edited_reason
    if payload.review_message_sent is not None:
        entry["review_message_sent"] = payload.review_message_sent
    if payload.reviewed_by is not None:
        entry["reviewed_by"] = payload.reviewed_by
    if payload.reviewed_at is not None:
        entry["reviewed_at"] = payload.reviewed_at

    return {"status": "ok", "entry": entry}


@app.get("/api/pulse-questions")
def get_pulse_questions(user_id: int | None = None, username: str | None = None):
    return {
        "status": "ok",
        "questions": {
            "green": pulse_question_choices("green", user_id, username),
            "red": pulse_question_choices("red", user_id, username),
        },
        "heat_threshold": pulse_heat_threshold(),
    }


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
        return {"status": "error", "message": "Please add a little more detail before sending your question."}

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
        "review_message_sent": False,
        "reviewed_at": None,
        "reviewed_by": None,
    }
    pulse_question_suggestions.append(entry)
    print(
        f"[{now_iso()}] pulse question submit success suggestion_id={entry['id']} "
        f"pool={pool} category={category!r} user_id={identity.get('user_id')} username={identity.get('username')!r}",
        flush=True,
    )
    return {
        "status": "ok",
        "message": "Question saved for a future Pulse round.",
        "entry": entry,
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
        "assigned": assignments,
        "received": receipts,
        "responded": responded,
        "sent": sent,
        "pending_queue": len([entry for entry in pulse_entries_for_day() if entry.get("status") == "queued"]),
    }


@app.get("/api/pulse-question-roster")
def get_pulse_question_roster():
    return {"status": "ok", "questions": pulse_question_roster()}


@app.post("/api/pulse-red-activate")
def activate_pulse_red(payload: PulseReceiptAck):
    identity = pulse_user_identity(payload.user_id, payload.username)
    if not identity:
        return {"status": "error", "message": "Could not identify this Pulse user."}

    slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    if not slots["red_ready"]:
        return {
            "status": "error",
            "message": "Red Pulse has not been unlocked yet.",
            "slots": slots,
        }

    activation = activate_red_pulse_for_user(identity, slots["day_key"])
    updated_slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    return {
        "status": "ok",
        "message": "Your Red Pulse is now active.",
        "activation": activation,
        "slots": updated_slots,
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

    sender_note = (entry.answer or "").strip()
    question = (entry.question or "").strip()
    if len(sender_note) < 3:
        print(f"[{now_iso()}] pulse submit rejected: answer too short", flush=True)
        return {"status": "error", "message": "Please add your own anonymous answer before sending your Pulse."}
    if question not in pulse_active_questions(pulse_type):
        print(f"[{now_iso()}] pulse submit rejected: question not active", flush=True)
        return {"status": "error", "message": "Please choose one of the current Pulse questions."}

    slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    if pulse_type == "green" and slots["green_available"] <= 0:
        print(f"[{now_iso()}] pulse submit rejected: no green slot available", flush=True)
        return {"status": "error", "message": "You do not have a green Pulse available right now."}
    if pulse_type == "red" and slots["red_available"] <= 0:
        print(f"[{now_iso()}] pulse submit rejected: no red slot available", flush=True)
        return {"status": "error", "message": "A red Pulse is not available right now."}

    data = {
        "id": len(pulse_entries) + 1,
        "day_key": pulse_day_key(),
        "pulse_type": pulse_type,
        "category": pulse_question_category(question, pulse_type),
        "question": question,
        "sender_note": sender_note,
        "answer": sender_note,
        "sender_user_id": identity.get("user_id"),
        "sender_username": identity.get("username"),
        "sender_display_name": identity.get("display_name") or identity.get("label"),
        "sent_at": now_iso(),
        "status": "queued",
        "delivered_to_user_id": None,
        "delivered_to_username": None,
        "delivered_to_display_name": None,
        "delivered_at": None,
        "assignment_notified_at": None,
        "assignment_notify_after": None,
        "response_answer": None,
        "responded_at": None,
        "admin_posted_at": None,
    }
    pulse_entries.append(data)
    assigned = pulse_match_next_receiver(identity, exclude_entry_id=data["id"])
    print(
        f"[{now_iso()}] pulse submit success pulse_id={data['id']} pulse_type={pulse_type} "
        f"sender_user_id={identity.get('user_id')} sender_username={identity.get('username')!r} "
        f"assigned_to_user_id={assigned.get('delivered_to_user_id') if assigned else None} queued={assigned is None}",
        flush=True,
    )
    updated_slots = pulse_slot_state(identity.get("user_id"), identity.get("username"))
    add_notification("pulse", "Anonymous Pulse submitted", False)
    return {
        "status": "ok",
        "pulse_id": data["id"],
        "slots": updated_slots,
        "assigned": public_pulse_payload(assigned) if assigned else None,
        "queued": assigned is None,
    }


@app.post("/api/pulse-assignments/{pulse_id}/respond")
def respond_to_pulse_assignment(pulse_id: int, payload: PulseAssignmentResponse):
    print(
        f"[{now_iso()}] pulse answer attempt pulse_id={pulse_id} user_id={payload.user_id} "
        f"username={payload.username!r}",
        flush=True,
    )
    entry = next((item for item in pulse_entries if int(item.get("id") or 0) == int(pulse_id)), None)
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
    return {"status": "ok", "receipt": pulse_receipt_payload(receipt)}


@app.get("/api/bot-sync/pulses/pending")
def bot_pending_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    queued = [entry for entry in pulse_entries_for_day() if entry.get("status") == "queued"]
    return {"status": "ok", "entries": queued}


@app.get("/api/bot-sync/pulses/completed")
def bot_completed_pulses(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    return {"status": "ok", "entries": pulse_completed_admin_entries()}


@app.get("/api/bot-sync/pulse-questions/pending")
def bot_pending_pulse_question_suggestions(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entries = [
        entry for entry in pulse_question_suggestions
        if entry.get("status") == "pending_review" and not entry.get("review_message_sent")
    ]
    return {"status": "ok", "entries": entries}


@app.get("/api/bot-sync/pulse-questions/roster")
def bot_pulse_question_roster(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    return {"status": "ok", "questions": pulse_question_roster()}


@app.get("/api/bot-sync/pulse-questions/{suggestion_id}")
def bot_pulse_question_suggestion(suggestion_id: int, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    return {"status": "ok", "entry": entry}


@app.post("/api/bot-sync/pulse-questions/{suggestion_id}")
def bot_update_pulse_question_suggestion(suggestion_id: int, payload: dict | None = None, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = find_pulse_question_suggestion(suggestion_id)
    if not entry:
        return {"status": "error", "message": "Pulse question suggestion not found."}
    payload = payload or {}
    if "edited_question" in payload:
        entry["edited_question"] = (payload.get("edited_question") or "").strip() or None
    if "category" in payload:
        entry["category"] = payload.get("category") or entry.get("category")
    if "status" in payload:
        entry["status"] = payload.get("status")
    if "review_message_sent" in payload:
        entry["review_message_sent"] = bool(payload.get("review_message_sent"))
    if "reviewed_by" in payload:
        entry["reviewed_by"] = payload.get("reviewed_by")
    if "reviewed_at" in payload:
        entry["reviewed_at"] = payload.get("reviewed_at")
    return {"status": "ok", "entry": entry}


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

    return {"status": "ok", "deleted": entry}


@app.post("/api/bot-sync/pulses/completed/{pulse_id}")
def mark_completed_pulse_posted(pulse_id: int, x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    entry = next((item for item in pulse_entries if int(item.get("id") or 0) == int(pulse_id)), None)
    if not entry:
        return {"status": "error", "message": "Pulse not found."}
    entry["admin_posted_at"] = now_iso()
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
    return {"status": "ok", "summary": state}


@app.get("/api/bot-sync/pulses/notifications")
def bot_pending_pulse_notifications(x_bot_sync_secret: str | None = Header(default=None)):
    verify_bot_sync_secret(x_bot_sync_secret)
    rows = []
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
        if receipt.get("notified_at") or receipt.get("acknowledged_at"):
            continue
        if not iso_has_passed(receipt.get("notify_after")):
            continue
        payload = pulse_receipt_payload(receipt)
        if not payload:
            continue
        rows.append({
            "notification_id": f"receipt-{receipt.get('id')}",
            "kind": "reply_received",
            "receipt_id": receipt.get("id"),
            "recipient_user_id": receipt.get("recipient_user_id"),
            "recipient_username": receipt.get("recipient_username"),
            "recipient_display_name": receipt.get("recipient_display_name"),
            "received_at": receipt.get("received_at"),
            "notify_after": receipt.get("notify_after"),
            "pulse": payload,
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
        return {"status": "ok", "pulse": public_pulse_payload(entry)}

    receipt_id = int(notification_id.replace("receipt-", "", 1))
    receipt = next((item for item in pulse_receipts if int(item.get("id") or 0) == receipt_id), None)
    if not receipt:
        return {"status": "error", "message": "Pulse receipt not found."}
    receipt["notified_at"] = now_iso()
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
