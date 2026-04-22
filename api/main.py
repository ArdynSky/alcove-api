from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from urllib.parse import urlparse
from pathlib import Path
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


# ---------------------------------
# Helpers
# ---------------------------------

def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


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


def spotlight_today_exists(nominator_user_id=None, nominator_username=None):
    today = datetime.datetime.utcnow().date().isoformat()
    nominator_username = (nominator_username or "").lower()
    for entry in spotlight_entries:
        if not str(entry.get("time", "")).startswith(today):
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
    verify_bot_sync_secret(x_bot_sync_secret)
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
    nominee = find_verified_alcove_user(entry.nominee_user_id, entry.nominee_username)
    if not nominee:
        return {"status": "error", "message": "That user is not a verified Alcove resident."}

    if not entry.nominator_user_id and not entry.nominator_username:
        return {
            "status": "error",
            "message": "Could not identify who submitted this Spotlight. Please open the Mini App from Telegram and try again.",
        }

    nominator = find_verified_alcove_user(entry.nominator_user_id, entry.nominator_username)
    if entry.nominator_user_id and nominee.get("user_id") == entry.nominator_user_id:
        return {"status": "error", "message": "You cannot nominate yourself."}
    if entry.nominator_username and (nominee.get("username") or "").lower() == entry.nominator_username.lower():
        return {"status": "error", "message": "You cannot nominate yourself."}

    if spotlight_today_exists(entry.nominator_user_id, entry.nominator_username):
        return {"status": "error", "message": "You have already submitted a Spotlight today."}

    data = entry.dict()
    data["id"] = len(spotlight_entries) + 1
    data["time"] = now_iso()
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
    add_notification("spotlight", f"Spotlight submitted for {entry.nominee_display_name}", False)
    return {"status": "ok", "spotlight_id": data["id"], "spotlights": len(spotlight_entries)}


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
