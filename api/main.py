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
from fastapi import WebSocket, WebSocketDisconnect
from websocket_manager import manager

app = FastAPI()

API_BUILD = "2026-03-23.1"

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
    local_filename: str = ""
    local_path: str = ""
    video_title: str | None = None


class PayoutPayload(BaseModel):
    copy_from_path: str | None = None


# ---------------------------------
# Helpers
# ---------------------------------

def now_iso() -> str:
    return datetime.datetime.utcnow().isoformat()


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


def entry_is_spin_eligible(entry: dict) -> bool:
    """An entry is eligible for the wheel spin if it was approved by the host and hasn't been played yet."""
    return entry.get("approval_status") == "approved" and not entry.get("played", False)


def get_ready_unplayed_entries(round_number: int):
    return [
        entry
        for entry in wheel_entries
        if entry.get("round_id") == round_number
        and entry_is_spin_eligible(entry)
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
    return {"status": "Alcove API running", "build": API_BUILD}


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
        "build": API_BUILD,
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
        "approval_status": "pending",  # pending | approved | rejected
        "approval_time": None,
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

    if not entry_is_spin_eligible(entry):
        return {"status": "error", "message": "winner entry is not approved or has already been played"}

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
# Entry Moderation (Basic approval system)
# ---------------------------------

@app.post("/api/entry/approve/{entry_id}")
def approve_entry(entry_id: int):
    """Host approves an entry for potential wheel inclusion."""
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "Entry not found"}
    
    entry["approval_status"] = "approved"
    entry["approval_time"] = now_iso()
    # For domains configured without auto-download, immediately make the entry
    # wheel-eligible so approved submissions are not blocked in pending state.
    domain_cfg = get_domain_config(entry.get("submitted_url") or "") or {}
    if not domain_cfg.get("auto_download", False):
        entry["download_status"] = "manual_ready"
        entry["download_error"] = None
        entry["download_method"] = "manual"
        entry["download_completed_at"] = now_iso()
    add_notification("system", f"Entry approved: {entry['data'].get('display_name', 'Unknown')}", False)
    ws_broadcast_bundle()
    
    return {
        "status": "ok",
        "message": f"Entry {entry_id} approved",
    }


@app.post("/api/entry/reject/{entry_id}")
def reject_entry(entry_id: int):
    """Host rejects an entry."""
    entry = find_entry(entry_id)
    if not entry:
        return {"status": "error", "message": "Entry not found"}
    
    entry["approval_status"] = "rejected"
    entry["approval_time"] = now_iso()
    add_notification("system", f"Entry rejected: {entry['data'].get('display_name', 'Unknown')}", False)
    ws_broadcast_bundle()
    
    return {
        "status": "ok",
        "message": f"Entry {entry_id} rejected",
    }


@app.get("/api/entries/pending-approval")
def list_pending_approval():
    """List entries pending host review/approval."""
    current_round = state["current_round"]
    pending = [
        {
            "entry_id": e["id"],
            "round_id": e["round_id"],
            "display_name": e["data"].get("display_name"),
            "video_title": e["data"].get("video_title"),
            "note": e["data"].get("note"),
            "submitted_url": e.get("submitted_url"),
            "submitted_time": e.get("time"),
            "approval_status": e.get("approval_status"),
        }
        for e in wheel_entries
        if e.get("round_id") == current_round and e.get("approval_status") == "pending"
    ]
    return pending


@app.get("/api/entries/approved")
def list_approved_entries():
    """List approved entries ready for download/wheel."""
    current_round = state["current_round"]
    approved = [
        {
            "entry_id": e["id"],
            "round_id": e["round_id"],
            "display_name": e["data"].get("display_name"),
            "video_title": e["data"].get("video_title"),
            "submitted_url": e.get("submitted_url"),
            "approval_time": e.get("approval_time"),
        }
        for e in wheel_entries
        if e.get("round_id") == current_round and e.get("approval_status") == "approved"
    ]
    return approved


# ---------------------------------
# Legacy / extra sections
# ---------------------------------

@app.post("/api/wheel-entry/allow-more")
def allow_more(payload: dict):
    name = payload.get("display_name", "").lower()
    limit = wheel_submission_limits.get(name, 1)
    wheel_submission_limits[name] = limit + 1
    return {"status": "ok"}


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
