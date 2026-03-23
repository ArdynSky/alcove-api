import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

API_BASE = os.getenv("ALCOVE_API_BASE", "https://alcove-api.onrender.com/api")

CHROME_PATH = os.environ.get("ALCOVE_CHROME_PATH", r"C:\Program Files\Google\Chrome\Application\chrome.exe")

DOWNLOADS_DIR = Path.home() / "Desktop" / "Alcove" / "Downloads"
READY_DIR = Path.home() / "Desktop" / "Alcove" / "Ready"

POLL_INTERVAL_SECONDS = 3
DOWNLOAD_WAIT_TIMEOUT_SECONDS = 600   # 10 minutes
FILE_STABLE_SECONDS = 3
OPEN_IN_NEW_WINDOW = False

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v"}
TEMP_EXTENSIONS = {".crdownload", ".part", ".tmp"}

# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def api_get(path: str):
    url = f"{API_BASE}{path}"
    req = Request(url, method="GET")
    with urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def api_post(path: str, payload: dict | None = None):
    url = f"{API_BASE}{path}"
    data = None
    headers = {"Content-Type": "application/json"}

    if payload is not None:
        data = json.dumps(payload).encode("utf-8")

    req = Request(url, data=data, headers=headers, method="POST")
    with urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


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


def get_file_signature(path: Path):
    try:
        stat = path.stat()
        return (str(path), stat.st_size, int(stat.st_mtime))
    except FileNotFoundError:
        return None


def snapshot_downloads():
    if not DOWNLOADS_DIR.exists():
        DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    return {get_file_signature(p) for p in DOWNLOADS_DIR.iterdir() if p.is_file()}


def is_temp_file(path: Path) -> bool:
    return path.suffix.lower() in TEMP_EXTENSIONS


def is_video_file(path: Path) -> bool:
    name_lower = path.name.lower()
    return any(name_lower.endswith(ext) for ext in VIDEO_EXTENSIONS)


def wait_until_file_stable(path: Path, stable_seconds: int, timeout_seconds: int) -> bool:
    start = time.time()
    last_size = -1
    stable_since = None

    while time.time() - start < timeout_seconds:
        if not path.exists():
            time.sleep(1)
            continue

        if is_temp_file(path):
            time.sleep(1)
            continue

        try:
            size = path.stat().st_size
        except FileNotFoundError:
            time.sleep(1)
            continue

        if size > 0 and size == last_size:
            if stable_since is None:
                stable_since = time.time()
            elif time.time() - stable_since >= stable_seconds:
                return True
        else:
            stable_since = None

        last_size = size
        time.sleep(1)

    return False


def wait_for_next_download(before_snapshot: set, timeout_seconds: int):
    start = time.time()

    while time.time() - start < timeout_seconds:
        current_files = [p for p in DOWNLOADS_DIR.iterdir() if p.is_file()]

        new_candidates = []
        for path in current_files:
            sig = get_file_signature(path)
            if sig not in before_snapshot and is_video_file(path) and not is_temp_file(path):
                new_candidates.append(path)

        if new_candidates:
            new_candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            candidate = new_candidates[0]
            if wait_until_file_stable(candidate, FILE_STABLE_SECONDS, 60):
                return candidate

        time.sleep(1)

    return None


def build_target_filename(entry_id: int, display_name: str, source_path: Path) -> str:
    safe_name = sanitize_name(display_name)
    ext = source_path.suffix.lower() or ".mp4"
    if ext not in VIDEO_EXTENSIONS:
        ext = ".mp4"
    return f"{entry_id:04d}_{safe_name}{ext}"


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    counter = 2
    while True:
        candidate = parent / f"{stem}_{counter}{suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def move_to_ready(source_path: Path, entry_id: int, display_name: str) -> Path:
    READY_DIR.mkdir(parents=True, exist_ok=True)
    target_name = build_target_filename(entry_id, display_name, source_path)
    target_path = READY_DIR / target_name
    target_path = ensure_unique_path(target_path)
    shutil.move(str(source_path), str(target_path))
    return target_path


def open_in_chrome(url: str):
    args = [CHROME_PATH]
    if OPEN_IN_NEW_WINDOW:
        args.append("--new-window")
    args.append(url)
    subprocess.Popen(args)


def print_banner():
    print("=" * 72)
    print("ALCOVE DOWNLOADER WORKER")
    print("=" * 72)
    print(f"API_BASE:      {API_BASE}")
    print(f"CHROME_PATH:   {CHROME_PATH}")
    print(f"DOWNLOADS_DIR: {DOWNLOADS_DIR}")
    print(f"READY_DIR:     {READY_DIR}")
    print("=" * 72)


# --------------------------------------------------
# MAIN LOOP
# --------------------------------------------------

def main():
    print_banner()

    if not os.path.exists(CHROME_PATH):
        print(f"[FATAL] Chrome not found at: {CHROME_PATH}")
        return

    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    READY_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            pending = api_get("/api/downloads/pending")
        except (HTTPError, URLError, TimeoutError, ConnectionError) as exc:
            print(f"[ERROR] Cannot reach backend: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        except Exception as exc:
            print(f"[ERROR] Unexpected backend error: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        if not pending:
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        entry = pending[0]
        entry_id = entry["entry_id"]
        display_name = entry.get("display_name") or "Unknown"
        submitted_url = entry.get("submitted_url") or ""
        source_domain = entry.get("source_domain") or "unknown"
        current_status = entry.get("download_status") or "pending"

        print()
        print("-" * 72)
        print(f"[NEXT] Entry #{entry_id:04d} | {display_name}")
        print(f"[SITE] {source_domain}")
        print(f"[URL]  {submitted_url}")
        print(f"[STATUS BEFORE] {current_status}")

        try:
            api_post(f"/api/downloads/start/{entry_id}")
        except Exception as exc:
            print(f"[ERROR] Could not mark download start for #{entry_id}: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        before_snapshot = snapshot_downloads()

        try:
            open_in_chrome(submitted_url)
        except Exception as exc:
            print(f"[ERROR] Could not open Chrome for #{entry_id}: {exc}")
            try:
                api_post(f"/api/downloads/failed/{entry_id}", {"error": f"Could not open Chrome: {exc}"})
            except Exception:
                pass
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        print(f"[OPENED] Chrome page for #{entry_id:04d}")
        print("[ACTION] Use Video DownloadHelper now:")
        print("         1. Click the extension icon")
        print("         2. Click Download")
        print("         3. Wait for the file to appear in Downloads")
        print(f"[WAITING] Up to {DOWNLOAD_WAIT_TIMEOUT_SECONDS} seconds for next downloaded file...")

        downloaded_file = wait_for_next_download(before_snapshot, DOWNLOAD_WAIT_TIMEOUT_SECONDS)

        if downloaded_file is None:
            print(f"[FAILED] No new downloaded file detected for entry #{entry_id:04d}")
            try:
                api_post(
                    f"/api/downloads/failed/{entry_id}",
                    {"error": "No downloaded file detected within timeout"}
                )
            except Exception as exc:
                print(f"[WARN] Could not update failed status: {exc}")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        print(f"[DETECTED] {downloaded_file.name}")

        try:
            target_path = move_to_ready(downloaded_file, entry_id, display_name)
        except Exception as exc:
            print(f"[FAILED] Could not move file for #{entry_id:04d}: {exc}")
            try:
                api_post(
                    f"/api/downloads/failed/{entry_id}",
                    {"error": f"Could not move file: {exc}"}
                )
            except Exception:
                pass
            time.sleep(POLL_INTERVAL_SECONDS)
            continue

        payload = {
            "local_filename": target_path.name,
            "local_path": str(target_path),
            "direct_media_url": None,
            "video_title": entry.get("video_title"),
            "download_method": "manual"
        }

        try:
            api_post(f"/api/downloads/complete/{entry_id}", payload)
            print(f"[READY] Entry #{entry_id:04d} marked ready")
            print(f"[FILE]  {target_path}")
        except Exception as exc:
            print(f"[WARN] File moved, but backend update failed for #{entry_id:04d}: {exc}")
            print("[WARN] You can recover by using /api/downloads/manual-ready/{entry_id} in /docs")
            print(f"[WARN] local_filename = {target_path.name}")
            print(f"[WARN] local_path     = {target_path}")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
