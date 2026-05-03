from __future__ import annotations

import json
import os
import shutil
import subprocess
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
from yt_dlp import YoutubeDL


HOST = "127.0.0.1"
PORT = 8011
API_DEFAULT = os.getenv("ALCOVE_REMOTE_API_BASE", "https://alcove-api.onrender.com/api")
ALCOVE_ROOT = Path.home() / "Desktop" / "Alcove"
DOWNLOADS_DIR = ALCOVE_ROOT / "Downloads"
READY_DIR = ALCOVE_ROOT / "Ready"
PLAYOUT_DIR = ALCOVE_ROOT / "Playout"
CURRENT_PICK_PATH = PLAYOUT_DIR / "current_pick.mp4"
VIDEO_EXTENSIONS = {".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".wmv"}
DEFAULT_FFMPEG_EXE = Path(
    r"F:\Downloads - Copy\ffmpeg-8.0.1-full_build (2)\ffmpeg-8.0.1-full_build\bin\ffmpeg.exe"
)
FFMPEG_EXE = Path(os.getenv("ALCOVE_FFMPEG_EXE") or DEFAULT_FFMPEG_EXE)
TARGET_VIDEO_HEIGHT = 720
TARGET_VIDEO_BITRATE = "2800k"
TARGET_MAXRATE = "3200k"
TARGET_BUFSIZE = "6400k"
TARGET_AUDIO_BITRATE = "128k"
YTDLP_FORMAT = "best[height<=480][ext=mp4]/best[height<=480]/best[ext=mp4]/best[height<=480]/best"
ALLOWED_ORIGINS = {
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "https://thealcove.netlify.app",
    "https://ardyn-alcove.com",
    "https://www.ardyn-alcove.com",
}


def normalize_api_base(value: str | None) -> str:
    raw = (value or API_DEFAULT).strip()
    if not raw:
        raw = API_DEFAULT
    base = raw.rstrip("/")
    if not base.lower().endswith("/api"):
        base = f"{base}/api"
    return base


def api_post(api_base: str, path: str, payload: dict) -> dict:
    url = f"{normalize_api_base(api_base)}{path}"
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def sanitize_name(text: str) -> str:
    keep: list[str] = []
    for ch in text:
        if ch.isalnum():
            keep.append(ch)
        elif ch in {" ", "-", "_"}:
            keep.append("_")
    cleaned = "".join(keep).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned or "Unknown"


def clean_video_title(text: str | None) -> str:
    raw = str(text or "").strip()
    if not raw:
        return "Unknown"
    raw = raw[:80]
    return sanitize_name(raw)


def is_video_file(path: Path) -> bool:
    return path.suffix.lower() in VIDEO_EXTENSIONS


def ensure_unique_path(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    counter = 2
    while True:
        candidate = path.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def build_ready_filename(entry_id: int, display_name: str, source_path: Path, video_title: str | None = None) -> str:
    safe_name = clean_video_title(video_title) if video_title else sanitize_name(display_name)
    ext = ".mp4" if ffmpeg_available() else (source_path.suffix.lower() or ".mp4")
    if ext not in VIDEO_EXTENSIONS:
        ext = ".mp4"
    return f"{entry_id:04d}_{safe_name}{ext}"


def ffmpeg_available() -> bool:
    return FFMPEG_EXE.exists() and FFMPEG_EXE.is_file()


def compress_to_ready(source_path: Path, target_path: Path) -> Path:
    command = [
        str(FFMPEG_EXE),
        "-y",
        "-i",
        str(source_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        f"scale=-2:{TARGET_VIDEO_HEIGHT}:force_original_aspect_ratio=decrease",
        "-r",
        "30",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-profile:v",
        "high",
        "-level",
        "4.1",
        "-pix_fmt",
        "yuv420p",
        "-b:v",
        TARGET_VIDEO_BITRATE,
        "-maxrate",
        TARGET_MAXRATE,
        "-bufsize",
        TARGET_BUFSIZE,
        "-c:a",
        "aac",
        "-b:a",
        TARGET_AUDIO_BITRATE,
        "-movflags",
        "+faststart",
        str(target_path),
    ]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip().splitlines()
        detail = stderr[-1] if stderr else "Unknown ffmpeg error"
        raise RuntimeError(f"ffmpeg compression failed: {detail}")
    try:
        source_path.unlink(missing_ok=True)
    except Exception:
        pass
    return target_path


def move_to_ready(
    source_path: Path,
    entry_id: int,
    display_name: str,
    video_title: str | None = None,
    use_ffmpeg: bool = False,
) -> Path:
    READY_DIR.mkdir(parents=True, exist_ok=True)
    if source_path.parent.resolve() == READY_DIR.resolve():
        return source_path
    target_name = build_ready_filename(entry_id, display_name, source_path, video_title)
    target_path = ensure_unique_path(READY_DIR / target_name)
    if use_ffmpeg and ffmpeg_available():
        return compress_to_ready(source_path, target_path)
    if source_path.resolve() != target_path.resolve():
        shutil.move(str(source_path), str(target_path))
    return target_path


def latest_video_in_downloads() -> Path | None:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    candidates = [path for path in DOWNLOADS_DIR.iterdir() if path.is_file() and is_video_file(path)]
    if not candidates:
        return None
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0]


def find_by_filename(filename: str) -> Path | None:
    raw_name = str(filename or "").strip().strip('"')
    if not raw_name:
        return None
    for directory in (READY_DIR, DOWNLOADS_DIR):
        candidate = directory / raw_name
        if candidate.exists() and candidate.is_file():
            return candidate
    return None


def report_download_ready(api_base: str, entry_id: int, target_path: Path, video_title: str | None) -> dict:
    return api_post(
        api_base,
        f"/downloads/complete/{entry_id}",
        {
            "local_filename": target_path.name,
            "local_path": str(target_path),
            "direct_media_url": None,
            "video_title": video_title,
            "download_method": "manual",
        },
    )


def download_low_res_video(url: str, entry_id: int) -> tuple[Path, str | None]:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    template = str(DOWNLOADS_DIR / f"alcove_{entry_id}_%(title).80s.%(ext)s")
    options = {
        "format": YTDLP_FORMAT,
        "outtmpl": template,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "windowsfilenames": True,
        "restrictfilenames": True,
        "nopart": True,
        "overwrites": True,
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(url, download=True)
    title = str(info.get("title") or "").strip() or None
    prefix = f"alcove_{entry_id}_"
    candidates = [
        path for path in DOWNLOADS_DIR.iterdir()
        if path.is_file() and is_video_file(path) and path.name.startswith(prefix)
    ]
    if not candidates:
        raise RuntimeError("yt-dlp did not produce a usable local video file.")
    candidates.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return candidates[0], title


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: dict):
    body = json.dumps(payload).encode("utf-8")
    origin = handler.headers.get("Origin", "")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    if origin in ALLOWED_ORIGINS:
        handler.send_header("Access-Control-Allow-Origin", origin)
        handler.send_header("Vary", "Origin")
    handler.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
    handler.send_header("Access-Control-Allow-Headers", "Content-Type")
    handler.end_headers()
    handler.wfile.write(body)


class HelperHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        return

    def do_OPTIONS(self):
        origin = self.headers.get("Origin", "")
        self.send_response(204)
        if origin in ALLOWED_ORIGINS:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self):
        if self.path == "/api/health":
            return json_response(
                self,
                200,
                {
                    "status": "ok",
                    "downloads_dir": str(DOWNLOADS_DIR),
                    "ready_dir": str(READY_DIR),
                    "playout_file": str(CURRENT_PICK_PATH),
                    "download_mode": "yt-dlp direct low-res",
                    "ffmpeg_path": str(FFMPEG_EXE),
                    "ffmpeg_available": ffmpeg_available(),
                    "compression_profile": {
                        "height": TARGET_VIDEO_HEIGHT,
                        "video_bitrate": TARGET_VIDEO_BITRATE,
                        "audio_bitrate": TARGET_AUDIO_BITRATE,
                    },
                },
            )
        return json_response(self, 404, {"status": "error", "message": "Not found"})

    def do_POST(self):
        try:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(content_length) if content_length else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return json_response(self, 400, {"status": "error", "message": "Invalid JSON payload"})

        if self.path == "/api/downloads/manual-ready-latest":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            video_title = payload.get("video_title")
            source = latest_video_in_downloads()
            if source is None:
                return json_response(self, 200, {"status": "error", "message": "No video files found in Downloads."})
            try:
                target = move_to_ready(source, entry_id, display_name, video_title, use_ffmpeg=False)
                remote = report_download_ready(api_base, entry_id, target, video_title)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {
                "status": remote.get("status", "ok"),
                "local_path": str(target),
                "local_filename": target.name,
                "remote": remote,
            })

        if self.path == "/api/downloads/manual-ready-by-filename":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            video_title = payload.get("video_title")
            source = find_by_filename(payload.get("filename"))
            if source is None:
                return json_response(self, 200, {"status": "error", "message": "That file was not found in Ready or Downloads."})
            try:
                target = move_to_ready(source, entry_id, display_name, video_title, use_ffmpeg=False)
                remote = report_download_ready(api_base, entry_id, target, video_title)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {
                "status": remote.get("status", "ok"),
                "local_path": str(target),
                "local_filename": target.name,
                "remote": remote,
            })

        if self.path == "/api/downloads/fetch-low-res":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            submitted_url = str(payload.get("submitted_url") or "").strip()
            video_title = payload.get("video_title")
            if not submitted_url:
                return json_response(self, 200, {"status": "error", "message": "No source URL was supplied."})
            try:
                source, extracted_title = download_low_res_video(submitted_url, entry_id)
                final_title = extracted_title or video_title
                target = move_to_ready(source, entry_id, display_name, final_title, use_ffmpeg=False)
                remote = report_download_ready(api_base, entry_id, target, final_title)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {
                "status": remote.get("status", "ok"),
                "local_path": str(target),
                "local_filename": target.name,
                "video_title": final_title,
                "remote": remote,
            })

        if self.path == "/api/playout/load":
            source_value = str(payload.get("local_path") or "").strip()
            if not source_value:
                return json_response(self, 200, {"status": "error", "message": "No local path was supplied."})
            source_path = Path(source_value)
            if not source_path.exists() or not source_path.is_file():
                return json_response(self, 200, {"status": "error", "message": "Local file not found on this host machine."})
            try:
                PLAYOUT_DIR.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(str(source_path), str(CURRENT_PICK_PATH))
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {"status": "ok", "current_pick_path": str(CURRENT_PICK_PATH)})

        return json_response(self, 404, {"status": "error", "message": "Not found"})


if __name__ == "__main__":
    print(f"Starting Alcove local wheel host helper on http://{HOST}:{PORT}/api/health")
    ThreadingHTTPServer((HOST, PORT), HelperHandler).serve_forever()
