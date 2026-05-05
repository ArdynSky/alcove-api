from __future__ import annotations

import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import time
import base64
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, quote, urljoin, urlencode, urlparse, urlunparse
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
COMMON_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
MOBILE_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 14; Pixel 7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Mobile Safari/537.36"
)
COOKIE_BROWSER_ATTEMPTS = ("edge", "chrome", "firefox")
CHROME_EXE_CANDIDATES = [
    Path(r"C:\Program Files\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe"),
    Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
]
CDP_PORT = 9223
ALLOWED_ORIGINS = {
    "http://127.0.0.1:5500",
    "http://localhost:5500",
    "https://thealcove.netlify.app",
    "https://ardyn-alcove.com",
    "https://www.ardyn-alcove.com",
}
CURRENT_STREAM_STATE: dict | None = None


def normalize_api_base(value: str | None) -> str:
    raw = (value or API_DEFAULT).strip()
    if not raw:
        raw = API_DEFAULT
    base = raw.rstrip("/")
    if not base.lower().endswith("/api"):
        base = f"{base}/api"
    return base


def chrome_executable() -> Path | None:
    for candidate in CHROME_EXE_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


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


def fetch_video_title(url: str | None) -> str | None:
    source = str(url or "").strip()
    if not source:
        return None
    options = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "windowsfilenames": True,
        "restrictfilenames": True,
    }
    with YoutubeDL(options) as ydl:
        info = ydl.extract_info(source, download=False)
    title = str(info.get("title") or "").strip()
    return title or None


def build_extract_options(extra: dict | None = None) -> dict:
    options = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "windowsfilenames": True,
        "restrictfilenames": True,
    }
    if extra:
        options.update(extra)
    return options


def resolve_video_title(url: str | None, fallback_title: str | None = None) -> str | None:
    try:
        extracted = fetch_video_title(url)
        if extracted:
            return extracted
    except Exception:
        pass
    fallback = str(fallback_title or "").strip()
    return fallback or None


def classify_resolution_error(message: str) -> str:
    text = str(message or "").lower()
    if "unsupported url" in text:
        return "unsupported"
    if "no video formats found" in text or "no playable direct media stream" in text:
        return "no-formats"
    if "cookie" in text:
        return "cookies-unavailable"
    return "unknown"


def select_stream_format(info: dict) -> dict | None:
    formats = info.get("formats")
    if not isinstance(formats, list):
        return None

    def score(item: dict) -> tuple[int, int, int]:
        height = int(item.get("height") or 0)
        ext = (item.get("ext") or "").lower()
        protocol = str(item.get("protocol") or "").lower()
        preference = 0 if ext == "mp4" else 1
        protocol_penalty = 0 if protocol.startswith("http") else 1
        tbr = int(item.get("tbr") or item.get("vbr") or 0)
        capped_height = min(height or 9999, 540)
        return (preference, protocol_penalty, abs(capped_height - 480), -tbr)

    playable = []
    for item in formats:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        vcodec = str(item.get("vcodec") or "").strip().lower()
        if vcodec in {"none", ""}:
            continue
        playable.append(item)
    if not playable:
        return None
    playable.sort(key=score)
    return playable[0]


def page_origin(url: str) -> str | None:
    parsed = urlparse(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/"


def normalize_source_url(url: str) -> str:
    source = str(url or "").strip()
    if not source:
        return ""
    parsed = urlparse(source)
    fragment = (parsed.fragment or "").strip()
    if fragment.startswith("/"):
        return urlunparse(parsed._replace(path=fragment, params="", query="", fragment=""))
    if fragment.startswith("vids/"):
        return urlunparse(parsed._replace(path=f"/{fragment}", params="", query="", fragment=""))
    if fragment.startswith("video/"):
        return urlunparse(parsed._replace(path=f"/{fragment}", params="", query="", fragment=""))
    return source


def build_proxy_url(target_url: str, referer: str | None = None) -> str:
    params = {"url": target_url}
    if referer:
        params["referer"] = referer
    return f"http://{HOST}:{PORT}/api/stream/proxy?{urlencode(params)}"


def build_stream_state(payload: dict) -> dict:
    media_url = str(payload.get("media_url") or "").strip()
    if not media_url:
        raise ValueError("No stream media URL was supplied.")
    submitted_url = str(payload.get("submitted_url") or payload.get("webpage_url") or "").strip()
    referer = (
        str(payload.get("referer") or "").strip()
        or page_origin(submitted_url)
        or submitted_url
        or None
    )
    media_kind = str(payload.get("media_kind") or "").strip().lower()
    if not media_kind:
        media_kind = "hls" if ".m3u8" in media_url.lower() else "file"
    playback_url = str(payload.get("playback_url") or "").strip() or build_proxy_url(media_url, referer)
    prepared_at = int(time.time() * 1000)
    stream_key = hashlib.sha1(f"{payload.get('entry_id')}-{media_url}-{prepared_at}".encode("utf-8")).hexdigest()[:12]
    return {
        "entry_id": int(payload.get("entry_id") or 0) or None,
        "entrant_name": str(payload.get("entrant_name") or "").strip() or None,
        "title": str(payload.get("title") or payload.get("video_title") or "").strip() or None,
        "media_url": media_url,
        "playback_url": playback_url,
        "media_kind": media_kind,
        "height": payload.get("height"),
        "resolve_strategy": str(payload.get("resolve_strategy") or payload.get("download_method") or "stream-ready").strip(),
        "submitted_url": submitted_url or None,
        "webpage_url": submitted_url or None,
        "referer": referer,
        "stream_key": stream_key,
        "prepared_at": prepared_at,
    }


def fetch_page_html(url: str, referer: str | None = None) -> str:
    headers = {"User-Agent": COMMON_USER_AGENT}
    if referer:
        headers["Referer"] = referer
    request = Request(url, headers=headers)
    with urlopen(request, timeout=20) as response:
        return response.read().decode("utf-8", errors="ignore")


def extract_title_from_text(text: str) -> str | None:
    title_match = re.search(r"<title>(.*?)<\/title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not title_match:
        return None
    return re.sub(r"\s+", " ", title_match.group(1)).strip() or None


def looks_like_media_candidate(url: str) -> bool:
    lowered = str(url or "").strip().lower()
    if not lowered or not re.match(r"^https?:\/\/", lowered):
        return False
    if lowered.endswith(".js") or ".js?" in lowered or "/js/" in lowered:
        return False
    media_tokens = (".m3u8", ".mp4", ".m4v", ".webm", "get_file", "playlist", "master.m3u8", "video")
    return any(token in lowered for token in media_tokens)


def build_resolved_media_payload(
    media_url: str,
    source: str,
    title: str | None,
    strategy: str,
) -> dict:
    lowered = media_url.lower()
    ext = "m3u8" if ".m3u8" in lowered else (
        "mp4" if ".mp4" in lowered else (
            "webm" if ".webm" in lowered else (
                "m4v" if ".m4v" in lowered else None
            )
        )
    )
    return {
        "media_url": media_url,
        "title": title,
        "ext": ext,
        "height": None,
        "extractor": strategy,
        "webpage_url": source,
        "submitted_url": source,
        "normalized_url": source,
        "resolve_strategy": strategy,
        "attempts": [],
    }


def extract_script_candidates(text: str) -> list[str]:
    key_patterns = [
        r'"(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)"\s*:\s*"([^"]+)"',
        r"'(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)'\s*:\s*'([^']+)'",
        r'(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)\s*[:=]\s*"([^"]+)"',
        r"(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)\s*[:=]\s*'([^']+)'",
    ]
    candidates: list[str] = []
    for pattern in key_patterns:
        candidates.extend(re.findall(pattern, text, flags=re.IGNORECASE))
    return candidates


def resolve_from_page_scan(source: str) -> dict | None:
    origin = page_origin(source)
    try:
        html = fetch_page_html(source, origin)
    except Exception:
        return None

    normalized = html.replace("\\/", "/").replace("&amp;", "&")
    patterns = [
        r"https?:\/\/[^\"' <>()]+\.m3u8[^\"' <>()]*",
        r"https?:\/\/[^\"' <>()]+\.mp4[^\"' <>()]*",
        r"https?:\/\/[^\"' <>()]+get_file[^\"' <>()]*",
    ]
    for pattern in patterns:
        matches = re.findall(pattern, normalized, flags=re.IGNORECASE)
        for candidate in matches:
            cleaned = candidate.strip()
            if cleaned and looks_like_media_candidate(cleaned):
                return build_resolved_media_payload(cleaned, source, extract_title_from_text(normalized), "page-scan")
    return None


def resolve_from_script_payload(source: str) -> dict | None:
    origin = page_origin(source)
    try:
        html = fetch_page_html(source, origin)
    except Exception:
        return None

    normalized = (
        html.replace("\\u002F", "/")
        .replace("\\/", "/")
        .replace("&amp;", "&")
        .replace("&quot;", '"')
        .replace("&#x2F;", "/")
    )

    title = extract_title_from_text(normalized)
    candidates = extract_script_candidates(normalized)
    seen: set[str] = set()
    js_payload_candidates: list[str] = []
    for candidate in candidates:
        cleaned = str(candidate or "").strip().strip('"\' ')
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        lowered = cleaned.lower()
        if lowered.endswith(".js") or ".js?" in lowered or "/js/" in lowered:
            if re.match(r"^https?:\/\/", cleaned, flags=re.IGNORECASE):
                js_payload_candidates.append(cleaned)
            continue
        if looks_like_media_candidate(cleaned):
            return build_resolved_media_payload(cleaned, source, title, "script-scan")

    for script_url in js_payload_candidates[:6]:
        try:
            script_text = fetch_page_html(script_url, page_origin(source) or source)
        except Exception:
            continue
        normalized_script = (
            script_text.replace("\\u002F", "/")
            .replace("\\/", "/")
            .replace("&amp;", "&")
            .replace("&quot;", '"')
            .replace("&#x2F;", "/")
        )
        nested_candidates = extract_script_candidates(normalized_script)
        nested_seen: set[str] = set()
        for nested in nested_candidates:
            cleaned = str(nested or "").strip().strip('"\' ')
            if not cleaned or cleaned in nested_seen:
                continue
            nested_seen.add(cleaned)
            if looks_like_media_candidate(cleaned):
                return build_resolved_media_payload(cleaned, source, title, "script-js-scan")
    return None


def rewrite_playlist_urls(content: str, playlist_url: str, referer: str | None) -> str:
    effective_referer = referer or page_origin(playlist_url) or playlist_url

    def rewrite_uri(value: str) -> str:
        absolute = urljoin(playlist_url, value)
        return build_proxy_url(absolute, effective_referer)

    def rewrite_tag_line(line: str) -> str:
        def replace_uri(match: re.Match) -> str:
            original = match.group(1)
            return f'URI="{rewrite_uri(original)}"'
        return re.sub(r'URI="([^"]+)"', replace_uri, line)

    lines: list[str] = []
    for raw_line in content.splitlines():
      line = raw_line.strip()
      if not line:
          lines.append(raw_line)
          continue
      if line.startswith("#"):
          lines.append(rewrite_tag_line(raw_line))
          continue
      lines.append(rewrite_uri(line))
    return "\n".join(lines)


def fetch_remote_response(target_url: str, referer: str | None, range_header: str | None = None):
    headers = {"User-Agent": COMMON_USER_AGENT}
    if referer:
        headers["Referer"] = referer
    if range_header:
        headers["Range"] = range_header
    request = Request(target_url, headers=headers)
    try:
        return urlopen(request, timeout=30)
    except HTTPError as error:
        return error


def inspect_remote_stream(target_url: str, referer: str | None) -> dict:
    upstream = fetch_remote_response(target_url, referer, "bytes=0-0")
    status_code = getattr(upstream, "status", None) or getattr(upstream, "code", None) or 200
    content_type = upstream.headers.get("Content-Type", "application/octet-stream")
    final_url = getattr(upstream, "url", target_url)
    return {
        "status_code": int(status_code),
        "content_type": content_type,
        "final_url": final_url,
        "accept_ranges": upstream.headers.get("Accept-Ranges"),
        "content_range": upstream.headers.get("Content-Range"),
    }


def download_captured_media(media_url: str, referer: str | None, entry_id: int, title: str | None, ext_hint: str | None = None) -> Path:
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    inspected = inspect_remote_stream(media_url, referer)
    content_type = str(inspected.get("content_type") or "")
    final_url = str(inspected.get("final_url") or media_url)

    ext = (ext_hint or "").strip().lower()
    if not ext:
        lowered = final_url.lower()
        if ".mp4" in lowered:
            ext = "mp4"
        elif ".webm" in lowered:
            ext = "webm"
        elif ".m4v" in lowered:
            ext = "m4v"
    if not ext and content_type.startswith("video/"):
        ext = content_type.split("/", 1)[1].split(";", 1)[0].strip().lower()
    if ext == "mpegurl" or "mpegurl" in content_type.lower() or ".m3u8" in final_url.lower():
        raise RuntimeError("Browser capture found an HLS stream. Direct low-res file download is not available for this source yet.")
    if ext not in {"mp4", "webm", "m4v", "mov"}:
        ext = "mp4"

    safe_title = clean_video_title(title) if title else f"capture_{entry_id}"
    target_path = ensure_unique_path(DOWNLOADS_DIR / f"alcove_capture_{entry_id}_{safe_title}.{ext}")
    upstream = fetch_remote_response(media_url, referer, None)
    if (getattr(upstream, "status", None) or getattr(upstream, "code", None) or 200) >= 400:
        raise RuntimeError(f"Captured media download failed with HTTP {(getattr(upstream, 'status', None) or getattr(upstream, 'code', None) or 500)}.")
    with open(target_path, "wb") as handle:
        while True:
            chunk = upstream.read(1024 * 128)
            if not chunk:
                break
            handle.write(chunk)
    return target_path


def websocket_handshake(sock: socket.socket, host: str, path: str) -> None:
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    headers = (
        f"GET {path} HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    )
    sock.sendall(headers.encode("ascii"))
    response = b""
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            break
        response += chunk
    if b"101" not in response.split(b"\r\n", 1)[0]:
        raise RuntimeError("Chrome DevTools websocket handshake failed.")


def websocket_send_text(sock: socket.socket, text: str) -> None:
    payload = text.encode("utf-8")
    first = 0x81
    mask_bit = 0x80
    length = len(payload)
    if length < 126:
        header = bytes([first, mask_bit | length])
    elif length < 65536:
        header = bytes([first, mask_bit | 126]) + length.to_bytes(2, "big")
    else:
        header = bytes([first, mask_bit | 127]) + length.to_bytes(8, "big")
    mask = os.urandom(4)
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    sock.sendall(header + mask + masked)


def websocket_recv_text(sock: socket.socket, timeout: float = 1.0) -> str | None:
    sock.settimeout(timeout)
    try:
        header = sock.recv(2)
        if not header:
            return None
        first, second = header[0], header[1]
        opcode = first & 0x0F
        masked = bool(second & 0x80)
        length = second & 0x7F
        if length == 126:
            length = int.from_bytes(sock.recv(2), "big")
        elif length == 127:
            length = int.from_bytes(sock.recv(8), "big")
        mask = sock.recv(4) if masked else b""
        payload = b""
        while len(payload) < length:
            payload += sock.recv(length - len(payload))
        if masked and mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        if opcode == 0x8:
            return None
        if opcode != 0x1:
            return None
        return payload.decode("utf-8", errors="ignore")
    except socket.timeout:
        return None


def chrome_devtools_json(path: str, method: str = "GET") -> dict | list:
    request = Request(f"http://127.0.0.1:{CDP_PORT}{path}", method=method)
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_for_devtools() -> None:
    start = time.time()
    last_error = None
    while time.time() - start < 15:
        try:
            chrome_devtools_json("/json/version")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.35)
    raise RuntimeError(f"Chrome DevTools endpoint did not start: {last_error}")


def start_capture_browser() -> subprocess.Popen:
    browser = chrome_executable()
    if not browser:
        raise RuntimeError("No Chrome or Edge executable was found on this machine.")
    profile_dir = Path(tempfile.gettempdir()) / "alcove-stream-capture-profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    command = [
        str(browser),
        f"--remote-debugging-port={CDP_PORT}",
        f"--user-data-dir={profile_dir}",
        "--headless=new",
        "--autoplay-policy=no-user-gesture-required",
        "--disable-gpu",
        "--mute-audio",
        "--no-first-run",
        "--no-default-browser-check",
        "about:blank",
    ]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_for_devtools()
    return process


def browser_capture_media(source: str) -> dict:
    process = start_capture_browser()
    try:
        new_target = chrome_devtools_json(f"/json/new?{quote(source, safe=':/?&=%#')}", method="PUT")
        ws_url = new_target.get("webSocketDebuggerUrl")
        if not ws_url:
            raise RuntimeError("Chrome did not provide a DevTools websocket URL.")
        parsed_ws = urlparse(ws_url)
        sock = socket.create_connection((parsed_ws.hostname, parsed_ws.port), timeout=15)
        if parsed_ws.scheme == "wss":
            context = ssl.create_default_context()
            sock = context.wrap_socket(sock, server_hostname=parsed_ws.hostname)
        websocket_handshake(sock, parsed_ws.netloc, parsed_ws.path)

        message_id = 0
        def send(method: str, params: dict | None = None):
            nonlocal message_id
            message_id += 1
            payload = {"id": message_id, "method": method}
            if params:
                payload["params"] = params
            websocket_send_text(sock, json.dumps(payload))

        send("Network.enable", {})
        send("Page.enable", {})
        send("Runtime.enable", {})
        send("Page.navigate", {"url": source})

        deadline = time.time() + 18
        candidates: list[dict] = []
        seen_urls: set[str] = set()
        click_sent = False
        while time.time() < deadline:
            message = websocket_recv_text(sock, timeout=0.75)
            if not message:
                if not click_sent and time.time() > deadline - 10:
                    click_sent = True
                    send("Runtime.evaluate", {
                        "expression": """
(() => {
  const selectors = ['button', '[role="button"]', '.play', '.play-button', '.xplayer-play'];
  for (const selector of selectors) {
    const node = document.querySelector(selector);
    if (node) { try { node.click(); return 'clicked'; } catch (e) {} }
  }
  const video = document.querySelector('video');
  if (video) { try { video.play(); return 'video-play'; } catch (e) { return String(e); } }
  return 'no-click-target';
})()
                        """.strip()
                    })
                continue
            try:
                payload = json.loads(message)
            except json.JSONDecodeError:
                continue
            if payload.get("method") == "Network.responseReceived":
                response = payload.get("params", {}).get("response", {})
                url = str(response.get("url") or "").strip()
                mime = str(response.get("mimeType") or "").strip()
                if not url or url in seen_urls:
                    continue
                lowered = url.lower()
                if (
                    ".m3u8" in lowered
                    or ".mp4" in lowered
                    or mime.startswith("video/")
                    or "mpegurl" in mime.lower()
                ):
                    seen_urls.add(url)
                    candidates.append({
                        "url": url,
                        "mime": mime,
                        "status": response.get("status"),
                    })
        sock.close()
        if not candidates:
            raise RuntimeError("Browser capture did not observe any playable media requests.")
        preferred = sorted(
            candidates,
            key=lambda item: (
                0 if ".m3u8" in item["url"].lower() or "mpegurl" in item["mime"].lower() else 1,
                0 if ".mp4" in item["url"].lower() or item["mime"].startswith("video/") else 1,
            ),
        )[0]
        media_url = preferred["url"]
        title = None
        try:
            page_html = fetch_page_html(source, page_origin(source))
            title = extract_title_from_text(page_html)
        except Exception:
            title = None
        return {
            "media_url": media_url,
            "playback_url": build_proxy_url(media_url, page_origin(source) or source),
            "title": title,
            "ext": "m3u8" if ".m3u8" in media_url.lower() else ("mp4" if ".mp4" in media_url.lower() else None),
            "height": None,
            "extractor": "browser-capture",
            "webpage_url": source,
            "submitted_url": source,
            "normalized_url": source,
            "media_kind": "hls" if ".m3u8" in media_url.lower() or "mpegurl" in (preferred.get("mime") or "").lower() else "file",
            "resolve_strategy": "browser-capture",
            "attempts": [{"name": "browser-capture", "status": "ok"}],
        }
    finally:
        try:
            process.terminate()
        except Exception:
            pass


def extract_info(source: str, extra_options: dict | None = None) -> dict:
    with YoutubeDL(build_extract_options(extra_options)) as ydl:
        info = ydl.extract_info(source, download=False)
    if not isinstance(info, dict):
        raise RuntimeError("Could not extract media details from that page.")
    return info


def resolve_direct_media(url: str) -> dict:
    source = normalize_source_url(url)
    if not source:
        raise RuntimeError("No source URL was supplied.")

    origin = page_origin(source)
    base_headers = {"User-Agent": COMMON_USER_AGENT}
    if origin:
        base_headers["Referer"] = origin

    attempts: list[dict] = [
        {"name": "direct", "options": {}},
        {"name": "browser-headers", "options": {"http_headers": base_headers}},
        {
            "name": "mobile-headers",
            "options": {
                "http_headers": {
                    **({"Referer": origin} if origin else {}),
                    "User-Agent": MOBILE_USER_AGENT,
                }
            },
        },
    ]
    for browser in COOKIE_BROWSER_ATTEMPTS:
        attempts.append({
            "name": f"cookies-{browser}",
            "options": {
                "http_headers": base_headers,
                "cookiesfrombrowser": (browser,),
            },
        })

    diagnostics: list[dict] = []
    last_error = "No direct media stream could be resolved from that page."
    for attempt in attempts:
        try:
            info = extract_info(source, attempt["options"])
            chosen = select_stream_format(info)
            direct_url = str((chosen or {}).get("url") or info.get("url") or "").strip()
            if not direct_url:
                raise RuntimeError("No playable direct media stream was exposed.")

            title = str(info.get("title") or "").strip() or None
            ext = str((chosen or {}).get("ext") or info.get("ext") or "").strip() or None
            height = int((chosen or {}).get("height") or info.get("height") or 0) or None
            extractor = str(info.get("extractor") or info.get("extractor_key") or "").strip() or None
            webpage_url = str(info.get("webpage_url") or source).strip()
            media_kind = "hls" if (ext or "").lower() == "m3u8" else "file"
            diagnostics.append({"name": attempt["name"], "status": "ok"})
            return {
                "media_url": direct_url,
                "playback_url": build_proxy_url(direct_url, origin or page_origin(webpage_url) or webpage_url),
                "title": title,
                "ext": ext,
                "height": height,
                "extractor": extractor,
                "webpage_url": webpage_url,
                "submitted_url": str(url or "").strip(),
                "normalized_url": source,
                "media_kind": media_kind,
                "resolve_strategy": attempt["name"],
                "attempts": diagnostics,
            }
        except Exception as exc:
            last_error = str(exc) or last_error
            diagnostics.append({
                "name": attempt["name"],
                "status": "error",
                "message": last_error,
                "kind": classify_resolution_error(last_error),
            })

    page_scan = resolve_from_page_scan(source)
    if page_scan:
        page_scan["playback_url"] = build_proxy_url(page_scan["media_url"], origin or page_origin(page_scan["webpage_url"]) or page_scan["webpage_url"])
        page_scan["media_kind"] = "hls" if (page_scan.get("ext") or "").lower() == "m3u8" else "file"
        page_scan["attempts"] = diagnostics + [{"name": "page-scan", "status": "ok"}]
        return page_scan

    script_scan = resolve_from_script_payload(source)
    if script_scan:
        script_scan["playback_url"] = build_proxy_url(script_scan["media_url"], origin or page_origin(script_scan["webpage_url"]) or script_scan["webpage_url"])
        script_scan["media_kind"] = "hls" if (script_scan.get("ext") or "").lower() == "m3u8" else "file"
        script_scan["attempts"] = diagnostics + [{"name": "script-scan", "status": "ok"}]
        return script_scan

    raise RuntimeError(json.dumps({
        "message": last_error,
        "attempts": diagnostics,
        "failure_class": classify_resolution_error(last_error),
    }))


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
        global CURRENT_STREAM_STATE
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
                    "stream_resolve_available": True,
                    "stream_resolve_cookie_attempts": list(COOKIE_BROWSER_ATTEMPTS),
                },
            )
        if self.path == "/api/stream/current":
            return json_response(
                self,
                200,
                {
                    "status": "ok",
                    "active": bool(CURRENT_STREAM_STATE),
                    "stream": CURRENT_STREAM_STATE,
                },
            )
        if self.path.startswith("/api/stream/proxy"):
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            target_url = str((params.get("url") or [""])[0] or "").strip()
            referer = str((params.get("referer") or [""])[0] or "").strip() or None
            if not target_url:
                return json_response(self, 400, {"status": "error", "message": "No target URL supplied."})

            upstream = fetch_remote_response(target_url, referer, self.headers.get("Range"))
            status_code = getattr(upstream, "status", None) or getattr(upstream, "code", None) or 200
            content_type = upstream.headers.get("Content-Type", "application/octet-stream")
            is_playlist = ".m3u8" in target_url.lower() or "mpegurl" in content_type.lower()
            origin = self.headers.get("Origin", "")

            if is_playlist:
                raw_body = upstream.read().decode("utf-8", errors="ignore")
                rewritten = rewrite_playlist_urls(raw_body, target_url, referer)
                body = rewritten.encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                if origin in ALLOWED_ORIGINS:
                    self.send_header("Access-Control-Allow-Origin", origin)
                    self.send_header("Vary", "Origin")
                self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "Content-Type, Range")
                self.end_headers()
                self.wfile.write(body)
                return

            self.send_response(status_code)
            for header in ("Content-Type", "Content-Length", "Accept-Ranges", "Content-Range", "Cache-Control"):
                value = upstream.headers.get(header)
                if value:
                    self.send_header(header, value)
            if origin in ALLOWED_ORIGINS:
                self.send_header("Access-Control-Allow-Origin", origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type, Range")
            self.end_headers()
            while True:
                chunk = upstream.read(1024 * 64)
                if not chunk:
                    break
                self.wfile.write(chunk)
            return
        return json_response(self, 404, {"status": "error", "message": "Not found"})

    def do_POST(self):
        global CURRENT_STREAM_STATE
        try:
            content_length = int(self.headers.get("Content-Length", "0") or "0")
            raw = self.rfile.read(content_length) if content_length else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except Exception:
            return json_response(self, 400, {"status": "error", "message": "Invalid JSON payload"})

        if self.path == "/api/stream/current":
            if payload.get("clear"):
                CURRENT_STREAM_STATE = None
                return json_response(self, 200, {"status": "ok", "active": False, "stream": None})
            try:
                CURRENT_STREAM_STATE = build_stream_state(payload)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {"status": "ok", "active": True, "stream": CURRENT_STREAM_STATE})

        if self.path == "/api/downloads/manual-ready-latest":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            video_title = payload.get("video_title")
            submitted_url = payload.get("submitted_url")
            source = latest_video_in_downloads()
            if source is None:
                return json_response(self, 200, {"status": "error", "message": "No video files found in Downloads."})
            try:
                final_title = resolve_video_title(submitted_url, video_title)
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

        if self.path == "/api/downloads/manual-ready-by-filename":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            video_title = payload.get("video_title")
            submitted_url = payload.get("submitted_url")
            source = find_by_filename(payload.get("filename"))
            if source is None:
                return json_response(self, 200, {"status": "error", "message": "That file was not found in Ready or Downloads."})
            try:
                final_title = resolve_video_title(submitted_url, video_title)
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

        if self.path == "/api/downloads/fetch-low-res":
            entry_id = int(payload.get("entry_id") or 0)
            display_name = str(payload.get("display_name") or "").strip() or "Unknown"
            api_base = payload.get("api_base") or API_DEFAULT
            submitted_url = str(payload.get("submitted_url") or "").strip()
            video_title = payload.get("video_title")
            if not submitted_url:
                return json_response(self, 200, {"status": "error", "message": "No source URL was supplied."})
            try:
                download_method = "yt-dlp"
                try:
                    source, extracted_title = download_low_res_video(submitted_url, entry_id)
                    final_title = extracted_title or video_title
                except Exception:
                    captured = browser_capture_media(submitted_url)
                    referer = page_origin(submitted_url) or submitted_url
                    source = download_captured_media(
                        captured["media_url"],
                        referer,
                        entry_id,
                        captured.get("title") or video_title,
                        captured.get("ext"),
                    )
                    final_title = captured.get("title") or video_title
                    download_method = "browser-capture-fallback"
                target = move_to_ready(source, entry_id, display_name, final_title, use_ffmpeg=False)
                remote = report_download_ready(api_base, entry_id, target, final_title)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {
                "status": remote.get("status", "ok"),
                "local_path": str(target),
                "local_filename": target.name,
                "video_title": final_title,
                "download_method": download_method,
                "remote": remote,
            })

        if self.path == "/api/stream/resolve":
            submitted_url = str(payload.get("submitted_url") or "").strip()
            if not submitted_url:
                return json_response(self, 200, {"status": "error", "message": "No source URL was supplied."})
            try:
                resolved = resolve_direct_media(submitted_url)
            except Exception as exc:
                message = str(exc)
                extra = {}
                if message.startswith("{"):
                    try:
                        extra = json.loads(message)
                        message = str(extra.get("message") or message)
                    except Exception:
                        extra = {}
                return json_response(
                    self,
                    200,
                    {
                        "status": "error",
                        "message": message,
                        **extra,
                        "fallback": "download",
                        "stream_support": "fallback-needed",
                    },
                )
            resolved.setdefault("stream_support", "supported")
            return json_response(self, 200, {"status": "ok", **resolved})

        if self.path == "/api/stream/inspect":
            target_url = str(payload.get("target_url") or payload.get("media_url") or "").strip()
            referer = str(payload.get("referer") or "").strip() or None
            if not target_url:
                return json_response(self, 200, {"status": "error", "message": "No target URL was supplied."})
            try:
                inspected = inspect_remote_stream(target_url, referer)
            except Exception as exc:
                return json_response(self, 200, {"status": "error", "message": str(exc)})
            return json_response(self, 200, {"status": "ok", **inspected})

        if self.path == "/api/stream/browser-capture":
            submitted_url = str(payload.get("submitted_url") or "").strip()
            if not submitted_url:
                return json_response(self, 200, {"status": "error", "message": "No source URL was supplied."})
            try:
                captured = browser_capture_media(submitted_url)
                captured.setdefault("stream_support", "supported")
            except Exception as exc:
                return json_response(
                    self,
                    200,
                    {
                        "status": "error",
                        "message": str(exc),
                        "fallback": "download",
                        "stream_support": "fallback-needed",
                    },
                )
            return json_response(self, 200, {"status": "ok", **captured})

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
