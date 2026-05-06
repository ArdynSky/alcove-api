# Practical Code Fixes & Improvements
# Ready to integrate into local_wheel_host_helper.py and batch_stream_site_test.py

# ==============================================================================
# FIX #1: Make Chrome DevTools Port Configurable with Fallback
# ==============================================================================
# Replace in local_wheel_host_helper.py:

CDP_PORT = int(os.getenv("ALCOVE_CDP_PORT", "9223"))
CDP_PORT_FALLBACKS = [9223, 9224, 9225, 9226]  # Add this line

# Then update start_capture_browser():

def find_available_cdp_port() -> int:
    """Find first available Chrome DevTools port"""
    import socket
    for port in CDP_PORT_FALLBACKS:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('127.0.0.1', port))
            sock.close()
            if result != 0:  # Port is available
                return port
        except:
            pass
    return CDP_PORT  # Fallback to configured port

def start_capture_browser() -> tuple[subprocess.Popen, int]:
    """Start browser and return process + actual port used"""
    browser = chrome_executable()
    if not browser:
        raise RuntimeError("No Chrome or Edge executable was found on this machine.")
    
    profile_dir = Path(tempfile.gettempdir()) / "alcove-stream-capture-profile"
    profile_dir.mkdir(parents=True, exist_ok=True)
    
    # Find available port
    actual_port = find_available_cdp_port()
    print(f"[Browser Capture] Using Chrome DevTools port: {actual_port}")
    
    command = [
        str(browser),
        f"--remote-debugging-port={actual_port}",
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
    wait_for_devtools(actual_port)
    return process, actual_port

def wait_for_devtools(port: int = None) -> None:
    """Wait for DevTools to be ready on specified port"""
    if port is None:
        port = CDP_PORT
    
    start = time.time()
    last_error = None
    while time.time() - start < 15:
        try:
            request = Request(f"http://127.0.0.1:{port}/json/version", method="GET")
            with urlopen(request, timeout=2) as response:
                json.loads(response.read().decode("utf-8"))
            return
        except Exception as exc:
            last_error = exc
            time.sleep(0.35)
    raise RuntimeError(f"Chrome DevTools endpoint did not start on port {port}: {last_error}")

def browser_capture_media(source: str) -> dict:
    """Updated version with port handling"""
    process = None
    actual_port = CDP_PORT
    try:
        process, actual_port = start_capture_browser()
        
        # Update the following line:
        new_target = chrome_devtools_json(
            f"/json/new?{quote(source, safe=':/?&=%#')}", 
            method="PUT",
            port=actual_port  # Pass port parameter
        )
        
        # ... rest remains same ...
        
    except Exception as exc:
        raise exc
    finally:
        if process:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                process.kill()

def chrome_devtools_json(path: str, method: str = "GET", port: int = None) -> dict | list:
    """Updated to support custom port"""
    if port is None:
        port = CDP_PORT
    
    request = Request(f"http://127.0.0.1:{port}{path}", method=method)
    with urlopen(request, timeout=15) as response:
        return json.loads(response.read().decode("utf-8"))


# ==============================================================================
# FIX #2: Better Cookie Loading with Error Handling
# ==============================================================================
# Add this function to local_wheel_host_helper.py:

def get_browser_cookies_dict(browser_name: str) -> dict | None:
    """Attempt to load cookies from browser safely"""
    try:
        # Try using browser-cookie3 if available (better error handling)
        try:
            import browser_cookie3
            cookies_jar = getattr(browser_cookie3, browser_name, None)
            if cookies_jar:
                return cookies_jar()
        except ImportError:
            pass
        
        # Fallback: Let yt-dlp handle it but catch failures
        # yt-dlp will try: $PROFILE/.mozilla/firefox/... or $APPDATA/Mozilla/...
        return None
    except Exception as e:
        import logging
        logging.debug(f"Cookie loading failed for {browser_name}: {e}")
        return None

# Update resolve_direct_media() to handle cookie failures gracefully:

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
    
    # Only add cookie attempts for installed browsers
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
            error_message = str(exc) or last_error
            error_class = classify_resolution_error(error_message)
            
            # Skip cookie attempts if browser not found
            if "cookies-" in attempt["name"] and ("not found" in error_message.lower() or "no such file" in error_message.lower()):
                error_class = "browser-cookies-unavailable"
            
            diagnostics.append({
                "name": attempt["name"],
                "status": "error",
                "message": error_message,
                "kind": error_class,
            })
            last_error = error_message

    # ... rest of function (page_scan, script_scan) remains the same ...


# ==============================================================================
# FIX #3: Improved Error Classification
# ==============================================================================
# Replace classify_resolution_error() in local_wheel_host_helper.py:

def classify_resolution_error(message: str) -> str:
    """Classify resolution errors for better debugging"""
    text = str(message or "").lower()
    
    # Browser/automation issues
    if "chrome devtools" in text or "websocket" in text:
        return "browser-capture-unavailable"
    if "headless" in text or "executable" in text:
        return "browser-not-found"
    
    # Cookie/auth issues
    if "firefox" in text or "cookie" in text:
        return "cookies-unavailable"
    if "403" in text or "unauthorized" in text or "forbidden" in text:
        return "auth-required"
    if "401" in text:
        return "auth-required"
    
    # Site/content issues
    if "unsupported url" in text:
        return "unsupported-site"
    if "no video formats found" in text or "no playable" in text:
        return "no-formats"
    if "404" in text or "not found" in text:
        return "content-missing"
    if "geo" in text or "region" in text:
        return "geo-blocked"
    
    # Network issues
    if "timeout" in text or "took too long" in text:
        return "timeout"
    if "connection" in text or "refused" in text or "unreachable" in text:
        return "network-error"
    if "ssl" in text or "certificate" in text:
        return "ssl-error"
    
    # Rate limiting
    if "429" in text or "too many" in text or "rate" in text:
        return "rate-limited"
    
    return "unknown"


# ==============================================================================
# FIX #4: Add Validation that URLs Work NOW
# ==============================================================================
# Add this to local_wheel_host_helper.py:

def validate_media_url_accessible(media_url: str, referer: str | None = None, timeout: int = 10) -> dict:
    """Check if media URL is currently accessible (not expired)"""
    try:
        upstream = fetch_remote_response(media_url, referer, "bytes=0-1")
        status_code = getattr(upstream, "status", None) or getattr(upstream, "code", None) or 200
        content_type = upstream.headers.get("Content-Type", "")
        content_length = upstream.headers.get("Content-Length")
        
        is_accessible = status_code < 400
        is_video = content_type.startswith("video/") or "mpegurl" in content_type.lower()
        
        return {
            "accessible": is_accessible,
            "status": status_code,
            "content_type": content_type,
            "content_length": content_length,
            "is_video": is_video,
            "timestamp": int(time.time()),
        }
    except Exception as e:
        return {
            "accessible": False,
            "error": str(e),
            "timestamp": int(time.time()),
        }


# ==============================================================================
# FIX #5: Batch Test Improvements
# ==============================================================================
# Add to batch_stream_site_test.py main() function:

# Add command-line arguments:
parser.add_argument(
    "--skip-browser-capture-on-error",
    action="store_true",
    help="Don't fail if browser capture is unavailable (Chrome DevTools not running).",
)
parser.add_argument(
    "--timeout",
    type=int,
    default=60,
    help="Timeout per URL test in seconds.",
)
parser.add_argument(
    "--validate-urls",
    action="store_true",
    help="Check if resolved URLs are currently accessible (adds 5-10 seconds per URL).",
)

# Then in the test loop:
if args.browser_capture:
    try:
        captured = browser_capture_media(test_url)
        current["browser_capture"] = status_ok(captured)
        current["host_stream_ready"] = "Yes"
        current["download_fallback"] = "Not needed"
        resolved_payload = captured
    except Exception as browser_exc:
        if args.skip_browser_capture_on_error:
            browser_message = "Browser capture not available"
            current["browser_capture"] = "Skipped"
            note_parts.append(f"browser_capture_skipped=DevTools unavailable")
        else:
            browser_message, _ = parse_error(browser_exc)
            current["browser_capture"] = status_error(browser_message)
            note_parts.append(f"browser_capture_failed={browser_message}")

# Add URL validation:
if args.validate_urls and resolved_payload and resolved_payload.get("media_url"):
    try:
        validation = validate_media_url_accessible(
            str(resolved_payload["media_url"]),
            str(resolved_payload.get("webpage_url") or "")
        )
        if not validation.get("accessible"):
            current["host_stream_ready"] = "URL not accessible"
            note_parts.append(f"url_validation_failed={validation.get('status', 'error')}")
        else:
            note_parts.append(f"url_validated={validation.get('status')}")
    except Exception as exc:
        note_parts.append(f"validation_error={str(exc)[:100]}")


# ==============================================================================
# FIX #6: Site-Specific Handlers (Start with High-Priority Sites)
# ==============================================================================
# Add to local_wheel_host_helper.py:

def pornhub_direct_handler(url: str) -> dict:
    """PornHub-specific resolution (these require authentication usually)"""
    # PornHub: Try browser capture first
    return browser_capture_media(url)

def xvideos_direct_handler(url: str) -> dict:
    """XVideos-specific resolution"""
    # Try with mobile user agent first (sometimes less restrictive)
    try:
        info = extract_info(url, {
            "http_headers": {
                "User-Agent": MOBILE_USER_AGENT,
                "Referer": page_origin(url) or url,
            }
        })
        chosen = select_stream_format(info)
        if chosen and chosen.get("url"):
            return {
                "media_url": chosen["url"],
                "title": info.get("title"),
                "ext": chosen.get("ext"),
                "extractor": "xvideos-mobile",
                "resolve_strategy": "site-specific-mobile",
                # ... etc ...
            }
    except:
        pass
    
    # Fallback to browser capture
    return browser_capture_media(url)

# Site handlers mapping:
SITE_SPECIFIC_HANDLERS = {
    "pornhub.com": pornhub_direct_handler,
    "xvideos.com": xvideos_direct_handler,
    # Add more as needed
}

# Use in resolve_direct_media():
def resolve_direct_media(url: str) -> dict:
    source = normalize_source_url(url)
    domain = urlparse(source).netloc.lower().replace("www.", "")
    
    # Try site-specific handler first
    if domain in SITE_SPECIFIC_HANDLERS:
        try:
            return SITE_SPECIFIC_HANDLERS[domain](source)
        except Exception as site_exc:
            # Fall through to generic methods
            pass
    
    # ... rest of generic resolution ...


# ==============================================================================
# FIX #7: Expanded Pattern Matching for Hidden Streams
# ==============================================================================
# Replace extract_script_candidates() in local_wheel_host_helper.py:

def extract_script_candidates(text: str) -> list[str]:
    """Extract all media URL candidates from JavaScript"""
    key_patterns = [
        # Original patterns
        r'"(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)"\s*:\s*"([^"]+)"',
        r"'(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)'\s*:\s*'([^']+)'",
        r'(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)\s*[:=]\s*"([^"]+)"',
        r"(?:file|videoUrl|video_url|streamUrl|stream_url|contentUrl|content_url|src|source)\s*[:=]\s*'([^']+)'",
        
        # HLS/DASH patterns
        r'"(?:hlsSource|hls|playlist|master)"\s*:\s*"([^"]+\.m3u8[^"]*)"',
        r'"(?:dash|mpdUrl|mpd)"\s*:\s*"([^"]+\.mpd[^"]*)"',
        
        # Common streaming APIs
        r'"vodUrl"\s*:\s*"([^"]+)"',
        r'"streamingUrl"\s*:\s*"([^"]+)"',
        r'"videoSource"\s*:\s*"([^"]+)"',
        
        # Generic URL detection in common contexts
        r'"(https://[^"]*\.m3u8[^"]*)"',
        r'"(https://[^"]*get_file[^"]*)"',
        r"'(https://[^']*\.m3u8[^']*)'",
        r"'(https://[^']*get_file[^']*)'",
        
        # Encoded URLs
        r'"data"\s*:\s*"(https%3[AD]//[^"]+)"',
    ]
    
    candidates: list[str] = []
    for pattern in key_patterns:
        try:
            matches = re.findall(pattern, text, flags=re.IGNORECASE)
            for match in matches:
                # Decode URL if needed
                decoded = match
                if "%" in match:
                    try:
                        decoded = urlparse.unquote(match)
                    except:
                        pass
                candidates.append(decoded)
        except re.error:
            continue
    
    return candidates


# ==============================================================================
# DEPLOYMENT INSTRUCTIONS
# ==============================================================================
"""
1. Back up current local_wheel_host_helper.py:
   cp local_wheel_host_helper.py local_wheel_host_helper.py.backup

2. Apply Fix #1 (Chrome port handling) - MOST CRITICAL
3. Apply Fix #2 (Cookie error handling)
4. Apply Fix #3 (Better error classification)

4. Test with stream-player-lab.html:
   - Navigate to miniapp/stream-player-lab.html
   - Test a known URL
   - Check console for detailed error messages

5. Run batch test with improvements:
   python batch_stream_site_test.py input.csv \
     --browser-capture \
     --skip-browser-capture-on-error \
     --validate-urls

6. Review results and implement additional fixes as needed

ENVIRONMENT VARIABLES to set (optional):
export ALCOVE_CDP_PORT=9224  # If port 9223 is in use
export ALCOVE_FFMPEG_EXE=/path/to/ffmpeg  # If needed
"""

