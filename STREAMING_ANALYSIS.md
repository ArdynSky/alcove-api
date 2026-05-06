# Streaming Site Compatibility Analysis & Improvements

## Current State Summary

**What's Working**: The stream-player-lab.html successfully resolves and streams video from several sites via the local HTTP helper server.

**What Failed in Batch Tests**: 
- **Firefox Cookies Unavailable** - The resolver tried to use Firefox's saved cookies but the cookie database wasn't found
- **Chrome DevTools Refused Connection** - Browser capture method failed because Chrome Remote DevTools couldn't start on port 9223

**Why This Matters**: The batch tester is using direct function calls (`resolve_direct_media()`, `browser_capture_media()`) while stream-player-lab.html uses HTTP API calls. These are the same functions, but batch tests have different environment/configuration constraints.

---

## Architecture Overview

### Resolution Strategy Hierarchy (resolve_direct_media)
The system tries these methods in order, stopping on first success:

1. **Direct (no options)** - Try yt-dlp without special headers
2. **Browser Headers** - Add User-Agent + Referer headers
3. **Mobile Headers** - Use mobile user agent (some sites redirect differently)
4. **Cookies-Firefox** - Attempt to use Firefox's saved login cookies
5. **Cookies-Chrome** - Attempt to use Chrome's saved login cookies  
6. **Cookies-Edge** - Attempt to use Edge's saved login cookies
7. **Page Scan** - Parse HTML for obvious media URLs (m3u8, mp4, get_file patterns)
8. **Script Scan** - Extract URLs from JavaScript (including nested JS files)

### Fallback When Direct Fails (browser_capture_media)
- Launches headless Chrome with remote debugging enabled
- Opens target URL and monitors ALL network requests
- Looks for video/mp4, application/x-mpegURL, etc.
- Attempts to click play buttons or trigger video.play()
- Returns first detected media URL

---

## Issues & Proposed Solutions

### Issue #1: Chrome DevTools Port Conflict (Port 9223)
**Problem**: `browser_capture_media()` can't connect to Chrome DevTools endpoint
**Root Cause**: Either:
- Chrome isn't starting with `--remote-debugging-port=9223`
- Another process is using port 9223
- Chrome process terminates before DevTools is ready

**Solutions** (in order of priority):

#### Quick Fix: Skip browser capture in batch tests
```python
# In batch_stream_site_test.py, make --browser-capture conditional
if args.browser_capture and chrome_executable():
    try:
        captured = browser_capture_media(test_url)
        # ... existing code
    except RuntimeError as exc:
        if "did not start" in str(exc):
            logger.warning(f"Skipping browser capture: Chrome DevTools unavailable")
            current["browser_capture"] = "Not available"
        else:
            raise
```

#### Better Fix: Use a fallback port
Modify `local_wheel_host_helper.py` to allow configurable CDP port:
```python
CDP_PORT = int(os.getenv("ALCOVE_CDP_PORT", "9223"))

def wait_for_devtools() -> None:
    # Add port recovery logic
    for port_attempt in [CDP_PORT, 9224, 9225]:
        # Try each port...
```

#### Best Fix: Use Chrome in Docker/Isolated Container
If Chrome keeps crashing on Windows, consider using a pre-built Docker image with headless Chrome and better process isolation.

---

### Issue #2: Firefox Cookies Database Not Found
**Problem**: `cookies-firefox` attempt fails because `cookiesfrombrowser=("firefox",)` can't locate Firefox profile

**Root Causes**:
- Firefox portable or non-standard installation
- Wrong profile path for Windows user
- Firefox not installed at all

**Solutions**:

#### Fix: Implement cookie harvesting from multiple browsers
```python
def get_browser_cookies_safe(browser_name: str) -> dict | None:
    """Safely attempt to load browser cookies with fallbacks"""
    try:
        # Use selenium or browser-cookie3 library instead of yt-dlp's cookiesfrombrowser
        # Browser-cookie3 handles edge cases better
        from browser_cookie3 import get_cookie_string
        return get_cookie_string(browser_name)
    except Exception as e:
        logger.debug(f"Could not load cookies from {browser_name}: {e}")
        return None

# In resolve_direct_media:
for browser in COOKIE_BROWSER_ATTEMPTS:
    cookies = get_browser_cookies_safe(browser)
    if cookies:
        attempts.append({
            "name": f"cookies-{browser}",
            "options": {"http_headers": {**base_headers, "Cookie": cookies}},
        })
```

#### Alternative: Use Selenium WebDriver
For sites that absolutely require cookies, launch real browser instances:
```python
from selenium import webdriver

def resolve_with_selenium(url: str) -> dict | None:
    """Fallback: Use Selenium to get authenticated access"""
    driver = webdriver.Chrome()
    driver.get(url)
    # Wait for video player to load
    time.sleep(3)
    
    # Extract from window.location or video sources
    video_src = driver.execute_script(
        "return document.querySelector('video')?.src || "
        "Array.from(document.querySelectorAll('source')).map(s=>s.src)[0]"
    )
    # ...
```

---

### Issue #3: Sites Not Covered by Current Scanners
**Problem**: Script Scan finds 6 JS files max, and some sites hide URLs in:
- Service Workers
- IndexedDB/LocalStorage
- WebSocket connections
- Dynamically generated iframes

**Solutions**:

#### Expand Pattern Matching
```python
def extract_script_candidates(text: str) -> list[str]:
    # Add patterns for common streaming APIs
    key_patterns = [
        # ... existing patterns ...
        
        # HLS playlist detection
        r'"hlsSource"\s*:\s*"([^"]+)"',
        r'"hls"\s*:\s*"([^"]+)"',
        
        # DASH/MPD (adaptive bitrate)
        r'"dash"\s*:\s*"([^"]+\.mpd[^"]*)"',
        r'"mpdUrl"\s*:\s*"([^"]+)"',
        
        # Generic streaming service APIs
        r'"(https://[^"]*\.m3u8[^"]*)"',
        r'"(https://[^"]*get_file[^"]*)"',
        
        # Encoded/base64 URLs (some sites obfuscate)
        r'"videoData"\s*:\s*"([A-Za-z0-9+/=]+)"',
    ]
```

#### Add Site-Specific Handlers
Create handler plugins for problematic sites:
```python
SITE_SPECIFIC_HANDLERS = {
    "pornhub.com": pornhub_resolve_handler,
    "xvideos.com": xvideos_resolve_handler,
    "xhamster.com": xhamster_resolve_handler,
    # ...
}

def resolve_direct_media(url: str) -> dict:
    source = normalize_source_url(url)
    domain = urlparse(source).netloc.lower()
    
    # Try site-specific handler first
    if domain in SITE_SPECIFIC_HANDLERS:
        try:
            return SITE_SPECIFIC_HANDLERS[domain](source)
        except Exception:
            pass  # Fall back to generic methods
    
    # ... rest of generic resolution ...
```

---

### Issue #4: Stream Authentication & Expiring URLs
**Problem**: Many sites generate expiring stream URLs with token authentication

**Symptoms**: Test passes (URL found) but streaming fails later (401 Unauthorized)

**Solutions**:

#### Implement URL Validation in Batch Test
```python
if args.inspect_stream and resolved_payload and resolved_payload.get("media_url"):
    try:
        # Check if URL is accessible RIGHT NOW
        upstream = fetch_remote_response(
            resolved_payload["media_url"],
            resolved_payload.get("referer"),
            "bytes=0-1"  # Only fetch 1 byte to test access
        )
        status = getattr(upstream, "status") or getattr(upstream, "code") or 200
        if status >= 400:
            note_parts.append(f"url_inaccessible={status}")
            current["host_stream_ready"] = "URL expired/auth failed"
```

#### Add OBS Bridge Mode
Create a lightweight proxy that maintains active connections:
```python
class StreamProxy:
    """Keep URLs "warm" by periodically accessing them"""
    def __init__(self, media_url: str, referer: str):
        self.media_url = media_url
        self.referer = referer
        self.last_access = time.time()
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat,
            daemon=True
        )
        self.heartbeat_thread.start()
    
    def _heartbeat(self):
        while True:
            try:
                # Keep connection alive every 30 seconds
                fetch_remote_response(self.media_url, self.referer, "bytes=0-1")
            except:
                pass
            time.sleep(30)
```

---

## Specific Site Strategies

### PornHub / XVideos / xHamster
These sites require authentication and use token-based URLs

**Current Attempts**:
1. ✅ Direct - May work for some public videos
2. ⚠️ Browser headers - Usually fails (needs auth)
3. ⚠️ Cookies - Fails if not logged in
4. ✅ Browser capture - Works (headless Chrome with cookies)
5. ⚠️ Page/Script scan - Finds placeholder, not actual stream

**Recommendation**: Force browser capture for these domains
```python
ALWAYS_USE_BROWSER_CAPTURE_DOMAINS = {
    "pornhub.com",
    "xvideos.com", 
    "xhamster.com",
    "xnxx.com",
}

if any(domain in source for domain in ALWAYS_USE_BROWSER_CAPTURE_DOMAINS):
    # Skip expensive yt-dlp attempts, go straight to browser capture
    if args.browser_capture:
        return browser_capture_media(source)
```

### Sites Using HLS/DASH (Adaptive Bitrate)
Examples: Many premium content sites

**Better Handling**:
```python
def select_stream_format(info: dict) -> dict | None:
    # Prefer HLS over direct MP4 for streaming stability
    formats = info.get("formats", [])
    
    # HLS playlists are better for OBS streaming (auto-bitrate adjustment)
    hls_formats = [f for f in formats if f.get("format_id", "").startswith("m3u8")]
    if hls_formats:
        return hls_formats[0]
    
    # Fall back to MP4
    mp4_formats = [f for f in formats if f.get("ext") == "mp4"]
    if mp4_formats:
        return sorted(mp4_formats, key=lambda f: f.get("height", 0), reverse=True)[0]
```

---

## OBS Streaming Integration

### Priority 1: Resolve and Stream (Recommended)
```javascript
// stream-player-lab.html (current implementation)
// 1. Call /api/stream/resolve to get media_url
// 2. OBS adds stream-player-overlay.html as browser source
// 3. Overlay loads media_url via proxy at /api/stream/proxy
// Benefits: Real-time streaming, no local download needed
```

### Priority 2: Download and Play (Fallback)
```javascript
// When streaming fails:
// 1. Call /api/stream/download-clip-test (or full download)
// 2. Video saved to ~/Desktop/Alcove/Downloads/
// 3. OBS points to media file or watch folder
// Benefits: Reliable, works offline
```

### OBS Configuration Template
```
[OBS Browser Source]
URL: http://127.0.0.1:8011/overlay/stream-player-overlay.html?payload=...
Width: 1920
Height: 1080
FPS: 60
Shutdown source when not visible: ✓
Refresh cache when page loads: ✓

[Optional: Fallback Media Source]
When above URL fails, use: ~/Desktop/Alcove/Ready/ (watch folder)
```

---

## Implementation Roadmap

### Phase 1: Fix Immediate Issues (This Week)
- [ ] Make Chrome DevTools port configurable with fallback logic
- [ ] Add Firefox cookie loading error handling
- [ ] Implement "skip browser capture if unavailable" mode for batch tests
- [ ] Add validation that resolved URLs are immediately accessible

### Phase 2: Expand Site Support (Next Week)
- [ ] Add site-specific handlers for top 10 problematic domains
- [ ] Implement pattern expansion for HLS/DASH/WebSocket streams  
- [ ] Add Selenium fallback for auth-required sites
- [ ] Improve page-scan to handle more obfuscation patterns

### Phase 3: Optimize for OBS (Week After)
- [ ] Test all streaming methods with actual OBS integration
- [ ] Implement URL "keep-alive" proxy for token-based URLs
- [ ] Add fallback chain UI to stream-player-lab.html
- [ ] Create comprehensive OBS setup guide

---

## Code Snippets Ready to Implement

### Quick Wins (Copy-Paste)

**1. Better error classification:**
```python
def classify_resolution_error(message: str) -> str:
    text = str(message or "").lower()
    
    if "chrome devtools" in text or "websocket" in text:
        return "browser-capture-unavailable"
    if "firefox" in text or "cookie" in text:
        return "cookies-unavailable"
    if "unsupported url" in text or "no formats" in text:
        return "site-unsupported"
    if "403" in text or "unauthorized" in text:
        return "auth-required"
    if "404" in text or "not found" in text:
        return "content-missing"
    if "timeout" in text or "connection" in text:
        return "network-error"
    
    return "unknown"
```

**2. Timeout-aware resolution:**
```python
def resolve_direct_media_timeout(url: str, timeout_seconds: int = 45) -> dict:
    """Wrap resolve with timeout"""
    import signal
    
    class TimeoutException(Exception): pass
    
    def timeout_handler(signum, frame):
        raise TimeoutException("Resolution took too long")
    
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout_seconds)
    
    try:
        return resolve_direct_media(url)
    finally:
        signal.alarm(0)  # Cancel alarm
```

**3. Retry logic for flaky sites:**
```python
def resolve_with_retry(url: str, max_attempts: int = 3, backoff_seconds: float = 2.0) -> dict:
    """Retry resolution with exponential backoff"""
    last_error = None
    
    for attempt in range(max_attempts):
        try:
            return resolve_direct_media(url)
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                wait_time = backoff_seconds * (2 ** attempt)
                time.sleep(wait_time)
    
    raise last_error
```

---

## Testing & Validation

### Run Tests with Improvements
```bash
# Current (failing):
python batch_stream_site_test.py stream-site-compatibility-matrix.csv --browser-capture

# Should become (passing):
python batch_stream_site_test.py stream-site-compatibility-matrix.csv \
  --browser-capture \
  --inspect-stream \
  --retry-attempts 2 \
  --skip-unavailable-fallbacks
```

### Recommended Test URLs by Site Type
```python
TEST_URLS = {
    "Direct MP4": "https://www.example.com/video.mp4",  # Simple, public
    "Page Scan": "https://embed.example.com/player.html",  # HTML embedded
    "Script Scan": "https://player.example.com/",  # JS-based
    "HLS Stream": "https://stream.example.com/playlist.m3u8",
    "Auth Required": "https://premium.example.com/",  # Login needed
}
```

---

## FAQ & Troubleshooting

**Q: Why does streaming work in stream-player-lab but batch tests fail?**  
A: Batch tests use direct Python function calls without the HTTP server. The HTTP server helps with CORS, proxy rewriting, and keeping connections alive.

**Q: Should I use OBS streaming or download fallback?**  
A: **Streaming is better** (real-time, lower bandwidth), but download is more reliable when:
- Site uses token authentication
- URLs expire quickly (< 5 minutes)
- You need guaranteed playback

**Q: Can I cache resolved URLs to speed up repeated plays?**  
A: Yes, but cache lifetime varies by site (10 minutes to 1 hour typically). Add cache invalidation logic.

**Q: What bitrate/resolution should I use in OBS?**  
A: Test each site. Recommended starting point: **1280x720 @ 2800 kbps**

---

## Summary

Your system has solid foundations:
- ✅ Multiple resolution strategies with good fallbacks
- ✅ Browser automation capability
- ✅ OBS integration framework
- ✅ Working stream-player-lab for manual testing

Main blockers for batch automation:
- ❌ Chrome DevTools connection issues
- ❌ Firefox cookie loading failures  
- ❌ No retry/timeout logic
- ❌ Missing site-specific handlers

**Next Steps**: Implement Phase 1 fixes above, prioritizing the browser DevTools port issue and cookie handling. Then test with actual OBS integration.

