# Quick Start: Getting OBS Streaming Working

## Diagnosis: Why Your Batch Tests Failed

The CSV results show two key failures:

1. **Firefox Cookies Not Found**
   - ❌ `ERROR: could not find firefox cookies database in 'C:\Users\jd_ar\AppData\Roaming\Mozilla\Firefox\Profiles'`
   - This means: The batch test tried to use your saved Firefox login cookies, but the system couldn't locate them
   - **Why it matters**: Auth-required sites (PornHub, XVideos, etc.) often need login cookies

2. **Chrome DevTools Won't Connect** 
   - ❌ `Error: Chrome DevTools endpoint did not start: <urlopen error [WinError 10061] No connection could be made because the target machine actively refused it>`
   - This means: The system tried to launch Chrome with remote debugging, but connection to port 9223 failed
   - **Why it matters**: This is the fallback when direct methods fail

## Why stream-player-lab.html Works But Batch Tests Don't

**stream-player-lab.html:**
- ✅ Uses HTTP API calls to the local helper server
- ✅ Helper server is running and listening
- ✅ Can resolve URLs successfully when calling via API

**Batch tests (batch_stream_site_test.py):**
- ❌ Calls functions directly in Python (no server involved)
- ❌ Uses different environment configuration
- ❌ Chrome/Firefox setup issues affect it differently

---

## 3-Step Plan to Fix This

### STEP 1: Start the Helper Server (If Not Already Running)

```bash
# In PowerShell, from c:\Users\jd_ar\Desktop\Alcove:
python local_wheel_host_helper.py

# You should see output like:
# Serving on http://127.0.0.1:8011
```

**Check if it's working:**
```bash
# In another PowerShell, try:
curl http://127.0.0.1:8011/api/health

# Should return JSON with status: "ok"
```

### STEP 2: Test Via stream-player-lab.html (Manual Testing)

```
1. Open: file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/stream-player-lab.html
2. Paste a test URL: https://www.pornhub.com/view_video.php?viewkey=69e9654d1db0e
3. Click "Resolve And Load"
4. Watch the console for detailed error messages

Expected Results:
✅ "Direct stream resolved" = Works immediately
⚠️ "Browser capture found playable media request" = Found but via browser (slower)
❌ "Could not resolve a direct video stream" = Need to investigate
```

### STEP 3: Run Batch Tests with Fixes Applied

```bash
# With Chrome DevTools skip flag:
python batch_stream_site_test.py stream-site-compatibility-matrix.csv \
  --output results_improved.csv \
  --skip-browser-capture-on-error \
  --validate-urls

# Then compare:
# - Original: stream-site-compatibility-matrix-results.csv
# - New: results_improved.csv
```

---

## Implementation Order (Do These in Sequence)

### Priority 1 (Required - Do First)
**Fix Chrome DevTools Port Issue**

File: `local_wheel_host_helper.py`

Find this code block (around line 970):
```python
def start_capture_browser() -> subprocess.Popen:
    browser = chrome_executable()
    if not browser:
        raise RuntimeError("No Chrome or Edge executable was found on this machine.")
    profile_dir = Path(tempfile.gettempdir()) / "alcove-stream-capture-profile"
    # ... rest of function
```

Replace with (from STREAMING_FIXES.md, FIX #1):
- Add `find_available_cdp_port()` function
- Update `start_capture_browser()` to return port number
- Update `browser_capture_media()` to use the found port
- Update `chrome_devtools_json()` to accept port parameter

**Why**: This lets the system find an available port automatically instead of always using 9223.

### Priority 2 (Recommended - Do Second)  
**Improve Error Classification**

File: `local_wheel_host_helper.py`

Find `classify_resolution_error()` function (around line 600).

Replace with improved version from STREAMING_FIXES.md, FIX #3:
- Better detection of Chrome/Browser issues
- Distinguish between auth failures vs site unsupported
- Separate network errors from content errors

**Why**: Better error messages help you understand what went wrong.

### Priority 3 (Optional - Do Third)
**Add Batch Test Improvements**

File: `batch_stream_site_test.py`

Add new command-line arguments:
- `--skip-browser-capture-on-error` (don't fail if Chrome DevTools unavailable)
- `--validate-urls` (check if resolved URLs work NOW)

**Why**: Makes batch testing more resilient and informative.

---

## OBS Configuration for Streaming

Once you have URLs resolving successfully, configure OBS to use them:

### Method A: Direct Stream (Recommended)

```
OBS Setup:
1. Add new Source → Browser
2. Set URL to: http://127.0.0.1:8011/miniapp/stream-player-overlay.html
3. Set Resolution: 1920x1080
4. Set FPS: 60
5. Enable: "Shutdown source when not visible"

To use with Wheel:
- Wheel selects video URL
- Helper resolves media URL
- OBS browser source loads the overlay
- Overlay displays the resolved stream

This gives you REAL-TIME streaming (no download needed).
```

### Method B: Download Fallback

```
OBS Setup (as backup):
1. Open: http://127.0.0.1:8011/miniapp/stream-player-lab.html in browser
2. Paste URL and click "Download Clip Test"
3. Wait for file to appear in ~/Desktop/Alcove/Downloads/
4. In OBS, add Media Source → point to Downloads folder
5. Enable "Watch folder for changes"

This gives you RELIABLE fallback (file-based playback).
```

### Method C: Hybrid (My Recommendation)

```
OBS Scene:
├─ Browser Source (Method A) - Primary stream
├─ VLC/Media Source (Method B) - Fallback video file
└─ Overlay - Status indicator

Workflow:
1. Try to stream URL (Method A)
2. If timeout or error → automatically switch to downloaded file (Method B)
3. Indicate to viewer which method is active
```

---

## Troubleshooting Checklist

### "Chrome DevTools endpoint did not start"
```
Quick Fix:
1. Kill any Chrome processes: taskkill /F /IM chrome.exe
2. Set environment variable: 
   $env:ALCOVE_CDP_PORT = 9224
3. Try again

Detailed Fix:
1. Check if port 9223 is in use:
   netstat -ano | findstr :9223
2. If in use, kill the process:
   taskkill /PID <PID>
3. Or set different port:
   $env:ALCOVE_CDP_PORT = 9225
```

### "could not find firefox cookies database"
```
Quick Fix (Skip Firefox):
Run batch tests with: --skip-browser-capture-on-error

OR

Permanent Fix:
1. Open Firefox
2. Go to: about:preferences#privacy
3. Ensure "Logins and Passwords" are saved
4. Close Firefox completely
5. Retry batch test

Verify Firefox profile exists:
dir "$env:APPDATA\Mozilla\Firefox\Profiles"
```

### "Could not resolve a direct video stream"
```
This means ALL methods failed (yt-dlp, page-scan, script-scan).

Options:
1. Try with stream-player-lab.html (manually)
2. Enable browser capture (slower but more thorough)
3. This site might require:
   - Paid membership
   - JavaScript execution beyond what yt-dlp can handle
   - Special authentication beyond cookies
```

### "Browser capture did not observe any playable media requests"
```
This means Chrome opened but didn't see any video URLs.

Reasons:
1. Site requires user interaction (login button, accept ToS, etc.)
2. Video loads via JavaScript after page interaction
3. Site uses iframe embedding from another domain

Fix for some cases:
- Modify the click selectors in browser_capture_media()
- Search for site-specific selectors (e.g., [class*="play"], [data-video-id])
- Consider adding to SITE_SPECIFIC_HANDLERS
```

---

## Testing Each Piece

### Test 1: Is Helper Server Running?
```bash
curl http://127.0.0.1:8011/api/health
# Should return: {"status":"ok", "downloads_dir":"...", "ffmpeg_available":true, ...}
```

### Test 2: Can Helper Resolve a URL?
```bash
$body = @{"submitted_url"="https://www.example.com/video.php"} | ConvertTo-Json
$response = Invoke-WebRequest -Uri "http://127.0.0.1:8011/api/stream/resolve" `
  -Method POST -Body $body -ContentType "application/json"
$response.Content
# Should show: "media_url": "https://...", "title": "...", "resolve_strategy": "..."
```

### Test 3: Can stream-player-lab.html Load?
```bash
# Open in browser:
file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/stream-player-lab.html

# Should show a form with:
- Video URL input
- Buttons: "Resolve And Load", "Browser Capture", etc.
- Status display
- Overlay preview on right
```

### Test 4: Can OBS Connect?
```
1. In OBS, add Browser Source
2. URL: http://127.0.0.1:8011/miniapp/stream-player-overlay.html
3. Check if preview appears
4. If not, check OBS logs for errors
```

---

## Quick Performance Benchmarks

Expected timing for each method:

```
Direct Resolution (yt-dlp):        2-5 seconds
  - Fast when site exposes URLs in HTML/JS
  - Preferred method

Browser Capture (Chrome + DevTools): 15-25 seconds
  - Slower, more reliable
  - Good for JavaScript-heavy sites
  - Used as fallback

Download:                           Varies (10s - 5+ minutes)
  - For low-res versions: 30-60 seconds
  - For full videos: Much longer
  - Used as final fallback
```

---

## Advanced: Site-Specific Optimization

If you find certain sites always fail, create a handler:

### Example: Optimize for PornHub

```python
# In local_wheel_host_helper.py, add:

def pornhub_handler(url: str) -> dict:
    """PornHub typically requires browser capture"""
    # Skip expensive yt-dlp attempts
    return browser_capture_media(url)

# In SITE_SPECIFIC_HANDLERS:
SITE_SPECIFIC_HANDLERS["pornhub.com"] = pornhub_handler

# Now when resolving pornhub.com URLs, it goes straight to browser capture
```

Apply similar patterns for other problematic sites.

---

## Success Indicators

### You've Got It Working When:

✅ stream-player-lab.html can resolve URLs  
✅ Resolution takes < 10 seconds per URL  
✅ OBS browser source displays streaming video  
✅ Batch tests show > 70% success rate  
✅ Errors are clear and actionable  

### Next Level (OBS Integration):

✅ Wheel successfully selects videos  
✅ Selected video streams to OBS  
✅ Stream plays without buffering  
✅ Fallback to download works when needed  

---

## Additional Resources

**Files in your Alcove directory:**
- `STREAMING_ANALYSIS.md` - Detailed technical analysis
- `STREAMING_FIXES.md` - Code snippets ready to copy-paste
- `stream-site-compatibility-matrix.csv` - Your test URLs
- `batch_stream_site_test.py` - The test runner
- `local_wheel_host_helper.py` - The main helper server

**Commands you might need:**
```bash
# Start helper server
cd c:\Users\jd_ar\Desktop\Alcove
python local_wheel_host_helper.py

# Run improved batch tests
python batch_stream_site_test.py stream-site-compatibility-matrix.csv `
  --skip-browser-capture-on-error `
  --validate-urls `
  --output results_v2.csv

# Check what's listening on port 8011
netstat -ano | findstr :8011

# Kill stuck Chrome process
taskkill /F /IM chrome.exe
```

---

## Questions?

If something doesn't work:

1. **Check the detailed error message** - Run with `--validate-urls` flag
2. **Review STREAMING_ANALYSIS.md** - Has issue-specific solutions
3. **Check stream-player-lab.html console** - Open DevTools (F12) and look for errors
4. **Test in isolation** - Use individual test URLs before running batch tests

Next steps after getting this working: Integrate with your wheel system and OBS pipeline!

