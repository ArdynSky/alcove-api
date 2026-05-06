# OBS Setup Guide for Alcove Streaming

## What You Need

1. **OBS Studio** (v29.1 or later) - [Download](https://obsproject.com/download)
2. **OBS WebSocket Plugin v5.0+** - [GitHub](https://github.com/obsproject/obs-websocket/releases)
3. **Alcove Streaming Helper** - Already running on `http://127.0.0.1:8011`

---

## Step 1: Install OBS WebSocket Plugin

### Windows Installation:

**Option A: Using OBS Installer**
1. Download OBS Studio v29.1+
2. The WebSocket plugin is **built-in** as of v28.0+
3. Go to `Tools → WebSocket Server Settings`
4. Enable WebSocket Server and set port to **4444**
5. Set password if you prefer (or leave blank for local testing)

**Option B: Manual Installation**
1. Go to [OBS WebSocket Releases](https://github.com/obsproject/obs-websocket/releases)
2. Download the latest `.exe` installer for Windows
3. Run the installer
4. Restart OBS

**Verify Installation:**
- Open OBS
- Go to `Tools → WebSocket Server Settings`
- You should see the WebSocket Server section
- Toggle "Enable WebSocket Server" ON
- Note the port (default: 4444)

---

## Step 2: Configure OBS Scenes

Create these scenes in OBS:

### Scene 1: "Main Stream"
**Purpose**: Primary streaming scene with overlay

**Sources:**
1. Add → Browser Source
   - Name: `stream-player-overlay`
   - URL: `http://127.0.0.1:8011/miniapp/stream-player-overlay.html`
   - Width: 1920
   - Height: 1080
   - Refresh: Every 5 seconds (optional)

2. (Optional) Add → Text Source
   - Name: `Stream Title`
   - Text: "Wheel of Desire - Stream"

### Scene 2: "Wheel Display"  
**Purpose**: Show wheel while selecting

**Sources:**
1. Add → Browser Source
   - Name: `wheel-overlay`
   - URL: `http://127.0.0.1:8011/miniapp/wheel-v2.html`
   - Width: 1920
   - Height: 1080

### Scene 3: "Fallback Media"
**Purpose**: Show downloaded video file as backup

**Sources:**
1. Add → Media Source
   - Name: `fallback-video`
   - File: `C:\Users\jd_ar\Desktop\Alcove\Ready\` (watch folder)
   - Loop: ON

### Scene 4: "Intermission"
**Purpose**: Show during breaks

**Sources:**
1. Add → Color Source
   - Color: Black
2. Add → Text Source
   - Text: "Thanks for watching!"

---

## Step 3: Test WebSocket Connection

### Using PowerShell

```powershell
# Test if WebSocket is responding
$ws = New-WebSocket -Uri "ws://localhost:4444"
$ws.Connect()
$ws.Send('{"request-type":"GetVersion"}')
# Should get version response
```

### Or Visit the Control Panel

```
Open: file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/obs-control-panel.html

1. Enter host: localhost
2. Enter port: 4444
3. Leave password blank (unless you set one)
4. Click "Connect to OBS"
```

---

## Step 4: Set Up Streaming

### In OBS:
1. `Settings → Stream`
2. Service: Choose your streaming platform (Twitch, YouTube, etc.)
3. Server & Key: Enter your streaming credentials
4. Apply settings

### For Testing (Local):
1. Skip service setup (just stream locally)
2. Or use a test RTMP server

---

## Step 5: Control Panel Usage

### Option A: Browser Dashboard
```
Open: file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/obs-control-panel.html
```

**What you can do:**
- ✅ Switch between scenes
- ✅ Start/stop streaming
- ✅ Update stream overlay URL
- ✅ Refresh browser sources
- ✅ View streaming status

### Option B: Command Line (Python)
```bash
# Install dependency
pip install obs-websocket-py

# List available scenes
python obs_controller.py --action list-scenes

# Switch to a scene
python obs_controller.py --action set-scene --scene "Main Stream"

# Start streaming
python obs_controller.py --action start

# Update URL in browser source
python obs_controller.py --action set-url \
  --scene "Main Stream" \
  --source "stream-player-overlay" \
  --url "http://127.0.0.1:8011/miniapp/stream-player-overlay.html?payload=..."

# Check status
python obs_controller.py --action status
```

---

## Complete Workflow

### Step-by-Step Playthrough:

```
1. User clicks "Select Video" in wheel
2. Wheel sends URL to Alcove API
3. Helper resolves media URL from website
4. Helper returns direct stream URL
5. OBS updates browser source with new payload
6. Stream automatically loads resolved video
7. OBS stream plays to audience
8. User watches stream in OBS preview
9. When done, click "Stop Stream" or switch scene
```

### Automation with Python:

```python
import subprocess
import time

# Get video URL from wheel API
video_url = "https://www.example.com/video.php"

# Resolve it via helper
import requests
response = requests.post(
    "http://127.0.0.1:8011/api/stream/resolve",
    json={"submitted_url": video_url}
)
resolved = response.json()
media_url = resolved["media_url"]

# Update OBS
subprocess.run([
    "python", "obs_controller.py",
    "--action", "set-url",
    "--scene", "Main Stream",
    "--source", "stream-player-overlay",
    "--url", media_url
])

# Start streaming
subprocess.run([
    "python", "obs_controller.py",
    "--action", "start"
])
```

---

## Troubleshooting

### Problem: "WebSocket Server Settings" Not Found

**Solution:**
1. Update OBS to v28.0+ (includes WebSocket built-in)
2. Or manually install WebSocket plugin from [GitHub](https://github.com/obsproject/obs-websocket)
3. Restart OBS after installing

### Problem: "Connection refused" on port 4444

**Solution:**
1. Open OBS
2. Go to `Tools → WebSocket Server Settings`
3. Make sure checkbox "Enable WebSocket Server" is **checked**
4. Verify port is 4444
5. Click "Apply" and "OK"
6. Try connecting again

### Problem: Browser source shows blank/black

**Solution:**
1. Check URL is correct: `http://127.0.0.1:8011/miniapp/stream-player-overlay.html`
2. Test URL in browser first
3. In OBS, right-click source → Refresh
4. Check console (F12) in OBS browser source for errors

### Problem: Video URL won't update

**Solution:**
1. Make sure helper server is running: `python local_wheel_host_helper.py`
2. Test with: `curl http://127.0.0.1:8011/api/health`
3. Verify browser source name matches exactly (case-sensitive)
4. Try refreshing the source after updating URL

### Problem: Streaming starts but no video

**Solution:**
1. Check video URL is accessible
2. Open stream-player-lab.html to test URL manually
3. If it fails in lab, it will fail in OBS
4. Try downloading instead (fallback mode)

---

## Scene Configuration Reference

### Minimal Setup (Just Streaming)

```
Scene: "Stream"
├─ Browser Source: stream-overlay
│  └─ URL: http://127.0.0.1:8011/miniapp/stream-player-overlay.html
└─ Optional: Text with current title
```

### Pro Setup (Wheel + Stream + Fallback)

```
Scene 1: "Main Stream"
├─ Browser: stream-overlay (1920x1080)
├─ Text: Stream title
└─ Chat: Browser source for chat

Scene 2: "Wheel"
├─ Browser: wheel-v2.html (1920x1080)
└─ Lights/Effects

Scene 3: "Fallback"
├─ Media: Watch folder (Ready/)
├─ Watermark: PNG overlay
└─ Music: Audio source

Scene 4: "Break"
├─ Color source (background)
├─ Countdown timer
└─ Social media info text
```

---

## Advanced: Automating Everything

### Option 1: Create a Custom Launcher

```python
#!/usr/bin/env python3
"""Launcher that starts everything and sets up OBS"""

import subprocess
import time
import sys

# Start helper server
print("Starting Alcove helper server...")
helper = subprocess.Popen([sys.executable, "local_wheel_host_helper.py"])

# Wait for server to start
time.sleep(2)

# Open OBS
print("Opening OBS...")
subprocess.Popen(["C:\\Program Files\\OBS Studio\\bin\\64bit\\obs.exe"])

# Open control panel in browser
time.sleep(3)
print("Opening control panel...")
subprocess.Popen(["start", "http://127.0.0.1:8011/miniapp/obs-control-panel.html"], shell=True)

# Open stream lab
print("Opening stream lab...")
subprocess.Popen(["start", "file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/stream-player-lab.html"], shell=True)

print("\n✓ Everything started! Ready to stream.")
print("  - OBS: Configure scenes manually or use control panel")
print("  - Helper: http://127.0.0.1:8011/api/health")
print("  - Control: http://127.0.0.1:8011/miniapp/obs-control-panel.html")

try:
    helper.wait()
except KeyboardInterrupt:
    print("\nShutting down...")
    helper.terminate()
```

### Option 2: PowerShell Shortcuts

Create `C:\Users\jd_ar\Desktop\Alcove\start-streaming.ps1`:

```powershell
# Start helper server
Write-Host "Starting Alcove helper..."
Start-Process python -ArgumentList "local_wheel_host_helper.py" -WorkingDirectory "$PSScriptRoot"

# Wait a moment
Start-Sleep -Seconds 2

# Open OBS
Write-Host "Opening OBS..."
& "C:\Program Files\OBS Studio\bin\64bit\obs.exe"

# Open control panel
Write-Host "Opening control panel..."
Start-Process "http://127.0.0.1:8011/miniapp/obs-control-panel.html"

Write-Host "Ready to stream!"
```

Run with: `powershell -ExecutionPolicy Bypass -File start-streaming.ps1`

---

## Quick Commands Reference

```bash
# Test helper is running
curl http://127.0.0.1:8011/api/health

# Resolve a URL
curl -X POST http://127.0.0.1:8011/api/stream/resolve \
  -H "Content-Type: application/json" \
  -d '{"submitted_url":"https://www.example.com/video"}'

# Download low-res for fallback
curl -X POST http://127.0.0.1:8011/api/stream/download \
  -H "Content-Type: application/json" \
  -d '{"submitted_url":"https://www.example.com/video","entry_id":1}'

# List OBS scenes
python obs_controller.py --action list-scenes

# Get streaming status  
python obs_controller.py --action status
```

---

## You're Ready!

1. ✅ Helper server configured
2. ✅ OBS WebSocket enabled
3. ✅ Scenes created
4. ✅ Control panel ready

**Next steps:**
- Test with stream-player-lab.html
- Configure your streaming service (Twitch, YouTube, etc.)
- Set up alerts and overlays in OBS
- Practice switching scenes and URLs
- Go live!

