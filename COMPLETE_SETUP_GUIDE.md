# 🎬 Alcove Streaming System - Complete Setup & Usage Guide

## 📦 What You Now Have

Your streaming infrastructure is now complete with three layers:

### Layer 1: Backend (Python Helper)
```
local_wheel_host_helper.py
├─ HTTP Server on port 8011
├─ Video resolution engine (9-layer fallback)
├─ Browser automation (Chrome DevTools)
├─ HLS/MP4/Direct stream support
└─ Download with compression (720p, 2800k)
```

### Layer 2: Frontend (Web Testing)
```
miniapp/
├─ stream-player-lab.html         → Manual stream testing
├─ stream-player-overlay.html     → OBS browser source
├─ obs-control-panel.html         → OBS control dashboard
├─ wheel-v2.html                  → Wheel selector
└─ host-panel.html                → Host controls
```

### Layer 3: OBS Integration (Control Scripts)
```
obs_controller.py                  → Python WebSocket OBS controller
wheel-streaming-bridge.py          → Wheel → Streaming workflow
OBS WebSocket Plugin               → (Install in OBS)
```

---

## 🚀 Quick Start (5 Minutes)

### 1️⃣ Install OBS WebSocket Plugin

```
OBS Studio v29.1+: Built-in (Tools → WebSocket Server Settings → Enable)
Older OBS: Download from https://github.com/obsproject/obs-websocket/releases
```

### 2️⃣ Start Helper Server

```bash
cd C:\Users\jd_ar\Desktop\Alcove
python local_wheel_host_helper.py
```

**Expected output:**
```
[INFO] Starting HTTP server on http://127.0.0.1:8011
[INFO] WebSocket server on ws://127.0.0.1:9001 (if enabled)
[INFO] Ready for connections...
```

### 3️⃣ Test in Browser

```
Open: http://127.0.0.1:8011/miniapp/stream-player-lab.html
Paste: https://example.com/video
Click: "Resolve Stream"
Result: Should display video or error info
```

### 4️⃣ Open OBS Control Panel

```
Open: file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/obs-control-panel.html
1. Enter OBS Host: localhost
2. Enter OBS Port: 4444
3. Click: "Connect to OBS"
4. Result: Scene list should populate
```

### 5️⃣ Create OBS Scenes

In OBS, create these scenes:

```
Scene: "Main Stream"
├─ Browser Source (URL: http://127.0.0.1:8011/miniapp/stream-player-overlay.html)
└─ Size: 1920x1080

Scene: "Wheel Display"
├─ Browser Source (URL: http://127.0.0.1:8011/miniapp/wheel-v2.html)
└─ Size: 1920x1080

Scene: "Break"
├─ Color Source (black background)
└─ Text: "Be right back..."
```

**Done!** You can now:
- ✅ Select videos via wheel
- ✅ Stream them in OBS
- ✅ Switch scenes with control panel
- ✅ Fall back to download if stream fails

---

## 📊 Complete Workflow

### Manual Workflow (Testing)

```
1. Open stream-player-lab.html
2. Paste video URL
3. Click "Resolve Stream" → Gets media URL
4. Click "Load in Overlay" → Updates OBS browser source
5. OBS displays video automatically
6. Click "Start Stream" in OBS → Stream goes live
7. When done: "Stop Stream" in OBS
```

### Automated Workflow (Production)

```python
# Python code (called by wheel or API)
from wheel_streaming_bridge import WheelStreamingBridge

bridge = WheelStreamingBridge()

result = bridge.process_wheel_selection(
    video_url="https://example.com/video",
    entry_id=1,
    auto_start=True  # Auto-start streaming
)

# Result:
# {
#   "success": true,
#   "mode": "stream",
#   "media_url": "https://cdn.example.com/stream.m3u8",
#   "stream_type": "hls",
#   "duration": 1847  # seconds
# }
```

---

## 🎮 Control Options

### Option 1: OBS Control Panel (Browser)
```
http://127.0.0.1:8011/miniapp/obs-control-panel.html

Controls:
- Scene Selector: Dropdown list
- Source Toggles: Per-scene visibility
- Stream Button: Large green/red toggle
- Status Indicator: Shows live/offline
- Refresh: Reload browser source
```

**Best for:** Visual, easy-to-use dashboard

### Option 2: Command Line (Python)
```bash
# Switch scene
python obs_controller.py --action set-scene --scene "Main Stream"

# Start streaming
python obs_controller.py --action start

# Stop streaming
python obs_controller.py --action stop

# Get status
python obs_controller.py --action status
```

**Best for:** Scripting, automation, headless servers

### Option 3: Python API (In Your Code)
```python
from obs_controller import OBSController

obs = OBSController(host="localhost", port=4444, password="")

# Scene control
obs.set_active_scene("Main Stream")

# Source control
obs.set_source_active("Main Stream", "stream-player-overlay", True)

# Stream control
obs.start_stream()
time.sleep(60)
obs.stop_stream()

# Status
status = obs.get_stream_status()
print(f"Streaming: {status['outputActive']}")
```

**Best for:** Integration with wheel system

### Option 4: Keyboard Shortcuts (Control Panel)
```
Space       → Toggle stream on/off
S + 1-9     → Switch to scene 1-9
R           → Refresh browser source
H           → Show help
```

**Best for:** Quick adjustments during stream

---

## 🔧 Advanced Configuration

### Custom Helper Settings

Edit `local_wheel_host_helper.py`:

```python
# Line ~40: Adjust CDPs port discovery
CDP_PORTS = [9223, 9222, 9221, 9220, 9225]

# Line ~100: Video compression settings
FFMPEG_ARGS = {
    'format': 'bestvideo+bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegVideoConvertor',
        'prefixedformat': 'best',
        'args': ['-vf', 'scale=1280:720', '-b:v', '2800k', '-b:a', '128k']
    }]
}

# Line ~150: Download folder
DOWNLOAD_FOLDER = r"C:\Users\jd_ar\Desktop\Alcove\Ready"
COMPRESSED_FOLDER = r"C:\Users\jd_ar\Desktop\Alcove\Compressed"
```

### Custom OBS Settings

```python
# obs_controller.py line ~20
class OBSController:
    def __init__(
        self,
        host="localhost",        # Change for network OBS
        port=4444,              # Change if OBS uses different port
        password="",            # Add if OBS has password
        auto_reconnect=True,    # Reconnect on disconnect
        timeout=10              # WebSocket timeout
    ):
```

### Nginx Reverse Proxy (For Remote OBS)

If your OBS is on another machine:

```nginx
# /etc/nginx/sites-available/obs
upstream obs_api {
    server 192.168.1.50:8011;
}

server {
    listen 8011;
    
    location / {
        proxy_pass http://obs_api;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

Then update:
```python
bridge = WheelStreamingBridge(
    obs_host="192.168.1.50",  # Your OBS machine
    obs_port=4455             # Custom port
)
```

---

## 📋 Supported Video Sites

The helper can resolve streams from:

### Direct Support (yt-dlp extractors)
- PornHub ✅
- XVideos ✅
- xHamster ✅
- GayForIt ✅
- BoyFriendTV ✅
- YouPorn ✅
- Redtube ✅
- RedGifs ✅
- Twitter/X ✅
- YouTube ✅
- Twitch ✅
- Vimeo ✅
- Dailymotion ✅
- (500+ more via yt-dlp)

### Browser Capture (Chrome DevTools)
- Sites with complex JavaScript
- Streaming sites with player obfuscation
- Any site viewable in browser

### Fallback Methods
- HLS playlist (.m3u8) scanning
- MP4/WebM URL detection
- JavaScript variable extraction
- Direct HTTP headers inspection

### Unsupported Sites
- Geographic restrictions (can't bypass)
- Age-restricted without cookies
- DRM-protected streams
- Subscription-locked content

---

## 🐛 Troubleshooting

### "Helper not responding"
```bash
# Check if running
netstat -an | findstr 8011

# If not running, start it
python local_wheel_host_helper.py

# Test connection
curl http://127.0.0.1:8011/api/health
```

### "OBS WebSocket connection refused"
```
1. Open OBS
2. Go to Tools → WebSocket Server Settings
3. Make sure "Enable WebSocket Server" is CHECKED
4. Note the port (default 4444)
5. Click Apply and OK
6. Try control panel again
```

### "Browser source shows blank in OBS"
```
1. Right-click source → Refresh
2. Check URL is correct in browser first
3. If URL is resolved successfully in stream-player-lab.html, it should work in OBS
4. Check OBS browser source has correct size (1920x1080)
5. Try disabling "Shutdown source when not visible"
```

### "Video URL resolves but doesn't play"
```
1. Try with stream-player-lab.html first
2. Check if it's age-restricted → Needs user cookies (manual test required)
3. If it works in lab but not OBS → Check CORS headers
4. If HLS playlist → Check rewritten URLs in proxy
```

### "Download works but stream doesn't"
```
1. Download indicates media URL is found
2. Issue is likely OBS browser source not loading it
3. Try: Clear browser cache, Refresh source, Restart OBS
4. Check streaming output logs: Help → View Log
```

### "Can't find Chrome DevTools port"
```
# Manual port discovery
lsof -i | grep devtools  # Show open DevTools ports
# Or in PowerShell:
Get-NetTCPConnection -LocalAddress 127.0.0.1 | Where-Object {$_.LocalPort -in 9220..9230}

# If nothing found:
1. Close all Chrome/Chromium instances
2. Restart OBS (which may launch Chrome DevTools)
3. Or disable browser-capture: Set CDP_PORTS = [] in helper
```

---

## 📈 Performance Optimization

### Stream Quality

```python
# In local_wheel_host_helper.py, adjust FFMPEG_ARGS
# Current: 720p, 2800k (good balance)
# Smaller stream: 480p, 1500k
# Larger stream: 1080p, 5000k

FFMPEG_ARGS = {
    'postprocessors': [{
        'key': 'FFmpegVideoConvertor',
        'args': [
            '-vf', 'scale=1280:720',  # ← Change here
            '-b:v', '2800k',          # ← Or here
            '-b:a', '128k'
        ]
    }]
}
```

### Network Bandwidth

```
OBS Settings:
- Encoding: NVENC (if GPU available) or CPU
- Bitrate: 5000-8000 kbps (Twitch recommended)
- Keyframe Interval: 2 seconds
- Profile: High
```

### Server Performance

```bash
# Monitor helper while streaming
python -m cProfile -s cumtime local_wheel_host_helper.py

# Enable compression
gzip on;
gzip_types text/plain application/json video/mp4;
```

---

## 🔐 Security

### Protect OBS WebSocket

```python
# obs_controller.py - Add password protection
obs = OBSController(
    host="localhost",
    port=4444,
    password="super_secret_password"  # Set in OBS too
)
```

In OBS:
```
Tools → WebSocket Server Settings
↓
Server Password: super_secret_password
```

### Firewall Rules (Windows)

```powershell
# Allow only localhost
New-NetFirewallRule -DisplayName "OBS WebSocket" -Direction Inbound -LocalPort 4444 -Protocol TCP -RemoteAddress 127.0.0.1 -Action Allow

# Allow specific network
New-NetFirewallRule -DisplayName "OBS WebSocket" -Direction Inbound -LocalPort 4444 -Protocol TCP -RemoteAddress 192.168.1.0/24 -Action Allow
```

### HTTPS for Helper (nginx)

```nginx
server {
    listen 443 ssl;
    server_name your-domain.com;
    
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://127.0.0.1:8011;
    }
}
```

---

## 📚 File Reference

| File | Purpose | Type |
|------|---------|------|
| `local_wheel_host_helper.py` | Main streaming engine | Python |
| `obs_controller.py` | OBS WebSocket client | Python |
| `wheel-streaming-bridge.py` | Wheel integration | Python |
| `stream-player-lab.html` | Testing interface | HTML/JS |
| `stream-player-overlay.html` | OBS browser source | HTML/JS |
| `obs-control-panel.html` | Control dashboard | HTML/JS |
| `wheel-v2.html` | Wheel selector UI | HTML/JS |
| `host-panel.html` | Host controls | HTML/JS |

---

## 🎯 Next Steps

1. **Test Everything**
   ```bash
   python stream-player-lab.html  # Manual testing
   ```

2. **Configure OBS Scenes** (see OBS_SETUP_GUIDE.md)

3. **Integrate with Wheel** (see wheel-streaming-bridge.py)

4. **Go Live!**
   ```bash
   python wheel-streaming-bridge.py --url "https://..." --entry-id 1
   ```

---

## 📞 Quick Links

- **Helper Health**: http://127.0.0.1:8011/api/health
- **Stream Lab**: http://127.0.0.1:8011/miniapp/stream-player-lab.html
- **OBS Panel**: file:///C:/Users/jd_ar/Desktop/Alcove/miniapp/obs-control-panel.html
- **OBS Download**: https://obsproject.com/download
- **WebSocket Plugin**: https://github.com/obsproject/obs-websocket

---

## ✅ Verification Checklist

- [ ] Helper server running (`http://127.0.0.1:8011/api/health` responds)
- [ ] OBS WebSocket enabled (Tools → WebSocket Server Settings)
- [ ] stream-player-lab.html resolves at least one URL successfully
- [ ] obs-control-panel.html connects to OBS
- [ ] Scene switching works via control panel
- [ ] Browser source displays video in OBS
- [ ] Streaming service configured (Twitch/YouTube)
- [ ] First test stream successful

**All green?** 🎉 Ready to go live!

