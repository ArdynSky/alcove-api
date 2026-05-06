# Alcove Streaming - Quick Reference Card

## 🚀 Start Everything

```bash
# Terminal 1: Start the helper server
python local_wheel_host_helper.py

# Terminal 2+: Open in browser
http://127.0.0.1:8011/miniapp/stream-player-lab.html     # Manual stream testing
http://127.0.0.1:8011/miniapp/obs-control-panel.html     # OBS controller
```

---

## 📋 API Endpoints (All on `http://127.0.0.1:8011`)

### Streaming Resolution
```
POST /api/stream/resolve
Body: {"submitted_url": "https://..."}
Response: {"media_url": "https://...", "stream_type": "hls|mp4", ...}
```

### Browser Capture (Fallback)
```
POST /api/stream/browser-capture
Body: {"submitted_url": "https://..."}
Response: {"media_url": "https://...", "method": "browser-capture", ...}
```

### Download Video
```
POST /api/stream/download-clip-test
Body: {"submitted_url": "https://...", "entry_id": 1}
Response: {"file_path": "C:\\...\\video.mp4", "size_mb": 12.5}
```

### Health Check
```
GET /api/health
Response: {"status": "ok", "version": "1.0"}
```

---

## 🎬 OBS Configuration

### WebSocket Connection
- Host: `localhost`
- Port: `4444`
- Password: (leave blank if not set)

### Browser Sources in OBS
```
Scene: "Main Stream"
  Source: "stream-player-overlay"
  URL: http://127.0.0.1:8011/miniapp/stream-player-overlay.html
  Size: 1920x1080
```

### With Payload (Advanced)
```
URL: http://127.0.0.1:8011/miniapp/stream-player-overlay.html?payload={
  "url": "https://example.com/video",
  "media_url": "https://cdn.example.com/stream.m3u8",
  "stream_type": "hls"
}
```

---

## ⚙️ Control Panel Buttons

| Button | Action | Result |
|--------|--------|--------|
| **Scene List** | Select scene | Switch OBS to scene |
| **Source Toggles** | Click source | Show/hide in current scene |
| **Start Stream** | Click green button | Begin streaming to service |
| **Stop Stream** | Click red button | End stream + save VOD |
| **Refresh Source** | F5 or refresh button | Reload browser source URL |

---

## ⌨️ Keyboard Shortcuts (Control Panel)

| Key | Action |
|-----|--------|
| `Space` | Toggle stream on/off |
| `S` + `1-9` | Switch to scene 1-9 |
| `R` | Refresh browser source |
| `H` | Show help |

---

## 🔍 Testing Checklist

- [ ] Helper server running: `curl http://127.0.0.1:8011/api/health`
- [ ] OBS WebSocket enabled: `Tools → WebSocket Server Settings`
- [ ] Control panel connects to OBS (test via browser)
- [ ] Browser source displays video overlay
- [ ] Streaming service configured (Twitch/YouTube auth)
- [ ] First test stream successful
- [ ] Wheel integration tested with 1 video URL

---

## 🛠️ Command Reference

```bash
# Test resolution on a URL
curl -X POST http://127.0.0.1:8011/api/stream/resolve \
  -d '{"submitted_url":"https://pornhub.com/view_video.php?viewkey=abc"}' \
  -H "Content-Type: application/json"

# Download video for fallback
curl -X POST http://127.0.0.1:8011/api/stream/download-clip-test \
  -d '{"submitted_url":"https://...","entry_id":1}' \
  -H "Content-Type: application/json"

# Get OBS scenes (Python)
python obs_controller.py --action list-scenes

# Switch OBS scene
python obs_controller.py --action set-scene --scene "Main Stream"

# Start streaming
python obs_controller.py --action start

# Stop streaming
python obs_controller.py --action stop
```

---

## 📁 Important File Paths

```
Helper Server:
  C:\Users\jd_ar\Desktop\Alcove\local_wheel_host_helper.py

OBS Control Scripts:
  C:\Users\jd_ar\Desktop\Alcove\obs_controller.py
  C:\Users\jd_ar\Desktop\Alcove\miniapp\obs-control-panel.html

Stream Testing Interface:
  C:\Users\jd_ar\Desktop\Alcove\miniapp\stream-player-lab.html

Downloaded Videos (Fallback):
  C:\Users\jd_ar\Desktop\Alcove\Ready\

Compressed Output:
  C:\Users\jd_ar\Desktop\Alcove\Compressed\
```

---

## 🚨 Troubleshooting One-Liners

```bash
# Is helper running?
curl http://127.0.0.1:8011/api/health

# Is OBS WebSocket working?
curl http://localhost:4444 2>&1 | grep -q "Connection refused" && echo "NOT RUNNING" || echo "RUNNING"

# Can I resolve a video?
curl -s -X POST http://127.0.0.1:8011/api/stream/resolve \
  -d '{"submitted_url":"https://example.com"}' \
  -H "Content-Type: application/json" | jq .

# What's the error?
python stream-player-lab.html  # Open in browser, check Console (F12)

# Clear cache and restart
python local_wheel_host_helper.py --clear-cache
```

---

## 💡 Pro Tips

1. **Use stream-player-lab.html first** - Test URLs here before putting in OBS
2. **Keep helper running in background** - Use Python `ensurepip` for venv
3. **Monitor obs_controller.py logs** - Shows which scenes/sources updated
4. **Set OBS to 60fps** - Better for streaming video
5. **Use nvenc encoding** - Faster than CPU encoding if you have NVIDIA
6. **Create backup scenes** - In case one fails, quick switch

---

## 🔗 Quick Links

- OBS Download: https://obsproject.com/download
- OBS WebSocket GitHub: https://github.com/obsproject/obs-websocket
- yt-dlp (used by helper): https://github.com/yt-dlp/yt-dlp
- Twitch Streaming Setup: https://www.twitch.tv/creators/

---

## 📞 Support

If something breaks:

1. Check console (F12 in browser)
2. Check helper logs (terminal running helper)
3. Check OBS logs: `Help → View Log`
4. Restart helper and OBS
5. Test with stream-player-lab.html manually

