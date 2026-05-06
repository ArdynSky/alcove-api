#!/usr/bin/env python3
"""
Alcove Streaming - Wheel Integration Bridge

This script integrates the wheel selector with the streaming system:
1. Watches for wheel selection events
2. Resolves video URL via helper API
3. Updates OBS browser source with resolved video
4. Starts streaming automatically
5. Falls back to download if streaming fails
"""

import json
import time
import subprocess
import sys
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from pathlib import Path

import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('wheel-streaming-bridge.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
HELPER_URL = "http://127.0.0.1:8011"
OBS_HOST = "localhost"
OBS_PORT = 4444
TIMEOUT = 30

class WheelStreamingBridge:
    """Bridge between wheel selection and OBS streaming"""
    
    def __init__(self, obs_host: str = OBS_HOST, obs_port: int = OBS_PORT):
        self.helper_url = HELPER_URL
        self.obs_host = obs_host
        self.obs_port = obs_port
        self.last_entry_id = None
        self.streaming = False
        
    def resolve_video_url(self, video_url: str) -> Optional[Dict[str, Any]]:
        """Resolve a video URL to playable media URL"""
        try:
            logger.info(f"Resolving video URL: {video_url}")
            
            response = requests.post(
                f"{self.helper_url}/api/stream/resolve",
                json={"submitted_url": video_url},
                timeout=TIMEOUT
            )
            response.raise_for_status()
            
            result = response.json()
            
            if result.get("success"):
                logger.info(f"✓ Resolved: {result.get('media_url', 'N/A')}")
                logger.info(f"  Type: {result.get('stream_type', 'unknown')}")
                logger.info(f"  Duration: {result.get('duration', 'N/A')}")
                return result
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"✗ Resolution failed: {error}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"✗ API error: {e}")
            return None
    
    def update_obs_source(self, media_url: str, entry_id: int) -> bool:
        """Update OBS browser source with new URL"""
        try:
            logger.info(f"Updating OBS source with: {media_url}")
            
            # Build payload for browser source
            payload = {
                "url": media_url,
                "entry_id": entry_id,
                "timestamp": datetime.now().isoformat(),
                "mode": "stream"
            }
            
            # Create overlay URL with payload
            overlay_url = f"{self.helper_url}/miniapp/stream-player-overlay.html"
            
            # Call obs_controller to update source
            result = subprocess.run([
                sys.executable, "obs_controller.py",
                "--action", "set-url",
                "--scene", "Main Stream",
                "--source", "stream-player-overlay",
                "--url", f"{overlay_url}?payload={json.dumps(payload)}"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✓ OBS source updated")
                return True
            else:
                logger.error(f"✗ OBS update failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error updating OBS: {e}")
            return False
    
    def start_streaming(self) -> bool:
        """Start OBS stream"""
        try:
            logger.info("Starting OBS stream...")
            
            result = subprocess.run([
                sys.executable, "obs_controller.py",
                "--action", "start"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✓ Stream started")
                self.streaming = True
                return True
            else:
                logger.error(f"✗ Stream start failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error starting stream: {e}")
            return False
    
    def stop_streaming(self) -> bool:
        """Stop OBS stream"""
        try:
            logger.info("Stopping OBS stream...")
            
            result = subprocess.run([
                sys.executable, "obs_controller.py",
                "--action", "stop"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info("✓ Stream stopped")
                self.streaming = False
                return True
            else:
                logger.error(f"✗ Stream stop failed: {result.stderr}")
                return False
                
        except Exception as e:
            logger.error(f"✗ Error stopping stream: {e}")
            return False
    
    def download_fallback(self, video_url: str, entry_id: int) -> Optional[str]:
        """Download video as fallback if streaming fails"""
        try:
            logger.info(f"Downloading video as fallback: {video_url}")
            
            response = requests.post(
                f"{self.helper_url}/api/stream/download-clip-test",
                json={
                    "submitted_url": video_url,
                    "entry_id": entry_id
                },
                timeout=120  # Longer timeout for download
            )
            response.raise_for_status()
            
            result = response.json()
            
            if result.get("success"):
                file_path = result.get("file_path")
                size_mb = result.get("size_mb", 0)
                logger.info(f"✓ Downloaded: {file_path} ({size_mb}MB)")
                return file_path
            else:
                logger.error(f"✗ Download failed: {result.get('error')}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"✗ Download error: {e}")
            return None
    
    def process_wheel_selection(self, video_url: str, entry_id: int, auto_start: bool = True) -> Dict[str, Any]:
        """
        Main workflow: Process a wheel selection
        
        Args:
            video_url: URL of video to stream
            entry_id: Unique ID of wheel entry
            auto_start: Automatically start streaming if resolution succeeds
        
        Returns:
            Status dict with success/failure info
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Processing wheel selection: Entry {entry_id}")
        logger.info(f"{'='*60}")
        
        self.last_entry_id = entry_id
        
        # Step 1: Resolve URL
        resolved = self.resolve_video_url(video_url)
        
        if not resolved or not resolved.get("success"):
            logger.warning("Resolution failed, attempting download fallback...")
            
            # Fallback: Download
            download_path = self.download_fallback(video_url, entry_id)
            
            return {
                "success": bool(download_path),
                "mode": "download",
                "file_path": download_path,
                "error": resolved.get("error") if resolved else "Unknown error"
            }
        
        media_url = resolved.get("media_url")
        
        # Step 2: Update OBS
        if not self.update_obs_source(media_url, entry_id):
            logger.warning("Failed to update OBS, but URL is resolved")
        
        # Step 3: Start streaming
        if auto_start:
            self.start_streaming()
        
        return {
            "success": True,
            "mode": "stream",
            "media_url": media_url,
            "stream_type": resolved.get("stream_type", "unknown"),
            "duration": resolved.get("duration", None)
        }
    
    def check_helper_status(self) -> bool:
        """Check if helper API is alive"""
        try:
            response = requests.get(
                f"{self.helper_url}/api/health",
                timeout=5
            )
            return response.status_code == 200
        except:
            return False

def main():
    """CLI interface for testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Wheel-OBS Streaming Bridge")
    parser.add_argument("--url", help="Video URL to test")
    parser.add_argument("--entry-id", type=int, default=1, help="Entry ID (default: 1)")
    parser.add_argument("--obs-host", default=OBS_HOST, help="OBS host (default: localhost)")
    parser.add_argument("--obs-port", type=int, default=OBS_PORT, help="OBS port (default: 4444)")
    parser.add_argument("--no-auto-start", action="store_true", help="Don't auto-start stream")
    parser.add_argument("--check-status", action="store_true", help="Check helper status and exit")
    
    args = parser.parse_args()
    
    bridge = WheelStreamingBridge(args.obs_host, args.obs_port)
    
    # Check helper status
    if args.check_status or args.url is None:
        if bridge.check_helper_status():
            logger.info("✓ Helper API is healthy")
            sys.exit(0)
        else:
            logger.error("✗ Helper API is not responding")
            sys.exit(1)
    
    # Process selection
    result = bridge.process_wheel_selection(
        args.url,
        args.entry_id,
        auto_start=not args.no_auto_start
    )
    
    # Print result
    print("\n" + "="*60)
    print("RESULT:")
    print("="*60)
    print(json.dumps(result, indent=2))
    
    sys.exit(0 if result["success"] else 1)

if __name__ == "__main__":
    main()


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

"""
Test from command line:

# Check helper status
python wheel-streaming-bridge.py --check-status

# Test with a URL (auto-start streaming)
python wheel-streaming-bridge.py \
  --url "https://www.pornhub.com/view_video.php?viewkey=abc123" \
  --entry-id 5

# Test without auto-starting stream
python wheel-streaming-bridge.py \
  --url "https://example.com/video" \
  --entry-id 1 \
  --no-auto-start

# Specify custom OBS host/port
python wheel-streaming-bridge.py \
  --url "https://example.com/video" \
  --obs-host 192.168.1.100 \
  --obs-port 4455


Integration with wheel-v2.html (JavaScript):

```javascript
// When wheel selects a winner:
async function onWheelWinner(entry) {
    const response = await fetch('http://127.0.0.1:8011/api/wheel-stream', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            url: entry.video_url,
            entry_id: entry.id,
            auto_start: true
        })
    });
    
    const result = await response.json();
    
    if (result.success) {
        showNotification(`✓ Streaming ${entry.name}`);
        updateStreamStatus('LIVE');
    } else {
        showNotification(`✗ Failed: ${result.error}. Downloading instead...`);
        updateStreamStatus('DOWNLOADING');
    }
}
```


Automation with cron (on 24/7 server):

# Test every minute if helper is alive
* * * * * python /path/to/wheel-streaming-bridge.py --check-status

# Check and log status every 5 minutes
*/5 * * * * python /path/to/wheel-streaming-bridge.py --check-status >> /var/log/alcove-bridge.log 2>&1
"""
