#!/usr/bin/env python3
"""
OBS WebSocket Controller for Alcove Streaming
Controls OBS scenes, sources, and streaming via Python + WebSocket.
Requires: pip install obs-websocket-py
"""

import argparse
import json
import sys

try:
    from obswebsocket import obsws, requests as obs_requests
except ImportError:
    print("ERROR: obs-websocket-py not installed")
    print("Install it with: pip install obs-websocket-py")
    sys.exit(1)


class OBSController:
    def __init__(self, host: str = "localhost", port: int = 4455, password: str = ""):
        self.host = host
        self.port = port
        self.password = password
        self.ws = None
        self.connected = False

    @staticmethod
    def _scene_name(scene: dict) -> str:
        """Handle both OBS v4-style and v5-style scene dictionaries."""
        return scene.get("sceneName") or scene.get("name") or ""

    def connect(self) -> bool:
        """Connect to OBS WebSocket server."""
        try:
            self.ws = obsws(self.host, self.port, self.password)
            self.ws.connect()
            self.connected = True
            print(f"Connected to OBS at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"Failed to connect to OBS: {e}")
            print("Make sure OBS is running and WebSocket server is enabled")
            return False

    def disconnect(self):
        """Disconnect from OBS."""
        if self.ws:
            self.ws.disconnect()
            self.connected = False

    def get_scenes(self) -> list[str]:
        """Get list of all scenes."""
        try:
            response = self.ws.call(obs_requests.GetSceneList())
            return [
                self._scene_name(scene)
                for scene in response.getScenes()
                if self._scene_name(scene)
            ]
        except Exception as e:
            print(f"Error getting scenes: {e}")
            return []

    def set_scene(self, scene_name: str) -> bool:
        """Switch to a scene."""
        try:
            try:
                self.ws.call(obs_requests.SetCurrentProgramScene(scene_name))
            except Exception:
                self.ws.call(obs_requests.SetCurrentPreviewScene(scene_name))
            print(f"Switched to scene: {scene_name}")
            return True
        except Exception as e:
            print(f"Error switching scene: {e}")
            return False

    def start_streaming(self) -> bool:
        """Start streaming."""
        try:
            self.ws.call(obs_requests.StartStreaming())
            print("Streaming started")
            return True
        except Exception as e:
            print(f"Error starting stream: {e}")
            return False

    def stop_streaming(self) -> bool:
        """Stop streaming."""
        try:
            self.ws.call(obs_requests.StopStreaming())
            print("Streaming stopped")
            return True
        except Exception as e:
            print(f"Error stopping stream: {e}")
            return False

    def get_streaming_status(self) -> dict:
        """Get current streaming status."""
        try:
            response = self.ws.call(obs_requests.GetStreamingStatus())
            stats = {}
            try:
                stats = response.getStats() or {}
            except Exception:
                stats = {}
            return {
                "streaming": response.getStreaming(),
                "recording": response.getRecording(),
                "total_frames": stats.get("total_frames", 0),
                "dropped_frames": stats.get("dropped_frames", 0),
            }
        except Exception as e:
            print(f"Error getting status: {e}")
            return {}

    def set_source_url(self, scene_name: str, source_name: str, url: str) -> bool:
        """Update a browser/media source with a new URL."""
        try:
            response = self.ws.call(obs_requests.GetSceneItemProperties(scene_name, source_name))
            settings = response.getSourceSettings()
            settings["url"] = url
            self.ws.call(obs_requests.SetSceneItemProperties(scene_name, source_name, **settings))
            print(f"Updated {source_name} with URL: {url[:50]}...")
            return True
        except Exception as e:
            print(f"Error updating source: {e}")
            return False

    def reload_browser_source(self, scene_name: str, source_name: str) -> bool:
        """Refresh a browser source."""
        try:
            self.ws.call(obs_requests.RefreshBrowserSource(scene_name, source_name))
            print(f"Refreshed browser source: {source_name}")
            return True
        except Exception as e:
            print(f"Error refreshing source: {e}")
            return False

    def get_source_settings(self, scene_name: str, source_name: str) -> dict:
        """Get settings for a source."""
        try:
            response = self.ws.call(obs_requests.GetSceneItemProperties(scene_name, source_name))
            return response.getSourceSettings()
        except Exception as e:
            print(f"Error getting source settings: {e}")
            return {}


def main():
    parser = argparse.ArgumentParser(description="Control OBS from command line")
    parser.add_argument("--host", default="localhost", help="OBS WebSocket host (default: localhost)")
    parser.add_argument("--port", type=int, default=4455, help="OBS WebSocket port (default: 4455)")
    parser.add_argument("--password", default="", help="OBS WebSocket password")
    parser.add_argument(
        "--action",
        choices=["status", "list-scenes", "set-scene", "start", "stop", "set-url", "refresh"],
        help="Action to perform",
    )
    parser.add_argument("--scene", help="Scene name for set-scene or set-url actions")
    parser.add_argument("--source", help="Source name for set-url or refresh actions")
    parser.add_argument("--url", help="URL for set-url action")

    args = parser.parse_args()

    controller = OBSController(args.host, args.port, args.password)
    if not controller.connect():
        return 1

    try:
        if args.action == "status":
            status = controller.get_streaming_status()
            print(json.dumps(status, indent=2))

        elif args.action == "list-scenes":
            scenes = controller.get_scenes()
            print("Available scenes:")
            for scene in scenes:
                print(f"  - {scene}")

        elif args.action == "set-scene":
            if not args.scene:
                print("ERROR: --scene required for set-scene action")
                return 1
            controller.set_scene(args.scene)

        elif args.action == "start":
            controller.start_streaming()

        elif args.action == "stop":
            controller.stop_streaming()

        elif args.action == "set-url":
            if not args.scene or not args.source or not args.url:
                print("ERROR: --scene, --source, and --url required for set-url action")
                return 1
            controller.set_source_url(args.scene, args.source, args.url)

        elif args.action == "refresh":
            if not args.scene or not args.source:
                print("ERROR: --scene and --source required for refresh action")
                return 1
            controller.reload_browser_source(args.scene, args.source)

        else:
            print("Please specify an --action")
            return 1

        return 0

    finally:
        controller.disconnect()


if __name__ == "__main__":
    sys.exit(main())
