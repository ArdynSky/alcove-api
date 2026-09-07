import io
import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ["ALCOVE_STATE_DB_PATH"] = os.path.join(_temp_dir.name, "state.db")
os.environ["ALCOVE_RUNTIME_STATE_PATH"] = os.path.join(_temp_dir.name, "runtime.json")
os.environ["FEATURE_FLAGS_PATH"] = os.path.join(_temp_dir.name, "feature_flags.json")
os.environ["PULSE_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "pulse_settings.json")
os.environ["SAFETY_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "safety_settings.json")
os.environ["VERIFY_FLOW_LOG_PATH"] = os.path.join(_temp_dir.name, "verification_flow_events.jsonl")
os.environ["HOMEPAGE_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "homepage_settings.json")
os.environ["HOMEPAGE_MEDIA_DIR"] = os.path.join(_temp_dir.name, "homepage-media")
os.environ["BOT_SYNC_SECRET"] = "test-admin-secret"

from fastapi.testclient import TestClient

from api import homepage_settings, main


class HomepageSettingsTests(unittest.TestCase):
    def setUp(self):
        settings_path = Path(os.environ["HOMEPAGE_SETTINGS_PATH"])
        media_dir = Path(os.environ["HOMEPAGE_MEDIA_DIR"])
        if settings_path.exists():
            settings_path.unlink()
        media_dir.mkdir(parents=True, exist_ok=True)
        for child in media_dir.iterdir():
            if child.is_file():
                child.unlink()
        main.BOT_SYNC_SECRET = "test-admin-secret"
        self.client = TestClient(main.app)

    def test_get_defaults_without_saved_file(self):
        response = self.client.get("/api/homepage-settings")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        settings = payload["settings"]
        self.assertEqual(settings["background_video_url"], "")
        self.assertEqual(settings["fallback_image_url"], "")
        self.assertEqual(settings["logo_url"], "")
        self.assertEqual(settings["tiles"]["live_room"]["cta"], "LET'S GO!")
        self.assertEqual(settings["tiles"]["live_room"]["preview_opacity"], 50)
        self.assertEqual(settings["tiles"]["archive"]["info_title"], "ARCHIVE")

    def test_post_round_trip(self):
        body = {
            "admin_secret": "test-admin-secret",
            "background_video_url": "https://example.com/bg.mp4",
            "fallback_image_url": "https://example.com/bg.jpg",
            "logo_url": "https://example.com/logo.png",
            "tiles": {
                "live_room": {
                    "preview_video_url": "https://example.com/live.mp4",
                    "preview_opacity": 75,
                    "cta": "LET'S GO!",
                    "info_title": "LIVE ROOM",
                    "info_description": "Custom live room copy.",
                },
                "profile": {
                    "preview_video_url": "",
                    "preview_opacity": 25,
                    "cta": "LET'S GO!",
                    "info_title": "PROFILE",
                    "info_description": "Custom profile copy.",
                },
                "connect": {
                    "preview_video_url": "",
                    "preview_opacity": 99,
                    "cta": "ENTER",
                    "info_title": "CONNECT",
                    "info_description": "Custom connect copy.",
                },
                "archive": {
                    "preview_video_url": "",
                    "cta": "LET'S GO!",
                    "info_title": "ARCHIVE",
                    "info_description": "Custom archive copy.",
                },
            },
        }
        save = self.client.post("/api/homepage-settings", json=body)
        self.assertEqual(save.status_code, 200, save.text)
        loaded = self.client.get("/api/homepage-settings").json()["settings"]
        self.assertEqual(loaded["background_video_url"], "https://example.com/bg.mp4")
        self.assertEqual(loaded["tiles"]["connect"]["cta"], "ENTER")
        self.assertEqual(loaded["tiles"]["live_room"]["info_description"], "Custom live room copy.")
        self.assertEqual(loaded["tiles"]["live_room"]["preview_opacity"], 75)
        self.assertEqual(loaded["tiles"]["profile"]["preview_opacity"], 25)
        self.assertEqual(loaded["tiles"]["connect"]["preview_opacity"], 50)
        self.assertEqual(loaded["tiles"]["archive"]["preview_opacity"], 50)

    def test_upload_rejects_unsupported_type(self):
        response = self.client.post(
            "/api/admin/homepage-media/upload",
            data={"admin_secret": "test-admin-secret", "slot": "logo"},
            files={"file": ("notes.txt", io.BytesIO(b"hello"), "text/plain")},
        )
        self.assertEqual(response.status_code, 400)

    def test_upload_and_serve_image(self):
        png_bytes = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
            b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        upload = self.client.post(
            "/api/admin/homepage-media/upload",
            data={"admin_secret": "test-admin-secret", "label": "logo", "slot": "logo"},
            files={"file": ("logo.png", io.BytesIO(png_bytes), "image/png")},
        )
        self.assertEqual(upload.status_code, 200, upload.text)
        asset = upload.json()["asset"]
        self.assertTrue(asset["url"].startswith("/api/homepage-media/"))
        served = self.client.get(asset["url"])
        self.assertEqual(served.status_code, 200)
        self.assertEqual(served.content, png_bytes)

    def test_missing_media_returns_404(self):
        response = self.client.get("/api/homepage-media/does-not-exist.mp4")
        self.assertEqual(response.status_code, 404)

    def test_url_normalization_drive(self):
        cleaned = homepage_settings._clean_url(
            "https://drive.google.com/file/d/abc123/view?usp=sharing"
        )
        self.assertEqual(cleaned, "https://drive.google.com/uc?export=view&id=abc123")


if __name__ == "__main__":
    unittest.main()
