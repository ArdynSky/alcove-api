import io
import os
import tempfile
import unittest

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ.setdefault("ALCOVE_LIVE_ROOM_STATE_PATH", os.path.join(_temp_dir.name, "live_room_state.json"))
os.environ["BOT_SYNC_SECRET"] = "test-admin-secret"
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from fastapi.testclient import TestClient

from api import main


class ComposePopupBackgroundTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app, headers={"X-Bot-Sync-Secret": main.BOT_SYNC_SECRET})
        self._previous = main.state.get("compose_popup_background_filename")
        main.state["compose_popup_background_filename"] = None

    def tearDown(self):
        filename = main.state.get("compose_popup_background_filename")
        if filename:
            path = os.path.join(main.COMPOSE_POPUP_BG_DIR, filename)
            if os.path.isfile(path):
                os.remove(path)
        main.state["compose_popup_background_filename"] = self._previous

    def test_upload_serves_and_clears_compose_background(self):
        empty = self.client.get("/api/live-room/compose-background")
        self.assertEqual(empty.status_code, 200)
        self.assertFalse(empty.json()["has_background"])

        png = (
            b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
            b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f\x00"
            b"\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
        )
        uploaded = self.client.post(
            "/api/live-room/compose-background",
            files={"file": ("compose.png", io.BytesIO(png), "image/png")},
        )
        self.assertEqual(uploaded.status_code, 200)
        payload = uploaded.json()
        self.assertTrue(payload["has_background"])
        self.assertTrue(payload["compose_popup_background_url"].startswith("/api/live-room/compose-background/file/"))

        app_state = self.client.get("/api/app-state?view=live")
        self.assertEqual(app_state.status_code, 200)
        self.assertEqual(
            app_state.json()["compose_popup_background_url"],
            payload["compose_popup_background_url"],
        )

        media = self.client.get(payload["compose_popup_background_url"])
        self.assertEqual(media.status_code, 200)
        self.assertEqual(media.headers.get("content-type"), "image/png")

        removed = self.client.delete("/api/live-room/compose-background")
        self.assertEqual(removed.status_code, 200)
        self.assertFalse(removed.json()["has_background"])
        self.assertEqual(self.client.get("/api/app-state?view=live").json()["compose_popup_background_url"], "")


if __name__ == "__main__":
    unittest.main()
