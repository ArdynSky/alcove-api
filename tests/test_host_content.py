import os
import tempfile
import unittest

_temp_dir = tempfile.TemporaryDirectory()
os.environ["HOST_CONTENT_PATH"] = os.path.join(_temp_dir.name, "host_content.json")
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ.setdefault("ALCOVE_LIVE_ROOM_STATE_PATH", os.path.join(_temp_dir.name, "live_room_state.json"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")  # pragma: allowlist secret
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")  # pragma: allowlist secret

from fastapi.testclient import TestClient

from api import main
from api import host_content

ADMIN_SECRET = os.environ["BOT_SYNC_SECRET"]


class HostContentTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        path = host_content._store_path()
        if path.exists():
            path.unlink()

    def test_requires_admin_secret(self):
        denied = self.client.get("/api/host-content")
        self.assertEqual(denied.status_code, 403)
        put = self.client.put("/api/host-content", json={"admin_secret": "nope", "items": []})
        self.assertEqual(put.status_code, 403)

    def test_put_get_round_trip_and_dedupe(self):
        items = [
            {"id": "a1", "kind": "debate", "title_2": "Pineapple?"},
            {"id": "d1", "kind": "discussion", "title_2": "Best tip?"},
            {"id": "a1", "kind": "debate", "title_2": "Duplicate id ignored"},
            {"id": "", "kind": "custom"},
            "skip-me",
        ]
        saved = self.client.put(
            "/api/host-content",
            json={"admin_secret": ADMIN_SECRET, "items": items},
        )
        self.assertEqual(saved.status_code, 200)
        payload = saved.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual([row["id"] for row in payload["items"]], ["a1", "d1"])
        self.assertEqual(payload["items"][0]["title_2"], "Pineapple?")

        loaded = self.client.get("/api/host-content", params={"admin_secret": ADMIN_SECRET})
        self.assertEqual(loaded.status_code, 200)
        self.assertEqual(loaded.json()["items"], payload["items"])
        self.assertTrue(host_content._store_path().exists())

    def test_empty_library_when_missing_file(self):
        loaded = self.client.get("/api/host-content", params={"admin_secret": ADMIN_SECRET})
        self.assertEqual(loaded.status_code, 200)
        self.assertEqual(loaded.json()["items"], [])


if __name__ == "__main__":
    unittest.main()
