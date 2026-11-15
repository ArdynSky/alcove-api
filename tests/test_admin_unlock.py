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
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")
ADMIN_SECRET = os.environ["BOT_SYNC_SECRET"]

from fastapi.testclient import TestClient

from api import main


class AdminUnlockTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)

    def test_unlock_rejects_bad_secret(self):
        bad = self.client.post("/api/admin/unlock", json={"admin_secret": "nope"})
        self.assertEqual(bad.status_code, 403)

    def test_unlock_accepts_admin_secret(self):
        ok = self.client.post("/api/admin/unlock", json={"admin_secret": ADMIN_SECRET})
        self.assertEqual(ok.status_code, 200)
        self.assertTrue(ok.json().get("ok"))


if __name__ == "__main__":
    unittest.main()
