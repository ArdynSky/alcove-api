import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")
os.environ.setdefault("LIVE_ENTRY_SECRET", "test-live-entry-secret")
ADMIN_SECRET = os.environ["BOT_SYNC_SECRET"]

from fastapi.testclient import TestClient

from api import agendas, live_entry_code, main


class LiveEntryCodeTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        agendas.DB_PATH = self.root / "agendas.sqlite3"
        live_entry_code.DB_PATH = self.root / "live_entry.sqlite3"
        self.client = TestClient(main.app)
        self.admin = {"admin_secret": ADMIN_SECRET}

    def _activate(self, title="Friday Live VC", event_date="2026-11-20"):
        return agendas.create_agenda(
            agendas.AgendaPayload(
                title=title,
                event_date=event_date,
                start_time="19:00",
                timezone="Europe/London",
                status="active",
            )
        )

    def test_going_live_mints_four_digit_code(self):
        created = self._activate()
        payload = self.client.get("/api/live-entry/code").json()
        self.assertTrue(payload["is_live"])
        self.assertEqual(payload["agenda_id"], created["id"])
        self.assertRegex(payload["code"], r"^\d{4}$")

    def test_verify_issues_session_token(self):
        self._activate()
        code = self.client.get("/api/live-entry/code").json()["code"]
        bad = self.client.post("/api/live-entry/verify", json={"code": "0000" if code != "0000" else "1111"})
        self.assertEqual(bad.status_code, 403)
        ok = self.client.post("/api/live-entry/verify", json={"code": code})
        self.assertEqual(ok.status_code, 200)
        token = ok.json()["token"]
        self.assertTrue(token)
        session = self.client.get("/api/live-entry/session", params={"token": token})
        self.assertEqual(session.status_code, 200)
        self.assertTrue(session.json()["ok"])

    def test_rotate_requires_host_auth_and_ending_live_clears_code(self):
        created = self._activate()
        first = self.client.get("/api/live-entry/code").json()["code"]
        denied = self.client.post("/api/live-entry/rotate")
        self.assertEqual(denied.status_code, 403)
        rotated = self.client.post("/api/live-entry/rotate", json=self.admin).json()["code"]
        self.assertRegex(rotated, r"^\d{4}$")
        self.assertNotEqual(first, rotated)
        agendas.set_status(created["id"], agendas.AgendaStatusPayload(status="finished"))
        offline = self.client.get("/api/live-entry/code").json()
        self.assertFalse(offline["is_live"])
        self.assertIsNone(offline["code"])

    def test_session_does_not_carry_across_agendas(self):
        first = self._activate(title="First", event_date="2026-11-20")
        code = self.client.get("/api/live-entry/code").json()["code"]
        token = self.client.post("/api/live-entry/verify", json={"code": code}).json()["token"]
        agendas.set_status(first["id"], agendas.AgendaStatusPayload(status="finished"))
        self._activate(title="Second", event_date="2026-11-21")
        stale = self.client.get("/api/live-entry/session", params={"token": token})
        self.assertEqual(stale.status_code, 401)

    def test_ensure_for_agenda_is_stable_under_lock(self):
        created = self._activate()
        a = live_entry_code.ensure_for_agenda(created["id"])
        b = live_entry_code.ensure_for_agenda(created["id"])
        self.assertEqual(a, b)
        self.assertRegex(a, r"^\d{4}$")


if __name__ == "__main__":
    unittest.main()
