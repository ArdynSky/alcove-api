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
os.environ["BOT_SYNC_SECRET"] = "test-admin-secret"
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from fastapi.testclient import TestClient

from api import main


GREEN_QUESTION = "What kind of day have you really been having?"
RED_QUESTION = main.PULSE_RED_DAILY_QUESTION_DEFAULT


class RedPulseContributionTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self._entries = list(main.pulse_entries)
        self._receipts = list(main.pulse_receipts)
        self._alerts = list(main.pulse_red_unlock_notifications)
        self._questions = list(main.PULSE_QUESTIONS.get("green") or [])
        self._dms = main.PULSE_RED_UNLOCK_DMS_ENABLED
        self._notify_raw = main.PULSE_RED_UNLOCK_NOTIFY_USERNAMES_RAW
        self._heat = main.pulse_heat_threshold
        self._save = main.save_runtime_state
        self._verified = main.get_verified_alcove_users
        main.pulse_entries[:] = []
        main.pulse_receipts[:] = []
        main.pulse_red_unlock_notifications[:] = []
        main.PULSE_QUESTIONS["green"] = [GREEN_QUESTION]
        main.PULSE_RED_UNLOCK_DMS_ENABLED = True
        main.PULSE_RED_UNLOCK_NOTIFY_USERNAMES_RAW = "*"
        main.pulse_heat_threshold = lambda: 1
        main.save_runtime_state = lambda *args, **kwargs: True
        main.get_verified_alcove_users = lambda: [
            {"user_id": 11, "username": "contributor", "display_name": "Contributor", "verified_at": "2026-01-01"},
            {"user_id": 22, "username": "lurker", "display_name": "Lurker", "verified_at": "2026-01-01"},
        ]

    def tearDown(self):
        main.pulse_entries[:] = self._entries
        main.pulse_receipts[:] = self._receipts
        main.pulse_red_unlock_notifications[:] = self._alerts
        main.PULSE_QUESTIONS["green"] = self._questions
        main.PULSE_RED_UNLOCK_DMS_ENABLED = self._dms
        main.PULSE_RED_UNLOCK_NOTIFY_USERNAMES_RAW = self._notify_raw
        main.pulse_heat_threshold = self._heat
        main.save_runtime_state = self._save
        main.get_verified_alcove_users = self._verified

    def add_answer(self, user_id, pulse_type="green", entry_id=1, username=None):
        main.pulse_entries.append({
            "id": entry_id,
            "day_key": main.pulse_day_key(),
            "pulse_type": pulse_type,
            "question": GREEN_QUESTION if pulse_type == "green" else RED_QUESTION,
            "status": "completed",
            "sender_user_id": user_id,
            "sender_username": username or f"user{user_id}",
            "response_answer": "This is a real answer.",
        })

    def test_red_stays_locked_for_members_who_have_not_answered_today(self):
        self.add_answer(11, "green", 1, "contributor")
        slots = main.pulse_slot_state(22, "lurker")
        self.assertTrue(slots["community_red_unlocked"])
        self.assertTrue(slots["red_unlocked"])
        self.assertFalse(slots["contributed_today"])
        self.assertFalse(slots["red_eligible"])
        self.assertEqual(slots["red_available"], 0)
        self.assertFalse(slots["red_ready"])

    def test_red_opens_after_one_green_answer(self):
        self.add_answer(11, "green", 1, "contributor")
        slots = main.pulse_slot_state(11, "contributor")
        self.assertTrue(slots["contributed_today"])
        self.assertTrue(slots["red_eligible"])
        self.assertEqual(slots["red_available"], 1)
        self.assertTrue(slots["red_ready"])

    def test_activate_rejects_without_a_green_answer(self):
        self.add_answer(11, "green", 1, "contributor")
        result = self.client.post("/api/pulse-red-activate", json={"user_id": 22, "username": "lurker"})
        self.assertEqual(result.status_code, 200)
        body = result.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["message"], main.RED_PULSE_CONTRIBUTE_MESSAGE)

    def test_activate_allows_contributor(self):
        self.add_answer(11, "green", 1, "contributor")
        result = self.client.post("/api/pulse-red-activate", json={"user_id": 11, "username": "contributor"})
        self.assertEqual(result.status_code, 200)
        body = result.json()
        self.assertEqual(body["status"], "ok")
        self.assertTrue(body["slots"]["red_available"])

    def test_red_submit_rejected_without_green_answer(self):
        self.add_answer(11, "green", 1, "contributor")
        result = self.client.post("/api/pulse-entry", json={
            "user_id": 22,
            "username": "lurker",
            "pulse_type": "red",
            "question": RED_QUESTION,
            "answer": "I should not be able to post this yet.",
        })
        self.assertEqual(result.status_code, 200)
        body = result.json()
        self.assertEqual(body["status"], "error")
        self.assertEqual(body["message"], main.RED_PULSE_CONTRIBUTE_MESSAGE)

    def test_red_submit_allowed_after_green_answer(self):
        self.add_answer(11, "green", 1, "contributor")
        result = self.client.post("/api/pulse-entry", json={
            "user_id": 11,
            "username": "contributor",
            "pulse_type": "red",
            "question": RED_QUESTION,
            "answer": "Now I have earned the Red Pulse.",
        })
        self.assertEqual(result.status_code, 200)
        body = result.json()
        self.assertEqual(body["status"], "ok")
        self.assertEqual(body["slots"]["red_used"], 1)
        self.assertTrue(body["slots"]["contributed_today"])

    def test_unlock_dm_only_goes_to_contributors(self):
        self.add_answer(11, "green", 1, "contributor")
        main.queue_red_pulse_unlock_notifications(main.pulse_day_key(), 1)
        recipients = {row["recipient_user_id"] for row in main.pulse_red_unlock_notifications}
        self.assertEqual(recipients, {11})

    def test_late_green_answer_queues_personal_unlock_dm(self):
        self.add_answer(99, "green", 1, "other")
        result = self.client.post("/api/pulse-entry", json={
            "user_id": 22,
            "username": "lurker",
            "pulse_type": "green",
            "question": GREEN_QUESTION,
            "answer": "I showed up after the bar filled.",
        })
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.json()["status"], "ok")
        recipients = {row["recipient_user_id"] for row in main.pulse_red_unlock_notifications}
        self.assertIn(22, recipients)


if __name__ == "__main__":
    unittest.main()
