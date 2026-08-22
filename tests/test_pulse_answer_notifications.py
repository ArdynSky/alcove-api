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

from api import main


class PulseAnswerNotificationTests(unittest.TestCase):
    def setUp(self):
        self.original_entries = list(main.pulse_entries)
        self.original_receipts = list(main.pulse_receipts)
        self.original_alerts = list(main.pulse_red_unlock_notifications)
        self.original_verify = main.verify_bot_sync_secret
        self.original_save = main.save_runtime_state
        main.pulse_entries[:] = []
        main.pulse_receipts[:] = []
        main.pulse_red_unlock_notifications[:] = []
        main.verify_bot_sync_secret = lambda *args, **kwargs: None
        main.save_runtime_state = lambda *args, **kwargs: True

    def tearDown(self):
        main.pulse_entries[:] = self.original_entries
        main.pulse_receipts[:] = self.original_receipts
        main.pulse_red_unlock_notifications[:] = self.original_alerts
        main.verify_bot_sync_secret = self.original_verify
        main.save_runtime_state = self.original_save

    def add_answer(self, answer_id):
        main.pulse_entries.append({
            "id": answer_id,
            "pulse_type": "green",
            "question": f"Question {answer_id}",
            "response_answer": f"Answer {answer_id}",
            "delivery_mode": "question_answer",
            "status": "completed",
        })
        main.pulse_receipts.append({
            "id": answer_id,
            "pulse_id": answer_id,
            "recipient_user_id": 42,
            "notify_after": "2000-01-01T00:00:00",
        })

    def pending_ids(self):
        result = main.bot_pending_pulse_notifications("test-secret")
        return [row["notification_id"] for row in result["notifications"]]

    def test_same_answer_is_handed_off_only_once_without_acknowledgement(self):
        self.add_answer(101)

        self.assertEqual(self.pending_ids(), ["receipt-101"])
        self.assertEqual(self.pending_ids(), [])
        self.assertIsNotNone(main.pulse_receipts[0].get("notification_handed_off_at"))

    def test_new_answer_still_creates_a_new_notification(self):
        self.add_answer(201)
        self.assertEqual(self.pending_ids(), ["receipt-201"])

        self.add_answer(202)
        self.assertEqual(self.pending_ids(), ["receipt-202"])

    def test_existing_leased_receipt_is_not_replayed_after_upgrade(self):
        self.add_answer(301)
        main.pulse_receipts[0]["notification_claim_until"] = "2000-01-01T00:00:00+00:00"

        self.assertEqual(self.pending_ids(), [])


if __name__ == "__main__":
    unittest.main()
