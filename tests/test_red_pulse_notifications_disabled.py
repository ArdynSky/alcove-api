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


class RedPulseNotificationPauseTests(unittest.TestCase):
    def setUp(self):
        self.original_enabled = main.PULSE_RED_UNLOCK_DMS_ENABLED
        self.original_alerts = list(main.pulse_red_unlock_notifications)
        self.original_users = main.get_verified_alcove_users
        main.PULSE_RED_UNLOCK_DMS_ENABLED = False
        main.pulse_red_unlock_notifications[:] = []
        main.get_verified_alcove_users = lambda: [{"user_id": 1, "username": "Ardyn_Sky"}]

    def tearDown(self):
        main.PULSE_RED_UNLOCK_DMS_ENABLED = self.original_enabled
        main.pulse_red_unlock_notifications[:] = self.original_alerts
        main.get_verified_alcove_users = self.original_users

    def test_unlock_does_not_queue_any_direct_messages(self):
        main.queue_red_pulse_unlock_notifications("2026-08-22", 1)
        self.assertEqual(main.pulse_red_unlock_notifications, [])

    def test_existing_unsent_unlocks_are_not_returned_to_fox(self):
        main.pulse_red_unlock_notifications.append({
            "notification_id": "red-unlock-1",
            "recipient_user_id": 1,
            "recipient_username": "Ardyn_Sky",
            "notified_at": None,
        })
        original_verify = main.verify_bot_sync_secret
        main.verify_bot_sync_secret = lambda *_: None
        try:
            result = main.bot_pending_pulse_notifications("test")
        finally:
            main.verify_bot_sync_secret = original_verify
        self.assertFalse(any(row.get("kind") == "red_pulse_active" for row in result["notifications"]))


if __name__ == "__main__":
    unittest.main()
