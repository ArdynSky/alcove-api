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


class MiniappVerificationUpsertTests(unittest.TestCase):
    def setUp(self):
        self.original_verifications = list(main.miniapp_verifications)
        main.miniapp_verifications[:] = []

    def tearDown(self):
        main.miniapp_verifications[:] = self.original_verifications

    def test_failed_verification_retry_reuses_existing_record(self):
        existing = {
            "id": 7,
            "user_id": 12345,
            "username": "old_name",
            "first_name": "Old",
            "last_name": "User",
            "display_name": "Old User",
            "status": "failed",
            "requested_at": "2026-07-01T00:00:00",
            "last_seen_at": "2026-07-01T00:00:00",
            "completed_at": "2026-07-01T00:01:00",
            "detail": "join request missing",
        }
        main.miniapp_verifications.append(existing)

        entry = main.upsert_miniapp_verification({
            "id": 12345,
            "username": "new_name",
            "first_name": "New",
            "last_name": "User",
        })

        self.assertIs(entry, existing)
        self.assertEqual(len(main.miniapp_verifications), 1)
        self.assertEqual(entry["id"], 7)
        self.assertEqual(entry["status"], "pending")
        self.assertIsNone(entry["completed_at"])
        self.assertNotIn("detail", entry)
        self.assertEqual(entry["username"], "new_name")
        self.assertEqual(entry["display_name"], "New User")
        self.assertNotEqual(entry["requested_at"], "2026-07-01T00:00:00")

    def test_completed_verification_resubmit_stays_completed(self):
        existing = {
            "id": 9,
            "user_id": 98765,
            "username": "member",
            "first_name": "Verified",
            "last_name": "Member",
            "display_name": "Verified Member",
            "status": "completed",
            "requested_at": "2026-07-02T00:00:00",
            "last_seen_at": "2026-07-02T00:00:00",
            "completed_at": "2026-07-02T00:01:00",
            "detail": "access_processed",
        }
        main.miniapp_verifications.append(existing)

        entry = main.upsert_miniapp_verification({
            "id": 98765,
            "username": "member_new",
            "first_name": "Verified",
            "last_name": "Member",
        })

        self.assertIs(entry, existing)
        self.assertEqual(len(main.miniapp_verifications), 1)
        self.assertEqual(entry["status"], "completed")
        self.assertEqual(entry["requested_at"], "2026-07-02T00:00:00")
        self.assertEqual(entry["completed_at"], "2026-07-02T00:01:00")
        self.assertEqual(entry["detail"], "access_processed")
        self.assertEqual(entry["username"], "member_new")


if __name__ == "__main__":
    unittest.main()
