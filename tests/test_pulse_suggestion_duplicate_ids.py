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
os.environ.setdefault("BOT_SYNC_SECRET", "test-bot-sync-secret")

from api import main


class PulseSuggestionDuplicateIdTests(unittest.TestCase):
    def setUp(self):
        self.original_suggestions = list(main.pulse_question_suggestions)
        self.original_notifications = list(main.pulse_question_review_notifications)
        self.original_save = main.save_runtime_state
        main.pulse_question_suggestions[:] = []
        main.pulse_question_review_notifications[:] = []
        main.save_runtime_state = lambda *args, **kwargs: True

    def tearDown(self):
        main.pulse_question_suggestions[:] = self.original_suggestions
        main.pulse_question_review_notifications[:] = self.original_notifications
        main.save_runtime_state = self.original_save

    def test_next_id_uses_max_plus_one_not_list_length(self):
        main.pulse_question_suggestions[:] = [
            {"id": 10, "status": "approved", "question": "older kept"},
            {"id": 50, "status": "pending_review", "question": "still pending"},
        ]
        self.assertEqual(main.next_pulse_question_suggestion_id(), 51)

    def test_find_prefers_live_pending_over_deleted_twin(self):
        main.pulse_question_suggestions[:] = [
            {
                "id": 133,
                "status": "deleted",
                "question": "old deleted twin",
                "submitted_at": "2026-07-29T16:17:53Z",
            },
            {
                "id": 133,
                "status": "pending_review",
                "question": "live pending twin",
                "submitted_at": "2026-09-24T05:31:45Z",
            },
        ]
        found = main.find_pulse_question_suggestion(133)
        self.assertIsNotNone(found)
        self.assertEqual(found.get("status"), "pending_review")
        self.assertEqual(found.get("question"), "live pending twin")

    def test_delete_clears_all_pending_duplicates_with_same_id(self):
        ghost = {
            "id": 129,
            "status": "deleted",
            "question": "ghost",
            "submitted_at": "2026-08-01T00:00:00Z",
        }
        first = {
            "id": 129,
            "status": "pending_review",
            "question": "first pending",
            "submitted_at": "2026-09-20T00:00:00Z",
        }
        second = {
            "id": 129,
            "status": "pending_review",
            "question": "second pending",
            "submitted_at": "2026-09-21T00:00:00Z",
        }
        approved = {
            "id": 129,
            "status": "approved",
            "question": "keep approved",
            "submitted_at": "2026-09-10T00:00:00Z",
            "active_from_day_key": "2026-09-11",
        }
        main.pulse_question_suggestions[:] = [ghost, first, second, approved]

        main.apply_admin_pulse_question_action(first, "delete")

        self.assertEqual(ghost.get("status"), "deleted")
        self.assertEqual(first.get("status"), "deleted")
        self.assertEqual(second.get("status"), "deleted")
        self.assertEqual(approved.get("status"), "approved")

        pending = [
            entry for entry in main.pulse_question_suggestions
            if entry.get("status") == "pending_review" and int(entry.get("id") or 0) == 129
        ]
        self.assertEqual(pending, [])


if __name__ == "__main__":
    unittest.main()
