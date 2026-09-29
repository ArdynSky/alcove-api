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
from api.main import AdminPulseQuestionAction


class PulseSuggestionDuplicateIdTests(unittest.TestCase):
    def setUp(self):
        self.original_suggestions = list(main.pulse_question_suggestions)
        self.original_save = main.save_runtime_state
        self.original_cancel = main.cancel_pending_pulse_question_review_notifications
        self.original_secret = main.verify_admin_secret
        main.pulse_question_suggestions[:] = []
        main.save_runtime_state = lambda *args, **kwargs: True
        main.cancel_pending_pulse_question_review_notifications = lambda *args, **kwargs: None
        main.verify_admin_secret = lambda *args, **kwargs: None

    def tearDown(self):
        main.pulse_question_suggestions[:] = self.original_suggestions
        main.save_runtime_state = self.original_save
        main.cancel_pending_pulse_question_review_notifications = self.original_cancel
        main.verify_admin_secret = self.original_secret

    def _row(self, sid, status, submitted_at, question="Q?"):
        return {
            "id": sid,
            "pool": "green",
            "category": "General",
            "question": question,
            "edited_question": None,
            "submitted_at": submitted_at,
            "day_key": "2026-09-28",
            "user_id": 1,
            "username": "tester",
            "display_name": "Tester",
            "status": status,
            "schedule_mode": "tomorrow",
            "needs_admin_notify": False,
            "review_message_sent": False,
            "reviewed_at": None,
            "reviewed_by": None,
            "active_from_day_key": None,
        }

    def test_next_id_uses_max_plus_one_not_list_length(self):
        # Simulate prune shrinking the list while high IDs remain.
        main.pulse_question_suggestions[:] = [
            self._row(10, "approved", "2026-09-01T00:00:00"),
            self._row(133, "pending_review", "2026-09-24T12:00:00"),
        ]
        self.assertEqual(main.next_pulse_question_suggestion_id(), 134)

    def test_find_prefers_pending_over_deleted_duplicate(self):
        main.pulse_question_suggestions[:] = [
            self._row(133, "deleted", "2026-07-29T10:00:00", question="Old deleted"),
            self._row(133, "pending_review", "2026-09-24T12:00:00", question="Still pending"),
        ]
        found = main.find_pulse_question_suggestion(133)
        self.assertIsNotNone(found)
        self.assertEqual(found.get("status"), "pending_review")
        self.assertEqual(found.get("question"), "Still pending")

    def test_admin_delete_clears_all_pending_reserved_twins(self):
        main.pulse_question_suggestions[:] = [
            self._row(133, "deleted", "2026-07-29T10:00:00", question="Ghost"),
            self._row(133, "pending_review", "2026-09-20T09:00:00", question="Pending A"),
            self._row(133, "pending_review", "2026-09-24T12:00:00", question="Pending B"),
            self._row(133, "reserved", "2026-09-25T08:00:00", question="Reserved twin"),
            self._row(129, "pending_review", "2026-09-24T15:00:00", question="Other id"),
        ]
        result = main.admin_pulse_question_action(
            133,
            AdminPulseQuestionAction(admin_secret="test", action="delete"),
        )
        self.assertEqual(result.get("status"), "ok")
        statuses = {
            (entry.get("question"), entry.get("status"))
            for entry in main.pulse_question_suggestions
            if int(entry.get("id") or 0) == 133
        }
        self.assertEqual(
            statuses,
            {
                ("Ghost", "deleted"),
                ("Pending A", "deleted"),
                ("Pending B", "deleted"),
                ("Reserved twin", "deleted"),
            },
        )
        other = main.find_pulse_question_suggestion(129)
        self.assertEqual(other.get("status"), "pending_review")

    def test_admin_delete_no_longer_stuck_on_already_deleted_ghost(self):
        """Regression: second delete must clear the live twin, not re-hit the ghost."""
        main.pulse_question_suggestions[:] = [
            self._row(133, "deleted", "2026-07-29T10:00:00", question="Ghost"),
            self._row(133, "pending_review", "2026-09-24T12:00:00", question="Live"),
        ]
        first = main.admin_pulse_question_action(
            133,
            AdminPulseQuestionAction(admin_secret="test", action="delete"),
        )
        self.assertEqual(first.get("status"), "ok")
        live_after = [
            entry for entry in main.pulse_question_suggestions
            if int(entry.get("id") or 0) == 133
            and (entry.get("status") or "") == "pending_review"
        ]
        self.assertEqual(live_after, [])


if __name__ == "__main__":
    unittest.main()
