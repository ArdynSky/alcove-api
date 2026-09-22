import unittest
from unittest import mock

from api import main


class RuntimeMemoryHelpersTest(unittest.TestCase):
    def setUp(self):
        self._unlock = list(main.pulse_red_unlock_notifications)
        self._review = list(main.pulse_question_review_notifications)
        self._users = list(main.synced_alcove_users)
        self._fingerprint = main._last_saved_runtime_fingerprint
        main.pulse_red_unlock_notifications[:] = []
        main.pulse_question_review_notifications[:] = []
        main.synced_alcove_users[:] = []

    def tearDown(self):
        main.pulse_red_unlock_notifications[:] = self._unlock
        main.pulse_question_review_notifications[:] = self._review
        main.synced_alcove_users[:] = self._users
        main._last_saved_runtime_fingerprint = self._fingerprint

    def test_prune_notification_queue_keeps_pending_and_trims_done(self):
        items = [{"id": i, "notified_at": None} for i in range(5)]
        items.extend({"id": 100 + i, "notified_at": "2026-01-01T00:00:00Z"} for i in range(200))
        removed = main.prune_notification_queue(items, 40)
        self.assertGreater(removed, 0)
        pending = [row for row in items if not row.get("notified_at")]
        done = [row for row in items if row.get("notified_at")]
        self.assertEqual(len(pending), 5)
        self.assertLessEqual(len(items), 40)
        self.assertLessEqual(len(done), 35)

    def test_slim_synced_users_strips_extra_fields_in_lean_mode(self):
        with mock.patch.object(main, "lean_mode_enabled", return_value=True):
            slim = main.slim_synced_user(
                {
                    "user_id": 1,
                    "username": "ardyn",
                    "display_name": "Ardyn",
                    "verified_at": "2026-01-01T00:00:00Z",
                    "huge_blob": "x" * 5000,
                    "nested": {"a": 1},
                }
            )
        self.assertEqual(slim.get("user_id"), 1)
        self.assertNotIn("huge_blob", slim)
        self.assertNotIn("nested", slim)

    def test_flush_runtime_state_keeps_short_fingerprint(self):
        main.synced_alcove_users[:] = [
            {
                "user_id": 7,
                "username": "tester",
                "display_name": "Tester",
                "verified_at": "2026-01-01T00:00:00Z",
            }
        ]
        with mock.patch.object(main, "prune_pulse_runtime_data", return_value={}):
            with mock.patch.object(main, "save_runtime_state_to_db") as save_db:
                with mock.patch("builtins.open", mock.mock_open()) as mocked_open:
                    with mock.patch.object(main.os, "replace"):
                        with mock.patch.object(main.os, "makedirs"):
                            wrote = main._flush_runtime_state(force=True)
        self.assertTrue(wrote)
        self.assertEqual(len(main._last_saved_runtime_fingerprint or ""), 64)
        save_db.assert_called_once()
        kwargs = save_db.call_args.kwargs
        self.assertIn("serialized", kwargs)
        self.assertIsInstance(kwargs["serialized"], str)
        mocked_open.assert_called()

        with mock.patch.object(main, "prune_pulse_runtime_data", return_value={}):
            with mock.patch.object(main, "save_runtime_state_to_db") as save_db_again:
                wrote_again = main._flush_runtime_state(force=False)
        self.assertFalse(wrote_again)
        save_db_again.assert_not_called()


if __name__ == "__main__":
    unittest.main()
