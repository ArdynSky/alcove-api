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

from api import main


class RedPulseArchiveActivationTests(unittest.TestCase):
    def setUp(self):
        self._suggestions = list(main.pulse_question_suggestions)
        self._entries = list(main.pulse_entries)
        self._activations = list(main.pulse_red_activations)
        self._save = main.save_runtime_state
        self._heat = main.pulse_heat_threshold
        main.pulse_question_suggestions[:] = []
        main.pulse_entries[:] = []
        main.pulse_red_activations[:] = []
        main.save_runtime_state = lambda *args, **kwargs: True
        main.pulse_heat_threshold = lambda: 2
        main.ensure_pulse_archive_store()
        main.pulse_archive_meta_set("red_activation_v1", "")
        with main.closing(main.sqlite3.connect(main.STATE_DB_PATH)) as conn, conn:
            conn.execute("DELETE FROM pulse_archive_questions")
            conn.execute("DELETE FROM pulse_archive_answers")
            conn.execute("DELETE FROM pulse_archive_meta WHERE meta_key = ?", ("red_activation_v1",))
            conn.commit()

    def tearDown(self):
        main.pulse_question_suggestions[:] = self._suggestions
        main.pulse_entries[:] = self._entries
        main.pulse_red_activations[:] = self._activations
        main.save_runtime_state = self._save
        main.pulse_heat_threshold = self._heat

    def add_red_suggestion(self, day_key, suggestion_id=7, question="Red archive probe?"):
        entry = {
            "id": suggestion_id,
            "pool": "red",
            "category": "General",
            "question": question,
            "edited_question": None,
            "submitted_at": "2026-11-14T10:00:00+00:00",
            "status": "approved",
            "active_from_day_key": day_key,
            "user_id": 99,
            "username": "host",
            "display_name": "Host",
            "source": "feature_admin",
            "reviewed_at": "2026-11-14T10:01:00+00:00",
        }
        main.pulse_question_suggestions.append(entry)
        return entry

    def member_questions(self):
        return main.build_member_pulse_archive_payload(
            viewer={"user_id": 1, "username": "member"},
            view="questions",
            page=1,
            limit=50,
        ).get("questions") or []

    def test_scheduled_red_stays_out_of_member_archive_until_unlocked(self):
        day = main.pulse_day_key()
        entry = self.add_red_suggestion(day)
        main.archive_pulse_question_from_suggestion(entry)
        self.assertEqual(self.member_questions(), [])

        # One green answer — still under the heat target.
        main.pulse_entries.append({
            "id": 1,
            "day_key": day,
            "pulse_type": "green",
            "question": "Green?",
            "status": "completed",
            "sender_user_id": 1,
            "sender_username": "member",
            "response_answer": "hello",
            "sent_at": "2026-11-14T11:00:00+00:00",
        })
        main.maybe_queue_red_pulse_unlock_notifications(day, 0, main.pulse_red_unlocked_cycles(day))
        self.assertEqual(self.member_questions(), [])

        # Second green answer unlocks Red.
        main.pulse_entries.append({
            "id": 2,
            "day_key": day,
            "pulse_type": "green",
            "question": "Green again?",
            "status": "completed",
            "sender_user_id": 2,
            "sender_username": "other",
            "response_answer": "world",
            "sent_at": "2026-11-14T11:05:00+00:00",
        })
        previous = 0
        new_cycles = main.pulse_red_unlocked_cycles(day)
        self.assertEqual(new_cycles, 1)
        main.maybe_queue_red_pulse_unlock_notifications(day, previous, new_cycles)
        questions = self.member_questions()
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0]["pool"], "red")
        self.assertEqual(questions[0]["question"], "Red archive probe?")

    def test_reconcile_removes_unactivated_red_and_keeps_activated(self):
        day = main.pulse_day_key()
        activated = self.add_red_suggestion(day, 11, "Activated red?")
        pending = self.add_red_suggestion(main.pulse_next_day_key(), 12, "Tomorrow red?")
        # Force-write both as if the old schedule path archived them early.
        main.archive_pulse_question_from_suggestion(activated, activated=True)
        with main.closing(main.sqlite3.connect(main.STATE_DB_PATH)) as conn, conn:
            conn.execute(
                """
                INSERT INTO pulse_archive_questions (
                    suggestion_id, pool, question, category, status, active_from_day_key,
                    submitted_at, reviewed_at, user_id, username, display_name, source,
                    archived_at, activated_at
                ) VALUES (?, 'red', ?, 'General', 'approved', ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    pending["id"],
                    pending["question"],
                    pending["active_from_day_key"],
                    pending["submitted_at"],
                    pending["reviewed_at"],
                    pending["user_id"],
                    pending["username"],
                    pending["display_name"],
                    pending["source"],
                    main.now_iso(),
                ),
            )
            conn.commit()
        main.pulse_entries.append({
            "id": 3,
            "day_key": day,
            "pulse_type": "green",
            "question": "Green?",
            "status": "completed",
            "sender_user_id": 1,
            "response_answer": "enough",
            "sent_at": "2026-11-14T12:00:00+00:00",
        })
        main.pulse_entries.append({
            "id": 4,
            "day_key": day,
            "pulse_type": "green",
            "question": "Green 2?",
            "status": "completed",
            "sender_user_id": 2,
            "response_answer": "heat",
            "sent_at": "2026-11-14T12:01:00+00:00",
        })
        main.pulse_archive_meta_set("red_activation_v1", "")
        with main.closing(main.sqlite3.connect(main.STATE_DB_PATH)) as conn, conn:
            conn.execute("DELETE FROM pulse_archive_meta WHERE meta_key = ?", ("red_activation_v1",))
            conn.commit()
        result = main.reconcile_red_pulse_archive_activation()
        self.assertFalse(result.get("skipped"))
        questions = self.member_questions()
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0]["question"], "Activated red?")

    def test_reconcile_keeps_historical_red_when_only_archive_answers_remain(self):
        old_day = "2026-10-01"
        historical = self.add_red_suggestion(old_day, 21, "Old unlocked red?")
        with main.closing(main.sqlite3.connect(main.STATE_DB_PATH)) as conn, conn:
            conn.execute(
                """
                INSERT INTO pulse_archive_questions (
                    suggestion_id, pool, question, category, status, active_from_day_key,
                    submitted_at, reviewed_at, user_id, username, display_name, source,
                    archived_at, activated_at
                ) VALUES (?, 'red', ?, 'General', 'approved', ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                """,
                (
                    historical["id"],
                    historical["question"],
                    old_day,
                    historical["submitted_at"],
                    historical["reviewed_at"],
                    historical["user_id"],
                    historical["username"],
                    historical["display_name"],
                    historical["source"],
                    "2026-10-01T20:00:00Z",
                ),
            )
            conn.execute(
                """
                INSERT INTO pulse_archive_answers (
                    pulse_id, suggestion_id, day_key, pool, category, question, answer,
                    responded_at, sent_at, sender_user_id, sender_username, sender_display_name,
                    question_owner_user_id, question_owner_username, question_owner_display_name,
                    archived_at
                ) VALUES (9001, ?, ?, 'red', 'General', ?, 'I answered', ?, ?, 5, 'member', 'Member',
                          NULL, NULL, NULL, ?)
                """,
                (
                    historical["id"],
                    old_day,
                    historical["question"],
                    "2026-10-01T18:00:00Z",
                    "2026-10-01T18:00:00Z",
                    "2026-10-01T20:00:00Z",
                ),
            )
            conn.commit()
        # Runtime retention already pruned live evidence for that day.
        main.pulse_entries[:] = []
        main.pulse_red_activations[:] = []
        with main.closing(main.sqlite3.connect(main.STATE_DB_PATH)) as conn, conn:
            conn.execute("DELETE FROM pulse_archive_meta WHERE meta_key = ?", ("red_activation_v1",))
            conn.commit()
        result = main.reconcile_red_pulse_archive_activation()
        self.assertFalse(result.get("skipped"))
        self.assertEqual(result.get("activated"), 1)
        questions = self.member_questions()
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0]["question"], "Old unlocked red?")

    def test_scheduling_red_after_unlock_archives_immediately(self):
        day = main.pulse_day_key()
        main.pulse_entries.extend([
            {
                "id": 31,
                "day_key": day,
                "pulse_type": "green",
                "question": "Green A?",
                "status": "completed",
                "sender_user_id": 1,
                "response_answer": "one",
                "sent_at": "2026-11-14T12:00:00+00:00",
            },
            {
                "id": 32,
                "day_key": day,
                "pulse_type": "green",
                "question": "Green B?",
                "status": "completed",
                "sender_user_id": 2,
                "response_answer": "two",
                "sent_at": "2026-11-14T12:01:00+00:00",
            },
        ])
        self.assertEqual(main.pulse_red_unlocked_cycles(day), 1)
        entry = self.add_red_suggestion(day, 33, "Late scheduled red?")
        main.archive_pulse_question_from_suggestion(entry)
        questions = self.member_questions()
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0]["question"], "Late scheduled red?")


if __name__ == "__main__":
    unittest.main()
