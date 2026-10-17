import datetime
import json
import os
import tempfile
import unittest
from unittest import mock


_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ.setdefault("FOX_LOGS_DB_PATH", os.path.join(_temp_dir.name, "missing_fox_logs.db"))

from api import main


class VerificationPackLookupTests(unittest.TestCase):
    def setUp(self):
        self._verifications = list(main.miniapp_verifications)
        self._choices = dict(main.verification_pack_choices)
        self._users = list(main.synced_alcove_users)
        self._prune_day = main._last_pulse_prune_day
        self._log_path = main.VERIFY_FLOW_LOG_PATH
        self._fox_db = main.FOX_LOGS_DB_PATH
        main.miniapp_verifications[:] = []
        main.verification_pack_choices.clear()
        main.synced_alcove_users[:] = []
        main._last_pulse_prune_day = None
        main._verification_flow_pack_index["stamp"] = None
        main._verification_flow_pack_index["by_user"] = {}
        main._verification_flow_pack_index["by_name"] = {}
        self.log_dir = tempfile.TemporaryDirectory()
        main.VERIFY_FLOW_LOG_PATH = os.path.join(self.log_dir.name, "flow.jsonl")
        main.FOX_LOGS_DB_PATH = os.path.join(self.log_dir.name, "missing_fox_logs.db")

    def tearDown(self):
        main.miniapp_verifications[:] = self._verifications
        main.verification_pack_choices.clear()
        main.verification_pack_choices.update(self._choices)
        main.synced_alcove_users[:] = self._users
        main._last_pulse_prune_day = self._prune_day
        main.VERIFY_FLOW_LOG_PATH = self._log_path
        main.FOX_LOGS_DB_PATH = self._fox_db
        main._verification_flow_pack_index["stamp"] = None
        self.log_dir.cleanup()

    def test_old_completed_pack_survives_prune(self):
        main.miniapp_verifications[:] = [
            {
                "id": 1,
                "user_id": 42,
                "username": "partner",
                "status": "completed",
                "selected_pack": "Dancer",
                "requested_at": "2020-01-01T00:00:00Z",
                "completed_at": "2020-01-01T00:05:00Z",
            },
            {
                "id": 2,
                "user_id": 43,
                "username": "plain",
                "status": "completed",
                "requested_at": "2020-01-01T00:00:00Z",
                "completed_at": "2020-01-01T00:05:00Z",
            },
            {
                "id": 3,
                "user_id": 44,
                "username": "waiting",
                "status": "pending",
                "requested_at": "2020-01-01T00:00:00Z",
            },
        ]
        stats = main.prune_pulse_runtime_data(force=True)
        self.assertGreaterEqual(stats["pruned_verifications"], 1)
        kept = {entry["id"] for entry in main.miniapp_verifications}
        self.assertIn(1, kept)
        self.assertNotIn(2, kept)
        self.assertIn(3, kept)
        self.assertEqual(main.find_user_verification_pack_key(42, None), "dancer")
        main.retain_verification_pack_choices()
        main.miniapp_verifications[:] = [entry for entry in main.miniapp_verifications if entry["id"] != 1]
        self.assertEqual(main.verification_pack_choices["42"]["pack_key"], "dancer")
        self.assertEqual(main.find_user_verification_pack_key(42, None), "dancer")

    def test_recent_completion_without_pack_is_kept(self):
        today = datetime.datetime.now(main.UK_TZ).strftime("%Y-%m-%dT12:00:00Z")
        main.miniapp_verifications[:] = [{
            "id": 8,
            "user_id": 8,
            "username": "new",
            "status": "completed",
            "requested_at": today,
            "completed_at": today,
        }]
        main.prune_pulse_runtime_data(force=True)
        self.assertEqual([entry["id"] for entry in main.miniapp_verifications], [8])

    def test_synced_fox_form_is_the_pack_when_verification_row_is_gone(self):
        main.synced_alcove_users[:] = [{
            "user_id": 99,
            "username": "sam",
            "fox_form": "pup",
            "verified_at": "2026-10-01T00:00:00Z",
        }]
        self.assertEqual(main.find_user_verification_pack_key(99, None), "pup")
        self.assertEqual(main.find_user_verification_pack_key(None, "@sam"), "pup")
        with mock.patch.object(main, "load_reward_catalog", return_value={
            "verification_packs_active": True,
            "verification_packs": main.normalize_verification_packs({}),
            "verification_packs_activated_at": "2026-10-17T00:50:41Z",
        }):
            result = main.verification_pack_eligibility(99, "sam")
        self.assertTrue(result["eligible"])
        self.assertEqual(result["pack_key"], "pup")

    def test_flow_log_recovers_a_pruned_choice(self):
        with open(main.VERIFY_FLOW_LOG_PATH, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({
                "user_id": 77,
                "username": "jamie",
                "selected_pack": None,
                "event": "step",
            }) + "\n")
            handle.write(json.dumps({
                "user_id": 77,
                "username": "jamie",
                "selected_pack": "Biker",
                "event": "api_verification_submitted",
            }) + "\n")
        self.assertEqual(main.find_user_verification_pack_key(77, None), "biker")
        self.assertEqual(main.verification_pack_choices["77"]["pack_key"], "biker")

    def test_unrecognised_label_does_not_grant_a_pack(self):
        main.synced_alcove_users[:] = [{
            "user_id": 5,
            "username": "nope",
            "fox_form": "biker pack",
            "verified_at": "2026-10-01T00:00:00Z",
        }]
        self.assertIsNone(main.find_user_verification_pack_key(5, "nope"))


if __name__ == "__main__":
    unittest.main()
