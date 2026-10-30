import copy
import os
import tempfile
import unittest
from unittest import mock

from fastapi.testclient import TestClient
from api import main, member_progress


class BulkProgressResetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {"ALCOVE_STATE_DB_PATH": self.tmp.name + "/state.db"})
        self.env.start()
        self.secret = mock.patch.object(main, "BOT_SYNC_SECRET", "bulk-secret")
        self.secret.start()
        self.users = [{"user_id": 1, "username": "Ardyn_Sky"}, {"user_id": 2, "username": "member"}, {"user_id": 3, "username": "localonly"}]
        self.directory = mock.patch.object(main, "get_verified_alcove_users", return_value=self.users)
        self.directory.start()
        self.joined = mock.patch.object(member_progress, "resolve_member_since", return_value="2026-09-01T00:00:00+00:00")
        self.joined.start()
        self.pulses = mock.patch.object(main, "pulse_question_suggestions", [
            {"id": 10, "user_id": 2, "status": "approved", "reviewed_at": "2026-09-01T10:00:00Z"},
            {"id": 11, "user_id": 2, "status": "pending_review", "submitted_at": "2026-09-01T10:00:00Z"},
        ])
        self.pulses.start()
        self.spots = mock.patch.object(main, "spotlight_entries", [
            {"id": 20, "nominator_user_id": 2, "nominee_user_id": 1, "status": "approved", "reviewed_at": "2026-09-01T10:00:00Z"},
            {"id": 21, "nominator_user_id": 2, "nominee_user_id": 1, "status": "pending_review", "time": "2026-09-01T10:00:00Z"},
        ])
        self.spots.start()
        self.client = TestClient(main.app)
        self.profile = {"level": 12, "exp": 45, "stats": {"pulseSubmitted": 20}, "owned": {"feedSkins": ["rare"]}, "updated_at": "2026-10-01T00:00:00Z"}
        member_progress.save_profile("1", self.profile)
        member_progress.save_profile("2", self.profile)

    def tearDown(self):
        for p in (self.spots, self.pulses, self.joined, self.directory, self.secret, self.env):
            p.stop()
        self.tmp.cleanup()

    def preview(self, exclusions=None):
        return self.client.post("/api/members/progress-reset/preview", json={"admin_secret": "bulk-secret", "excluded_usernames": exclusions})

    def execute(self, preview):
        return self.client.post("/api/members/progress-reset/execute", json={"admin_secret": "bulk-secret", "preview_id": preview["preview_id"], "confirmation": "RESET PROGRESS"})

    def test_preview_exclusions_and_atomic_retry(self):
        before = member_progress.load_profile("1")
        preview = self.preview(["@ARDYN_SKY"]).json()
        self.assertEqual(preview["reset_count"], 2)
        self.assertEqual(member_progress.load_profile("1"), before)
        result = self.execute(preview)
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(member_progress.load_profile("1"), before)
        fresh = member_progress.load_profile("2")
        self.assertEqual((fresh["level"], fresh["exp"]), (1, 0))
        self.assertEqual(fresh["stats"]["pulseSubmitted"], 0)
        self.assertNotIn("rare", fresh["owned"]["feedSkins"])
        self.assertEqual(member_progress.load_profile("3")["level"], 1)
        member_progress.save_profile("2", {**fresh, "exp": 25})
        self.assertTrue(self.execute(preview).json()["already_done"])
        self.assertEqual(member_progress.load_profile("2")["exp"], 25)

    def test_auth_and_unresolved_exclusion_fail_closed(self):
        self.assertIn(self.client.post("/api/members/progress-reset/preview", json={"admin_secret": "wrong"}).status_code, [401, 403])
        self.assertEqual(self.preview(["@typo"]).status_code, 400)
        self.assertEqual(member_progress.load_profile("2")["level"], 12)

    def test_saved_exclusion_ids_survive_username_change(self):
        preview = self.preview(["@ardyn_sky"]).json()
        self.execute(preview)
        self.users[0]["username"] = "renamed"
        again = self.preview().json()
        self.assertEqual(again["excluded"][0]["user_id"], "1")

    def test_stale_replace_cannot_restore_progress(self):
        self.execute(self.preview().json())
        result = member_progress.save_profile("2", {**self.profile, "updated_at": "2099-01-01T00:00:00Z"}, replace=True)
        self.assertEqual((result["level"], result["exp"]), (1, 0))

    def test_future_browser_clock_cannot_bypass_admin_reset(self):
        stale = {**self.profile, "progressionResetToken": "4070908800000-future-browser-clock", "progressionResetAt": "2099-01-01T00:00:00Z"}
        member_progress.save_profile("2", stale, replace=True)
        self.execute(self.preview().json())
        fresh = member_progress.load_profile("2")
        self.assertEqual(member_progress.save_profile("2", stale, replace=True)["level"], 1)
        # A later personal reset retains the server-owned admin generation.
        personal = {**fresh, "progressionResetToken": "4070908800000-personal-reset", "exp": 3}
        self.assertEqual(member_progress.save_profile("2", personal, replace=True)["exp"], 3)

    def test_confirmation_expiry_and_changed_members_fail_closed(self):
        preview = self.preview().json()
        wrong = self.client.post("/api/members/progress-reset/execute", json={"admin_secret": "bulk-secret", "preview_id": preview["preview_id"], "confirmation": "yes"})
        self.assertEqual(wrong.status_code, 400)
        self.users.append({"user_id": 4, "username": "newmember"})
        self.assertEqual(self.execute(preview).status_code, 409)
        preview = self.preview().json()
        with mock.patch("api.progress_reset.time.time", return_value=9999999999):
            self.assertEqual(self.execute(preview).status_code, 409)
        self.assertEqual(member_progress.load_profile("2")["level"], 12)

    def test_failure_rolls_back_every_profile_and_can_retry(self):
        from api import progress_reset
        original = progress_reset.fresh_profile
        count = 0
        def fail_second(*args):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError("storage failed")
            return original(*args)
        preview = self.preview().json()
        with mock.patch.object(progress_reset, "fresh_profile", side_effect=fail_second):
            with self.assertRaises(RuntimeError):
                self.execute(preview)
        self.assertEqual(member_progress.load_profile("1")["level"], 12)
        self.assertEqual(member_progress.load_profile("2")["level"], 12)
        self.assertEqual(self.execute(preview).status_code, 200)

    def test_approval_receipts_survive_more_than_300_general_events(self):
        claims = {f"pulse-approved:{i}": True for i in range(450)}
        stored = member_progress.save_profile("2", {**self.profile, "approvalClaims": claims})
        self.assertEqual(len(stored["approvalClaims"]), 450)
        merged = member_progress.save_profile("2", {**stored, "approvalClaims": {"spotlight:1": True}})
        self.assertEqual(len(merged["approvalClaims"]), 451)

    def test_api_payloads_expose_approval_time_for_pending_before_reset(self):
        self.execute(self.preview().json())
        entry = main.pulse_question_suggestions[1]
        with mock.patch.object(main, "now_iso", return_value="2099-01-01T00:00:00Z"), mock.patch.object(main, "save_runtime_state"), mock.patch.object(main, "queue_pulse_question_review_notification"):
            main.apply_admin_pulse_question_action(entry, "tomorrow")
        data = main.pulse_owned_suggestion_payload({"user_id": 2}, entry)
        self.assertTrue(data["progression_eligible"])
        self.assertEqual(data["progression_approved_at"], "2099-01-01T00:00:00Z")
        spot = main.spotlight_entries[1]
        with mock.patch.object(main, "now_iso", return_value="2099-01-01T00:00:00Z"), mock.patch.object(main, "save_runtime_state"):
            main.admin_spotlight_action(21, main.AdminSpotlightAction(admin_secret="bulk-secret", action="approve"))
        nomination = next(row for row in main.spotlight_nominations_for_user(2) if row["spotlight_id"] == 21)
        self.assertTrue(nomination["progression_eligible"])

    def test_only_approvals_after_reset_are_eligible(self):
        self.execute(self.preview(["ardyn_sky"]).json())
        from api.progress_reset import approval_metadata
        self.assertFalse(approval_metadata("2", "pulse", main.pulse_question_suggestions[0])["progression_eligible"])
        self.assertFalse(approval_metadata("2", "spotlight", main.spotlight_entries[0])["progression_eligible"])
        for kind, entry in [("pulse", main.pulse_question_suggestions[1]), ("spotlight", main.spotlight_entries[1])]:
            self.assertFalse(approval_metadata("2", kind, entry)["progression_eligible"])
            entry.update(status="approved", reviewed_at="2099-01-01T00:00:00Z")
            self.assertTrue(approval_metadata("2", kind, entry)["progression_eligible"])
        # An old approved item re-reviewed/published later stays blocked.
        main.spotlight_entries[0].update(reviewed_at="2099-01-01T00:00:00Z", published_at="2099-01-02T00:00:00Z")
        self.assertFalse(approval_metadata("2", "spotlight", main.spotlight_entries[0])["progression_eligible"])
        # Excluded account keeps ordinary approval recovery.
        self.assertTrue(approval_metadata("1", "spotlight", main.spotlight_entries[0])["progression_eligible"])


if __name__ == "__main__":
    unittest.main()
