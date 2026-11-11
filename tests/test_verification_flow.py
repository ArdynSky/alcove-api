import importlib
import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch


class VerificationFlowTests(unittest.TestCase):
    def setUp(self):
        self.assertIsNotNone(importlib.util.find_spec("api.verification_flow"), "Server verification protocol must exist")
        self.flow = importlib.import_module("api.verification_flow")
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.env = patch.dict(os.environ, {"ALCOVE_STATE_DB_PATH": os.path.join(self.tmp.name, "state.db")})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.user = {"id": 123456, "username": "alice"}

    def reject(self, fn, *args):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as error:
            fn(*args)
        self.assertIn(error.exception.status_code, (400, 401, 403, 404, 409, 410, 422))

    def captcha(self, state, real=True):
        return next(tile["id"] for tile in state["captcha_tiles"]
                    if (tile["image"] == "assets/captcha_images/verify-real-fox.png") == real)

    def through_captcha(self):
        state = self.flow.start(self.user)
        sid = state["session_id"]
        self.flow.advance(self.user, sid, "ethos", True)
        self.flow.advance(self.user, sid, "captcha", self.captcha(state))
        return state

    def finish(self):
        state = self.through_captcha()
        sid = state["session_id"]
        self.flow.advance(self.user, sid, "feedback", "  A thoughtful community  ")
        self.flow.advance(self.user, sid, "vote", "pup")
        return sid

    def test_full_path_returns_only_trusted_completion(self):
        sid = self.finish()
        self.assertEqual(self.flow.completed(self.user, sid), {
            "session_id": sid, "protocol_version": 2,
            "feedback": "A thoughtful community", "selected_pack": "pup",
        })
        self.assertEqual(self.flow.completed(self.user, sid), self.flow.completed(self.user, sid))

    def test_start_is_fresh_and_challenge_has_nine_actual_images(self):
        first, second = self.flow.start(self.user), self.flow.start(self.user)
        self.assertNotEqual(first["session_id"], second["session_id"])
        self.assertEqual(first["stage"], "ethos")
        self.assertEqual(first["protocol_version"], 2)
        self.assertEqual(len({tile["id"] for tile in first["captcha_tiles"]}), 9)
        self.assertEqual({tile["image"] for tile in first["captcha_tiles"]}, {
            "assets/captcha_images/verify-real-fox.png",
            *[f"assets/captcha_images/verify-imposter-{i}.png" for i in range(1, 9)],
        })
        self.assertNotEqual({t["id"] for t in first["captcha_tiles"]}, {t["id"] for t in second["captcha_tiles"]})

    def test_wrong_captcha_requires_backup_and_cannot_be_changed(self):
        state = self.flow.start(self.user)
        sid = state["session_id"]
        self.flow.advance(self.user, sid, "ethos", True)
        backup = self.flow.advance(self.user, sid, "captcha", self.captcha(state, False))
        self.assertEqual(backup["stage"], "backup")
        self.reject(self.flow.advance, self.user, sid, "feedback", "hello")
        self.reject(self.flow.advance, self.user, sid, "captcha", self.captcha(state))
        self.reject(self.flow.advance, self.user, sid, "backup", "dog")
        self.assertEqual(self.flow.advance(self.user, sid, "backup", "fOx")["stage"], "feedback")
        self.flow.advance(self.user, sid, "feedback", "hello")
        self.flow.advance(self.user, sid, "vote", "biker")
        self.assertEqual(self.flow.completed(self.user, sid)["selected_pack"], "biker")

    def test_steps_cannot_be_skipped_and_completion_requires_every_step(self):
        state = self.flow.start(self.user)
        sid = state["session_id"]
        for step, value in (("captcha", self.captcha(state)), ("feedback", "hello"), ("vote", "daddy"), ("unknown", True)):
            self.reject(self.flow.advance, self.user, sid, step, value)
        self.reject(self.flow.completed, self.user, sid)
        self.flow.advance(self.user, sid, "ethos", True)
        self.reject(self.flow.completed, self.user, sid)
        self.flow.advance(self.user, sid, "captcha", self.captcha(state))
        self.reject(self.flow.completed, self.user, sid)
        self.flow.advance(self.user, sid, "feedback", "hello")
        self.reject(self.flow.completed, self.user, sid)

    def test_invalid_values_do_not_advance(self):
        state = self.flow.start(self.user)
        sid = state["session_id"]
        for invalid in (False, "true", 1, None):
            self.reject(self.flow.advance, self.user, sid, "ethos", invalid)
        self.flow.advance(self.user, sid, "ethos", True)
        self.reject(self.flow.advance, self.user, sid, "captcha", "invented")
        self.flow.advance(self.user, sid, "captcha", self.captcha(state))
        for invalid in ("", " a ", "x" * 1201, None, 123):
            self.reject(self.flow.advance, self.user, sid, "feedback", invalid)
        self.flow.advance(self.user, sid, "feedback", "x" * 1200)
        self.reject(self.flow.advance, self.user, sid, "vote", "unknown")
        self.flow.advance(self.user, sid, "vote", "daddy")

    def test_bound_identity_and_session_id_are_required(self):
        sid = self.flow.start(self.user)["session_id"]
        for user in ({"id": 654321}, {}, {"id": "not-numeric"}, {"id": True}, {"id": 12.3}):
            self.reject(self.flow.advance, user, sid, "ethos", True)
            self.reject(self.flow.completed, user, sid)
        for bad_sid in (None, "", "missing", [], {}):
            self.reject(self.flow.completed, self.user, bad_sid)
        for invalid_user in ({}, {"id": True}, {"id": 12.3}, {"id": "not-numeric"}):
            self.reject(self.flow.start, invalid_user)

    def test_expiry_blocks_steps_and_completed_sessions_after_two_hours(self):
        with patch("time.time", return_value=100000):
            sid = self.finish()
            pending = self.flow.start(self.user)["session_id"]
        with patch("time.time", return_value=107199):
            self.assertEqual(self.flow.completed(self.user, sid)["selected_pack"], "pup")
        with patch("time.time", return_value=107200):
            self.reject(self.flow.completed, self.user, sid)
            self.reject(self.flow.advance, self.user, pending, "ethos", True)

    def test_sessions_survive_module_reload(self):
        state = self.through_captcha()
        self.flow = importlib.reload(self.flow)
        sid = state["session_id"]
        self.flow.advance(self.user, sid, "feedback", "persisted")
        self.flow.advance(self.user, sid, "vote", "dancer")
        self.flow = importlib.reload(self.flow)
        self.assertEqual(self.flow.completed(self.user, sid)["feedback"], "persisted")

    def test_invalidation_revokes_all_user_sessions_without_affecting_other_users(self):
        sid = self.finish()
        pending = self.flow.start(self.user)["session_id"]
        other_user = {"id": 777}
        other = self.flow.start(other_user)["session_id"]
        self.assertTrue(callable(getattr(self.flow, "invalidate_user", None)), "Revocation must invalidate stored sessions")
        self.flow.invalidate_user(str(self.user["id"]))
        self.reject(self.flow.completed, self.user, sid)
        self.reject(self.flow.advance, self.user, pending, "ethos", True)
        self.assertEqual(self.flow.advance(other_user, other, "ethos", True)["stage"], "captcha")
        self.flow.invalidate_user(self.user["id"])

    def test_ready_state_exposes_recorded_values(self):
        sid = self.finish()
        state = self.flow.advance(self.user, sid, "vote", "pup")
        self.assertEqual(state["feedback"], "A thoughtful community")
        self.assertEqual(state["selected_pack"], "pup")

    def test_duplicate_accepted_steps_are_idempotent_including_concurrent_retry(self):
        state = self.flow.start(self.user)
        sid = state["session_id"]
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.flow.advance(self.user, sid, "ethos", True), range(2)))
        self.assertEqual([r["stage"] for r in results], ["captcha", "captcha"])
        self.flow.advance(self.user, sid, "captcha", self.captcha(state))
        self.flow.advance(self.user, sid, "feedback", "original")
        self.assertEqual(self.flow.advance(self.user, sid, "ethos", True)["stage"], "vote")
        self.reject(self.flow.advance, self.user, sid, "feedback", "changed")
        self.flow.advance(self.user, sid, "vote", "pup")
        self.assertEqual(self.flow.advance(self.user, sid, "vote", "pup")["stage"], "ready")
        self.reject(self.flow.advance, self.user, sid, "vote", "biker")


if __name__ == "__main__":
    unittest.main()
