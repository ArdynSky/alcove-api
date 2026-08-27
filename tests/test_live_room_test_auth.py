import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone


_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ["LIVE_ROOM_TEST_DB"] = os.path.join(_temp_dir.name, "live_room_test.sqlite3")
os.environ["LIVE_ROOM_TEST_HANDOFF_SECRET"] = "test-handoff-secret"
os.environ["LIVE_ROOM_TEST_PUBLIC_BASE"] = "https://example.test"
os.environ["WHEREBY_LIVE_ROOM_TEST_PARTICIPANT_URL"] = "https://the-alcove.whereby.com/test-room"
os.environ["WHEREBY_LIVE_ROOM_TEST_HOST_URL"] = (
    "https://the-alcove.whereby.com/test-room?roomKey=HOSTKEYSECRET"
)
os.environ["LIVE_ROOM_TEST_HOST_USER_IDS"] = "111"
os.environ["TELEGRAM_BOT_TOKEN"] = "test-bot-token"

from fastapi.testclient import TestClient

from api import live_room_test, main


MEMBER = {
    "id": 4242,
    "username": "member",
    "first_name": "Test",
    "last_name": "Member",
}
HOST = {
    "id": 111,
    "username": "host",
    "first_name": "Host",
    "last_name": "User",
}


class LiveRoomTestAuthTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self._orig_verify = live_room_test._verify_init_data
        live_room_test._verify_init_data = lambda init_data: {
            "init_data": init_data,
            **(HOST if init_data == "host-init" else MEMBER),
        }
        if os.path.exists(live_room_test.DB_PATH):
            os.remove(live_room_test.DB_PATH)

    def tearDown(self):
        live_room_test._verify_init_data = self._orig_verify

    def test_feature_flag_defaults_off(self):
        payload = main.get_feature_flags()
        self.assertIn("live_room_test", payload["features"]["pages"])
        self.assertFalse(payload["features"]["pages"]["live_room_test"])

    def test_session_returns_participant_embed_without_host_key(self):
        response = self.client.post(
            "/api/live-room-test/session",
            json={"init_data": "member-init"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["role"], "participant")
        self.assertEqual(data["user"]["id"], 4242)
        self.assertIn("the-alcove.whereby.com/test-room", data["embed_url"])
        self.assertNotIn("HOSTKEYSECRET", data["embed_url"])
        self.assertNotIn("HOSTKEYSECRET", response.text)
        self.assertTrue(data["configured"])

    def test_host_session_receives_host_embed(self):
        response = self.client.post(
            "/api/live-room-test/session",
            json={"init_data": "host-init"},
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["role"], "host")
        self.assertIn("roomKey=HOSTKEYSECRET", data["embed_url"])

    def test_handoff_is_single_use_and_expires(self):
        handoff = self.client.post(
            "/api/live-room-test/handoff",
            json={"init_data": "member-init"},
        ).json()
        self.assertIn("handoff=", handoff["open_url"])
        self.assertTrue(handoff["open_url"].startswith("https://example.test/live-room-test.html?handoff="))

        claimed = self.client.post(
            "/api/live-room-test/claim",
            json={"token": handoff["token"]},
        )
        self.assertEqual(claimed.status_code, 200)
        session = claimed.json()
        self.assertEqual(session["user"]["id"], 4242)
        self.assertEqual(session["role"], "participant")
        self.assertNotIn("HOSTKEYSECRET", claimed.text)

        reused = self.client.post(
            "/api/live-room-test/claim",
            json={"token": handoff["token"]},
        )
        self.assertEqual(reused.status_code, 409)

        expired = self.client.post(
            "/api/live-room-test/handoff",
            json={"init_data": "member-init"},
        ).json()
        with live_room_test._LOCK:
            con = live_room_test._conn()
            try:
                token_hash = live_room_test._hash_token(expired["token"])
                con.execute(
                    "UPDATE live_room_test_tokens SET expires_at=? WHERE token_hash=?",
                    (
                        (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat(),
                        token_hash,
                    ),
                )
                con.commit()
            finally:
                con.close()
        stale = self.client.post(
            "/api/live-room-test/claim",
            json={"token": expired["token"]},
        )
        self.assertEqual(stale.status_code, 401)

    def test_invalid_token_rejected(self):
        response = self.client.post(
            "/api/live-room-test/claim",
            json={"token": "not-a-real-token"},
        )
        self.assertEqual(response.status_code, 401)

    def test_session_overrides_spoofed_comment_user_id(self):
        session = self.client.post(
            "/api/live-room-test/session",
            json={"init_data": "member-init"},
        ).json()
        original = list(main.approved_comments)
        try:
            response = self.client.post(
                "/api/stream-comment",
                json={
                    "user_id": 999999,
                    "username": "spoof",
                    "display_name": "Spoof",
                    "text": "hello from the test room",
                },
                headers={"Authorization": f"Bearer {session['session_token']}"},
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json().get("status"), "ok")
            self.assertEqual(main.approved_comments[-1]["user_id"], 4242)
            self.assertEqual(main.approved_comments[-1]["username"], "member")
        finally:
            main.approved_comments[:] = original

    def test_me_requires_session(self):
        missing = self.client.get("/api/live-room-test/me")
        self.assertEqual(missing.status_code, 401)
        session = self.client.post(
            "/api/live-room-test/session",
            json={"init_data": "member-init"},
        ).json()
        ok = self.client.get(
            "/api/live-room-test/me",
            headers={"Authorization": f"Bearer {session['session_token']}"},
        )
        self.assertEqual(ok.status_code, 200)
        self.assertNotIn("HOSTKEYSECRET", ok.text)
        self.assertEqual(ok.json()["user"]["id"], 4242)


if __name__ == "__main__":
    unittest.main()
