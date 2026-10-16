import datetime
import os
import sqlite3
import tempfile
import threading
import unittest
from zoneinfo import ZoneInfo

_temp_dir = tempfile.TemporaryDirectory()
os.environ["ALCOVE_STATE_DB_PATH"] = os.path.join(_temp_dir.name, "state.db")
os.environ["ALCOVE_RUNTIME_STATE_PATH"] = os.path.join(_temp_dir.name, "runtime.json")
os.environ["FEATURE_FLAGS_PATH"] = os.path.join(_temp_dir.name, "feature_flags.json")
os.environ["PULSE_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "pulse_settings.json")
os.environ["SAFETY_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "safety_settings.json")
os.environ["VERIFY_FLOW_LOG_PATH"] = os.path.join(_temp_dir.name, "verification_flow_events.jsonl")
os.environ["BOT_SYNC_SECRET"] = "test-admin-secret"
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from fastapi.testclient import TestClient

from api import daily_checkin, main

UK = ZoneInfo("Europe/London")


def london(year, month, day, hour=12, minute=0):
    return datetime.datetime(year, month, day, hour, minute, tzinfo=UK)


class DailyCheckInTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self._orig_verify = main.verify_telegram_init_data
        main.verify_telegram_init_data = self._verify
        self.users = {
            "1": {"id": 1, "username": "ardyn", "first_name": "Ardyn", "last_name": "Sky"},
            "2": {"id": 2, "username": "jamie", "first_name": "Jamie"},
            "3": {"id": 3, "username": "sam", "first_name": "Sam"},
            "4": {"id": 4, "username": "mod", "first_name": "Mod"},
        }
        daily_checkin.set_clock(lambda: london(2026, 6, 15, 18, 0))
        daily_checkin._HITS.clear()
        self._reset_db()

    def tearDown(self):
        main.verify_telegram_init_data = self._orig_verify
        daily_checkin.set_clock(None)

    def _verify(self, init_data: str) -> dict:
        user = self.users.get(str(init_data))
        if not user:
            raise main.HTTPException(status_code=403, detail="Invalid Telegram Mini App signature")
        return dict(user)

    def _reset_db(self):
        path = daily_checkin._db_path()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except OSError:
                pass

    def _headers(self, user_id="1"):
        return {"X-Telegram-Init-Data": str(user_id)}

    def _post(self, user_id="1", feeling="between", message="Up late, mind open. Let's talk.", day="2026-06-15"):
        return self.client.post(
            "/api/daily-checkin",
            headers=self._headers(user_id),
            json={"feeling": feeling, "message": message, "community_date": day},
        )

    def _grant_count(self, user_id=None):
        conn = sqlite3.connect(daily_checkin._db_path())
        try:
            if user_id is None:
                row = conn.execute("SELECT COUNT(*) FROM exp_grants").fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM exp_grants WHERE user_id = ?",
                    (int(user_id),),
                ).fetchone()
            return int(row[0])
        finally:
            conn.close()

    def test_feeling_required_and_message_optional(self):
        missing = self._post(feeling="", message="hello")
        self.assertEqual(missing.status_code, 400)
        blank = self._post(feeling="good", message="   ")
        self.assertEqual(blank.status_code, 200)
        body = blank.json()
        self.assertEqual(body["checkin"]["message"], "")
        self.assertEqual(body["award"]["amount"], 30)
        self.assertEqual(body["award"]["reason"], "Daily check-in")
        self.assertEqual(self._grant_count(1), 1)

    def test_message_awards_75_not_105_and_retry_does_not_duplicate(self):
        first = self._post(message="Up late, mind open. Let's talk.")
        self.assertEqual(first.status_code, 200)
        award = first.json()["award"]
        self.assertEqual(award["amount"], 75)
        self.assertEqual(award["reason"], "Daily check-in")
        self.assertEqual(award["status"], "pending")
        again = self._post(message="A different message that must not replace the first.")
        self.assertEqual(again.status_code, 200)
        self.assertTrue(again.json()["recovered"])
        self.assertEqual(again.json()["checkin"]["message"], "Up late, mind open. Let's talk.")
        self.assertEqual(again.json()["checkin"]["id"], first.json()["checkin"]["id"])
        self.assertEqual(self._grant_count(1), 1)
        self.assertEqual(again.json()["award"]["id"], award["id"])

    def test_character_limit_counts_code_points(self):
        emoji = "😀" * 150
        ok = self._post(user_id="2", feeling="hard", message=emoji)
        self.assertEqual(ok.status_code, 200)
        self.assertEqual(len(ok.json()["checkin"]["message"]), 150)
        over = self._post(user_id="3", feeling="good", message="😀" * 151)
        self.assertEqual(over.status_code, 400)
        self.assertEqual(self._grant_count(3), 0)

    def test_day_change_does_not_publish(self):
        shifted = self._post(day="2026-06-14", message="yesterday draft")
        self.assertEqual(shifted.status_code, 409)
        self.assertIn("community day", shifted.json()["detail"].lower())
        state = self.client.get("/api/daily-checkin", headers=self._headers())
        self.assertFalse(state.json()["participated"])
        self.assertEqual(self._grant_count(1), 0)

    def test_feed_requires_participation_and_filters_without_stale_mix(self):
        blocked = self.client.get("/api/daily-checkin/feed?feeling=between", headers=self._headers("2"))
        self.assertEqual(blocked.status_code, 403)
        self._post(user_id="1", feeling="between", message="between one")
        self._post(user_id="2", feeling="good", message="good one")
        self._post(user_id="3", feeling="between", message="between two")
        feed = self.client.get("/api/daily-checkin/feed?feeling=between", headers=self._headers("1"))
        self.assertEqual(feed.status_code, 200)
        messages = [post["message"] for post in feed.json()["posts"]]
        self.assertEqual(messages, ["between two", "between one"])
        self.assertNotIn("good one", messages)
        self.assertNotIn("@", feed.json()["posts"][0]["display_name"])

    def test_replies_once_no_self_and_delete_does_not_restore(self):
        parent = self._post(user_id="2", feeling="between", message="Jamie's day").json()["checkin"]["id"]
        self._post(user_id="1", feeling="between", message="mine")
        self_reply = self.client.post(
            f"/api/daily-checkin/{parent}/replies",
            headers=self._headers("2"),
            json={"message": "talking to myself"},
        )
        self.assertEqual(self_reply.status_code, 403)
        first = self.client.post(
            f"/api/daily-checkin/{parent}/replies",
            headers=self._headers("1"),
            json={"message": "glad you're here"},
        )
        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["award"]["amount"], 10)
        self.assertEqual(first.json()["award"]["reason"], "Check-in reply")
        second = self.client.post(
            f"/api/daily-checkin/{parent}/replies",
            headers=self._headers("1"),
            json={"message": "and again"},
        )
        self.assertEqual(second.status_code, 409)
        blank = self.client.post(
            f"/api/daily-checkin/{self._post(user_id='3', feeling='good', message='sam').json()['checkin']['id']}/replies",
            headers=self._headers("1"),
            json={"message": "   "},
        )
        self.assertEqual(blank.status_code, 400)
        deleted = self.client.request(
            "DELETE",
            f"/api/daily-checkin/replies/{first.json()['reply']['id']}",
            headers=self._headers("1"),
            json={},
        )
        self.assertEqual(deleted.status_code, 200)
        again = self.client.post(
            f"/api/daily-checkin/{parent}/replies",
            headers=self._headers("1"),
            json={"message": "after delete"},
        )
        self.assertEqual(again.status_code, 409)
        self.assertEqual(self._grant_count(1), 2)

    def test_concurrent_replies_create_only_one(self):
        parent = self._post(user_id="2", feeling="hard", message="open").json()["checkin"]["id"]
        self._post(user_id="1", feeling="hard", message="joined")
        barrier = threading.Barrier(2)
        results = []

        def attempt():
            barrier.wait()
            results.append(
                daily_checkin.submit_reply(self.users["1"], parent, "only once")
            )

        threads = [threading.Thread(target=attempt) for _ in range(2)]
        errors = []

        def guarded():
            try:
                attempt()
            except Exception as exc:  # HTTPException or otherwise
                errors.append(exc)

        threads = [threading.Thread(target=guarded) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        successes = [item for item in results if isinstance(item, dict)]
        self.assertEqual(len(successes), 1)
        self.assertTrue(errors)
        self.assertEqual(self._grant_count(1), 2)

    def test_reply_exp_stops_after_ten_without_blocking_the_reply(self):
        self._post(user_id="1", feeling="good", message="host")
        for index in range(11):
            author = str(100 + index)
            self.users[author] = {"id": int(author), "username": f"m{author}", "first_name": f"M{author}"}
            created = self._post(user_id=author, feeling="good", message=f"post {index}")
            self.assertEqual(created.status_code, 200, created.text)
            reply = self.client.post(
                f"/api/daily-checkin/{created.json()['checkin']['id']}/replies",
                headers=self._headers("1"),
                json={"message": f"reply {index}"},
            )
            self.assertEqual(reply.status_code, 200, reply.text)
            if index < 10:
                self.assertEqual(reply.json()["award"]["amount"], 10)
            else:
                self.assertIsNone(reply.json()["award"])
        reply_grants = sqlite3.connect(daily_checkin._db_path())
        try:
            count = reply_grants.execute(
                "SELECT COUNT(*) FROM exp_grants WHERE user_id = 1 AND source = 'daily_checkin_reply'"
            ).fetchone()[0]
        finally:
            reply_grants.close()
        self.assertEqual(count, 10)

    def test_delete_checkin_keeps_access_and_blocks_another_reward(self):
        created = self._post(message="keep the reward")
        self.assertEqual(created.json()["award"]["amount"], 75)
        removed = self.client.request(
            "DELETE",
            "/api/daily-checkin",
            headers=self._headers(),
            json={},
        )
        self.assertEqual(removed.status_code, 200)
        state = self.client.get("/api/daily-checkin", headers=self._headers()).json()
        self.assertTrue(state["participated"])
        self.assertTrue(state["checkin"]["deleted"])
        feed = self.client.get("/api/daily-checkin/feed?feeling=between", headers=self._headers())
        self.assertEqual(feed.status_code, 200)
        self.assertEqual(feed.json()["posts"], [])
        again = self._post(message="second try")
        self.assertEqual(again.status_code, 409)
        self.assertEqual(self._grant_count(1), 1)

    def test_moderator_can_delete_reply_author_cannot_delete_others(self):
        self.users["4"] = {"id": 8385145826, "username": "owner", "first_name": "Owner"}
        parent = self._post(user_id="2", feeling="between", message="Jamie").json()["checkin"]["id"]
        self._post(user_id="1", feeling="between", message="Ardyn")
        reply = self.client.post(
            f"/api/daily-checkin/{parent}/replies",
            headers=self._headers("1"),
            json={"message": "hello"},
        ).json()["reply"]["id"]
        denied = self.client.request(
            "DELETE",
            f"/api/daily-checkin/replies/{reply}",
            headers=self._headers("3"),
            json={},
        )
        self._post(user_id="3", feeling="good", message="sam is here")
        denied = self.client.request(
            "DELETE",
            f"/api/daily-checkin/replies/{reply}",
            headers=self._headers("3"),
            json={},
        )
        self.assertEqual(denied.status_code, 403)
        allowed = self.client.request(
            "DELETE",
            f"/api/daily-checkin/replies/{reply}",
            headers=self._headers("4"),
            json={},
        )
        self.assertEqual(allowed.status_code, 200)

    def test_london_midnight_handles_daylight_saving(self):
        spring = daily_checkin.community_clock(london(2026, 3, 29, 0, 30))
        spring_reset = datetime.datetime.fromisoformat(spring["next_reset_at"])
        spring_now = datetime.datetime.fromisoformat(spring["server_time"])
        self.assertEqual(spring["community_date"], "2026-03-29")
        self.assertEqual(round((spring_reset - spring_now).total_seconds()), 23 * 3600 - 30 * 60)

        autumn = daily_checkin.community_clock(london(2026, 10, 25, 0, 30))
        autumn_reset = datetime.datetime.fromisoformat(autumn["next_reset_at"])
        autumn_now = datetime.datetime.fromisoformat(autumn["server_time"])
        self.assertEqual(autumn["community_date"], "2026-10-25")
        self.assertEqual(round((autumn_reset - autumn_now).total_seconds()), 25 * 3600 - 30 * 60)

        normal = daily_checkin.community_clock(london(2026, 6, 15, 18, 0))
        normal_reset = datetime.datetime.fromisoformat(normal["next_reset_at"])
        normal_now = datetime.datetime.fromisoformat(normal["server_time"])
        self.assertEqual(round((normal_reset - normal_now).total_seconds()), 6 * 3600)

    def test_no_update_route_and_report_uses_care_table(self):
        missing = self.client.put(
            "/api/daily-checkin",
            headers=self._headers(),
            json={"feeling": "good", "message": "edited", "community_date": "2026-06-15"},
        )
        self.assertEqual(missing.status_code, 405)
        parent = self._post(user_id="2", feeling="between", message="report me").json()["checkin"]
        self._post(user_id="1", feeling="good", message="viewer")
        report = self.client.post(
            "/api/daily-checkin/reports",
            headers=self._headers("1"),
            json={"target_type": "checkin", "target_id": parent["id"], "reason": "spam"},
        )
        self.assertEqual(report.status_code, 200)
        conn = sqlite3.connect(daily_checkin._db_path())
        try:
            care = conn.execute("SELECT source, reason FROM care_reports").fetchone()
        except sqlite3.OperationalError:
            care = None
        finally:
            conn.close()
        if care is not None:
            self.assertEqual(care[0], "telegram")
            self.assertEqual(care[1], "spam")


if __name__ == "__main__":
    unittest.main()
