import copy
import os
import sqlite3
import tempfile
import unittest

_temp_dir = tempfile.TemporaryDirectory()
os.environ["ALCOVE_STATE_DB_PATH"] = os.path.join(_temp_dir.name, "state.db")
os.environ["ALCOVE_RUNTIME_STATE_PATH"] = os.path.join(_temp_dir.name, "runtime.json")
os.environ["FOX_LOGS_DB_PATH"] = os.path.join(_temp_dir.name, "fox.db")
os.environ["SAFETY_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "safety_settings.json")
os.environ["FEATURE_FLAGS_PATH"] = os.path.join(_temp_dir.name, "feature_flags.json")
os.environ["PULSE_SETTINGS_PATH"] = os.path.join(_temp_dir.name, "pulse_settings.json")
os.environ["VERIFY_FLOW_LOG_PATH"] = os.path.join(_temp_dir.name, "verification_flow_events.jsonl")
os.environ["BOT_SYNC_SECRET"] = "test-admin-secret"

from fastapi.testclient import TestClient

from api import main


SECRET = "test-admin-secret"
BOT_HEADERS = {"X-Bot-Sync-Secret": SECRET}


class SafetyEnforcementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self._admin_jobs = copy.deepcopy(main.admin_jobs)
        self._state_db = main.STATE_DB_PATH
        self._runtime = main.RUNTIME_STATE_PATH
        self._fox = main.FOX_LOGS_DB_PATH
        self._safety = main.SAFETY_SETTINGS_PATH
        self._secret = main.BOT_SYNC_SECRET
        self._fingerprint = main._last_saved_runtime_fingerprint
        main.STATE_DB_PATH = os.path.join(self.tmp.name, "state.db")
        main.RUNTIME_STATE_PATH = os.path.join(self.tmp.name, "runtime.json")
        main.FOX_LOGS_DB_PATH = os.path.join(self.tmp.name, "fox.db")
        main.SAFETY_SETTINGS_PATH = os.path.join(self.tmp.name, "safety_settings.json")
        main.BOT_SYNC_SECRET = SECRET
        main.admin_jobs = {}
        main._last_saved_runtime_fingerprint = None
        with main._persist_runtime_lock:
            if main._persist_runtime_timer is not None:
                main._persist_runtime_timer.cancel()
                main._persist_runtime_timer = None
        self.client = TestClient(main.app)
        self._seed_fox()

    def tearDown(self):
        with main._persist_runtime_lock:
            if main._persist_runtime_timer is not None:
                main._persist_runtime_timer.cancel()
                main._persist_runtime_timer = None
        main.admin_jobs = self._admin_jobs
        main.STATE_DB_PATH = self._state_db
        main.RUNTIME_STATE_PATH = self._runtime
        main.FOX_LOGS_DB_PATH = self._fox
        main.SAFETY_SETTINGS_PATH = self._safety
        main.BOT_SYNC_SECRET = self._secret
        main._last_saved_runtime_fingerprint = self._fingerprint
        self.tmp.cleanup()

    def _seed_fox(self):
        with open(main.FOX_LOGS_DB_PATH, "a", encoding="utf-8"):
            pass
        main.fox_db_rows("SELECT 1")
        now = main.now_iso()
        with sqlite3.connect(main.FOX_LOGS_DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO user_profiles (user_id, username, display_name, verified_at)
                VALUES (504, 'casey', 'Casey', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT INTO flood_flags (
                    user_id, username, display_name, message_count, window_seconds,
                    message_excerpt, logged_at
                ) VALUES (501, 'river', 'River', 12, 60, 'too many messages', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT INTO link_violations (
                    message_id, user_id, username, display_name, message_excerpt, link_samples, logged_at
                ) VALUES (9001, 502, 'linkuser', 'Link User', 'check this url', 'https://example.test', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT INTO tone_flags (
                    message_id, user_id, username, display_name, categories, severity, score,
                    matched_terms, message_excerpt, logged_at
                ) VALUES (9002, 503, 'toneuser', 'Tone User', 'hostility', 'high', 3, 'slug', 'harsh line', ?)
                """,
                (now,),
            )
            conn.execute(
                """
                INSERT INTO user_strikes (user_id, admin_user_id, reason, active, created_at)
                VALUES (501, 1, 'one', 1, ?), (501, 1, 'two', 1, ?), (501, 1, 'old', 0, ?)
                """,
                (now, now, now),
            )
            conn.commit()

    def _queue(self):
        response = self.client.get(
            "/api/admin/safety/action-queue",
            params={"admin_secret": SECRET, "period": "today"},
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def _row(self, kind):
        rows = [row for row in self._queue()["rows"] if row["kind"] == kind]
        self.assertTrue(rows, kind)
        return rows[0]

    def test_existing_safety_reads_still_work(self):
        settings = self.client.get("/api/admin/safety/settings", params={"admin_secret": SECRET})
        self.assertEqual(settings.status_code, 200, settings.text)
        self.assertEqual(settings.json()["status"], "ok")
        self.assertIn("flood_message_threshold", settings.json()["settings"])

        bot_settings = self.client.get("/api/bot-sync/safety-settings", headers=BOT_HEADERS)
        self.assertEqual(bot_settings.status_code, 200, bot_settings.text)
        self.assertEqual(bot_settings.json()["status"], "ok")

        queue = self._queue()
        self.assertIn("items", queue)
        self.assertIn("recent_flood", queue)
        self.assertIn("settings", queue)
        self.assertTrue(any(item["kind"] == "flood" for item in queue["items"]))
        self.assertTrue(queue["row_gaps"])

    def test_action_queue_rows_use_stored_fox_fields(self):
        flood = self._row("flood")
        self.assertEqual(flood["user_id"], 501)
        self.assertEqual(flood["display_name"], "River")
        self.assertEqual(flood["excerpt"], "too many messages")
        self.assertIsNone(flood["message_id"])
        self.assertEqual(flood["message_ids"], [])
        self.assertIsNone(flood["chat_id"])
        self.assertEqual(flood["strike_count"], 2)
        self.assertEqual(flood["status"], "open")
        self.assertIn("message_id", flood["missing"])
        self.assertIn("chat_id", flood["missing"])
        self.assertEqual(flood["detail"]["message_count"], 12)

        link = self._row("link")
        self.assertEqual(link["kind"], "link")
        self.assertEqual(link["message_id"], 9001)
        self.assertEqual(link["message_ids"], [9001])
        self.assertIsNone(link["chat_id"])
        self.assertIn("chat_id", link["missing"])
        self.assertNotIn("message_id", link["missing"])
        self.assertEqual(link["detail"]["link_samples"], "https://example.test")

        tone = self._row("tone")
        self.assertEqual(tone["message_id"], 9002)
        self.assertEqual(tone["detail"]["severity"], "high")
        self.assertEqual(tone["strike_count"], 0)

    def test_enqueue_claim_complete_and_keep_bulk_job(self):
        muted = self.client.post(
            "/api/admin/safety/actions",
            json={
                "admin_secret": SECRET,
                "action": "mute",
                "telegram_user_id": 501,
                "chat_id": -100123,
                "message_id": 77,
                "hours": 6,
                "reason": "flooding",
                "source": "feature_admin",
                "queue_item_id": "flood:1",
                "admin_user_id": 42,
            },
        )
        self.assertEqual(muted.status_code, 200, muted.text)
        job = muted.json()["job"]
        self.assertEqual(job["job"], "safety_action")
        self.assertEqual(job["action"], "mute")
        self.assertEqual(job["status"], "pending")
        self.assertEqual(job["hours"], 6)
        self.assertEqual(job["chat_id"], -100123)
        self.assertFalse(muted.json()["already_queued"])
        job_id = job["id"]
        admin_list = self.client.get("/api/admin/safety/actions", params={"admin_secret": SECRET})
        self.assertEqual(admin_list.status_code, 200, admin_list.text)
        self.assertEqual(admin_list.json()["jobs"][0]["id"], job_id)

        again = self.client.post(
            "/api/admin/safety/actions",
            json={
                "admin_secret": SECRET,
                "action": "mute",
                "telegram_user_id": 501,
                "chat_id": -100123,
                "message_id": 77,
                "hours": 6,
                "queue_item_id": "flood:1",
            },
        )
        self.assertEqual(again.status_code, 200, again.text)
        self.assertTrue(again.json()["already_queued"])
        self.assertEqual(again.json()["job"]["id"], job_id)

        flood = self._row("flood")
        self.assertEqual(flood["status"], "acted")
        self.assertEqual(flood["job_id"], job_id)
        self.assertEqual(flood["job_status"], "pending")

        bulk = self.client.post("/api/admin/verify-all-group-members", params={"admin_secret": SECRET})
        self.assertEqual(bulk.status_code, 200, bulk.text)

        listed = self.client.get("/api/bot-sync/admin-jobs", headers=BOT_HEADERS)
        self.assertEqual(listed.status_code, 200, listed.text)
        kinds = {item["job"] for item in listed.json()["jobs"]}
        self.assertEqual(kinds, {"bulk_verify_group", "safety_action"})
        safety = next(item for item in listed.json()["jobs"] if item["job"] == "safety_action")
        self.assertEqual(safety["id"], job_id)
        self.assertEqual(safety["action"], "mute")

        denied = self.client.get("/api/bot-sync/admin-jobs")
        self.assertEqual(denied.status_code, 403)

        claim = self.client.post(f"/api/bot-sync/admin-jobs/{job_id}/claim", headers=BOT_HEADERS)
        self.assertEqual(claim.status_code, 200, claim.text)
        self.assertEqual(claim.json()["job"]["status"], "claimed")
        reclaim = self.client.post(f"/api/bot-sync/admin-jobs/{job_id}/claim", headers=BOT_HEADERS)
        self.assertEqual(reclaim.status_code, 409)

        done = self.client.post(
            f"/api/bot-sync/admin-jobs/{job_id}/complete",
            headers=BOT_HEADERS,
            json={"status": "completed", "result": {"telegram_ok": True}},
        )
        self.assertEqual(done.status_code, 200, done.text)
        self.assertEqual(done.json()["job"]["status"], "completed")
        self.assertEqual(done.json()["job"]["result"]["telegram_ok"], True)
        repeat = self.client.post(
            f"/api/bot-sync/admin-jobs/{job_id}/complete",
            headers=BOT_HEADERS,
            json={"status": "completed"},
        )
        self.assertTrue(repeat.json()["already_done"])

        saved = main.load_runtime_state_from_db()
        stored = saved["admin_jobs"]["actions"]
        self.assertEqual(stored[0]["id"], job_id)
        self.assertEqual(stored[0]["status"], "completed")

        hidden = self.client.get("/api/bot-sync/admin-jobs", headers=BOT_HEADERS)
        self.assertTrue(all(item["job"] != "safety_action" for item in hidden.json()["jobs"]))
        self.assertTrue(any(item["job"] == "bulk_verify_group" for item in hidden.json()["jobs"]))

    def test_failed_action_reopens_queue_row_and_ban_is_request_only(self):
        bad_hours = self.client.post(
            "/api/admin/safety/actions",
            json={"admin_secret": SECRET, "action": "mute", "telegram_user_id": 501, "hours": 3},
        )
        self.assertEqual(bad_hours.status_code, 400)
        extra_hours = self.client.post(
            "/api/admin/safety/actions",
            json={"admin_secret": SECRET, "action": "strike_add", "telegram_user_id": 501, "hours": 1},
        )
        self.assertEqual(extra_hours.status_code, 400)
        bare_dismiss = self.client.post(
            "/api/admin/safety/actions",
            json={"admin_secret": SECRET, "action": "dismiss"},
        )
        self.assertEqual(bare_dismiss.status_code, 400)
        unknown = self.client.post(
            "/api/admin/safety/actions",
            json={"admin_secret": SECRET, "action": "kick", "telegram_user_id": 501},
        )
        self.assertEqual(unknown.status_code, 422)
        forbidden = self.client.post(
            "/api/admin/safety/actions",
            json={"admin_secret": "nope", "action": "ban_request", "telegram_user_id": 501},
        )
        self.assertEqual(forbidden.status_code, 403)

        created = self.client.post(
            "/api/admin/safety/actions",
            json={
                "admin_secret": SECRET,
                "action": "warn_public",
                "telegram_user_id": 502,
                "queue_item_id": "link:1",
                "reason": "link",
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        job_id = created.json()["job"]["id"]
        self.assertEqual(self._row("link")["status"], "acted")
        failed = self.client.post(
            f"/api/bot-sync/admin-jobs/{job_id}/complete",
            headers=BOT_HEADERS,
            json={"status": "failed", "error": "telegram timeout"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)
        self.assertEqual(failed.json()["job"]["error"], "telegram timeout")
        self.assertEqual(self._row("link")["status"], "open")

        banned = self.client.post(
            "/api/admin/safety/actions",
            json={
                "admin_secret": SECRET,
                "action": "ban_request",
                "telegram_user_id": 501,
                "source": "telegram",
                "reason": "needs Ardyn",
            },
        )
        self.assertEqual(banned.status_code, 200, banned.text)
        self.assertEqual(banned.json()["job"]["action"], "ban_request")
        self.assertEqual(banned.json()["job"]["status"], "pending")
        self.assertIsNone(banned.json()["job"]["hours"])
        grants = self.client.get("/api/bot-sync/exp-grants", headers=BOT_HEADERS)
        self.assertEqual(grants.json()["grants"], [])
        missing_job = self.client.post("/api/bot-sync/admin-jobs/missing-job/claim", headers=BOT_HEADERS)
        self.assertEqual(missing_job.status_code, 404)
        missing_grant = self.client.post(
            "/api/bot-sync/exp-grants/missing-grant/complete",
            headers=BOT_HEADERS,
            json={"status": "applied"},
        )
        self.assertEqual(missing_grant.status_code, 404)
        missing_report = self.client.post(
            "/api/admin/safety/care-reports/missing-report/resolve",
            json={"admin_secret": SECRET, "decision": "keep", "admin_user_id": 7},
        )
        self.assertEqual(missing_report.status_code, 404)

    def test_dismiss_marks_row_and_stale_claim_can_be_reclaimed(self):
        dismissed = self.client.post(
            "/api/admin/safety/actions",
            json={
                "admin_secret": SECRET,
                "action": "dismiss",
                "queue_item_id": "tone:1",
                "source": "feature_admin",
            },
        )
        self.assertEqual(dismissed.status_code, 200, dismissed.text)
        job_id = dismissed.json()["job"]["id"]
        self.assertEqual(self._row("tone")["status"], "dismissed")

        claim = self.client.post(f"/api/bot-sync/admin-jobs/{job_id}/claim", headers=BOT_HEADERS)
        self.assertEqual(claim.status_code, 200, claim.text)
        from api import safety_enforcement

        with safety_enforcement._LOCK:
            job = safety_enforcement._find_job(job_id)
            job["claimed_at"] = "2020-01-01T00:00:00Z"
        listed = self.client.get("/api/bot-sync/admin-jobs", headers=BOT_HEADERS)
        stale = next(item for item in listed.json()["jobs"] if item["id"] == job_id)
        self.assertEqual(stale["status"], "claimed")
        self.assertTrue(stale["reclaimable"])
        reclaimed = self.client.post(f"/api/bot-sync/admin-jobs/{job_id}/claim", headers=BOT_HEADERS)
        self.assertEqual(reclaimed.status_code, 200, reclaimed.text)
        self.assertEqual(reclaimed.json()["job"]["status"], "claimed")
        self.assertNotEqual(reclaimed.json()["job"]["claimed_at"], "2020-01-01T00:00:00Z")

    def test_care_report_review_and_exp_grant(self):
        created = self.client.post(
            "/api/bot-sync/care-reports",
            headers=BOT_HEADERS,
            json={
                "reporter_id": 900,
                "target_user_id": 504,
                "chat_id": -100555,
                "message_id": 42,
                "reason": "harassment",
                "note": "they kept piling on",
                "source": "fox_care",
            },
        )
        self.assertEqual(created.status_code, 200, created.text)
        report = created.json()["report"]
        self.assertEqual(report["status"], "pending")
        self.assertEqual(report["reporter_id"], 900)
        self.assertEqual(created.json()["reporter_activity"]["count_24h"], 1)
        self.assertTrue(created.json()["reporter_activity"]["last_created_at"])
        report_id = report["id"]

        second = self.client.post(
            "/api/bot-sync/care-reports",
            headers=BOT_HEADERS,
            json={
                "reporter_id": 900,
                "target_user_id": 501,
                "reason": "spam",
                "source": "fox_care",
            },
        )
        self.assertEqual(second.status_code, 200, second.text)
        self.assertEqual(second.json()["reporter_activity"]["count_24h"], 2)
        self.assertGreaterEqual(second.json()["reporter_activity"]["count_7d"], 2)

        listed = self.client.get(
            "/api/admin/safety/care-reports",
            params={"admin_secret": SECRET, "reporter_id": 900, "status": "pending"},
        )
        self.assertEqual(listed.status_code, 200, listed.text)
        self.assertEqual(len(listed.json()["reports"]), 2)
        self.assertEqual(listed.json()["reporter_activity"]["reporter_id"], 900)

        matching = [
            row for row in self._queue()["rows"]
            if row["kind"] == "care_report" and row["care_report_id"] == report_id
        ][0]
        self.assertEqual(matching["user_id"], 504)
        self.assertEqual(matching["display_name"], "Casey")
        self.assertEqual(matching["username"], "casey")
        self.assertEqual(matching["excerpt"], "they kept piling on")
        self.assertEqual(matching["message_id"], 42)
        self.assertEqual(matching["chat_id"], -100555)
        self.assertEqual(matching["status"], "open")
        self.assertEqual(matching["reporter_id"], 900)
        self.assertEqual(matching["reason"], "harassment")
        self.assertEqual(matching["missing"], [])

        kept = self.client.post(
            f"/api/admin/safety/care-reports/{second.json()['report']['id']}/resolve",
            json={"admin_secret": SECRET, "decision": "keep", "admin_user_id": 7},
        )
        self.assertEqual(kept.status_code, 200, kept.text)
        self.assertEqual(kept.json()["report"]["status"], "kept")
        self.assertEqual(kept.json()["report"]["decision"], "keep")
        self.assertIsNone(kept.json()["exp_grant"])

        taught = self.client.post(
            "/api/bot-sync/care-reports",
            headers=BOT_HEADERS,
            json={"reporter_id": 901, "target_user_id": 504, "reason": "worried_about_someone"},
        )
        self.assertEqual(taught.status_code, 200, taught.text)
        teach = self.client.post(
            f"/api/admin/safety/care-reports/{taught.json()['report']['id']}/resolve",
            json={"admin_secret": SECRET, "decision": "teach", "admin_user_id": 7},
        )
        self.assertEqual(teach.status_code, 200, teach.text)
        self.assertEqual(teach.json()["report"]["status"], "taught")
        self.assertIsNone(teach.json()["exp_grant"])

        removed = self.client.post(
            f"/api/admin/safety/care-reports/{report_id}/resolve",
            json={"admin_secret": SECRET, "decision": "remove", "admin_user_id": 7},
        )
        self.assertEqual(removed.status_code, 200, removed.text)
        self.assertEqual(removed.json()["report"]["status"], "removed")
        grant = removed.json()["exp_grant"]
        self.assertEqual(grant["user_id"], 900)
        self.assertEqual(grant["amount"], 30)
        self.assertEqual(grant["status"], "pending")
        self.assertEqual(grant["reason"], "care_report_remove")
        self.assertEqual(grant["dedupe_key"], f"care_report:{report_id}")

        again = self.client.post(
            f"/api/admin/safety/care-reports/{report_id}/resolve",
            json={"admin_secret": SECRET, "decision": "remove", "admin_user_id": 7},
        )
        self.assertEqual(again.status_code, 409)

        pending = self.client.get("/api/bot-sync/exp-grants", headers=BOT_HEADERS)
        self.assertEqual(len(pending.json()["grants"]), 1)
        grant_id = pending.json()["grants"][0]["id"]
        applied = self.client.post(
            f"/api/bot-sync/exp-grants/{grant_id}/complete",
            headers=BOT_HEADERS,
            json={"status": "applied", "result": {"profile_updated": True}},
        )
        self.assertEqual(applied.status_code, 200, applied.text)
        self.assertEqual(applied.json()["grant"]["status"], "applied")
        self.assertEqual(applied.json()["grant"]["result"]["profile_updated"], True)
        repeat = self.client.post(
            f"/api/bot-sync/exp-grants/{grant_id}/complete",
            headers=BOT_HEADERS,
            json={"status": "applied"},
        )
        self.assertTrue(repeat.json()["already_done"])
        self.assertEqual(self.client.get("/api/bot-sync/exp-grants", headers=BOT_HEADERS).json()["grants"], [])

        acted = [
            row for row in self._queue()["rows"]
            if row.get("care_report_id") == report_id
        ][0]
        self.assertEqual(acted["status"], "acted")
        self.assertEqual(acted["care_status"], "removed")

        admin_grants = self.client.get(
            "/api/admin/safety/exp-grants",
            params={"admin_secret": SECRET, "status": "applied"},
        )
        self.assertEqual(len(admin_grants.json()["grants"]), 1)

    def test_care_rows_survive_when_fox_logs_are_missing(self):
        main.FOX_LOGS_DB_PATH = os.path.join(self.tmp.name, "missing-fox.db")
        created = self.client.post(
            "/api/bot-sync/care-reports",
            headers=BOT_HEADERS,
            json={"reporter_id": 1, "target_user_id": 2, "reason": "other", "note": "check in"},
        )
        self.assertEqual(created.status_code, 200, created.text)
        queue = self._queue()
        self.assertIsInstance(queue["items"], list)
        care_rows = [row for row in queue["rows"] if row["kind"] == "care_report"]
        self.assertEqual(len(care_rows), 1)
        self.assertIsNone(care_rows[0]["strike_count"])
        self.assertIn("strike_count", care_rows[0]["missing"])
        self.assertEqual(care_rows[0]["display_name"], "")

    def test_member_lists_and_claims_own_exp_grant(self):
        def remove_report(reporter_id: int, target_user_id: int):
            created = self.client.post(
                "/api/bot-sync/care-reports",
                headers=BOT_HEADERS,
                json={
                    "reporter_id": reporter_id,
                    "target_user_id": target_user_id,
                    "reason": "harassment",
                    "source": "fox_care",
                },
            )
            self.assertEqual(created.status_code, 200, created.text)
            report_id = created.json()["report"]["id"]
            removed = self.client.post(
                f"/api/admin/safety/care-reports/{report_id}/resolve",
                json={"admin_secret": SECRET, "decision": "remove", "admin_user_id": 7},
            )
            self.assertEqual(removed.status_code, 200, removed.text)
            return report_id, removed.json()["exp_grant"]

        report_id, grant = remove_report(900, 504)
        _, other_grant = remove_report(902, 501)

        listed = self.client.get("/api/exp-grants", params={"user_id": 900})
        self.assertEqual(listed.status_code, 200, listed.text)
        self.assertEqual([row["id"] for row in listed.json()["grants"]], [grant["id"]])
        self.assertEqual(listed.json()["grants"][0]["amount"], 30)
        self.assertEqual(listed.json()["grants"][0]["status"], "pending")
        self.assertEqual(listed.json()["grants"][0]["dedupe_key"], f"care_report:{report_id}")
        self.assertEqual(listed.json()["grants"][0]["user_id"], 900)

        someone_else = self.client.get("/api/exp-grants", params={"user_id": 901})
        self.assertEqual(someone_else.status_code, 200, someone_else.text)
        self.assertEqual(someone_else.json()["grants"], [])
        self.assertEqual(self.client.get("/api/exp-grants").status_code, 422)

        wrong = self.client.post(
            f"/api/exp-grants/{grant['id']}/claim",
            json={"user_id": 901},
        )
        self.assertEqual(wrong.status_code, 403)
        self.assertEqual(
            [row["id"] for row in self.client.get("/api/exp-grants", params={"user_id": 900}).json()["grants"]],
            [grant["id"]],
        )

        missing = self.client.post("/api/exp-grants/missing-grant/claim", json={"user_id": 900})
        self.assertEqual(missing.status_code, 404)

        claimed = self.client.post(
            f"/api/exp-grants/{grant['id']}/claim",
            json={"user_id": 900},
        )
        self.assertEqual(claimed.status_code, 200, claimed.text)
        self.assertFalse(claimed.json()["already_done"])
        self.assertEqual(claimed.json()["grant"]["status"], "applied")
        self.assertEqual(claimed.json()["grant"]["dedupe_key"], f"care_report:{report_id}")
        self.assertTrue(claimed.json()["grant"]["applied_at"])

        again = self.client.post(
            f"/api/exp-grants/{grant['id']}/claim",
            json={"user_id": 900},
        )
        self.assertEqual(again.status_code, 200, again.text)
        self.assertTrue(again.json()["already_done"])
        self.assertEqual(again.json()["grant"]["status"], "applied")
        self.assertEqual(again.json()["grant"]["applied_at"], claimed.json()["grant"]["applied_at"])
        self.assertEqual(self.client.get("/api/exp-grants", params={"user_id": 900}).json()["grants"], [])

        failed = self.client.post(
            f"/api/bot-sync/exp-grants/{other_grant['id']}/complete",
            headers=BOT_HEADERS,
            json={"status": "failed", "error": "ledger skipped"},
        )
        self.assertEqual(failed.status_code, 200, failed.text)
        blocked = self.client.post(
            f"/api/exp-grants/{other_grant['id']}/claim",
            json={"user_id": 902},
        )
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(self.client.get("/api/exp-grants", params={"user_id": 902}).json()["grants"], [])


if __name__ == "__main__":
    unittest.main()
