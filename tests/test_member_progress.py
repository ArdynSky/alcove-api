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

from fastapi.testclient import TestClient

from api import main, member_progress


class MemberProgressTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self._orig_secret = main.BOT_SYNC_SECRET
        main.BOT_SYNC_SECRET = "test-admin-secret"
        self._orig_resolve = member_progress.resolve_telegram_user
        member_progress.resolve_telegram_user = lambda init_data: {
            "id": 4242,
            "username": "member",
            "init_data": init_data,
        }
        if os.path.exists(member_progress._db_path()):
            os.remove(member_progress._db_path())

    def tearDown(self):
        member_progress.resolve_telegram_user = self._orig_resolve
        main.BOT_SYNC_SECRET = self._orig_secret

    def test_empty_profile_then_put_and_get(self):
        missing = self.client.get("/api/members/profile", headers={"X-Telegram-Init-Data": "member-init"})
        self.assertEqual(missing.status_code, 200)
        self.assertFalse(missing.json()["found"])

        saved = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 4,
                    "exp": 80,
                    "owned": {"feedColors": ["base", "gold"], "feedSkins": ["foxlove"]},
                    "feed": {"color": "gold", "skin": "foxlove"},
                    "updated_at": "2026-08-29T12:00:00+00:00",
                },
            },
        )
        self.assertEqual(saved.status_code, 200)
        body = saved.json()["profile"]
        self.assertEqual(body["level"], 4)
        self.assertIn("gold", body["owned"]["feedColors"])

        again = self.client.get("/api/members/profile", headers={"X-Telegram-Init-Data": "member-init"})
        self.assertTrue(again.json()["found"])
        self.assertEqual(again.json()["profile"]["level"], 4)

    def test_merge_unions_owned_and_keeps_highest_level(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 3,
                    "exp": 10,
                    "owned": {"feedColors": ["base", "gold"]},
                    "updated_at": "2026-08-29T10:00:00+00:00",
                },
            },
        )
        merged = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 2,
                    "exp": 99,
                    "owned": {"feedColors": ["rose"], "feedSkins": ["pulse"]},
                    "feed": {"color": "rose"},
                    "updated_at": "2026-08-29T11:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(merged["level"], 3)
        self.assertEqual(sorted(merged["owned"]["feedColors"]), ["base", "gold", "rose"])
        self.assertEqual(merged["owned"]["feedSkins"], ["pulse"])
        self.assertEqual(merged["feed"]["color"], "rose")

    def test_admin_can_read_and_write_any_user(self):
        written = self.client.put(
            "/api/members/8385145826/profile",
            json={
                "admin_secret": "test-admin-secret",
                "profile": {
                    "level": 6,
                    "owned": {"feedSkins": ["spotlight"]},
                    "updated_at": "2026-08-29T12:00:00+00:00",
                },
            },
        )
        self.assertEqual(written.status_code, 200)
        fetched = self.client.get(
            "/api/members/8385145826/profile",
            params={"admin_secret": "test-admin-secret"},
        )
        self.assertTrue(fetched.json()["found"])
        self.assertEqual(fetched.json()["profile"]["level"], 6)

    def test_reset_token_replaces_progress(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 8,
                    "exp": 40,
                    "owned": {"feedColors": ["gold"]},
                    "progressionResetToken": "old",
                    "updated_at": "2026-08-29T10:00:00+00:00",
                },
            },
        )
        reset = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 1,
                    "exp": 0,
                    "owned": {"feedColors": ["base"]},
                    "progressionResetToken": "new-reset",
                    "updated_at": "2026-08-29T12:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(reset["level"], 1)
        self.assertEqual(reset["owned"]["feedColors"], ["base"])
        self.assertEqual(reset["progressionResetToken"], "new-reset")

    def test_newer_reset_token_wins_over_later_stale_timestamp(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 1,
                    "exp": 0,
                    "stats": {"pulseSubmitted": 0},
                    "progressionResetToken": "reset-1756710999999",
                    "progressionResetAt": "2026-09-01T08:00:00+00:00",
                    "updated_at": "2026-08-29T10:00:00+00:00",
                },
            },
        )
        stale = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 12,
                    "exp": 400,
                    "stats": {"pulseSubmitted": 10},
                    "progressionResetToken": "reset-1756700000000",
                    "updated_at": "2026-09-01T12:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(stale["level"], 1)
        self.assertEqual(stale["exp"], 0)
        self.assertEqual(stale["stats"].get("pulseSubmitted"), 0)
        self.assertEqual(stale["progressionResetToken"], "reset-1756710999999")

    def test_dismissed_new_unlocks_are_not_unioned_back(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 2,
                    "newUnlocks": ["sticker:heart", "sticker:star"],
                    "updated_at": "2026-09-01T10:00:00+00:00",
                },
            },
        )
        remaining = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 2,
                    "newUnlocks": ["sticker:heart"],
                    "updated_at": "2026-09-01T12:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(remaining["newUnlocks"], ["sticker:heart"])

    def test_spotlight_colour_set_unions_and_counts(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "spotlightColourSet": ["purple", "pink", "blue"],
                    "updated_at": "2026-09-01T10:00:00+00:00",
                },
            },
        )
        merged = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "spotlightColourSet": ["green", "gold"],
                    "updated_at": "2026-09-01T11:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(sorted(merged["spotlightColourSet"]), ["blue", "gold", "green", "purple"])
        self.assertEqual(merged["stats"]["spotlightColours"], 4)
        self.assertNotIn("pink", merged["spotlightColourSet"])

    def test_public_profile_returns_safe_card(self):
        missing = self.client.get("/api/members/public-profile", params={"user_id": "9999"})
        self.assertEqual(missing.status_code, 200)
        self.assertFalse(missing.json()["found"])
        self.assertIsNone(missing.json()["profile"])
        self.assertEqual(missing.json()["equipped_achievements"], [])

        saved = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "level": 7,
                    "memberSince": "2026-01-15T00:00:00+00:00",
                    "title": "Pulse",
                    "feed": {"color": "gold", "skin": "foxlove"},
                    "equippedAchievements": [
                        {
                            "key": "first-pulse",
                            "name": "First Pulse",
                            "description": "Submit your first pulse question",
                            "image": "assets/icons/pulse.png",
                        }
                    ],
                    "pendingRewards": [{"id": "secret"}],
                    "claimReceipts": {"x": 1},
                    "updated_at": "2026-08-30T12:00:00+00:00",
                },
            },
        )
        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.json()["profile"]["equippedAchievements"][0]["name"], "First Pulse")

        card = self.client.get("/api/members/public-profile", params={"user_id": "4242"}).json()
        self.assertTrue(card["found"])
        self.assertEqual(card["profile"]["level"], 7)
        self.assertEqual(card["profile"]["memberSince"], "2026-01-15T00:00:00+00:00")
        self.assertEqual(card["feed_style"]["color"], "gold")
        self.assertEqual(card["equipped_achievements"][0]["name"], "First Pulse")
        self.assertEqual(card["equipped_achievements"][0]["description"], "Submit your first pulse question")
        self.assertNotIn("pendingRewards", card)
        self.assertNotIn("pendingRewards", card["profile"])
        self.assertNotIn("claimReceipts", card["profile"])


class LiveAppStateBandwidthTests(unittest.TestCase):
    def test_live_view_omits_heavy_collections(self):
        original_comments = list(main.approved_comments)
        original_entries = list(main.wheel_entries)
        original_notifications = list(main.notification_feed)
        original_archive = list(main.archived_wheel_entries)
        try:
            main.approved_comments[:] = [
                {"id": index, "text": f"hello {index}", "display_name": "Sam"}
                for index in range(60)
            ]
            main.notification_feed[:] = [{"id": 1, "text": "noise"}]
            main.archived_wheel_entries[:] = [{"id": 1, "data": {"display_name": "Old"}}]
            payload = main.get_app_state("live")
            self.assertEqual(payload["view"], "live")
            self.assertEqual(len(payload["approved_comments_list"]), 40)
            self.assertEqual(payload["pending_comments_list"], [])
            self.assertEqual(payload["notifications"], [])
            self.assertEqual(payload["video_reviews"], [])
            self.assertEqual(payload["room_qa_archive"], [])
            self.assertEqual(payload["poll_history"], [])
            self.assertEqual(payload["media_submissions"], [])
            self.assertEqual(payload["room_users"], [])
            self.assertNotIn("paths", payload)
            full = main.get_app_state()
            self.assertGreaterEqual(len(full["approved_comments_list"]), 60)
            self.assertIn("paths", full)
        finally:
            main.approved_comments[:] = original_comments
            main.wheel_entries[:] = original_entries
            main.notification_feed[:] = original_notifications
            main.archived_wheel_entries[:] = original_archive

    def test_archived_wheel_entries_are_capped(self):
        original = list(main.archived_wheel_entries)
        try:
            main.archived_wheel_entries[:] = [{"id": index} for index in range(200)]
            main.trim_list_in_place(main.archived_wheel_entries, main.MAX_ARCHIVED_WHEEL_ENTRIES)
            self.assertLessEqual(len(main.archived_wheel_entries), main.MAX_ARCHIVED_WHEEL_ENTRIES)
            self.assertEqual(main.archived_wheel_entries[0]["id"], 200 - main.MAX_ARCHIVED_WHEEL_ENTRIES)
        finally:
            main.archived_wheel_entries[:] = original
