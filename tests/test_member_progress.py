import os
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

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

    def put_profile(self, **profile):
        return self.client.put(
            "/api/members/profile",
            headers={"X-Telegram-Init-Data": "member-init"},
            json={"profile": profile},
        ).json()["profile"]

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

    def test_concurrent_saves_preserve_both_progression_updates(self):
        with mock.patch.object(main, "find_verified_alcove_user", return_value=None):
            member_progress.save_profile(
                "4242",
                {"owned": {"feedColors": ["base"]}},
            )

            real_load = member_progress.load_profile
            both_reads_complete = threading.Barrier(2)

            def synchronized_load(user_id):
                result = real_load(user_id)
                both_reads_complete.wait(timeout=5)
                return result

            with mock.patch.object(
                member_progress,
                "load_profile",
                side_effect=synchronized_load,
            ):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [
                        pool.submit(
                            member_progress.save_profile,
                            "4242",
                            {"owned": {"feedColors": [colour]}},
                        )
                        for colour in ("gold", "rose")
                    ]
                    for future in futures:
                        future.result()

            stored = member_progress.load_profile("4242")
            self.assertCountEqual(
                stored["owned"]["feedColors"],
                ["base", "gold", "rose"],
            )

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

    def test_login_streak_resets_after_a_gap_and_keeps_unique_days(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 4, "loginStreak": 4},
                    "loginDayLast": "2026-09-04",
                    "updated_at": "2026-09-04T21:00:00+00:00",
                },
            },
        )
        resumed = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 5, "loginStreak": 1},
                    "loginDayLast": "2026-09-10",
                    "updated_at": "2026-09-10T09:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(resumed["loginDayLast"], "2026-09-10")
        self.assertEqual(resumed["stats"]["loginStreak"], 1)
        self.assertGreaterEqual(resumed["stats"]["loginDays"], 5)

        fetched = self.client.get("/api/members/profile", headers={"X-Telegram-Init-Data": "member-init"})
        self.assertEqual(fetched.json()["profile"]["loginDayLast"], "2026-09-10")
        self.assertEqual(fetched.json()["profile"]["stats"]["loginStreak"], 1)

    def test_login_streak_is_not_restored_when_cloud_copy_lacks_login_day(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 4, "loginStreak": 4},
                    "updated_at": "2026-09-04T21:00:00+00:00",
                },
            },
        )
        resumed = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 5, "loginStreak": 1},
                    "loginDayLast": "2026-09-10",
                    "updated_at": "2026-09-10T09:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(resumed["loginDayLast"], "2026-09-10")
        self.assertEqual(resumed["stats"]["loginStreak"], 1)
        self.assertGreaterEqual(resumed["stats"]["loginDays"], 5)

    def test_consecutive_login_days_keep_the_running_streak(self):
        self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 29, "loginStreak": 29},
                    "loginDayLast": "2026-09-01",
                    "updated_at": "2026-09-01T21:00:00+00:00",
                },
            },
        )
        continued = self.client.put(
            "/api/members/profile",
            json={
                "init_data": "member-init",
                "profile": {
                    "stats": {"loginDays": 1, "loginStreak": 1},
                    "loginDayLast": "2026-09-02",
                    "updated_at": "2026-09-02T08:00:00+00:00",
                },
            },
        ).json()["profile"]
        self.assertEqual(continued["loginDayLast"], "2026-09-02")
        self.assertEqual(continued["stats"]["loginStreak"], 30)
        self.assertEqual(continued["stats"]["loginDays"], 30)

    def test_server_joined_at_overrides_legacy_profile(self):
        with mock.patch.object(
            main,
            "find_verified_alcove_user",
            return_value={
                "user_id": 4242,
                "joined_at": "2025-11-03T20:15:00+00:00",
                "first_seen": "2026-06-30T00:00:00+00:00",
                "verified_at": "2026-07-01T00:00:00+00:00",
            },
        ):
            saved = self.put_profile(memberSince="June 30 2026", level=1)

        self.assertEqual(saved["memberSince"], "2025-11-03T20:15:00+00:00")

    def test_client_cannot_spoof_member_since(self):
        with mock.patch.object(
            main,
            "find_verified_alcove_user",
            return_value={
                "user_id": 4242,
                "joined_at": "2025-11-03T20:15:00+00:00",
            },
        ):
            saved = self.put_profile(memberSince="2040-01-01T00:00:00+00:00")

        self.assertEqual(saved["memberSince"], "2025-11-03T20:15:00+00:00")

    def test_legacy_placeholder_becomes_empty_without_evidence(self):
        with mock.patch.object(main, "find_verified_alcove_user", return_value=None):
            saved = self.put_profile(memberSince="June 30 2026")

        self.assertEqual(saved["memberSince"], "")

    def test_member_since_resolver_uses_fallback_order(self):
        cases = (
            (
                {
                    "joined_at": "2025-11-03T20:15:00Z",
                    "first_seen": "2026-01-01T00:00:00Z",
                    "verified_at": "2026-02-01T00:00:00Z",
                },
                "2025-11-03T20:15:00+00:00",
            ),
            (
                {
                    "joined_at": "",
                    "first_seen": "2026-01-01T00:00:00Z",
                    "verified_at": "2026-02-01T00:00:00Z",
                },
                "2026-01-01T00:00:00+00:00",
            ),
            (
                {
                    "joined_at": "bad",
                    "first_seen": "",
                    "verified_at": "2026-02-01T00:00:00Z",
                },
                "2026-02-01T00:00:00+00:00",
            ),
        )
        for user, expected in cases:
            with self.subTest(user=user), mock.patch.object(
                main, "find_verified_alcove_user", return_value=user
            ):
                self.assertEqual(member_progress.resolve_member_since("4242"), expected)

    def test_invalid_member_since_is_rejected(self):
        self.assertEqual(member_progress.canonical_member_since("not-a-date"), "")
        self.assertEqual(member_progress.canonical_member_since("June 30 2026"), "")

    def test_non_numeric_public_profile_id_remains_a_safe_miss(self):
        response = self.client.get(
            "/api/members/public-profile", params={"user_id": "not-a-user"}
        )

        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["found"])

    def test_fox_fallback_schema_migrates_joined_at(self):
        with sqlite3.connect(":memory:") as con:
            con.execute(
                "CREATE TABLE user_profiles "
                "(user_id INTEGER PRIMARY KEY, first_seen TEXT, verified_at TEXT)"
            )

            main.ensure_fox_read_tables(con)

            columns = {
                row[1] for row in con.execute("PRAGMA table_info(user_profiles)")
            }
        self.assertIn("joined_at", columns)

    def test_existing_valid_member_since_survives_roster_outage(self):
        with mock.patch.object(
            main,
            "find_verified_alcove_user",
            return_value={"joined_at": "2025-11-03T20:15:00+00:00"},
        ):
            self.put_profile(level=2)

        with mock.patch.object(main, "find_verified_alcove_user", return_value=None):
            saved = self.put_profile(
                memberSince="2040-01-01T00:00:00+00:00",
                level=3,
            )

        self.assertEqual(saved["memberSince"], "2025-11-03T20:15:00+00:00")

    def test_public_profile_uses_the_same_server_date(self):
        with mock.patch.object(
            main,
            "find_verified_alcove_user",
            return_value={"user_id": 4242, "joined_at": "2025-11-03T20:15:00+00:00"},
        ):
            self.put_profile(level=2)
            card = self.client.get(
                "/api/members/public-profile", params={"user_id": "4242"}
            ).json()

        self.assertEqual(card["profile"]["memberSince"], "2025-11-03T20:15:00+00:00")

    def test_public_profile_returns_safe_card(self):
        missing = self.client.get("/api/members/public-profile", params={"user_id": "9999"})
        self.assertEqual(missing.status_code, 200)
        self.assertFalse(missing.json()["found"])
        self.assertIsNone(missing.json()["profile"])
        self.assertEqual(missing.json()["equipped_achievements"], [])

        with mock.patch.object(
            main,
            "find_verified_alcove_user",
            return_value={"joined_at": "2026-01-15T00:00:00+00:00"},
        ):
            saved = self.client.put(
                "/api/members/profile",
                json={
                    "init_data": "member-init",
                    "profile": {
                        "level": 7,
                        "memberSince": "2040-01-01T00:00:00+00:00",
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
            card = self.client.get(
                "/api/members/public-profile", params={"user_id": "4242"}
            ).json()

        self.assertEqual(saved.status_code, 200)
        self.assertEqual(saved.json()["profile"]["equippedAchievements"][0]["name"], "First Pulse")
        self.assertTrue(card["found"])
        self.assertEqual(card["profile"]["level"], 7)
        self.assertEqual(card["profile"]["memberSince"], "2026-01-15T00:00:00+00:00")
        self.assertEqual(card["feed_style"]["color"], "gold")
        self.assertEqual(card["equipped_achievements"][0]["name"], "First Pulse")
        self.assertEqual(card["equipped_achievements"][0]["description"], "Submit your first pulse question")
        self.assertNotIn("pendingRewards", card)
        self.assertNotIn("pendingRewards", card["profile"])
        self.assertNotIn("claimReceipts", card["profile"])

    def test_merge_pending_rewards_prefers_newer_and_drops_receipted(self):
        older = {
            "updated_at": "2026-09-24T03:50:00+00:00",
            "level": 10,
            "pendingRewards": [
                {"id": "level_5", "sourceKey": "level_5", "opened": False, "items": []},
                {"id": "level_7", "sourceKey": "level_7", "opened": False, "items": []},
            ],
            "claimReceipts": {},
            "owned": {"feedColors": ["base"], "feedSkins": [], "feedStickers": [], "feedBackdrops": [], "titles": []},
        }
        newer = {
            "updated_at": "2026-09-24T04:00:00+00:00",
            "level": 10,
            "pendingRewards": [],
            "claimReceipts": {"level_5": {"claimedAt": "2026-09-24T03:59:00+00:00"}},
            "owned": {"feedColors": ["system_blue"], "feedSkins": [], "feedStickers": [], "feedBackdrops": ["fox_online_backdrop2"], "titles": []},
        }
        merged = member_progress.merge_profiles(older, newer)
        self.assertEqual(merged["pendingRewards"], [])
        self.assertIn("level_5", merged["claimReceipts"])
        self.assertIn("system_blue", merged["owned"]["feedColors"])
        self.assertIn("fox_online_backdrop2", merged["owned"]["feedBackdrops"])

        # Receipted rows on the newer copy must be stripped even when still listed.
        still_listed = {
            "updated_at": "2026-09-24T04:05:00+00:00",
            "level": 10,
            "pendingRewards": [
                {"id": "level_8", "sourceKey": "level_8", "opened": False, "items": []},
                {"id": "level_9", "sourceKey": "level_9", "opened": False, "items": [{"type": "color", "id": "gold"}]},
            ],
            "claimReceipts": {"level_8": {"claimedAt": "2026-09-24T04:01:00+00:00"}},
            "owned": {"feedColors": ["gold"], "feedSkins": [], "feedStickers": [], "feedBackdrops": [], "titles": []},
        }
        normalized = member_progress.normalize_profile(still_listed)
        self.assertEqual([row["id"] for row in normalized["pendingRewards"]], ["level_9"])


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

