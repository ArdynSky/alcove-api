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

from api import main


class FeatureFlagTesterUsernamesTests(unittest.TestCase):
    def setUp(self):
        if os.path.exists(main.FEATURE_FLAGS_PATH):
            os.remove(main.FEATURE_FLAGS_PATH)

    def test_tester_usernames_round_trip(self):
        main.save_feature_flags(main.load_feature_flags(), ["@Alice", "bob", "ALICE"])
        self.assertEqual(main.load_tester_usernames(), ["alice", "bob"])

    def test_preserves_tester_usernames_when_saving_page_flags(self):
        main.save_feature_flags(main.load_feature_flags(), ["preview_user"])
        flags = main.load_feature_flags()
        flags["pages"]["cards"] = True
        main.save_feature_flags(flags)
        self.assertEqual(main.load_tester_usernames(), ["preview_user"])

    def test_get_feature_flags_includes_tester_usernames(self):
        main.save_feature_flags(main.load_feature_flags(), ["tester_one"])
        payload = main.get_feature_flags()
        self.assertEqual(payload["tester_usernames"], ["tester_one"])
        self.assertIn("pages", payload["features"])

    def test_reward_catalog_rejects_retired_effect_items(self):
        self.assertIsNone(
            main._normalize_reward_pack_item({"type": "effect", "id": "shimmer"})
        )

    def test_reward_catalog_preserves_skin_layer_layout(self):
        stamp = main._normalize_reward_pack_item(
            {"type": "skin", "id": "crest", "layout": "square"}
        )
        banner = main._normalize_reward_pack_item(
            {"type": "skin", "id": "scene", "skin_layout": "wide"}
        )
        self.assertEqual(stamp["layout"], "stamp")
        self.assertEqual(banner["layout"], "banner")

    def test_achievement_uploads_filter_retired_effect_items(self):
        achievements = main.normalize_reward_achievements(
            [
                {
                    "id": "test-achievement",
                    "name": "Test",
                    "items": [
                        {"type": "effect", "id": "sparkle"},
                        {"type": "skin", "id": "crest", "layout": "stamp"},
                    ],
                }
            ]
        )
        self.assertEqual(
            achievements[0]["items"],
            [{"type": "skin", "id": "crest", "layout": "stamp"}],
        )


if __name__ == "__main__":
    unittest.main()
