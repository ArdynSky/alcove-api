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
from api.main import VideoReview


class ReviewDuplicateTests(unittest.TestCase):
    def setUp(self):
        self.original_reviews = list(main.video_reviews)
        self.original_history = list(main.wheel_review_history)
        self.original_now = main.current_now_playing
        self.original_save = main.save_runtime_state
        self.original_ws = main.ws_broadcast_bundle
        self.original_engage = main.ensure_wheel_user_engagement
        main.video_reviews[:] = []
        main.wheel_review_history[:] = []
        main.current_now_playing = {
            "id": 99,
            "data": {"video_title": "Clip", "display_name": "Host"},
        }
        main.save_runtime_state = lambda *args, **kwargs: True
        main.ws_broadcast_bundle = lambda *args, **kwargs: None
        main.ensure_wheel_user_engagement = lambda **kwargs: None

    def tearDown(self):
        main.video_reviews[:] = self.original_reviews
        main.wheel_review_history[:] = self.original_history
        main.current_now_playing = self.original_now
        main.save_runtime_state = self.original_save
        main.ws_broadcast_bundle = self.original_ws
        main.ensure_wheel_user_engagement = self.original_engage

    def test_blocks_second_review_from_same_user(self):
        first = main.submit_review(
            VideoReview(rating=5, review="Loved it", display_name="Sam", anonymous=False, user_id=55)
        )
        second = main.submit_review(
            VideoReview(rating=3, review="Again", display_name="Sam", anonymous=False, user_id=55)
        )
        self.assertEqual(first["status"], "ok")
        self.assertEqual(second["status"], "error")
        self.assertEqual(len(main.video_reviews), 1)

    def test_allows_other_users(self):
        main.submit_review(
            VideoReview(rating=5, review="A", display_name="Sam", anonymous=False, user_id=55)
        )
        other = main.submit_review(
            VideoReview(rating=4, review="B", display_name="Alex", anonymous=False, user_id=56)
        )
        self.assertEqual(other["status"], "ok")
        self.assertEqual(len(main.video_reviews), 2)


if __name__ == "__main__":
    unittest.main()
