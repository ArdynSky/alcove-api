import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_DEBATE_DIR", _temp_dir.name)
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from api import debate_audience


class DebateAudienceAvailabilityTests(unittest.TestCase):
    def test_thoughts_are_open_through_debate_and_close_for_voting(self):
        open_stages = {
            "pooling", "registration_closed", "selected", "intro_for", "speaker_for",
            "holding_against", "intro_against", "speaker_against", "holding_vote",
        }
        for stage in open_stages:
            self.assertTrue(debate_audience._submissions_open({"status": stage}))
        self.assertFalse(debate_audience._submissions_open({"status": "voting"}))
        self.assertFalse(debate_audience._submissions_open({"status": "results"}))

    def test_new_debate_session_has_a_fresh_member_submission(self):
        debate_audience.DB_PATH = Path(tempfile.mkdtemp()) / "audience.sqlite3"
        state = {"session_id": "debate-one", "status": "pooling", "contestants": []}
        original_load = debate_audience._load
        debate_audience._load = lambda: state
        try:
            first = debate_audience.AudienceThoughtPayload(
                user_id="member-1", display_name="Member", side="FOR", reason="First debate thought"
            )
            debate_audience.audience_submit(first)
            self.assertEqual(debate_audience.audience_state("member-1")["mine"]["reason"], "First debate thought")
            state["session_id"] = "debate-two"
            fresh = debate_audience.audience_state("member-1")
            self.assertIsNone(fresh["mine"])
            self.assertEqual(fresh["count"], 0)
            self.assertTrue(fresh["submissions_open"])
        finally:
            debate_audience._load = original_load
