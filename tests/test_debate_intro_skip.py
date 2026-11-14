import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_DEBATE_DIR", _temp_dir.name)
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from fastapi import HTTPException

from api import debate_games


class DebateIntroSkipTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        debate_games.DATA_DIR = self.root
        debate_games.DB_PATH = self.root / "debate.sqlite3"
        debate_games._save(debate_games._default_state())

    def _seed_intro(self, status="intro_for"):
        state = debate_games._default_state()
        state.update({
            "session_id": "debate-intro-1",
            "status": status,
            "statement": "Pineapple belongs on pizza",
            "duration_seconds": 90,
            "active_side": "FOR" if status == "intro_for" else "AGAINST",
            "intro_started_at": "2099-01-01T00:00:00+00:00",
            "intro_ends_at": "2099-01-01T00:00:10+00:00",
            "contestants": [
                {"user_id": "10", "display_name": "Sam", "side": "FOR"},
                {"user_id": "20", "display_name": "Alex", "side": "AGAINST"},
            ],
        })
        debate_games._save(state)
        return state

    def test_skip_intro_for_starts_speaker_timer(self):
        self._seed_intro("intro_for")
        out = debate_games.skip_intro()
        self.assertEqual(out["status"], "speaker_for")
        self.assertEqual(out["active_side"], "FOR")
        self.assertIsNone(out["intro_ends_at"])
        self.assertTrue(out["turn_started_at"])
        self.assertTrue(out["turn_ends_at"])

    def test_skip_intro_against_starts_speaker_timer(self):
        self._seed_intro("intro_against")
        out = debate_games.skip_intro()
        self.assertEqual(out["status"], "speaker_against")
        self.assertEqual(out["active_side"], "AGAINST")
        self.assertIsNone(out["intro_ends_at"])
        self.assertTrue(out["turn_started_at"])

    def test_skip_intro_rejects_non_intro_stages(self):
        self._seed_intro("speaker_for")
        with self.assertRaises(HTTPException) as raised:
            debate_games.skip_intro()
        self.assertEqual(raised.exception.status_code, 409)

    def test_holding_during_intro_no_longer_skips_the_speaker(self):
        self._seed_intro("intro_for")
        with self.assertRaises(HTTPException) as raised:
            debate_games.holding()
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(debate_games._load()["status"], "intro_for")

    def test_holding_still_ends_an_active_speaking_turn(self):
        state = self._seed_intro("intro_for")
        state["status"] = "speaker_for"
        state["intro_ends_at"] = None
        state["turn_ends_at"] = "2099-01-01T00:00:00+00:00"
        debate_games._save(state)
        out = debate_games.holding()
        self.assertEqual(out["status"], "holding_against")


if __name__ == "__main__":
    unittest.main()
