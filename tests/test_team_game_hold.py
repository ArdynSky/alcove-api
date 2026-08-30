import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_TEAM_GAME_DIR", _temp_dir.name)
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from api import team_games


class TeamGameHoldTests(unittest.TestCase):
    def setUp(self):
        root = Path(tempfile.mkdtemp())
        team_games.DATA_DIR = root
        team_games.UPLOAD_DIR = root / "uploads"
        team_games.BACKGROUND_DIR = root / "backgrounds"
        team_games.DB_PATH = root / "team_game.sqlite3"
        team_games.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        team_games.BACKGROUND_DIR.mkdir(parents=True, exist_ok=True)
        team_games._save_state(team_games._default_state())

    def _running_state(self, *, expired=False):
        now = datetime.now(timezone.utc)
        state = team_games._default_state()
        state.update({
            "session_id": "draw-1",
            "status": "running",
            "title": "Drawing Challenge",
            "prompt": "Draw your partner within 2 mins",
            "started_at": (now - timedelta(minutes=3)).isoformat(),
            "ends_at": (now - timedelta(seconds=5) if expired else now + timedelta(minutes=2)).isoformat(),
            "participants": [
                {"user_id": "10", "display_name": "Sam"},
                {"user_id": "20", "display_name": "Alex"},
            ],
            "teams": [{
                "id": "pair-1",
                "label": "Pair 1",
                "members": [
                    {"user_id": "10", "display_name": "Sam"},
                    {"user_id": "20", "display_name": "Alex"},
                ],
            }],
        })
        team_games._save_state(state)
        return state

    def test_hold_endpoint_closes_uploads(self):
        self._running_state()
        held = team_games.team_game_hold()
        self.assertEqual(held["status"], "holding")
        self.assertEqual(team_games._load_state()["status"], "holding")

    def test_expired_timer_moves_to_hold(self):
        self._running_state(expired=True)
        loaded = team_games._load_state()
        self.assertEqual(loaded["status"], "holding")

    def test_submit_blocked_while_holding(self):
        self._running_state()
        team_games.team_game_hold()
        state = team_games._load_state()
        self.assertEqual(state["status"], "holding")
        self.assertTrue(state.get("status") != "running" or team_games._timer_expired(state))
