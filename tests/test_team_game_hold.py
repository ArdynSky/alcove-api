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

    def _seed_submission(self, submission_id, user_id, name):
        con = team_games._conn()
        con.execute(
            "INSERT INTO team_game_submissions(id,session_id,team_id,user_id,display_name,filename,original_name,caption,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (submission_id, "draw-1", "pair-1", user_id, name, f"{submission_id}.png", "art.png", "", team_games._iso()),
        )
        con.commit()
        con.close()

    def test_favourite_vote_can_change_until_closed(self):
        self._running_state()
        team_games.team_game_hold()
        self._seed_submission("art-a", "10", "Sam")
        self._seed_submission("art-b", "20", "Alex")
        opened = team_games.team_game_voting_open()
        self.assertEqual(opened["status"], "voting")
        self.assertTrue(opened["voting"]["open"])
        self.assertEqual(opened["obs_mode"], "gallery")
        first = team_games.team_game_vote(team_games.VotePayload(user_id="99", submission_id="art-a"))
        self.assertEqual(first["my_vote"], "art-a")
        self.assertEqual(first["vote_counts"]["art-a"], 1)
        changed = team_games.team_game_vote(team_games.VotePayload(user_id="99", submission_id="art-b"))
        self.assertEqual(changed["my_vote"], "art-b")
        self.assertEqual(changed["vote_total"], 1)
        closed = team_games.team_game_voting_close()
        self.assertFalse(closed["voting"]["open"])
        self.assertEqual(closed["status"], "results_ready")
        with self.assertRaises(team_games.HTTPException):
            team_games.team_game_vote(team_games.VotePayload(user_id="99", submission_id="art-a"))

    def test_vote_timer_locks_and_results_reveal(self):
        self._running_state()
        team_games.team_game_hold()
        self._seed_submission("art-a", "10", "Sam")
        self._seed_submission("art-b", "20", "Alex")
        opened = team_games.team_game_voting_open(duration_seconds=45)
        self.assertTrue(opened["voting"]["open"])
        self.assertTrue(opened["voting"]["ends_at"])
        team_games.team_game_vote(team_games.VotePayload(user_id="1", submission_id="art-a"))
        team_games.team_game_vote(team_games.VotePayload(user_id="2", submission_id="art-a"))
        team_games.team_game_vote(team_games.VotePayload(user_id="3", submission_id="art-b"))
        state = team_games._load_state()
        voting = dict(state.get("voting") or {})
        voting["ends_at"] = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
        state["voting"] = voting
        team_games._save_state(state)
        locked = team_games._public_state(team_games._load_state())
        self.assertEqual(locked["status"], "results_ready")
        self.assertFalse(locked["voting"]["open"])
        revealed = team_games.team_game_results(team_games.ResultsPayload(visible=True, animation_seconds=5))
        self.assertEqual(revealed["status"], "results")
        self.assertTrue(revealed["results"]["visible"])
        self.assertEqual(revealed["vote_percentages"]["art-a"], 67)
        self.assertEqual(revealed["vote_percentages"]["art-b"], 33)

    def test_obs_gallery_and_single_focus(self):
        self._running_state()
        team_games.team_game_hold()
        self._seed_submission("art-a", "10", "Sam")
        team_games.team_game_voting_open()
        gallery = team_games.team_game_obs_state()
        self.assertTrue(gallery["visible"])
        self.assertEqual(gallery["mode"], "gallery")
        self.assertEqual(len(gallery["submissions"]), 1)
        focused = team_games.team_game_obs_focus(team_games.ObsFocusPayload(mode="single", submission_id="art-a"))
        self.assertEqual(focused["obs_mode"], "single")
        single = team_games.team_game_obs_state()
        self.assertEqual(single["mode"], "single")
        self.assertEqual(single["submission"]["id"], "art-a")
        gallery_again = team_games.team_game_obs_focus(team_games.ObsFocusPayload(mode="gallery"))
        self.assertEqual(gallery_again["obs_mode"], "gallery")
        self.assertFalse((gallery_again.get("reveal") or {}).get("visible"))
        back = team_games.team_game_obs_state()
        self.assertEqual(back["mode"], "gallery")
        self.assertIsNone(back["submission"])
