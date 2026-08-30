import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_DEBATE_DIR", _temp_dir.name)
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from api import debate_audience, debate_games


class DebateArchiveTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        debate_games.DATA_DIR = self.root
        debate_games.DB_PATH = self.root / "debate.sqlite3"
        debate_audience.DB_PATH = self.root / "audience.sqlite3"
        debate_games._save(debate_games._default_state())

    def _seed_finished_debate(self):
        state = debate_games._default_state()
        state.update({
            "session_id": "debate-archive-1",
            "status": "results",
            "title": "Live Debate",
            "statement": "Pineapple belongs on pizza",
            "created_at": "2026-08-30T18:00:00+00:00",
            "contestants": [
                {"user_id": "10", "display_name": "Sam", "username": "sam", "side": "FOR"},
                {"user_id": "20", "display_name": "Alex", "username": "alex", "side": "AGAINST"},
            ],
        })
        debate_games._save(state)
        with debate_games._conn() as con:
            con.execute(
                "INSERT INTO debate_votes(session_id,user_id,contestant_id,created_at) VALUES(?,?,?,?)",
                ("debate-archive-1", "100", "10", "2026-08-30T18:10:00+00:00"),
            )
            con.execute(
                "INSERT INTO debate_votes(session_id,user_id,contestant_id,created_at) VALUES(?,?,?,?)",
                ("debate-archive-1", "101", "10", "2026-08-30T18:10:00+00:00"),
            )
            con.execute(
                "INSERT INTO debate_votes(session_id,user_id,contestant_id,created_at) VALUES(?,?,?,?)",
                ("debate-archive-1", "102", "20", "2026-08-30T18:10:00+00:00"),
            )
            con.commit()
        debate_audience._ensure_settings("debate-archive-1")
        with debate_audience._LOCK:
            con = debate_audience._conn()
            con.execute(
                """
                INSERT INTO debate_audience_thoughts(id,session_id,user_id,display_name,username,side,reason,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                ("thought-1", "debate-archive-1", "200", "Riley", "riley", "FOR", "It is sweet and salty", "2026-08-30T18:05:00+00:00", "2026-08-30T18:05:00+00:00"),
            )
            con.commit()
            con.close()
        return state

    def test_ending_debate_archives_topic_result_and_thoughts(self):
        self._seed_finished_debate()
        ended = debate_games.end()
        self.assertEqual(ended["status"], "ended")
        archived = debate_audience.archive_debates()["debates"]
        self.assertEqual(len(archived), 1)
        card = archived[0]
        self.assertEqual(card["statement"], "Pineapple belongs on pizza")
        self.assertEqual(card["topic"], "Pineapple belongs on pizza")
        self.assertEqual(card["winner_user_id"], "10")
        self.assertEqual(card["vote_total"], 3)
        self.assertEqual(len(card["audience_thoughts"]), 1)
        self.assertEqual(card["audience_thoughts"][0]["reason"], "It is sweet and salty")
        self.assertEqual(card["contestants"][0]["percentage"], 66.7)

    def test_starting_next_debate_keeps_previous_archive(self):
        self._seed_finished_debate()
        debate_games.end()
        debate_games.start(debate_games.StartPayload(statement="Cats are better than dogs"))
        archived = debate_audience.archive_debates()["debates"]
        self.assertEqual(len(archived), 1)
        self.assertEqual(archived[0]["statement"], "Pineapple belongs on pizza")
        self.assertEqual(archived[0]["audience_thoughts"][0]["reason"], "It is sweet and salty")
        listed = debate_audience.archive_debates()
        self.assertEqual(listed["debates"][0]["vote_total"], 3)

    def test_starting_next_debate_archives_previous_without_end(self):
        self._seed_finished_debate()
        started = debate_games.start(debate_games.StartPayload(statement="Cats are better than dogs"))
        self.assertNotEqual(started["session_id"], "debate-archive-1")
        archived = debate_audience.archive_debates()["debates"]
        self.assertEqual(len(archived), 1)
        self.assertEqual(archived[0]["session_id"], "debate-archive-1")
        self.assertEqual(archived[0]["statement"], "Pineapple belongs on pizza")
        self.assertEqual(archived[0]["audience_thoughts"][0]["reason"], "It is sweet and salty")

    def test_archive_http_routes_are_registered(self):
        from fastapi.testclient import TestClient
        from api import main

        self._seed_finished_debate()
        client = TestClient(main.app)
        finalized = client.post("/api/debate/archive/finalize")
        self.assertEqual(finalized.status_code, 200, finalized.text)
        self.assertEqual(finalized.json()["statement"], "Pineapple belongs on pizza")
        listed = client.get("/api/debate/archive/debates")
        self.assertEqual(listed.status_code, 200, listed.text)
        debates = listed.json()["debates"]
        self.assertEqual(len(debates), 1)
        self.assertEqual(debates[0]["topic"], "Pineapple belongs on pizza")
        self.assertEqual(debates[0]["audience_thoughts"][0]["reason"], "It is sweet and salty")
        ended = client.post("/api/debate/end")
        self.assertEqual(ended.status_code, 200, ended.text)
        again = client.get("/api/debate/archive/debates")
        self.assertEqual(len(again.json()["debates"]), 1)
