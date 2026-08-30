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

from api import main


class RoomQATests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        self._items = list(main.room_qa_items)
        self._archive = list(main.room_qa_archive)
        self._discussion = dict(main.state.get("room_discussion") or {})
        main.room_qa_items.clear()
        main.room_qa_archive.clear()
        main.state["room_discussion"] = {
            "discussion_id": None,
            "title": "",
            "duration_minutes": None,
            "started_at": None,
            "ends_at": None,
            "status": "idle",
        }

    def tearDown(self):
        main.room_qa_items[:] = self._items
        main.room_qa_archive[:] = self._archive
        main.state["room_discussion"] = self._discussion

    def _start_discussion(self):
        started = self.client.post(
            "/api/room-discussion/start",
            json={"title": "Live discussion", "duration_minutes": 10},
        )
        self.assertEqual(started.status_code, 200)
        self.assertEqual(started.json()["status"], "ok")

    def test_question_answer_stays_on_item(self):
        self._start_discussion()
        created = self.client.post(
            "/api/room-qa",
            json={"display_name": "Member", "question": "What is the topic?"},
        )
        self.assertEqual(created.status_code, 200)
        item = created.json()["item"]
        self.assertEqual(item["kind"], "question")
        self.assertIsNone(item["answer"])

        answered = self.client.post(
            f"/api/room-qa/{item['id']}/answer",
            json={"answer": "Tonight we are talking about the wheel."},
        )
        self.assertEqual(answered.status_code, 200)
        self.assertEqual(answered.json()["item"]["answer"], "Tonight we are talking about the wheel.")

        listed = self.client.get("/api/room-qa")
        self.assertEqual(listed.status_code, 200)
        self.assertEqual(listed.json()[0]["answer"], "Tonight we are talking about the wheel.")

    def test_comment_goes_to_host_inbox_not_answerable(self):
        self._start_discussion()
        created = self.client.post(
            "/api/room-qa",
            json={"display_name": "Member", "kind": "comment", "comment": "Loved this prompt."},
        )
        self.assertEqual(created.status_code, 200)
        item = created.json()["item"]
        self.assertEqual(item["kind"], "comment")
        self.assertEqual(item["question"], "Loved this prompt.")

        refused = self.client.post(
            f"/api/room-qa/{item['id']}/answer",
            json={"answer": "This should not attach."},
        )
        self.assertEqual(refused.json()["status"], "error")
        self.assertIn("Comments", refused.json()["message"])


if __name__ == "__main__":
    unittest.main()
