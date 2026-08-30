import os
import tempfile
import unittest
from pathlib import Path

_temp_dir = tempfile.TemporaryDirectory()
os.environ.setdefault("ALCOVE_STATE_DB_PATH", os.path.join(_temp_dir.name, "state.db"))
os.environ.setdefault("ALCOVE_RUNTIME_STATE_PATH", os.path.join(_temp_dir.name, "runtime.json"))
os.environ.setdefault("FEATURE_FLAGS_PATH", os.path.join(_temp_dir.name, "feature_flags.json"))
os.environ.setdefault("PULSE_SETTINGS_PATH", os.path.join(_temp_dir.name, "pulse_settings.json"))
os.environ.setdefault("SAFETY_SETTINGS_PATH", os.path.join(_temp_dir.name, "safety_settings.json"))
os.environ.setdefault("VERIFY_FLOW_LOG_PATH", os.path.join(_temp_dir.name, "verification_flow_events.jsonl"))
os.environ.setdefault("BOT_SYNC_SECRET", "test-admin-secret")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-bot-token")

from fastapi.testclient import TestClient

from api import agendas, main


class AgendaTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        agendas.DB_PATH = self.root / "agendas.sqlite3"

    def test_agenda_times_recalculate(self):
        created = agendas.create_agenda(agendas.AgendaPayload(
            title="Friday Live VC",
            event_date="2026-08-21",
            start_time="19:00",
            timezone="Europe/London",
            items=[
                agendas.AgendaItem(title="Welcome", duration_minutes=5),
                agendas.AgendaItem(title="Debate", duration_minutes=15),
            ],
        ))
        self.assertEqual(created["total_duration_minutes"], 20)
        self.assertEqual(created["items"][0]["starts_at"], "2026-08-21T18:00:00+00:00")
        self.assertEqual(created["items"][1]["starts_at"], "2026-08-21T18:05:00+00:00")
        self.assertEqual(created["finishes_at"], "2026-08-21T18:20:00+00:00")

    def test_only_one_agenda_is_active(self):
        first = agendas.create_agenda(agendas.AgendaPayload(
            title="First", event_date="2026-08-21", start_time="19:00", status="active"
        ))
        second = agendas.create_agenda(agendas.AgendaPayload(
            title="Second", event_date="2026-08-22", start_time="19:00", status="active"
        ))
        self.assertEqual(agendas.get_agenda(first["id"])["status"], "finished")
        self.assertEqual(agendas.active_agenda()["agenda"]["id"], second["id"])

    def test_agenda_progress_tracks_current_and_completed_items(self):
        created = agendas.create_agenda(agendas.AgendaPayload(
            title="Live", event_date="2026-08-22", start_time="19:00", status="active",
            items=[agendas.AgendaItem(title="Discussion"), agendas.AgendaItem(title="Debate")],
        ))
        first, second = created["items"]
        active = agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="activate", item_id=first["id"]))
        self.assertEqual(active["current_item_id"], first["id"])
        self.assertEqual(active["completed_item_ids"], [])
        advanced = agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="advance", item_id=second["id"]))
        self.assertEqual(advanced["current_item_id"], second["id"])
        self.assertEqual(advanced["completed_item_ids"], [first["id"]])

    def test_agenda_keeps_schedule_image_and_holding_video(self):
        created = agendas.create_agenda(agendas.AgendaPayload(
            title="Friday Live VC",
            session_name="Wheel of Desire Wednesday",
            event_date="2026-08-21",
            start_time="19:00",
            timezone="Europe/London",
            schedule_image_url="https://cdn.example/agenda.png",
            holding_video_url="https://cdn.example/hold.mp4",
            items=[
                agendas.AgendaItem(
                    title="Welcome: Hello",
                    duration_minutes=5,
                    title_1="Welcome",
                    title_2="Hello",
                )
            ],
        ))
        self.assertEqual(created["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(created["holding_video_url"], "https://cdn.example/hold.mp4")
        self.assertEqual(created["session_name"], "Wheel of Desire Wednesday")
        self.assertEqual(created["items"][0]["title_1"], "Welcome")

        fetched = agendas.get_agenda(created["id"])
        self.assertEqual(fetched["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(fetched["holding_video_url"], "https://cdn.example/hold.mp4")

        updated = agendas.update_agenda(created["id"], agendas.AgendaPayload(
            title="Friday Live VC",
            event_date="2026-08-21",
            start_time="19:00",
            timezone="Europe/London",
            schedule_image_url="https://cdn.example/agenda.png",
            holding_video_url="https://cdn.example/hold.mp4",
            items=[agendas.AgendaItem(title="Welcome: Hello", duration_minutes=5, title_1="Welcome")],
        ))
        self.assertEqual(updated["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(updated["items"][0]["title_1"], "Welcome")

        copy = agendas.duplicate_agenda(created["id"])
        self.assertNotEqual(copy["id"], created["id"])
        self.assertEqual(copy["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(copy["holding_video_url"], "https://cdn.example/hold.mp4")

    def test_edit_save_without_progress_fields_keeps_runner_state(self):
        created = agendas.create_agenda(agendas.AgendaPayload(
            title="Live",
            event_date="2026-08-22",
            start_time="19:00",
            status="active",
            items=[agendas.AgendaItem(title="Discussion"), agendas.AgendaItem(title="Debate")],
        ))
        first = created["items"][0]
        agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="activate", item_id=first["id"]))
        saved = agendas.update_agenda(created["id"], agendas.AgendaPayload(
            title="Live",
            event_date="2026-08-22",
            start_time="19:00",
            status="active",
            items=[agendas.AgendaItem(**{k: v for k, v in item.items() if k != "starts_at"}) for item in created["items"]],
        ))
        self.assertEqual(saved["current_item_id"], first["id"])
        self.assertEqual(saved["completed_item_ids"], [])

    def test_host_reset_clears_progress_via_put(self):
        created = agendas.create_agenda(agendas.AgendaPayload(
            title="Live",
            event_date="2026-08-22",
            start_time="19:00",
            status="active",
            items=[agendas.AgendaItem(title="Discussion"), agendas.AgendaItem(title="Debate")],
        ))
        first, second = created["items"]
        agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="activate", item_id=first["id"]))
        agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="advance", item_id=second["id"]))
        reset = agendas.update_agenda(created["id"], agendas.AgendaPayload(
            title="Live",
            event_date="2026-08-22",
            start_time="19:00",
            status="draft",
            current_item_id=None,
            completed_item_ids=[],
            items=[agendas.AgendaItem(**{k: v for k, v in item.items() if k != "starts_at"}) for item in created["items"]],
        ))
        self.assertIsNone(reset["current_item_id"])
        self.assertEqual(reset["completed_item_ids"], [])


class AgendaHttpTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        agendas.DB_PATH = self.root / "agendas.sqlite3"
        self.client = TestClient(main.app)

    def test_host_control_payload_survives_create_get_edit(self):
        created = self.client.post("/api/agendas", json={
            "title": "Friday Live VC",
            "session_name": "Friday Live VC",
            "event_date": "2026-08-21",
            "start_time": "19:00",
            "timezone": "Europe/London",
            "status": "draft",
            "show_in_app": True,
            "schedule_image": "https://cdn.example/agenda.png",
            "holding_backdrop_url": "https://cdn.example/agenda.png",
            "holdingVideoUrl": "https://cdn.example/hold.mp4",
            "backgroundVideoUrl": "https://cdn.example/hold.mp4",
            "items": [{
                "kind": "intro",
                "title": "Welcome: Hello",
                "title_1": "Welcome",
                "title_2": "Hello",
                "duration_minutes": 5,
                "schedule_image_url": "https://cdn.example/agenda.png",
                "content_snapshot": {
                    "agenda_media": {
                        "schedule_image_url": "https://cdn.example/agenda.png",
                        "holding_video_url": "https://cdn.example/hold.mp4",
                    }
                },
            }],
        })
        self.assertEqual(created.status_code, 200, created.text)
        body = created.json()
        self.assertEqual(body["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(body["holding_video_url"], "https://cdn.example/hold.mp4")
        self.assertEqual(body["items"][0]["title_1"], "Welcome")
        self.assertEqual(body["items"][0]["content_snapshot"]["agenda_media"]["schedule_image_url"], "https://cdn.example/agenda.png")

        listed = self.client.get("/api/agendas")
        self.assertEqual(listed.status_code, 200)
        match = next(row for row in listed.json()["agendas"] if row["id"] == body["id"])
        self.assertEqual(match["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(match["items"][0]["title_1"], "Welcome")

        edited = self.client.put(f"/api/agendas/{body['id']}", json={
            "title": "Friday Live VC",
            "session_name": "Friday Live VC",
            "event_date": "2026-08-21",
            "start_time": "19:00",
            "timezone": "Europe/London",
            "status": "draft",
            "show_in_app": True,
            "schedule_image_url": "https://cdn.example/agenda.png",
            "holding_video_url": "https://cdn.example/hold.mp4",
            "items": [{
                "id": body["items"][0]["id"],
                "kind": "intro",
                "title": "Welcome: Hello",
                "title_1": "Welcome",
                "title_2": "Hello",
                "duration_minutes": 5,
                "schedule_image_url": "https://cdn.example/agenda.png",
            }],
        })
        self.assertEqual(edited.status_code, 200, edited.text)
        again = self.client.get(f"/api/agendas/{body['id']}")
        self.assertEqual(again.json()["schedule_image_url"], "https://cdn.example/agenda.png")
        self.assertEqual(again.json()["holding_video_url"], "https://cdn.example/hold.mp4")
        self.assertEqual(again.json()["items"][0]["title_1"], "Welcome")
