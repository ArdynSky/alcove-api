from api import agendas


def test_agenda_times_recalculate(monkeypatch, tmp_path):
    monkeypatch.setattr(agendas, "DB_PATH", tmp_path / "agendas.sqlite3")
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
    assert created["total_duration_minutes"] == 20
    assert created["items"][0]["starts_at"] == "2026-08-21T18:00:00+00:00"
    assert created["items"][1]["starts_at"] == "2026-08-21T18:05:00+00:00"
    assert created["finishes_at"] == "2026-08-21T18:20:00+00:00"


def test_only_one_agenda_is_active(monkeypatch, tmp_path):
    monkeypatch.setattr(agendas, "DB_PATH", tmp_path / "agendas.sqlite3")
    first = agendas.create_agenda(agendas.AgendaPayload(
        title="First", event_date="2026-08-21", start_time="19:00", status="active"
    ))
    second = agendas.create_agenda(agendas.AgendaPayload(
        title="Second", event_date="2026-08-22", start_time="19:00", status="active"
    ))
    assert agendas.get_agenda(first["id"])["status"] == "finished"
    assert agendas.active_agenda()["agenda"]["id"] == second["id"]


def test_agenda_progress_tracks_current_and_completed_items(monkeypatch, tmp_path):
    monkeypatch.setattr(agendas, "DB_PATH", tmp_path / "agendas.sqlite3")
    created = agendas.create_agenda(agendas.AgendaPayload(
        title="Live", event_date="2026-08-22", start_time="19:00", status="active",
        items=[agendas.AgendaItem(title="Discussion"), agendas.AgendaItem(title="Debate")],
    ))
    first, second = created["items"]
    active = agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="activate", item_id=first["id"]))
    assert active["current_item_id"] == first["id"]
    assert active["completed_item_ids"] == []
    advanced = agendas.set_progress(created["id"], agendas.AgendaProgressPayload(action="advance", item_id=second["id"]))
    assert advanced["current_item_id"] == second["id"]
    assert advanced["completed_item_ids"] == [first["id"]]


def test_agenda_keeps_schedule_image_and_holding_video(monkeypatch, tmp_path):
    monkeypatch.setattr(agendas, "DB_PATH", tmp_path / "agendas.sqlite3")
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
    assert created["schedule_image_url"] == "https://cdn.example/agenda.png"
    assert created["holding_video_url"] == "https://cdn.example/hold.mp4"
    assert created["session_name"] == "Wheel of Desire Wednesday"
    assert created["items"][0]["title_1"] == "Welcome"

    fetched = agendas.get_agenda(created["id"])
    assert fetched["schedule_image_url"] == "https://cdn.example/agenda.png"
    assert fetched["holding_video_url"] == "https://cdn.example/hold.mp4"

    updated = agendas.update_agenda(created["id"], agendas.AgendaPayload(
        title="Friday Live VC",
        event_date="2026-08-21",
        start_time="19:00",
        timezone="Europe/London",
        schedule_image_url="https://cdn.example/agenda.png",
        holding_video_url="https://cdn.example/hold.mp4",
        items=[agendas.AgendaItem(title="Welcome: Hello", duration_minutes=5, title_1="Welcome")],
    ))
    assert updated["schedule_image_url"] == "https://cdn.example/agenda.png"
    assert updated["items"][0]["title_1"] == "Welcome"

    copy = agendas.duplicate_agenda(created["id"])
    assert copy["id"] != created["id"]
    assert copy["schedule_image_url"] == "https://cdn.example/agenda.png"
    assert copy["holding_video_url"] == "https://cdn.example/hold.mp4"
