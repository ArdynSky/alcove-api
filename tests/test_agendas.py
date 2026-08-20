from api import agendas


def test_agenda_times_recalculate(monkeypatch, tmp_path):
    monkeypatch.setattr(agendas, "DATA_DIR", tmp_path)
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
    monkeypatch.setattr(agendas, "DATA_DIR", tmp_path)
    monkeypatch.setattr(agendas, "DB_PATH", tmp_path / "agendas.sqlite3")
    first = agendas.create_agenda(agendas.AgendaPayload(
        title="First", event_date="2026-08-21", start_time="19:00", status="active"
    ))
    second = agendas.create_agenda(agendas.AgendaPayload(
        title="Second", event_date="2026-08-22", start_time="19:00", status="active"
    ))
    assert agendas.get_agenda(first["id"])["status"] == "finished"
    assert agendas.active_agenda()["agenda"]["id"] == second["id"]
