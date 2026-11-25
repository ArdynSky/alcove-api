"""Discussion thought overlay for the shared OBS audience card."""

from fastapi.testclient import TestClient


def test_room_qa_overlay_show_and_clear(monkeypatch):
    from api import main

    main.room_qa_items[:] = [
        {
            "id": 42,
            "discussion_id": "disc_test",
            "display_name": "Sam · Rehearsal",
            "username": "rehearse_sam",
            "kind": "comment",
            "question": "Loving the pace of this dry run.",
            "status": "active",
            "created_at": "2026-11-25T12:00:00Z",
        }
    ]
    main.room_qa_overlay_id = None
    monkeypatch.setattr(main, "persist_live_room", lambda *a, **k: None)

    client = TestClient(main.app)
    empty = client.get("/api/room-qa/overlay")
    assert empty.status_code == 200
    assert empty.json()["overlay"] is None

    shown = client.post("/api/room-qa/overlay", json={"item_id": 42})
    assert shown.status_code == 200
    overlay = shown.json()["overlay"]
    assert overlay["id"] == 42
    assert overlay["side"] == ""
    assert overlay["reason"] == "Loving the pace of this dry run."
    assert overlay["source"] == "discussion"

    app_state = client.get("/api/app-state").json()
    assert app_state["room_qa_overlay"]["id"] == 42

    cleared = client.post("/api/room-qa/overlay", json={"item_id": None})
    assert cleared.status_code == 200
    assert cleared.json()["overlay"] is None
