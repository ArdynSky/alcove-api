import sys
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def api(tmp_path, monkeypatch):
    from api import question_games as q, question_game_routes as r, custom_rewards as cr, live_room_test as auth
    from api import rehearsal_dummies as dummies

    monkeypatch.setattr(q, "DB_PATH", tmp_path / "games.sqlite3")
    monkeypatch.setattr(cr, "DB_PATH", tmp_path / "packs.sqlite3")
    monkeypatch.setenv("BOT_SYNC_SECRET", "test-admin")

    def identify(value):
        if not value or not value.startswith("Bearer user-"):
            return None
        uid = value.removeprefix("Bearer user-")
        return {"user_id": uid, "display_name": "Member " + uid, "username": "u" + uid, "role": "participant"}

    monkeypatch.setattr(auth, "identity_from_authorization", identify)
    from api import main

    monkeypatch.setattr(
        main,
        "find_verified_alcove_user",
        lambda user_id=None, username=None: {"user_id": user_id, "verified_at": "2026-01-01"}
        if str(user_id) in {"1", "2"}
        else None,
    )
    monkeypatch.setattr(dummies, "rehearsal_show_active", lambda: True)
    app = FastAPI()
    app.include_router(r.router)
    return TestClient(app), q, r, cr, dummies


ADMIN = {"X-Alcove-Admin": "test-admin"}
BOT = {"X-Bot-Sync-Secret": "test-admin"}


def test_rehearsal_dummies_can_join_and_submit_while_rehearsing(api):
    client, *_ = api
    started = client.post(
        "/api/question-game/start",
        headers=ADMIN,
        json={
            "title": "Herd Mentality",
            "mode": "crowd",
            "team_size": 1,
            "seconds": 30,
            "questions": [{"text": "Name a colour.", "answers": []}],
        },
    ).json()
    join = client.post(
        "/api/question-game/join",
        headers=BOT,
        json={"game_id": started["game_id"], "user_id": "91001", "display_name": "Sam · Rehearsal"},
    )
    assert join.status_code == 200, join.text
    host = client.get("/api/question-game/host-state", headers=ADMIN).json()
    begin = client.post(
        "/api/question-game/action",
        headers=ADMIN,
        json={"game_id": host["game_id"], "revision": host["revision"], "action": "begin"},
    )
    assert begin.status_code == 200, begin.text
    submit = client.post(
        "/api/question-game/submit",
        headers=BOT,
        json={
            "game_id": host["game_id"],
            "round": 1,
            "text": "Blue",
            "user_id": "91001",
            "display_name": "Sam · Rehearsal",
        },
    )
    assert submit.status_code == 200, submit.text
    assert submit.json()["submitted"] is True


def test_rehearsal_dummies_blocked_when_not_rehearsing(api, monkeypatch):
    client, *_, dummies = api
    monkeypatch.setattr(dummies, "rehearsal_show_active", lambda: False)
    started = client.post(
        "/api/question-game/start",
        headers=ADMIN,
        json={
            "title": "Quiz",
            "mode": "quiz",
            "team_size": 1,
            "seconds": 30,
            "questions": [{"text": "Capital?", "answers": ["Paris"]}],
        },
    ).json()
    denied = client.post(
        "/api/question-game/join",
        headers=BOT,
        json={"game_id": started["game_id"], "user_id": "91001", "display_name": "Sam · Rehearsal"},
    )
    assert denied.status_code == 403, denied.text


def test_is_rehearsal_dummy_range():
    from api.rehearsal_dummies import is_rehearsal_dummy

    assert is_rehearsal_dummy("91001")
    assert is_rehearsal_dummy(91006)
    assert is_rehearsal_dummy(91012)
    assert is_rehearsal_dummy("91007")
    assert not is_rehearsal_dummy(91013)
    assert not is_rehearsal_dummy("91000")
    assert not is_rehearsal_dummy("sam")
