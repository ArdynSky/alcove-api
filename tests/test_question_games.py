import importlib
import os
import tempfile
from pathlib import Path

import pytest

@pytest.fixture
def game(tmp_path, monkeypatch):
    monkeypatch.setenv("ALCOVE_QUESTION_GAME_DIR", str(tmp_path))
    from api import question_games as q
    monkeypatch.setattr(q, "DB_PATH", tmp_path / "games.sqlite3")
    return q

def configuration(mode="crowd", size=1, count=2):
    return {"title":"Test", "mode":mode, "team_size":size, "seconds":30,
            "background_url":"https://example.com/bg.mp4",
            "questions":[{"text":f"Question {n}?", "answers":["Paris"]} for n in range(count)]}

def player(n):
    return {"user_id":str(n), "display_name":f"Player {n}", "username":f"p{n}"}

def start(game, mode="crowd", size=1, count=2, players=6):
    state = game.create_game(configuration(mode, size, count))
    for n in range(1,players+1):
        state = game.join_game(state, player(n))
    state = game.begin_round(state)
    return state

def answer(game, state, values):
    for n, value in enumerate(values, 1):
        state = game.submit_answer(state, str(n), value, now=state["opened_at"]+1)
    return state

def test_crowd_scores_matches_and_unique(game):
    s=answer(game,start(game),["Pizza","pizza!","PIZZA","Burger","burger","Curry"])
    s=game.close_round(s)
    assert sorted(s["round_points"].values()) == [0,1,1,2,2,2]
    assert all(p["score"] == 0 for p in s["entries"])
    assert len(s["reveal_groups"]) == 2
    s=game.reveal_next(s)
    assert sum(p["score"] for p in s["entries"]) == 2
    s=game.reveal_next(s)
    assert sum(p["score"] for p in s["entries"]) == 8

def test_tied_groups_get_two_and_unique_answers_zero(game):
    s=answer(game,start(game),["A","a","B","b","C","D"])
    s=game.close_round(s)
    assert sorted(s["round_points"].values()) == [0,0,2,2,2,2]
    assert len(s["reveal_groups"]) == 1
    s=answer(game,start(game),["A","B","C","D","E","F"])
    assert set(game.close_round(s)["round_points"].values()) == {0}

def test_quiz_accepted_aliases_and_host_correction(game):
    s=start(game,"quiz")
    s=answer(game,s,[" Paris ","PARIS!","London","Lyon","Paris","London"])
    s=game.close_round(s)
    s=game.review_answers(s, {"3": True}, {})
    assert s["round_points"]["3"] == 1
    assert s["round_points"]["4"] == 0

def test_duplicate_submission_and_deadline_rejected(game):
    s=start(game)
    s=game.submit_answer(s,"1","Pizza",now=s["opened_at"]+1)
    with pytest.raises(Exception):
        game.submit_answer(s,"1","Burger",now=s["opened_at"]+2)
    with pytest.raises(Exception):
        game.submit_answer(s,"2","Pizza",now=s["ends_at"])
    with pytest.raises(Exception):
        game.submit_answer(s,"999","Pizza",now=s["opened_at"]+1)

def test_team_captain_one_answer_and_no_singleton_with_five(game):
    s=start(game,size=3,players=5)
    assert sorted(len(e["members"]) for e in s["entries"]) == [2,3]
    team=s["entries"][0]
    captain=team["captain_id"]
    other=next(m["user_id"] for m in team["members"] if m["user_id"] != captain)
    with pytest.raises(Exception):
        game.submit_answer(s,other,"Paris",now=s["opened_at"]+1)
    s=game.submit_answer(s,captain,"Paris",now=s["opened_at"]+1)
    assert list(s["submissions"]) == [team["id"]]

def test_public_state_hides_answers_and_host_configuration(game):
    s=answer(game,start(game,"quiz"),["Paris","Paris","X","X","X","X"])
    s=game.close_round(s)
    p=game.public_state(s,"1")
    assert "questions" not in p
    assert "submissions" not in p
    assert "round_points" not in p
    assert "reveal_groups" not in p
    assert p["cards"] == []
    assert p["submitted"] is True
    assert "Paris" not in str(p)

def test_revision_rejects_repeat_reveal_and_store_survives_reload(game):
    s=game.close_round(answer(game,start(game),["A","A","A","B","B","C"]))
    game.save_state(s)
    old=s["revision"]
    s=game.mutate(old, lambda current: game.reveal_next(current))
    with pytest.raises(Exception):
        game.mutate(old, lambda current: game.reveal_next(current))
    assert game.load_state()["revision"] == s["revision"]
    assert sum(e["score"] for e in game.load_state()["entries"]) == 2

def test_late_individual_waits_until_next_round(game):
    s=start(game)
    s=game.join_game(s,player(7))
    with pytest.raises(Exception):
        game.submit_answer(s,"7","A",now=s["opened_at"]+1)
    s=game.close_round(s)
    while s["stage"] != "all_answers":
        s=game.reveal_next(s)
    s=game.begin_round(s)
    assert any(e["id"]=="7" for e in s["entries"])
    s=game.submit_answer(s,"7","A",now=s["opened_at"]+1)
    assert s["submissions"]["7"]["text"] == "A"

def test_validation(game):
    for count in [0,21]:
        with pytest.raises(Exception):
            game.create_game(configuration(count=count))
    bad=configuration("quiz")
    bad["questions"][0]["answers"]=[]
    with pytest.raises(Exception):
        game.create_game(bad)
