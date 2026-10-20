import copy
import importlib
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

@pytest.fixture
def api(tmp_path, monkeypatch):
    from api import question_games as q, question_game_routes as r, custom_rewards as cr, live_room_test as auth
    monkeypatch.setattr(q,"DB_PATH",tmp_path/"games.sqlite3")
    monkeypatch.setattr(cr,"DB_PATH",tmp_path/"packs.sqlite3")
    monkeypatch.setenv("BOT_SYNC_SECRET","test-admin")
    def identify(value):
        if not value or not value.startswith("Bearer user-"):
            return None
        uid=value.removeprefix("Bearer user-")
        return {"user_id":uid,"display_name":"Member "+uid,"username":"u"+uid,"role":"participant"}
    monkeypatch.setattr(auth,"identity_from_authorization",identify)
    app=FastAPI();app.include_router(r.router)
    return TestClient(app),q,r,cr

def config(size=1,mode="quiz",count=1):
    return {"title":"Quiz","mode":mode,"team_size":size,"seconds":30,"questions":[{"text":"Capital?","answers":["Paris"]} for _ in range(count)]}

ADMIN={"X-Alcove-Admin":"test-admin"}
def member(n):
    return {"Authorization":"Bearer user-"+str(n)}

def setup(client,size=1,players=4):
    s=client.post("/api/question-game/start",headers=ADMIN,json=config(size)).json()
    for n in range(1,players+1):
        response=client.post("/api/question-game/join",headers=member(n),json={"game_id":s["game_id"]})
        assert response.status_code==200,response.text
    s=client.get("/api/question-game/host-state",headers=ADMIN).json()
    return action(client,s,"begin")

def action(client,s,name,payload=None):
    response=client.post("/api/question-game/action",headers=ADMIN,json={"game_id":s["game_id"],"revision":s["revision"],"action":name,"payload":payload or {}})
    assert response.status_code==200,response.text
    return response.json()

def test_host_endpoints_reject_unverified_and_nonhost(api):
    client,*_=api
    assert client.post("/api/question-game/start",json=config()).status_code==401
    assert client.post("/api/question-game/start",headers=member(1),json=config()).status_code==403
    assert client.get("/api/question-game/templates").status_code==401
    assert client.get("/api/question-game/host-state").status_code==401

def test_hidden_answers_and_unchanged_public_state_between_other_submissions(api):
    client,q,*_=api;s=setup(client)
    before=client.get("/api/question-game/state",headers=member(2)).json()
    response=client.post("/api/question-game/submit",headers=member(1),json={"game_id":s["game_id"],"round":1,"text":"Paris"})
    assert response.status_code==200
    public=client.get("/api/question-game/state",headers=member(2),params={"after_revision":before["revision"]}).json()
    assert public["unchanged"]
    own=client.get("/api/question-game/state",headers=member(1)).json()
    assert own["submitted"] and own["revision"]!=before["revision"]
    assert "Paris" not in str(own)
    assert client.get("/api/question-game/clock").json()["stage"]=="answering"

def test_private_chat_isolates_teams_and_requires_captain_submission(api):
    client,q,*_=api;s=setup(client,2)
    a,b=s["entries"]
    uid=a["captain_id"];other=b["captain_id"]
    assert client.post("/api/question-game/chat",headers=member(uid),json={"game_id":s["game_id"],"round":1,"body":"Private idea"}).status_code==200
    assert client.get("/api/question-game/chat",headers=member(other)).json()["messages"]==[]
    assert client.get("/api/question-game/chat",headers=member(uid)).json()["messages"][0]["body"]=="Private idea"
    assert client.get("/api/question-game/chat",params={"user_id":uid}).status_code==401
    teammate=next(m["user_id"] for m in a["members"] if m["user_id"]!=uid)
    assert client.post("/api/question-game/submit",headers=member(teammate),json={"game_id":s["game_id"],"round":1,"text":"Paris"}).status_code==403
    action(client,s,"close")
    assert client.get("/api/question-game/chat",headers=member(uid)).status_code==403

def test_final_rewards_retry_safe_and_winner_has_two_items(api):
    client,q,r,cr=api
    with cr._LOCK:
        con=cr._conn()
        for tid,items in [("participation",[{"type":"sticker","id":"hello"}]),("winner",[{"type":"sticker","id":"win"},{"type":"color","id":"gold"}])]:
            import json
            con.execute("INSERT INTO custom_pack_templates(id,name,description,image,items_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",(tid,tid,"","",json.dumps(items),cr._now(),cr._now()))
        con.commit();con.close()
    cfg=config();cfg.update(participation_template_id="participation",winner_template_id="winner")
    s=client.post("/api/question-game/start",headers=ADMIN,json=cfg).json()
    for uid in [1,2]:
        client.post("/api/question-game/join",headers=member(uid),json={"game_id":s["game_id"]})
    s=client.get("/api/question-game/host-state",headers=ADMIN).json();s=action(client,s,"begin")
    for uid,text in [(1,"Paris"),(2,"London")]:
        client.post("/api/question-game/submit",headers=member(uid),json={"game_id":s["game_id"],"round":1,"text":text})
    s=client.get("/api/question-game/host-state",headers=ADMIN).json()
    s=action(client,s,"close");s=action(client,s,"reveal");s=action(client,s,"reveal")
    early=client.post("/api/question-game/action",headers=ADMIN,json={"game_id":s["game_id"],"revision":s["revision"],"action":"send_packs"})
    assert early.status_code==409
    s=action(client,s,"finish")
    con=cr._conn()
    assert con.execute("SELECT COUNT(*) FROM custom_pack_grants").fetchone()[0]==0
    con.close()
    assert not s.get("rewards_sent")
    # Simulate a successful reward commit followed by a failed game-state commit.
    r.grant_rewards(copy.deepcopy(s))
    s=action(client,s,"send_packs")
    assert s["rewards_sent"]
    assert s["reward_status"]["winner"]==1
    s=action(client,s,"send_packs")
    con=cr._conn()
    assert con.execute("SELECT COUNT(*) FROM custom_pack_grants").fetchone()[0]==3
    winner=con.execute("SELECT pack_json FROM custom_pack_grants WHERE template_id='winner'").fetchone()[0]
    import json
    assert len(json.loads(winner)["items"])==2
    con.close()
    assert s["stage"]=="finished"

def test_captain_replace_and_expiry_closes_on_server(api):
    client,q,*_=api;s=setup(client,2)
    team=s["entries"][0];new=next(m["user_id"] for m in team["members"] if m["user_id"]!=team["captain_id"])
    s=action(client,s,"captain",{"entry_id":team["id"],"user_id":new})
    assert s["entries"][0]["captain_id"]==new
    s["ends_at"]=0;q.save_state(s)
    assert client.get("/api/question-game/clock").json()["stage"]=="review"

def test_agenda_question_snapshot_never_contains_quiz_answers(api):
    from api import agendas
    import json
    row={"id":"x","title":"Session","event_date":"2026-10-20","start_time":"20:00","timezone":"Europe/London","status":"active","show_in_app":1,"items_json":json.dumps([{"id":"q","kind":"question_game","title":"Quiz","duration_minutes":5,"content_snapshot":{"question_game":{"id":"private-template","questions":[{"text":"Capital?","answers":["Paris"]}]}}}])}
    public=agendas._public(row)
    assert "Paris" not in str(public)
    assert public["items"][0]["content_snapshot"]["question_game_template_id"]=="private-template"
