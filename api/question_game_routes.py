"""HTTP adapter for question games; identity is never accepted from request bodies."""
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any
import hashlib

from fastapi import APIRouter, Body, Depends, Header
from pydantic import BaseModel, Field

from . import question_games as q

router = APIRouter(prefix="/api/question-game", tags=["question-game"])


def identity(authorization: str | None = Header(default=None), x_telegram_init_data: str | None = Header(default=None)):
    from .live_room_test import identity_from_authorization, _verify_init_data, _public_user, _role_for
    user = identity_from_authorization(authorization)
    if not user and x_telegram_init_data:
        verified = _verify_init_data(x_telegram_init_data)
        user = {**_public_user(verified), "role": _role_for(verified)}
    if not user:
        q.fail("Open the Live Room through Telegram to take part.", 401)
    user["user_id"] = str(user.get("user_id") or user.get("id") or "")
    if not user["user_id"]:
        q.fail("Verified identity is required.", 401)
    return user


def host(x_alcove_admin: str | None = Header(default=None), authorization: str | None = Header(default=None), x_telegram_init_data: str | None = Header(default=None)):
    if x_alcove_admin:
        from .custom_rewards import _admin
        _admin(x_alcove_admin)
        return {"role": "host"}
    user = identity(authorization, x_telegram_init_data)
    if user.get("role") != "host":
        q.fail("Host access required.", 403)
    return user


def optional_identity(authorization, init_data):
    return identity(authorization, init_data) if authorization or init_data else None


def current_state():
    state = q.load_state()
    if state["stage"] == "answering" and time.time() >= state["ends_at"]:
        try:
            state = q.mutate(state["revision"], q.close_round, state["game_id"])
        except q.HTTPException as exc:
            if exc.status_code != 409:
                raise
            state = q.load_state()
    return state


class Action(BaseModel):
    game_id: str
    revision: int
    action: str
    payload: dict[str, Any] = Field(default_factory=dict)


class Submission(BaseModel):
    game_id: str
    round: int
    text: str = Field(max_length=80)


class Join(BaseModel):
    game_id: str
    style: dict[str, Any] = Field(default_factory=dict)


class Chat(BaseModel):
    game_id: str
    round: int
    body: str = Field(min_length=1, max_length=300)


@router.get("/state")
def state(after_revision: str | None = None, authorization: str | None = Header(default=None), x_telegram_init_data: str | None = Header(default=None)):
    user = optional_identity(authorization, x_telegram_init_data)
    current = current_state()
    public = q.public_state(current, user["user_id"] if user else None)
    public.pop("revision", None)
    version = hashlib.sha256(json.dumps(public, sort_keys=True).encode()).hexdigest()[:20]
    if after_revision == version:
        return {"unchanged": True, "revision": version}
    return {**public, "revision": version}


@router.get("/host-state")
def host_state(after_revision: int | None = None, _=Depends(host)):
    current = current_state()
    if after_revision == current["revision"]:
        return {"unchanged": True, "revision": after_revision}
    return current


def validate_prizes(config):
    from . import custom_rewards as cr
    winner = config.get("winner_template_id")
    participation = config.get("participation_template_id")
    with cr._LOCK:
        con = cr._conn()
        try:
            for tid in [winner, participation]:
                if not tid or tid == "__none__":
                    continue
                row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (tid,)).fetchone()
                if not row:
                    q.fail("Selected reward pack no longer exists.", 422)
                if tid == winner and len(cr._pack(row)["items"]) != 2:
                    q.fail("The winner pack must contain exactly two items.", 422)
        finally:
            con.close()


@router.post("/start")
def start(config: dict = Body(...), _=Depends(host)):
    cleaned = q.validate_config(config)
    validate_prizes(cleaned)
    current = q.load_state()
    if current["stage"] not in {"idle", "ended", "finished"}:
        q.fail("End the current question game before starting another.")
    result = q.mutate(current["revision"], lambda old: q.create_game(cleaned))
    con = q.connect()
    try:
        con.execute("DELETE FROM messages")
        con.commit()
    finally:
        con.close()
    return result


@router.post("/join")
def join(payload: Join, user=Depends(identity)):
    user = {**user, "style": payload.style}
    current = q.mutate(None, lambda s: q.join_game(s, user), payload.game_id)
    return q.public_state(current, user["user_id"])


@router.post("/submit")
def submit(payload: Submission, user=Depends(identity)):
    def operation(s):
        if s.get("round") != payload.round:
            q.fail("The question has changed.")
        return q.submit_answer(s, user["user_id"], payload.text)
    current = q.mutate(None, operation, payload.game_id)
    return q.public_state(current, user["user_id"])


def finish(state):
    if state["stage"] != "all_answers" or state["round"] != len(state["questions"]):
        q.fail("Complete all questions before final results.")
    qualified = set(state.get("qualified", []))
    eligible = [e for e in state["entries"] if any(m["user_id"] in qualified for m in e["members"])]
    top = max((e["score"] for e in eligible), default=0)
    state["winners"] = [e["id"] for e in eligible if e["score"] == top]
    state["reward_status"] = {}
    state["rewards_sent"] = False
    state["stage"] = "finished"
    # Allow the polling answer board to finish fading before celebration sources start.
    state["celebration_at"] = time.time() + 2.2
    return state


def grant_rewards(state):
    """Reward ledger and pack grants share one transaction: retries cannot duplicate packs."""
    from . import custom_rewards as cr
    from . import participation_rewards as pr
    qualified = set(state.get("qualified", []))
    winners = {m["user_id"] for e in state["entries"] if e["id"] in state["winners"] for m in e["members"]} & qualified
    results = {"participation": 0, "winner": 0, "collection_complete": 0}
    with cr._LOCK:
        con = cr._conn()
        try:
            pr._ensure_tables(con)
            con.execute("CREATE TABLE IF NOT EXISTS question_game_reward_awards(game_id TEXT NOT NULL,user_id TEXT NOT NULL,kind TEXT NOT NULL,grant_id TEXT,PRIMARY KEY(game_id,user_id,kind))")
            con.commit()
            cfg, participation_row = pr._participation_template(con)
            tid = state.get("participation_template_id")
            if tid == "__none__":
                participation_row = None
            elif tid:
                participation_row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (tid,)).fetchone()
            winner_row = None
            if state.get("winner_template_id"):
                winner_row = con.execute("SELECT * FROM custom_pack_templates WHERE id=?", (state["winner_template_id"],)).fetchone()
                if not winner_row or len(cr._pack(winner_row)["items"]) != 2:
                    q.fail("Restore the selected two-item winner pack before sending packs.", 422)
            con.execute("BEGIN IMMEDIATE")
            for person in state["participants"]:
                uid = person["user_id"]
                if uid not in qualified:
                    continue
                for kind, row in [("participation", participation_row), ("winner", winner_row if uid in winners else None)]:
                    if row is None:
                        continue
                    existing = con.execute("SELECT grant_id FROM question_game_reward_awards WHERE game_id=? AND user_id=? AND kind=?", (state["game_id"], uid, kind)).fetchone()
                    if existing:
                        if existing["grant_id"]:
                            results[kind] += 1
                        else:
                            results["collection_complete"] += 1
                        continue
                    if kind == "participation":
                        award = pr._grant_one(con, cfg, row, {
                            "user_key": "id:" + uid, "user_id": uid, "username": person["username"],
                            "event_id": "question-game:" + state["game_id"], "activity": "question_game",
                        })
                        grant_id = award.get("grant_id")
                        if award["status"] == "collection_complete":
                            if state.get("repeat_participation"):
                                pack = cr._pack(row)
                                pack["items"] = [random_item(pack["items"])]
                                grant_id = str(uuid.uuid4())
                                pack.update(event_id=state["game_id"], activity="question_game")
                                con.execute("INSERT INTO custom_pack_grants(id,template_id,target_user_id,target_username,pack_json,sent_at,claimed_at) VALUES(?,?,?,?,?,?,NULL)",
                                            (grant_id, row["id"], uid, person["username"] or None, json.dumps(pack), cr._now()))
                            else:
                                results["collection_complete"] += 1
                    else:
                        pack = cr._pack(row)
                        if kind == "participation":
                            pack["items"] = [random_item(pack["items"])]
                        grant_id = str(uuid.uuid4())
                        pack.update(event_id=state["game_id"], activity="question_game")
                        con.execute("INSERT INTO custom_pack_grants(id,template_id,target_user_id,target_username,pack_json,sent_at,claimed_at) VALUES(?,?,?,?,?,?,NULL)",
                                    (grant_id, row["id"], uid, person["username"] or None, json.dumps(pack), cr._now()))
                    con.execute("INSERT INTO question_game_reward_awards(game_id,user_id,kind,grant_id) VALUES(?,?,?,?)", (state["game_id"], uid, kind, grant_id))
                    if grant_id:
                        results[kind] += 1
            con.commit()
            return results
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def random_item(items):
    import random
    if not items:
        q.fail("The participation pack is empty.", 422)
    return random.choice(items)


@router.post("/action")
def action(payload: Action, _=Depends(host)):
    def operation(s):
        kind = payload.action
        p = payload.payload
        if kind == "begin":
            return q.begin_round(s)
        if kind == "close":
            return q.close_round(s)
        if kind == "quiz_results":
            return q.start_quiz_results(s)
        if kind == "next_result":
            return q.next_quiz_result(s)
        if kind == "review":
            return q.review_answers(s, p.get("corrections", {}), p.get("merges", {}))
        if kind == "reveal":
            return q.reveal_next(s)
        if kind == "finish":
            return finish(s)
        if kind == "send_packs":
            if s["stage"] != "finished":
                q.fail("Reveal final results before sending packs.")
            if not s.get("rewards_sent"):
                s["reward_status"] = grant_rewards(s)
                s["rewards_sent"] = True
            return s
        if kind == "end":
            s.update(stage="ended", active_ids=[])
            return s
        if kind == "captain":
            entry = next((e for e in s["entries"] if e["id"] == p.get("entry_id")), None)
            uid = str(p.get("user_id") or "")
            if not entry or uid not in {m["user_id"] for m in entry["members"]}:
                q.fail("Choose a member of that team.", 422)
            entry["captain_id"] = uid
            return s
        q.fail("Unknown host action.", 422)
    return q.mutate(payload.revision, operation, payload.game_id)


def chat_team(s, user):
    entry = q.entry_for(s, user["user_id"])
    if not entry or s.get("team_size", 1) == 1 or s["stage"] != "answering" or time.time() >= s["ends_at"]:
        q.fail("Team chat is available during answer time.", 403)
    return entry


@router.get("/chat")
def chat(after_id: int = 0, user=Depends(identity)):
    s = current_state()
    entry = chat_team(s, user)
    con = q.connect()
    try:
        rows = con.execute("SELECT id,user_id,name,body FROM messages WHERE game_id=? AND team_id=? AND id>? ORDER BY id LIMIT 100",
                           (s["game_id"], entry["id"], max(0, after_id))).fetchall()
        return {"game_id": s["game_id"], "team_id": entry["id"], "messages": [dict(r) for r in rows]}
    finally:
        con.close()


@router.post("/chat")
def chat_post(payload: Chat, user=Depends(identity)):
    con = q.connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        s = q.load_state(con)
        if s["game_id"] != payload.game_id or s["round"] != payload.round:
            q.fail("The question has changed.")
        entry = chat_team(s, user)
        body = payload.body.strip()
        if not body:
            q.fail("Enter a message.", 422)
        previous = con.execute("SELECT sent FROM messages WHERE game_id=? AND user_id=? ORDER BY id DESC LIMIT 1", (s["game_id"], user["user_id"])).fetchone()
        if previous and time.time() - previous["sent"] < 0.8:
            q.fail("Wait a moment before sending again.", 429)
        con.execute("INSERT INTO messages(game_id,team_id,user_id,name,body,sent) VALUES(?,?,?,?,?,?)",
                    (s["game_id"], entry["id"], user["user_id"], user.get("display_name") or "Member", body, time.time()))
        con.execute("DELETE FROM messages WHERE game_id=? AND team_id=? AND id NOT IN (SELECT id FROM messages WHERE game_id=? AND team_id=? ORDER BY id DESC LIMIT 100)",
                    (s["game_id"], entry["id"], s["game_id"], entry["id"]))
        con.commit()
        return {"ok": True}
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


@router.get("/templates")
def templates(_=Depends(host)):
    con = q.connect()
    try:
        return {"templates": [json.loads(r["payload"]) for r in con.execute("SELECT payload FROM templates ORDER BY rowid DESC")]}
    finally:
        con.close()


@router.post("/templates")
def save_template(payload: dict = Body(...), _=Depends(host)):
    cleaned = q.validate_config(payload)
    validate_prizes(cleaned)
    cleaned["id"] = str(payload.get("id") or uuid.uuid4().hex)[:100]
    con = q.connect()
    try:
        con.execute("INSERT INTO templates(id,payload) VALUES(?,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (cleaned["id"], json.dumps(cleaned)))
        con.commit()
    finally:
        con.close()
    return cleaned


@router.delete("/templates/{template_id}")
def delete_template(template_id: str, _=Depends(host)):
    con = q.connect()
    try:
        con.execute("DELETE FROM templates WHERE id=?", (template_id,))
        con.commit()
    finally:
        con.close()
    return {"ok": True}


@router.get("/clock")
def clock():
    s = current_state()
    return {"game_id": s.get("game_id"), "stage": s["stage"], "ends_at": s.get("ends_at")}


@router.get("/packs")
def packs(_=Depends(host)):
    from . import custom_rewards as cr
    with cr._LOCK:
        con = cr._conn()
        try:
            rows = con.execute("SELECT * FROM custom_pack_templates ORDER BY name").fetchall()
            return {"packs": [{"id": row["id"], "name": row["name"], "item_count": len(cr._pack(row)["items"])} for row in rows]}
        finally:
            con.close()
