"""Persistent, host-paced text games. Independent of the drawing game."""
from __future__ import annotations

import copy
import json
import os
import random
import sqlite3
import time
import unicodedata
import uuid
from pathlib import Path

from fastapi import HTTPException

def data_dir():
    direct = os.getenv("ALCOVE_QUESTION_GAME_DIR") or os.getenv("ALCOVE_TEAM_GAME_DIR")
    if direct:
        return Path(direct)
    for key in ("ALCOVE_STATE_DB_PATH", "ALCOVE_RUNTIME_STATE_PATH", "FOX_LOGS_DB_PATH"):
        value = os.getenv(key)
        if value:
            return Path(value).expanduser().resolve().parent / "question_game_data"
    return Path.cwd() / "team_game_data"


DATA_DIR = data_dir()
DB_PATH = DATA_DIR / "question_games.sqlite3"


def fail(message, code=409):
    raise HTTPException(status_code=code, detail=message)


def normalize(value):
    text = unicodedata.normalize("NFKC", str(value)).casefold()
    # Ignore surrounding sentence punctuation without changing numeric meaning:
    # "-5" is not "5", and "0.5" is not "05".
    return " ".join(text.strip().strip(chr(34) + chr(39) + "“”‘’.,!?;:").split())


def connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(DB_PATH), timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("CREATE TABLE IF NOT EXISTS state (id INTEGER PRIMARY KEY CHECK(id=1), payload TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS templates (id TEXT PRIMARY KEY, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, game_id TEXT NOT NULL, team_id TEXT NOT NULL, user_id TEXT NOT NULL, name TEXT NOT NULL, body TEXT NOT NULL, sent REAL NOT NULL)")
    return con


def idle():
    return {"stage": "idle", "revision": 0, "game_id": "", "entries": [], "participants": []}


def load_state(con=None):
    owned = con is None
    con = con or connect()
    try:
        row = con.execute("SELECT payload FROM state WHERE id=1").fetchone()
        return json.loads(row["payload"]) if row else idle()
    finally:
        if owned:
            con.close()


def save_state(state, con=None):
    owned = con is None
    con = con or connect()
    try:
        con.execute("INSERT INTO state(id,payload) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET payload=excluded.payload", (json.dumps(state),))
        if owned:
            con.commit()
    finally:
        if owned:
            con.close()
    return state


def mutate(expected_revision, operation, game_id=None):
    con = connect()
    try:
        con.execute("BEGIN IMMEDIATE")
        state = load_state(con)
        if game_id is not None and game_id != state.get("game_id"):
            fail("This game has changed. Refresh and try again.")
        if expected_revision is not None and expected_revision != state["revision"]:
            fail("The game has moved on. Refresh and try again.")
        updated = operation(copy.deepcopy(state))
        updated["revision"] = state["revision"] + 1
        save_state(updated, con)
        con.commit()
        return updated
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def validate_config(config):
    mode = config.get("mode", "crowd")
    size = config.get("team_size", 1)
    seconds = config.get("seconds", 30)
    questions = config.get("questions", [])
    if mode not in {"crowd", "quiz"} or size not in {1, 2, 3}:
        fail("Choose a game mode and individual, pairs or teams of three.", 422)
    if not isinstance(seconds, int) or not 10 <= seconds <= 300:
        fail("Answer time must be 10–300 seconds.", 422)
    if not isinstance(questions, list) or not 1 <= len(questions) <= 20:
        fail("Enter 1–20 questions.", 422)
    cleaned = []
    for question in questions:
        text = str(question.get("text", "")).strip()
        answers = question.get("answers", [])
        if isinstance(answers, str):
            answers = answers.split(";")
        if not isinstance(answers, list):
            fail("Accepted answers must be a list.", 422)
        answers = [str(a).strip() for a in answers if normalize(a)]
        if not text or len(text) > 300 or any(len(a) > 80 for a in answers) or len(answers) > 30:
            fail("Check question text and accepted answers.", 422)
        if mode == "quiz" and not answers:
            fail("Every quiz question needs an accepted answer.", 422)
        cleaned.append({"text": text, "answers": answers if mode == "quiz" else []})
    return {
        "title": str(config.get("title") or ("Match the Crowd" if mode == "crowd" else "Quiz"))[:100],
        "mode": mode, "team_size": size, "seconds": seconds, "questions": cleaned,
        "background_url": str(config.get("background_url") or "")[:2000],
        "participation_template_id": str(config.get("participation_template_id") or "")[:100],
        "winner_template_id": str(config.get("winner_template_id") or "")[:100],
        "repeat_participation": bool(config.get("repeat_participation", False)),
    }


def create_game(config):
    return {
        **validate_config(config), "game_id": uuid.uuid4().hex, "revision": 0,
        "stage": "lobby", "round": 0, "participants": [], "entries": [],
        "submissions": {}, "round_points": {}, "reveal_groups": [], "reveal_index": -1,
        "revealed": [], "active_ids": [], "active_kind": "", "round_members": [],
        "opened_at": None, "ends_at": None, "winners": [], "reward_status": None,
        "quiz_phase": "collecting" if config.get("mode", "crowd") == "quiz" else "",
        "quiz_rounds": [],
    }


def join_game(state, person):
    uid = str(person["user_id"])
    if any(p["user_id"] == uid for p in state["participants"]):
        return state
    if state["stage"] in {"finished", "ended", "idle"}:
        fail("Registration is closed.")
    if state.get("quiz_phase") == "results":
        fail("The quiz is complete. Join the next game.")
    if state["team_size"] > 1 and state["stage"] != "lobby":
        fail("Teams are fixed for this game. Join the next game.")
    person = {
        "user_id": uid, "display_name": str(person.get("display_name") or "Member")[:80],
        "username": str(person.get("username") or "")[:80],
        "style": person.get("style") if isinstance(person.get("style"), dict) else {},
    }
    if len(json.dumps(person["style"])) > 16000:
        fail("Customisation is too large.", 422)
    state["participants"].append(person)
    return state


def make_entries(state):
    people = state["participants"]
    size = state["team_size"]
    if size == 1:
        existing = {e["id"]: e for e in state["entries"]}
        state["entries"] = [existing.get(p["user_id"]) or {
            "id": p["user_id"], "name": p["display_name"], "members": [p],
            "captain_id": p["user_id"], "score": 0, "style": p["style"]
        } for p in people]
        return
    if state["entries"]:
        return
    if len(people) < 2:
        fail("At least two players are needed for teams.")
    people = list(people)
    random.shuffle(people)
    groups = [people[i:i + size] for i in range(0, len(people), size)]
    if len(groups) > 1 and len(groups[-1]) == 1:
        if len(groups[-2]) > 2:
            groups[-1].insert(0, groups[-2].pop())
        else:
            groups[-2].extend(groups.pop())
    state["entries"] = [{
        "id": "team-" + str(i + 1), "name": "Team " + str(i + 1),
        "members": members, "captain_id": members[0]["user_id"],
        "score": 0, "style": members[0]["style"],
    } for i, members in enumerate(groups)]


def begin_round(state):
    if state.get("quiz_phase") == "results":
        fail("Advance to the next question's results.")
    if state["stage"] not in {"lobby", "all_answers", "quiz_wait"}:
        fail("Show all answers before the next question.")
    if state["round"] >= len(state["questions"]):
        fail("Reveal the final results.")
    if not state["participants"]:
        fail("Wait for players to join.")
    make_entries(state)
    state["round"] += 1
    state.update(stage="answering", submissions={}, round_points={}, reveal_groups=[],
                 reveal_index=-1, revealed=[], active_ids=[], active_kind="",
                 opened_at=time.time(), round_members=[e["id"] for e in state["entries"]])
    state["ends_at"] = state["opened_at"] + state["seconds"]
    return state


def entry_for(state, user_id):
    return next((e for e in state.get("entries", []) if any(m["user_id"] == str(user_id) for m in e["members"])), None)


def submit_answer(state, user_id, text, now=None):
    now = time.time() if now is None else now
    entry = entry_for(state, user_id)
    if state["stage"] != "answering" or now >= state["ends_at"]:
        fail("Answers are closed.")
    if not entry or entry["id"] not in state["round_members"]:
        fail("You can enter from the next question.")
    if entry["captain_id"] != str(user_id):
        fail("Only your captain can submit the team's answer.", 403)
    if entry["id"] in state["submissions"]:
        fail("Your answer has already been submitted.")
    text = str(text).strip()
    if not normalize(text) or len(text) > 80:
        fail("Enter an answer of 1–80 characters.", 422)
    state["submissions"][entry["id"]] = {"text": text, "user_id": str(user_id)}
    state.setdefault("qualified", [])
    for member in entry["members"]:
        if member["user_id"] not in state["qualified"]:
            state["qualified"].append(member["user_id"])
    return state


def calculate(state):
    groups = {}
    for eid, item in state["submissions"].items():
        key = state.get("merges", {}).get(eid) or normalize(item["text"])
        groups.setdefault(key, []).append(eid)
    points = {eid: 0 for eid in state["round_members"]}
    reveals = []
    if state["mode"] == "crowd":
        largest = max((len(ids) for ids in groups.values()), default=0)
        winners = []
        for ids in groups.values():
            if len(ids) < 2:
                continue
            is_top = len(ids) == largest
            for eid in ids:
                points[eid] = 2 if is_top else 1
            if is_top:
                winners.extend(ids)
            else:
                reveals.append({"ids": ids, "kind": "match"})
        reveals.sort(key=lambda group: (len(group["ids"]), group["ids"][0]))
        if winners:
            reveals.append({"ids": winners, "kind": "winner"})
    else:
        accepted = {normalize(a) for a in state["questions"][state["round"]-1]["answers"]}
        winners = []
        for eid, item in state["submissions"].items():
            correct = state.get("corrections", {}).get(eid, normalize(item["text"]) in accepted)
            points[eid] = 1 if correct else 0
            if correct:
                winners.append(eid)
        if winners:
            reveals.append({"ids": winners, "kind": "winner"})
    state.update(round_points=points, reveal_groups=reveals)
    return state


def close_round(state):
    if state["stage"] != "answering":
        fail("This question is already closed.")
    if state.get("quiz_phase") == "collecting":
        state["quiz_rounds"].append({
            "round": state["round"], "submissions": copy.deepcopy(state["submissions"]),
            "round_members": list(state["round_members"]),
        })
        state.update(stage="quiz_wait", ends_at=None)
        return state
    state.update(stage="review", merges={}, corrections={})
    return calculate(state)


def load_quiz_result(state, number):
    saved = state["quiz_rounds"][number - 1]
    state.update(round=number, quiz_phase="results", stage="review",
                 submissions=copy.deepcopy(saved["submissions"]),
                 round_members=list(saved["round_members"]), merges={}, corrections={},
                 round_points={}, reveal_groups=[], reveal_index=-1,
                 revealed=[], active_ids=[], active_kind="", ends_at=None)
    return calculate(state)


def start_quiz_results(state):
    if (state.get("quiz_phase") != "collecting" or state["stage"] != "quiz_wait"
            or len(state["quiz_rounds"]) != len(state["questions"])):
        fail("Complete all quiz questions before revealing answers.")
    return load_quiz_result(state, 1)


def next_quiz_result(state):
    if (state.get("quiz_phase") != "results" or state["stage"] != "all_answers"
            or state["round"] >= len(state["questions"])):
        fail("Show incorrect answers before the next question's results.")
    return load_quiz_result(state, state["round"] + 1)


def review_answers(state, corrections, merges):
    if state["stage"] != "review":
        fail("Answers can only be reviewed before revealing.")
    valid = set(state["submissions"])
    if not set(corrections) <= valid or not set(merges) <= valid:
        fail("Unknown answer.", 422)
    state["corrections"] = {k: bool(v) for k, v in corrections.items()}
    state["merges"] = {k: normalize(str(v)[:80]) for k, v in merges.items()}
    return calculate(state)


def reveal_next(state):
    if state["stage"] not in {"review", "revealing"}:
        fail("No reveal is waiting.")
    index = state["reveal_index"] + 1
    state["reveal_index"] = index
    if index >= len(state["reveal_groups"]):
        state.update(stage="all_answers", active_ids=[], active_kind="")
        state["revealed"] = list(state["submissions"])
        if state["mode"] == "quiz":
            state["active_ids"] = [eid for eid in state["submissions"] if not state["round_points"].get(eid)]
            state["active_kind"] = "incorrect"
        return state
    group = state["reveal_groups"][index]
    for entry in state["entries"]:
        if entry["id"] in group["ids"] and entry["id"] not in state["revealed"]:
            entry["score"] += state["round_points"].get(entry["id"], 0)
    state["revealed"] = list(dict.fromkeys(state["revealed"] + group["ids"]))
    state.update(stage="revealing", active_ids=group["ids"], active_kind=group["kind"])
    return state


def public_state(state, user_id=None):
    if state.get("stage") == "idle":
        return {"stage": "idle", "revision": state["revision"]}
    mine = entry_for(state, user_id)
    ids = list(state["submissions"]) if state["stage"] == "all_answers" else state["active_ids"]
    if state["mode"] == "quiz" and state["stage"] == "all_answers":
        ids = state["active_ids"]
    cards = [{
        "id": e["id"], "name": e["name"], "members": [m["display_name"] for m in e["members"]],
        "answer": state["submissions"][e["id"]]["text"], "style": e["style"],
    } for e in state["entries"] if e["id"] in ids and e["id"] in state["revealed"]]
    # OBS keeps a stable board in submission order; unrevealed text never leaves the server.
    entries = {e["id"]: e for e in state["entries"]}
    winning_ids = {eid for group in state["reveal_groups"] if group["kind"] == "winner" for eid in group["ids"]}
    overlay_cards = [{
        "id": eid, "name": entries[eid]["name"], "style": entries[eid]["style"],
        "members": [m["display_name"] for m in entries[eid]["members"]],
        "answer": item["text"] if eid in state["revealed"] else "",
        "revealed": eid in state["revealed"],
        "winning": eid in winning_ids and eid in state["revealed"],
    } for eid, item in state["submissions"].items() if eid in entries]
    return {
        "stage": state["stage"], "revision": state["revision"], "game_id": state["game_id"],
        "title": state["title"], "mode": state["mode"], "team_size": state["team_size"],
        "quiz_phase": state.get("quiz_phase", ""),
        "round": state["round"], "total_rounds": len(state["questions"]),
        "question": state["questions"][state["round"]-1]["text"] if state["round"] else "",
        "correct_answer": state["questions"][state["round"]-1]["answers"][0]
                          if state["mode"] == "quiz" and state["stage"] in {"revealing", "all_answers", "finished"} else "",
        "ends_at": state["ends_at"], "background_url": state["background_url"],
        "participant_count": len(state["participants"]),
        "joined": any(p["user_id"] == str(user_id) for p in state["participants"]),
        "my_entry": {k: mine[k] for k in ("id", "name", "captain_id")} if mine else None,
        "teammates": [{"user_id": m["user_id"], "name": m["display_name"]} for m in mine["members"]] if mine else [],
        "submitted": bool(mine and mine["id"] in state["submissions"]),
        "eligible_this_round": bool(mine and mine["id"] in state["round_members"]),
        "overlay_cards": overlay_cards, "cards": cards, "active_ids": state["active_ids"], "active_kind": state["active_kind"],
        "leaderboard": [{"id": e["id"], "name": e["name"], "score": e["score"]} for e in sorted(state["entries"], key=lambda e: -e["score"])],
        "winner_cards": [{"id": e["id"], "name": e["name"], "style": e["style"],
                          "members": [m["display_name"] for m in e["members"]],
                          "answer": str(e["score"]) + (" point" if e["score"] == 1 else " points")}
                         for e in state["entries"] if state["stage"] == "finished" and e["id"] in state["winners"]],
        "winners": state["winners"], "reward_status": state["reward_status"],
    }

