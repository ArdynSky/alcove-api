"""Alcove Cards multiplayer: rooms, matchmaking, and authoritative game engine."""
from __future__ import annotations

import asyncio
import json
import random
import string
import threading
import time
import uuid
import os
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from pydantic import BaseModel, Field

from .cards_progress import default_loadout, profile_summary, record_match_results, save_loadout
from .cards_ws_manager import cards_ws_manager

DECK_PATH = Path(__file__).resolve().parent / "data" / "cards_deck.json"
with open(DECK_PATH, encoding="utf-8") as _deck_file:
    _DECK = json.load(_deck_file)

PROMPTS: List[str] = _DECK["prompts"]
BASE_ANSWERS: List[str] = _DECK["base_answers"]
REWARD_ANSWERS: List[dict] = _DECK["reward_answers"]

MIN_PLAYERS = 4
MAX_PLAYERS = 6
ROOM_TTL_SECONDS = 30 * 60
BOT_PLAYERS = [
    (-1, "Nova"),
    (-2, "Jax"),
    (-3, "Mira"),
    (-4, "Sol"),
    (-5, "Rue"),
]
ROUND_TIMER_SECONDS = 18
STAGE_TIMERS = {
    "subject_pick": 60,
    "submit": 50,
    "judge_pick": 45,
    "crowd_vote": 40,
}

GAME_STAGES = [
    "lobby",
    "subject_pick",
    "subject_wait",
    "submit",
    "judge_pick",
    "crowd_vote",
    "results",
    "game_over",
]

BOOSTER_PRIORITY = {
    "shield": 0,
    "hype": 1,
    "heckle": 2,
    "tie_thief": 3,
    "nullseal": 9,
}


def cards_demo_online_open() -> bool:
    raw = os.getenv("CARDS_DEMO_ONLINE", "true").strip().lower()
    return raw in ("1", "true", "yes", "on")


def cards_resolve_player(user_id: int | None, username: str | None, find_verified) -> tuple[dict | None, bool]:
    verified = find_verified(user_id, username)
    if verified:
        return verified, True
    if not cards_demo_online_open():
        return None, False
    if user_id is None and not username:
        return None, False
    cleaned = (username or "").strip().lstrip("@") or None
    display = cleaned or (str(user_id) if user_id is not None else "Player")
    return {
        "user_id": int(user_id) if user_id is not None else None,
        "username": cleaned,
        "display_name": display,
        "label": f"@{cleaned}" if cleaned else display,
    }, False


class CardsIdentity(BaseModel):
    user_id: int
    username: str | None = None
    display_name: str | None = None


class CreateRoomPayload(BaseModel):
    user_id: int
    username: str | None = None
    display_name: str | None = None
    mode: str = "standard"
    max_players: int = Field(default=6, ge=4, le=6)


class JoinRoomPayload(BaseModel):
    user_id: int
    username: str | None = None
    display_name: str | None = None
    loadout: dict | None = None


class QueuePayload(BaseModel):
    user_id: int
    username: str | None = None
    display_name: str | None = None
    mode: str = "standard"
    max_players: int = Field(default=6, ge=4, le=6)
    loadout: dict | None = None


class LoadoutPayload(BaseModel):
    user_id: int
    loadout: dict


class UserIdPayload(BaseModel):
    user_id: int


class RoomActionPayload(BaseModel):
    user_id: int
    action: str
    payload: dict = Field(default_factory=dict)


class CardsAuthPayload(BaseModel):
    init_data: str | None = None
    user_id: int | None = None
    username: str | None = None


@dataclass
class PlayerState:
    user_id: int
    username: str | None
    display_name: str
    loadout: dict
    score: int = 0
    xp: float = 0
    connected: bool = True
    hand: List[dict] = field(default_factory=list)
    xp_multiplier: float = 1.0
    stamp_used: bool = False
    support_used: bool = False

    @property
    def id(self) -> str:
        return str(self.user_id)

    @property
    def is_bot(self) -> bool:
        return self.user_id < 0


@dataclass
class RoomState:
    code: str
    host_id: int
    mode: str
    max_players: int
    players: Dict[int, PlayerState] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    match_id: str | None = None
    stage: str = "lobby"
    round: int = 0
    total_rounds: int = 0
    judge_index: int = 0
    prompt: str = ""
    subject_options: List[str] = field(default_factory=list)
    submissions: List[dict] = field(default_factory=list)
    judge_winner_id: str | None = None
    crowd_votes: Dict[str, str] = field(default_factory=dict)
    feed: List[dict] = field(default_factory=list)
    deadline_at: float | None = None
    resolution: dict = field(default_factory=dict)
    hands: Dict[int, List[dict]] = field(default_factory=dict)
    used_base_answers: List[str] = field(default_factory=list)
    used_prompts: List[str] = field(default_factory=list)
    public_queue: bool = False
    started: bool = False

    def player_list(self) -> List[PlayerState]:
        return list(self.players.values())

    def ordered_players(self) -> List[PlayerState]:
        return sorted(self.player_list(), key=lambda p: p.user_id)


class CardsGameService:
    def __init__(self, verify_user: Callable[[int | None, str | None], dict | None]):
        self._verify_user = verify_user
        self._lock = threading.RLock()
        self._rooms: Dict[str, RoomState] = {}
        self._user_room: Dict[int, str] = {}
        self._queue: Dict[tuple, List[dict]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_event_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def _resolve_player(self, user_id: int | None, username: str | None) -> tuple[dict | None, bool]:
        return cards_resolve_player(user_id, username, self._verify_user)

    def _identity(self, user_id: int | None, username: str | None) -> dict | None:
        player, _verified = self._resolve_player(user_id, username)
        return player

    def status(self, user_id: int | None, username: str | None) -> dict:
        player, is_verified = self._resolve_player(user_id, username)
        if not player:
            return {
                "status": "error",
                "verified": False,
                "online_allowed": False,
                "message": "Open Alcove Cards from Telegram so F.O.X can identify you, or join The Alcove first.",
            }
        profile = profile_summary(int(player["user_id"]))
        room_code = self._user_room.get(int(player["user_id"]))
        room = self._rooms.get(room_code) if room_code else None
        return {
            "status": "ok",
            "verified": is_verified,
            "online_allowed": True,
            "demo_mode": cards_demo_online_open() and not is_verified,
            "bots_enabled": cards_demo_online_open(),
            "user": {
                "user_id": player["user_id"],
                "username": player.get("username"),
                "display_name": player.get("display_name") or player.get("label"),
            },
            "profile": profile,
            "room_code": room_code,
            "in_queue": self._in_queue(int(player["user_id"])),
            "room": self.public_room_snapshot(room) if room else None,
            "hand": self.private_hand(room, int(player["user_id"])) if room else [],
        }

    def _in_queue(self, user_id: int) -> bool:
        for entries in self._queue.values():
            if any(entry["user_id"] == user_id for entry in entries):
                return True
        return False

    def _leave_queue_unlocked(self, user_id: int) -> None:
        for key, entries in list(self._queue.items()):
            self._queue[key] = [e for e in entries if e["user_id"] != user_id]
            if not self._queue[key]:
                self._queue.pop(key, None)

    def _make_code(self) -> str:
        for _ in range(50):
            code = "".join(random.choices(string.digits, k=6))
            if code not in self._rooms:
                return code
        return str(random.randint(100000, 999999))

    def create_room(self, payload: CreateRoomPayload) -> dict:
        user = self._identity(payload.user_id, payload.username)
        if not user:
            return {"status": "error", "message": "Verified Alcove membership required."}
        uid = int(user["user_id"])
        profile = profile_summary(uid)
        with self._lock:
            self._leave_room_unlocked(uid)
            self._leave_queue_unlocked(uid)
            code = self._make_code()
            player = PlayerState(
                user_id=uid,
                username=user.get("username"),
                display_name=user.get("display_name") or user.get("label") or str(uid),
                loadout=profile.get("loadout") or default_loadout(),
            )
            room = RoomState(code=code, host_id=uid, mode=payload.mode, max_players=payload.max_players, players={uid: player})
            room.feed.append({"system": True, "text": f"{player.display_name} created a private room."})
            self._rooms[code] = room
            self._user_room[uid] = code
        self._schedule_broadcast(code)
        return {"status": "ok", "room_code": code, "room": self.public_room_snapshot(room)}

    def join_room(self, code: str, payload: JoinRoomPayload) -> dict:
        user = self._identity(payload.user_id, payload.username)
        if not user:
            return {"status": "error", "message": "Verified Alcove membership required."}
        uid = int(user["user_id"])
        with self._lock:
            room = self._rooms.get(code)
            if not room:
                return {"status": "error", "message": "Room not found."}
            if room.started:
                if uid in room.players:
                    loadout = payload.loadout or profile_summary(uid).get("loadout") or default_loadout()
                    room.players[uid].loadout = loadout
                    room.players[uid].connected = True
                    self._user_room[uid] = code
                    snapshot = self.public_room_snapshot(room)
                    hand = self.private_hand(room, uid)
                else:
                    return {"status": "error", "message": "Match already in progress."}
                self._schedule_broadcast(code)
                return {"status": "ok", "room": snapshot, "hand": hand, "reconnected": True}
            if len(room.players) >= room.max_players and uid not in room.players:
                return {"status": "error", "message": "Room is full."}
            self._leave_queue_unlocked(uid)
            if uid in self._user_room and self._user_room[uid] != code:
                self._leave_room_unlocked(uid)
            loadout = payload.loadout or profile_summary(uid).get("loadout") or default_loadout()
            if uid not in room.players:
                room.players[uid] = PlayerState(
                    user_id=uid,
                    username=user.get("username"),
                    display_name=user.get("display_name") or user.get("label") or str(uid),
                    loadout=loadout,
                )
                room.feed.append({"system": True, "text": f"{room.players[uid].display_name} joined the room."})
            else:
                room.players[uid].loadout = loadout
                room.players[uid].connected = True
            self._user_room[uid] = code
        self._schedule_broadcast(code)
        snapshot = self.public_room_snapshot(room)
        hand = self.private_hand(room, uid) if room.started else []
        return {"status": "ok", "room": snapshot, "hand": hand}

    def sync_room(self, code: str, user_id: int | None, username: str | None) -> dict:
        user = self._identity(user_id, username)
        if not user:
            return {"status": "error", "message": "Could not identify player."}
        uid = int(user["user_id"])
        with self._lock:
            room = self._rooms.get(code)
            if not room or uid not in room.players:
                return {"status": "error", "message": "You are not in this room."}
            snapshot = self.public_room_snapshot(room)
            hand = self.private_hand(room, uid)
        return {"status": "ok", "room": snapshot, "hand": hand, "my_user_id": uid}

    def leave_room(self, user_id: int) -> dict:
        with self._lock:
            code = self._leave_room_unlocked(user_id)
        if code:
            self._schedule_broadcast(code)
        return {"status": "ok"}

    def _leave_room_unlocked(self, user_id: int) -> str | None:
        code = self._user_room.pop(user_id, None)
        if not code:
            return None
        room = self._rooms.get(code)
        if not room:
            return code
        player = room.players.get(user_id)
        if not player:
            return code
        room.players.pop(user_id, None)
        room.feed.append({"system": True, "text": f"{player.display_name} left the room."})
        if not room.players:
            self._rooms.pop(code, None)
        elif room.host_id == user_id:
            humans = [pid for pid, p in room.players.items() if not p.is_bot]
            if humans:
                room.host_id = humans[0]
            else:
                room.host_id = next(iter(room.players))
        return code

    def join_queue(self, payload: QueuePayload) -> dict:
        user = self._identity(payload.user_id, payload.username)
        if not user:
            return {"status": "error", "message": "Verified Alcove membership required."}
        uid = int(user["user_id"])
        key = (payload.mode, payload.max_players)
        entry = {
            "user_id": uid,
            "username": user.get("username"),
            "display_name": user.get("display_name") or user.get("label") or str(uid),
            "loadout": payload.loadout or profile_summary(uid).get("loadout") or default_loadout(),
        }
        with self._lock:
            self._leave_room_unlocked(uid)
            self._leave_queue_unlocked(uid)
            self._queue.setdefault(key, []).append(entry)
            formed = self._try_form_queue_room_unlocked(key)
        if formed:
            self._schedule_broadcast(formed)
            return {"status": "ok", "matched": True, "room_code": formed, "room": self.public_room_snapshot(self._rooms[formed])}
        return {"status": "ok", "matched": False, "queue_size": len(self._queue.get(key, []))}

    def leave_queue(self, user_id: int) -> dict:
        with self._lock:
            self._leave_queue_unlocked(user_id)
        return {"status": "ok"}

    def _try_form_queue_room_unlocked(self, key: tuple) -> str | None:
        entries = self._queue.get(key, [])
        mode, max_players = key
        if len(entries) < MIN_PLAYERS:
            return None
        picked = entries[:max_players]
        self._queue[key] = entries[max_players:]
        if not self._queue[key]:
            self._queue.pop(key, None)
        code = self._make_code()
        host_id = picked[0]["user_id"]
        room = RoomState(code=code, host_id=host_id, mode=mode, max_players=max_players, public_queue=True)
        for entry in picked:
            room.players[entry["user_id"]] = PlayerState(
                user_id=entry["user_id"],
                username=entry.get("username"),
                display_name=entry["display_name"],
                loadout=entry["loadout"],
            )
            self._user_room[entry["user_id"]] = code
        room.feed.append({"system": True, "text": "Public match found. Waiting for host to start."})
        self._rooms[code] = room
        return code

    def set_loadout(self, payload: LoadoutPayload) -> dict:
        user = self._identity(payload.user_id, None)
        if not user:
            return {"status": "error", "message": "Verified Alcove membership required."}
        uid = int(user["user_id"])
        save_loadout(uid, payload.loadout)
        with self._lock:
            code = self._user_room.get(uid)
            if code and code in self._rooms:
                player = self._rooms[code].players.get(uid)
                if player:
                    player.loadout = payload.loadout
                    self._schedule_broadcast(code)
        return {"status": "ok", "loadout": payload.loadout}

    def handle_action(self, code: str, payload: RoomActionPayload) -> dict:
        with self._lock:
            room = self._rooms.get(code)
            if not room:
                return {"status": "error", "message": "Room not found."}
            if payload.user_id not in room.players:
                return {"status": "error", "message": "You are not in this room."}
            action = payload.action
            actor_id = payload.user_id
            data = payload.payload or {}
            if action == "start_match":
                result = self._start_match(room, payload.user_id)
            elif action == "pick_subject":
                result = self._pick_subject(room, payload.user_id, int(data.get("index", 0)))
            elif action == "submit_card":
                result = self._submit_card(room, payload.user_id, data.get("card_id"), bool(data.get("attach_booster")))
            elif action == "judge_pick":
                result = self._judge_pick(room, payload.user_id, data.get("submission_id"))
            elif action == "crowd_vote":
                result = self._crowd_vote(room, payload.user_id, data.get("submission_id"))
            elif action == "next_round":
                result = self._next_round(room, payload.user_id)
            elif action == "feed_message":
                result = self._feed_message(room, payload.user_id, str(data.get("text", ""))[:120])
            elif action == "fill_bots":
                result = self._fill_bots(room, payload.user_id, int(data.get("count", 0) or 0))
            elif action == "abort_match":
                result = self._abort_match(room, payload.user_id)
            else:
                return {"status": "error", "message": f"Unknown action: {action}"}
            if result.get("status") == "ok" and action != "abort_match":
                self._advance_bots(room)
            if result.get("status") == "ok" and action in ("fill_bots", "start_match", "abort_match", "submit_card", "pick_subject", "feed_message", "judge_pick", "crowd_vote"):
                enriched = {**result, "room": self.public_room_snapshot(room)}
                if action == "start_match":
                    enriched["hand"] = self.private_hand(room, actor_id)
                elif action == "abort_match":
                    enriched["hand"] = []
                elif action in ("submit_card", "pick_subject"):
                    enriched["hand"] = self.private_hand(room, actor_id)
                result = enriched
            if result.get("broadcast", True):
                self._schedule_broadcast(code)
            return result

    def _start_match(self, room: RoomState, user_id: int) -> dict:
        if room.host_id != user_id:
            return {"status": "error", "message": "Only the host can start the match.", "broadcast": False}
        if len(room.players) < MIN_PLAYERS:
            return {"status": "error", "message": f"Need at least {MIN_PLAYERS} players.", "broadcast": False}
        if room.started:
            return {"status": "error", "message": "Match already started.", "broadcast": False}
        room.started = True
        room.match_id = str(uuid.uuid4())
        count = len(room.players)
        multiplier = 2 if room.mode == "extended" else 1
        room.total_rounds = count * multiplier
        room.round = 0
        room.judge_index = random.randint(0, count - 1)
        room.used_base_answers = []
        room.used_prompts = []
        for player in room.players.values():
            player.score = 0
            player.xp = 0
            player.stamp_used = False
            player.support_used = False
            player.xp_multiplier = 1.0
        room.feed.append({"system": True, "text": "Match started."})
        self._begin_round(room)
        self._advance_bots(room)
        return {"status": "ok"}

    def _abort_match(self, room: RoomState, user_id: int) -> dict:
        if room.host_id != user_id:
            return {"status": "error", "message": "Only the host can abort the match.", "broadcast": False}
        if not room.started and room.stage == "lobby":
            return {"status": "ok", "message": "Match already in the lobby."}
        room.started = False
        room.stage = "lobby"
        room.round = 0
        room.total_rounds = 0
        room.match_id = None
        room.prompt = ""
        room.subject_options = []
        room.submissions = []
        room.judge_winner_id = None
        room.crowd_votes = {}
        room.resolution = {}
        room.deadline_at = None
        room.hands = {}
        room.judge_index = 0
        room.used_base_answers = []
        room.used_prompts = []
        stale = [
            uid
            for uid, player in list(room.players.items())
            if not player.is_bot and uid != room.host_id and not player.connected
        ]
        for uid in stale:
            room.players.pop(uid, None)
            self._user_room.pop(uid, None)
        for player in room.players.values():
            player.score = 0
            player.xp = 0
            player.hand = []
            player.stamp_used = False
            player.support_used = False
            player.xp_multiplier = 1.0
        room.feed.append({"system": True, "text": "Match aborted. Back in the lobby."})
        return {"status": "ok"}

    def _fill_bots(self, room: RoomState, user_id: int, target_count: int = 0) -> dict:
        if not cards_demo_online_open():
            return {"status": "error", "message": "CPU players are only available in demo online mode.", "broadcast": False}
        if room.host_id != user_id:
            return {"status": "error", "message": "Only the host can add CPU players.", "broadcast": False}
        if room.started:
            return {"status": "error", "message": "Match already started.", "broadcast": False}
        goal = target_count if target_count >= MIN_PLAYERS else MIN_PLAYERS
        goal = min(goal, room.max_players)
        if len(room.players) >= goal:
            return {"status": "ok", "added": 0, "message": "Room already has enough players."}
        added = 0
        for bot_id, bot_name in BOT_PLAYERS:
            if bot_id in room.players:
                continue
            if len(room.players) >= goal:
                break
            room.players[bot_id] = PlayerState(
                user_id=bot_id,
                username=None,
                display_name=bot_name,
                loadout=default_loadout(),
                connected=True,
            )
            added += 1
            room.feed.append({"system": True, "text": f"{bot_name} joined as a CPU player."})
        if added == 0:
            return {"status": "error", "message": "No CPU slots available.", "broadcast": False}
        return {"status": "ok", "added": added, "room": self.public_room_snapshot(room)}

    def _advance_bots(self, room: RoomState) -> None:
        if not cards_demo_online_open() or room.stage in ("lobby", "game_over"):
            return
        guard = 0
        while guard < 24:
            guard += 1
            judge = self._current_judge(room)
            progressed = False

            if room.stage == "subject_pick" and judge.is_bot:
                index = random.randrange(len(room.subject_options)) if room.subject_options else 0
                self._pick_subject(room, judge.user_id, index)
                progressed = True
            elif room.stage == "submit":
                for player in room.players.values():
                    if player.user_id == judge.user_id or not player.is_bot:
                        continue
                    if any(sub["player_id"] == str(player.user_id) for sub in room.submissions):
                        continue
                    if not player.hand:
                        continue
                    card = random.choice(player.hand)
                    attach = bool(player.loadout.get("booster")) and random.random() < 0.2
                    self._submit_card(room, player.user_id, card["id"], attach)
                    progressed = True
            elif room.stage == "crowd_vote":
                if judge.is_bot and room.submissions and not room.judge_winner_id:
                    submission = random.choice(room.submissions)
                    self._judge_pick(room, judge.user_id, submission["id"])
                    progressed = True
                voters = [p for p in room.players.values() if p.user_id != judge.user_id]
                for player in voters:
                    if not player.is_bot or str(player.user_id) in room.crowd_votes:
                        continue
                    candidates = [s for s in room.submissions if s["player_id"] != str(player.user_id)]
                    if not candidates:
                        continue
                    submission = random.choice(candidates)
                    self._crowd_vote(room, player.user_id, submission["id"])
                    progressed = True

            if not progressed:
                break

    def _begin_round(self, room: RoomState) -> None:
        room.round += 1
        if room.round > room.total_rounds:
            self._finish_match(room)
            return
        players = room.ordered_players()
        judge = players[room.judge_index % len(players)]
        room.submissions = []
        room.judge_winner_id = None
        room.crowd_votes = {}
        room.resolution = {}
        room.prompt = "Select this round's subject card."
        prompt_pool = [prompt for prompt in PROMPTS if prompt not in room.used_prompts]
        room.subject_options = random.sample(prompt_pool, min(6, len(prompt_pool))) if prompt_pool else random.sample(PROMPTS, min(6, len(PROMPTS)))
        room.stage = "subject_pick"
        room.deadline_at = time.time() + STAGE_TIMERS["subject_pick"]
        room.feed.append({"system": True, "text": f"Round {room.round} started. {judge.display_name} is judging."})
        for player in players:
            if player.user_id == judge.user_id:
                continue
            if room.round == 1:
                player.hand = self._draw_hand(room, player)
            else:
                self._draw_replacement_card(room, player)
            room.hands[player.user_id] = player.hand
        self._advance_bots(room)

    def _take_unique_base(self, room: RoomState) -> str | None:
        pool = [answer for answer in BASE_ANSWERS if answer not in room.used_base_answers]
        if not pool:
            return None
        text = random.choice(pool)
        room.used_base_answers.append(text)
        return text

    def _draw_hand(self, room: RoomState, player: PlayerState) -> List[dict]:
        hand: List[dict] = []
        while len(hand) < 5:
            text = self._take_unique_base(room)
            if not text:
                break
            hand.append({"id": f"{player.user_id}-{room.round}-{len(hand)}", "text": text, "reward": False, "is_new": False})
        return hand

    def _draw_replacement_card(self, room: RoomState, player: PlayerState) -> dict | None:
        for card in player.hand:
            card["is_new"] = False
        text = self._take_unique_base(room)
        if not text:
            return None
        card = {
            "id": f"{player.user_id}-{room.round}-r{len(player.hand)}",
            "text": text,
            "reward": False,
            "is_new": True,
        }
        player.hand.append(card)
        return card

    def _current_judge(self, room: RoomState) -> PlayerState:
        players = room.ordered_players()
        return players[room.judge_index % len(players)]

    def _pick_subject(self, room: RoomState, user_id: int, index: int) -> dict:
        judge = self._current_judge(room)
        uid = int(user_id)
        judge_id = int(judge.user_id)
        if uid != judge_id or room.stage not in ("subject_pick", "subject_wait"):
            return {"status": "error", "message": "Not your turn to pick subject.", "broadcast": False}
        prompt = room.subject_options[index] if 0 <= index < len(room.subject_options) else random.choice(PROMPTS)
        room.prompt = prompt
        if prompt not in room.used_prompts:
            room.used_prompts.append(prompt)
        room.stage = "submit"
        room.deadline_at = time.time() + STAGE_TIMERS["submit"]
        room.feed.append({"system": True, "text": "Subject locked in. Submit your answers."})
        return {"status": "ok"}

    def _submit_card(self, room: RoomState, user_id: int, card_id: str | None, attach_booster: bool) -> dict:
        if room.stage != "submit":
            return {"status": "error", "message": "Not in submit phase.", "broadcast": False}
        judge = self._current_judge(room)
        if user_id == judge.user_id:
            return {"status": "error", "message": "Judge cannot submit.", "broadcast": False}
        if any(sub["player_id"] == str(user_id) for sub in room.submissions):
            return {"status": "error", "message": "Already submitted.", "broadcast": False}
        player = room.players[user_id]
        card = next((c for c in player.hand if c["id"] == card_id), None)
        if not card:
            return {"status": "error", "message": "Invalid card.", "broadcast": False}
        booster_id = player.loadout.get("booster") if attach_booster and player.loadout.get("booster") else None
        submission = {
            "id": f"s-{user_id}-{room.round}",
            "player_id": str(user_id),
            "card": card,
            "booster_id": booster_id,
            "votes": 0,
            "xp": 0,
            "bonus_xp": 0,
        }
        room.submissions.append(submission)
        player.hand = [c for c in player.hand if c["id"] != card_id]
        room.feed.append({"text": f"{player.display_name} submitted an answer.", "player": player.display_name})
        submitters = [p for p in room.players.values() if p.user_id != judge.user_id]
        if len(room.submissions) >= len(submitters):
            room.stage = "crowd_vote"
            room.deadline_at = time.time() + STAGE_TIMERS["crowd_vote"]
            room.feed.append({"system": True, "text": "Vote phase open. Judge picks (+3) while players vote (+1 each)."})
        return {"status": "ok"}

    def _vote_phase_complete(self, room: RoomState) -> bool:
        judge = self._current_judge(room)
        voters_needed = [p for p in room.players.values() if p.user_id != judge.user_id]
        return bool(room.judge_winner_id) and len(room.crowd_votes) >= len(voters_needed)

    def _judge_pick(self, room: RoomState, user_id: int, submission_id: str | None) -> dict:
        if room.stage != "crowd_vote":
            return {"status": "error", "message": "Not in vote phase.", "broadcast": False}
        judge = self._current_judge(room)
        if user_id != judge.user_id:
            return {"status": "error", "message": "Only the judge can pick.", "broadcast": False}
        if room.judge_winner_id:
            return {"status": "error", "message": "Judge already picked.", "broadcast": False}
        if not any(sub["id"] == submission_id for sub in room.submissions):
            return {"status": "error", "message": "Invalid submission.", "broadcast": False}
        room.judge_winner_id = submission_id
        room.feed.append({"system": True, "text": "Judge locked in their pick (+3)."})
        if self._vote_phase_complete(room):
            self._resolve_round(room)
        return {"status": "ok"}

    def _crowd_vote(self, room: RoomState, user_id: int, submission_id: str | None) -> dict:
        if room.stage != "crowd_vote":
            return {"status": "error", "message": "Not crowd vote phase.", "broadcast": False}
        judge = self._current_judge(room)
        if user_id == judge.user_id:
            return {"status": "error", "message": "Judge cannot crowd vote.", "broadcast": False}
        sub = next((s for s in room.submissions if s["id"] == submission_id), None)
        if not sub or sub["player_id"] == str(user_id):
            return {"status": "error", "message": "Invalid vote target.", "broadcast": False}
        room.crowd_votes[str(user_id)] = submission_id
        if self._vote_phase_complete(room):
            self._resolve_round(room)
        return {"status": "ok"}

    def _resolve_round(self, room: RoomState) -> None:
        for sub in room.submissions:
            sub["votes"] = 0
            sub["xp"] = 0
            sub["bonus_xp"] = 0
        for voter_id, sub_id in room.crowd_votes.items():
            sub = next((s for s in room.submissions if s["id"] == sub_id), None)
            if sub:
                sub["votes"] += 1
                owner = room.players.get(int(sub["player_id"]))
                if owner:
                    owner.score += 1

        judge_winner = next((s for s in room.submissions if s["id"] == room.judge_winner_id), room.submissions[0] if room.submissions else None)
        if judge_winner:
            player = room.players[int(judge_winner["player_id"])]
            player.score += 3
            player.xp += 50 * player.xp_multiplier

        shields = {
            sub["player_id"]
            for sub in room.submissions
            if sub.get("booster_id") == "shield"
        }
        for sub in sorted(room.submissions, key=lambda s: BOOSTER_PRIORITY.get(s.get("booster_id") or "", 5)):
            booster_id = sub.get("booster_id")
            if booster_id == "hype":
                sub["votes"] += 2
            elif booster_id == "heckle":
                targets = [s for s in room.submissions if s["player_id"] != sub["player_id"] and s["votes"] > 0]
                if targets:
                    target = random.choice(targets)
                    if target["player_id"] not in shields:
                        target["votes"] = max(0, target["votes"] - 1)
                        sub["votes"] += 1

        max_votes = max((s["votes"] for s in room.submissions), default=0)
        crowd_winners = [s for s in room.submissions if s["votes"] == max_votes and max_votes > 0]
        crowd_bonus = 30
        if len(crowd_winners) > 1:
            share = crowd_bonus / len(crowd_winners)
            for sub in crowd_winners:
                sub["xp"] = share
        elif crowd_winners:
            crowd_winners[0]["xp"] = crowd_bonus

        for sub in room.submissions:
            pid = int(sub["player_id"])
            if pid in room.players:
                room.players[pid].xp += sub.get("xp", 0) * room.players[pid].xp_multiplier

        room.stage = "results"
        room.deadline_at = None
        room.resolution = {
            "judge_winner_id": room.judge_winner_id,
            "crowd_winner_ids": [s["id"] for s in crowd_winners],
            "submissions": deepcopy(room.submissions),
        }
        room.feed.append({"system": True, "text": "Round results settled."})

    def _next_round(self, room: RoomState, user_id: int) -> dict:
        if room.stage != "results":
            return {"status": "error", "message": "Round not finished.", "broadcast": False}
        if room.host_id != user_id:
            return {"status": "error", "message": "Only host advances rounds.", "broadcast": False}
        room.judge_index += 1
        self._begin_round(room)
        return {"status": "ok"}

    def _finish_match(self, room: RoomState) -> None:
        room.stage = "game_over"
        room.deadline_at = None
        ranking = sorted(room.ordered_players(), key=lambda p: (-p.score, -p.xp))
        results = []
        for index, player in enumerate(ranking, start=1):
            if player.is_bot:
                continue
            metrics = {"matches_played": 1}
            if index == 1:
                metrics["matches_won"] = 1
            results.append(
                {
                    "user_id": player.user_id,
                    "placement": index,
                    "xp_earned": player.xp,
                    "points_earned": player.score,
                    "metrics": metrics,
                }
            )
        if room.match_id:
            record_match_results(room.match_id, results, room.mode)
        room.feed.append({"system": True, "text": "Match finished."})

    def _feed_message(self, room: RoomState, user_id: int, text: str) -> dict:
        text = text.strip()
        if len(text) < 1:
            return {"status": "error", "message": "Empty message.", "broadcast": False}
        player = room.players[user_id]
        room.feed.append({"text": text, "player": player.display_name, "user_id": user_id})
        return {"status": "ok"}

    def public_room_snapshot(self, room: RoomState | None) -> dict | None:
        if not room:
            return None
        players = []
        for player in room.ordered_players():
            players.append(
                {
                    "id": player.id,
                    "user_id": player.user_id,
                    "name": player.display_name,
                    "username": player.username,
                    "score": player.score,
                    "xp": round(player.xp, 1),
                    "connected": player.connected,
                    "loadout": player.loadout,
                    "is_bot": player.is_bot,
                }
            )
        public_submissions = []
        for sub in room.submissions:
            entry = {
                "id": sub["id"],
                "player_id": sub["player_id"] if room.stage in ("results", "game_over") else None,
                "card_text": sub["card"]["text"] if room.stage in ("judge_pick", "crowd_vote", "results", "game_over") else None,
                "booster_id": sub.get("booster_id") if room.stage in ("results", "game_over") else None,
                "votes": sub.get("votes", 0) if room.stage in ("results", "game_over") else 0,
                "xp": sub.get("xp", 0) if room.stage in ("results", "game_over") else 0,
            }
            public_submissions.append(entry)
        return {
            "code": room.code,
            "host_id": room.host_id,
            "mode": room.mode,
            "max_players": room.max_players,
            "player_count": len(room.players),
            "min_players": MIN_PLAYERS,
            "bots_enabled": cards_demo_online_open(),
            "started": room.started,
            "stage": room.stage,
            "round": room.round,
            "total_rounds": room.total_rounds,
            "judge_index": room.judge_index,
            "judge_user_id": room.ordered_players()[room.judge_index % max(len(room.players), 1)].user_id if room.players else None,
            "prompt": room.prompt,
            "subject_options": room.subject_options if room.stage == "subject_pick" else [],
            "submissions": public_submissions,
            "submitted_user_ids": [str(sub["player_id"]) for sub in room.submissions] if room.stage == "submit" else [],
            "judge_winner_id": room.judge_winner_id,
            "crowd_votes": room.crowd_votes if room.stage in ("results", "game_over") else {k: True for k in room.crowd_votes},
            "players": players,
            "feed": room.feed[-40:],
            "deadline_at": room.deadline_at,
            "resolution": room.resolution,
            "match_id": room.match_id,
        }

    def private_hand(self, room: RoomState, user_id: int) -> List[dict]:
        return room.hands.get(user_id, room.players[user_id].hand if user_id in room.players else [])

    def attach_user(self, user_id: int, connected: bool = True) -> None:
        with self._lock:
            code = self._user_room.get(user_id)
            if code and code in self._rooms:
                player = self._rooms[code].players.get(user_id)
                if player:
                    player.connected = connected
        if code:
            self._schedule_broadcast(code)

    def resync_room(self, code: str, user_id: int) -> dict | None:
        with self._lock:
            room = self._rooms.get(code)
            if not room or user_id not in room.players:
                return None
            snapshot = self.public_room_snapshot(room)
            hand = self.private_hand(room, user_id)
        return {"room": snapshot, "hand": hand, "my_user_id": user_id}

    def cleanup_stale(self) -> None:
        now = time.time()
        with self._lock:
            stale = [code for code, room in self._rooms.items() if now - room.created_at > ROOM_TTL_SECONDS and not room.started]
            for code in stale:
                for uid in list(self._rooms[code].players):
                    self._user_room.pop(uid, None)
                self._rooms.pop(code, None)

    def _schedule_broadcast(self, code: str) -> None:
        if not self._loop or not self._loop.is_running():
            return
        asyncio.run_coroutine_threadsafe(self._broadcast_room(code), self._loop)

    async def _broadcast_room(self, code: str) -> None:
        with self._lock:
            room = self._rooms.get(code)
            if not room:
                return
            snapshot = self.public_room_snapshot(room)
            hands = {uid: self.private_hand(room, uid) for uid in room.players}
        await cards_ws_manager.broadcast_private_hands(code, hands, snapshot)


_service: CardsGameService | None = None


def get_cards_service(verify_user: Callable) -> CardsGameService:
    global _service
    if _service is None:
        _service = CardsGameService(verify_user)
    return _service
