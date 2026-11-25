#!/usr/bin/env python3
"""Drive a fake audience against a closed-room rehearsal.

Uses BOT_SYNC_SECRET (same value as Feature Admin) to join/answer as the
91001–91006 rehearsal dummies while agendas/running is status=rehearsal.

Usage:
  BOT_SYNC_SECRET=… python tools/rehearsal_audience.py
  python tools/rehearsal_audience.py --once
  python tools/rehearsal_audience.py --api https://alcove-api.onrender.com/api

Does not print the secret. Safe defaults: no pack grants to dummies (API skips
them once the matching API change is deployed); debate/drawing/poll/QA work
with the bot secret alone.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request

# Keep roster inline so this ops script runs without installing the API package.
ROSTER = [
    {"user_id": "91001", "display_name": "Sam · Rehearsal", "username": "rehearse_sam"},
    {"user_id": "91002", "display_name": "Jamie · Rehearsal", "username": "rehearse_jamie"},
    {"user_id": "91003", "display_name": "Riley · Rehearsal", "username": "rehearse_riley"},
    {"user_id": "91004", "display_name": "Morgan · Rehearsal", "username": "rehearse_morgan"},
    {"user_id": "91005", "display_name": "Taylor · Rehearsal", "username": "rehearse_taylor"},
    {"user_id": "91006", "display_name": "Charlie · Rehearsal", "username": "rehearse_charlie"},
]

DEFAULT_API = "https://alcove-api.onrender.com/api"

HERD_ANSWERS = [
    ["Pepperoni", "Pepperoni", "Mushroom", "Pineapple", "Pepperoni", "Olive"],
    ["Blue", "Green", "Blue", "Red", "Blue", "Yellow"],
    ["Toast", "Eggs", "Toast", "Cereal", "Toast", "Porridge"],
    ["Dog", "Cat", "Dog", "Rabbit", "Dog", "Hamster"],
    ["Tea", "Coffee", "Tea", "Tea", "Chocolate", "Coffee"],
    ["Spain", "Italy", "Spain", "Greece", "Spain", "France"],
    ["Football", "Tennis", "Football", "Rugby", "Football", "Cricket"],
    ["Keys", "Phone", "Keys", "Wallet", "Keys", "Glasses"],
]

DEBATE_THOUGHTS = {
    "FOR": [
        "Sweet and savoury together is why Hawaiian pizza works.",
        "Menus worldwide already settled this — pineapple belongs.",
        "Acid and sweetness cut through cheese in a good way.",
    ],
    "AGAINST": [
        "Fruit juice wrecks a crisp crust. Keep pizza savoury.",
        "Tomato, cheese, meat — pineapple is a distraction.",
        "I want comfort food, not a fruit salad with mozzarella.",
    ],
}

DISCUSSION_QUESTIONS = [
    "What's one thing that made you smile this week?",
    "Anyone else nervous about trying new activities?",
    "What should we rehearse more carefully next time?",
]
DISCUSSION_COMMENTS = [
    "Loving the pace of this dry run.",
    "Overlays look clearer with real content flowing.",
    "Happy to jump into the next round.",
]


class Client:
    def __init__(self, api: str, secret: str):
        self.api = api.rstrip("/")
        self.secret = secret
        self.bot = {"Content-Type": "application/json", "X-Bot-Sync-Secret": secret}
        self.admin = {"Content-Type": "application/json", "X-Alcove-Admin": secret}
        self._seen_qa = False
        self._answered_rounds: set[tuple[str, int]] = set()
        self._joined_games: set[str] = set()
        self._debate_joined = False
        self._thoughts_sent = False
        self._drawing_joined = False
        self._poll_voted: set[str] = set()

    def request(self, method: str, path: str, *, body=None, headers=None):
        url = self.api + path
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"detail": raw[:300]}
            return exc.code, parsed

    def get(self, path: str, headers=None):
        return self.request("GET", path, headers=headers or {})

    def post(self, path: str, body=None, headers=None):
        return self.request("POST", path, body=body or {}, headers=headers or self.bot)


def log(message: str) -> None:
    print(message, flush=True)


def tick(client: Client) -> None:
    status, running = client.get("/agendas/running")
    agenda = (running or {}).get("agenda") if status == 200 else None
    if not agenda or agenda.get("status") != "rehearsal":
        log("Waiting for a rehearsal agenda…")
        return

    _debate(client)
    _question_game(client)
    _drawing(client)
    _discussion(client)
    _poll(client)


def _debate(client: Client) -> None:
    st, state = client.get("/debate/state")
    if st != 200 or not isinstance(state, dict):
        return
    status = state.get("status")
    if status == "pooling" and not client._debate_joined:
        for i, person in enumerate(ROSTER):
            side = "FOR" if i % 2 == 0 else "AGAINST"
            code, _ = client.post(
                "/debate/join",
                {
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "side_preference": side,
                },
            )
            if code == 200:
                log(f"Debate join {person['display_name']} ({side})")
        client._debate_joined = True

    st, audience = client.get("/debate/audience/submissions")
    if st == 200 and audience.get("submissions_open") and not client._thoughts_sent:
        contestants = {str(c.get("user_id")) for c in state.get("contestants") or []}
        for i, person in enumerate(ROSTER):
            if person["user_id"] in contestants:
                continue
            side = "FOR" if i % 2 == 0 else "AGAINST"
            reason = random.choice(DEBATE_THOUGHTS[side])
            code, _ = client.post(
                "/debate/audience/submit",
                {
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "side": side,
                    "reason": reason,
                },
            )
            if code == 200:
                log(f"Debate thought {person['display_name']}")
        client._thoughts_sent = True

    if status in {"idle", "ended"}:
        client._debate_joined = False
        client._thoughts_sent = False


def _question_game(client: Client) -> None:
    st, state = client.get("/question-game/host-state", headers=client.admin)
    if st != 200 or not isinstance(state, dict):
        return
    stage = state.get("stage")
    game_id = state.get("game_id") or ""
    if stage in {"idle", "ended", "finished"} or not game_id:
        client._joined_games.discard(game_id)
        return

    already = {str(p.get("user_id")) for p in (state.get("participants") or [])}
    missing = [p for p in ROSTER if str(p["user_id"]) not in already]
    if missing:
        joined = 0
        denied = 0
        for person in missing:
            code, body = client.post(
                "/question-game/join",
                {
                    "game_id": game_id,
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "style": {"color": "rehearsal", "color_swatch": "#176B67"},
                },
            )
            if code == 200:
                joined += 1
            elif code == 403:
                denied += 1
            else:
                log(f"QG join {person['display_name']} failed ({code}): {body}")
        if joined:
            log(f"Question game joined {joined}/{len(missing)} missing dummies")
            client._joined_games.add(game_id)
        elif denied:
            log("Question game join blocked (membership). Re-registering dummies…")
            try:
                register_members(client)
                client._joined_games.discard(game_id)
            except Exception as exc:
                log(f"Re-register failed: {exc}")
    else:
        client._joined_games.add(game_id)

    if stage != "answering":
        return
    round_no = int(state.get("round") or 0)
    key = (game_id, round_no)
    if key in client._answered_rounds:
        return
    mode = state.get("mode") or "crowd"
    questions = state.get("questions") or []
    qrow = questions[round_no - 1] if 0 < round_no <= len(questions) else {}
    ok = 0
    for index, person in enumerate(ROSTER):
        if mode == "quiz":
            answers = qrow.get("answers") or ["Paris"]
            text = answers[0] if index % 2 == 0 else "No idea"
        else:
            bank = HERD_ANSWERS[(round_no - 1) % len(HERD_ANSWERS)]
            text = bank[index % len(bank)]
        code, body = client.post(
            "/question-game/submit",
            {
                "game_id": game_id,
                "round": round_no,
                "text": text,
                "user_id": person["user_id"],
                "display_name": person["display_name"],
                "username": person["username"],
            },
        )
        detail = ""
        if isinstance(body, dict):
            detail = str(body.get("detail") or "")
        if code == 200:
            ok += 1
            log(f"QG round {round_no} answer {person['display_name']}: {text}")
        elif code == 409 and "already been submitted" in detail.lower():
            ok += 1
        else:
            log(f"QG submit {person['display_name']} failed ({code}): {body}")
    if ok == len(ROSTER):
        client._answered_rounds.add(key)
    elif ok:
        # Partial success — retry remaining next tick.
        log(f"QG round {round_no} partial answers {ok}/{len(ROSTER)}")


def _drawing(client: Client) -> None:
    st, state = client.get("/team-game/state")
    if st != 200 or not isinstance(state, dict):
        return
    status = state.get("status")
    if status != "pooling":
        if status in {"idle", "ended"}:
            client._drawing_joined = False
        return
    if client._drawing_joined:
        return
    joined = 0
    for person in ROSTER:
        code, _ = client.post(
            "/team-game/join",
            {
                "user_id": person["user_id"],
                "display_name": person["display_name"],
                "username": person["username"],
            },
        )
        if code == 200:
            joined += 1
    if joined:
        log(f"Drawing join {joined}/{len(ROSTER)}")
        client._drawing_joined = True


def _discussion(client: Client) -> None:
    st, live = client.get("/app-state")
    discussion = (live or {}).get("room_discussion") if st == 200 and isinstance(live, dict) else {}
    discussion_open = str((discussion or {}).get("status") or "") == "active"
    if not discussion_open:
        client._seen_qa = False
        return
    if client._seen_qa:
        return

    seeded = 0
    for i, person in enumerate(ROSTER):
        kind = "question" if i % 2 == 0 else "comment"
        text = (
            DISCUSSION_QUESTIONS[i % len(DISCUSSION_QUESTIONS)]
            if kind == "question"
            else DISCUSSION_COMMENTS[i % len(DISCUSSION_COMMENTS)]
        )
        code, body = client.post(
            "/room-qa",
            {
                "user_id": person["user_id"],
                "display_name": person["display_name"],
                "username": person["username"],
                "kind": kind,
                "question": text,
                "comment": text,
            },
        )
        if code == 200 and isinstance(body, dict) and body.get("status") == "ok":
            seeded += 1
            log(f"Discussion {kind} from {person['display_name']}")
        elif code == 200 and isinstance(body, dict) and "No active discussion" in str(body.get("message") or ""):
            return
    if seeded:
        client._seen_qa = True


def _poll(client: Client) -> None:
    st, data = client.get("/app-state")
    if st != 200 or not isinstance(data, dict):
        return
    poll = data.get("active_poll")
    if not isinstance(poll, dict) or poll.get("status") not in {"open", "active"}:
        return
    poll_id = str(poll.get("id") or poll.get("poll_id") or "")
    options = poll.get("options") or []
    if not options or (poll_id and poll_id in client._poll_voted):
        return
    for i, person in enumerate(ROSTER):
        option = options[i % len(options)]
        option_id = option.get("id") if isinstance(option, dict) else option
        code, _ = client.post(
            "/room-poll/vote",
            {
                "user_id": person["user_id"],
                "display_name": person["display_name"],
                "option_id": option_id,
                "option": option_id,
            },
        )
        if code == 200:
            log(f"Poll vote {person['display_name']}")
    if poll_id:
        client._poll_voted.add(poll_id)


def register_members(client: Client) -> None:
    """Upsert rehearsal dummies on the live membership roster (no complete snapshot).

    Needed for Herd Mentality / Quiz until the API rehearsal-dummy bypass is deployed.
    Always refreshes verified_at so a prior FOX revocation does not keep blocking joins.
    """
    from datetime import datetime, timezone, timedelta

    st, payload = client.get("/alcove-users", headers=client.bot)
    if st != 200 or not isinstance(payload, dict):
        raise RuntimeError(f"Could not load alcove-users ({st})")
    users = list(payload.get("users") or [])
    by_id = {
        int(u.get("user_id")): u
        for u in users
        if str(u.get("user_id") or "").isdigit()
    }
    now = datetime.now(timezone.utc)
    # Beat any revocation timestamp written moments ago by an authoritative FOX sync.
    fresh = (now + timedelta(seconds=5)).isoformat()
    updated = 0
    for person in ROSTER:
        uid = int(person["user_id"])
        row = by_id.get(uid)
        if row:
            row["verified_at"] = fresh
            row["display_name"] = person["display_name"]
            row["username"] = person["username"]
            row["source"] = "rehearsal_dummy"
            row["last_seen"] = fresh
            updated += 1
            continue
        users.append(
            {
                "user_id": uid,
                "username": person["username"],
                "display_name": person["display_name"],
                "verified_at": fresh,
                "joined_at": fresh,
                "first_seen": fresh,
                "last_seen": fresh,
                "source": "rehearsal_dummy",
                "label": person["display_name"],
                "message_count": 0,
                "link_attempts": 0,
                "tone_flags": 0,
                "active_strikes": 0,
                "fox_form": None,
            }
        )
        updated += 1
    code, result = client.post(
        "/bot-sync/alcove",
        {"complete_membership_snapshot": False, "users": users, "synced_at": now.isoformat()},
        headers=client.bot,
    )
    if code != 200:
        raise RuntimeError(f"bot-sync failed ({code}): {result}")
    log(f"Registered rehearsal members (refreshed {updated}; roster size {result.get('users')})")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api", default=os.getenv("ALCOVE_API_BASE", DEFAULT_API))
    parser.add_argument("--once", action="store_true", help="Run a single tick then exit")
    parser.add_argument("--interval", type=float, default=3.0, help="Seconds between ticks")
    parser.add_argument(
        "--register-members",
        action="store_true",
        help="Temporarily add dummies to the membership roster for Herd/Quiz joins",
    )
    args = parser.parse_args()
    secret = (os.getenv("BOT_SYNC_SECRET") or os.getenv("ALCOVE_FEATURE_ADMIN_SECRET") or "").strip()
    if not secret:
        print("Set BOT_SYNC_SECRET (Feature Admin secret).", file=sys.stderr)
        return 2
    client = Client(args.api, secret)
    if args.register_members:
        register_members(client)
    log(f"Rehearsal audience watching {client.api} as {len(ROSTER)} dummies")
    if args.once:
        tick(client)
        return 0
    last_register = time.time()
    while True:
        try:
            # FOX complete snapshots can revoke dummies; refresh membership every 90s.
            if args.register_members and time.time() - last_register >= 90:
                register_members(client)
                client._joined_games.clear()
                last_register = time.time()
            tick(client)
        except Exception as exc:
            log(f"Tick error: {exc}")
        time.sleep(max(1.0, args.interval))


if __name__ == "__main__":
    raise SystemExit(main())
