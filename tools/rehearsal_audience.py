#!/usr/bin/env python3
"""Drive a fake audience against a closed-room rehearsal.

Uses BOT_SYNC_SECRET (same value as Feature Admin) to join/answer as the
91001–91012 rehearsal dummies while agendas/running is status=rehearsal.

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
    {"user_id": "91007", "display_name": "Avery · Rehearsal", "username": "rehearse_avery"},
    {"user_id": "91008", "display_name": "Quinn · Rehearsal", "username": "rehearse_quinn"},
    {"user_id": "91009", "display_name": "Jordan · Rehearsal", "username": "rehearse_jordan"},
    {"user_id": "91010", "display_name": "Casey · Rehearsal", "username": "rehearse_casey"},
    {"user_id": "91011", "display_name": "Harper · Rehearsal", "username": "rehearse_harper"},
    {"user_id": "91012", "display_name": "Rowan · Rehearsal", "username": "rehearse_rowan"},
]

DEFAULT_API = "https://alcove-api.onrender.com/api"

# Aligned to Herd Mentality template (pizza / superhero / breakfast / pet /
# lose / chocolate / holiday / bathroom). Each round mixes a shifting majority
# with pairs and singles so the herd leaderboard reshuffles between rounds.
HERD_ANSWERS = [
    # Q1 pizza topping — Pepperoni herd (5), Mushroom pair, rest unique
    ["Pepperoni", "Mushroom", "Pepperoni", "Pineapple", "Pepperoni", "Olive", "Pepperoni", "Ham", "Mushroom", "Onion", "Pepperoni", "Anchovy"],
    # Q2 superhero — Batman herd (4), Spider-Man pair, Superman pair, rest unique
    ["Batman", "Spider-Man", "Superman", "Batman", "Wonder Woman", "Spider-Man", "Batman", "Iron Man", "Superman", "Hulk", "Batman", "Thor"],
    # Q3 breakfast — Eggs herd (4), Toast pair, Cereal pair, rest unique
    ["Eggs", "Toast", "Cereal", "Eggs", "Porridge", "Toast", "Eggs", "Yoghurt", "Cereal", "Fruit", "Eggs", "Pancakes"],
    # Q4 household pet — Cat herd (5), Dog pair, rest unique
    ["Cat", "Dog", "Cat", "Rabbit", "Cat", "Hamster", "Dog", "Fish", "Cat", "Bird", "Cat", "Guinea pig"],
    # Q5 people regularly lose — Phone herd (4), Keys pair, Wallet pair, rest unique
    ["Phone", "Keys", "Wallet", "Phone", "Glasses", "Keys", "Phone", "Umbrella", "Wallet", "Badge", "Phone", "AirPods"],
    # Q6 chocolate bar — Dairy Milk herd (3), Mars pair, Twix pair, KitKat pair, rest unique
    ["Dairy Milk", "Mars", "Twix", "Dairy Milk", "KitKat", "Mars", "Snickers", "Twix", "Dairy Milk", "Bounty", "KitKat", "Galaxy"],
    # Q7 holiday destination — Spain herd (4), Italy pair, Greece pair, rest unique
    ["Spain", "Italy", "Greece", "Spain", "France", "Italy", "Spain", "Portugal", "Greece", "Malta", "Spain", "Croatia"],
    # Q8 bathroom find — Toothbrush herd (5), Towel pair, rest unique
    ["Toothbrush", "Towel", "Toothbrush", "Soap", "Toothbrush", "Shampoo", "Towel", "Mirror", "Toothbrush", "Toilet roll", "Toothbrush", "Flannel"],
]

# Quiz: rotating wrong-sets so different dummies miss different rounds (leaderboard moves).
# Index into ROSTER. ~4–5 wrong each round; overlap shifts so nobody stays on top forever.
QUIZ_WRONG_BY_ROUND = [
    {0, 1, 4, 7, 10},
    {2, 3, 5, 8, 11},
    {0, 3, 6, 9},
    {1, 4, 5, 8, 10},
    {2, 6, 7, 9, 11},
    {0, 2, 4, 8},
    {1, 3, 7, 10, 11},
    {5, 6, 9, 0, 3},
]
QUIZ_WRONG_TEXT = [
    "No idea",
    "Pass",
    "Not sure",
    "Skipped",
    "Hmm",
    "Can't remember",
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
        self._seen_qa_ids: set[str] = set()
        self._answered_rounds: set[tuple[str, int]] = set()
        self._joined_games: set[str] = set()
        self._debate_session = ""
        self._thoughts_session = ""
        self._drawing_session = ""
        self._drawing_submitted: set[str] = set()
        self._poll_voted: set[str] = set()
        self._agenda_id = ""

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

    def post_multipart(self, path: str, fields: dict, files: dict):
        """POST multipart/form-data (drawing uploads). files: name -> (filename, bytes, content_type)."""
        boundary = f"----AlcoveRehearsal{int(time.time() * 1000)}"
        chunks: list[bytes] = []
        for key, value in fields.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode())
            chunks.append(str(value).encode())
            chunks.append(b"\r\n")
        for key, (filename, blob, content_type) in files.items():
            chunks.append(f"--{boundary}\r\n".encode())
            chunks.append(
                f'Content-Disposition: form-data; name="{key}"; filename="{filename}"\r\n'.encode()
            )
            chunks.append(f"Content-Type: {content_type}\r\n\r\n".encode())
            chunks.append(blob)
            chunks.append(b"\r\n")
        chunks.append(f"--{boundary}--\r\n".encode())
        body = b"".join(chunks)
        headers = {
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "X-Bot-Sync-Secret": self.secret,
        }
        req = urllib.request.Request(self.api + path, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                raw = resp.read().decode()
                return resp.status, json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode()
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = {"detail": raw[:300]}
            return exc.code, parsed


def log(message: str) -> None:
    print(message, flush=True)


def tick(client: Client) -> None:
    status, running = client.get("/agendas/running")
    agenda = (running or {}).get("agenda") if status == 200 else None
    if not agenda or agenda.get("status") != "rehearsal":
        log("Waiting for a rehearsal agenda…")
        return
    agenda_id = str(agenda.get("id") or "")
    if agenda_id and agenda_id != client._agenda_id:
        # New rehearsal run — clear session-scoped caches so dummies rejoin.
        client._agenda_id = agenda_id
        client._debate_session = ""
        client._thoughts_session = ""
        client._drawing_session = ""
        client._drawing_submitted.clear()
        client._joined_games.clear()
        client._answered_rounds.clear()
        client._seen_qa_ids.clear()
        client._poll_voted.clear()
        log(f"Rehearsal agenda active ({agenda_id[:8]}…) — dummy audience armed")

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
    session = str(state.get("session_id") or "")
    if status in {"idle", "ended"}:
        return

    already = {str(p.get("user_id")) for p in (state.get("participants") or [])}
    need_join = status in {"pooling", "registration_closed"} and (
        session != client._debate_session or any(p["user_id"] not in already for p in ROSTER)
    )
    if need_join:
        joined = 0
        for i, person in enumerate(ROSTER):
            if person["user_id"] in already:
                continue
            side = "FOR" if i % 2 == 0 else "AGAINST"
            code, body = client.post(
                "/debate/join",
                {
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "side_preference": side,
                },
            )
            if code == 200:
                joined += 1
                log(f"Debate join {person['display_name']} ({side})")
            else:
                log(f"Debate join {person['display_name']} failed ({code}): {body}")
        if joined or len(already) >= len(ROSTER):
            client._debate_session = session

    st, audience = client.get("/debate/audience/submissions")
    if st == 200 and audience.get("submissions_open") and session != client._thoughts_session:
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
        client._thoughts_session = session


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
        palette = [
            "#176B67", "#2F6FED", "#C45C26", "#1F8A5B",
            "#7A3EB1", "#B53A4A", "#2A6F7A", "#8A6B1F",
            "#3D4F8F", "#E94B78", "#28A4E7", "#71C49D",
        ]
        for person in missing:
            color = palette[int(person["user_id"]) % len(palette)]
            code, body = client.post(
                "/question-game/join",
                {
                    "game_id": game_id,
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "style": {"color": "rehearsal", "color_swatch": color},
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
    wrong_set = QUIZ_WRONG_BY_ROUND[(round_no - 1) % len(QUIZ_WRONG_BY_ROUND)]
    for index, person in enumerate(ROSTER):
        if mode == "quiz":
            answers = qrow.get("answers") or ["Paris"]
            if index in wrong_set:
                text = QUIZ_WRONG_TEXT[index % len(QUIZ_WRONG_TEXT)]
            else:
                text = answers[0]
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


def _png_drawing(seed: int, label: str) -> bytes:
    """Tiny unique PNG so each dummy has a distinct rehearsal drawing."""
    try:
        from PIL import Image, ImageDraw
    except ImportError:
        Image = None  # type: ignore
    if Image is None:
        # Minimal 1x1 PNG fallback (still accepted as image/png).
        import base64

        return base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="
        )
    hue = (seed * 47) % 360
    # Approximate HSL → RGB for a saturated wash.
    import colorsys

    r, g, b = colorsys.hls_to_rgb(hue / 360.0, 0.42, 0.65)
    color = (int(r * 255), int(g * 255), int(b * 255))
    img = Image.new("RGB", (640, 480), (12, 18, 28))
    draw = ImageDraw.Draw(img)
    draw.rounded_rectangle((40, 40, 600, 440), radius=36, fill=color)
    draw.ellipse((180, 120, 460, 360), fill=(255, 255, 255, 40) if False else (250, 245, 235))
    draw.text((70, 60), label[:18], fill=(255, 255, 255))
    import io

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


def _drawing(client: Client) -> None:
    st, state = client.get("/team-game/state")
    if st != 200 or not isinstance(state, dict):
        return
    status = state.get("status")
    session = str(state.get("session_id") or "")
    if status in {"idle", "ended"}:
        return
    if session and session != client._drawing_session:
        client._drawing_session = session
        client._drawing_submitted.clear()

    if status == "pooling":
        already = {
            str(p.get("user_id")): p
            for p in (state.get("participants") or [])
        }
        palette = [
            "#176B67", "#2F6FED", "#C45C26", "#1F8A5B",
            "#7A3EB1", "#B53A4A", "#2A6F7A", "#8A6B1F",
            "#3D4F8F", "#E94B78", "#28A4E7", "#71C49D",
        ]
        skins = ["foxlove", "pulse", "spotlight", "basechat", "foxlove", "pulse"]
        colors = ["gold", "rose", "blue", "green", "violet", "base"]
        # Join missing dummies, and refresh empty styles on ones already seated.
        targets = [
            person for person in ROSTER
            if person["user_id"] not in already
            or not (already.get(person["user_id"]) or {}).get("style")
        ]
        joined = 0
        for person in targets:
            idx = int(person["user_id"]) % len(palette)
            code, body = client.post(
                "/team-game/join",
                {
                    "user_id": person["user_id"],
                    "display_name": person["display_name"],
                    "username": person["username"],
                    "style": {
                        "color": colors[idx % len(colors)],
                        "color_swatch": palette[idx],
                        "skin": skins[idx % len(skins)],
                    },
                },
            )
            if code == 200:
                joined += 1
            else:
                log(f"Drawing join {person['display_name']} failed ({code}): {body}")
        if joined:
            log(f"Drawing join/style refresh {joined}/{len(targets)} dummies")
        return

    if status != "running":
        return
    for index, person in enumerate(ROSTER):
        uid = person["user_id"]
        if uid in client._drawing_submitted:
            continue
        png = _png_drawing(index + 1, person["display_name"])
        code, body = client.post_multipart(
            "/team-game/submit",
            {
                "user_id": uid,
                "display_name": person["display_name"],
                "caption": f"Rehearsal sketch by {person['display_name']}",
            },
            {"file": (f"{person['username']}.png", png, "image/png")},
        )
        if code == 200:
            client._drawing_submitted.add(uid)
            log(f"Drawing submit {person['display_name']}")
        elif code == 409 and "already" in str(body).lower():
            client._drawing_submitted.add(uid)
        else:
            log(f"Drawing submit {person['display_name']} failed ({code}): {body}")


def _discussion(client: Client) -> None:
    st, live = client.get("/app-state")
    discussion = (live or {}).get("room_discussion") if st == 200 and isinstance(live, dict) else {}
    discussion_open = str((discussion or {}).get("status") or "") == "active"
    if not discussion_open:
        return
    disc_id = str((discussion or {}).get("discussion_id") or (discussion or {}).get("id") or "active")
    if disc_id in client._seen_qa_ids:
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
        else:
            log(f"Discussion {kind} {person['display_name']} failed ({code}): {body}")
    if seeded:
        client._seen_qa_ids.add(disc_id)


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
