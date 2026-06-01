"""Cards progression: profiles, match history, daily/weekly challenges."""
from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

UK_TZ = ZoneInfo("Europe/London")

DEFAULT_CHALLENGES = {
    "daily": [
        {"id": "daily_play_2", "label": "Play 2 matches", "target": 2, "metric": "matches_played", "reward": "Daily player badge"},
        {"id": "daily_crowd_win", "label": "Win 1 crowd vote", "target": 1, "metric": "crowd_wins", "reward": "Crowd favourite stamp"},
        {"id": "daily_judge", "label": "Judge 1 round", "target": 1, "metric": "rounds_judged", "reward": "Judge's gavel flair"},
    ],
    "weekly": [
        {"id": "weekly_xp_500", "label": "Earn 500 XP", "target": 500, "metric": "xp_earned", "reward": "Weekly XP boost"},
        {"id": "weekly_wins_3", "label": "Win 3 matches", "target": 3, "metric": "matches_won", "reward": "Champion playmat token"},
        {"id": "weekly_booster", "label": "Use a booster successfully", "target": 1, "metric": "boosters_used", "reward": "Booster bundle"},
    ],
}


def _db_path() -> str:
    return os.getenv("ALCOVE_STATE_DB_PATH", os.path.join(os.getcwd(), "alcove_state.db"))


def _day_key(now: datetime | None = None) -> str:
    current = (now or datetime.now(timezone.utc)).astimezone(UK_TZ)
    return current.strftime("%Y-%m-%d")


def _week_key(now: datetime | None = None) -> str:
    current = (now or datetime.now(timezone.utc)).astimezone(UK_TZ)
    iso = current.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


def ensure_cards_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS cards_profiles (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            display_name TEXT,
            lifetime_xp REAL DEFAULT 0,
            lifetime_points INTEGER DEFAULT 0,
            matches_played INTEGER DEFAULT 0,
            matches_won INTEGER DEFAULT 0,
            loadout_json TEXT DEFAULT '{}',
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS cards_match_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            placement INTEGER,
            xp_earned REAL DEFAULT 0,
            points_earned INTEGER DEFAULT 0,
            mode TEXT,
            finished_at TEXT
        );
        CREATE TABLE IF NOT EXISTS cards_challenge_progress (
            user_id INTEGER NOT NULL,
            challenge_id TEXT NOT NULL,
            period_key TEXT NOT NULL,
            progress REAL DEFAULT 0,
            completed_at TEXT,
            reward_claimed INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, challenge_id, period_key)
        );
        CREATE TABLE IF NOT EXISTS cards_pending_rewards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            reward_label TEXT NOT NULL,
            source TEXT,
            created_at TEXT,
            claimed INTEGER DEFAULT 0
        );
        """
    )


def _connect():
    path = _db_path()
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    ensure_cards_tables(conn)
    return conn


def default_loadout() -> dict:
    return {
        "playmat": "mat_midnight",
        "cardColour": "card_ivory",
        "skinSet": "foxlove_select",
        "avatar": "foxlove_avatar",
        "skin": "skin_foil",
        "chat": "foxlove_chat",
        "booster": "hype",
        "stamp": "spotlight",
        "supportStamp": "golden",
        "equippedRewardCardIds": ["r1", "r2"],
    }


def get_or_create_profile(user_id: int, username: str | None = None, display_name: str | None = None) -> dict:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM cards_profiles WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            profile = dict(row)
            profile["loadout"] = json.loads(profile.pop("loadout_json") or "{}")
            return profile
        now = datetime.now(timezone.utc).isoformat()
        loadout = default_loadout()
        conn.execute(
            """
            INSERT INTO cards_profiles (user_id, username, display_name, loadout_json, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (user_id, username, display_name, json.dumps(loadout), now),
        )
        return {
            "user_id": user_id,
            "username": username,
            "display_name": display_name,
            "lifetime_xp": 0,
            "lifetime_points": 0,
            "matches_played": 0,
            "matches_won": 0,
            "loadout": loadout,
            "updated_at": now,
        }


def save_loadout(user_id: int, loadout: dict) -> dict:
    profile = get_or_create_profile(user_id)
    with _connect() as conn:
        conn.execute(
            "UPDATE cards_profiles SET loadout_json = ?, updated_at = ? WHERE user_id = ?",
            (json.dumps(loadout), datetime.now(timezone.utc).isoformat(), user_id),
        )
    profile["loadout"] = loadout
    return profile


def challenge_period_key(challenge_type: str) -> str:
    return _day_key() if challenge_type == "daily" else _week_key()


def get_challenges_payload(user_id: int) -> dict:
    daily_key = _day_key()
    weekly_key = _week_key()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT challenge_id, period_key, progress, completed_at, reward_claimed FROM cards_challenge_progress WHERE user_id = ?",
            (user_id,),
        ).fetchall()
    progress_map = {(r["challenge_id"], r["period_key"]): dict(r) for r in rows}

    def build(group: str, period_key: str):
        items = []
        for spec in DEFAULT_CHALLENGES[group]:
            entry = progress_map.get((spec["id"], period_key), {})
            progress = float(entry.get("progress") or 0)
            completed = bool(entry.get("completed_at"))
            items.append(
                {
                    **spec,
                    "progress": progress,
                    "completed": completed,
                    "reward_claimed": bool(entry.get("reward_claimed")),
                    "period_key": period_key,
                }
            )
        return items

    return {"daily": build("daily", daily_key), "weekly": build("weekly", weekly_key)}


def _increment_challenge(conn, user_id: int, metric: str, amount: float = 1) -> List[dict]:
    completed: List[dict] = []
    for group, specs in DEFAULT_CHALLENGES.items():
        period_key = challenge_period_key(group)
        for spec in specs:
            if spec["metric"] != metric:
                continue
            row = conn.execute(
                """
                SELECT progress, completed_at FROM cards_challenge_progress
                WHERE user_id = ? AND challenge_id = ? AND period_key = ?
                """,
                (user_id, spec["id"], period_key),
            ).fetchone()
            if row and row["completed_at"]:
                continue
            progress = float(row["progress"] if row else 0) + amount
            completed_at = None
            if progress >= spec["target"]:
                progress = spec["target"]
                completed_at = datetime.now(timezone.utc).isoformat()
                completed.append({"challenge_id": spec["id"], "reward": spec["reward"], "period_key": period_key})
            conn.execute(
                """
                INSERT INTO cards_challenge_progress (user_id, challenge_id, period_key, progress, completed_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id, challenge_id, period_key) DO UPDATE SET
                    progress = excluded.progress,
                    completed_at = COALESCE(cards_challenge_progress.completed_at, excluded.completed_at)
                """,
                (user_id, spec["id"], period_key, progress, completed_at),
            )
    return completed


def record_match_results(match_id: str, results: List[dict], mode: str) -> List[dict]:
    """results: [{user_id, placement, xp_earned, points_earned, metrics: {...}}]"""
    pending_rewards: List[dict] = []
    now = datetime.now(timezone.utc).isoformat()
    with _connect() as conn:
        for item in results:
            user_id = int(item["user_id"])
            xp = float(item.get("xp_earned") or 0)
            points = int(item.get("points_earned") or 0)
            placement = int(item.get("placement") or 0)
            metrics = item.get("metrics") or {}
            conn.execute(
                """
                INSERT INTO cards_match_results (match_id, user_id, placement, xp_earned, points_earned, mode, finished_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (match_id, user_id, placement, xp, points, mode, now),
            )
            won = 1 if placement == 1 else 0
            conn.execute(
                """
                UPDATE cards_profiles SET
                    lifetime_xp = lifetime_xp + ?,
                    lifetime_points = lifetime_points + ?,
                    matches_played = matches_played + 1,
                    matches_won = matches_won + ?,
                    updated_at = ?
                WHERE user_id = ?
                """,
                (xp, points, won, now, user_id),
            )
            conn.execute(
                """
                INSERT INTO cards_profiles (user_id, lifetime_xp, lifetime_points, matches_played, matches_won, loadout_json, updated_at)
                SELECT ?, ?, ?, 1, ?, ?, ?
                WHERE NOT EXISTS (SELECT 1 FROM cards_profiles WHERE user_id = ?)
                """,
                (user_id, xp, points, won, json.dumps(default_loadout()), now, user_id),
            )
            for metric, amount in metrics.items():
                rewards = _increment_challenge(conn, user_id, metric, amount)
                for reward in rewards:
                    conn.execute(
                        """
                        INSERT INTO cards_pending_rewards (user_id, reward_label, source, created_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (user_id, reward["reward"], f"challenge:{reward['challenge_id']}", now),
                    )
                    pending_rewards.append({"user_id": user_id, **reward})
            _increment_challenge(conn, user_id, "matches_played", 1)
            if won:
                _increment_challenge(conn, user_id, "matches_won", 1)
            if xp:
                _increment_challenge(conn, user_id, "xp_earned", xp)
    return pending_rewards


def profile_summary(user_id: int) -> dict:
    profile = get_or_create_profile(user_id)
    return {
        "user_id": profile["user_id"],
        "username": profile.get("username"),
        "display_name": profile.get("display_name"),
        "lifetime_xp": profile.get("lifetime_xp", 0),
        "lifetime_points": profile.get("lifetime_points", 0),
        "matches_played": profile.get("matches_played", 0),
        "matches_won": profile.get("matches_won", 0),
        "loadout": profile.get("loadout") or default_loadout(),
        "challenges": get_challenges_payload(user_id),
    }


def fetch_pending_rewards(limit: int = 50) -> List[dict]:
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT id, user_id, reward_label, source, created_at
            FROM cards_pending_rewards WHERE claimed = 0
            ORDER BY id ASC LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def mark_rewards_claimed(reward_ids: List[int]) -> None:
    if not reward_ids:
        return
    placeholders = ",".join("?" for _ in reward_ids)
    with _connect() as conn:
        conn.execute(f"UPDATE cards_pending_rewards SET claimed = 1 WHERE id IN ({placeholders})", reward_ids)
