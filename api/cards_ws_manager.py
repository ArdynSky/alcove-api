from __future__ import annotations

import asyncio
import json
from typing import Any, Dict, List, Optional, Set

from fastapi import WebSocket


class CardsWebSocketManager:
    """Room-scoped WebSocket connections for Alcove Cards."""

    def __init__(self):
        self._rooms: Dict[str, Dict[int, WebSocket]] = {}
        self._user_rooms: Dict[int, str] = {}
        self._lock = asyncio.Lock()

    async def connect(self, room_code: str, user_id: int, websocket: WebSocket) -> None:
        async with self._lock:
            old_room = self._user_rooms.get(user_id)
            if old_room and old_room != room_code:
                await self._disconnect_unlocked(user_id)
            self._rooms.setdefault(room_code, {})[user_id] = websocket
            self._user_rooms[user_id] = room_code

    async def disconnect(self, user_id: int) -> None:
        async with self._lock:
            await self._disconnect_unlocked(user_id)

    async def _disconnect_unlocked(self, user_id: int) -> None:
        room_code = self._user_rooms.pop(user_id, None)
        if not room_code:
            return
        room = self._rooms.get(room_code, {})
        room.pop(user_id, None)
        if not room:
            self._rooms.pop(room_code, None)

    async def send_user(self, room_code: str, user_id: int, event: str, data: Any) -> None:
        async with self._lock:
            websocket = self._rooms.get(room_code, {}).get(user_id)
        if not websocket:
            return
        try:
            await websocket.send_text(json.dumps({"event": event, "data": data}))
        except Exception:
            await self.disconnect(user_id)

    async def broadcast_room(self, room_code: str, event: str, data: Any, exclude: Optional[Set[int]] = None) -> None:
        exclude = exclude or set()
        async with self._lock:
            targets = list(self._rooms.get(room_code, {}).items())
        dead: List[int] = []
        message = json.dumps({"event": event, "data": data})
        for user_id, websocket in targets:
            if user_id in exclude:
                continue
            try:
                await websocket.send_text(message)
            except Exception:
                dead.append(user_id)
        for user_id in dead:
            await self.disconnect(user_id)

    async def broadcast_private_hands(
        self,
        room_code: str,
        hands: Dict[int, Any],
        public_payload: Any,
    ) -> None:
        async with self._lock:
            targets = dict(self._rooms.get(room_code, {}))
        for user_id, websocket in targets.items():
            payload = dict(public_payload)
            payload["hand"] = hands.get(user_id, [])
            try:
                await websocket.send_text(json.dumps({"event": "game_state", "data": payload}))
            except Exception:
                await self.disconnect(user_id)

    def connected_user_ids(self, room_code: str) -> List[int]:
        return list(self._rooms.get(room_code, {}).keys())

    def user_room(self, user_id: int) -> Optional[str]:
        return self._user_rooms.get(user_id)


cards_ws_manager = CardsWebSocketManager()
