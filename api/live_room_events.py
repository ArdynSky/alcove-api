"""Public LiveRoom invalidations, isolated from Wheel and private activity payloads.

Call ``install_live_room_events(app)`` once during app construction. Existing
activity GET routes remain the source of truth; these frames only tell clients
when to refresh. State is local to one application process (like existing room
state), so multiple worker processes require a shared pub/sub transport.
"""
import asyncio
from uuid import uuid4

import anyio

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from starlette.concurrency import run_in_threadpool

from . import agendas

HEARTBEAT_SECONDS = 30.0
SEND_TIMEOUT_SECONDS = 10.0
_MUTATIONS = {'POST', 'PUT', 'PATCH', 'DELETE'}


def _channel(path: str):
    parts = path.strip('/').split('/')
    if any(part in {'chat', 'presence', 'heartbeat', 'templates'} for part in parts):
        return None
    for prefix, channel in (
        ('/api/agendas', 'agenda'), ('/api/debate', 'debate'),
        ('/api/team-game', 'drawing'), ('/api/question-game', 'question'),
    ):
        if path == prefix or path.startswith(prefix + '/'):
            return channel
    if path.startswith(('/api/room-', '/api/wheel-', '/api/review', '/api/round', '/api/play')):
        return 'room'
    for prefix in ('/api/stream-comment', '/api/comments', '/api/spin-result',
                   '/api/winner', '/api/set-now-playing', '/api/mark-played'):
        if path == prefix or path.startswith(prefix + '/'):
            return 'room'
    return None


class LiveRoomEvents:
    def __init__(self):
        self.boot_id = uuid4().hex
        self.revision = 0
        self.is_live = None
        self.lock = asyncio.Lock()
        self.subscribers = set()

    def _frame(self, changed=()):
        return {'event': 'live_room', 'data': {
            'is_live': self.is_live,
            'revision': f'{self.boot_id}:{self.revision}',
            'changed': list(changed),
        }}

    async def _read_live(self):
        result = await run_in_threadpool(agendas.active_agenda)
        self.is_live = bool(result.get('agenda'))

    async def snapshot(self):
        async with self.lock:
            if self.is_live is None:
                await self._read_live()
            return self._frame()

    async def subscribe(self):
        async with self.lock:
            if self.is_live is None:
                await self._read_live()
            queue = asyncio.Queue(maxsize=1)
            queue.put_nowait(self._frame())
            self.subscribers.add(queue)
            return queue

    async def publish(self, channel):
        async with self.lock:
            if self.is_live is None or channel == 'agenda':
                await self._read_live()
            self.revision += 1
            for queue in self.subscribers:
                changed = {channel}
                if queue.full():
                    # Coalesce pending invalidations without dropping channels.
                    changed.update(queue.get_nowait()['data']['changed'])
                queue.put_nowait(self._frame(sorted(changed)))


class _MutationMiddleware:
    def __init__(self, app, events):
        self.app = app
        self.events = events

    async def __call__(self, scope, receive, send):
        channel = (_channel(scope.get('path', ''))
                   if scope['type'] == 'http' and scope.get('method') in _MUTATIONS else None)
        if channel is None:
            await self.app(scope, receive, send)
            return
        successful = False
        published = False

        async def after_persistence(message):
            nonlocal successful, published
            if message['type'] == 'http.response.start':
                successful = 200 <= message['status'] < 300
            elif (message['type'] == 'http.response.body' and not message.get('more_body', False)
                  and successful and not published):
                published = True
                await self.events.publish(channel)
            await send(message)

        await self.app(scope, receive, after_persistence)


def install_live_room_events(app: FastAPI):
    """Install unauthenticated, metadata-only HTTP and WebSocket endpoints."""
    if hasattr(app.state, 'live_room_events'):
        return
    events = LiveRoomEvents()
    app.state.live_room_events = events
    app.add_middleware(_MutationMiddleware, events=events)

    @app.get('/api/live-room/connection')
    async def live_room_connection():
        return await events.snapshot()

    @app.websocket('/ws/live-room')
    async def live_room_socket(websocket: WebSocket):
        await websocket.accept()
        queue = await events.subscribe()
        initial_frame = queue.get_nowait()

        async def receive_disconnect():
            while True:
                if (await websocket.receive())['type'] == 'websocket.disconnect':
                    return

        async def send_updates():
            await asyncio.wait_for(websocket.send_json(initial_frame), SEND_TIMEOUT_SECONDS)
            last_frame = initial_frame
            while True:
                try:
                    frame = await asyncio.wait_for(queue.get(), HEARTBEAT_SECONDS)
                except asyncio.TimeoutError:
                    # Keep the last delivered revision: a heartbeat must never
                    # advance ahead of a mutation already waiting in the queue.
                    frame = {'event': 'live_room', 'data': {**last_frame['data'], 'changed': []}}
                await asyncio.wait_for(websocket.send_json(frame), SEND_TIMEOUT_SECONDS)
                last_frame = frame

        async def run_until_disconnect(operation, tasks):
            try:
                await operation()
            except (WebSocketDisconnect, RuntimeError, asyncio.TimeoutError):
                pass
            finally:
                tasks.cancel_scope.cancel()

        try:
            async with anyio.create_task_group() as tasks:
                tasks.start_soon(run_until_disconnect, receive_disconnect, tasks)
                tasks.start_soon(run_until_disconnect, send_updates, tasks)
        finally:
            events.subscribers.discard(queue)
