"""Public invalidations use the real agenda persistence and ASGI WebSocket stack."""
import asyncio
import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import agendas


class LiveRoomEventsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = patch.object(agendas, 'DB_PATH', Path(self.temp.name) / 'agendas.db')
        self.db.start()
        self.addCleanup(self.db.stop)
        self.app = FastAPI()
        self.app.include_router(agendas.router)
        for path in ['/api/debate/vote', '/api/team-game/draw', '/api/question-game/answer',
                     '/api/room-qa/open', '/api/question-game/chat',
                     '/api/question-game/presence/heartbeat', '/api/question-game/templates',
                     '/api/unrelated', '/api/agendas-other']:
            self.app.add_api_route(path, lambda: {'ok': True}, methods=['POST'])

        @self.app.post('/api/debate/fail')
        def fail():
            raise HTTPException(403, 'private detail')

    def install(self):
        # A missing installer must be an assertion failure during the initial RED run.
        module = importlib.util.find_spec('api.live_room_events')
        self.assertIsNotNone(module, 'LiveRoom push module is not implemented')
        self.module = importlib.import_module('api.live_room_events')
        self.module.install_live_room_events(self.app)
        return TestClient(self.app)

    def assert_frame(self, frame, live, changed):
        self.assertEqual(set(frame), {'event', 'data'})
        self.assertEqual(frame['event'], 'live_room')
        self.assertEqual(set(frame['data']), {'is_live', 'revision', 'changed'})
        self.assertIs(frame['data']['is_live'], live)
        self.assertEqual(frame['data']['changed'], changed)
        self.assertIsInstance(frame['data']['revision'], str)
        self.assertTrue(frame['data']['revision'])
        return frame['data']['revision']

    def test_closed_initial_snapshot_then_immediate_open_close_without_private_data(self):
        with self.install() as client:
            with client.websocket_connect('/ws/live-room') as socket:
                initial = self.assert_frame(socket.receive_json(), False, [])
                response = client.post('/api/agendas', json={
                    'title': 'PRIVATE AGENDA', 'event_date': '2026-10-21',
                    'start_time': '19:00', 'status': 'active',
                    'items': [{'title': 'PRIVATE ANSWER'}],
                })
                self.assertEqual(response.status_code, 200)
                opened = self.assert_frame(socket.receive_json(), True, ['agenda'])
                self.assertNotEqual(initial, opened)
                self.assert_frame(client.get('/api/live-room/connection').json(), True, [])
                client.post('/api/agendas/' + response.json()['id'] + '/status', json={'status': 'finished'})
                closed = self.assert_frame(socket.receive_json(), False, ['agenda'])
                self.assertNotEqual(opened, closed)

    def test_only_successful_relevant_mutations_advance_revision_and_push(self):
        with self.install() as client:
            with client.websocket_connect('/ws/live-room') as socket:
                revision = self.assert_frame(socket.receive_json(), False, [])
                for path in ['/api/debate/fail', '/api/unrelated', '/api/agendas-other',
                             '/api/question-game/chat', '/api/question-game/presence/heartbeat',
                             '/api/question-game/templates']:
                    client.post(path)
                    self.assertEqual(client.get('/api/live-room/connection').json()['data']['revision'], revision)
                for path, channel in [('/api/debate/vote', 'debate'), ('/api/team-game/draw', 'drawing'),
                                      ('/api/question-game/answer', 'question'), ('/api/room-qa/open', 'room')]:
                    client.post(path)
                    updated = self.assert_frame(socket.receive_json(), False, [channel])
                    self.assertNotEqual(updated, revision)
                    revision = updated

    def test_connections_and_heartbeat_reuse_cache_and_disconnect_cleans_up(self):
        with self.install() as client:
            with patch.object(agendas, 'active_agenda', wraps=agendas.active_agenda) as active:
                first = client.get('/api/live-room/connection').json()
                with patch.object(self.module, 'HEARTBEAT_SECONDS', 0.01):
                    with client.websocket_connect('/ws/live-room') as socket:
                        self.assertEqual(socket.receive_json(), first)
                        self.assertEqual(socket.receive_json(), first)
                        self.assertEqual(len(self.app.state.live_room_events.subscribers), 1)
                self.assertEqual(len(self.app.state.live_room_events.subscribers), 0)
                client.get('/api/live-room/connection')
                self.assertEqual(active.call_count, 1)

    def test_existing_active_agenda_is_live_on_first_connection(self):
        agendas.create_agenda(agendas.AgendaPayload(
            title='Private', event_date='2026-10-21', start_time='19:00', status='active'))
        with self.install() as client:
            with client.websocket_connect('/ws/live-room') as socket:
                self.assert_frame(socket.receive_json(), True, [])

    def test_main_activity_mutations_invalidate_room_metadata(self):
        paths = ['/api/stream-comment', '/api/comments/approve/example', '/api/wheel-reaction',
                 '/api/reviews/reveal/start', '/api/round/start', '/api/playout/load/example',
                 '/api/spin-result', '/api/winner/clear', '/api/set-now-playing/example',
                 '/api/mark-played/example']
        for path in paths:
            self.app.add_api_route(path, lambda: {'private': 'answer'}, methods=['POST'])
        with self.install() as client:
            revision = client.get('/api/live-room/connection').json()['data']['revision']
            for path in paths:
                self.assertEqual(client.post(path).status_code, 200)
                frame = client.get('/api/live-room/connection').json()
                updated = self.assert_frame(frame, False, [])
                self.assertNotEqual(updated, revision, path)
                revision = updated

    def test_slow_subscriber_queue_is_bounded_and_preserves_all_changed_channels(self):
        self.install()

        async def exercise():
            events = self.app.state.live_room_events
            queue = await events.subscribe()
            await events.publish('agenda')
            await events.publish('debate')
            await events.publish('drawing')
            self.assertEqual(queue.qsize(), 1)
            self.assert_frame(queue.get_nowait(), False, ['agenda', 'debate', 'drawing'])
            events.subscribers.discard(queue)

        asyncio.run(exercise())

    def test_revisions_are_unique_between_app_instances(self):
        with self.install() as client:
            first = client.get('/api/live-room/connection').json()['data']['revision']
        second_app = FastAPI()
        self.module.install_live_room_events(second_app)
        with TestClient(second_app) as client:
            self.assertNotEqual(first, client.get('/api/live-room/connection').json()['data']['revision'])


if __name__ == '__main__':
    unittest.main()
