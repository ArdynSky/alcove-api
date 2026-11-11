import copy
import os
import tempfile
import unittest
from unittest import mock

from fastapi.testclient import TestClient
from api import main


class PublicWheelTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        state=copy.deepcopy(main.state);state.update(round_status='open',current_round=10,review_prompt_open=True);state['modules']['wheel']=True
        patches=[mock.patch.dict(os.environ,{'ALCOVE_STATE_DB_PATH':self.tmp.name+'/state.db'}),mock.patch.object(main,'state',state),
            *[mock.patch.object(main,k,[]) for k in ['wheel_entries','archived_wheel_entries','video_reviews','wheel_review_history','approved_comments','pending_comments','wheel_reaction_history','wheel_reaction_events']],
            mock.patch.object(main,'wheel_submission_limits',{}),mock.patch.object(main,'muted_users',[]),
            mock.patch.object(main,'current_now_playing',{'id':7,'data':{'display_name':'Host','video_title':'Test'}}),
            mock.patch.object(main,'save_runtime_state'),mock.patch.object(main,'ws_broadcast_bundle'),mock.patch.object(main,'add_notification'),
            mock.patch.object(main,'ensure_wheel_user_engagement',return_value=None)]
        self.patches=patches
        for p in patches:p.start()
        self.client=TestClient(main.app)

    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()

    def guest(self,name='Public Guest'):
        response=self.client.post('/api/public-wheel/session',json={'display_name':name})
        self.assertEqual(response.status_code,200,response.text)
        data=response.json();self.assertLess(data['identity']['user_id'],0)
        return data,{'X-Public-Wheel-Session':data['session_token']}

    def test_guest_can_submit_video_without_telegram_and_cannot_spoof_member(self):
        guest,headers=self.guest()
        r=self.client.post('/api/public-wheel/entry',headers=headers,json={'link':'https://example.com/video','display_name':'Owner','telegram_id':5737853594,'username':'ardyn_sky','note':'Hello'})
        self.assertEqual(r.status_code,200,r.text);self.assertEqual(r.json()['status'],'ok')
        stored=main.wheel_entries[-1]['data']
        self.assertEqual(stored['telegram_id'],guest['identity']['user_id']);self.assertEqual(stored['display_name'],'Public Guest');self.assertIsNone(stored['username'])
        self.assertEqual(main.wheel_entries[-1]['approval_status'],'pending')

    def test_guest_can_chat_review_and_react_using_issued_identity(self):
        guest,headers=self.guest()
        for route,body in [('comment',{'text':'Good evening','user_id':5737853594,'display_name':'Owner'}),('review',{'rating':4,'review':'Great','anonymous':False}),('reaction',{'reaction_key':'love'})]:
            r=self.client.post('/api/public-wheel/'+route,headers=headers,json=body)
            self.assertEqual(r.status_code,200,r.text);self.assertEqual(r.json()['status'],'ok',r.text)
        self.assertEqual(main.approved_comments[-1]['user_id'],guest['identity']['user_id'])
        self.assertEqual(main.video_reviews[-1]['user_id'],guest['identity']['user_id'])

    def test_guest_session_cannot_authorize_member_app_or_legacy_member_routes(self):
        _,headers=self.guest()
        for path in ['/api/membership/access','/api/stream-comment','/api/wheel-entry','/api/daily-checkin','/api/pulse-entry']:
            r=self.client.post(path,headers=headers,json={'text':'test','display_name':'Guest','link':'https://example.com'})
            self.assertEqual(r.status_code,401,(path,r.text))

    def test_closed_round_duplicate_entry_and_muted_chat_rules_still_apply(self):
        _,headers=self.guest()
        body={'link':'https://example.com/video'}
        self.assertEqual(self.client.post('/api/public-wheel/entry',headers=headers,json=body).json()['status'],'ok')
        self.assertIn('already',self.client.post('/api/public-wheel/entry',headers=headers,json=body).json()['message'])
        main.state['round_status']='closed'
        self.assertEqual(self.client.post('/api/public-wheel/entry',headers=headers,json=body).json()['status'],'error')
        main.muted_users.append('public guest')
        self.assertEqual(self.client.post('/api/public-wheel/comment',headers=headers,json={'text':'test'}).json()['status'],'error')

    def test_missing_invalid_and_expired_session_are_rejected(self):
        for token in ['', 'made-up-token']:
            r=self.client.post('/api/public-wheel/comment',headers={'X-Public-Wheel-Session':token},json={'text':'test'})
            self.assertEqual(r.status_code,401,r.text)
        _,headers=self.guest()
        from api import public_wheel
        with mock.patch.object(public_wheel.time,'time',return_value=10**12):
            self.assertEqual(self.client.post('/api/public-wheel/comment',headers=headers,json={'text':'test'}).status_code,401)

    def test_refresh_reuses_same_session_and_invalid_display_names_do_not_create_guests(self):
        guest,headers=self.guest()
        for _ in range(2):
            r=self.client.post('/api/public-wheel/comment',headers=headers,json={'text':'test'})
            self.assertEqual(r.status_code,200)
        for value in ['', '   ', 'x'*49]:
            self.assertGreaterEqual(self.client.post('/api/public-wheel/session',json={'display_name':value}).status_code,400)

    def test_same_display_name_cannot_merge_guest_with_telegram_member(self):
        main.wheel_entries.append({'id':1,'round_id':10,'data':{'telegram_id':12345,'username':'member','display_name':'Same Name'},'approval_status':'pending'})
        guest,headers=self.guest('Same Name')
        self.assertFalse(main.wheel_entry_matches_submitter(main.wheel_entries[0],telegram_id=guest['identity']['user_id'],display_name='Same Name'))
        r=self.client.post('/api/public-wheel/comment',headers=headers,json={'text':'Guest comment'})
        self.assertEqual(r.json()['status'],'ok')
        users=main.get_room_users()
        self.assertEqual(len(users),2)
        by_id={u['user_id']:u for u in users}
        self.assertEqual(by_id[12345]['username'],'member')
        self.assertIsNone(by_id[guest['identity']['user_id']]['username'])
