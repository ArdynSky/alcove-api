import json
import os
import tempfile
import unittest
from unittest import mock

from fastapi.testclient import TestClient
from api import main, member_access


class OnboardingAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.patches = [
            mock.patch.dict(os.environ, {'ALCOVE_STATE_DB_PATH': self.tmp.name+'/state.db', 'ALCOVE_VERIFIED_MEMBER_RECOVERIES':'[]', 'RENDER_EXTERNAL_HOSTNAME':'alcove-api.onrender.com'}),
            mock.patch.object(main, 'synced_alcove_users', []),
            mock.patch.object(main, 'miniapp_verifications', []),
            mock.patch.object(main, 'fox_db_rows', return_value=[]),
            mock.patch.object(main, 'verify_telegram_init_data', return_value={'id':42,'username':'newmember'}),
            mock.patch.object(main, 'verify_bot_sync_secret', return_value=None),
            mock.patch.object(main, 'save_runtime_state', return_value=None),
        ]
        for p in self.patches:p.start()
        self.client=TestClient(main.app)
        self.signed={'X-Telegram-Init-Data':'signed'}

    def tearDown(self):
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()

    def test_signed_identity_and_tester_flag_do_not_replace_onboarding(self):
        with mock.patch.object(main,'load_tester_usernames',return_value=['newmember']):
            self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)

    def test_historical_completed_record_and_registry_alone_do_not_grant_new_access(self):
        main.miniapp_verifications.append({'user_id':42,'status':'completed','completed_at':'2026-01-01','selected_pack':'biker'})
        member_access.remember_member({'user_id':42,'verified_at':'2026-01-01'})
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)

    def test_bot_confirmed_returning_member_remains_allowed(self):
        main.synced_alcove_users.append({'user_id':42,'verified_at':'2026-01-01'})
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,200)
        self.assertEqual(self.client.get('/api/membership/status?user_id=42').json()['user'].get('verified_at'),'2026-01-01')

    def test_revoke_overrides_stale_bot_snapshot_and_old_completed_record(self):
        main.synced_alcove_users.append({'user_id':42,'verified_at':'2026-01-01'})
        main.miniapp_verifications.append({'id':1,'user_id':42,'status':'completed','protocol_version':2,'admission_confirmed_at':'2026-01-01','completed_at':'2026-01-01'})
        r=self.client.post('/api/bot-sync/membership/revoke',headers={'X-Bot-Sync-Secret':'test'},json={'user_id':42,'reason':'manual_unverify'})
        self.assertEqual(r.status_code,200)
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)
        self.assertEqual(main.miniapp_verifications[0]['status'],'revoked')
        r=self.client.post('/api/bot-sync/verification/1',json={'status':'completed'},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,409)
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)

    def test_old_final_submit_cannot_create_pending_approval(self):
        r=self.client.post('/api/verification/miniapp',json={'init_data':'signed','feedback':'skipped','selected_pack':'biker'})
        self.assertNotEqual(r.status_code,200)
        self.assertEqual(main.miniapp_verifications,[])

    def test_protocol_flow_only_queues_after_every_checkpoint_and_bot_confirmed_admission(self):
        s=self.client.post('/api/verification/flow/start',json={'init_data':'signed'}).json()['session']
        sid=s['session_id']
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)
        def step(name,value):
            r=self.client.post('/api/verification/flow/step',json={'init_data':'signed','session_id':sid,'step':name,'value':value})
            self.assertEqual(r.status_code,200,r.text)
            return r.json()['session']
        step('ethos',True)
        correct=next(t for t in s['captcha_tiles'] if t['image'].endswith('verify-real-fox.png'))
        step('captcha',correct['id']);step('feedback','Happy to join');step('vote','biker')
        r=self.client.post('/api/verification/miniapp',json={'init_data':'signed','session_id':sid,'feedback':'forged','selected_pack':'pup'})
        self.assertEqual(r.status_code,200,r.text)
        entry=r.json()['verification']
        self.assertEqual(entry['selected_pack'],'biker')
        self.assertEqual(entry['feedback'],'Happy to join')
        self.assertEqual(entry['protocol_version'],2)
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)
        self.client.post('/api/bot-sync/verification/'+str(entry['id']),json={'status':'completed','detail':'join_request_approved','session_id':sid},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,200)

    def test_replica_uses_authority_instead_of_its_local_approved_record(self):
        main.synced_alcove_users.append({'user_id':42,'verified_at':'2026-01-01'})
        with mock.patch.dict(os.environ,{'RENDER_EXTERNAL_HOSTNAME':'ardyn-alcove-api.onrender.com'}),mock.patch.object(member_access,'authority_member',return_value=None) as authority:
            self.assertIsNone(main.find_verified_alcove_user(42))
            authority.assert_called_once_with(42,None)

    def test_explicit_empty_bot_snapshot_clears_membership(self):
        main.synced_alcove_users.append({'user_id':42,'verified_at':'2026-01-01'})
        r=self.client.post('/api/bot-sync/alcove',json={'users':[],'complete_membership_snapshot':True},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,200)
        self.assertEqual(main.synced_alcove_users,[])

    def test_revocation_tombstone_cannot_move_backwards(self):
        member_access.revoke_member(42,'manual','2026-10-01T10:02:00Z')
        self.assertTrue(member_access.revoked_member(42,'2026-10-01T10:01:30Z'))
        member_access.revoke_member(42,'stale_snapshot','2026-10-01T10:01:00Z')
        self.assertTrue(member_access.revoked_member(42,'2026-10-01T10:01:30Z'))

    def test_completed_retry_never_refreshes_old_admission_past_revocation(self):
        main.miniapp_verifications.append({'id':1,'user_id':42,'session_id':'old','protocol_version':2,'status':'completed','completed_at':'2026-01-01','admission_confirmed_at':'2026-01-01'})
        member_access.revoke_member(42,'snapshot','2026-06-01')
        r=self.client.post('/api/bot-sync/verification/1',json={'status':'completed','session_id':'old'},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,200)
        self.assertEqual(main.miniapp_verifications[0]['completed_at'],'2026-01-01')
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,403)

    def queue_session(self):
        session=self.client.post('/api/verification/flow/start',json={'init_data':'signed'}).json()['session']
        sid=session['session_id']
        correct=next(t['id'] for t in session['captcha_tiles'] if t['image'].endswith('verify-real-fox.png'))
        for step,value in [('ethos',True),('captcha',correct),('feedback','Happy to join'),('vote','biker')]:
            r=self.client.post('/api/verification/flow/step',json={'init_data':'signed','session_id':sid,'step':step,'value':value})
            self.assertEqual(r.status_code,200,r.text)
        r=self.client.post('/api/verification/miniapp',json={'init_data':'signed','session_id':sid})
        self.assertEqual(r.status_code,200,r.text)
        return r.json()['verification']

    def test_fresh_session_has_new_id_and_rejects_old_poll_updates_and_rollback(self):
        first=self.queue_session();second=self.queue_session()
        self.assertNotEqual(first['id'],second['id'])
        r=self.client.post('/api/bot-sync/verification/'+str(first['id']),json={'status':'completed','session_id':first['session_id']},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,409)
        r=self.client.post('/api/bot-sync/membership/revoke',json={'user_id':42,'expected_verification_id':first['id'],'expected_session_id':first['session_id']},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,409)
        r=self.client.post('/api/bot-sync/verification/'+str(second['id']),json={'status':'completed','session_id':second['session_id']},headers={'X-Bot-Sync-Secret':'test'})
        self.assertEqual(r.status_code,200)
        self.assertEqual(self.client.post('/api/membership/access',headers=self.signed).status_code,200)
