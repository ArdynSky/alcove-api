import unittest

from tests import test_safety_enforcement as safety_tests
from tests.test_safety_enforcement import BOT_HEADERS, SECRET
from api import safety_enforcement as safety


class CareFollowupTests(unittest.TestCase):
    setUp = safety_tests.SafetyEnforcementTests.setUp
    tearDown = safety_tests.SafetyEnforcementTests.tearDown
    _seed_fox = safety_tests.SafetyEnforcementTests._seed_fox

    def seed(self, rid, resolved='2026-01-01T00:00:00', decision='remove'):
        conn = safety._db()
        conn.execute('INSERT INTO care_reports (id, reporter_id,target_user_id,reason,created_at,status,resolved_at,decision,source) VALUES (?,1,2,?,?,?,?,?,?)',
                     (rid, 'other', resolved, safety.CARE_DECISIONS[decision], resolved, decision, 'telegram'))
        conn.commit()
        conn.close()

    def ack(self, report, outcome='completed', **extra):
        return self.client.post('/api/bot-sync/care-followups/'+report['id']+'/ack', headers=BOT_HEADERS,
                                json=dict(resolution_key=report['resolution_key'], outcome=outcome, **extra))

    def test_oldest_first_and_durable_receipt(self):
        self.seed('new', '2026-02-01T00:00:00')
        self.seed('old')
        items = safety.pending_care_followups(limit=1)
        self.assertEqual([x['id'] for x in items], ['old'])
        self.assertEqual(self.ack(items[0]).status_code, 200)
        self.assertEqual([x['id'] for x in safety.pending_care_followups()], ['new'])
        self.assertTrue(self.ack(items[0]).json()['already_done'])

    def test_header_auth_and_changed_resolution_not_lost(self):
        self.seed('one')
        item = safety.pending_care_followups()[0]
        self.assertEqual(self.client.post('/api/bot-sync/care-followups/one/ack', json=dict(resolution_key=item['resolution_key'], outcome='completed')).status_code, 403)
        self.ack(item)
        conn = safety._db()
        conn.execute("UPDATE care_reports SET resolved_at='2026-03-01T00:00:00', decision='keep', status='kept'")
        conn.commit(); conn.close()
        changed = safety.pending_care_followups()[0]
        self.assertNotEqual(item['resolution_key'], changed['resolution_key'])
        self.assertEqual(self.ack(item).status_code, 409)

    def test_retry_backoff_and_blocked_visible(self):
        self.seed('one')
        item = safety.pending_care_followups()[0]
        self.assertEqual(self.ack(item, 'retry', error='temporary network failure').status_code, 200)
        self.assertEqual(safety.pending_care_followups(), [])
        conn = safety._db()
        conn.execute("UPDATE care_followup_receipts SET next_attempt_at='2000-01-01T00:00:00'")
        conn.commit(); conn.close()
        self.assertEqual(len(safety.pending_care_followups()), 1)
        self.assertEqual(self.ack(item, 'blocked', error='MessageDeleteForbiddenError', result={'deleted':False,'manual_review':True}).status_code, 200)
        self.assertEqual(safety.pending_care_followups(), [])
        rows = self.client.get('/api/admin/safety/care-reports', params={'admin_secret':SECRET}).json()['reports']
        self.assertEqual(rows[0]['followup']['outcome'], 'blocked')
        self.assertFalse(rows[0]['followup']['result']['deleted'])
