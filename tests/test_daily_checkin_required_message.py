import os
import sqlite3
import tempfile
import unittest
from fastapi import HTTPException
from api import daily_checkin as checkin

class RequiredCheckinTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.previous = os.environ.get('ALCOVE_STATE_DB_PATH')
        os.environ['ALCOVE_STATE_DB_PATH'] = os.path.join(self.tmp.name, 'test.db')
        checkin._HITS.clear()
        self.user = {'id': 999000002, 'first_name': 'Test'}
        self.day = checkin.community_clock()['community_date']
    def tearDown(self):
        if self.previous is None:
            os.environ.pop('ALCOVE_STATE_DB_PATH', None)
        else:
            os.environ['ALCOVE_STATE_DB_PATH'] = self.previous
        self.tmp.cleanup()
    def test_blank_messages_cannot_create_checkins_or_rewards(self):
        for text in [None, '', '   ', '\n\t', '\u2003']:
            with self.subTest(text=text):
                with self.assertRaises(HTTPException) as caught:
                    checkin.submit_checkin(self.user, 'good', text, self.day)
                self.assertEqual(caught.exception.status_code, 400)
        connection = checkin._connect()
        for table in ['daily_checkins', 'exp_grants']:
            self.assertEqual(connection.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)
        connection.close()
    def test_message_without_feeling_is_rejected(self):
        with self.assertRaises(HTTPException) as caught:
            checkin.submit_checkin(self.user, '', 'A good day', self.day)
        self.assertEqual(caught.exception.status_code, 400)
    def test_both_inputs_create_one_75_exp_reward_and_retry_preserves_it(self):
        first = checkin.submit_checkin(self.user, 'good', '  A good day  ', self.day)
        self.assertEqual(first['checkin']['message'], 'A good day')
        self.assertEqual(first['award']['amount'], 75)
        retry = checkin.submit_checkin(self.user, 'good', 'Another message', self.day)
        self.assertTrue(retry['recovered'])
        self.assertEqual(first['award']['id'], retry['award']['id'])

if __name__ == '__main__':
    unittest.main()
