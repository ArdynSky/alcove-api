"""Archive contract: real SQLite rows and HTTP routes, no live network calls."""
import datetime
import json
import os
import tempfile
import unittest
from unittest import mock

from fastapi.testclient import TestClient
from api import daily_checkin as dc, main


class DailyCheckInArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.env = mock.patch.dict(os.environ, {"ALCOVE_STATE_DB_PATH": self.tmp.name + "/state.db"})
        self.env.start()
        self.auth = mock.patch.object(main, "verify_telegram_init_data", side_effect=self.verify)
        self.members = mock.patch.object(main, "find_verified_alcove_user", return_value={"user_id": 1, "verified_at": "2026-01-01"})
        self.auth.start()
        self.members.start()
        dc.set_clock(lambda: datetime.datetime(2026, 6, 15, 12, tzinfo=dc.UK_TZ))
        dc._HITS.clear()
        self.client = TestClient(main.app)
        self.conn = dc._connect()

    def tearDown(self):
        self.conn.close()
        dc.set_clock(None)
        self.members.stop()
        self.auth.stop()
        self.env.stop()
        self.tmp.cleanup()

    def verify(self, token):
        if token not in {"1", "2"}:
            raise main.HTTPException(403, "Invalid signature")
        return {"id": int(token), "first_name": "Viewer"}

    def post(self, id, day="2026-06-14", user="2", feeling="good", name="Sam", deleted=None):
        self.conn.execute("INSERT INTO daily_checkins(id,user_id,community_date,feeling,message,display_name,feed_style_json,created_at,deleted_at) VALUES(?,?,?,?,?,?,?,?,?)", (id,user,day,feeling,"hello",name,json.dumps({"color_swatch":"#ABCDEF"}),day+"T10:00:00+00:00",deleted))
        self.conn.commit()

    def reply(self, id, parent, user="1", deleted=None):
        self.conn.execute("INSERT INTO daily_checkin_replies VALUES(?,?,?,?,?,?,?,?)", (id,parent,user,"2026-06-14","reply","Viewer","2026-06-14T11:00:00+00:00",deleted))
        self.conn.commit()

    def get(self, path="", **params):
        return self.client.get("/api/daily-checkin/archive"+path, params=params, headers={"X-Telegram-Init-Data":"1"})

    def ids(self, **params):
        res = self.get(**params)
        self.assertEqual(res.status_code, 200, res.text)
        return [p["id"] for p in res.json()["posts"]]

    def test_archive_requires_signed_verified_member(self):
        self.assertEqual(self.client.get("/api/daily-checkin/archive").status_code, 401)
        self.assertEqual(self.client.get("/api/daily-checkin/archive", headers={"X-Telegram-Init-Data":"invalid"}).status_code, 403)
        with mock.patch.object(main, "find_verified_alcove_user", return_value=None):
            self.assertEqual(self.get().status_code, 403)

    def test_history_excludes_today_future_deleted_without_participation(self):
        self.post("past")
        self.post("today", "2026-06-15")
        self.post("future", "2026-06-16")
        self.post("deleted", user="3", deleted="gone")
        self.assertEqual(self.ids(period="all"), ["past"])
        payload = self.get().json()
        self.assertEqual(payload["community_date"], "2026-06-15")
        self.assertFalse(payload["posts"][0]["can_reply"])
        self.assertEqual(payload["posts"][0]["feed_style"], {"color_swatch":"#ABCDEF"})
        self.assertEqual(self.client.get("/api/daily-checkin/feed?feeling=good", headers={"X-Telegram-Init-Data":"1"}).status_code, 403)
        dc.set_clock(lambda: datetime.datetime(2026, 6, 15, 23, 1, tzinfo=datetime.timezone.utc))
        self.assertEqual(self.ids(period="all"), ["today", "past"])

    def test_combined_filters_and_literal_name_search(self):
        self.post("a", user="1", name="A_100%", feeling="hard")
        self.post("b", "2026-06-13", user="1", name="A_100%", feeling="hard")
        self.post("c", "2026-06-12", user="1", name="AX100z", feeling="hard")
        self.reply("r", "a")
        self.reply("deleted-reply", "b", deleted="gone")
        self.assertEqual(self.ids(mine=True, feeling="hard", period="7d", name="_100%", replies="mine"), ["a"])
        self.assertEqual(self.ids(replies="without"), ["b", "c"])
        self.assertEqual(self.ids(replies="with"), ["a"])

    def test_defaults_periods_and_custom_dates(self):
        for id, day in [("recent","2026-06-14"),("week","2026-06-08"),("month","2026-05-16"),("older","2026-05-15"),("year","2026-01-01"),("old","2025-12-31")]:
            self.post(id,day)
        self.assertEqual(self.ids(), ["recent","week","month"])
        self.assertEqual(self.ids(period="7d"), ["recent","week"])
        self.assertEqual(self.ids(period="year"), ["recent","week","month","older","year"])
        self.assertEqual(self.ids(period="custom", date_from="2026-05-15", date_to="2026-05-16"), ["month","older"])
        for params in [{"period":"custom"},{"period":"custom","date_from":"2026-06-10","date_to":"2026-06-01"},{"period":"custom","date_from":"2026-02-30","date_to":"2026-06-01"},{"period":"bogus"},{"feeling":"bogus"},{"replies":"bogus"},{"sort":"bogus"},{"name":"x"*81}]:
            self.assertEqual(self.get(**params).status_code, 400, params)

    def test_keyset_ties_all_sorts_and_filter_bound_cursors(self):
        for id, user, name in [("a","2","Zed"),("b","3","Amy"),("c","4","Amy")]:
            self.post(id,user=user,name=name)
        for sort, wanted in [("newest",["c","b","a"]),("oldest",["a","b","c"]),("az",["b","c","a"]),("za",["a","c","b"])]:
            found=[]
            cursor=None
            while True:
                params={"period":"all","sort":sort,"limit":1}
                if cursor: params["cursor"]=cursor
                response=self.get(**params)
                self.assertEqual(response.status_code,200,response.text)
                found.extend(p["id"] for p in response.json()["posts"])
                cursor=response.json()["next_cursor"]
                if not cursor: break
            self.assertEqual(found,wanted)
        cursor=self.get(period="all",limit=1).json()["next_cursor"]
        self.assertEqual(self.get(cursor=cursor,period="all",mine=True).status_code,400)
        self.assertEqual(self.get(cursor="junk").status_code,400)
        dc.set_clock(lambda: datetime.datetime(2026,6,16,12,tzinfo=dc.UK_TZ))
        self.assertEqual(self.get(cursor=cursor,period="all").status_code,400)

    def test_limits_and_lazy_historical_replies(self):
        self.post("past")
        for id in ["a","b","c"]: self.reply(id,"past")
        self.reply("gone","past",deleted="gone")
        first=self.get("/past/replies",limit=2).json()
        self.assertEqual([r["id"] for r in first["replies"]],["a","b"])
        self.assertFalse(first["can_reply"])
        self.assertTrue(all(not r["can_delete"] for r in first["replies"]))
        second=self.get("/past/replies",limit=2,cursor=first["next_cursor"]).json()
        self.assertEqual([r["id"] for r in second["replies"]],["c"])
        self.assertIsNone(second["next_cursor"])
        self.post("today","2026-06-15")
        self.assertEqual(self.get("/today/replies").status_code,404)
        self.assertEqual(self.get("/past/replies",cursor="junk").status_code,400)
        for limit in [0,51]: self.assertEqual(self.get(limit=limit).status_code,422)
        self.assertEqual(self.get().json()["posts"][0]["reply_count"],3)
        self.assertNotIn("replies",self.get().json()["posts"][0])

    def test_only_author_can_delete_past_post_and_no_exp_changes(self):
        self.post("own",user="1")
        self.post("other",user="2")
        self.post("today","2026-06-15",user="1")
        self.reply("r","own")
        self.assertTrue(next(p for p in self.get().json()["posts"] if p["id"]=="own")["can_delete"])
        for id, status in [("other",403),("today",404),("own",200),("own",404)]:
            response=self.client.delete("/api/daily-checkin/archive/"+id,headers={"X-Telegram-Init-Data":"1"})
            self.assertEqual(response.status_code,status,response.text)
        self.assertEqual(self.ids(),["other"])
        self.assertEqual(self.get("/own/replies").status_code,404)
        self.assertEqual(self.conn.execute("SELECT message FROM daily_checkin_replies WHERE id='r'").fetchone()[0],"")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM exp_grants").fetchone()[0],0)

    def test_page_bounds_default_twenty_and_max_fifty(self):
        for i in range(55):
            self.post(f'p{i:02}', user=str(i+10))
        default = self.get().json()
        self.assertEqual(len(default['posts']), 20)
        self.assertIsNotNone(default['next_cursor'])
        largest = self.get(limit=50).json()
        self.assertEqual(len(largest['posts']), 50)
        self.assertEqual(len(self.get(limit=50, cursor=largest['next_cursor']).json()['posts']), 5)
        for i in range(55):
            self.reply(f'r{i:02}', 'p00')
        self.assertEqual(len(self.get('/p00/replies').json()['replies']), 20)
        self.assertEqual(len(self.get('/p00/replies', limit=50).json()['replies']), 50)
        for limit in [0,51]:
            self.assertEqual(self.get('/p00/replies', limit=limit).status_code, 422)

    def test_reply_cursor_cannot_cross_parent_or_member(self):
        self.post('a')
        self.post('b', user='3')
        self.reply('r1', 'a')
        self.reply('r2', 'a')
        cursor = self.get('/a/replies', limit=1).json()['next_cursor']
        self.assertEqual(self.get('/b/replies', cursor=cursor).status_code, 400)
        response = self.client.get('/api/daily-checkin/archive/a/replies', params={'cursor':cursor}, headers={'X-Telegram-Init-Data':'2'})
        self.assertEqual(response.status_code, 400)

    def test_deletion_retains_existing_grants_and_uniqueness(self):
        self.post('own', user='1')
        self.conn.execute("INSERT INTO exp_grants(id,user_id,amount,reason,source,source_id,status,created_at) VALUES('grant',1,75,'Daily check-in','daily_checkin','own','pending','2026-06-14')")
        self.conn.commit()
        before = tuple(self.conn.execute('SELECT * FROM exp_grants').fetchone())
        response = self.client.delete('/api/daily-checkin/archive/own', headers={'X-Telegram-Init-Data':'1'})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(tuple(self.conn.execute('SELECT * FROM exp_grants').fetchone()), before)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM daily_checkins WHERE user_id='1' AND community_date='2026-06-14'").fetchone()[0], 1)

    def test_legacy_schema_migrates_before_archive_indexes(self):
        self.conn.close()
        import sqlite3
        path = self.tmp.name + '/legacy.db'
        old = sqlite3.connect(path)
        old.execute("CREATE TABLE daily_checkins(id TEXT PRIMARY KEY,user_id TEXT NOT NULL,community_date TEXT NOT NULL,feeling TEXT NOT NULL,message TEXT NOT NULL DEFAULT '',display_name TEXT NOT NULL,created_at TEXT NOT NULL,deleted_at TEXT,UNIQUE(user_id,community_date))")
        old.execute("INSERT INTO daily_checkins VALUES('old','2','2026-06-14','good','retained','Member','2026-06-14',NULL)")
        old.commit()
        old.close()
        with mock.patch.dict(os.environ, {'ALCOVE_STATE_DB_PATH':path}):
            self.assertEqual(self.ids(period='all'), ['old'])
        self.conn = dc._connect()

    def test_archive_module_can_load_before_main(self):
        # Worker/tools may import the service directly before loading api.main.
        import subprocess
        import sys
        result = subprocess.run([sys.executable, '-c', 'from api import daily_checkin_archive; from api.daily_checkin import router; assert any(r.path == "/api/daily-checkin/archive" for r in router.routes)'], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
