import importlib
import os
import tempfile
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient


class HelpCmsTests(unittest.TestCase):
    def setUp(self):
        self.previous_env = {key: os.environ.get(key) for key in ("HELP_CMS_DB_PATH", "HELP_MEDIA_DIR", "BOT_SYNC_SECRET")}
        self.temp = tempfile.TemporaryDirectory()
        os.environ["HELP_CMS_DB_PATH"] = os.path.join(self.temp.name, "help.sqlite3")
        os.environ["HELP_MEDIA_DIR"] = os.path.join(self.temp.name, "media")
        os.environ["BOT_SYNC_SECRET"] = "test-secret"
        from api import help_cms
        self.help_cms = importlib.reload(help_cms)
        app = FastAPI()
        app.include_router(self.help_cms.router)
        self.client = TestClient(app)

    def tearDown(self):
        self.temp.cleanup()
        for key, value in self.previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def test_seed_preserves_legacy_telegram_menu_and_adds_app_roots(self):
        telegram = self.client.get("/api/help", params={"destination": "telegram"}).json()
        self.assertEqual(telegram["settings"]["introduction"], "_Thank you for accessing the terminal._\n**How can I help today?**")
        self.assertEqual(
            [item["button_text"] for item in telegram["items"]],
            ["🔵 ┃ Group Rules", "🟣 ┃ Message from Ardyn", "🟢 ┃ Mini App Guide", "🟠 ┃ F.A.Q.'s"],
        )
        app = self.client.get("/api/help", params={"destination": "app"}).json()
        self.assertEqual([item["title"] for item in app["items"]], ["LIVE ROOM", "PROFILE", "CONNECT", "ARCHIVE"])

    def test_seed_populates_draft_help_sections_without_exposing_them(self):
        admin = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        roots = {item["title"]: item for item in admin["items"] if item["id"].startswith("app-")}

        self.assertEqual(
            [child["title"] for child in roots["LIVE ROOM"]["children"]],
            ["LOBBY", "DEBATE", "DRAWING CHALLENGE", "DISCUSSIONS"],
        )
        self.assertEqual(
            [child["title"] for child in roots["PROFILE"]["children"]],
            ["EXPERIENCE POINTS", "REWARD PACKS", "ACHIEVEMENTS", "CUSTOMISATION"],
        )
        self.assertEqual(
            [child["title"] for child in roots["CONNECT"]["children"]],
            ["PULSE", "SPOTLIGHT", "DAILY CHECK-IN"],
        )
        self.assertEqual(
            [child["title"] for child in roots["ARCHIVE"]["children"]],
            ["PULSE HISTORY", "SPOTLIGHT HISTORY", "LIVE ROOM HISTORY"],
        )
        for root in roots.values():
            self.assertEqual(
                [child["sort_order"] for child in root["children"]],
                list(range(len(root["children"]))),
            )

        seeded = [
            item
            for root in roots.values()
            for section in root["children"]
            for item in [section, *section["children"]]
        ]
        self.assertTrue(seeded)
        self.assertTrue(all(item["status"] == "draft" for item in seeded))
        self.assertTrue(all(item["media_url"] == "" for item in seeded))
        self.assertTrue(all(item["skin_media_url"] == "" for item in seeded))
        self.assertTrue(all(section["subtitle"] for root in roots.values() for section in root["children"]))
        self.assertTrue(all(tutorial["body"] for root in roots.values() for section in root["children"] for tutorial in section["children"]))
        self.assertEqual(sum(item["type"] == "container" for item in seeded), 14)
        self.assertEqual(sum(item["type"] == "content" for item in seeded), 54)
        con = self.help_cms._conn()
        migration_count = con.execute(
            "SELECT COUNT(*) FROM help_content_migrations WHERE name='app-help-content-v1'"
        ).fetchone()[0]
        con.close()
        self.assertEqual(migration_count, 1)

        public = self.client.get("/api/help", params={"destination": "app"}).json()
        self.assertTrue(all(not root["children"] for root in public["items"] if root["id"].startswith("app-")))

    def test_help_content_seed_is_idempotent_and_preserves_admin_edits(self):
        def flatten(items):
            return [item for row in items for item in [row, *flatten(row["children"])]]

        first = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        first_items = flatten(first["items"])
        pulse = next(
            child
            for root in first["items"]
            for child in root["children"]
            if child["id"] == "help-connect-pulse"
        )
        update = {
            key: value
            for key, value in pulse.items()
            if key not in {"id", "children", "legacy_key", "created_at", "updated_at"}
        }
        update.update(admin_secret="test-secret", subtitle="Ardyn's edited Pulse summary")
        response = self.client.put(f"/api/admin/help/items/{pulse['id']}", json=update)
        self.assertEqual(response.status_code, 200, response.text)

        self.help_cms._conn().close()
        second = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        second_items = flatten(second["items"])
        edited_pulse = next(
            child
            for root in second["items"]
            for child in root["children"]
            if child["id"] == "help-connect-pulse"
        )
        self.assertEqual(len(second_items), len(first_items))
        self.assertEqual(edited_pulse["subtitle"], "Ardyn's edited Pulse summary")

    def test_existing_database_seed_repairs_missing_or_invalid_structural_roots(self):
        con = self.help_cms._conn()
        con.execute("DELETE FROM help_content_migrations WHERE name='app-help-content-v1'")
        con.execute("DELETE FROM help_items WHERE parent_id IN (SELECT id FROM help_items WHERE parent_id='app-profile')")
        con.execute("DELETE FROM help_items WHERE parent_id='app-profile'")
        con.execute("DELETE FROM help_items WHERE id='app-profile'")
        con.execute("UPDATE help_items SET type='content',parent_id='app-live-room',subtitle='Keep this edit' WHERE id='app-connect'")
        con.execute("UPDATE help_items SET type='content',parent_id='app-live-room' WHERE id='help-connect-pulse'")
        con.commit()
        con.close()

        self.help_cms._conn().close()
        admin = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        roots = {item["id"]: item for item in admin["items"]}
        self.assertEqual(roots["app-profile"]["type"], "container")
        self.assertEqual(roots["app-profile"]["status"], "draft")
        self.assertEqual(roots["app-connect"]["type"], "container")
        self.assertEqual(roots["app-connect"]["subtitle"], "Keep this edit")
        pulse = next(child for child in roots["app-connect"]["children"] if child["id"] == "help-connect-pulse")
        self.assertEqual(pulse["type"], "container")

    def test_draft_and_platform_visibility_are_filtered(self):
        created = self.client.post("/api/admin/help/items", json={
            "admin_secret": "test-secret", "internal_name": "Draft", "title": "DRAFT",
            "button_text": "Draft", "type": "content", "status": "draft",
            "show_in_app": True, "show_in_telegram": True,
        })
        self.assertEqual(created.status_code, 200, created.text)
        for destination in ("app", "telegram"):
            payload = self.client.get("/api/help", params={"destination": destination}).json()
            self.assertNotIn("DRAFT", [item["title"] for item in payload["items"]])

    def test_published_child_is_not_promoted_when_parent_is_not_visible(self):
        parent = self.client.post("/api/admin/help/items", json={
            "admin_secret": "test-secret", "internal_name": "Draft parent", "title": "DRAFT PARENT",
            "button_text": "Draft parent", "type": "container", "status": "draft",
            "show_in_app": True, "show_in_telegram": True,
        }).json()["item"]
        child = self.client.post("/api/admin/help/items", json={
            "admin_secret": "test-secret", "internal_name": "Published child", "title": "PUBLISHED CHILD",
            "button_text": "Published child", "type": "content", "status": "published",
            "show_in_app": True, "show_in_telegram": True, "parent_id": parent["id"],
        })
        self.assertEqual(child.status_code, 200, child.text)

        for destination in ("app", "telegram"):
            payload = self.client.get("/api/help", params={"destination": destination}).json()
            self.assertNotIn("PUBLISHED CHILD", [item["title"] for item in payload["items"]])

    def test_reorder_renumbers_siblings_without_ties(self):
        parent = self.client.post("/api/admin/help/items", json={
            "admin_secret": "test-secret", "internal_name": "Parent", "title": "PARENT",
            "button_text": "Parent", "type": "container", "status": "published",
            "show_in_app": True, "show_in_telegram": False,
        }).json()["item"]
        created = []
        for title in ("ONE", "TWO", "THREE"):
            response = self.client.post("/api/admin/help/items", json={
                "admin_secret": "test-secret", "internal_name": title, "title": title,
                "button_text": title, "type": "content", "status": "published",
                "show_in_app": True, "show_in_telegram": False, "parent_id": parent["id"],
            })
            self.assertEqual(response.status_code, 200, response.text)
            created.append(response.json()["item"])

        response = self.client.patch(f"/api/admin/help/items/{created[2]['id']}/position", json={
            "admin_secret": "test-secret", "parent_id": parent["id"], "sort_order": 0,
        })
        self.assertEqual(response.status_code, 200, response.text)
        admin = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        parent_item = next(item for item in admin["items"] if item["id"] == parent["id"])
        moved = parent_item["children"]
        self.assertEqual([item["title"] for item in moved], ["THREE", "ONE", "TWO"])
        self.assertEqual([item["sort_order"] for item in moved], [0, 1, 2])

    def test_reparent_rejects_descendant_cycle(self):
        def create(title, parent_id=None):
            response = self.client.post("/api/admin/help/items", json={
                "admin_secret": "test-secret", "internal_name": title, "title": title,
                "button_text": title, "type": "container", "status": "published",
                "show_in_app": True, "show_in_telegram": True, "parent_id": parent_id,
            })
            self.assertEqual(response.status_code, 200, response.text)
            return response.json()["item"]
        parent = create("Parent")
        child = create("Child", parent["id"])
        response = self.client.patch(f"/api/admin/help/items/{parent['id']}/position", json={
            "admin_secret": "test-secret", "parent_id": child["id"], "sort_order": 0,
        })
        self.assertEqual(response.status_code, 400)

    def test_editing_parent_places_item_at_end_and_renumbers_old_parent(self):
        def create(title, item_type="container", parent_id=None):
            response = self.client.post("/api/admin/help/items", json={
                "admin_secret": "test-secret", "internal_name": title, "title": title,
                "button_text": title, "type": item_type, "status": "published",
                "show_in_app": True, "show_in_telegram": True, "parent_id": parent_id,
            })
            self.assertEqual(response.status_code, 200, response.text)
            return response.json()["item"]
        first_parent = create("FIRST")
        second_parent = create("SECOND")
        moving = create("MOVING", "content", first_parent["id"])
        sibling = create("SIBLING", "content", second_parent["id"])
        body = {key: value for key, value in moving.items() if key not in {"id", "children", "legacy_key", "created_at", "updated_at"}}
        body.update(admin_secret="test-secret", parent_id=second_parent["id"], sort_order=None)
        response = self.client.put(f"/api/admin/help/items/{moving['id']}", json=body)
        self.assertEqual(response.status_code, 200, response.text)
        admin = self.client.get("/api/admin/help", params={"admin_secret": "test-secret"}).json()
        second = next(item for item in admin["items"] if item["id"] == second_parent["id"])
        self.assertEqual([item["id"] for item in second["children"]], [sibling["id"], moving["id"]])
        self.assertEqual([item["sort_order"] for item in second["children"]], [0, 1])

    def test_launcher_media_settings_round_trip(self):
        response = self.client.put("/api/admin/help/settings", json={
            "admin_secret": "test-secret", "terminal_title": "F.O.X HELP TERMINAL",
            "introduction": "How can I help today?", "default_back_wording": "BACK",
            "default_home_wording": "HELP HOME", "default_button_style": "primary",
            "published": True, "launcher_video_url": "/api/help/media/fox.mp4",
            "launcher_fallback_image_url": "/api/help/media/fox.webp",
            "launcher_video_opacity": 75,
        })
        self.assertEqual(response.status_code, 200, response.text)
        public = self.client.get("/api/help", params={"destination": "app"}).json()
        self.assertEqual(public["settings"]["launcher_video_url"], "/api/help/media/fox.mp4")
        self.assertEqual(public["settings"]["launcher_video_opacity"], 75)

    def test_launcher_video_opacity_only_accepts_admin_options(self):
        response = self.client.put("/api/admin/help/settings", json={
            "admin_secret": "test-secret", "terminal_title": "F.O.X HELP TERMINAL",
            "launcher_video_opacity": 40,
        })
        self.assertEqual(response.status_code, 422)

    def test_telegram_root_settings_respect_caption_and_button_limits(self):
        base = {
            "admin_secret": "test-secret", "terminal_title": "F.O.X HELP TERMINAL",
            "default_button_style": "primary", "default_back_wording": "BACK",
            "default_home_wording": "HELP HOME", "published": True,
        }
        self.assertEqual(self.client.put("/api/admin/help/settings", json={**base, "introduction": "x" * 1001}).status_code, 422)
        self.assertEqual(self.client.put("/api/admin/help/settings", json={**base, "default_back_wording": "🦊" * 33}).status_code, 422)
        self.assertEqual(self.client.put("/api/admin/help/settings", json={**base, "default_home_wording": "x" * 65}).status_code, 422)

    def test_telegram_visible_items_respect_caption_and_button_limits(self):
        base = {
            "admin_secret": "test-secret", "internal_name": "Long", "title": "LONG",
            "button_text": "Guide", "type": "content", "status": "published",
            "show_in_app": True, "show_in_telegram": True,
        }
        self.assertEqual(self.client.post("/api/admin/help/items", json={**base, "body": "x" * 1100}).status_code, 422)
        self.assertEqual(self.client.post("/api/admin/help/items", json={**base, "button_text": "x" * 65}).status_code, 422)
        self.assertEqual(self.client.post("/api/admin/help/items", json={**base, "button_text": "🦊" * 33}).status_code, 422)
        app_only = self.client.post("/api/admin/help/items", json={**base, "body": "x" * 1100, "show_in_telegram": False})
        self.assertEqual(app_only.status_code, 200, app_only.text)

    def test_main_application_registers_help_routes(self):
        from api import main
        client = TestClient(main.app)
        self.assertEqual(client.get("/api/help").status_code, 200)
        self.assertEqual(client.get("/api/admin/help", params={"admin_secret": "wrong"}).status_code, 403)

    def test_admin_read_accepts_secret_header_without_query_string(self):
        response = self.client.get("/api/admin/help", headers={"X-Admin-Secret": "test-secret"})
        self.assertEqual(response.status_code, 200, response.text)

    def test_item_can_be_updated_duplicated_hidden_and_deleted(self):
        created = self.client.post("/api/admin/help/items", json={
            "admin_secret":"test-secret","type":"content","internal_name":"Guide","title":"GUIDE",
            "button_text":"Guide","body":"Original","status":"published","show_in_app":True,"show_in_telegram":True,
        }).json()["item"]
        update = dict(created, admin_secret="test-secret", title="UPDATED", body="Changed")
        update.pop("id"); update.pop("children", None); update.pop("legacy_key", None); update.pop("created_at", None); update.pop("updated_at", None)
        self.assertEqual(self.client.put(f"/api/admin/help/items/{created['id']}", json=update).json()["item"]["title"], "UPDATED")
        duplicate = self.client.post(f"/api/admin/help/items/{created['id']}/duplicate", json={"admin_secret":"test-secret"})
        self.assertEqual(duplicate.status_code, 200, duplicate.text)
        self.assertEqual(duplicate.json()["item"]["status"], "draft")
        self.assertEqual(self.client.delete(f"/api/admin/help/items/{created['id']}", params={"admin_secret":"test-secret"}).status_code, 200)


if __name__ == "__main__":
    unittest.main()
