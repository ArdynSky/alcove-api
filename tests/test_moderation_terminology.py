"""Lightweight checks for shared moderation terminology helpers."""

from __future__ import annotations

import ast
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SAFETY_PATH = ROOT / "api" / "safety_enforcement.py"


class ModerationTerminologySourceTests(unittest.TestCase):
    def test_terminology_endpoints_are_defined(self):
        source = SAFETY_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        routes = []
        for node in tree.body:
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for deco in node.decorator_list:
                if not isinstance(deco, ast.Call):
                    continue
                if not isinstance(deco.func, ast.Attribute):
                    continue
                if deco.func.attr not in {"get", "post", "put", "delete"}:
                    continue
                if deco.args and isinstance(deco.args[0], ast.Constant):
                    routes.append(deco.args[0].value)
        self.assertIn("/api/admin/safety/terminology", routes)
        self.assertIn("/api/admin/safety/teach", routes)
        self.assertIn("/api/bot-sync/safety-terminology", routes)
        self.assertIn("/api/admin/safety/terminology/delete", routes)
        self.assertIn("moderation_terminology", source)
        self.assertIn("message_excerpt", source)
        self.assertIn("CREATE TABLE IF NOT EXISTS moderation_terminology", source)


class ModerationTerminologySchemaTests(unittest.TestCase):
    def test_schema_creates_and_stores_phrases(self):
        source = SAFETY_PATH.read_text(encoding="utf-8")
        start = source.index('CREATE TABLE IF NOT EXISTS moderation_terminology')
        end = source.index('"""', start)
        schema = source[start:end]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "state.db"
            conn = sqlite3.connect(path)
            conn.executescript(schema)
            phrase = "this wording is fine for alcove chats"
            row_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO moderation_terminology (
                    id, phrase, decision, source, source_id, queue_item_id,
                    admin_user_id, learned_at, updated_at
                ) VALUES (?, ?, 'teach', 'test', '', '', NULL, 'now', 'now')
                """,
                (row_id, phrase),
            )
            conn.commit()
            rows = conn.execute(
                "SELECT phrase, decision FROM moderation_terminology"
            ).fetchall()
            conn.close()
            self.assertEqual(rows, [(phrase, "teach")])


if __name__ == "__main__":
    unittest.main()
