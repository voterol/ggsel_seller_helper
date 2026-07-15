import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from database import Database, Message, SCHEMA_VERSION
from fix_duplicates import fix_processed_messages
from migrate import migrate


class DatabaseMigrationTests(unittest.TestCase):
    def test_upgrades_global_message_identity_and_preserves_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.db"
            with sqlite3.connect(path) as conn:
                conn.execute("CREATE TABLE chats (id_i INTEGER PRIMARY KEY)")
                conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, message_id TEXT UNIQUE, content TEXT, timestamp TIMESTAMP, is_sent_to_telegram BOOLEAN DEFAULT FALSE)")
                conn.execute("INSERT INTO messages (chat_id, message_id, content) VALUES (1, 'same', 'old')")

            db = Database(str(path))
            self.assertTrue(db.last_backup_path)
            self.assertTrue(Path(db.last_backup_path).exists())
            self.assertTrue(db.save_message(Message(2, "same", "new", None)))
            self.assertTrue(db.message_exists("same", chat_id=1))
            self.assertTrue(db.message_exists("same", chat_id=2))
            db.mark_message_sent("same", chat_id=2)
            with sqlite3.connect(path) as conn:
                self.assertEqual(conn.execute("PRAGMA user_version").fetchone()[0], SCHEMA_VERSION)
                rows = conn.execute("SELECT chat_id, is_sent_to_telegram FROM messages ORDER BY chat_id").fetchall()
            self.assertEqual(rows, [(1, 0), (2, 1)])

    def test_json_import_is_additive_and_allows_same_id_in_two_chats(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db_path = root / "configured.db"
            Database(str(db_path))
            with sqlite3.connect(db_path) as conn:
                conn.execute("INSERT INTO purchases VALUES ('existing', '{}')")
            (root / "processed_messages.json").write_text(json.dumps({
                "a": {"chat_id": 1, "message_id": "7", "content": "one"},
                "b": {"chat_id": 2, "message_id": "7", "content": "two"},
            }), encoding="utf-8")
            migrate(str(db_path), str(root))
            with sqlite3.connect(db_path) as conn:
                self.assertEqual(conn.execute("SELECT count(*) FROM messages").fetchone()[0], 2)
                self.assertEqual(conn.execute("SELECT count(*) FROM purchases WHERE invoice_id='existing'").fetchone()[0], 1)
            self.assertTrue(list(root.glob("configured.db.pre-import*.bak")))

    def test_duplicate_fixer_preserves_every_record_and_backup(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "messages.json"
            original = {
                "old-a": {"chat_id": 0, "message_id": "5"},
                "old-b": {"chat_id": 0, "message_id": "5"},
            }
            path.write_text(json.dumps(original), encoding="utf-8")
            backup = Path(fix_processed_messages(str(path)))
            rewritten = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(len(rewritten), len(original))
            self.assertEqual(json.loads(backup.read_text(encoding="utf-8")), original)


if __name__ == "__main__":
    unittest.main()
