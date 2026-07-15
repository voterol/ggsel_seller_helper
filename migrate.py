#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from database import Database


DB_PATH = os.getenv("DATABASE_PATH", "ggsel_bot.db")


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _backup_database(db_path: Path) -> Path:
    backup = db_path.with_name(db_path.name + ".pre-import.bak")
    suffix = 1
    while backup.exists():
        backup = db_path.with_name(db_path.name + f".pre-import.{suffix}.bak")
        suffix += 1
    with sqlite3.connect(str(db_path)) as source, sqlite3.connect(str(backup)) as target:
        source.backup(target)
    return backup


def migrate(db_path: str = DB_PATH, data_dir: str = ".") -> None:
    """Upgrade the database and import legacy JSON without deleting existing data."""
    db_file = Path(db_path).expanduser()
    source_dir = Path(data_dir).expanduser()
    if not source_dir.is_dir():
        raise FileNotFoundError(f"Legacy data directory does not exist: {source_dir}")

    database = Database(str(db_file))
    backup = _backup_database(db_file)
    print(f"Database backup created: {backup}")

    files = {
        name: source_dir / name
        for name in (
            "processed_purchases.json", "topics.json", "processed_messages.json",
            "processed_reviews.json",
        )
    }
    # Parse every input before opening the write transaction. A malformed file
    # therefore cannot leave a partially imported database.
    loaded = {name: _load_json(path) for name, path in files.items() if path.exists()}

    with sqlite3.connect(str(db_file), isolation_level=None) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("BEGIN IMMEDIATE")
        try:
            purchases = loaded.get("processed_purchases.json", {})
            if not isinstance(purchases, dict):
                raise ValueError("processed_purchases.json must contain an object")
            for invoice_id, data in purchases.items():
                conn.execute(
                    "INSERT OR IGNORE INTO purchases (invoice_id, data) VALUES (?, ?)",
                    (str(invoice_id), json.dumps(data, ensure_ascii=False)),
                )

            topics = loaded.get("topics.json", {})
            if not isinstance(topics, dict):
                raise ValueError("topics.json must contain an object")
            for key, data in topics.items():
                conn.execute(
                    "INSERT OR IGNORE INTO topics (key, data) VALUES (?, ?)",
                    (str(key), json.dumps(data, ensure_ascii=False)),
                )

            messages = loaded.get("processed_messages.json", {})
            if not isinstance(messages, dict):
                raise ValueError("processed_messages.json must contain an object")
            seen = set()
            for key, data in messages.items():
                if not isinstance(data, dict):
                    raise ValueError(f"Message {key!r} must contain an object")
                chat_id = data.get("chat_id", 0)
                message_id = str(data.get("message_id", ""))
                identity = (chat_id, message_id)
                if identity in seen:
                    raise ValueError(f"Duplicate message identity in JSON: {identity!r}")
                seen.add(identity)
                conn.execute("INSERT OR IGNORE INTO chats (id_i) VALUES (?)", (chat_id,))
                conn.execute(
                    "INSERT OR IGNORE INTO messages "
                    "(chat_id, message_id, content, timestamp, is_sent_to_telegram) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (chat_id, message_id, data.get("content", ""),
                     data.get("timestamp", data.get("processed_at", "")),
                     data.get("sent_to_telegram", data.get("is_sent_to_telegram", True))),
                )

            reviews = loaded.get("processed_reviews.json", {})
            if isinstance(reviews, list):
                review_items = ((review_id, "") for review_id in reviews)
            elif isinstance(reviews, dict):
                review_items = reviews.items()
            else:
                raise ValueError("processed_reviews.json must contain an object or array")
            for review_id, hash_value in review_items:
                conn.execute(
                    "INSERT OR IGNORE INTO processed_reviews (review_id, hash) VALUES (?, ?)",
                    (str(review_id), str(hash_value)),
                )
            conn.commit()
        except Exception:
            conn.rollback()
            print(f"Import rolled back. Pre-import backup remains at: {backup}")
            raise

    print(f"Migration complete: {db_file}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Safely migrate GGSel legacy data to SQLite")
    parser.add_argument("--db", default=DB_PATH, help="SQLite path (default: DATABASE_PATH or ggsel_bot.db)")
    parser.add_argument("--data-dir", default=".", help="Directory containing legacy JSON files")
    args = parser.parse_args()
    migrate(args.db, args.data_dir)


if __name__ == "__main__":
    main()
