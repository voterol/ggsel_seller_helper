import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence, Tuple


SCHEMA_VERSION = 5


@dataclass
class Chat:
    id_i: int
    email: Optional[str]
    product: int
    last_message: str
    cnt_msg: int
    cnt_new: int
    telegram_topic_id: Optional[int] = None


@dataclass
class Message:
    chat_id: int
    message_id: str
    content: str
    timestamp: datetime
    is_sent_to_telegram: bool = False


class Database:
    def __init__(self, db_path: str):
        self.db_path = str(db_path)
        self.last_backup_path: Optional[str] = None
        self.init_db()

    def _backup(self) -> str:
        """Create a consistent pre-migration copy without modifying the source."""
        path = Path(self.db_path)
        backup = path.with_name(path.name + ".pre-v2.bak")
        suffix = 1
        while backup.exists():
            backup = path.with_name(path.name + f".pre-v2.{suffix}.bak")
            suffix += 1
        # SQLite's backup API also safely captures databases using WAL mode.
        with sqlite3.connect(self.db_path) as source, sqlite3.connect(str(backup)) as target:
            source.backup(target)
        self.last_backup_path = str(backup)
        return str(backup)

    @staticmethod
    def _create_schema(conn: sqlite3.Connection) -> None:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS chats (
                id_i INTEGER PRIMARY KEY, email TEXT, product INTEGER,
                last_message TEXT, cnt_msg INTEGER, cnt_new INTEGER,
                telegram_topic_id INTEGER, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER,
                message_id TEXT, content TEXT, timestamp TIMESTAMP,
                is_sent_to_telegram BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (chat_id) REFERENCES chats (id_i),
                UNIQUE (chat_id, message_id)
            )
        ''')
        conn.execute('CREATE TABLE IF NOT EXISTS topics (key TEXT PRIMARY KEY, data TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS purchases (invoice_id TEXT PRIMARY KEY, data TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS processed_reviews (review_id TEXT PRIMARY KEY, hash TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS pending_topics (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS service_settings (
                key TEXT PRIMARY KEY, value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS message_effects (
                chat_id INTEGER NOT NULL, message_id TEXT NOT NULL,
                effect TEXT NOT NULL, status TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (chat_id, message_id, effect)
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS review_effects (
                review_id TEXT NOT NULL, review_hash TEXT NOT NULL,
                effect TEXT NOT NULL, status TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (review_id, review_hash, effect)
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)')

    @staticmethod
    def _messages_has_composite_identity(conn: sqlite3.Connection) -> bool:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='messages'"
        ).fetchone() is None:
            return True
        for index in conn.execute("PRAGMA index_list(messages)").fetchall():
            if not index[2]:
                continue
            columns = [row[2] for row in conn.execute(
                f'PRAGMA index_info("{index[1]}")'
            ).fetchall()]
            if columns == ["chat_id", "message_id"]:
                return True
        return False

    @staticmethod
    def _migrate_messages_v2(conn: sqlite3.Connection) -> None:
        if Database._messages_has_composite_identity(conn):
            return
        conn.execute('''
            CREATE TABLE messages_v2 (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER,
                message_id TEXT, content TEXT, timestamp TIMESTAMP,
                is_sent_to_telegram BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (chat_id) REFERENCES chats (id_i),
                UNIQUE (chat_id, message_id)
            )
        ''')
        conn.execute('''
            INSERT INTO messages_v2
                (id, chat_id, message_id, content, timestamp, is_sent_to_telegram)
            SELECT id, chat_id, message_id, content, timestamp, is_sent_to_telegram
            FROM messages
        ''')
        conn.execute('DROP TABLE messages')
        conn.execute('ALTER TABLE messages_v2 RENAME TO messages')

    def init_db(self) -> None:
        """Apply versioned schema changes atomically, backing up existing files first."""
        path = Path(self.db_path)
        if path.parent and not path.parent.exists():
            raise FileNotFoundError(f"Database directory does not exist: {path.parent}")
        existed = path.exists() and path.stat().st_size > 0
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            needs_v2 = not self._messages_has_composite_identity(conn)
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"Database schema version {version} is newer than supported version {SCHEMA_VERSION}"
                )
            if existed and (version < SCHEMA_VERSION or needs_v2):
                self._backup()
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._create_schema(conn)
                self._migrate_messages_v2(conn)
                conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)')
                conn.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def save_chat(self, chat: Chat) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR REPLACE INTO chats (id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)',
                         (chat.id_i, chat.email, chat.product, chat.last_message, chat.cnt_msg, chat.cnt_new, chat.telegram_topic_id))

    def get_setting(self, key: str) -> Optional[str]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                'SELECT value FROM service_settings WHERE key = ?', (key,)
            ).fetchone()
        return row[0] if row else None

    def set_setting(self, key: str, value: str) -> None:
        self.set_settings({key: value})

    def set_settings(self, settings: dict) -> None:
        """Persist related service settings in one transaction."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executemany(
                'INSERT INTO service_settings (key, value) VALUES (?, ?) '
                'ON CONFLICT(key) DO UPDATE SET value = excluded.value, '
                'updated_at = CURRENT_TIMESTAMP',
                settings.items(),
            )

    def get_chat(self, chat_id: int) -> Optional[Chat]:
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute('SELECT id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id FROM chats WHERE id_i = ?', (chat_id,)).fetchone()
            return Chat(*row) if row else None

    def message_exists(self, message_id: str, chat_id: Optional[int] = None) -> bool:
        """Check message identity; pass chat_id for the collision-safe contract.

        Omitting chat_id retains the historical global lookup for existing callers.
        """
        with sqlite3.connect(self.db_path) as conn:
            if chat_id is None:
                row = conn.execute('SELECT 1 FROM messages WHERE message_id = ? LIMIT 1', (message_id,)).fetchone()
            else:
                row = conn.execute('SELECT 1 FROM messages WHERE chat_id = ? AND message_id = ?', (chat_id, message_id)).fetchone()
            return row is not None

    def save_message(self, message: Message) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO messages (chat_id, message_id, content, timestamp, is_sent_to_telegram) VALUES (?, ?, ?, ?, ?)',
                             (message.chat_id, message.message_id, message.content, message.timestamp, message.is_sent_to_telegram))
            return True
        except sqlite3.IntegrityError:
            return False

    def save_message_with_effects(
        self,
        message: Message,
        effects: Sequence[Tuple[str, str]],
    ) -> bool:
        """Insert a message and all of its initial effect states atomically.

        False means that the message identity already exists. Effect insertion
        failures are allowed to propagate after SQLite rolls the transaction
        back, so callers do not mistake an incomplete write for a duplicate.
        """
        with sqlite3.connect(self.db_path, isolation_level=None) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                try:
                    conn.execute(
                        "INSERT INTO messages "
                        "(chat_id, message_id, content, timestamp, is_sent_to_telegram) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (message.chat_id, message.message_id, message.content,
                         message.timestamp, message.is_sent_to_telegram),
                    )
                except sqlite3.IntegrityError:
                    duplicate = conn.execute(
                        "SELECT 1 FROM messages "
                        "WHERE chat_id = ? AND message_id = ?",
                        (message.chat_id, message.message_id),
                    ).fetchone() is not None
                    conn.rollback()
                    if duplicate:
                        return False
                    raise

                conn.executemany(
                    "INSERT INTO message_effects "
                    "(chat_id, message_id, effect, status) VALUES (?, ?, ?, ?)",
                    ((message.chat_id, message.message_id, effect, status)
                     for effect, status in effects),
                )
                conn.commit()
                return True
            except Exception:
                conn.rollback()
                raise

    def mark_message_sent(self, message_id: str, chat_id: Optional[int] = None) -> None:
        """Mark one chat message when chat_id is supplied; preserve legacy usage otherwise."""
        with sqlite3.connect(self.db_path) as conn:
            if chat_id is None:
                conn.execute('UPDATE messages SET is_sent_to_telegram = TRUE WHERE message_id = ?', (message_id,))
            else:
                conn.execute('UPDATE messages SET is_sent_to_telegram = TRUE WHERE chat_id = ? AND message_id = ?', (chat_id, message_id))

    def get_unsent_messages(self, chat_id: int) -> List[Message]:
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute('SELECT chat_id, message_id, content, timestamp, is_sent_to_telegram FROM messages WHERE chat_id = ? AND is_sent_to_telegram = FALSE ORDER BY timestamp ASC', (chat_id,)).fetchall()
            return [Message(*row) for row in rows]
