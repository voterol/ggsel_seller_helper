import sqlite3
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass

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
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        """Initialize the core database structure for Single-user mode"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    id_i INTEGER PRIMARY KEY, email TEXT, product INTEGER,
                    last_message TEXT, cnt_msg INTEGER, cnt_new INTEGER,
                    telegram_topic_id INTEGER, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, message_id TEXT UNIQUE,
                    content TEXT, timestamp TIMESTAMP, is_sent_to_telegram BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (chat_id) REFERENCES chats (id_i)
                )
            ''')
            
            # Tables required for the JSON-to-SQLite migration and optimized lookups
            conn.execute('CREATE TABLE IF NOT EXISTS topics (key TEXT PRIMARY KEY, data TEXT)')
            conn.execute('CREATE TABLE IF NOT EXISTS purchases (invoice_id TEXT PRIMARY KEY, data TEXT)')
            conn.execute('CREATE TABLE IF NOT EXISTS processed_reviews (review_id TEXT PRIMARY KEY, hash TEXT)')
            conn.execute('CREATE TABLE IF NOT EXISTS pending_topics (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)')
            
            conn.execute('CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)')
            
    def save_chat(self, chat: Chat) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('INSERT OR REPLACE INTO chats (id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)', 
                        (chat.id_i, chat.email, chat.product, chat.last_message, chat.cnt_msg, chat.cnt_new, chat.telegram_topic_id))
    
    def get_chat(self, chat_id: int) -> Optional[Chat]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id FROM chats WHERE id_i = ?', (chat_id,))
            row = cursor.fetchone()
            return Chat(*row) if row else None

    def message_exists(self, message_id: str) -> bool:
        """Check if a message already exists in the SQLite database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT 1 FROM messages WHERE message_id = ?', (message_id,))
            return cursor.fetchone() is not None

    def save_message(self, message: Message) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('INSERT INTO messages (chat_id, message_id, content, timestamp, is_sent_to_telegram) VALUES (?, ?, ?, ?, ?)',
                            (message.chat_id, message.message_id, message.content, message.timestamp, message.is_sent_to_telegram))
                return True
        except: return False

    def mark_message_sent(self, message_id: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('UPDATE messages SET is_sent_to_telegram = TRUE WHERE message_id = ?', (message_id,))
            
    def get_unsent_messages(self, chat_id: int) -> List[Message]:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT chat_id, message_id, content, timestamp, is_sent_to_telegram FROM messages WHERE chat_id = ? AND is_sent_to_telegram = FALSE ORDER BY timestamp ASC', (chat_id,))
            return [Message(*row) for row in cursor.fetchall()]
