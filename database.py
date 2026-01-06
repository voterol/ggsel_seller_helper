import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

@dataclass
class Chat:
    id_i: int
    email: Optional[str]  # Может быть None
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
        """Инициализация базы данных"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS chats (
                    id_i INTEGER PRIMARY KEY,
                    email TEXT,
                    product INTEGER,
                    last_message TEXT,
                    cnt_msg INTEGER,
                    cnt_new INTEGER,
                    telegram_topic_id INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            conn.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER,
                    message_id TEXT UNIQUE,
                    content TEXT,
                    timestamp TIMESTAMP,
                    is_sent_to_telegram BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (chat_id) REFERENCES chats (id_i)
                )
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id)
            ''')
            
            conn.execute('''
                CREATE INDEX IF NOT EXISTS idx_messages_sent ON messages(is_sent_to_telegram)
            ''')
    
    def save_chat(self, chat: Chat) -> None:
        """Сохранение или обновление чата"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO chats 
                (id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ''', (chat.id_i, chat.email, chat.product, chat.last_message, 
                  chat.cnt_msg, chat.cnt_new, chat.telegram_topic_id))
    
    def get_chat(self, chat_id: int) -> Optional[Chat]:
        """Получение чата по ID"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id
                FROM chats WHERE id_i = ?
            ''', (chat_id,))
            row = cursor.fetchone()
            if row:
                return Chat(*row)
        return None
    
    def get_all_chats(self) -> List[Chat]:
        """Получение всех чатов"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT id_i, email, product, last_message, cnt_msg, cnt_new, telegram_topic_id
                FROM chats
            ''')
            return [Chat(*row) for row in cursor.fetchall()]
    
    def save_message(self, message: Message) -> bool:
        """Сохранение сообщения. Возвращает True если сообщение новое"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT INTO messages 
                    (chat_id, message_id, content, timestamp, is_sent_to_telegram)
                    VALUES (?, ?, ?, ?, ?)
                ''', (message.chat_id, message.message_id, message.content, 
                      message.timestamp, message.is_sent_to_telegram))
                return True
        except sqlite3.IntegrityError:
            # Сообщение уже существует
            return False
    
    def mark_message_sent(self, message_id: str) -> None:
        """Отметить сообщение как отправленное в Telegram"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE messages SET is_sent_to_telegram = TRUE 
                WHERE message_id = ?
            ''', (message_id,))
    
    def get_unsent_messages(self, chat_id: int) -> List[Message]:
        """Получение неотправленных сообщений для чата"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('''
                SELECT chat_id, message_id, content, timestamp, is_sent_to_telegram
                FROM messages 
                WHERE chat_id = ? AND is_sent_to_telegram = FALSE
                ORDER BY timestamp ASC
            ''', (chat_id,))
            return [Message(*row) for row in cursor.fetchall()]
    
    def update_telegram_topic_id(self, chat_id: int, topic_id: int) -> None:
        """Обновление ID топика Telegram для чата"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE chats SET telegram_topic_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id_i = ?
            ''', (topic_id, chat_id))