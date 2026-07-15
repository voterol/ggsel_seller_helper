import asyncio
import sqlite3
from datetime import datetime
from database import Message

class MessageManager:
    PENDING = "pending"
    COMPLETED = "completed"
    PERMANENT_FAILURE = "permanent_failure"

    def __init__(self, db):
        self.db = db
        self._lock = asyncio.Lock()
        # Initialize an empty dict for backward compatibility checks in bot_service.py
        self.processed_messages = {} 
    
    async def add_processed_message(self, chat_id: int, message_id: str, content: str, timestamp: datetime, sent_to_telegram: bool = False) -> bool:
        async with self._lock:
            # Message ids are only unique within a chat.  An unsent row is a
            # durable delivery attempt, not a successfully processed message.
            if self.db.message_exists(message_id, chat_id=chat_id):
                # A prior failed attempt must remain retryable.  True means
                # the caller owns delivery work, not that a new row was made.
                return not self.is_message_processed(chat_id, message_id)
                
            msg = Message(chat_id=chat_id, message_id=message_id, content=content, timestamp=timestamp, is_sent_to_telegram=sent_to_telegram)
            effects = (
                ("telegram", self.COMPLETED if sent_to_telegram else self.PENDING),
                ("autoresponder", self.PENDING),
                ("autoresponder_mirror", self.COMPLETED),
                ("group_notification", self.COMPLETED),
                ("autoresponder_plan", self.PENDING),
            )
            if not self.db.save_message_with_effects(msg, effects):
                return False
            return True
    
    def is_message_processed(self, chat_id: int, message_id: str) -> bool:
        """Return True when both independently tracked effects are terminal."""
        telegram = self.get_effect_status(chat_id, message_id, "telegram")
        autoresponder = self.get_effect_status(chat_id, message_id, "autoresponder")
        group_notification = self.get_effect_status(chat_id, message_id, "group_notification")
        autoresponder_mirror = self.get_effect_status(chat_id, message_id, "autoresponder_mirror")
        return (telegram in (self.COMPLETED, self.PERMANENT_FAILURE)
                and autoresponder in (self.COMPLETED, self.PERMANENT_FAILURE)
                and autoresponder_mirror in (self.COMPLETED, self.PERMANENT_FAILURE)
                and group_notification in (self.COMPLETED, self.PERMANENT_FAILURE))

    def is_message_delivered(self, chat_id: int, message_id: str) -> bool:
        """Return whether this exact chat message reached Telegram."""
        with sqlite3.connect(self.db.db_path) as conn:
            row = conn.execute(
                "SELECT is_sent_to_telegram FROM messages "
                "WHERE chat_id = ? AND message_id = ?",
                (chat_id, message_id),
            ).fetchone()
        return bool(row and row[0])
    
    def mark_message_sent(self, chat_id: int, message_id: str) -> None:
        self.db.mark_message_sent(message_id, chat_id=chat_id)
        self.set_effect_status(chat_id, message_id, "telegram", self.COMPLETED)

    def get_effect_status(self, chat_id: int, message_id: str, effect: str) -> str:
        with sqlite3.connect(self.db.db_path) as conn:
            row = conn.execute(
                "SELECT status FROM message_effects WHERE chat_id = ? AND message_id = ? AND effect = ?",
                (chat_id, message_id, effect),
            ).fetchone()
        if row:
            return row[0]
        # Compatibility for rows created before effect tracking.
        if effect == "telegram" and self.is_message_delivered(chat_id, message_id):
            return self.COMPLETED
        if effect in ("autoresponder", "autoresponder_mirror", "group_notification", "autoresponder_plan") and self.db.message_exists(message_id, chat_id=chat_id):
            # Legacy rows predate independent effect state and were previously
            # considered fully processed. Do not replay external effects.
            return self.COMPLETED
        return self.PENDING

    def set_effect_status(self, chat_id: int, message_id: str, effect: str, status: str) -> None:
        if status not in (self.PENDING, self.COMPLETED, self.PERMANENT_FAILURE):
            raise ValueError(f"Invalid message effect status: {status}")
        with sqlite3.connect(self.db.db_path) as conn:
            conn.execute(
                "INSERT INTO message_effects (chat_id, message_id, effect, status) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(chat_id, message_id, effect) DO UPDATE SET status = excluded.status, updated_at = CURRENT_TIMESTAMP",
                (chat_id, message_id, effect, status),
            )
