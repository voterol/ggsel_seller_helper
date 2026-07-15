import asyncio
from datetime import datetime
from database import Message

class MessageManager:
    def __init__(self, db):
        self.db = db
        self._lock = asyncio.Lock()
        # Initialize an empty dict for backward compatibility checks in bot_service.py
        self.processed_messages = {} 
    
    async def add_processed_message(self, chat_id: int, message_id: str, content: str, timestamp: datetime, sent_to_telegram: bool = False) -> bool:
        async with self._lock:
            # Use the new SQLite existence check
            if self.db.message_exists(message_id):
                return False
                
            msg = Message(chat_id=chat_id, message_id=message_id, content=content, timestamp=timestamp, is_sent_to_telegram=sent_to_telegram)
            return self.db.save_message(msg)
    
    def is_message_processed(self, chat_id: int, message_id: str) -> bool:
        """Compatibility wrapper for SQLite existence check"""
        return self.db.message_exists(message_id)
    
    def mark_message_sent(self, chat_id: int, message_id: str) -> None:
        self.db.mark_message_sent(message_id)
