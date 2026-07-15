import json
import logging
from typing import Dict, Optional, List
from datetime import datetime

class TopicManager:
    def __init__(self, db):
        self.db = db
        self.topics: Dict[str, Dict] = self.load_topics()
    
    def load_topics(self) -> Dict[str, Dict]:
        try:
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                # FIXED: Querying 'key', not 'topic_key'
                cursor = conn.execute("SELECT key, data FROM topics")
                return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Error loading topics from DB: {e}")
            return {}
            
    def _save_single(self, key: str, data: dict):
        self.topics[key] = data
        try:
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                conn.execute("INSERT OR REPLACE INTO topics (key, data) VALUES (?, ?)", (key, json.dumps(data)))
        except Exception as e:
            logging.error(f"Error saving topic to DB: {e}")

    def _delete_single(self, key: str) -> bool:
        if key in self.topics:
            del self.topics[key]
        try:
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                conn.execute("DELETE FROM topics WHERE key = ?", (key,))
            return True
        except: return False

    def get_all_topics(self) -> Dict[str, Dict]:
        """Fetch fresh topics from SQLite"""
        try:
            with __import__('sqlite3').connect(self.db.db_path) as conn:
                # FIXED: Querying 'key', not 'topic_key'
                cursor = conn.execute("SELECT key, data FROM topics")
                self.topics = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Error getting all topics: {e}")
        return self.topics.copy()

    def add_topic(self, chat_id: int, email: Optional[str], topic_id: int, topic_name: str) -> None:
        key = str(chat_id)
        data = {
            "chat_id": chat_id, "email": email, "topic_id": topic_id, 
            "topic_name": topic_name, "created_at": datetime.now().isoformat()
        }
        self._save_single(key, data)
    
    def get_topic_id(self, chat_id: int) -> Optional[int]:
        return self.topics.get(str(chat_id), {}).get("topic_id")
    
    def topic_exists(self, chat_id: int) -> bool:
        return str(chat_id) in self.topics
    
    def add_topic_for_purchase(self, purchase, topic_id: int, topic_name: str, chat_ids: List[int] = None) -> None:
        key = f"purchase_{purchase.invoice_id}"
        customer_id = purchase.buyer_email or purchase.buyer_account or f"customer_{purchase.invoice_id}"
        data = {
            "type": "purchase", "invoice_id": purchase.invoice_id, "customer_id": customer_id,
            "email": purchase.buyer_email, "account": purchase.buyer_account, "topic_id": topic_id,
            "topic_name": topic_name, "chat_ids": chat_ids or [], "created_at": datetime.now().isoformat()
        }
        self._save_single(key, data)
    
    def get_topic_by_email(self, customer_id: str) -> Optional[Dict]:
        for topic_data in self.topics.values():
            if topic_data.get('type') == 'purchase':
                if topic_data.get('email') == customer_id or topic_data.get('account') == customer_id or topic_data.get('customer_id') == customer_id:
                    return topic_data
        return None
    
    def update_topic_chat_ids(self, topic_key: str, chat_ids: List[int]) -> None:
        if topic_key in self.topics:
            self.topics[topic_key]['chat_ids'] = chat_ids
            self._save_single(topic_key, self.topics[topic_key])
    
    def remove_topic(self, topic_key: str) -> bool:
        return self._delete_single(topic_key)