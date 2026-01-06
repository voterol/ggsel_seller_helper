import json
import os
import logging
from typing import Dict, Optional, List
from datetime import datetime

class TopicManager:
    def __init__(self, topics_file: str = "topics.json"):
        self.topics_file = topics_file
        self.topics: Dict[str, Dict] = self.load_topics()
    
    def load_topics(self) -> Dict[str, Dict]:
        """Загрузка топиков из JSON файла"""
        if os.path.exists(self.topics_file):
            try:
                with open(self.topics_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Ошибка загрузки топиков: {e}")
        return {}
    
    def save_topics(self) -> None:
        """Сохранение топиков в JSON файл"""
        try:
            with open(self.topics_file, 'w', encoding='utf-8') as f:
                json.dump(self.topics, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка сохранения топиков: {e}")
    
    def add_topic(self, chat_id: int, email: Optional[str], topic_id: int, topic_name: str) -> None:
        """Добавление нового топика"""
        key = str(chat_id)
        self.topics[key] = {
            "chat_id": chat_id,
            "email": email,
            "topic_id": topic_id,
            "topic_name": topic_name,
            "created_at": datetime.now().isoformat()
        }
        self.save_topics()
    
    def get_topic_id(self, chat_id: int) -> Optional[int]:
        """Получение ID топика по ID чата"""
        key = str(chat_id)
        if key in self.topics:
            return self.topics[key].get("topic_id")
        return None
    
    def topic_exists(self, chat_id: int) -> bool:
        """Проверка существования топика"""
        return str(chat_id) in self.topics
    
    def add_topic_for_purchase(self, purchase, topic_id: int, topic_name: str, chat_ids: List[int] = None) -> None:
        """Добавление топика для покупки"""
        # Используем invoice_id как основной ключ для простоты
        key = f"purchase_{purchase.invoice_id}"
        
        # Определяем customer_id для поиска
        customer_id = purchase.buyer_email or purchase.buyer_account or f"customer_{purchase.invoice_id}"
        
        self.topics[key] = {
            "type": "purchase",
            "invoice_id": purchase.invoice_id,
            "customer_id": customer_id,
            "email": purchase.buyer_email,
            "account": purchase.buyer_account,
            "topic_id": topic_id,
            "topic_name": topic_name,
            "chat_ids": chat_ids or [],  # Список ID чатов для этого покупателя
            "created_at": datetime.now().isoformat()
        }
        self.save_topics()
    
    def topic_exists_by_email(self, customer_id: str) -> bool:
        """Проверка существования топика по email/account покупателя"""
        # Ищем по customer_id в существующих топиках
        for topic_data in self.topics.values():
            if topic_data.get('type') == 'purchase':
                if (topic_data.get('email') == customer_id or 
                    topic_data.get('account') == customer_id or
                    topic_data.get('customer_id') == customer_id):
                    return True
        return False
    
    def get_topic_by_email(self, customer_id: str) -> Optional[Dict]:
        """Получение топика по email/account покупателя"""
        # Ищем по customer_id в существующих топиках
        for topic_data in self.topics.values():
            if topic_data.get('type') == 'purchase':
                if (topic_data.get('email') == customer_id or 
                    topic_data.get('account') == customer_id or
                    topic_data.get('customer_id') == customer_id):
                    return topic_data
        return None
    
    def get_all_topics(self) -> Dict[str, Dict]:
        """Получение всех топиков"""
        return self.topics.copy()
    
    def update_topic_chat_ids(self, topic_key: str, chat_ids: List[int]) -> None:
        """Обновление списка chat_ids для топика"""
        if topic_key in self.topics:
            self.topics[topic_key]['chat_ids'] = chat_ids
            self.save_topics()
            logging.info(f"Обновлены chat_ids для топика {topic_key}: {chat_ids}")
        else:
            logging.warning(f"Топик {topic_key} не найден для обновления chat_ids")
    
    def update_topic_search_time(self, topic_key: str, search_time: str) -> None:
        """Обновление времени последнего поиска чатов для топика"""
        if topic_key in self.topics:
            self.topics[topic_key]['last_chat_search'] = search_time
            self.save_topics()
            logging.debug(f"Обновлено время поиска для топика {topic_key}")
        else:
            logging.warning(f"Топик {topic_key} не найден для обновления времени поиска")