import json
import os
from typing import Dict, List, Optional
from datetime import datetime

class MessageManager:
    def __init__(self, messages_file: str = "processed_messages.json"):
        self.messages_file = messages_file
        self.processed_messages: Dict[str, Dict] = self.load_messages()
    
    def load_messages(self) -> Dict[str, Dict]:
        """Загрузка проверенных сообщений из JSON файла"""
        if os.path.exists(self.messages_file):
            try:
                with open(self.messages_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Ошибка загрузки сообщений: {e}")
        return {}
    
    def save_messages(self) -> None:
        """Сохранение проверенных сообщений в JSON файл"""
        try:
            with open(self.messages_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_messages, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка сохранения сообщений: {e}")
    
    def add_processed_message(self, chat_id: int, message_id: str, content: str, 
                            timestamp: datetime, sent_to_telegram: bool = False) -> bool:
        """Добавление проверенного сообщения"""
        try:
            key = f"{chat_id}_{message_id}"
            
            # Проверяем, не обработано ли уже это сообщение
            if key in self.processed_messages:
                return False
            
            self.processed_messages[key] = {
                "chat_id": chat_id,
                "message_id": message_id,
                "content": content,
                "timestamp": timestamp.isoformat(),
                "sent_to_telegram": sent_to_telegram,
                "processed_at": datetime.now().isoformat()
            }
            
            self.save_messages()
            return True
            
        except Exception as e:
            print(f"Ошибка добавления сообщения: {e}")
            return False
    
    def is_message_processed(self, chat_id: int, message_id: str) -> bool:
        """Проверка, обработано ли сообщение"""
        key = f"{chat_id}_{message_id}"
        return key in self.processed_messages
    
    def mark_message_sent(self, chat_id: int, message_id: str) -> None:
        """Отметить сообщение как отправленное в Telegram"""
        key = f"{chat_id}_{message_id}"
        if key in self.processed_messages:
            self.processed_messages[key]["sent_to_telegram"] = True
            self.processed_messages[key]["sent_at"] = datetime.now().isoformat()
            self.save_messages()
    
    def get_unsent_messages(self, chat_id: int) -> List[Dict]:
        """Получение неотправленных сообщений для чата"""
        unsent = []
        for key, msg_data in self.processed_messages.items():
            if (msg_data.get("chat_id") == chat_id and 
                not msg_data.get("sent_to_telegram", False)):
                unsent.append(msg_data)
        return unsent
    
    def get_chat_messages_count(self, chat_id: int) -> int:
        """Получение количества сообщений в чате"""
        count = 0
        for msg_data in self.processed_messages.values():
            if msg_data.get("chat_id") == chat_id:
                count += 1
        return count
    
    def cleanup_old_messages(self, days: int = 30) -> int:
        """Очистка старых сообщений (старше указанного количества дней)"""
        try:
            cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
            keys_to_remove = []
            
            for key, msg_data in self.processed_messages.items():
                try:
                    processed_at = datetime.fromisoformat(msg_data.get("processed_at", ""))
                    if processed_at.timestamp() < cutoff_date:
                        keys_to_remove.append(key)
                except:
                    continue
            
            for key in keys_to_remove:
                del self.processed_messages[key]
            
            if keys_to_remove:
                self.save_messages()
            
            return len(keys_to_remove)
            
        except Exception as e:
            print(f"Ошибка очистки старых сообщений: {e}")
            return 0