"""
Модуль автоответов для GGSel бота.

Поддерживает:
- Автоматические приветствия новых покупателей
- Триггеры на ключевые слова в сообщениях
- Автоответы на отзывы (хорошие/плохие)
- Режим ЧСВ - реакция на опции покупки с поддержкой условий по значению
"""

import json
import os
import logging
import copy
import tempfile
from typing import Dict, List, Optional


class AutoResponder:
    """Менеджер автоответов и триггеров."""
    
    DEFAULT_CONFIG = {
        "enabled": True,
        "first_message_enabled": True,
        "first_message_text": "Здравствуйте! Спасибо за покупку. Чем могу помочь?",
        "notify_text": "🔔 Требуется ответ!",
        "triggers": [],
        "review_responses": {
            "enabled": False,
            "good_enabled": False,
            "good_text": "Спасибо за отзыв! 🙏",
            "bad_enabled": False,
            "bad_text": "Извините за неудобства. Напишите, чем можем помочь?"
        },
        "csv_mode": {
            "enabled": False,
            "rules": []
        }
    }
    MAX_TEXT_LENGTH = 4000
    MAX_MATCH_LENGTH = 500
    TRIGGER_FIELDS = frozenset({
        "phrase", "response", "enabled", "notify_group", "notify_text", "exact_match"
    })
    CSV_RULE_FIELDS = frozenset({
        "option_name", "option_value", "match_type", "case_sensitive", "enabled",
        "send_to_user", "user_message", "send_to_topic", "topic_message"
    })
    
    def __init__(self, config_file: str = None):
        if config_file is None:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            config_file = os.path.join(base_dir, "autoresponder.json")
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        """Загрузка конфигурации с мержем дефолтных значений."""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not isinstance(data, dict):
                        raise ValueError("top-level configuration must be an object")
                    return self._merge_config(data, self.DEFAULT_CONFIG)
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                # Do not log the path or raw config: it may contain customer text.
                logging.error("Не удалось загрузить конфигурацию автоответов")
        return copy.deepcopy(self.DEFAULT_CONFIG)
    
    def _merge_config(self, data: Dict, default: Dict) -> Dict:
        """Рекурсивный мерж конфига с дефолтными значениями."""
        if not isinstance(data, dict):
            return copy.deepcopy(default)
        result = copy.deepcopy(default)
        for key, value in data.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_config(value, result[key])
            else:
                result[key] = value
        return result
    
    def save_config(self):
        """Сохранение конфигурации в файл."""
        temp_name = None
        try:
            parent = os.path.dirname(os.path.abspath(self.config_file))
            with tempfile.NamedTemporaryFile(
                'w', encoding='utf-8', dir=parent, delete=False
            ) as f:
                temp_name = f.name
                json.dump(self.config, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())
            os.chmod(temp_name, 0o600)
            os.replace(temp_name, self.config_file)
        except (OSError, TypeError, ValueError):
            logging.error("Не удалось сохранить конфигурацию автоответов")
            if temp_name:
                try:
                    os.unlink(temp_name)
                except OSError:
                    pass

    @classmethod
    def _text(cls, value, max_length: int = None) -> str:
        """Normalize untrusted text before matching, persistence, or delivery."""
        if not isinstance(value, str):
            return ""
        limit = max_length or cls.MAX_TEXT_LENGTH
        return "".join(ch for ch in value if ch in "\n\r\t" or ord(ch) >= 32)[:limit]
    
    # ==================== Основные настройки ====================
    
    def is_enabled(self) -> bool:
        return self.config.get("enabled", True)
    
    def toggle_enabled(self) -> bool:
        self.config["enabled"] = not self.is_enabled()
        self.save_config()
        return self.config["enabled"]
    
    def is_first_message_enabled(self) -> bool:
        return self.config.get("first_message_enabled", True)
    
    def toggle_first_message(self) -> bool:
        self.config["first_message_enabled"] = not self.is_first_message_enabled()
        self.save_config()
        return self.config["first_message_enabled"]
    
    def get_first_message_text(self) -> str:
        return self.config.get("first_message_text", "")
    
    def set_first_message_text(self, text: str):
        self.config["first_message_text"] = self._text(text)
        self.save_config()
    
    def get_notify_text(self) -> str:
        return self.config.get("notify_text", "🔔 Требуется ответ!")
    
    def set_notify_text(self, text: str):
        self.config["notify_text"] = self._text(text)
        self.save_config()
    
    def should_send_first_message(self) -> bool:
        return self.is_enabled() and self.is_first_message_enabled()
    
    # ==================== Триггеры ====================
    
    def get_triggers(self) -> List[Dict]:
        triggers = self.config.get("triggers", [])
        return triggers if isinstance(triggers, list) else []
    
    def add_trigger(self, phrase: str, response: str, notify_group: bool = False, 
                    notify_text: str = "", exact_match: bool = False) -> int:
        """Добавить триггер. Возвращает индекс."""
        triggers = self.config.setdefault("triggers", [])
        triggers.append({
            "phrase": self._text(phrase, self.MAX_MATCH_LENGTH).lower(),
            "response": self._text(response),
            "enabled": True,
            "notify_group": notify_group,
            "notify_text": self._text(notify_text),
            "exact_match": exact_match
        })
        self.save_config()
        return len(triggers) - 1
    
    def remove_trigger(self, index: int) -> bool:
        triggers = self.get_triggers()
        if 0 <= index < len(triggers):
            triggers.pop(index)
            self.save_config()
            return True
        return False
    
    def get_trigger(self, index: int) -> Optional[Dict]:
        triggers = self.get_triggers()
        return triggers[index] if 0 <= index < len(triggers) else None
    
    def update_trigger(self, index: int, **kwargs) -> bool:
        triggers = self.get_triggers()
        if 0 <= index < len(triggers):
            updates = {key: value for key, value in kwargs.items() if key in self.TRIGGER_FIELDS}
            if "phrase" in updates:
                updates["phrase"] = self._text(updates["phrase"], self.MAX_MATCH_LENGTH).lower()
            for key in ("response", "notify_text"):
                if key in updates:
                    updates[key] = self._text(updates[key])
            triggers[index].update(updates)
            self.save_config()
            return True
        return False
    
    def toggle_trigger(self, index: int) -> Optional[bool]:
        trigger = self.get_trigger(index)
        if trigger:
            trigger["enabled"] = not trigger.get("enabled", True)
            self.save_config()
            return trigger["enabled"]
        return None
    
    def toggle_trigger_notify(self, index: int) -> Optional[bool]:
        trigger = self.get_trigger(index)
        if trigger:
            trigger["notify_group"] = not trigger.get("notify_group", False)
            self.save_config()
            return trigger["notify_group"]
        return None
    
    def toggle_trigger_exact_match(self, index: int) -> Optional[bool]:
        trigger = self.get_trigger(index)
        if trigger:
            trigger["exact_match"] = not trigger.get("exact_match", False)
            self.save_config()
            return trigger["exact_match"]
        return None
    
    def find_response(self, message: str) -> Optional[Dict]:
        """Найти автоответ для сообщения."""
        if not self.is_enabled():
            return None
        
        message_lower = self._text(message).lower().strip()
        
        for trigger in self.get_triggers():
            if not isinstance(trigger, dict):
                continue
            if not trigger.get("enabled", True):
                continue
            
            phrase = trigger.get("phrase", "").lower()
            if not phrase:
                continue
            
            exact_match = trigger.get("exact_match", False)
            matched = (message_lower == phrase) if exact_match else (phrase in message_lower)
            
            if matched:
                return {
                    "response": trigger.get("response"),
                    "notify_group": trigger.get("notify_group", False),
                    "notify_text": trigger.get("notify_text", "")
                }
        
        return None
    
    # ==================== Автоответы на отзывы ====================
    
    def _get_review_config(self) -> Dict:
        return self.config.setdefault("review_responses", {})
    
    def is_review_responses_enabled(self) -> bool:
        return self._get_review_config().get("enabled", False)
    
    def toggle_review_responses(self) -> bool:
        config = self._get_review_config()
        config["enabled"] = not config.get("enabled", False)
        self.save_config()
        return config["enabled"]
    
    def is_good_review_response_enabled(self) -> bool:
        return self._get_review_config().get("good_enabled", False)
    
    def toggle_good_review_response(self) -> bool:
        config = self._get_review_config()
        config["good_enabled"] = not config.get("good_enabled", False)
        self.save_config()
        return config["good_enabled"]
    
    def get_good_review_text(self) -> str:
        return self._get_review_config().get("good_text", "Спасибо за отзыв! 🙏")
    
    def set_good_review_text(self, text: str):
        self._get_review_config()["good_text"] = self._text(text)
        self.save_config()
    
    def is_bad_review_response_enabled(self) -> bool:
        return self._get_review_config().get("bad_enabled", False)
    
    def toggle_bad_review_response(self) -> bool:
        config = self._get_review_config()
        config["bad_enabled"] = not config.get("bad_enabled", False)
        self.save_config()
        return config["bad_enabled"]
    
    def get_bad_review_text(self) -> str:
        return self._get_review_config().get("bad_text", "Извините за неудобства.")
    
    def set_bad_review_text(self, text: str):
        self._get_review_config()["bad_text"] = self._text(text)
        self.save_config()
    
    def get_review_response(self, review_type: str) -> Optional[str]:
        """Получить текст ответа на отзыв (если включено)."""
        if not self.is_review_responses_enabled():
            return None
        
        if review_type == "good" and self.is_good_review_response_enabled():
            return self.get_good_review_text()
        elif review_type == "bad" and self.is_bad_review_response_enabled():
            return self.get_bad_review_text()
        
        return None

    # ==================== Режим ЧСВ (реакция на опции покупки) ====================
    
    def _get_csv_config(self) -> Dict:
        return self.config.setdefault("csv_mode", {"enabled": False, "rules": []})
    
    def is_csv_mode_enabled(self) -> bool:
        return self._get_csv_config().get("enabled", False)
    
    def toggle_csv_mode(self) -> bool:
        config = self._get_csv_config()
        config["enabled"] = not config.get("enabled", False)
        self.save_config()
        return config["enabled"]
    
    def get_csv_rules(self) -> List[Dict]:
        config = self._get_csv_config()
        rules = config.setdefault("rules", [])
        if not isinstance(rules, list):
            config["rules"] = []
            return config["rules"]
        return rules
    
    def add_csv_rule(self, option_name: str, option_value: str = "", 
                     match_type: str = "name", case_sensitive: bool = False,
                     send_to_user: bool = False, user_message: str = "",
                     send_to_topic: bool = True, topic_message: str = "") -> int:
        """
        Добавить правило ЧСВ.
        
        Args:
            option_name: Название опции для сопоставления
            option_value: Значение опции (user_data) для сопоставления (опционально)
            match_type: Тип сопоставления:
                - "name" - только по названию опции
                - "value" - по названию И значению
                - "contains" - значение содержит подстроку
            case_sensitive: Учитывать регистр
            send_to_user: Отправлять сообщение покупателю
            user_message: Текст сообщения покупателю
            send_to_topic: Отправлять сообщение в топик
            topic_message: Текст сообщения в топик
        
        Returns:
            Индекс добавленного правила
        """
        # Валидация match_type
        if match_type not in ("name", "value", "contains"):
            match_type = "name"
        
        rules = self.get_csv_rules()
        rules.append({
            "option_name": self._text(option_name, self.MAX_MATCH_LENGTH),
            "option_value": self._text(option_value, self.MAX_MATCH_LENGTH),
            "match_type": match_type,
            "case_sensitive": case_sensitive,
            "enabled": True,
            "send_to_user": send_to_user,
            "user_message": self._text(user_message),
            "send_to_topic": send_to_topic,
            "topic_message": self._text(topic_message)
        })
        self.save_config()
        return len(rules) - 1
    
    def remove_csv_rule(self, index: int) -> bool:
        rules = self.get_csv_rules()
        if 0 <= index < len(rules):
            rules.pop(index)
            self.save_config()
            return True
        return False
    
    def get_csv_rule(self, index: int) -> Optional[Dict]:
        rules = self.get_csv_rules()
        return rules[index] if 0 <= index < len(rules) else None
    
    def update_csv_rule(self, index: int, **kwargs) -> bool:
        rule = self.get_csv_rule(index)
        if rule:
            updates = {key: value for key, value in kwargs.items() if key in self.CSV_RULE_FIELDS}
            if updates.get("match_type") not in (None, "name", "value", "contains"):
                updates["match_type"] = "name"
            for key in ("option_name", "option_value"):
                if key in updates:
                    updates[key] = self._text(updates[key], self.MAX_MATCH_LENGTH)
            for key in ("user_message", "topic_message"):
                if key in updates:
                    updates[key] = self._text(updates[key])
            rule.update(updates)
            self.save_config()
            return True
        return False
    
    def toggle_csv_rule(self, index: int) -> Optional[bool]:
        rule = self.get_csv_rule(index)
        if rule:
            rule["enabled"] = not rule.get("enabled", True)
            self.save_config()
            return rule["enabled"]
        return None
    
    def toggle_csv_rule_case_sensitive(self, index: int) -> Optional[bool]:
        rule = self.get_csv_rule(index)
        if rule:
            rule["case_sensitive"] = not rule.get("case_sensitive", False)
            self.save_config()
            return rule["case_sensitive"]
        return None
    
    def toggle_csv_rule_send_to_user(self, index: int) -> Optional[bool]:
        rule = self.get_csv_rule(index)
        if rule:
            rule["send_to_user"] = not rule.get("send_to_user", False)
            self.save_config()
            return rule["send_to_user"]
        return None
    
    def toggle_csv_rule_send_to_topic(self, index: int) -> Optional[bool]:
        rule = self.get_csv_rule(index)
        if rule:
            rule["send_to_topic"] = not rule.get("send_to_topic", True)
            self.save_config()
            return rule["send_to_topic"]
        return None
    
    def cycle_csv_rule_match_type(self, index: int) -> Optional[str]:
        """Переключить тип сопоставления: name -> value -> contains -> name."""
        rule = self.get_csv_rule(index)
        if rule:
            current = rule.get("match_type", "name")
            next_type = {"name": "value", "value": "contains", "contains": "name"}
            rule["match_type"] = next_type.get(current, "name")
            self.save_config()
            return rule["match_type"]
        return None
    
    def _match_string(self, pattern: str, value: str, case_sensitive: bool) -> bool:
        """Сравнение строк с учётом регистра."""
        if case_sensitive:
            return pattern == value
        return pattern.lower() == value.lower()
    
    def _match_contains(self, pattern: str, value: str, case_sensitive: bool) -> bool:
        """Проверка вхождения подстроки."""
        if case_sensitive:
            return pattern in value
        return pattern.lower() in value.lower()
    
    def check_csv_options(self, options: List[Dict]) -> List[Dict]:
        """
        Проверить опции покупки на совпадение с правилами ЧСВ.
        
        Args:
            options: Список опций вида [{"name": "...", "user_data": "..."}, ...]
        
        Returns:
            Список сработавших правил с данными для отправки
        """
        if not self.is_csv_mode_enabled() or not isinstance(options, list):
            return []
        
        results = []
        
        for option in options:
            if not isinstance(option, dict):
                continue
            option_name = self._text(option.get("name", ""), self.MAX_MATCH_LENGTH)
            option_value = self._text(option.get("user_data", ""), self.MAX_TEXT_LENGTH)
            
            if not option_name:
                continue
            
            for rule in self.get_csv_rules():
                if not isinstance(rule, dict):
                    continue
                if not rule.get("enabled", True):
                    continue
                
                rule_name = rule.get("option_name", "")
                if not rule_name:
                    continue
                
                case_sensitive = rule.get("case_sensitive", False)
                match_type = rule.get("match_type", "name")
                
                # Проверяем совпадение названия опции
                if not self._match_string(rule_name, option_name, case_sensitive):
                    continue
                
                # Дополнительная проверка по значению
                if match_type == "value":
                    rule_value = rule.get("option_value", "")
                    if not self._match_string(rule_value, option_value, case_sensitive):
                        continue
                elif match_type == "contains":
                    rule_value = rule.get("option_value", "")
                    if rule_value and not self._match_contains(rule_value, option_value, case_sensitive):
                        continue
                
                results.append({
                    "rule": rule,
                    "option": option,
                    "option_name": option_name,
                    "option_value": option_value,
                    "send_to_user": rule.get("send_to_user", False),
                    "user_message": rule.get("user_message", ""),
                    "send_to_topic": rule.get("send_to_topic", True),
                    "topic_message": rule.get("topic_message", "")
                })
        
        return results
