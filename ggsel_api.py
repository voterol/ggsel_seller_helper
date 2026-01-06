import requests
import hashlib
import time
import json
import logging
from typing import Dict, List, Optional, Any
from config import Config
from database import Chat, Message

class GGSelAPI:
    def __init__(self, config: Config):
        self.config = config
        self.base_url = config.ggsel_base_url
        self.fallback_urls = config.ggsel_fallback_urls or [config.ggsel_base_url]
        self.current_url_index = 0
        self.token: Optional[str] = None
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'text/xml',
            'Accept': 'application/json'
        })
        # Увеличиваем таймауты для стабильности
        self.session.timeout = 30
    
    def _get_current_url(self) -> str:
        """Получить текущий URL API"""
        if self.current_url_index < len(self.fallback_urls):
            return self.fallback_urls[self.current_url_index]
        return self.fallback_urls[0]
    
    def _try_next_url(self) -> bool:
        """Переключиться на следующий URL"""
        self.current_url_index = (self.current_url_index + 1) % len(self.fallback_urls)
        self.base_url = self._get_current_url()
        logging.info(f"Переключение на резервный URL: {self.base_url}")
        return True
    
    def _make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Выполнить запрос с обработкой ошибок DNS и переключением URL"""
        max_url_attempts = len(self.fallback_urls)
        
        for url_attempt in range(max_url_attempts):
            current_base = self._get_current_url()
            full_url = url if url.startswith('http') else f"{current_base}{url}"
            
            for retry in range(3):  # 3 попытки на каждый URL
                try:
                    if method.upper() == 'GET':
                        response = self.session.get(full_url, **kwargs)
                    elif method.upper() == 'POST':
                        response = self.session.post(full_url, **kwargs)
                    else:
                        continue
                    
                    # Если запрос успешен, сбрасываем индекс URL
                    if response.status_code == 200:
                        if self.current_url_index != 0:
                            logging.info(f"🔄 Переключились обратно на основной сервер")
                            self.current_url_index = 0
                            self.base_url = self.fallback_urls[0]
                        return response
                    
                    # Если 4xx или 5xx ошибка, не переключаем URL (проблема не в сети)
                    if 400 <= response.status_code < 600:
                        return response
                        
                except (requests.exceptions.ConnectionError, 
                        requests.exceptions.Timeout,
                        requests.exceptions.DNSLookupError) as e:
                    if retry < 2:  # Не последняя попытка для этого URL
                        time.sleep(2 ** retry)  # Экспоненциальная задержка: 1s, 2s, 4s
                        continue
                    break  # Переходим к следующему URL
                except Exception as e:
                    break
            
            # Переключаемся на следующий URL
            if url_attempt < max_url_attempts - 1:
                self._try_next_url()
                logging.warning(f"🔄 Переключение на резервный сервер #{url_attempt + 2}")
        
        return None
    
    def _generate_sign(self, timestamp: str) -> str:
        """Генерация подписи"""
        data = f"{self.config.ggsel_api_key}{timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()

    def login(self) -> bool:
        """Авторизация с обработкой ошибок DNS и переключением серверов"""
        timestamp = str(int(time.time()))
        sign = self._generate_sign(timestamp)
        
        payload = {
            "seller_id": self.config.ggsel_seller_id,
            "timestamp": timestamp,
            "sign": sign
        }
        
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        
        try:
            response = self._make_request('POST', '/apilogin', headers=headers, json=payload)
            
            if response and response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'token' in data:
                    self.token = data['token']
                    return True
            
            return False
                
        except Exception as e:
            logging.error(f"Критическая ошибка авторизации: {e}")
            return False
    
    def get_chats(self, filter_new: Optional[int] = None, email: Optional[str] = None, 
                  id_ds: Optional[str] = None, pagesize: int = 100, page: int = 1) -> Optional[Dict[str, Any]]:
        """Получение чатов с улучшенной обработкой ошибок"""
        if not self.token and not self.login():
            return None
        
        params = {'token': self.token, 'pagesize': pagesize, 'page': page}
        if filter_new is not None:
            params['filter_new'] = filter_new
        if email:
            params['email'] = email
        if id_ds:
            params['id_ds'] = id_ds
        
        try:
            response = self._make_request('GET', '/debates/v2/chats', params=params)
            if response and response.status_code == 200:
                return response.json()
            
            # Если токен истёк, пробуем переавторизоваться
            if response and response.status_code == 401:
                if self.login():
                    params['token'] = self.token
                    response = self._make_request('GET', '/debates/v2/chats', params=params)
                    if response and response.status_code == 200:
                        return response.json()
            
            return None
        except Exception as e:
            logging.error(f"Ошибка получения чатов: {e}")
            return None
    
    def get_chat_messages(self, chat_id: int) -> Optional[List[Dict[str, Any]]]:
        """Получение сообщений чата с улучшенной обработкой ошибок"""
        if not self.token and not self.login():
            return None
        
        params = {'token': self.token, 'id_i': chat_id}
        
        try:
            response = self._make_request('GET', '/debates/v2', params=params)
            if response and response.status_code == 200:
                data = response.json()
                
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict) and 'messages' in data:
                    return data['messages']
                return []
            return None
        except Exception as e:
            logging.error(f"Ошибка получения сообщений чата {chat_id}: {e}")
            return None
    
    def send_message(self, chat_id: int, message: str) -> bool:
        """Отправка сообщения с улучшенной обработкой ошибок"""
        if not self.token and not self.login():
            return False
        
        params = {'token': self.token, 'id_i': chat_id}
        payload = {'message': message}
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = self._make_request('POST', '/debates/v2', params=params, json=payload, headers=headers)
            
            if response and response.status_code == 200:
                try:
                    result = response.json()
                    return result.get('retval') == 0
                except json.JSONDecodeError:
                    return True
            return False
        except Exception as e:
            logging.error(f"Ошибка отправки сообщения в чат {chat_id}: {e}")
            return False
    
    def get_last_sales(self, top: int = 10) -> Optional[Dict[str, Any]]:
        """Получение продаж с улучшенной обработкой ошибок"""
        if not self.token and not self.login():
            return None
        
        params = {'token': self.token, 'top': top}
        headers = {'Accept': 'application/json', 'locale': 'ru'}
        
        try:
            response = self._make_request('GET', '/seller-last-sales', params=params, headers=headers)
            if response and response.status_code == 200:
                return response.json()
            
            # Если токен истёк, пробуем переавторизоваться
            if response and response.status_code == 401:
                if self.login():
                    params['token'] = self.token
                    response = self._make_request('GET', '/seller-last-sales', params=params, headers=headers)
                    if response and response.status_code == 200:
                        return response.json()
            
            return None
        except Exception as e:
            logging.error(f"Ошибка получения продаж: {e}")
            return None
    
    def get_purchase_info(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        """Получение информации о покупке"""
        if not self.token and not self.login():
            return None
        
        url = f"{self.base_url}/purchase/info/{invoice_id}?token={self.token}"
        headers = {'Accept': 'application/json'}
        
        try:
            response = self.session.get(url, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('retval') == 0:
                    return data
            return None
        except requests.RequestException:
            if self.login():
                try:
                    url = f"{self.base_url}/purchase/info/{invoice_id}?token={self.token}"
                    response = self.session.get(url, headers=headers, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('retval') == 0:
                            return data
                except:
                    pass
            return None
    
    def get_chats_by_email(self, email: str, pagesize: int = 100, page: int = 1) -> Optional[Dict[str, Any]]:
        """Получение чатов по email"""
        if not self.token and not self.login():
            return None
        
        params = {'token': self.token, 'email': email, 'pagesize': pagesize, 'page': page}
        
        try:
            response = self.session.get(f"{self.base_url}/debates/v2/chats", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if self.login():
                try:
                    params['token'] = self.token
                    response = self.session.get(f"{self.base_url}/debates/v2/chats", params=params)
                    response.raise_for_status()
                    return response.json()
                except:
                    pass
            return None
    
    def parse_chats_response(self, response_data: Dict[str, Any]) -> List[Chat]:
        """Парсинг чатов"""
        chats = []
        if 'items' in response_data:
            for item in response_data['items']:
                id_i = item.get('id_i')
                if id_i is None:
                    continue
                
                chat = Chat(
                    id_i=id_i,
                    email=item.get('email') or None,
                    product=item.get('product', 0),
                    last_message=item.get('last_message', ''),
                    cnt_msg=item.get('cnt_msg', 0),
                    cnt_new=item.get('cnt_new', 0)
                )
                chats.append(chat)
        return chats
    
    def get_reviews(self, count: int = 20, review_type: str = "all", page: int = 1) -> Optional[Dict[str, Any]]:
        """Получение отзывов с улучшенной обработкой ошибок"""
        if not self.token and not self.login():
            return None
        
        params = {
            'token': self.token,
            'type': review_type,
            'page': page,
            'count': count
        }
        headers = {'Accept': 'application/json', 'locale': 'ru-RU'}
        
        try:
            response = self._make_request('GET', '/reviews', params=params, headers=headers)
            if response and response.status_code == 200:
                data = response.json()
                if data.get('retval') == 0:
                    logging.debug(f"Получено отзывов: {len(data.get('reviews', []))}")
                    return data
                else:
                    logging.warning(f"API вернул ошибку для отзывов: {data.get('retval')}")
            
            # Если токен истёк, пробуем переавторизоваться
            if response and response.status_code == 401:
                if self.login():
                    params['token'] = self.token
                    response = self._make_request('GET', '/reviews', params=params, headers=headers)
                    if response and response.status_code == 200:
                        data = response.json()
                        if data.get('retval') == 0:
                            return data
            
            return None
        except Exception as e:
            logging.error(f"Ошибка получения отзывов: {e}")
            return None
