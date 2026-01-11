import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
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
        self.token: Optional[str] = None
        self.session = requests.Session()
        
        # Увеличиваем пул соединений для параллельных запросов
        adapter = HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=Retry(total=2, backoff_factor=0.5)
        )
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        
        self.session.headers.update({
            'Content-Type': 'text/xml',
            'Accept': 'application/json'
        })
        self._last_auth_error_time = None  # Для ограничения спама ошибок
    
    def _generate_sign(self, timestamp: str) -> str:
        """Генерация подписи"""
        data = f"{self.config.ggsel_api_key}{timestamp}"
        return hashlib.sha256(data.encode()).hexdigest()

    def login(self) -> bool:
        """Авторизация"""
        timestamp = str(int(time.time()))
        sign = self._generate_sign(timestamp)
        
        payload = {
            "seller_id": self.config.ggsel_seller_id,
            "timestamp": timestamp,
            "sign": sign
        }
        
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        
        try:
            response = requests.post(f"{self.base_url}/apilogin", headers=headers, json=payload)
            
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict) and 'token' in data:
                    self.token = data['token']
                    return True
            return False
                
        except Exception as e:
            logging.error(f"Ошибка авторизации: {e}")
            return False
    
    def get_chats(self, filter_new: Optional[int] = None, email: Optional[str] = None, 
                  id_ds: Optional[str] = None, pagesize: int = 100, page: int = 1) -> Optional[Dict[str, Any]]:
        """Получение чатов"""
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
            response = self.session.get(f"{self.base_url}/debates/v2/chats", params=params)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            if self.login():
                try:
                    params['token'] = self.token
                    response = self.session.get(f"{self.base_url}/debates/v2/chats", params=params)
                    response.raise_for_status()
                    return response.json()
                except:
                    pass
            return None
    
    def get_chat_messages(self, chat_id: int) -> Optional[List[Dict[str, Any]]]:
        """Получение сообщений чата"""
        if not self.token and not self.login():
            return None
        
        params = {'token': self.token, 'id_i': chat_id}
        
        try:
            response = self.session.get(f"{self.base_url}/debates/v2", params=params)
            response.raise_for_status()
            data = response.json()
            
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'messages' in data:
                return data['messages']
            return []
        except:
            return None
    
    def send_message(self, chat_id: int, message: str) -> bool:
        """Отправка сообщения"""
        if not self.token and not self.login():
            return False
        
        # Ограничение длины сообщения (защита от DoS)
        if len(message) > 4000:
            message = message[:4000]
        
        url = f"{self.base_url}/debates/v2"
        params = {'token': self.token, 'id_i': chat_id}
        payload = {'message': message}
        headers = {'Content-Type': 'application/json'}
        
        try:
            response = self.session.post(url, params=params, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    return result.get('retval') == 0
                except json.JSONDecodeError:
                    return True
            return False
        except Exception as e:
            logging.error(f"Ошибка отправки: {e}")
            return False
    
    def get_last_sales(self, top: int = 10) -> Optional[Dict[str, Any]]:
        """Получение продаж"""
        if not self.token and not self.login():
            return None
        
        url = f"{self.base_url}/seller-last-sales"
        params = {'token': self.token, 'top': top}
        headers = {'Accept': 'application/json', 'locale': 'ru'}
        
        try:
            response = self.session.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException:
            if self.login():
                try:
                    params['token'] = self.token
                    response = self.session.get(url, params=params, headers=headers)
                    response.raise_for_status()
                    return response.json()
                except:
                    pass
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
    
    def get_reviews(self, count: int = 20, review_type: str = "all", page: int = 1, product_id: int = None) -> Optional[Dict[str, Any]]:
        """Получение отзывов
        
        Args:
            count: количество отзывов (default 20)
            review_type: тип отзывов - 'good', 'bad', 'all' (default 'all')
            page: номер страницы (default 1)
            product_id: ID товара для фильтрации (опционально)
        """
        if not self.token and not self.login():
            return None
        
        # Правильный эндпоинт согласно документации
        url = "https://seller.ggsel.net/api_sellers/api/reviews"
        params = {
            'token': self.token,
            'type': review_type,
            'page': page,
            'count': count
        }
        if product_id:
            params['product_id'] = product_id
            
        headers = {'Accept': 'application/json', 'locale': 'ru-RU'}
        
        try:
            response = self.session.get(url, params=params, headers=headers, timeout=30)
            if response.status_code == 200:
                data = response.json()
                if data.get('retval') == 0:
                    return data
            return None
        except requests.RequestException as e:
            logging.debug(f"Ошибка получения отзывов: {e}")
            if self.login():
                try:
                    params['token'] = self.token
                    response = self.session.get(url, params=params, headers=headers, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        if data.get('retval') == 0:
                            return data
                except:
                    pass
            return None

    def get_review_by_invoice(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        """Поиск отзыва по invoice_id
        
        Ищет отзыв в нескольких страницах результатов.
        Возвращает отзыв или None если не найден.
        """
        for page in range(1, 20):  # Ищем в первых 20 страницах
            data = self.get_reviews(count=50, page=page)
            if not data:
                break
            
            reviews = data.get('reviews', [])
            if not reviews:
                break
            
            for review in reviews:
                if review.get('invoice_id') == invoice_id:
                    return review
        
        return None
