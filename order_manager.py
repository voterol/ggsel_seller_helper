import json
import os
import re
from typing import Dict, Optional, List
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Order:
    id_i: int
    id_d: int
    amount: float
    currency: str
    email: str
    date: str
    sha256: str
    ip: str
    is_my_product: str
    created_at: str

class OrderManager:
    def __init__(self, orders_file: str = "orders.json"):
        self.orders_file = orders_file
        self.orders: Dict[str, Dict] = self.load_orders()
    
    def load_orders(self) -> Dict[str, Dict]:
        """Загрузка заказов из JSON файла"""
        if os.path.exists(self.orders_file):
            try:
                with open(self.orders_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Ошибка загрузки заказов: {e}")
        return {}
    
    def save_orders(self) -> None:
        """Сохранение заказов в JSON файл"""
        try:
            with open(self.orders_file, 'w', encoding='utf-8') as f:
                json.dump(self.orders, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка сохранения заказов: {e}")
    
    def parse_order_message(self, message_text: str) -> Optional[Order]:
        """Парсинг сообщения о новом заказе"""
        try:
            # Регулярные выражения для извлечения данных
            patterns = {
                'id_i': r'ID_I:\s*(\d+)',
                'id_d': r'ID_D:\s*(\d+)',
                'amount': r'Amount:\s*([\d.]+)',
                'currency': r'Currency:\s*(\w+)',
                'email': r'Email:\s*([^\s]+)',
                'date': r'Date:\s*([^\s]+)',
                'sha256': r'SHA256:\s*([a-f0-9]+)',
                'ip': r'IP:\s*([^\s]+)',
                'is_my_product': r'IsMyProduct:\s*(.+?)(?:\n|$)'
            }
            
            extracted = {}
            for key, pattern in patterns.items():
                match = re.search(pattern, message_text, re.IGNORECASE)
                if match:
                    extracted[key] = match.group(1).strip()
                else:
                    print(f"Не найдено поле {key} в сообщении")
                    return None
            
            # Создаем объект заказа
            order = Order(
                id_i=int(extracted['id_i']),
                id_d=int(extracted['id_d']),
                amount=float(extracted['amount']),
                currency=extracted['currency'],
                email=extracted['email'],
                date=extracted['date'],
                sha256=extracted['sha256'],
                ip=extracted['ip'],
                is_my_product=extracted['is_my_product'],
                created_at=datetime.now().isoformat()
            )
            
            return order
            
        except Exception as e:
            print(f"Ошибка парсинга заказа: {e}")
            return None
    
    def add_order(self, order: Order) -> bool:
        """Добавление нового заказа"""
        try:
            key = str(order.id_i)
            
            # Проверяем, не существует ли уже такой заказ
            if key in self.orders:
                return False
            
            self.orders[key] = {
                "id_i": order.id_i,
                "id_d": order.id_d,
                "amount": order.amount,
                "currency": order.currency,
                "email": order.email,
                "date": order.date,
                "sha256": order.sha256,
                "ip": order.ip,
                "is_my_product": order.is_my_product,
                "created_at": order.created_at
            }
            
            self.save_orders()
            return True
            
        except Exception as e:
            print(f"Ошибка добавления заказа: {e}")
            return False
    
    def get_order_by_id(self, id_i: int) -> Optional[Dict]:
        """Получение заказа по ID_I"""
        key = str(id_i)
        return self.orders.get(key)
    
    def get_order_by_email(self, email: str) -> Optional[Dict]:
        """Получение заказа по email"""
        for order_data in self.orders.values():
            if order_data.get('email') == email:
                return order_data
        return None
    
    def order_exists(self, id_i: int) -> bool:
        """Проверка существования заказа"""
        return str(id_i) in self.orders
    
    def get_all_orders(self) -> Dict[str, Dict]:
        """Получение всех заказов"""
        return self.orders.copy()