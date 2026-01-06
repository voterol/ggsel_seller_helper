import json
import os
from typing import Dict, List, Optional
from datetime import datetime
from dataclasses import dataclass

@dataclass
class Purchase:
    invoice_id: int
    item_id: int
    content_id: int
    cart_uid: str
    name: str
    amount: float
    currency_type: str
    invoice_state: int
    purchase_date: str
    date_pay: str
    buyer_email: str
    buyer_account: str
    buyer_phone: str
    buyer_ip: str
    payment_method: str
    processed_at: str

class PurchaseManager:
    def __init__(self, purchases_file: str = "processed_purchases.json"):
        self.purchases_file = purchases_file
        self.processed_purchases: Dict[str, Dict] = self.load_purchases()
    
    def load_purchases(self) -> Dict[str, Dict]:
        """Загрузка обработанных покупок из JSON файла"""
        if os.path.exists(self.purchases_file):
            try:
                with open(self.purchases_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Ошибка загрузки покупок: {e}")
        return {}
    
    def save_purchases(self) -> None:
        """Сохранение обработанных покупок в JSON файл"""
        try:
            with open(self.purchases_file, 'w', encoding='utf-8') as f:
                json.dump(self.processed_purchases, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Ошибка сохранения покупок: {e}")
    
    def parse_purchase_response(self, response_data: Dict, invoice_id: int) -> Optional[Purchase]:
        """Парсинг ответа API с информацией о покупке"""
        try:
            if response_data.get('retval') == 0 and 'content' in response_data:
                content = response_data['content']
                buyer_info = content.get('buyer_info', {})
                
                purchase = Purchase(
                    invoice_id=invoice_id,
                    item_id=content.get('item_id', 0),
                    content_id=content.get('content_id', 0),
                    cart_uid=content.get('cart_uid', '') or '',
                    name=content.get('name', ''),
                    amount=float(content.get('amount', 0)),
                    currency_type=content.get('currency_type', 'USD'),
                    invoice_state=content.get('invoice_state', 0),
                    purchase_date=content.get('purchase_date', ''),
                    date_pay=content.get('date_pay', ''),
                    buyer_email=buyer_info.get('email', '') or '',
                    buyer_account=buyer_info.get('account', '') or '',
                    buyer_phone=buyer_info.get('phone', '') or '',
                    buyer_ip=buyer_info.get('ip_address', '') or '',
                    payment_method=buyer_info.get('payment_method', '') or '',
                    processed_at=datetime.now().isoformat()
                )
                
                return purchase
            else:
                print(f"Ошибка в ответе API для покупки {invoice_id}: {response_data}")
                return None
                
        except Exception as e:
            print(f"Ошибка парсинга покупки {invoice_id}: {e}")
            return None
    
    def add_purchase(self, purchase: Purchase) -> bool:
        """Добавление новой покупки"""
        try:
            key = str(purchase.invoice_id)
            
            # Проверяем, не обработана ли уже эта покупка
            if key in self.processed_purchases:
                return False
            
            self.processed_purchases[key] = {
                "invoice_id": purchase.invoice_id,
                "item_id": purchase.item_id,
                "content_id": purchase.content_id,
                "cart_uid": purchase.cart_uid,
                "name": purchase.name,
                "amount": purchase.amount,
                "currency_type": purchase.currency_type,
                "invoice_state": purchase.invoice_state,
                "purchase_date": purchase.purchase_date,
                "date_pay": purchase.date_pay,
                "buyer_email": purchase.buyer_email,
                "buyer_account": purchase.buyer_account,
                "buyer_phone": purchase.buyer_phone,
                "buyer_ip": purchase.buyer_ip,
                "payment_method": purchase.payment_method,
                "processed_at": purchase.processed_at
            }
            
            self.save_purchases()
            return True
            
        except Exception as e:
            print(f"Ошибка добавления покупки: {e}")
            return False
    
    def is_purchase_processed(self, invoice_id: int) -> bool:
        """Проверка, обработана ли покупка"""
        return str(invoice_id) in self.processed_purchases
    
    def get_purchase_by_id(self, invoice_id: int) -> Optional[Dict]:
        """Получение покупки по ID"""
        key = str(invoice_id)
        return self.processed_purchases.get(key)
    
    def get_purchase_by_email(self, email: str) -> Optional[Dict]:
        """Получение покупки по email покупателя"""
        for purchase_data in self.processed_purchases.values():
            if purchase_data.get("buyer_email") == email:
                return purchase_data
        return None
    
    def get_all_purchases(self) -> Dict[str, Dict]:
        """Получение всех покупок"""
        return self.processed_purchases.copy()
    
    def cleanup_old_purchases(self, days: int = 30) -> int:
        """Очистка старых покупок (старше указанного количества дней)"""
        try:
            cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
            keys_to_remove = []
            
            for key, purchase_data in self.processed_purchases.items():
                try:
                    processed_at = datetime.fromisoformat(purchase_data.get("processed_at", ""))
                    if processed_at.timestamp() < cutoff_date:
                        keys_to_remove.append(key)
                except:
                    continue
            
            for key in keys_to_remove:
                del self.processed_purchases[key]
            
            if keys_to_remove:
                self.save_purchases()
            
            return len(keys_to_remove)
            
        except Exception as e:
            print(f"Ошибка очистки старых покупок: {e}")
            return 0