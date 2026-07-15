import json
import logging
import sqlite3
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
    buyer_id: str
    buyer_phone: str
    buyer_ip: str
    payment_method: str
    payment_aggregator: str
    processed_at: str
    amount_rub: float = 0.0
    amount_usd: float = 0.0
    profit: float = 0.0

class PurchaseManager:
    _STATE_KEY = "_delivery_state"
    _PENDING = "pending"
    _DELIVERED = "delivered"

    def __init__(self, db):
        self.db = db
    
    def parse_purchase_response(self, response_data: Dict, invoice_id: int) -> Optional[Purchase]:
        try:
            if response_data.get('retval') == 0 and 'content' in response_data:
                content = response_data['content']
                buyer_info = content.get('buyer_info', {})
                
                amt_rub = float(content.get('amount_rub', content.get('amount', 0)))
                amt_usd = float(content.get('amount_usd', 0))
                
                # Using the exact profit key confirmed from the GGSel API
                profit = float(content.get('profit', content.get('seller_profit', 0)))
                
                b_account = str(buyer_info.get('account', ''))
                b_id = str(buyer_info.get('id', content.get('id_buyer', b_account)))
                aggregator = str(buyer_info.get('payment_aggregator', content.get('payment_aggregator', '')))
                
                return Purchase(
                    invoice_id=invoice_id, item_id=content.get('item_id', 0), content_id=content.get('content_id', 0),
                    cart_uid=content.get('cart_uid', '') or '', name=content.get('name', ''), amount=float(content.get('amount', 0)),
                    currency_type=content.get('currency_type', 'USD'), invoice_state=content.get('invoice_state', 0),
                    purchase_date=content.get('purchase_date', ''), date_pay=content.get('date_pay', ''),
                    buyer_email=buyer_info.get('email', '') or '', buyer_account=b_account,
                    buyer_id=b_id, buyer_phone=buyer_info.get('phone', '') or '', buyer_ip=buyer_info.get('ip_address', '') or '',
                    payment_method=buyer_info.get('payment_method', '') or '', payment_aggregator=aggregator, 
                    processed_at=datetime.now().isoformat(), amount_rub=amt_rub, amount_usd=amt_usd, profit=profit
                )
            return None
        except Exception as e:
            logging.error(f"Error parsing purchase {invoice_id}: {e}")
            return None
    
    def add_purchase(self, purchase: Purchase) -> bool:
        """Insert a purchase as pending delivery.

        The purchases table remains schema-compatible: delivery state is kept
        in the existing JSON value.  Use mark_purchase_processed only after
        the externally visible side effect succeeds.
        """
        try:
            data = dict(purchase.__dict__)
            data[self._STATE_KEY] = self._PENDING
            with sqlite3.connect(self.db.db_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                cur = conn.execute(
                    "INSERT OR IGNORE INTO purchases (invoice_id, data) VALUES (?, ?)",
                    (str(purchase.invoice_id), json.dumps(data)),
                )
                if cur.rowcount == 1:
                    return True
                row = conn.execute(
                    "SELECT data FROM purchases WHERE invoice_id = ?",
                    (str(purchase.invoice_id),),
                ).fetchone()
                if not row:
                    return False
                existing = json.loads(row[0])
                return existing.get(self._STATE_KEY, self._DELIVERED) == self._PENDING
        except Exception as e:
            logging.error(f"Error adding purchase: {e}")
            return False
    
    def is_purchase_processed(self, invoice_id: int) -> bool:
        with sqlite3.connect(self.db.db_path) as conn:
            row = conn.execute(
                "SELECT data FROM purchases WHERE invoice_id = ?", (str(invoice_id),)
            ).fetchone()
        if row is None:
            return False
        try:
            data = json.loads(row[0])
        except (TypeError, json.JSONDecodeError):
            # Preserve the historical contract for pre-existing/corrupt rows;
            # operators can still reconcile their topics explicitly.
            return True
        # Rows written by older versions represented completed processing.
        return data.get(self._STATE_KEY, self._DELIVERED) == self._DELIVERED

    def get_pending_purchase_ids(self) -> List[int]:
        """Return staged deliveries so callers can retry beyond API windows."""
        pending = []
        with sqlite3.connect(self.db.db_path) as conn:
            rows = conn.execute("SELECT invoice_id, data FROM purchases").fetchall()
        for invoice_id, raw_data in rows:
            try:
                data = json.loads(raw_data)
                if data.get(self._STATE_KEY) == self._PENDING:
                    pending.append(int(invoice_id))
            except (TypeError, ValueError, json.JSONDecodeError):
                logging.warning(f"Ignoring invalid pending purchase row {invoice_id}")
        return pending

    def mark_purchase_processed(self, invoice_id: int) -> bool:
        """Atomically mark an already-staged purchase as delivered."""
        try:
            with sqlite3.connect(self.db.db_path) as conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT data FROM purchases WHERE invoice_id = ?", (str(invoice_id),)
                ).fetchone()
                if row is None:
                    return False
                data = json.loads(row[0])
                data[self._STATE_KEY] = self._DELIVERED
                conn.execute(
                    "UPDATE purchases SET data = ? WHERE invoice_id = ?",
                    (json.dumps(data), str(invoice_id)),
                )
            return True
        except (sqlite3.Error, TypeError, json.JSONDecodeError) as e:
            logging.error(f"Error marking purchase {invoice_id} processed: {e}")
            return False
