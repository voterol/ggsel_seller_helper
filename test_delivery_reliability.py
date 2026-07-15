import asyncio
import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime
from unittest.mock import patch

from database import Database
from message_manager import MessageManager
from order_manager import Order, OrderManager
from purchase_manager import Purchase, PurchaseManager


def make_purchase(invoice_id=123):
    return Purchase(
        invoice_id=invoice_id, item_id=1, content_id=2, cart_uid="cart",
        name="item", amount=10.0, currency_type="USD", invoice_state=1,
        purchase_date="", date_pay="", buyer_email="buyer@example.test",
        buyer_account="buyer", buyer_id="1", buyer_phone="", buyer_ip="",
        payment_method="card", payment_aggregator="", processed_at="now",
    )


class DeliveryReliabilityTests(unittest.TestCase):
    def test_message_is_retryable_until_exact_chat_message_is_sent(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            async def exercise():
                # Construct and use asyncio primitives on one running loop;
                # this is portable to Python 3.9's loop-binding behavior.
                manager = MessageManager(db)
                self.assertTrue(await manager.add_processed_message(
                    1, "same-id", "one", datetime.now()
                ))
                self.assertFalse(manager.is_message_processed(1, "same-id"))
                self.assertTrue(await manager.add_processed_message(
                    1, "same-id", "one", datetime.now()
                ))

                # The same API message id in another chat is a distinct delivery.
                self.assertTrue(await manager.add_processed_message(
                    2, "same-id", "two", datetime.now()
                ))
                manager.mark_message_sent(1, "same-id")
                self.assertFalse(manager.is_message_processed(1, "same-id"))
                manager.set_effect_status(1, "same-id", "autoresponder", manager.COMPLETED)
                self.assertTrue(manager.is_message_processed(1, "same-id"))
                self.assertFalse(manager.is_message_processed(2, "same-id"))
                self.assertFalse(await manager.add_processed_message(
                    1, "same-id", "one", datetime.now()
                ))

            asyncio.run(exercise())

    def test_message_effects_are_durable_and_independent(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            async def exercise():
                manager = MessageManager(db)
                await manager.add_processed_message(1, "7", "hello", datetime.now())
                manager.set_effect_status(1, "7", "telegram", manager.PERMANENT_FAILURE)
                self.assertTrue(await manager.add_processed_message(
                    1, "7", "hello", datetime.now()
                ))
                manager.set_effect_status(1, "7", "autoresponder", manager.COMPLETED)

                reloaded = MessageManager(Database(db.db_path))
                self.assertTrue(reloaded.is_message_processed(1, "7"))
                self.assertEqual(
                    reloaded.get_effect_status(1, "7", "telegram"),
                    manager.PERMANENT_FAILURE,
                )

            asyncio.run(exercise())

    def test_message_and_initial_effects_roll_back_together_and_restart_retries(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            with sqlite3.connect(db.db_path) as conn:
                conn.execute('''
                    CREATE TRIGGER fail_initial_message_effect
                    BEFORE INSERT ON message_effects
                    WHEN NEW.effect = 'autoresponder'
                    BEGIN
                        SELECT RAISE(FAIL, 'injected effect failure');
                    END
                ''')

            async def fail_insert():
                manager = MessageManager(db)
                await manager.add_processed_message(
                    5, "atomic", "hello", datetime.now(), sent_to_telegram=True
                )

            with self.assertRaisesRegex(sqlite3.IntegrityError, "injected effect failure"):
                asyncio.run(fail_insert())

            self.assertFalse(db.message_exists("atomic", chat_id=5))
            with sqlite3.connect(db.db_path) as conn:
                self.assertEqual(
                    conn.execute(
                        "SELECT COUNT(*) FROM message_effects WHERE chat_id = 5 AND message_id = 'atomic'"
                    ).fetchone()[0],
                    0,
                )
                conn.execute("DROP TRIGGER fail_initial_message_effect")

            async def check_not_suppressed():
                self.assertFalse(MessageManager(db).is_message_processed(5, "atomic"))

            asyncio.run(check_not_suppressed())

            async def retry_after_restart():
                restarted = MessageManager(Database(db.db_path))
                self.assertTrue(await restarted.add_processed_message(
                    5, "atomic", "hello", datetime.now(), sent_to_telegram=True
                ))
                self.assertEqual(
                    restarted.get_effect_status(5, "atomic", "autoresponder"),
                    restarted.PENDING,
                )
                self.assertFalse(restarted.is_message_processed(5, "atomic"))

            asyncio.run(retry_after_restart())

    def test_genuine_legacy_message_without_effect_rows_remains_compatible(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            with sqlite3.connect(db.db_path) as conn:
                conn.execute(
                    "INSERT INTO messages "
                    "(chat_id, message_id, content, timestamp, is_sent_to_telegram) "
                    "VALUES (?, ?, ?, ?, TRUE)",
                    (3, "legacy", "old", datetime.now()),
                )

            async def check_legacy_row():
                manager = MessageManager(Database(db.db_path))
                self.assertTrue(manager.is_message_processed(3, "legacy"))
                self.assertFalse(await manager.add_processed_message(
                    3, "legacy", "old", datetime.now()
                ))

            asyncio.run(check_legacy_row())

    def test_purchase_stays_retryable_until_marked_processed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            manager = PurchaseManager(db)
            purchase = make_purchase()

            self.assertTrue(manager.add_purchase(purchase))
            self.assertFalse(manager.is_purchase_processed(purchase.invoice_id))
            self.assertEqual(manager.get_pending_purchase_ids(), [purchase.invoice_id])
            self.assertTrue(manager.add_purchase(purchase))
            self.assertTrue(manager.mark_purchase_processed(purchase.invoice_id))
            self.assertTrue(manager.is_purchase_processed(purchase.invoice_id))
            self.assertEqual(manager.get_pending_purchase_ids(), [])
            self.assertFalse(manager.add_purchase(purchase))

    def test_legacy_purchase_rows_remain_processed(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Database(os.path.join(directory, "state.db"))
            with sqlite3.connect(db.db_path) as conn:
                conn.execute(
                    "INSERT INTO purchases (invoice_id, data) VALUES (?, ?)",
                    ("99", json.dumps({"invoice_id": 99})),
                )
            self.assertTrue(PurchaseManager(db).is_purchase_processed(99))

    def test_order_is_not_retained_when_atomic_save_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "orders.json")
            manager = OrderManager(path)
            order = Order(1, 2, 3.0, "USD", "x@example.test", "today",
                          "abc", "127.0.0.1", "true", "now")
            with patch("order_manager.os.replace", side_effect=OSError("disk full")):
                self.assertFalse(manager.add_order(order))
            self.assertFalse(manager.order_exists(1))
            self.assertFalse(os.path.exists(path))

    def test_order_save_is_atomic_and_reloadable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, "orders.json")
            manager = OrderManager(path)
            order = Order(1, 2, 3.0, "USD", "x@example.test", "today",
                          "abc", "127.0.0.1", "true", "now")
            self.assertTrue(manager.add_order(order))
            self.assertEqual(OrderManager(path).get_order_by_id(1)["id_d"], 2)


if __name__ == "__main__":
    unittest.main()
