import asyncio
import json
import unittest
import tempfile
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bot_service import BotService
from telegram_bot import TelegramBot
from config import Config
from database import Database
from message_manager import MessageManager
from purchase_manager import Purchase
from locales import _


def make_purchase(**overrides) -> Purchase:
    kwargs = dict(
        invoice_id=777, item_id=1, content_id=2, cart_uid="cart", name="Product",
        amount=10.0, currency_type="USD", invoice_state=1,
        purchase_date="2024-01-02T00:00:00Z", date_pay="", buyer_email="buy@x.io",
        buyer_account="acct", buyer_id="1", buyer_phone="", buyer_ip="",
        payment_method="card", payment_aggregator="", processed_at="now",
        amount_rub=0.0, amount_usd=0.0, profit=0.0,
    )
    kwargs.update(overrides)
    return Purchase(**kwargs)


def make_topic_delivery_service(temp_dir: str) -> BotService:
    """Harness for create_topic_for_purchase with a fully mocked transport."""
    service = object.__new__(BotService)
    service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
    service.failed_topics = {}
    service.flood_control_until = None
    service.topic_manager = Mock()
    service.topic_manager.get_all_topics.return_value = {}
    service.telegram_bot = Mock()
    service.telegram_bot.create_topic = AsyncMock(return_value=(55, None))
    service.telegram_bot.send_message = AsyncMock(return_value=(True, None))
    service.message_flood_control_until = None
    service.pending_messages = []
    service.get_purchase_options_with_list = AsyncMock(return_value=(None, []))
    service.autoresponder = Mock()
    service.autoresponder.should_send_first_message.return_value = False
    service.usd_rub_rate = 50.0
    return service


class ReviewBatchFixTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.addCleanup(self.loop.close)

    # 1. FIX 1: buyer-controlled fields are HTML-escaped in the notification.
    def test_purchase_notification_escapes_buyer_fields(self):
        service = make_topic_delivery_service(None)
        purchase = make_purchase(
            payment_method="<a href='http://x'>p</a>",
            buyer_email="a<b>&c@x",
        )

        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            self.loop.run_until_complete(service.create_topic_for_purchase(purchase))

        sent_text = service.telegram_bot.send_message.await_args.args[0]
        self.assertIn("&lt;a href=", sent_text)
        self.assertIn("a&lt;b&gt;&amp;c", sent_text)
        self.assertNotIn("<a href='http://x'>", sent_text)

    # 2. FIX 6: an unparseable purchase date keeps the order retryable.
    def test_invalid_purchase_date_stays_pending(self):
        service = object.__new__(BotService)
        service.failed_topics = {}
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        service.ggsel_api.get_purchase_info.return_value = {"retval": 0, "content": {}}
        service.purchase_manager = Mock()
        service.purchase_manager.parse_purchase_response.return_value = (
            make_purchase(purchase_date="")
        )
        service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

        self.loop.run_until_complete(service.process_new_purchase(777))

        service.purchase_manager.mark_purchase_processed.assert_not_called()
        service.purchase_manager.add_purchase.assert_not_called()
        self.assertIn(777, service.failed_topics)

    # 3. FIX 5: successful topic sync marks the purchase processed.
    def test_sync_marks_purchase_processed(self):
        service = object.__new__(BotService)
        service.sync_enabled = True
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.check_deleted_topics = AsyncMock()
        service.ggsel_api = Mock()
        service.ggsel_api.get_last_sales.return_value = {
            "retval": 0, "sales": [{"invoice_id": 777, "date": "2024-01-02T00:00:00Z"}]
        }
        service.topic_manager = Mock()
        service.topic_manager.topics = {}
        service.purchase_manager = Mock()
        service.purchase_manager.parse_purchase_response.return_value = make_purchase()
        service.purchase_manager.add_purchase.return_value = True
        service.create_topic_for_purchase = AsyncMock(return_value=True)
        service.flood_control_until = None
        service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            self.loop.run_until_complete(service.sync_topics_with_purchases())

        service.purchase_manager.mark_purchase_processed.assert_called_once_with(777)

    # 4. FIX 3: chat-message mirrors dedup in the flood queue.
    def test_flood_queue_dedups_chat_mirror(self):
        service = object.__new__(BotService)
        service.message_manager = Mock()
        service.message_manager.COMPLETED = "completed"
        service.message_manager.PERMANENT_FAILURE = "permanent_failure"
        service.message_manager.get_effect_status.return_value = "pending"
        service.message_flood_control_until = datetime.now() + timedelta(seconds=60)
        service.pending_messages = []
        service.telegram_bot = Mock()

        for _ in range(2):
            self.loop.run_until_complete(
                service.send_message_with_cooldown("hello", 5, 99, "m1", effect="telegram")
            )

        self.assertEqual(len(service.pending_messages), 1)

    # 5. FIX 4: menu navigation cancels a pending text-input prompt.
    def test_menu_callback_clears_awaiting_input(self):
        service = object.__new__(BotService)
        service.awaiting_input = {42: {"type": "first_message"}}
        service.show_auto_menu = AsyncMock()

        update = Mock()
        update.callback_query.message.chat.id = 42
        update.callback_query.message.message_id = 100

        self.loop.run_until_complete(service.handle_callback("auto_menu", update, Mock()))

        self.assertEqual(service.awaiting_input, {})
        service.show_auto_menu.assert_awaited_once_with(42, 100)

    # 6. FIX 2: a permanent Telegram rejection of a review notification is terminal.
    def test_review_notification_permanent_failure_terminal(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        service = object.__new__(BotService)
        service.database = Database(str(Path(temp.name) / "state.db"))
        service.message_manager = MessageManager(service.database)
        service.processed_reviews = {}
        service.autoresponder = Mock()
        service.autoresponder.get_review_response.return_value = None
        service.message_flood_control_until = None
        service.pending_messages = []
        service.telegram_bot = Mock()
        service.telegram_bot.send_message = AsyncMock(return_value=(False, None))

        review = {"id": "3", "invoice_id": 9, "type": "good", "info": "ok"}
        topic = {9: {"topic_id": 4}}
        self.loop.run_until_complete(service._process_reviews([review], topic, None))

        self.assertEqual(
            service._get_review_effect_status("3", "good:ok", "telegram_notification"),
            MessageManager.PERMANENT_FAILURE,
        )
        self.assertEqual(service.telegram_bot.send_message.await_count, 1)

        # Re-polling must not re-send a permanently rejected notification.
        self.loop.run_until_complete(service._process_reviews([review], topic, None))
        self.assertEqual(service.telegram_bot.send_message.await_count, 1)

    # 7. FIX 7: no fabricated RUB price and no fake profit line.
    def test_currency_display_no_fabricated_rub(self):
        service = make_topic_delivery_service(None)
        purchase = make_purchase(amount=10.0, amount_rub=0.0, amount_usd=0.0, profit=0.0)

        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            self.loop.run_until_complete(service.create_topic_for_purchase(purchase))

        sent_text = service.telegram_bot.send_message.await_args.args[0]
        self.assertNotIn("RUB:", sent_text)
        # USD fallback uses the tracked rate (10 / 50), not the old /90.0.
        self.assertIn("• USD: 0.2", sent_text)
        self.assertNotIn(_("noti_profit"), sent_text)

    # 8. FIX 8: CSV menu disabled status renders a single leading ❌.
    def test_csv_menu_disabled_status(self):
        service = object.__new__(BotService)
        service.autoresponder = Mock()
        service.autoresponder.is_csv_mode_enabled.return_value = False
        service.autoresponder.get_csv_rules.return_value = []
        service.telegram_bot = Mock()
        service.telegram_bot.edit_message = AsyncMock()

        self.loop.run_until_complete(service.show_csv_menu(1, 2))

        text = service.telegram_bot.edit_message.await_args.args[2]
        self.assertIn(f"❌ {_('disabled')}", text)
        self.assertNotIn("✅ ❌", text)

    # 9. FIX 8: mixed Z / +03:00 history feeds sort without TypeError.
    def test_history_sort_mixed_timezone_offsets(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        service = object.__new__(BotService)
        service.database = Database(str(Path(temp.name) / "state.db"))
        service.message_manager = MessageManager(service.database)
        service.ggsel_api = Mock()
        service.ggsel_api.get_chat_messages.return_value = [
            {"id": "1", "message": "first", "date_written": "2024-01-01T00:00:00Z"},
            # +03:00 wall time 00:00:01 is 2023-12-31T21:00:01Z -> sorts first.
            {"id": "2", "message": "second", "date_written": "2024-01-01T00:00:01+03:00"},
            {"id": "3", "message": "third", "date_written": "2024-01-01T00:00:02Z"},
        ]
        service.autoresponder = Mock()
        service.autoresponder.should_send_first_message.return_value = False
        service.send_message_with_cooldown = AsyncMock(return_value=True)

        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            self.loop.run_until_complete(service.load_chat_history([9], 4))

        mirrored = [c.args[0] for c in service.send_message_with_cooldown.await_args_list]
        self.assertEqual(mirrored, ["second", "first", "third"])

    # 10. FIX 8: balance baseline only advances when the alert was delivered.
    def test_balance_baseline_updates_only_on_success(self):
        service = object.__new__(BotService)
        service.running = True
        service.sync_enabled = True
        service._sync_enabled_event = asyncio.Event()
        service._sync_enabled_event.set()
        service._sync_operation_lock = asyncio.Lock()
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        balances = iter([100.0, 150.0, 150.0, 150.0])
        service.ggsel_api.get_balance_info.side_effect = lambda: {
            "content": {"amount_t_free": next(balances), "amount_t_lock": 0.0}
        }
        service.telegram_bot = Mock()
        # (False, 60) is a transient RetryAfter: the baseline must stay put
        # so the change re-alerts. (False, None) is now permanent and would
        # advance the baseline instead — covered by test 15.
        send_results = iter([(False, 60), (True, None)])
        service.telegram_bot.send_message = AsyncMock(
            side_effect=lambda *a, **k: next(send_results)
        )

        sleeps = {"count": 0}

        def fake_sleep(seconds):
            sleeps["count"] += 1
            if sleeps["count"] >= 5:
                service.running = False

        with patch("asyncio.sleep", new=AsyncMock(side_effect=fake_sleep)):
            self.loop.run_until_complete(service.balance_monitor_loop())

        # Failed alert leaves the baseline at 100 so the change re-alerts;
        # the successful send then advances it to 150 and the next equal
        # observation sends nothing.
        self.assertEqual(service.telegram_bot.send_message.await_count, 2)
        self.assertEqual(service.last_available_balance, 150.0)

    # 11. FIX 2: BadRequest while creating a topic is terminal, not a 60s retry.
    def test_create_topic_badrequest_is_terminal(self):
        from telegram.error import BadRequest

        config = Config(
            ggsel_seller_id=1, ggsel_api_key="key",
            telegram_bot_token="123:abc", telegram_group_id=-100,
            telegram_allowed_user_ids=frozenset(),
        )
        bot = TelegramBot(config)
        # PTB's Bot forbids instance attribute patching; replace it wholesale.
        bot.bot = Mock()
        bot.bot.create_forum_topic = AsyncMock(side_effect=BadRequest("no admin"))

        self.assertEqual(asyncio.run(bot.create_topic("t")), (None, None))

        service = make_topic_delivery_service(None)
        service.telegram_bot.create_topic = AsyncMock(return_value=(None, None))
        purchase = make_purchase()

        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            delivered = self.loop.run_until_complete(
                service.create_topic_for_purchase(purchase)
            )

        self.assertFalse(delivered)
        self.assertIn(777, service.failed_topics)
        self.assertIsNone(service.flood_control_until)

    # 12. FIX 9: one corrupt pending-topic row must not wipe the rest.
    def test_corrupt_pending_topic_row_skipped(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        db = Database(str(Path(temp.name) / "state.db"))
        corrupt = json.dumps({
            "purchase": {"unexpected_field": 1},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        good = json.dumps({
            "purchase": asdict(make_purchase(invoice_id=888)),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "skip_greeting": False,
        })
        import sqlite3
        with sqlite3.connect(db.db_path) as conn:
            conn.execute("INSERT INTO pending_topics (data) VALUES (?)", (corrupt,))
            conn.execute("INSERT INTO pending_topics (data) VALUES (?)", (good,))

        service = object.__new__(BotService)
        service.database = db
        service.pending_topics = []

        service._load_pending_topics()

        self.assertEqual(len(service.pending_topics), 1)
        self.assertEqual(service.pending_topics[0]["purchase"].invoice_id, 888)


    # 13. REVIEW PASS 2: keyless review sends dedup in the flood queue.
    def test_review_send_deduped_during_flood(self):
        service = object.__new__(BotService)
        service.message_flood_control_until = datetime.now() + timedelta(seconds=60)
        service.pending_messages = []

        key = "review:3:good:ok:telegram_notification"
        for _ in range(3):
            self.loop.run_until_complete(
                service.send_message_with_cooldown(
                    "review!", 4, dedup_key=key,
                    review=("3", "good:ok", "telegram_notification"),
                )
            )

        self.assertEqual(len(service.pending_messages), 1)
        self.assertEqual(service.pending_messages[0]["dedup_key"], key)

    # 14. REVIEW PASS 2: a drained review notification is marked complete so
    # the next poll doesn't deliver it a second time.
    def test_drain_completes_review_effect(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        service = object.__new__(BotService)
        service.database = Database(str(Path(temp.name) / "state.db"))
        service.message_manager = MessageManager(service.database)
        service.message_flood_control_until = datetime.now() + timedelta(seconds=60)
        service.pending_messages = []
        service.telegram_bot = Mock()
        service.telegram_bot.send_message = AsyncMock(return_value=(True, None))
        service.purchase_manager = Mock()

        self.loop.run_until_complete(
            service.send_message_with_cooldown(
                "review!", 4,
                dedup_key="review:3:good:ok:telegram_notification",
                review=("3", "good:ok", "telegram_notification"),
            )
        )
        self.assertEqual(
            service._get_review_effect_status("3", "good:ok", "telegram_notification"),
            MessageManager.PENDING,
        )

        service.message_flood_control_until = None
        with patch("asyncio.sleep", new=AsyncMock(return_value=None)):
            self.loop.run_until_complete(service.process_pending_messages())

        self.assertEqual(
            service._get_review_effect_status("3", "good:ok", "telegram_notification"),
            MessageManager.COMPLETED,
        )
        self.assertEqual(service.telegram_bot.send_message.await_count, 1)
        self.assertEqual(service.pending_messages, [])

    # 15. REVIEW PASS 2: a permanently rejected balance alert advances the
    # baseline instead of retrying every 60s cycle forever.
    def test_balance_baseline_advances_on_permanent_rejection(self):
        service = object.__new__(BotService)
        service.running = True
        service.sync_enabled = True
        service._sync_enabled_event = asyncio.Event()
        service._sync_enabled_event.set()
        service._sync_operation_lock = asyncio.Lock()
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        balances = iter([100.0, 150.0, 150.0, 150.0])
        service.ggsel_api.get_balance_info.side_effect = lambda: {
            "content": {"amount_t_free": next(balances), "amount_t_lock": 0.0}
        }
        service.telegram_bot = Mock()
        service.telegram_bot.send_message = AsyncMock(return_value=(False, None))

        sleeps = {"count": 0}

        def fake_sleep(seconds):
            sleeps["count"] += 1
            if sleeps["count"] >= 5:
                service.running = False

        with patch("asyncio.sleep", new=AsyncMock(side_effect=fake_sleep)):
            self.loop.run_until_complete(service.balance_monitor_loop())

        # One permanent rejection, then the baseline advances and the equal
        # observations stop alerting instead of retrying forever.
        self.assertEqual(service.telegram_bot.send_message.await_count, 1)
        self.assertEqual(service.last_available_balance, 150.0)

    # 16. REVIEW PASS 2: telegram_bot's short-circuited language menu cancels
    # a pending text-input prompt (fix 4 previously missed these callbacks).
    def test_language_menu_clears_awaiting_input(self):
        config = Config(
            ggsel_seller_id=1, ggsel_api_key="key",
            telegram_bot_token="123:abc", telegram_group_id=-100,
            telegram_allowed_user_ids=frozenset({7}),
        )
        bot = TelegramBot(config)
        awaiting = {-100: {"type": "first_message"}}
        bot.set_awaiting_input(awaiting)

        update = Mock()
        update.effective_chat.id = -100
        update.effective_user.id = 7
        update.callback_query.data = "lang_toggle"
        update.callback_query.message.chat.id = -100
        update.callback_query.message.message_id = 5
        update.callback_query.answer = AsyncMock()
        update.callback_query.edit_message_text = AsyncMock()

        self.loop.run_until_complete(bot._handle_callback(update, Mock()))

        self.assertEqual(awaiting, {})
        update.callback_query.edit_message_text.assert_awaited_once()

    # 17. REVIEW PASS 2: distinct mirror texts (e.g. /history force-reload
    # variant) are not collapsed by the flood-queue dedup.
    def test_flood_queue_keeps_distinct_mirror_texts(self):
        service = object.__new__(BotService)
        service.message_manager = Mock()
        service.message_manager.get_effect_status.return_value = "pending"
        service.message_flood_control_until = datetime.now() + timedelta(seconds=60)
        service.pending_messages = []

        self.loop.run_until_complete(service.send_message_with_cooldown("hello", 5, 99, "m1"))
        self.loop.run_until_complete(service.send_message_with_cooldown("📜 hello", 5, 99, "m1"))

        self.assertEqual(len(service.pending_messages), 2)


if __name__ == "__main__":
    unittest.main()
