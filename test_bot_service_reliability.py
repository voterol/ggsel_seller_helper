import asyncio
import importlib
import sys
import types
import unittest
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, call, patch
from database import Database
from message_manager import MessageManager

# Keep this focused unit test independent from the optional Telegram package.
class TelegramValue:
    def __init__(self, *args, **kwargs):
        pass

telegram = types.ModuleType("telegram")
telegram.Update = telegram.InlineKeyboardButton = telegram.InlineKeyboardMarkup = TelegramValue
telegram_ext = types.ModuleType("telegram.ext")
for name in ("Application", "CommandHandler", "CallbackQueryHandler", "MessageHandler", "filters"):
    setattr(telegram_ext, name, object)

ggsel_api = types.ModuleType("ggsel_api")
ggsel_api.GGSelAPI = object
telegram_bot = types.ModuleType("telegram_bot")
telegram_bot.TelegramBot = object

# Keep this focused unit test independent from the optional Telegram package,
# while restoring the import cache exactly as it was for later test modules.
existing_bot_service = sys.modules.get("bot_service")
try:
    with patch.dict(sys.modules, {
        "telegram": telegram,
        "telegram.ext": telegram_ext,
        "ggsel_api": ggsel_api,
        "telegram_bot": telegram_bot,
    }):
        BotService = importlib.import_module("bot_service").BotService
finally:
    if existing_bot_service is None:
        sys.modules.pop("bot_service", None)
    else:
        sys.modules["bot_service"] = existing_bot_service


class BotServiceReliabilityTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.addCleanup(self.loop.close)
        # create_topic_for_purchase has a local Telegram import. Scope its
        # optional-dependency stand-in to each test and restore any real or
        # independently stubbed module afterward.
        self.telegram_modules = patch.dict(sys.modules, {
            "telegram": telegram,
            "telegram.ext": telegram_ext,
        })
        self.telegram_modules.start()
        self.addCleanup(self.telegram_modules.stop)

    def test_incoming_false_customer_send_stays_pending_and_is_retried(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        service = object.__new__(BotService)
        service.database = Database(str(Path(temp.name) / "state.db"))
        service.message_manager = MessageManager(service.database)
        service.send_message_with_cooldown = AsyncMock(return_value=True)
        service.autoresponder = Mock()
        service.autoresponder.find_response.return_value = {"response": "reply"}
        service.ggsel_api = Mock()
        service.ggsel_api.last_failure = "retryable"
        service.ggsel_api.send_message.side_effect = [False, True]

        message = {"id": "4", "message": "hello", "timestamp": "2024-01-01T00:00:00"}
        self.loop.run_until_complete(service.process_single_message_check(9, 20, message))
        self.assertEqual(service.message_manager.get_effect_status(9, "4", "autoresponder"), "pending")
        self.loop.run_until_complete(service.process_single_message_check(9, 20, message))

        self.assertEqual(service.ggsel_api.send_message.call_count, 2)
        self.assertEqual(service.message_manager.get_effect_status(9, "4", "autoresponder"), "completed")

    def test_incoming_customer_success_survives_mirror_failure_and_restart(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        db = Database(str(Path(temp.name) / "state.db"))
        service = object.__new__(BotService); service.database = db
        service.message_manager = MessageManager(db)
        service.autoresponder = Mock(); service.autoresponder.find_response.return_value = {"response": "reply"}
        service.ggsel_api = Mock(); service.ggsel_api.send_message.return_value = True
        service.send_message_with_cooldown = AsyncMock(side_effect=[True, False])
        message = {"id": "5", "message": "hello", "timestamp": "2024-01-01T00:00:00"}
        self.loop.run_until_complete(service.process_single_message_check(9, 20, message))
        # The helper is mocked in this focused test, so model its successful
        # first Telegram effect explicitly; the failed mirror remains pending.
        service.message_manager.set_effect_status(9, "5", "telegram", "completed")

        restarted = object.__new__(BotService); restarted.database = Database(db.db_path)
        restarted.message_manager = MessageManager(restarted.database)
        restarted.autoresponder = service.autoresponder; restarted.ggsel_api = service.ggsel_api
        restarted.send_message_with_cooldown = AsyncMock(return_value=True)
        self.loop.run_until_complete(restarted.process_single_message_check(9, 20, message))
        service.ggsel_api.send_message.assert_called_once_with(9, "reply")
        restarted.send_message_with_cooldown.assert_awaited_once()

    def test_permanent_telegram_failure_becomes_terminal_without_queue(self):
        service = object.__new__(BotService)
        service.message_manager = Mock()
        service.message_manager.COMPLETED = "completed"
        service.message_manager.PERMANENT_FAILURE = "permanent_failure"
        service.message_manager.get_effect_status.return_value = "pending"
        service.message_flood_control_until = None
        service.pending_messages = []
        service.telegram_bot = Mock()
        service.telegram_bot.send_message = AsyncMock(return_value=(False, None))

        self.assertFalse(asyncio.run(service.send_message_with_cooldown("x", 3, 7, "same")))
        service.message_manager.set_effect_status.assert_called_once_with(
            7, "same", "telegram", "permanent_failure"
        )
        self.assertEqual(service.pending_messages, [])

    def test_queued_purchase_notification_marks_purchase_after_delivery(self):
        service = object.__new__(BotService)
        service.pending_messages = [{
            "text": "order", "topic_id": 8, "purchase_invoice_id": 42
        }]
        service.message_flood_control_until = None
        service.send_message_with_cooldown = AsyncMock(return_value=True)
        service.purchase_manager = Mock()

        asyncio.run(service.process_pending_messages())

        service.purchase_manager.mark_purchase_processed.assert_called_once_with(42)

    def test_rate_limited_queue_preserves_unprocessed_tail(self):
        service = object.__new__(BotService)
        service.pending_messages = [
            {"text": "first", "topic_id": 1},
            {"text": "second", "topic_id": 2},
        ]
        service.message_flood_control_until = None

        async def rate_limited(*args, **kwargs):
            from datetime import timedelta
            service.message_flood_control_until = datetime.now() + timedelta(seconds=60)
            service.pending_messages.append({"text": "first", "topic_id": 1})
            return False

        service.send_message_with_cooldown = AsyncMock(side_effect=rate_limited)
        service.purchase_manager = Mock()

        asyncio.run(service.process_pending_messages())

        self.assertEqual(
            [item["text"] for item in service.pending_messages],
            ["first", "second"],
        )

    def test_existing_purchase_topic_retries_required_notification(self):
        service = object.__new__(BotService)
        service.failed_topics = {}
        service.flood_control_until = None
        service.topic_manager = Mock()
        service.topic_manager.get_all_topics.return_value = {
            "purchase_123": {"topic_id": 88}
        }
        service.telegram_bot = Mock()
        service.send_message_with_cooldown = AsyncMock(side_effect=[False, True])
        service.get_purchase_options_with_list = AsyncMock(return_value=("", []))
        service.autoresponder = Mock()
        service.autoresponder.should_send_first_message.return_value = False
        service.usd_rub_rate = 90
        service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)

        from purchase_manager import Purchase
        purchase = Purchase(123, 1, 2, "cart", "item", 10, "USD", 1, "2024-01-02T00:00:00Z", "", "e", "a", "1", "", "", "card", "", "now")
        self.assertFalse(asyncio.run(service.create_topic_for_purchase(purchase)))
        self.assertTrue(asyncio.run(service.create_topic_for_purchase(purchase)))
        service.telegram_bot.create_topic.assert_not_called()
        self.assertEqual(service.send_message_with_cooldown.await_count, 2)

    def test_purchase_scan_merges_durable_pending_and_uses_wide_window(self):
        service = object.__new__(BotService)
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        service.ggsel_api.get_last_sales.return_value = {
            "retval": 0, "sales": [{"invoice_id": 11, "date": "2024-01-02T00:00:00Z"}]
        }
        service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        service.purchase_manager = Mock()
        service.purchase_manager.get_pending_purchase_ids.return_value = [7, 11]
        service.purchase_manager.is_purchase_processed.return_value = False
        service.failed_topics = {}
        service.process_new_purchase = AsyncMock()

        asyncio.run(service.check_new_purchases())

        service.ggsel_api.get_last_sales.assert_called_once_with(100)
        self.assertEqual(
            [call.args[0] for call in service.process_new_purchase.await_args_list],
            [11, 7],
        )

    def test_purchase_scan_ignores_sales_before_installation(self):
        service = object.__new__(BotService)
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        service.ggsel_api.get_last_sales.return_value = {
            "retval": 0,
            "sales": [
                {"invoice_id": 10, "date": "2023-12-31T23:59:59Z"},
                {"invoice_id": 11, "date": "2024-01-01T00:00:00Z"},
            ],
        }
        service.installed_at = datetime(2024, 1, 1, tzinfo=timezone.utc)
        service.purchase_manager = Mock()
        service.purchase_manager.get_pending_purchase_ids.return_value = []
        service.purchase_manager.is_purchase_processed.return_value = False
        service.failed_topics = {}
        service.process_new_purchase = AsyncMock()

        asyncio.run(service.check_new_purchases())

        service.process_new_purchase.assert_awaited_once_with(11)

    def test_pre_install_purchase_cannot_create_telegram_topic(self):
        service = object.__new__(BotService)
        service.installed_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
        service.telegram_bot = Mock()
        service.topic_manager = Mock()
        service.send_message_with_cooldown = AsyncMock()
        from purchase_manager import Purchase
        purchase = Purchase(123, 1, 2, "cart", "item", 10, "USD", 1, "2024-01-01T00:00:00Z", "", "e", "a", "1", "", "", "card", "", "now")

        self.assertFalse(asyncio.run(service.create_topic_for_purchase(purchase)))

        service.telegram_bot.create_topic.assert_not_called()
        service.send_message_with_cooldown.assert_not_awaited()

    def test_detailed_pre_install_purchase_is_not_staged_or_delivered(self):
        service = object.__new__(BotService)
        service.installed_at = datetime(2024, 1, 2, tzinfo=timezone.utc)
        service.failed_topics = {}
        service.ensure_ggsel_auth = AsyncMock(return_value=True)
        service.ggsel_api = Mock()
        service.ggsel_api.get_purchase_info.return_value = {
            "retval": 0,
            "content": {"purchase_date": "2024-01-01T00:00:00Z"},
        }
        service.purchase_manager = Mock()
        from purchase_manager import PurchaseManager
        service.purchase_manager.parse_purchase_response.side_effect = (
            lambda data, invoice_id: PurchaseManager.parse_purchase_response(
                service.purchase_manager, data, invoice_id
            )
        )
        service.create_topic_for_purchase = AsyncMock()

        asyncio.run(service.process_new_purchase(123))

        service.purchase_manager.add_purchase.assert_not_called()
        service.create_topic_for_purchase.assert_not_awaited()

    def test_review_is_recorded_only_after_side_effects_succeed(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        service = object.__new__(BotService)
        service.database = Database(str(Path(temp.name) / "state.db"))
        service.message_manager = MessageManager(service.database)
        service.processed_reviews = {}
        service.autoresponder = Mock()
        service.autoresponder.get_review_response.return_value = None
        service.send_message_with_cooldown = AsyncMock(return_value=False)
        review = {"id": "3", "invoice_id": 9, "type": "good", "info": "ok"}
        topic = {9: {"topic_id": 4}}

        self.loop.run_until_complete(service._process_reviews([review], topic, None))
        self.assertNotIn("3", service.processed_reviews)

        service.send_message_with_cooldown.return_value = True
        self.loop.run_until_complete(service._process_reviews([review], topic, None))
        self.assertEqual(service.processed_reviews["3"], "good:ok")

    def test_review_customer_reply_is_not_resent_after_restart_and_mirror_failure(self):
        temp = tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        db = Database(str(Path(temp.name) / "state.db"))
        review = {"id": "8", "invoice_id": 9, "type": "good", "info": "ok"}; topic = {9: {"topic_id": 4}}
        service = object.__new__(BotService); service.database = db; service.message_manager = MessageManager(db)
        service.processed_reviews = {}; service.autoresponder = Mock(); service.autoresponder.get_review_response.return_value = "thanks"
        service.ggsel_api = Mock(); service.ggsel_api.send_message.return_value = True
        service.send_message_with_cooldown = AsyncMock(side_effect=[True, False])
        self.loop.run_until_complete(service._process_reviews([review], topic, self.loop))

        restarted = object.__new__(BotService); restarted.database = Database(db.db_path); restarted.message_manager = MessageManager(restarted.database)
        restarted.processed_reviews = {}; restarted.autoresponder = service.autoresponder; restarted.ggsel_api = service.ggsel_api
        restarted.send_message_with_cooldown = AsyncMock(return_value=True)
        self.loop.run_until_complete(restarted._process_reviews([review], topic, self.loop))
        service.ggsel_api.send_message.assert_called_once_with(9, "thanks")
        self.assertEqual(restarted.processed_reviews["8"], "good:ok")


if __name__ == "__main__":
    unittest.main()
