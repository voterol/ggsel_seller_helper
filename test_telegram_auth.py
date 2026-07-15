import asyncio
import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

# Keep focused tests runnable when the optional Telegram dependency is absent.
try:
    from telegram.error import BadRequest, RetryAfter, TimedOut
except ImportError:
    telegram = types.ModuleType("telegram")

    class Bot:
        def __init__(self, token):
            self.token = token

    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard):
            self.inline_keyboard = inline_keyboard

    class TelegramError(Exception):
        pass

    class BadRequest(TelegramError):
        pass

    class Forbidden(TelegramError):
        pass

    class InvalidToken(TelegramError):
        pass

    class NetworkError(TelegramError):
        pass

    class TimedOut(NetworkError):
        pass

    class RetryAfter(TelegramError):
        def __init__(self, retry_after):
            super().__init__(str(retry_after))
            self.retry_after = retry_after

    telegram.Bot = Bot
    telegram.Update = telegram.InlineKeyboardButton = telegram.ReactionTypeEmoji = object
    telegram.InlineKeyboardMarkup = InlineKeyboardMarkup
    telegram_error = types.ModuleType("telegram.error")
    for cls in (TelegramError, BadRequest, Forbidden, InvalidToken, NetworkError, TimedOut, RetryAfter):
        setattr(telegram_error, cls.__name__, cls)
    telegram_ext = types.ModuleType("telegram.ext")
    for name in ("Application", "MessageHandler", "CallbackQueryHandler", "CommandHandler", "filters"):
        setattr(telegram_ext, name, object)
    sys.modules["telegram"] = telegram
    sys.modules["telegram.error"] = telegram_error
    sys.modules["telegram.ext"] = telegram_ext

from config import Config
from telegram_bot import TelegramBot


def make_config(**overrides):
    values = dict(
        ggsel_seller_id=1,
        ggsel_api_key="key",
        telegram_bot_token="123456:token",
        telegram_group_id=-1001,
        telegram_allowed_user_ids=frozenset({42}),
    )
    values.update(overrides)
    return Config(**values)


def make_update(user_id=42, chat_id=-1001, data="action"):
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        message=SimpleNamespace(chat=SimpleNamespace(id=chat_id), delete=AsyncMock()),
    )
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_chat=SimpleNamespace(id=chat_id),
        callback_query=query,
    )


class ConfigTests(unittest.TestCase):
    def test_allowed_ids_are_strictly_parsed(self):
        self.assertEqual(Config._allowed_user_ids("12, 34 56"), frozenset({12, 34, 56}))
        with self.assertRaisesRegex(ValueError, "TELEGRAM_ALLOWED_USER_IDS"):
            Config._allowed_user_ids("12,nope")

    def test_from_env_fails_with_named_missing_setting(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ValueError, "GGSEL_SELLER_ID"):
                Config.from_env()


class TelegramBotTests(unittest.TestCase):
    def test_callback_is_denied_by_default_even_in_allowed_group(self):
        bot = TelegramBot(make_config(telegram_allowed_user_ids=frozenset()))
        bot.callback_handler = AsyncMock()
        update = make_update()

        asyncio.run(bot._handle_callback(update, None))

        bot.callback_handler.assert_not_awaited()
        update.callback_query.answer.assert_awaited_once_with("Not authorized", show_alert=True)

    def test_delegated_callback_is_answered_once(self):
        bot = TelegramBot(make_config())
        bot.callback_handler = AsyncMock()
        update = make_update()

        asyncio.run(bot._handle_callback(update, None))

        bot.callback_handler.assert_awaited_once()
        update.callback_query.answer.assert_awaited_once_with()

    def test_send_message_and_keyboard_contracts(self):
        bot = TelegramBot(make_config())
        bot.bot.send_message = AsyncMock(return_value=SimpleNamespace(message_id=7))

        result = asyncio.run(bot.send_message_with_keyboard("hello", [[object()]], None))

        self.assertEqual(result, (True, None))
        kwargs = bot.bot.send_message.await_args.kwargs
        self.assertNotIn("message_thread_id", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    def test_send_error_classification(self):
        bot = TelegramBot(make_config())
        bot.bot.send_message = AsyncMock(side_effect=BadRequest("bad markup"))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, None))

        bot.bot.send_message = AsyncMock(side_effect=TimedOut("timeout"))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, 60))

        bot.bot.send_message = AsyncMock(side_effect=RetryAfter(3))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, 3))


if __name__ == "__main__":
    unittest.main()
