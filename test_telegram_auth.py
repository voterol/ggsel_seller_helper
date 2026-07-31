import asyncio
import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

# Keep focused tests runnable when the optional Telegram dependency is absent.
try:
    from telegram.error import BadRequest, RetryAfter, TimedOut
except ImportError:
    telegram = types.ModuleType("telegram")

    class Bot:
        def __init__(self, token, **kwargs):
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
    telegram_request = types.ModuleType("telegram.request")

    class HTTPXRequest:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    telegram_request.HTTPXRequest = HTTPXRequest
    sys.modules["telegram.request"] = telegram_request

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
    message = SimpleNamespace(reply_text=AsyncMock())
    query = SimpleNamespace(
        data=data,
        answer=AsyncMock(),
        message=SimpleNamespace(chat=SimpleNamespace(id=chat_id), delete=AsyncMock()),
    )
    return SimpleNamespace(
        effective_user=SimpleNamespace(id=user_id),
        effective_chat=SimpleNamespace(id=chat_id),
        effective_message=message,
        message=message,
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

    def test_old_environment_without_proxy_remains_supported(self):
        env = {
            "GGSEL_SELLER_ID": "1",
            "GGSEL_API_KEY": "key",
            "TELEGRAM_BOT_TOKEN": "123:token",
            "TELEGRAM_GROUP_ID": "-1001",
            "TELEGRAM_ALLOWED_USER_IDS": "42",
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertIsNone(Config.from_env().telegram_proxy_url)

    def test_missing_or_blank_allowlist_starts_deny_by_default(self):
        base_env = {
            "GGSEL_SELLER_ID": "1",
            "GGSEL_API_KEY": "key",
            "TELEGRAM_BOT_TOKEN": "123:token",
            "TELEGRAM_GROUP_ID": "-1001",
        }
        for value in (None, "", "   "):
            with self.subTest(value=value):
                env = dict(base_env)
                if value is not None:
                    env["TELEGRAM_ALLOWED_USER_IDS"] = value
                with patch.dict(os.environ, env, clear=True):
                    config = Config.from_env()
                self.assertEqual(config.telegram_allowed_user_ids, frozenset())

    def test_non_positive_allowed_ids_are_rejected(self):
        for value in ("0", "-1", "42,-1"):
            with self.subTest(value=value), self.assertRaisesRegex(
                ValueError, "TELEGRAM_ALLOWED_USER_IDS"
            ):
                Config._allowed_user_ids(value)

    def test_http_and_socks5_proxy_urls_are_accepted(self):
        for value in (
            "http://proxy.example:8080",
            "socks5://user:p%40ss@proxy.example:1080",
        ):
            with self.subTest(value=value):
                Config._validate_telegram_proxy_url(value)

    def test_proxy_rejects_malformed_credentials_without_disclosure(self):
        secret = "secret%2"
        with self.assertRaises(ValueError) as raised:
            Config._validate_telegram_proxy_url(
                f"socks5://user:{secret}@proxy.example:1080"
            )
        self.assertNotIn(secret, str(raised.exception))


class TelegramBotTests(unittest.TestCase):
    def test_proxy_configures_both_ptb_transports(self):
        requests = []

        def request_factory(**kwargs):
            request = object()
            requests.append((request, kwargs))
            return request

        created_bot = SimpleNamespace(send_message=AsyncMock())
        with patch("telegram_bot.HTTPXRequest", side_effect=request_factory), patch(
            "telegram_bot.Bot", return_value=created_bot
        ) as bot_class:
            bot = TelegramBot(
                make_config(telegram_proxy_url="socks5://proxy.example:1080")
            )

        self.assertIs(bot.bot, created_bot)
        self.assertEqual(
            [kwargs for _request, kwargs in requests],
            [
                {"proxy": "socks5://proxy.example:1080"},
                {"proxy": "socks5://proxy.example:1080"},
            ],
        )
        bot_class.assert_called_once_with(
            token="123456:token",
            request=requests[0][0],
            get_updates_request=requests[1][0],
        )

    def test_application_uses_the_configured_bot(self):
        bot = TelegramBot(make_config())
        builder = unittest.mock.Mock()
        builder.bot.return_value = builder
        application = SimpleNamespace(
            add_handler=lambda _handler: None,
            initialize=AsyncMock(),
            start=AsyncMock(),
            updater=SimpleNamespace(start_polling=AsyncMock()),
        )
        builder.build.return_value = application
        application_class = SimpleNamespace(builder=lambda: builder)

        with patch("telegram_bot.Application", application_class), patch(
            "telegram_bot.CommandHandler", return_value=object()
        ), patch("telegram_bot.CallbackQueryHandler", return_value=object()):
            asyncio.run(bot.start())

        builder.bot.assert_called_once_with(bot.bot)

    def test_id_aliases_are_registered(self):
        bot = TelegramBot(make_config())
        builder = unittest.mock.Mock()
        application = SimpleNamespace(
            add_handler=lambda _handler: None,
            initialize=AsyncMock(),
            start=AsyncMock(),
            updater=SimpleNamespace(start_polling=AsyncMock()),
        )
        builder.bot.return_value = builder
        builder.build.return_value = application
        commands = []

        def command_handler(command, callback):
            commands.append((command, callback))
            return object()

        with patch("telegram_bot.Application", SimpleNamespace(builder=lambda: builder)), patch(
            "telegram_bot.CommandHandler", side_effect=command_handler
        ), patch("telegram_bot.CallbackQueryHandler", return_value=object()):
            asyncio.run(bot.start())

        self.assertIn(("id", "myid"), [command for command, _callback in commands])
        self.assertIn("start_sync", [command for command, _callback in commands])
        self.assertIn("stop_sync", [command for command, _callback in commands])

    def test_sync_commands_require_authorization_and_delegate(self):
        bot = TelegramBot(make_config())
        bot.start_sync_handler = AsyncMock(return_value="started")
        bot.stop_sync_handler = AsyncMock(return_value="stopped")
        authorized = make_update()

        asyncio.run(bot._handle_start_sync_command(authorized, None))
        asyncio.run(bot._handle_stop_sync_command(authorized, None))

        bot.start_sync_handler.assert_awaited_once_with()
        bot.stop_sync_handler.assert_awaited_once_with()
        self.assertEqual(
            [call.args[0] for call in authorized.effective_message.reply_text.await_args_list],
            ["started", "stopped"],
        )

        unauthorized = make_update(user_id=99)
        asyncio.run(bot._handle_start_sync_command(unauthorized, None))
        asyncio.run(bot._handle_stop_sync_command(unauthorized, None))
        unauthorized.effective_message.reply_text.assert_not_awaited()

    def test_id_command_reports_caller_in_configured_group_without_authorizing(self):
        bot = TelegramBot(make_config(telegram_allowed_user_ids=frozenset()))
        update = make_update(user_id=77)

        asyncio.run(bot._handle_id_command(update, None))

        update.effective_message.reply_text.assert_awaited_once_with(
            "Your Telegram user ID: 77"
        )
        self.assertEqual(bot.allowed_user_ids, frozenset())
        self.assertFalse(bot._is_authorized(update))

    def test_id_command_ignores_other_chats_and_missing_or_bot_users(self):
        bot = TelegramBot(make_config(telegram_allowed_user_ids=frozenset()))
        updates = [
            make_update(chat_id=-2002),
            make_update(),
            make_update(),
        ]
        updates[1].effective_user = None
        updates[2].effective_user.is_bot = True

        for update in updates:
            asyncio.run(bot._handle_id_command(update, None))
            update.effective_message.reply_text.assert_not_awaited()

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
        transport = AsyncMock(return_value=SimpleNamespace(message_id=7))
        object.__setattr__(bot.bot, "send_message", transport)

        result = asyncio.run(bot.send_message_with_keyboard("hello", [[object()]], None))

        self.assertEqual(result, (True, None))
        kwargs = bot.bot.send_message.await_args.kwargs
        self.assertNotIn("message_thread_id", kwargs)
        self.assertIsNotNone(kwargs["reply_markup"])

    def test_send_error_classification(self):
        bot = TelegramBot(make_config())
        object.__setattr__(bot.bot, "send_message", AsyncMock(side_effect=BadRequest("bad markup")))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, None))

        object.__setattr__(bot.bot, "send_message", AsyncMock(side_effect=TimedOut("timeout")))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, 60))

        object.__setattr__(bot.bot, "send_message", AsyncMock(side_effect=RetryAfter(3)))
        self.assertEqual(asyncio.run(bot.send_message("x", -1)), (False, 3))


if __name__ == "__main__":
    unittest.main()
