import asyncio
import logging
from typing import Optional, Tuple
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReactionTypeEmoji
from telegram.ext import Application, MessageHandler, CallbackQueryHandler, CommandHandler, filters
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.request import HTTPXRequest
from config import Config
from locales import locales, _ 

class TelegramBot:
    def __init__(self, config: Config):
        self.config = config
        bot_options = {}
        if config.telegram_proxy_url:
            # Separate request objects are required by PTB: getUpdates uses a
            # dedicated client while all other Bot API calls use `request`.
            bot_options["request"] = HTTPXRequest(proxy=config.telegram_proxy_url)
            bot_options["get_updates_request"] = HTTPXRequest(proxy=config.telegram_proxy_url)
        self.bot = Bot(token=config.telegram_bot_token, **bot_options)
        self.group_id = config.telegram_group_id
        self.allowed_user_ids = frozenset(getattr(config, 'telegram_allowed_user_ids', frozenset()))
        self.application = None
        self.topic_message_handler = None
        self.callback_handler = None 
        self.general_message_handler = None
        self.history_handler = None 
        self.options_handler = None 
        self.review_handler = None 
        
    def set_topic_message_handler(self, h): self.topic_message_handler = h
    def set_callback_handler(self, h): self.callback_handler = h
    def set_general_message_handler(self, h): self.general_message_handler = h
    def set_history_handler(self, h): self.history_handler = h
    def set_options_handler(self, h): self.options_handler = h
    def set_review_handler(self, h): self.review_handler = h

    async def start(self):
        self.config.validate()
        if not self.allowed_user_ids:
            logging.warning(
                "TELEGRAM_ALLOWED_USER_IDS is empty; all Telegram operator actions will be denied"
            )
        try:
            # Use the same configured Bot so polling and ordinary API calls
            # cannot accidentally diverge in their proxy configuration.
            self.application = Application.builder().bot(self.bot).build()
            self.application.add_handler(CommandHandler("menu", self._handle_menu_command))
            self.application.add_handler(CommandHandler("history", self._handle_history_command))
            self.application.add_handler(CommandHandler("options", self._handle_options_command))
            self.application.add_handler(CommandHandler("review", self._handle_review_command))
            self.application.add_handler(CallbackQueryHandler(self._handle_callback))
            
            if self.topic_message_handler:
                topic_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & filters.IS_TOPIC_MESSAGE
                self.application.add_handler(MessageHandler(topic_filter, self._handle_topic_message))
            
            if self.general_message_handler:
                general_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & ~filters.IS_TOPIC_MESSAGE & ~filters.COMMAND
                self.application.add_handler(MessageHandler(general_filter, self._handle_general_message))

            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            return True
        except Exception as e:
            logging.exception("Telegram startup failed")
            # BotService historically ignores the boolean return value. Raising
            # prevents it from reporting a successful start and spawning jobs.
            raise RuntimeError(f"Telegram startup failed: {e}") from e

    async def send_message(self, text: str, topic_id: int, parse_mode: str = None, reply_markup = None) -> Tuple[bool, Optional[int]]:
        """Accepts parse_mode and reply_markup for rich notifications"""
        try:
            if topic_id in (-1, None):
                await self.bot.send_message(chat_id=self.group_id, text=text[:4000], parse_mode=parse_mode, reply_markup=reply_markup)
            else: 
                await self.bot.send_message(chat_id=self.group_id, message_thread_id=topic_id, text=text[:4000], parse_mode=parse_mode, reply_markup=reply_markup)
            return True, None
        except RetryAfter as e:
            return False, e.retry_after
        except (BadRequest, Forbidden, InvalidToken) as e:
            # Retrying malformed requests, revoked tokens, or inaccessible chats
            # creates an endless pending-message loop.
            logging.error(f"Permanent Telegram send error: {e}")
            return False, None
        except (TimedOut, NetworkError) as e:
            logging.warning(f"Retryable Telegram send error: {e}")
            return False, 60
        except TelegramError as e:
            logging.warning(f"Telegram send error (will retry): {e}")
            return False, 60
        except Exception as e:
            # Preserve the historical non-raising send boundary for callers.
            logging.exception("Unexpected Telegram send error")
            return False, 60

    async def send_message_with_keyboard(self, text: str, keyboard, topic_id: int = None):
        """Compatibility helper returning the same ``(success, retry_after)`` contract."""
        reply_markup = keyboard if isinstance(keyboard, InlineKeyboardMarkup) else InlineKeyboardMarkup(keyboard)
        return await self.send_message(text, topic_id, reply_markup=reply_markup)

    def _is_authorized(self, update: Update) -> bool:
        chat = getattr(update, 'effective_chat', None)
        user = getattr(update, 'effective_user', None)
        authorized = bool(
            chat and chat.id == self.group_id and user and user.id in self.allowed_user_ids
        )
        if not authorized:
            logging.warning(
                "Denied Telegram operator action (chat_id=%s, user_id=%s)",
                getattr(chat, 'id', None), getattr(user, 'id', None)
            )
        return authorized

    # ... keep the rest of the methods exactly as they are ...
    async def _handle_menu_command(self, update: Update, context):
        if not self._is_authorized(update): return
        keyboard = [
            [InlineKeyboardButton(_("btn_auto"), callback_data="auto_menu")],
            [InlineKeyboardButton(_("btn_balance"), callback_data="check_balance")],
            [InlineKeyboardButton(_("btn_stats"), callback_data="stats")],
            [InlineKeyboardButton(_("btn_lang"), callback_data="lang_toggle")],
            [InlineKeyboardButton(_("btn_close"), callback_data="close")]
        ]
        markup = InlineKeyboardMarkup(keyboard)
        try:
            if update.callback_query: await update.callback_query.edit_message_text(_("menu_title"), reply_markup=markup)
            else: await update.message.reply_text(_("menu_title"), reply_markup=markup)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            if update.callback_query: await update.callback_query.edit_message_text(_("menu_title"), reply_markup=markup)
            else: await update.message.reply_text(_("menu_title"), reply_markup=markup)

    async def _handle_callback(self, update: Update, context):
        query = update.callback_query
        if not self._is_authorized(update):
            try: await query.answer("Not authorized", show_alert=True)
            except TelegramError: pass
            return
        try: await query.answer()
        except TelegramError: pass
        if query.data == "close":
            await query.message.delete()
            return
        if query.data == "lang_toggle":
            locales.toggle()
            await self._handle_menu_command(update, context)
            return
        if self.callback_handler:
            await self.callback_handler(query.data, update, context)

    async def edit_message(self, message_id: int, chat_id: int, text: str, keyboard: list = None):
        try:
            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            await self.bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
            return True
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after)
            return await self.edit_message(message_id, chat_id, text, keyboard)
        except Exception: return False

    async def create_topic(self, topic_name: str) -> Tuple[Optional[int], Optional[int]]:
        try:
            result = await self.bot.create_forum_topic(chat_id=self.group_id, name=topic_name[:120])
            return result.message_thread_id, None
        except RetryAfter as e: return None, e.retry_after
        except Exception: return None, 60

    async def check_topic_exists(self, topic_id: int, topic_name: str) -> bool:
        try:
            await self.bot.edit_forum_topic(chat_id=self.group_id, message_thread_id=topic_id, name=topic_name[:120])
            return True
        except Exception as e:
            if any(err in str(e).lower() for err in ["deleted", "not found", "invalid"]):
                return False
            return True

    async def add_reaction(self, message_id: int, topic_id: int, emoji: str = "🔥"):
        try: await self.bot.set_message_reaction(chat_id=self.group_id, message_id=message_id, reaction=[ReactionTypeEmoji(emoji=emoji)])
        except: pass

    async def stop(self):
        if self.application:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()

    async def _handle_topic_message(self, update: Update, context):
        if self._is_authorized(update) and update.message and update.message.text and not update.message.from_user.is_bot:
            if self.topic_message_handler:
                self.topic_message_handler(update.message.message_thread_id, update.message.text, 
                                         update.message.from_user.username or "User", update.message.message_id)

    async def _handle_general_message(self, update: Update, context):
        if self._is_authorized(update) and update.message and update.message.text and not update.message.from_user.is_bot:
            if self.general_message_handler:
                await self.general_message_handler(update.message.text)

    async def _handle_history_command(self, update: Update, context):
        if self._is_authorized(update) and update.message.message_thread_id and self.history_handler:
            await self.history_handler(update.message.message_thread_id)

    async def _handle_options_command(self, update: Update, context):
        if self._is_authorized(update) and update.message.message_thread_id and self.options_handler:
            await self.options_handler(update.message.message_thread_id)

    async def _handle_review_command(self, update: Update, context):
        if self._is_authorized(update) and update.message.message_thread_id and self.review_handler:
            await self.review_handler(update.message.message_thread_id)
