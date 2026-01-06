import asyncio
import logging
import re
from typing import Optional, Callable, Tuple, List
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReactionTypeEmoji
from telegram.ext import Application, MessageHandler, CallbackQueryHandler, CommandHandler, filters
from telegram.error import TelegramError
from config import Config

class TelegramBot:
    def __init__(self, config: Config):
        self.config = config
        self.bot = Bot(token=config.telegram_bot_token)
        self.group_id = config.telegram_group_id
        self.application = None
        self.topic_message_handler = None
        self.callback_handler = None  # Обработчик inline кнопок
        self.command_handler = None   # Обработчик команд
        self.general_message_handler = None  # Обработчик сообщений в General
        self.history_handler = None  # Обработчик команды /history
        self.options_handler = None  # Обработчик команды /options
        
    async def start(self):
        """Запуск бота"""
        try:
            bot_info = await self.bot.get_me()
            logging.info(f"Telegram бот: @{bot_info.username}")
            
            # Увеличенные таймауты для стабильности
            self.application = (
                Application.builder()
                .token(self.config.telegram_bot_token)
                .connect_timeout(30)
                .read_timeout(30)
                .write_timeout(30)
                .build()
            )
            
            # Команды
            self.application.add_handler(CommandHandler("menu", self._handle_menu_command))
            self.application.add_handler(CommandHandler("auto", self._handle_auto_command))
            self.application.add_handler(CommandHandler("history", self._handle_history_command))
            self.application.add_handler(CommandHandler("options", self._handle_options_command))
            
            # Callback для inline кнопок
            self.application.add_handler(CallbackQueryHandler(self._handle_callback))
            
            # Обработчик сообщений в топиках
            if self.topic_message_handler:
                topic_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & filters.IS_TOPIC_MESSAGE
                self.application.add_handler(MessageHandler(topic_filter, self._handle_topic_message))
            
            # Обработчик сообщений в General (для настроек)
            general_filter = filters.Chat(chat_id=self.group_id) & filters.TEXT & ~filters.IS_TOPIC_MESSAGE & ~filters.COMMAND
            self.application.add_handler(MessageHandler(general_filter, self._handle_general_message))
            
            await self.application.initialize()
            await self.application.start()
            await self.application.updater.start_polling()
            
            return True
        except Exception as e:
            logging.error(f"Ошибка запуска Telegram: {e}")
            return False
    
    async def stop(self):
        """Остановка"""
        if self.application:
            await self.application.updater.stop()
            await self.application.stop()
            await self.application.shutdown()
    
    def set_topic_message_handler(self, handler: Callable[[int, str, str, int], None]):
        self.topic_message_handler = handler
    
    def set_callback_handler(self, handler: Callable):
        self.callback_handler = handler
    
    def set_general_message_handler(self, handler: Callable):
        self.general_message_handler = handler
    
    def set_history_handler(self, handler: Callable):
        self.history_handler = handler
    
    def set_options_handler(self, handler: Callable):
        self.options_handler = handler
    
    async def _handle_menu_command(self, update: Update, context):
        """Команда /menu - главное меню"""
        if update.effective_chat.id != self.group_id:
            return
        
        keyboard = [
            [InlineKeyboardButton("⚙️ Автоответы", callback_data="auto_menu")],
            [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="close")]
        ]
        
        await update.message.reply_text(
            "🤖 Меню управления ботом",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    
    async def _handle_auto_command(self, update: Update, context):
        """Команда /auto - меню автоответов"""
        if update.effective_chat.id != self.group_id:
            return
        
        if self.callback_handler:
            await self.callback_handler("auto_menu", update, context)
    
    async def _handle_history_command(self, update: Update, context):
        """Команда /history - загрузка истории в топик"""
        if update.effective_chat.id != self.group_id:
            return
        
        topic_id = update.message.message_thread_id
        if not topic_id:
            try:
                await update.message.reply_text("❌ Используйте команду в топике")
            except:
                pass
            return
        
        if self.history_handler:
            try:
                await update.message.reply_text("⏳ Загружаю историю...")
            except:
                pass  # Игнорируем таймаут, продолжаем загрузку
            await self.history_handler(topic_id)
    
    async def _handle_options_command(self, update: Update, context):
        """Команда /options - показать опции покупки"""
        if update.effective_chat.id != self.group_id:
            return
        
        topic_id = update.message.message_thread_id
        if not topic_id:
            try:
                await update.message.reply_text("❌ Используйте команду в топике")
            except:
                pass
            return
        
        if self.options_handler:
            await self.options_handler(topic_id)
    
    async def _handle_callback(self, update: Update, context):
        """Обработка inline кнопок"""
        query = update.callback_query
        await query.answer()
        
        if query.message.chat.id != self.group_id:
            return
        
        data = query.data
        
        if data == "close":
            await query.message.delete()
            return
        
        if self.callback_handler:
            await self.callback_handler(data, update, context)
    
    async def _handle_topic_message(self, update: Update, context):
        """Обработка сообщений в топиках"""
        try:
            if update.message and update.message.text:
                if update.message.from_user and update.message.from_user.is_bot:
                    return
                
                text = update.message.text
                topic_id = update.message.message_thread_id
                message_id = update.message.message_id
                user = update.message.from_user
                username = user.username or user.first_name or "User"
                
                if self.topic_message_handler:
                    self.topic_message_handler(topic_id, text, username, message_id)
        except Exception as e:
            logging.error(f"Ошибка обработки топика: {e}")
    
    async def _handle_general_message(self, update: Update, context):
        """Обработка сообщений в General (для настроек)"""
        try:
            if update.message and update.message.text:
                if update.message.from_user and update.message.from_user.is_bot:
                    return
                
                text = update.message.text
                if self.general_message_handler:
                    await self.general_message_handler(text)
        except Exception as e:
            logging.error(f"Ошибка обработки General: {e}")
    
    async def create_topic(self, topic_name: str) -> Tuple[Optional[int], Optional[int]]:
        """Создание топика"""
        # Ограничение длины названия (Telegram лимит 128)
        if len(topic_name) > 120:
            topic_name = topic_name[:120] + "..."
        
        for attempt in range(self.config.max_retries):
            try:
                result = await self.bot.create_forum_topic(chat_id=self.group_id, name=topic_name)
                return result.message_thread_id, None
            except TelegramError as e:
                err = str(e).lower()
                if "not a forum" in err:
                    return -1, None
                elif "flood control" in err or "too many requests" in err:
                    return None, self._extract_cooldown(str(e))
                elif "bot was kicked" in err or "forbidden" in err:
                    return None, None
                elif "timed out" in err or "timeout" in err:
                    if attempt < self.config.max_retries - 1:
                        await asyncio.sleep(self.config.retry_delay)
                        continue
                return None, None
            except Exception:
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)
                    continue
                return None, None
        return None, None
    
    def _extract_cooldown(self, error: str) -> int:
        """Извлечение времени кулдауна"""
        try:
            match = re.search(r'retry in (\d+) seconds?', error.lower())
            if match:
                return int(match.group(1))
            match = re.search(r'(\d+) seconds?', error.lower())
            if match:
                return int(match.group(1))
            return 60
        except:
            return 60
    
    async def send_message(self, text: str, topic_id: int) -> Tuple[bool, Optional[int]]:
        """Отправка сообщения"""
        # Ограничение длины (Telegram лимит 4096)
        if len(text) > 4000:
            text = text[:4000] + "..."
        
        for attempt in range(self.config.max_retries):
            try:
                if topic_id == -1:
                    await self.bot.send_message(chat_id=self.group_id, text=text)
                else:
                    await self.bot.send_message(
                        chat_id=self.group_id, message_thread_id=topic_id, text=text
                    )
                return True, None
                
            except TelegramError as e:
                err = str(e).lower()
                if "flood control" in err or "too many requests" in err:
                    return False, self._extract_cooldown(str(e))
                elif "bot was kicked" in err or "forbidden" in err:
                    return False, None
                
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)
                    continue
                return False, None
            except Exception:
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(self.config.retry_delay)
                    continue
                return False, None
        return False, None
    
    async def send_message_with_keyboard(self, text: str, keyboard: list, topic_id: int = None) -> bool:
        """Отправка сообщения с inline клавиатурой"""
        try:
            reply_markup = InlineKeyboardMarkup(keyboard)
            if topic_id and topic_id != -1:
                await self.bot.send_message(
                    chat_id=self.group_id, 
                    message_thread_id=topic_id,
                    text=text, 
                    reply_markup=reply_markup
                )
            else:
                await self.bot.send_message(
                    chat_id=self.group_id, 
                    text=text, 
                    reply_markup=reply_markup
                )
            return True
        except Exception as e:
            logging.error(f"Ошибка отправки с клавиатурой: {e}")
            return False
    
    async def edit_message(self, message_id: int, chat_id: int, text: str, keyboard: list = None):
        """Редактирование сообщения"""
        try:
            reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                reply_markup=reply_markup
            )
            return True
        except Exception as e:
            logging.error(f"Ошибка редактирования: {e}")
            return False
    
    async def add_reaction(self, message_id: int, topic_id: int, emoji: str = "🔥") -> bool:
        """Добавление реакции на сообщение"""
        try:
            await self.bot.set_message_reaction(
                chat_id=self.group_id,
                message_id=message_id,
                reaction=[ReactionTypeEmoji(emoji=emoji)]
            )
            return True
        except Exception as e:
            logging.error(f"Ошибка реакции: {e}")
            return False
    
    async def get_forum_topics(self) -> List[dict]:
        """Получить список всех топиков в группе"""
        try:
            topics = []
            # Telegram API не даёт получить список топиков напрямую
            return topics
        except Exception as e:
            logging.error(f"Ошибка получения топиков: {e}")
            return []
    
    async def check_topic_exists(self, topic_id: int, topic_name: str = None) -> bool:
        """Проверить существует ли топик через edit_forum_topic"""
        try:
            # Пробуем отредактировать топик (ставим то же название)
            # Если топик удалён - получим ошибку
            name = topic_name or "💬"
            await self.bot.edit_forum_topic(
                chat_id=self.group_id,
                message_thread_id=topic_id,
                name=name
            )
            return True
        except Exception as e:
            err = str(e).lower()
            # Topic_deleted или not found = топик удалён
            if "deleted" in err or "not found" in err or "invalid" in err or "thread" in err or "message_thread_id" in err:
                return False
            # Другие ошибки (например rate limit) - считаем что топик существует
            return True
