import asyncio
import json
import os
import sqlite3
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from config import Config
from database import Database, Chat, Message
from ggsel_api import GGSelAPI
from telegram_bot import TelegramBot
from topic_manager import TopicManager
from message_manager import MessageManager
from purchase_manager import PurchaseManager, Purchase
from locales import locales, _
from autoresponder import AutoResponder

_executor = ThreadPoolExecutor(max_workers=20)

class BotService:
    def __init__(self, config: Config):
        self.config = config
        self.database = Database(config.database_path)
        self.ggsel_api = GGSelAPI(config)
        self.telegram_bot = TelegramBot(config)
        self.last_available_balance = 0.0
        self.usd_rub_rate = 79.14 # Default fallback rate
        
        # Managers now strictly use the SQLite Database - Zero JSON blocking!
        self.topic_manager = TopicManager(self.database)
        self.message_manager = MessageManager(self.database)
        self.purchase_manager = PurchaseManager(self.database)
        self.installed_at = self._parse_api_datetime(
            self.database.get_or_create_installation_time()
        )
        self.autoresponder = AutoResponder()
        
        self.running = False
        self.last_auth_time = None
        self.auth_interval = 15 * 60
        self.flood_control_until = None
        self.message_flood_control_until = None
        self.pending_messages = []
        self.pending_topics = []
        self.pending_history_loads = []
        self.awaiting_input = {}
        self.processed_reviews = {}
        self._review_lock = asyncio.Lock()
        self._sync_transition_lock = asyncio.Lock()
        self._sync_operation_lock = asyncio.Lock()
        self._customer_write_lock = asyncio.Lock()
        self.sync_enabled = self.database.get_setting("ggsel_sync_enabled") != "false"
        self.automatic_customer_messages_enabled = (
            self.database.get_setting("ggsel_automatic_customer_messages_enabled") != "false"
        )
        self._sync_enabled_event = asyncio.Event()
        if self.sync_enabled:
            self._sync_enabled_event.set()
        self.failed_topics = {}
        self.chat_locks = {}
        
        self._load_pending_topics()
        self._load_processed_reviews()

    @staticmethod
    def _parse_api_datetime(value: str) -> Optional[datetime]:
        if not isinstance(value, str) or not value.strip():
            return None
        normalized = value.strip()
        if normalized.endswith(("Z", "z")):
            normalized = normalized[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            # GGSel historically returns Moscow-local timestamps without an offset.
            parsed = parsed.replace(tzinfo=timezone(timedelta(hours=3)))
        return parsed.astimezone(timezone.utc)

    def _is_purchase_after_installation(self, purchase_date: str, invoice_id=None) -> bool:
        purchased_at = self._parse_api_datetime(purchase_date)
        if purchased_at is None:
            logging.warning(f"Ignoring purchase {invoice_id}: missing or invalid purchase date")
            return False
        if purchased_at < self.installed_at:
            logging.info(f"Ignoring pre-install purchase {invoice_id}")
            return False
        return True
        
    def get_main_menu_markup(self):
        keyboard = [
            [InlineKeyboardButton(_("btn_auto"), callback_data="auto_menu")],
            [InlineKeyboardButton(_("btn_balance"), callback_data="check_balance")],
            [InlineKeyboardButton(_("btn_stats"), callback_data="stats")],
            [InlineKeyboardButton(_("btn_lang"), callback_data="lang_toggle")],
            [InlineKeyboardButton(_("btn_close"), callback_data="close")]
        ]
        return InlineKeyboardMarkup(keyboard)

    # --- SQLite Replaced JSON Stores ---
    def _load_processed_reviews(self):
        try:
            with sqlite3.connect(self.database.db_path) as conn:
                cursor = conn.execute("SELECT review_id, hash FROM processed_reviews")
                self.processed_reviews = {row[0]: row[1] for row in cursor.fetchall()}
                if self.processed_reviews:
                    logging.info(f"Loaded {len(self.processed_reviews)} processed reviews from DB")
        except Exception as e:
            logging.error(f"Error loading reviews from DB: {e}")

    def _save_processed_review_db(self, review_id: str, hash_val: str) -> bool:
        try:
            with sqlite3.connect(self.database.db_path) as conn:
                conn.execute("INSERT OR REPLACE INTO processed_reviews (review_id, hash) VALUES (?, ?)", (review_id, hash_val))
            self.processed_reviews[review_id] = hash_val
            return True
        except Exception as e:
            logging.error(f"Error saving review to DB: {e}")
            return False

    def _get_review_effect_status(self, review_id: str, review_hash: str, effect: str) -> str:
        with sqlite3.connect(self.database.db_path) as conn:
            row = conn.execute(
                "SELECT status FROM review_effects WHERE review_id = ? AND review_hash = ? AND effect = ?",
                (review_id, review_hash, effect),
            ).fetchone()
        return row[0] if row else self.message_manager.PENDING

    def _set_review_effect_status(self, review_id: str, review_hash: str, effect: str, status: str) -> None:
        if status not in (self.message_manager.PENDING, self.message_manager.COMPLETED,
                          self.message_manager.PERMANENT_FAILURE):
            raise ValueError(f"Invalid review effect status: {status}")
        with sqlite3.connect(self.database.db_path) as conn:
            conn.execute(
                "INSERT INTO review_effects (review_id, review_hash, effect, status) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(review_id, review_hash, effect) DO UPDATE SET "
                "status = excluded.status, updated_at = CURRENT_TIMESTAMP",
                (review_id, review_hash, effect, status),
            )

    async def _send_customer_message(self, chat_id: int, text: str, executor=None, automatic: bool = False):
        """Return delivery result and its failure class from the same worker.

        Capturing last_failure in the worker avoids another concurrent API call
        overwriting the classification before the state transition is made.
        """
        if not hasattr(self, '_customer_write_lock'):
            self._customer_write_lock = asyncio.Lock()
        async with self._customer_write_lock:
            if not getattr(self, 'sync_enabled', True):
                return False, None
            if automatic and not getattr(self, 'automatic_customer_messages_enabled', True):
                return False, "suppressed"
            loop = asyncio.get_event_loop()
            def send():
                succeeded = self.ggsel_api.send_message(chat_id, text)
                return succeeded, getattr(self.ggsel_api, "last_failure", None)
            return await loop.run_in_executor(executor, send)

    @staticmethod
    def _is_terminal_api_failure(failure) -> bool:
        value = getattr(failure, "value", failure)
        return value in ("authentication", "permanent")

    async def _run_sync_operation(self, operation):
        async with self._sync_operation_lock:
            if self.running and self.sync_enabled:
                await operation()

    def _load_pending_topics(self):
        try:
            with sqlite3.connect(self.database.db_path) as conn:
                cursor = conn.execute("SELECT data FROM pending_topics")
                for row in cursor.fetchall():
                    item = json.loads(row[0])
                    purchase = Purchase(**item['purchase'])
                    self.pending_topics.append({
                        'purchase': purchase,
                        'timestamp': datetime.fromisoformat(item['timestamp']),
                        'skip_greeting': item.get('skip_greeting', False)
                    })
                if self.pending_topics:
                    logging.info(f"Loaded {len(self.pending_topics)} pending topics from DB")
        except Exception as e:
            logging.error(f"Error loading pending topics: {e}")

    def _save_pending_topics(self):
        try:
            with sqlite3.connect(self.database.db_path) as conn:
                conn.execute("DELETE FROM pending_topics")
                for item in self.pending_topics:
                    data_to_save = {
                        'purchase': item['purchase'].__dict__,
                        'timestamp': item['timestamp'].isoformat(),
                        'skip_greeting': item.get('skip_greeting', False)
                    }
                    conn.execute("INSERT INTO pending_topics (data) VALUES (?)", (json.dumps(data_to_save),))
        except Exception as e:
            logging.error(f"Error saving pending topics: {e}")

    async def start(self):
        """Fast Boot - Single User Mode"""
        logging.info("Starting GGSel bot...")
        
        self.telegram_bot.set_topic_message_handler(self.handle_topic_message)
        self.telegram_bot.set_callback_handler(self.handle_callback)
        self.telegram_bot.set_general_message_handler(self.handle_general_message)
        self.telegram_bot.set_history_handler(self.handle_history_command)
        self.telegram_bot.set_options_handler(self.handle_options_command)
        self.telegram_bot.set_review_handler(self.handle_review_command)
        self.telegram_bot.set_start_sync_handler(self.start_sync)
        self.telegram_bot.set_stop_sync_handler(self.pause_sync)
        self.telegram_bot.set_sync_nomessage_handler(self.start_sync_without_messages)
        
        await self.telegram_bot.start()
        self.running = True
        logging.info("Bot started and ready on Telegram (0.1s)!")
        
        asyncio.create_task(self._background_boot_sequence())
        
        # --- ADD THE BALANCE MONITOR TO THIS LIST ---
        tasks = [
            asyncio.create_task(self.monitor_messages()),
            asyncio.create_task(self.reauth_scheduler()),
            asyncio.create_task(self.purchase_checker()),
            asyncio.create_task(self.balance_monitor_loop()), 
            asyncio.create_task(self.update_exchange_rate_loop()) # <-- ADD THIS LINE
        ]
        
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            if self.running:
                await self.stop()

    async def _background_boot_sequence(self):
        """Background tasks loaded safely without blocking Telegram UI"""
        await self._sync_enabled_event.wait()
        if not self.running or not self.sync_enabled: return
        async with self._sync_operation_lock:
            if not self.running or not self.sync_enabled: return
            if not await self.ensure_ggsel_auth(): return
            await self.test_reviews_api()
            await self.process_pending_topics()
    
    async def test_reviews_api(self):
        try:
            loop = asyncio.get_event_loop()
            reviews_data = await loop.run_in_executor(_executor, lambda: self.ggsel_api.get_reviews(5))
            if not reviews_data:
                logging.warning("Reviews API: no data")
                return
            reviews = reviews_data.get('reviews', [])
            logging.info(f"Reviews API: received {len(reviews)} reviews")
        except Exception as e:
            logging.error(f"Error testing Reviews API: {e}")
    
    def handle_topic_message(self, topic_id: int, message_text: str, username: str, message_id: int):
        asyncio.create_task(self._handle_topic_message_async(topic_id, message_text, username, message_id))
    
    async def handle_general_message(self, text: str):
        chat_id = self.config.telegram_group_id
        if chat_id in self.awaiting_input:
            await self.handle_text_input(chat_id, text)
    
    async def _handle_topic_message_async(self, topic_id: int, message_text: str, username: str, message_id: int):
        try:
            if message_text.startswith('/'): return
            
            all_topics = self.topic_manager.get_all_topics()
            target_topic = next((info for info in all_topics.values() if info.get('topic_id') == topic_id), None)
            
            if not target_topic: return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.send_message_with_cooldown("⚠️ No invoice_id", topic_id)
                return
            
            try:
                result, _failure = await self._send_customer_message(invoice_id, message_text)
                if result:
                    await self.telegram_bot.add_reaction(message_id, topic_id, "🔥")
                else:
                    await self.send_message_with_cooldown("❌ Send error", topic_id)
            except Exception as e:
                logging.error(f"Error sending to chat {invoice_id}: {e}")
                await self.send_message_with_cooldown("❌ Send error", topic_id)
            
        except Exception as e:
            logging.error(f"Error processing message: {e}")
    
    async def ensure_ggsel_auth(self) -> bool:
        if not getattr(self, 'sync_enabled', True):
            return False
        current_time = datetime.now()
        if self.last_auth_time and current_time - self.last_auth_time < timedelta(seconds=self.auth_interval):
            return True
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(None, self.ggsel_api.login)
        if success:
            self.last_auth_time = current_time
            return True
        return False
    
    async def purchase_checker(self):
        while self.running:
            await self._sync_enabled_event.wait()
            if not self.running: break
            try:
                async with self._sync_operation_lock:
                    if self.running and self.sync_enabled:
                        await self.check_new_purchases()
            except Exception as e: logging.error(f"Purchase check error: {e}")
            await asyncio.sleep(30)
    
    async def check_new_purchases(self):
        if not getattr(self, 'sync_enabled', True): return
        if not await self.ensure_ggsel_auth(): return
        loop = asyncio.get_event_loop()
        # Use the largest practical API window and always merge durable
        # pending deliveries, so a transient topic failure cannot age out of
        # the historical "last 10" window.
        try: sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 100)
        except Exception as e:
            logging.debug(f"Sales fetch error: {e}")
            return
        
        if not sales_data or sales_data.get('retval') != 0: return
        
        invoice_ids = [
            sale.get('invoice_id')
            for sale in sales_data.get('sales', [])
            if self._is_purchase_after_installation(
                sale.get('date'), sale.get('invoice_id')
            )
        ]
        invoice_ids.extend(self.purchase_manager.get_pending_purchase_ids())
        for invoice_id in dict.fromkeys(invoice_ids):
            if not invoice_id: continue
            
            if invoice_id in self.failed_topics:
                if datetime.now() - self.failed_topics[invoice_id] < timedelta(minutes=10): continue
                del self.failed_topics[invoice_id]
            
            if not self.purchase_manager.is_purchase_processed(invoice_id):
                await self.process_new_purchase(invoice_id)
    
    async def process_new_purchase(self, invoice_id: int):
        try:
            if self.failed_topics.get(invoice_id) and datetime.now() - self.failed_topics[invoice_id] < timedelta(minutes=5): return
            if not await self.ensure_ggsel_auth():
                self.failed_topics[invoice_id] = datetime.now()
                return
            
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(None, self.ggsel_api.get_purchase_info, invoice_id)
            
            if not purchase_data:
                logging.warning(f"Failed to get info for purchase {invoice_id}, will retry")
                return
            
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase and not self._is_purchase_after_installation(
                purchase.purchase_date, purchase.invoice_id
            ):
                # Stop legacy pending rows from being retried forever.
                self.purchase_manager.mark_purchase_processed(purchase.invoice_id)
                return
            if purchase and self.purchase_manager.add_purchase(purchase):
                logging.info(f"Purchase: {purchase.invoice_id} - {purchase.buyer_email}")
                delivered = await self.create_topic_for_purchase(purchase)
                if delivered:
                    if not self.purchase_manager.mark_purchase_processed(purchase.invoice_id):
                        logging.error(f"Could not persist delivery state for purchase {purchase.invoice_id}")
        except Exception as e:
            self.failed_topics[invoice_id] = datetime.now()
            logging.error(f"Error processing purchase {invoice_id}: {e}")
    
    async def create_topic_for_purchase(self, purchase: Purchase, skip_greeting: bool = False):
        """Создание топика для покупки"""
        try:
            if not self._is_purchase_after_installation(
                purchase.purchase_date, purchase.invoice_id
            ):
                return False
            failed_time = self.failed_topics.get(purchase.invoice_id)
            if failed_time and datetime.now() - failed_time < timedelta(minutes=5):
                return
            
            if self.flood_control_until and datetime.now() < self.flood_control_until:
                logging.info(f"Flood control, добавляем в очередь: {purchase.invoice_id}")
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                return
            self.flood_control_until = None
            
            topic_key = f"purchase_{purchase.invoice_id}"
            existing_topic = self.topic_manager.get_all_topics().get(topic_key)
            
            customer_id = purchase.buyer_email or purchase.buyer_account or f"Customer_{purchase.invoice_id}"
            topic_name = f"💬 {purchase.invoice_id} | {customer_id}"
            
            if existing_topic:
                topic_id, cooldown = existing_topic.get('topic_id'), None
            else:
                await asyncio.sleep(2)
                topic_id, cooldown = await self.telegram_bot.create_topic(topic_name)
            
            if topic_id is not None:
                if not existing_topic:
                    self.topic_manager.add_topic_for_purchase(purchase, topic_id, topic_name)
                
                date_str = ""
                if purchase.purchase_date:
                    try:
                        dt = datetime.fromisoformat(purchase.purchase_date.replace('+03:00', ''))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = purchase.purchase_date
                
                # --- EXACT LAYOUT REPLICATION (NO DOUBLE EMOJIS) ---
                order_link = f"https://seller.ggsel.com/orders/{purchase.invoice_id}"
                header = _('noti_restored') if skip_greeting else _('noti_new_purchase')
                
                msg = f"{header}\n\n"
                
                safe_name = purchase.name.replace('<', '').replace('>', '').replace('&', '&amp;')
                msg += f"{_('noti_product')} {safe_name}\n"
                if getattr(purchase, 'item_id', 0): msg += f"{_('noti_item_id')} {purchase.item_id}\n"
                msg += f"{_('noti_invoice')} <a href='{order_link}'>{purchase.invoice_id}</a>\n"
                if date_str: msg += f"{_('noti_date')} {date_str}\n"
                
                # Prices Block
                msg += f"\n💰 <b>{_('noti_prices')}</b>\n"
                amt_rub = purchase.amount_rub if getattr(purchase, 'amount_rub', 0) > 0 else purchase.amount
                amt_usd = purchase.amount_usd if getattr(purchase, 'amount_usd', 0) > 0 else round(purchase.amount / 90.0, 2)
                msg += f"• RUB: {amt_rub}\n"
                msg += f"• USD: {amt_usd}\n"
                
                # Details Block
                state = purchase.invoice_state
                # Force "In progress" status default since auto-complete is off
                status_text = _('noti_status_processing') if state in (0, 1) else _('noti_status_done')
                profit = purchase.profit if getattr(purchase, 'profit', 0) > 0 else purchase.amount
                
                # --- NEW: Calculate the estimated USD profit dynamically ---
                if purchase.currency_type == 'RUB':
                    profit_usd = round(profit / self.usd_rub_rate, 2)
                    profit_str = f"{profit} RUB (~{profit_usd} USD)"
                elif purchase.currency_type == 'USD':
                    profit_str = f"{profit} USD"
                else:
                    profit_str = f"{profit} {purchase.currency_type}"
                
                msg += f"\n📊 <b>{_('noti_details')}</b>\n"
                msg += f"{_('noti_total')} {purchase.amount} {purchase.currency_type}\n"
                msg += f"{_('noti_status')} {status_text}\n"
                msg += f"{_('noti_profit')} {profit_str}\n"
                
                # Buyer Info Block
                msg += f"\n👤 <b>{_('noti_buyer_info')}</b>\n"
                if purchase.payment_method: msg += f"{_('noti_payment')} {purchase.payment_method}\n"
                if purchase.buyer_account: msg += f"{_('noti_account')} {purchase.buyer_account}\n"
                if purchase.buyer_email: msg += f"{_('noti_email')} {purchase.buyer_email}\n"
                if getattr(purchase, 'payment_aggregator', ''): msg += f"{_('noti_aggregator')} {purchase.payment_aggregator}\n"
                
                # Options Block
                options_text, options_list = await self.get_purchase_options_with_list(purchase.invoice_id)
                if options_text: 
                    safe_options = options_text.replace('<', '').replace('>', '').replace('&', '&amp;')
                    msg += f"\n⚙️ <b>{_('noti_options')}</b>\n{safe_options}\n"
                
                from telegram import InlineKeyboardMarkup, InlineKeyboardButton
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(_("btn_go_to_order"), url=order_link)]])
                
                notification_delivered = await self.send_message_with_cooldown(
                    msg, topic_id, parse_mode="HTML", reply_markup=keyboard,
                    purchase_invoice_id=purchase.invoice_id,
                )
                if not notification_delivered:
                    return False
                logging.info(f"Создан топик {topic_id} для {purchase.invoice_id}")
                
                if options_list and not skip_greeting and not existing_topic:
                    await self.process_csv_rules(purchase.invoice_id, topic_id, options_list)
                
                if not skip_greeting and not existing_topic and self.autoresponder.should_send_first_message():
                    greeting = self.autoresponder.get_first_message_text()
                    if greeting:
                        loop = asyncio.get_event_loop()
                        try:
                            sent, _failure = await self._send_customer_message(
                                purchase.invoice_id, greeting, automatic=True
                            )
                            if sent:
                                await self.send_message_with_cooldown(f"📤 {greeting}", topic_id)
                        except Exception as e:
                            logging.error(f"Ошибка отправки приветствия: {e}")
                return True
                    
            elif cooldown:
                self.flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
            else:
                self.failed_topics[purchase.invoice_id] = datetime.now()
            return False
                
        except Exception as e:
            self.failed_topics[purchase.invoice_id] = datetime.now()
            logging.error(f"Ошибка создания топика покупки {purchase.invoice_id}: {e}")
            return False
    
    async def load_chat_history(self, chat_ids: List[int], topic_id: int, force_reload: bool = False):
        try:
            all_messages = []
            loop = asyncio.get_event_loop()
            for chat_id in chat_ids:
                messages_data = await loop.run_in_executor(None, self.ggsel_api.get_chat_messages, chat_id)
                if messages_data:
                    for msg in messages_data:
                        msg['_chat_id'] = chat_id
                        all_messages.append(msg)
            
            if not all_messages and self.autoresponder.should_send_first_message():
                greeting = self.autoresponder.get_first_message_text()
                if greeting and chat_ids:
                    sent_any = False
                    for chat_id in chat_ids:
                        sent, _failure = await self._send_customer_message(
                            chat_id, greeting, automatic=True
                        )
                        sent_any = sent_any or sent
                    if sent_any:
                        await self.send_message_with_cooldown(greeting, topic_id)
                return
            
            if not all_messages: return
            
            def get_timestamp(msg):
                ts = msg.get('date_written', msg.get('timestamp', msg.get('created_at', msg.get('date', msg.get('time', '')))))
                if not ts: return datetime.min
                try: return datetime.fromisoformat(str(ts).replace('Z', '+00:00').replace('+03:00', ''))
                except: return datetime.min
            
            all_messages.sort(key=get_timestamp)
            logging.info(f"Loaded {len(all_messages)} messages for topic {topic_id}")
            
            for msg in all_messages:
                message_id = str(msg.get('id', ''))
                content = msg.get('message', msg.get('text', msg.get('content', '')))
                chat_id = msg.get('_chat_id')
                timestamp = get_timestamp(msg)
                
                if not content: continue
                if not force_reload and self.message_manager.is_message_processed(chat_id, message_id): continue
                
                message_text = f"📜 {content}" if force_reload else content
                if not force_reload:
                    if not await self.message_manager.add_processed_message(chat_id, message_id, content, timestamp):
                        continue
                    # History import mirrors messages but must not trigger a
                    # fresh customer reply when polling later sees the row.
                    self.message_manager.set_effect_status(
                        chat_id, message_id, "autoresponder", self.message_manager.COMPLETED
                    )
                await self.send_message_with_cooldown(message_text, topic_id, chat_id, message_id)
                await asyncio.sleep(0.5)
                
        except Exception as e: logging.error(f"History load error: {e}")
    
    async def process_pending_topics(self):
        if not self.pending_topics:
            await self.process_pending_history_loads()
            return
        
        if self.flood_control_until and datetime.now() < self.flood_control_until: return
        self.flood_control_until = None
        
        topics = self.pending_topics.copy()
        self.pending_topics.clear()
        self._save_pending_topics()
        
        logging.info(f"Processing {len(topics)} pending topics")
        
        for i, data in enumerate(topics):
            if data.get('timestamp') and (datetime.now() - data.get('timestamp')).total_seconds() < 30:
                self.pending_topics.append(data)
                continue
            
            delivered = await self.create_topic_for_purchase(data['purchase'], skip_greeting=data.get('skip_greeting', False))
            if delivered:
                self.purchase_manager.mark_purchase_processed(data['purchase'].invoice_id)
            if self.flood_control_until:
                remaining_topics = topics[i+1:]
                for t in remaining_topics:
                    if not any(p['purchase'].invoice_id == t['purchase'].invoice_id for p in self.pending_topics):
                        self.pending_topics.append(t)
                self._save_pending_topics()
                break
            await asyncio.sleep(3)
        await self.process_pending_history_loads()
    
    async def monitor_messages(self):
        logging.info("Starting message monitor")
        sync_counter = 0
        review_counter = 0
        if self.sync_enabled:
            asyncio.create_task(self._run_sync_operation(self.sync_topics_with_purchases))
            asyncio.create_task(self._run_sync_operation(self.check_new_reviews))
        
        while self.running:
            await self._sync_enabled_event.wait()
            if not self.running: break
            try:
                async with self._sync_operation_lock:
                    if not self.running or not self.sync_enabled:
                        continue
                    await self.process_pending_messages()
                    await self.process_pending_topics()

                    if not await self.ensure_ggsel_auth():
                        await asyncio.sleep(2)
                        continue

                    all_topics = self.topic_manager.get_all_topics()
                    purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
                    if purchase_topics: await self.check_topics_parallel(purchase_topics)

                    review_counter += 1
                    if review_counter >= 3:
                        review_counter = 0
                        asyncio.create_task(self._run_sync_operation(self.check_new_reviews))

                    sync_counter += 1
                    if sync_counter >= 43200:
                        sync_counter = 0
                        asyncio.create_task(self._run_sync_operation(self.sync_topics_with_purchases))

            except Exception as e: logging.error(f"Monitor error: {e}")
            await asyncio.sleep(2)
    
    async def check_topics_parallel(self, topics: Dict):
        if not topics: return
        semaphore = asyncio.Semaphore(10)
        
        async def check_with_semaphore(invoice_id: int, topic_id: int):
            async with semaphore:
                await self._check_single_chat(invoice_id, topic_id)
        
        tasks = [check_with_semaphore(t.get('invoice_id'), t.get('topic_id')) for t in topics.values() if t.get('topic_id') and t.get('invoice_id')]
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_single_chat(self, chat_id: int, topic_id: int):
        if chat_id not in self.chat_locks: self.chat_locks[chat_id] = asyncio.Lock()
        async with self.chat_locks[chat_id]:
            try: await self.check_chat_messages(chat_id, topic_id)
            except Exception as e: logging.error(f"Chat check error {chat_id}: {e}")
    
    async def check_chat_messages(self, chat_id: int, topic_id: int) -> bool:
        if not getattr(self, 'sync_enabled', True): return False
        try:
            loop = asyncio.get_event_loop()
            messages_data = await loop.run_in_executor(_executor, self.ggsel_api.get_chat_messages, chat_id)
            if not messages_data: return False
            has_new = False
            for msg_data in messages_data:
                if await self.process_single_message_check(chat_id, topic_id, msg_data): has_new = True
            return has_new
        except Exception as e:
            logging.error(f"Chat check error {chat_id}: {e}")
            return False
    
    async def process_single_message_check(self, chat_id: int, topic_id: int, msg_data: Dict) -> bool:
        try:
            message_id = str(msg_data.get('id', ''))
            content = msg_data.get('message', msg_data.get('text', msg_data.get('content', '')))
            timestamp_str = msg_data.get('date_written', msg_data.get('timestamp', msg_data.get('created_at', '')))
            if not message_id or not content: return False
            if self.message_manager.is_message_processed(chat_id, message_id): return False
            
            try: timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00').replace('+03:00', '')) if timestamp_str else datetime.now()
            except: timestamp = datetime.now()
            
            if await self.message_manager.add_processed_message(chat_id, message_id, content, timestamp):
                logging.info(f"New message in chat {chat_id}: {content[:50]}...")
                if self.message_manager.get_effect_status(chat_id, message_id, "telegram") == self.message_manager.PENDING:
                    await self.send_message_with_cooldown(content, topic_id, chat_id, message_id)

                try:
                    auto_result = self.autoresponder.find_response(content)
                    response_text = auto_result.get("response", "") if auto_result else ""
                    notify_group = bool(auto_result and auto_result.get("notify_group", False))
                    if self.message_manager.get_effect_status(chat_id, message_id, "autoresponder_plan") == self.message_manager.PENDING:
                        self.message_manager.set_effect_status(
                            chat_id, message_id, "autoresponder_mirror",
                            self.message_manager.PENDING if response_text else self.message_manager.COMPLETED,
                        )
                        self.message_manager.set_effect_status(
                            chat_id, message_id, "group_notification",
                            self.message_manager.PENDING if notify_group else self.message_manager.COMPLETED,
                        )
                        self.message_manager.set_effect_status(chat_id, message_id, "autoresponder_plan", self.message_manager.COMPLETED)
                    customer_status = self.message_manager.get_effect_status(chat_id, message_id, "autoresponder")
                    if customer_status == self.message_manager.PENDING:
                        if response_text:
                            reply_sent, failure = await self._send_customer_message(
                                chat_id, response_text, automatic=True
                            )
                            if reply_sent:
                                self.message_manager.set_effect_status(chat_id, message_id, "autoresponder", self.message_manager.COMPLETED)
                            elif failure == "suppressed":
                                self.message_manager.set_effect_status(chat_id, message_id, "autoresponder", self.message_manager.COMPLETED)
                                self.message_manager.set_effect_status(chat_id, message_id, "autoresponder_mirror", self.message_manager.COMPLETED)
                            elif self._is_terminal_api_failure(failure):
                                self.message_manager.set_effect_status(chat_id, message_id, "autoresponder", self.message_manager.PERMANENT_FAILURE)
                        else:
                            self.message_manager.set_effect_status(chat_id, message_id, "autoresponder", self.message_manager.COMPLETED)

                    customer_status = self.message_manager.get_effect_status(chat_id, message_id, "autoresponder")
                    mirror_status = self.message_manager.get_effect_status(chat_id, message_id, "autoresponder_mirror")
                    if mirror_status == self.message_manager.PENDING:
                        if response_text and customer_status == self.message_manager.COMPLETED:
                            await self.send_message_with_cooldown(response_text, topic_id, chat_id, message_id, effect="autoresponder_mirror")
                        elif not response_text or customer_status == self.message_manager.PERMANENT_FAILURE:
                            self.message_manager.set_effect_status(chat_id, message_id, "autoresponder_mirror", self.message_manager.COMPLETED)

                    if self.message_manager.get_effect_status(chat_id, message_id, "group_notification") == self.message_manager.PENDING:
                        if notify_group:
                            topic_info = next((info for info in self.topic_manager.get_all_topics().values() if info.get('topic_id') == topic_id), None)
                            notify_msg = auto_result.get("notify_text", "") or "🔔 Reply required!"
                            if topic_info: notify_msg += f"\n📧 {topic_info.get('email', 'N/A')}\n🆔 {topic_info.get('invoice_id', 'N/A')}"
                            await self.send_message_with_cooldown(notify_msg, topic_id, chat_id, message_id, effect="group_notification")
                        else:
                            self.message_manager.set_effect_status(chat_id, message_id, "group_notification", self.message_manager.COMPLETED)
                except Exception as e: logging.error(f"Auto-reply error: {e}")
                return True
            return False
        except Exception as e:
            logging.error(f"Message process error: {e}")
            return False
    
    async def stop(self):
        logging.info("Stopping bot...")
        self.running = False
        self._sync_enabled_event.set()
        current_task = asyncio.current_task()
        tasks = [t for t in asyncio.all_tasks() if t is not current_task and not t.done()]
        for task in tasks: task.cancel()
        if tasks: await asyncio.gather(*tasks, return_exceptions=True)
        await self.telegram_bot.stop()
        logging.info("Bot stopped")
    
    async def send_message_with_cooldown(self, text: str, topic_id: int, chat_id: int = None, message_id: str = None, parse_mode: str = None, reply_markup = None, purchase_invoice_id: int = None, effect: str = "telegram") -> bool:
        """Updated to support reply_markup for clickable order buttons"""
        try:
            # Message ids are scoped to a GGSel chat.
            if chat_id and message_id:
                if self.message_manager.get_effect_status(chat_id, message_id, effect) in (
                    self.message_manager.COMPLETED, self.message_manager.PERMANENT_FAILURE
                ):
                    return True

            if self.message_flood_control_until and datetime.now() < self.message_flood_control_until:
                if purchase_invoice_id is None or not any(
                    queued.get('purchase_invoice_id') == purchase_invoice_id
                    for queued in self.pending_messages
                ):
                    self.pending_messages.append({
                        'text': text, 'topic_id': topic_id, 'chat_id': chat_id,
                        'message_id': message_id, 'timestamp': datetime.now(),
                        'parse_mode': parse_mode, 'reply_markup': reply_markup,
                        'purchase_invoice_id': purchase_invoice_id,
                        'effect': effect,
                    })
                return False
            self.message_flood_control_until = None
            
            success, cooldown = await self.telegram_bot.send_message(text, topic_id, parse_mode=parse_mode, reply_markup=reply_markup)
            
            if success:
                if chat_id and message_id:
                    if effect == "telegram": self.message_manager.mark_message_sent(chat_id, message_id)
                    else: self.message_manager.set_effect_status(chat_id, message_id, effect, self.message_manager.COMPLETED)
                return True
                
            elif cooldown:
                self.message_flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                if purchase_invoice_id is None or not any(
                    queued.get('purchase_invoice_id') == purchase_invoice_id
                    for queued in self.pending_messages
                ):
                    self.pending_messages.append({
                        'text': text, 'topic_id': topic_id, 'chat_id': chat_id,
                        'message_id': message_id, 'timestamp': datetime.now(),
                        'parse_mode': parse_mode, 'reply_markup': reply_markup,
                        'purchase_invoice_id': purchase_invoice_id,
                        'effect': effect,
                    })
            elif chat_id and message_id:
                # A non-rate-limit Telegram rejection is terminal. Re-polling
                # cannot repair it and must not create a hot retry loop.
                self.message_manager.set_effect_status(
                    chat_id, message_id, effect, self.message_manager.PERMANENT_FAILURE
                )
            return False
                
        except Exception as e:
            logging.error(f"Send error: {e}")
            return False

    async def process_pending_messages(self):
        if not self.pending_messages: return
        if self.message_flood_control_until and datetime.now() < self.message_flood_control_until: return
        self.message_flood_control_until = None
        
        messages = self.pending_messages.copy()
        self.pending_messages.clear()
        
        for index, msg in enumerate(messages):
            success = await self.send_message_with_cooldown(
                msg['text'], msg['topic_id'], msg.get('chat_id'),
                msg.get('message_id'), msg.get('parse_mode'), msg.get('reply_markup'),
                msg.get('purchase_invoice_id'), msg.get('effect', 'telegram')
            )
            if success and msg.get('purchase_invoice_id') is not None:
                if not self.purchase_manager.mark_purchase_processed(msg['purchase_invoice_id']):
                    logging.error(
                        f"Could not persist queued purchase delivery {msg['purchase_invoice_id']}"
                    )
            if not success and self.message_flood_control_until:
                # The current message was re-queued by the send helper; retain
                # the untouched tail too instead of silently dropping it.
                self.pending_messages.extend(messages[index + 1:])
                break
            await asyncio.sleep(1)
    
    async def reauth_scheduler(self):
        while self.running:
            await asyncio.sleep(self.auth_interval)
            if self.running and self.sync_enabled:
                async with self._sync_operation_lock:
                    if self.running and self.sync_enabled:
                        await self.ensure_ggsel_auth()
    
    async def start_sync(self) -> str:
        async with self._sync_transition_lock:
            if self.sync_enabled and self.automatic_customer_messages_enabled:
                return "✅ Synchronization is already running."
            # Let writes already queued in no-message mode observe suppression
            # before opening admission for automatic messages again.
            async with self._customer_write_lock:
                self.database.set_settings({
                    "ggsel_sync_enabled": "true",
                    "ggsel_automatic_customer_messages_enabled": "true",
                })
                self.sync_enabled = True
                self.automatic_customer_messages_enabled = True
                self._sync_enabled_event.set()
            if self.running:
                asyncio.create_task(self._background_boot_sequence())
                asyncio.create_task(self._run_sync_operation(self.sync_topics_with_purchases))
                asyncio.create_task(self._run_sync_operation(self.check_new_reviews))
            logging.info("GGSel synchronization started by an operator")
            return "▶️ GGSel synchronization started."

    async def start_sync_without_messages(self) -> str:
        async with self._sync_transition_lock:
            already_active = self.sync_enabled and not self.automatic_customer_messages_enabled
            self.database.set_settings({
                "ggsel_sync_enabled": "true",
                "ggsel_automatic_customer_messages_enabled": "false",
            })
            self.sync_enabled = True
            self.automatic_customer_messages_enabled = False
            self._sync_enabled_event.set()
            async with self._customer_write_lock:
                pass
            if self.running and not already_active:
                asyncio.create_task(self._background_boot_sequence())
                asyncio.create_task(self._run_sync_operation(self.sync_topics_with_purchases))
                asyncio.create_task(self._run_sync_operation(self.check_new_reviews))
            if already_active:
                return "🔕 Synchronization without automatic customer messages is already active."
            return "🔕 Synchronization is running. Automatic customer messages are disabled."

    async def pause_sync(self) -> str:
        async with self._sync_transition_lock:
            if not self.sync_enabled:
                return "⏸ GGSel synchronization is already stopped."
            # Close admission, then wait for admitted sync work and customer
            # writes before acknowledging the pause.
            self.database.set_setting("ggsel_sync_enabled", "false")
            self.sync_enabled = False
            self._sync_enabled_event.clear()
            async with self._sync_operation_lock:
                async with self._customer_write_lock:
                    pass
            logging.info("GGSel synchronization stopped by an operator")
            return "⏸ GGSel synchronization stopped. The Telegram bot remains online."

    def stop_sync(self):
        """Stop the whole service; retained for the process signal handler."""
        self.running = False
        self._sync_enabled_event.set()

    def _safe_parse_idx(self, data: str, prefix: str) -> int:
        try: return int(data.replace(prefix, ""))
        except (ValueError, TypeError): return -1
    
    async def handle_callback(self, data: str, update, context):
        query = update.callback_query
        if data == "auto_menu_new":
            await self.send_auto_menu_new(update.effective_chat.id)
            return
        
        chat_id = query.message.chat.id
        message_id = query.message.message_id
        
        if data == "auto_menu": await self.show_auto_menu(chat_id, message_id)
        elif data == "check_balance":
            try:
                if not await self.ensure_ggsel_auth():
                    await self.telegram_bot.edit_message(query.message.message_id, query.message.chat.id, "❌ Auth Error", None)
                    return

                loop = asyncio.get_event_loop()
                res_data = await loop.run_in_executor(None, self.ggsel_api.get_balance_info)
                if not res_data:
                    raise RuntimeError("GGSel balance request failed")
                
                if res_data.get("retval") == 0:
                    content = res_data.get("content", {})
                    avail = float(content.get("amount_t_free") or 0.0)
                    hold = float(content.get("amount_t_lock") or 0.0)
                    total = avail + hold
                    
                    self.last_available_balance = avail
                    
                    balance_text = _("balance_header") + _("balance_body").format(
                        total=f"{total:.2f}", avail=f"{avail:.2f}", hold=f"{hold:.2f}", curr="USD"
                    )
                else:
                    balance_text = _("balance_error")

                keyboard = [[InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]]
                await self.telegram_bot.edit_message(query.message.message_id, query.message.chat.id, balance_text, keyboard)

            except Exception:
                logging.error("Manual balance check failed")
                keyboard = [[InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]]
                await self.telegram_bot.edit_message(query.message.message_id, query.message.chat.id, "⚠️ Error connecting to GGSel. They might be temporarily down.", keyboard)
        elif data == "main_menu": await self.telegram_bot._handle_menu_command(update, context)
        elif data == "auto_toggle": self.autoresponder.toggle_enabled(); await self.show_auto_menu(chat_id, message_id)
        elif data == "auto_first_toggle": self.autoresponder.toggle_first_message(); await self.show_auto_menu(chat_id, message_id)
        elif data == "auto_first_edit":
            self.awaiting_input[chat_id] = {"type": "first_message"}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_greeting"), [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_menu")]])
        elif data == "auto_triggers": await self.show_triggers_menu(chat_id, message_id)
        elif data == "auto_add_trigger":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase"}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_trigger_phrase"), [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_triggers")]])
        elif data == "auto_add_trigger_notify":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase", "notify_group": True}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_trigger_notify"), [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_triggers")]])
        elif data.startswith("auto_trigger_edit_"):
            idx = self._safe_parse_idx(data, "auto_trigger_edit_"); await self.show_trigger_edit_menu(chat_id, message_id, idx)
        elif data.startswith("auto_trigger_notify_"):
            idx = self._safe_parse_idx(data, "auto_trigger_notify_"); self.autoresponder.toggle_trigger_notify(idx); await self.show_trigger_edit_menu(chat_id, message_id, idx)
        elif data.startswith("auto_trigger_exact_"):
            idx = self._safe_parse_idx(data, "auto_trigger_exact_"); self.autoresponder.toggle_trigger_exact_match(idx); await self.show_trigger_edit_menu(chat_id, message_id, idx)
        elif data.startswith("auto_trigger_toggle_"):
            idx = self._safe_parse_idx(data, "auto_trigger_toggle_"); self.autoresponder.toggle_trigger(idx); await self.show_trigger_edit_menu(chat_id, message_id, idx)
        elif data.startswith("auto_trigger_phrase_"):
            idx = self._safe_parse_idx(data, "auto_trigger_phrase_"); self.awaiting_input[chat_id] = {"type": "edit_trigger_phrase", "trigger_idx": idx}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_edit_phrase"), [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]])
        elif data.startswith("auto_trigger_response_"):
            idx = self._safe_parse_idx(data, "auto_trigger_response_"); self.awaiting_input[chat_id] = {"type": "edit_trigger_response", "trigger_idx": idx}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_edit_answer"), [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]])
        elif data.startswith("auto_trigger_notifytext_"):
            idx = self._safe_parse_idx(data, "auto_trigger_notifytext_"); self.awaiting_input[chat_id] = {"type": "edit_trigger_notify_text", "trigger_idx": idx}
            trigger = self.autoresponder.get_trigger(idx)
            text_prompt = _("prompt_notify_text").replace("{current}", trigger.get('notify_text', '') if trigger else '(None)')
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"auto_trigger_edit_{idx}")]])
        elif data.startswith("auto_trigger_del_"):
            idx = self._safe_parse_idx(data, "auto_trigger_del_"); self.autoresponder.remove_trigger(idx); await self.show_triggers_menu(chat_id, message_id)
        elif data == "auto_reviews": await self.show_reviews_menu(chat_id, message_id)
        elif data == "auto_reviews_toggle": self.autoresponder.toggle_review_responses(); await self.show_reviews_menu(chat_id, message_id)
        elif data == "auto_reviews_good_toggle": self.autoresponder.toggle_good_review_response(); await self.show_reviews_menu(chat_id, message_id)
        elif data == "auto_reviews_bad_toggle": self.autoresponder.toggle_bad_review_response(); await self.show_reviews_menu(chat_id, message_id)
        elif data == "auto_reviews_good_edit":
            self.awaiting_input[chat_id] = {"type": "edit_good_review_text"}
            text_prompt = _("prompt_good_review").replace("{current}", self.autoresponder.get_good_review_text())
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_reviews")]])
        elif data == "auto_reviews_bad_edit":
            self.awaiting_input[chat_id] = {"type": "edit_bad_review_text"}
            text_prompt = _("prompt_bad_review").replace("{current}", self.autoresponder.get_bad_review_text())
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data="auto_reviews")]])
        elif data == "stats":
            topics = self.topic_manager.get_all_topics()
            purchases = len([k for k in topics if k.startswith('purchase_')])
            text = f"{_('stats_title')}\n\n{_('stats_topics')} {len(topics)}\n{_('stats_purchases')} {purchases}\n{_('stats_auto')} {'✅' if self.autoresponder.is_enabled() else '❌'}"
            await self.telegram_bot.edit_message(message_id, chat_id, text, [[InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]])
        elif data == "csv_menu": await self.show_csv_menu(chat_id, message_id)
        elif data == "csv_toggle": self.autoresponder.toggle_csv_mode(); await self.show_csv_menu(chat_id, message_id)
        elif data == "csv_add_rule":
            self.awaiting_input[chat_id] = {"type": "csv_option_name"}
            await self.telegram_bot.edit_message(message_id, chat_id, _("prompt_csv_option"), [[InlineKeyboardButton(_("btn_cancel"), callback_data="csv_menu")]])
        elif data.startswith("csv_rule_"): idx = self._safe_parse_idx(data, "csv_rule_"); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_toggle_"): idx = self._safe_parse_idx(data, "csv_toggle_"); self.autoresponder.toggle_csv_rule(idx); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_case_"): idx = self._safe_parse_idx(data, "csv_case_"); self.autoresponder.toggle_csv_rule_case_sensitive(idx); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_matchtype_"): idx = self._safe_parse_idx(data, "csv_matchtype_"); self.autoresponder.cycle_csv_rule_match_type(idx); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_value_"):
            idx = self._safe_parse_idx(data, "csv_value_"); self.awaiting_input[chat_id] = {"type": "csv_option_value", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            text_prompt = _("prompt_csv_value").replace("{current}", rule.get("option_value", "") if rule else "(Any)")
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]])
        elif data.startswith("csv_touser_"): idx = self._safe_parse_idx(data, "csv_touser_"); self.autoresponder.toggle_csv_rule_send_to_user(idx); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_totopic_"): idx = self._safe_parse_idx(data, "csv_totopic_"); self.autoresponder.toggle_csv_rule_send_to_topic(idx); await self.show_csv_rule_menu(chat_id, message_id, idx)
        elif data.startswith("csv_usermsg_"):
            idx = self._safe_parse_idx(data, "csv_usermsg_"); self.awaiting_input[chat_id] = {"type": "csv_user_message", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            text_prompt = _("prompt_csv_user").replace("{current}", rule.get("user_message", "") if rule else "(None)")
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]])
        elif data.startswith("csv_topicmsg_"):
            idx = self._safe_parse_idx(data, "csv_topicmsg_"); self.awaiting_input[chat_id] = {"type": "csv_topic_message", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            text_prompt = _("prompt_csv_topic").replace("{current}", rule.get("topic_message", "") if rule else "(None)")
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]])
        elif data.startswith("csv_name_"):
            idx = self._safe_parse_idx(data, "csv_name_"); self.awaiting_input[chat_id] = {"type": "csv_edit_name", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            text_prompt = _("prompt_csv_name").replace("{current}", rule.get("option_name", "") if rule else "")
            await self.telegram_bot.edit_message(message_id, chat_id, text_prompt, [[InlineKeyboardButton(_("btn_cancel"), callback_data=f"csv_rule_{idx}")]])
        elif data.startswith("csv_del_"): idx = self._safe_parse_idx(data, "csv_del_"); self.autoresponder.remove_csv_rule(idx); await self.show_csv_menu(chat_id, message_id)
    
    async def show_auto_menu(self, chat_id: int, message_id: int):
        enabled = self.autoresponder.is_enabled()
        first_enabled = self.autoresponder.is_first_message_enabled()
        triggers_count = len(self.autoresponder.get_triggers())
        review_enabled = self.autoresponder.is_review_responses_enabled()
        csv_enabled = self.autoresponder.is_csv_mode_enabled()
        
        text = f"{_('auto_title')}\n\n{_('auto_status')} {_('enabled') if enabled else _('disabled')}\n{_('auto_greeting')} {'✅' if first_enabled else '❌'}\n{_('auto_triggers')} {triggers_count}\n{_('auto_reviews')} {'✅' if review_enabled else '❌'}\n{_('auto_csv')} {'✅' if csv_enabled else '❌'} ({len(self.autoresponder.get_csv_rules())})"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="auto_toggle")],
            [InlineKeyboardButton(f"{_('btn_greeting')} {'✅' if first_enabled else '❌'}", callback_data="auto_first_toggle")],
            [InlineKeyboardButton(_("btn_greeting_text"), callback_data="auto_first_edit")],
            [InlineKeyboardButton(f"{_('btn_triggers')} ({triggers_count})", callback_data="auto_triggers")],
            [InlineKeyboardButton(f"{_('btn_reviews')} {'✅' if review_enabled else '❌'}", callback_data="auto_reviews")],
            [InlineKeyboardButton(f"{_('btn_csv')} {'✅' if csv_enabled else '❌'}", callback_data="csv_menu")],
            [InlineKeyboardButton(_("btn_back"), callback_data="main_menu")]
        ]
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)

    async def send_auto_menu_new(self, chat_id: int):
        await self.telegram_bot.send_message_with_keyboard(_('auto_title'), [[InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]], None)
         
    async def show_csv_menu(self, chat_id: int, message_id: int):
        enabled = self.autoresponder.is_csv_mode_enabled()
        rules = self.autoresponder.get_csv_rules()
        text = f"{_('csv_title')}\n\n{_('auto_status')} ✅ {_('enabled') if enabled else '❌ ' + _('disabled')}\n\n{_('csv_desc')}\n\n"
        
        if rules:
            text += f"📋 Rules ({len(rules)}):\n"
            for i, rule in enumerate(rules):
                status = "✅" if rule.get("enabled", True) else "❌"
                name = rule.get("option_name", "")[:15]
                text += f"{i+1}. {status} 📝 {name}\n"
        else: text += _('csv_rules_empty')
        
        keyboard = [[InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="csv_toggle")]]
        for i, rule in enumerate(rules):
            status = "✅" if rule.get("enabled", True) else "❌"
            name = rule.get("option_name", "")[:12]
            keyboard.append([InlineKeyboardButton(f"{status} {name}", callback_data=f"csv_rule_{i}"), InlineKeyboardButton(_("btn_delete"), callback_data=f"csv_del_{i}")])
        keyboard.append([InlineKeyboardButton(_("btn_add_rule"), callback_data="csv_add_rule")])
        keyboard.append([InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")])
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_csv_rule_menu(self, chat_id: int, message_id: int, idx: int):
        rule = self.autoresponder.get_csv_rule(idx)
        if not rule: return await self.show_csv_menu(chat_id, message_id)
        
        enabled = rule.get("enabled", True)
        option_name = rule.get("option_name", "")
        option_value = rule.get("option_value", "")
        send_to_user = rule.get("send_to_user", False)
        send_to_topic = rule.get("send_to_topic", True)
        
        text = f"{_('csv_rule_title')} #{idx+1}\n\n{_('csv_option')} {option_name}\n{_('auto_status')} {'✅' if enabled else '❌'}\n\n"
        
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data=f"csv_toggle_{idx}")],
            [InlineKeyboardButton(_("btn_edit_name"), callback_data=f"csv_name_{idx}")],
            [InlineKeyboardButton(_("btn_edit_value"), callback_data=f"csv_value_{idx}")],
            [InlineKeyboardButton(f"{_('csv_to_user')} {'✅' if send_to_user else '❌'}", callback_data=f"csv_touser_{idx}")],
        ]
        if send_to_user: keyboard.append([InlineKeyboardButton(_("btn_edit_user_msg"), callback_data=f"csv_usermsg_{idx}")])
        keyboard.append([InlineKeyboardButton(f"{_('csv_to_topic')} {'✅' if send_to_topic else '❌'}", callback_data=f"csv_totopic_{idx}")])
        if send_to_topic: keyboard.append([InlineKeyboardButton(_("btn_edit_topic_msg"), callback_data=f"csv_topicmsg_{idx}")])
        keyboard.append([InlineKeyboardButton(_("btn_delete"), callback_data=f"csv_del_{idx}"), InlineKeyboardButton(_("btn_back"), callback_data="csv_menu")])
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_triggers_menu(self, chat_id: int, message_id: int):
        triggers = self.autoresponder.get_triggers()
        text = f"{_('triggers_title')}\n\n"
        keyboard = []
        for i, trigger in enumerate(triggers):
            phrase = trigger.get('phrase', '')[:15]
            status = "✅" if trigger.get('enabled', True) else "❌"
            text += f"{i+1}. {status} \"{phrase}\"\n"
            keyboard.append([InlineKeyboardButton(f"{status} {phrase}", callback_data=f"auto_trigger_edit_{i}"), InlineKeyboardButton(_("btn_delete"), callback_data=f"auto_trigger_del_{i}")])
        if not triggers: text += _('triggers_empty')
        keyboard.append([InlineKeyboardButton(_("btn_add_trigger"), callback_data="auto_add_trigger"), InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")])
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_trigger_edit_menu(self, chat_id: int, message_id: int, idx: int):
        trigger = self.autoresponder.get_trigger(idx)
        if not trigger: return await self.show_triggers_menu(chat_id, message_id)
        
        enabled = trigger.get('enabled', True)
        notify = trigger.get('notify_group', False)
        exact_match = trigger.get('exact_match', False)
        
        text = f"{_('trigger_edit_title')} #{idx+1}\n\n{_('trigger_phrase')} {trigger.get('phrase', '')}\n{_('auto_status')} {'✅' if enabled else '❌'}\n"
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data=f"auto_trigger_toggle_{idx}")],
            [InlineKeyboardButton(_("btn_edit_phrase"), callback_data=f"auto_trigger_phrase_{idx}"), InlineKeyboardButton(_("btn_edit_answer"), callback_data=f"auto_trigger_response_{idx}")],
            [InlineKeyboardButton(f"🎯 {_('trigger_mode_exact') if exact_match else _('trigger_mode_contain')}", callback_data=f"auto_trigger_exact_{idx}")],
            [InlineKeyboardButton(f"{_('trigger_notify')} {'✅' if notify else '❌'}", callback_data=f"auto_trigger_notify_{idx}")],
            [InlineKeyboardButton(_("btn_delete"), callback_data=f"auto_trigger_del_{idx}"), InlineKeyboardButton(_("btn_back"), callback_data="auto_triggers")]
        ]
        if notify: keyboard.insert(4, [InlineKeyboardButton(_("btn_edit_notify_text"), callback_data=f"auto_trigger_notifytext_{idx}")])
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_reviews_menu(self, chat_id: int, message_id: int):
        enabled = self.autoresponder.is_review_responses_enabled()
        good_enabled = self.autoresponder.is_good_review_response_enabled()
        bad_enabled = self.autoresponder.is_bad_review_response_enabled()
        text = f"{_('reviews_title')}\n\n{_('auto_status')} {'✅' if enabled else '❌'}\n\n{_('reviews_good')} {'✅' if good_enabled else '❌'}\n\n{_('reviews_bad')} {'✅' if bad_enabled else '❌'}"
        keyboard = [
            [InlineKeyboardButton(f"{_('btn_turn_off') if enabled else _('btn_turn_on')}", callback_data="auto_reviews_toggle")],
            [InlineKeyboardButton(f"{_('reviews_good')} {'✅' if good_enabled else '❌'}", callback_data="auto_reviews_good_toggle"), InlineKeyboardButton(_("btn_good_edit"), callback_data="auto_reviews_good_edit")],
            [InlineKeyboardButton(f"{_('reviews_bad')} {'✅' if bad_enabled else '❌'}", callback_data="auto_reviews_bad_toggle"), InlineKeyboardButton(_("btn_bad_edit"), callback_data="auto_reviews_bad_edit")],
            [InlineKeyboardButton(_("btn_back"), callback_data="auto_menu")]
        ]
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def handle_text_input(self, chat_id: int, text: str):
        if chat_id not in self.awaiting_input: return False
        input_type = self.awaiting_input[chat_id].get("type")
        
        if input_type == "first_message":
            self.autoresponder.set_first_message_text(text)
            del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Greeting text updated:\n\n{text}", [[InlineKeyboardButton("◀️ Back", callback_data="auto_menu")]], None)
            return True
        elif input_type == "trigger_phrase":
            self.awaiting_input[chat_id] = {"type": "trigger_response", "phrase": text, "notify_group": self.awaiting_input[chat_id].get("notify_group", False)}
            await self.telegram_bot.send_message_with_keyboard(f"Trigger: \"{text}\"\n\n✏️ Now send the reply text:", [[InlineKeyboardButton("❌ Cancel", callback_data="auto_triggers")]], None)
            return True
        elif input_type == "trigger_response":
            phrase = self.awaiting_input[chat_id].get("phrase", "")
            notify_group = self.awaiting_input[chat_id].get("notify_group", False)
            idx = self.autoresponder.add_trigger(phrase, text, notify_group)
            del self.awaiting_input[chat_id]
            if notify_group:
                self.awaiting_input[chat_id] = {"type": "new_trigger_notify_text", "trigger_idx": idx}
                await self.telegram_bot.send_message_with_keyboard(f"✅ Trigger added!\n\n✏️ Send notification text (or skip):", [[InlineKeyboardButton("⏭ Skip", callback_data=f"auto_trigger_edit_{idx}")]], None)
            else:
                await self.telegram_bot.send_message_with_keyboard(f"✅ Trigger added!\n\nPhrase: \"{phrase}\"\nReply: \"{text}\"", [[InlineKeyboardButton("◀️ Back", callback_data="auto_triggers")]], None)
            return True
        elif input_type == "edit_trigger_phrase":
            idx = self.awaiting_input[chat_id].get("trigger_idx"); self.autoresponder.update_trigger(idx, phrase=text.lower()); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Phrase updated: \"{text}\"", [[InlineKeyboardButton("◀️ Back", callback_data=f"auto_trigger_edit_{idx}")]], None)
            return True
        elif input_type == "edit_trigger_response":
            idx = self.awaiting_input[chat_id].get("trigger_idx"); self.autoresponder.update_trigger(idx, response=text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Reply updated: \"{text}\"", [[InlineKeyboardButton("◀️ Back", callback_data=f"auto_trigger_edit_{idx}")]], None)
            return True
        elif input_type in ["edit_trigger_notify_text", "new_trigger_notify_text"]:
            idx = self.awaiting_input[chat_id].get("trigger_idx"); self.autoresponder.update_trigger(idx, notify_text=text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Notification text set!", [[InlineKeyboardButton("◀️ Back", callback_data="auto_triggers")]], None)
            return True
        elif input_type == "edit_good_review_text":
            self.autoresponder.set_good_review_text(text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Text for good reviews updated:\n\n{text}", [[InlineKeyboardButton("◀️ Back", callback_data="auto_reviews")]], None)
            return True
        elif input_type == "edit_bad_review_text":
            self.autoresponder.set_bad_review_text(text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Text for bad reviews updated:\n\n{text}", [[InlineKeyboardButton("◀️ Back", callback_data="auto_reviews")]], None)
            return True
        elif input_type == "csv_option_name":
            self.awaiting_input[chat_id] = {"type": "csv_topic_message_new", "option_name": text}
            await self.telegram_bot.send_message_with_keyboard(f"Option: {text}\n\n✏️ Enter message for the topic (or send - to skip):", [[InlineKeyboardButton("❌ Cancel", callback_data="csv_menu")]], None)
            return True
        elif input_type == "csv_topic_message_new":
            self.awaiting_input[chat_id].update({"type": "csv_user_message_new", "topic_message": text if text != "-" else ""})
            await self.telegram_bot.send_message_with_keyboard(f"✏️ Enter message for the user (or send - to skip):", [[InlineKeyboardButton("❌ Cancel", callback_data="csv_menu")]], None)
            return True
        elif input_type == "csv_user_message_new":
            option_name = self.awaiting_input[chat_id].get("option_name", "")
            topic_msg = self.awaiting_input[chat_id].get("topic_message", "")
            user_msg = text if text != "-" else ""
            self.autoresponder.add_csv_rule(option_name=option_name, case_sensitive=False, send_to_user=bool(user_msg), user_message=user_msg, send_to_topic=bool(topic_msg), topic_message=topic_msg)
            del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ CSV Rule added!\n\n📝 Option: {option_name}", [[InlineKeyboardButton("◀️ Back", callback_data="csv_menu")]], None)
            return True
        elif input_type == "csv_edit_name":
            idx = self.awaiting_input[chat_id].get("rule_idx"); self.autoresponder.update_csv_rule(idx, option_name=text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Option name updated: {text}", [[InlineKeyboardButton("◀️ Back", callback_data=f"csv_rule_{idx}")]], None)
            return True
        elif input_type == "csv_option_value":
            idx = self.awaiting_input[chat_id].get("rule_idx"); value = "" if text == "-" else text; self.autoresponder.update_csv_rule(idx, option_value=value); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Option value: {value}" if value else "✅ Value cleared", [[InlineKeyboardButton("◀️ Back", callback_data=f"csv_rule_{idx}")]], None)
            return True
        elif input_type == "csv_user_message":
            idx = self.awaiting_input[chat_id].get("rule_idx"); self.autoresponder.update_csv_rule(idx, user_message=text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ User message updated!", [[InlineKeyboardButton("◀️ Back", callback_data=f"csv_rule_{idx}")]], None)
            return True
        elif input_type == "csv_topic_message":
            idx = self.awaiting_input[chat_id].get("rule_idx"); self.autoresponder.update_csv_rule(idx, topic_message=text); del self.awaiting_input[chat_id]
            await self.telegram_bot.send_message_with_keyboard(f"✅ Topic message updated!", [[InlineKeyboardButton("◀️ Back", callback_data=f"csv_rule_{idx}")]], None)
            return True
        return False

    async def sync_topics_with_purchases(self):
        if not getattr(self, 'sync_enabled', True): return
        logging.info("Starting topic sync with purchases...")
        if not await self.ensure_ggsel_auth(): return
        await self.check_deleted_topics()
        
        loop = asyncio.get_event_loop()
        sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 30)
        if not sales_data or sales_data.get('retval') != 0: return
        
        api_invoice_ids = {
            sale.get('invoice_id')
            for sale in sales_data.get('sales', [])
            if sale.get('invoice_id') and self._is_purchase_after_installation(
                sale.get('date'), sale.get('invoice_id')
            )
        }
        existing_invoice_ids = {int(k.replace('purchase_', '')) for k in self.topic_manager.topics.keys() if k.startswith('purchase_')}
        
        missing_invoice_ids = api_invoice_ids - existing_invoice_ids
        if not missing_invoice_ids: return
        
        logging.info(f"Creating {len(missing_invoice_ids)} new topics...")
        
        for invoice_id in missing_invoice_ids:
            purchase_data = await loop.run_in_executor(None, self.ggsel_api.get_purchase_info, invoice_id)
            if not purchase_data: continue
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase:
                self.purchase_manager.add_purchase(purchase)
                await self.create_topic_for_purchase(purchase)
                if self.flood_control_until: break
            await asyncio.sleep(1.0)
    
    async def check_deleted_topics(self):
        all_topics = self.topic_manager.get_all_topics()
        purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
        if not purchase_topics: return
        
        for topic_key, topic_info in list(purchase_topics.items()):
            topic_id = topic_info.get('topic_id')
            topic_name = topic_info.get('topic_name', '💬')
            invoice_id = topic_info.get('invoice_id')
            if not topic_id or not topic_name: continue
            
            if not await self.telegram_bot.check_topic_exists(topic_id, topic_name):
                logging.info(f"Topic {topic_id} (invoice {invoice_id}) deleted, recreating...")
                self.topic_manager.remove_topic(topic_key)
                if invoice_id:
                    loop = asyncio.get_event_loop()
                    purchase_data = await loop.run_in_executor(None, self.ggsel_api.get_purchase_info, invoice_id)
                    if purchase_data:
                        purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
                        if purchase: await self.create_topic_for_purchase(purchase, skip_greeting=True)
            await asyncio.sleep(0.5)
    
    async def check_new_reviews(self):
        if not getattr(self, 'sync_enabled', True): return
        try:
            loop = asyncio.get_event_loop()
            invoice_to_topic = {int(info['invoice_id']): info for k, info in self.topic_manager.get_all_topics().items() if k.startswith('purchase_') and info.get('invoice_id')}
            await asyncio.gather(self._check_reviews_by_api(loop, invoice_to_topic), self._check_reviews_by_topics(loop, invoice_to_topic), return_exceptions=True)
        except Exception as e: logging.error(f"Review check error: {e}")

    async def _check_reviews_by_api(self, loop, invoice_to_topic: dict):
        try:
            all_reviews = []
            max_known_id = max([int(rid) for rid in self.processed_reviews.keys()] or [0])
            for page in range(1, 6):
                reviews_data = await loop.run_in_executor(_executor, lambda p=page: self.ggsel_api.get_reviews(50, page=p))
                if not reviews_data or not reviews_data.get('reviews'): break
                all_reviews.extend(reviews_data['reviews'])
                page_ids = [int(r.get('id', 0)) for r in reviews_data['reviews'] if r.get('id')]
                if page_ids and max(page_ids) <= max_known_id and all(str(rid) in self.processed_reviews for rid in page_ids): break
                await asyncio.sleep(0.3)
            
            if all_reviews:
                all_reviews.sort(key=lambda r: int(r.get('id', 0)), reverse=True)
                await self._process_reviews(all_reviews, invoice_to_topic, loop)
        except Exception as e: logging.error(f"API review check error: {e}")

    async def _check_reviews_by_topics(self, loop, invoice_to_topic: dict):
        try:
            semaphore = asyncio.Semaphore(10)
            async def check_single_invoice(invoice_id: int, topic_info: dict):
                async with semaphore:
                    try:
                        review = await loop.run_in_executor(_executor, lambda inv=invoice_id: self.ggsel_api.get_review_by_invoice(inv))
                        if review: await self._process_reviews([review], {invoice_id: topic_info}, loop)
                    except: pass
            tasks = [check_single_invoice(inv_id, info) for inv_id, info in invoice_to_topic.items()]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e: logging.error(f"Topic review check error: {e}")

    async def _process_reviews(self, reviews: list, invoice_to_topic: dict, loop):
        # API-wide and per-topic checks run concurrently.  Serialize the
        # check/side-effect/commit sequence to avoid duplicate review replies.
        lock = getattr(self, '_review_lock', None)
        if lock is None:
            lock = self._review_lock = asyncio.Lock()
        async with lock:
            await self._process_reviews_unlocked(reviews, invoice_to_topic, loop)

    async def _process_reviews_unlocked(self, reviews: list, invoice_to_topic: dict, loop):
        for review in reviews:
            review_id = str(review.get('id', ''))
            if not review_id: continue
            
            review_type = review.get('type', 'good')
            info = review.get('info', '') or review.get('text', '') or ''
            review_hash = f"{review_type}:{info}"
            
            old_hash = self.processed_reviews.get(review_id)
            if old_hash == review_hash: continue
            is_updated = old_hash is not None
            
            invoice_id = review.get('invoice_id')
            topic_info = invoice_to_topic.get(int(invoice_id)) if invoice_id else None
            topic_id = topic_info.get('topic_id') if topic_info else None
            if not topic_id: continue
            
            emoji = "👍" if review_type == 'good' else "👎"
            prefix = f"✏️ Review changed! {emoji}" if is_updated else f"{emoji} New review!"
            msg = f"{prefix}\n📊 Type: {'Positive' if review_type == 'good' else 'Negative'}\n"
            if review.get('name'): msg += f"📦 {review['name']}\n"
            if review.get('date'): msg += f"📅 {review['date']}\n"
            if info: msg += f"\n💬 {info}"
            
            notification_status = self._get_review_effect_status(review_id, review_hash, "telegram_notification")
            if notification_status == self.message_manager.PENDING:
                if await self.send_message_with_cooldown(msg, topic_id):
                    self._set_review_effect_status(review_id, review_hash, "telegram_notification", self.message_manager.COMPLETED)

            auto_response = self.autoresponder.get_review_response(review_type)
            customer_status = self._get_review_effect_status(review_id, review_hash, "customer_reply")
            if customer_status == self.message_manager.PENDING:
                if not auto_response:
                    self._set_review_effect_status(review_id, review_hash, "customer_reply", self.message_manager.COMPLETED)
                else:
                    try:
                        reply_sent, failure = await self._send_customer_message(
                            int(invoice_id), auto_response, _executor, automatic=True
                        )
                        if reply_sent:
                            self._set_review_effect_status(review_id, review_hash, "customer_reply", self.message_manager.COMPLETED)
                        elif failure == "suppressed":
                            self._set_review_effect_status(review_id, review_hash, "customer_reply", self.message_manager.COMPLETED)
                            self._set_review_effect_status(review_id, review_hash, "reply_mirror", self.message_manager.COMPLETED)
                        elif self._is_terminal_api_failure(failure):
                            self._set_review_effect_status(review_id, review_hash, "customer_reply", self.message_manager.PERMANENT_FAILURE)
                    except Exception as e:
                        logging.error(f"Review reply error: {e}")

            customer_status = self._get_review_effect_status(review_id, review_hash, "customer_reply")
            mirror_status = self._get_review_effect_status(review_id, review_hash, "reply_mirror")
            if mirror_status == self.message_manager.PENDING:
                if auto_response and customer_status == self.message_manager.COMPLETED:
                    if await self.send_message_with_cooldown(f"📤 {auto_response}", topic_id):
                        self._set_review_effect_status(review_id, review_hash, "reply_mirror", self.message_manager.COMPLETED)
                elif not auto_response or customer_status == self.message_manager.PERMANENT_FAILURE:
                    self._set_review_effect_status(review_id, review_hash, "reply_mirror", self.message_manager.COMPLETED)

            terminal = (self.message_manager.COMPLETED, self.message_manager.PERMANENT_FAILURE)
            required = ("telegram_notification", "customer_reply", "reply_mirror")
            if all(self._get_review_effect_status(review_id, review_hash, effect) in terminal for effect in required):
                try:
                    if self._save_processed_review_db(review_id, review_hash):
                        self._set_review_effect_status(review_id, review_hash, "db_completion", self.message_manager.COMPLETED)
                except Exception as e: logging.error(f"Review completion error: {e}")

    async def process_pending_history_loads(self):
        if not self.pending_history_loads: return
        loads = self.pending_history_loads.copy()
        self.pending_history_loads.clear()
        for item in loads:
            try:
                await self.load_chat_history(item['chat_ids'], item['topic_id'])
                await asyncio.sleep(1)
            except Exception as e: logging.error(f"History load error: {e}")

    async def handle_history_command(self, topic_id: int):
        try:
            target_topic = next((info for info in self.topic_manager.get_all_topics().values() if info.get('topic_id') == topic_id), None)
            if not target_topic: return await self.telegram_bot.send_message("❌ Topic not found in DB", topic_id)
            if not target_topic.get('invoice_id'): return await self.telegram_bot.send_message("❌ No invoice_id", topic_id)
            
            await self.telegram_bot.send_message("🔄 Loading history...", topic_id)
            await self.load_chat_history([target_topic['invoice_id']], topic_id, force_reload=True)
            await self.telegram_bot.send_message("✅ History loaded", topic_id)
        except Exception as e: logging.error(f"History load error: {e}")

    async def get_purchase_options(self, invoice_id: int) -> Optional[str]:
        text, _ = await self.get_purchase_options_with_list(invoice_id)
        return text
    
    async def get_purchase_options_with_list(self, invoice_id: int) -> tuple:
        try:
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(None, self.ggsel_api.get_purchase_info, invoice_id)
            if not purchase_data or purchase_data.get('retval') != 0: return None, []
            options = purchase_data.get('content', {}).get('options', [])
            if not options: return None, []
            
            lines = [f"• {opt.get('name')}: {opt.get('user_data')}" for opt in options if opt.get('name') and opt.get('user_data')]
            return "\n".join(lines) if lines else None, options
        except Exception as e: logging.error(f"Options fetch error: {e}"); return None, []
    
    async def process_csv_rules(self, invoice_id: int, topic_id: int, options: list):
        try:
            results = self.autoresponder.check_csv_options(options)
            if not results: return
            loop = asyncio.get_event_loop()
            
            for result in results:
                option_name, option_value = result.get("option_name", ""), result.get("option_value", "")
                if result.get("send_to_topic") and result.get("topic_message"):
                    topic_msg = result["topic_message"].replace("{option}", option_name).replace("{value}", option_value).replace("{sum}", option_value)
                    await self.send_message_with_cooldown(f"🎯 {topic_msg}", topic_id)
                
                if result.get("send_to_user") and result.get("user_message"):
                    user_msg = result["user_message"].replace("{option}", option_name).replace("{value}", option_value).replace("{sum}", option_value)
                    try:
                        sent, _failure = await self._send_customer_message(
                            invoice_id, user_msg, automatic=True
                        )
                        if sent:
                            await self.send_message_with_cooldown(f"📤 {user_msg}", topic_id)
                    except Exception as e: logging.error(f"CSV User msg error: {e}")
        except Exception as e: logging.error(f"CSV process error: {e}")
    
    async def handle_options_command(self, topic_id: int):
        try:
            target_topic = next((info for info in self.topic_manager.get_all_topics().values() if info.get('topic_id') == topic_id), None)
            if not target_topic: return await self.telegram_bot.send_message("❌ Topic not found", topic_id)
            if not target_topic.get('invoice_id'): return await self.telegram_bot.send_message("❌ No invoice_id", topic_id)
            
            options_text = await self.get_purchase_options(target_topic['invoice_id'])
            msg = f"⚙️ Purchase options #{target_topic['invoice_id']}:\n\n{options_text}" if options_text else f"ℹ️ No options for purchase #{target_topic['invoice_id']}"
            await self.telegram_bot.send_message(msg, topic_id)
        except Exception as e: logging.error(f"Options command error: {e}")

    async def handle_review_command(self, topic_id: int):
        try:
            target_topic = next((info for info in self.topic_manager.get_all_topics().values() if info.get('topic_id') == topic_id), None)
            if not target_topic: return await self.telegram_bot.send_message("❌ Topic not found", topic_id)
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id: return await self.telegram_bot.send_message("❌ No invoice_id", topic_id)
            
            await self.telegram_bot.send_message(f"🔍 Searching for review #{invoice_id}...", topic_id)
            loop = asyncio.get_event_loop()
            review = await loop.run_in_executor(_executor, lambda: self.ggsel_api.get_review_by_invoice(invoice_id))
            
            if review:
                emoji = "👍" if review.get('type') == 'good' else "👎"
                msg = f"{emoji} Review found!\n\n🆔 ID: {review.get('id')}\n"
                if review.get('name'): msg += f"📦 {review['name']}\n"
                if review.get('date'): msg += f"📅 {review['date']}\n"
                msg += f"📝 Type: {review.get('type')}\n"
                if review.get('info'): msg += f"\n💬 {review['info']}"
                await self.telegram_bot.send_message(msg, topic_id)
            else:
                await self.telegram_bot.send_message(f"ℹ️ Review for #{invoice_id} not found", topic_id)
        except Exception as e: logging.error(f"Review command error: {e}")

    def get_balance_markup(self):
        return InlineKeyboardMarkup([[InlineKeyboardButton(_("btn_refresh"), callback_data="check_balance")], [InlineKeyboardButton(_("btn_back"), callback_data="main_menu")]])

    async def show_balance(self, update, context):
        query = update.callback_query
        try:
            api = GGSelAPI(self.config)
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, api.get_balance_info)
            if not result:
                raise RuntimeError("GGSel balance request failed")
            content = result["content"]
            
            avail, hold = float(content.get("amount_t_free") or 0.0), float(content.get("amount_t_lock") or 0.0)
            text = f"{_('balance_header')}{_('balance_body').format(total=f'{avail+hold:.2f}', avail=f'{avail:.2f}', hold=f'{hold:.2f}', curr='USD')}"
            await self.telegram_bot.edit_message(query.message.message_id, query.message.chat.id, text, self.get_balance_markup().inline_keyboard)
        except Exception:
            logging.error("Balance request failed")
            await self.telegram_bot.edit_message(query.message.message_id, query.message.chat.id, _("balance_error"), self.get_main_menu_markup().inline_keyboard)

    async def balance_monitor_loop(self):
        """Continuous background loop for checking balance changes (increases and decreases)"""
        logging.info("Starting balance monitor loop (60s interval)...")
        self.last_available_balance = None 
        
        import httpx
        
        while self.running:
            await self._sync_enabled_event.wait()
            if not self.running: break
            await asyncio.sleep(60)
            try:
                async with self._sync_operation_lock:
                    if not self.running or not self.sync_enabled:
                        continue
                    if not await self.ensure_ggsel_auth():
                        continue

                    loop = asyncio.get_event_loop()
                    res_data = await loop.run_in_executor(None, self.ggsel_api.get_balance_info)
                    if not res_data:
                        continue

                    content = res_data.get("content", {})
                    current_avail = float(content.get("amount_t_free") or 0.0)
                    current_hold = float(content.get("amount_t_lock") or 0.0)
                    current_total = current_avail + current_hold

                    if self.last_available_balance is None:
                        self.last_available_balance = current_avail
                        continue

                    if current_avail != self.last_available_balance:
                        diff = current_avail - self.last_available_balance
                        current_time = datetime.now().strftime("%d.%m.%Y %H:%M:%S")

                        if diff > 0:
                            header = f"📈 **BALANCE INCREASE**\n🟢 **+{diff:.2f} USD**"
                        else:
                            header = f"📉 **BALANCE DECREASE**\n🔴 **{diff:.2f} USD**"

                        alert = (
                            f"{header}\n"
                            f"💰 **Current balance:**\n"
                            f"• Available: `{current_avail:.2f} USD`\n"
                            f"• Blocked: `{current_hold:.2f} USD`\n"
                            f"• Total: `{current_total:.2f} USD`\n"
                            f"📊 Previous balance: `{self.last_available_balance:.2f} USD`\n"
                            f"🕒 Time: `{current_time}`"
                        )

                        await self.telegram_bot.send_message(alert, -1, parse_mode="Markdown")
                        self.last_available_balance = current_avail
                
            except Exception:
                # Silently pass connection drops so it doesn't spam the logs
                pass
                
    async def update_exchange_rate_loop(self):
        """Fetches the official CBR exchange rate every 12 hours"""
        logging.info("Starting Exchange Rate updater...")
        import httpx
        while self.running:
            try:
                # Free public API for Russian Central Bank daily rates
                async with httpx.AsyncClient(timeout=10.0) as client:
                    resp = await client.get("https://www.cbr-xml-daily.ru/daily_json.js")
                    if resp.status_code == 200:
                        data = resp.json()
                        raw_cbr_rate = float(data['Valute']['USD']['Value'])
                        
                        # Add GGSel's ~1.3% hidden conversion markup
                        self.usd_rub_rate = raw_cbr_rate * 1.013 
                        logging.info(f"✅ USD/RUB Rate updated: {self.usd_rub_rate:.2f} (CBR: {raw_cbr_rate:.2f})")
            except Exception as e:
                logging.warning(f"⚠️ Failed to fetch exchange rate, using fallback: {e}")
            
            # Sleep for 12 hours before checking again
            await asyncio.sleep(43200)
