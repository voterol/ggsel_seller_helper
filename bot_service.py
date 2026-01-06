import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from telegram import InlineKeyboardButton
from config import Config
from database import Database, Chat, Message
from ggsel_api import GGSelAPI
from telegram_bot import TelegramBot
from topic_manager import TopicManager
from order_manager import OrderManager, Order
from message_manager import MessageManager
from purchase_manager import PurchaseManager, Purchase
from autoresponder import AutoResponder

class BotService:
    def __init__(self, config: Config):
        self.config = config
        self.database = Database(config.database_path)
        self.ggsel_api = GGSelAPI(config)
        self.telegram_bot = TelegramBot(config)
        self.topic_manager = TopicManager()
        self.order_manager = OrderManager()
        self.message_manager = MessageManager()
        self.purchase_manager = PurchaseManager()
        self.autoresponder = AutoResponder()
        self.running = False
        self.last_auth_time = None
        self.auth_interval = 15 * 60
        self.flood_control_until = None
        self.message_flood_control_until = None
        self.pending_messages = []
        self.pending_topics = []
        self.pending_topics_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pending_topics.json")
        self.pending_history_loads = []  # Очередь загрузки истории сообщений
        self.awaiting_input = {}  # {chat_id: {"type": "...", "data": ...}}
        self.processed_reviews = set()  # Обработанные отзывы
        self.connection_was_down = False  # Флаг для отслеживания восстановления соединения
        
        # Hot/Cold система - последние 25 топиков горячие
        self.hot_topics_max = 25
        
        self._load_pending_topics()
    
    def _load_pending_topics(self):
        """Загрузка отложенных топиков из файла"""
        try:
            if os.path.exists(self.pending_topics_file):
                with open(self.pending_topics_file, 'r') as f:
                    data = json.load(f)
                    for item in data:
                        # Восстанавливаем Purchase объект
                        purchase = Purchase(
                            invoice_id=item['invoice_id'],
                            item_id=item.get('item_id', 0),
                            content_id=item.get('content_id', 0),
                            cart_uid=item.get('cart_uid', ''),
                            name=item.get('name', ''),
                            amount=item.get('amount', 0),
                            currency_type=item.get('currency_type', ''),
                            invoice_state=item.get('invoice_state', 0),
                            purchase_date=item.get('purchase_date', ''),
                            date_pay=item.get('date_pay', ''),
                            buyer_email=item.get('buyer_email', ''),
                            buyer_account=item.get('buyer_account', ''),
                            buyer_phone=item.get('buyer_phone', ''),
                            buyer_ip=item.get('buyer_ip', ''),
                            payment_method=item.get('payment_method', ''),
                            processed_at=item.get('processed_at', '')
                        )
                        self.pending_topics.append({
                            'purchase': purchase, 
                            'timestamp': datetime.now(),
                            'skip_greeting': item.get('skip_greeting', False)
                        })
                    if self.pending_topics:
                        logging.info(f"Загружено {len(self.pending_topics)} отложенных топиков")
        except Exception as e:
            logging.error(f"Ошибка загрузки отложенных топиков: {e}")
    
    def _save_pending_topics(self):
        """Сохранение отложенных топиков в файл"""
        try:
            data = []
            for item in self.pending_topics:
                p = item['purchase']
                data.append({
                    'invoice_id': p.invoice_id,
                    'item_id': p.item_id,
                    'content_id': p.content_id,
                    'cart_uid': p.cart_uid,
                    'name': p.name,
                    'amount': p.amount,
                    'currency_type': p.currency_type,
                    'invoice_state': p.invoice_state,
                    'purchase_date': p.purchase_date,
                    'date_pay': p.date_pay,
                    'buyer_email': p.buyer_email,
                    'buyer_account': p.buyer_account,
                    'buyer_phone': p.buyer_phone,
                    'buyer_ip': p.buyer_ip,
                    'payment_method': p.payment_method,
                    'processed_at': p.processed_at,
                    'skip_greeting': item.get('skip_greeting', False)
                })
            with open(self.pending_topics_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            logging.error(f"Ошибка сохранения отложенных топиков: {e}")
        
    async def start(self):
        """Запуск бота"""
        logging.info("Запуск GGSel бота...")
        
        if self.config.orders_group_id:
            self.telegram_bot.set_order_message_handler(self.handle_order_message)
        self.telegram_bot.set_topic_message_handler(self.handle_topic_message)
        self.telegram_bot.set_callback_handler(self.handle_callback)
        self.telegram_bot.set_general_message_handler(self.handle_general_message)
        self.telegram_bot.set_history_handler(self.handle_history_command)
        self.telegram_bot.set_options_handler(self.handle_options_command)
        
        await self.telegram_bot.start()
        
        if not await self.ensure_ggsel_auth():
            logging.error("Ошибка авторизации GGSel API")
            return
        
        await self.process_pending_topics()
        await self.link_existing_topics_with_chats()
        
        logging.info("Бот запущен")
        self.running = True
        
        tasks = [
            asyncio.create_task(self.monitor_messages()),
            asyncio.create_task(self.reauth_scheduler()),
            asyncio.create_task(self.purchase_checker())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await self.stop()
    
    def handle_topic_message(self, topic_id: int, message_text: str, username: str, message_id: int):
        """Обработка сообщения в топике"""
        asyncio.create_task(self._handle_topic_message_async(topic_id, message_text, username, message_id))
    
    async def handle_general_message(self, text: str):
        """Обработка сообщений в General (для настроек автоответов)"""
        chat_id = self.config.telegram_group_id
        if chat_id in self.awaiting_input:
            await self.handle_text_input(chat_id, text)
    
    async def _handle_topic_message_async(self, topic_id: int, message_text: str, username: str, message_id: int):
        """Асинхронная отправка сообщения из ТГ в GGSel"""
        try:
            # Не отправляем команды (начинаются с /)
            if message_text.startswith('/'):
                return
            
            all_topics = self.topic_manager.get_all_topics()
            
            target_topic = None
            target_key = None
            for key, topic_info in all_topics.items():
                if topic_info.get('topic_id') == topic_id:
                    target_topic = topic_info
                    target_key = key
                    break
            
            if not target_topic:
                return
            
            # Получаем invoice_id топика — это и есть id_i чата
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.send_message_with_cooldown("⚠️ Нет invoice_id", topic_id)
                return
            
            # Отправляем только в чат с id_i == invoice_id
            loop = asyncio.get_event_loop()
            try:
                result = await loop.run_in_executor(
                    None, 
                    lambda: self.ggsel_api.send_message(invoice_id, message_text)
                )
                if result:
                    await self.telegram_bot.add_reaction(message_id, topic_id, "🔥")
                else:
                    await self.send_message_with_cooldown("❌ Ошибка отправки", topic_id)
            except Exception as e:
                logging.error(f"Ошибка отправки в чат {invoice_id}: {e}")
                await self.send_message_with_cooldown("❌ Ошибка отправки", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка обработки сообщения: {e}")
    
    def handle_order_message(self, message_text: str):
        """Обработка нового заказа"""
        try:
            order = self.order_manager.parse_order_message(message_text)
            if order and self.order_manager.add_order(order):
                asyncio.create_task(self.create_topic_for_order(order))
        except Exception as e:
            logging.error(f"Ошибка обработки заказа: {e}")
    
    async def create_topic_for_order(self, order: Order):
        """Создание топика для заказа"""
        try:
            if self.flood_control_until and datetime.now() < self.flood_control_until:
                return
            self.flood_control_until = None
            
            if self.topic_manager.topic_exists(order.id_i):
                return
            
            email_display = order.email or f"Пользователь"
            # Название: ID | email
            topic_name = f"💬 {order.id_i} | {email_display}"
            
            topic_id, cooldown = await self.telegram_bot.create_topic(topic_name)
            
            if topic_id is not None:
                self.topic_manager.add_topic(order.id_i, order.email, topic_id, topic_name)
                
                order_msg = f"🛒 Новый заказ\n\n"
                order_msg += f"📧 {order.email}\n"
                order_msg += f"🆔 {order.id_i}\n"
                order_msg += f"💰 {order.amount} {order.currency}"
                
                await self.send_message_with_cooldown(order_msg, topic_id)
                    
            elif cooldown:
                self.flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                
        except Exception as e:
            logging.error(f"Ошибка создания топика: {e}")
    
    async def ensure_ggsel_auth(self) -> bool:
        """Авторизация в GGSel API с логированием восстановления"""
        current_time = datetime.now()
        
        if self.last_auth_time and current_time - self.last_auth_time < timedelta(seconds=self.auth_interval):
            return True
        
        loop = asyncio.get_event_loop()
        success = await loop.run_in_executor(None, self.ggsel_api.login)
        
        if success:
            self.last_auth_time = current_time
            
            # Логируем восстановление соединения
            if self.connection_was_down:
                logging.info("🔄 Соединение с GGSel API восстановлено! Бот работает в полном режиме")
                self.connection_was_down = False
            
            return True
        else:
            # Отмечаем что соединение упало
            if not self.connection_was_down:
                logging.warning("⚠️ Потеряно соединение с GGSel API - работаем в ограниченном режиме")
                self.connection_was_down = True
            
            return False
    
    async def purchase_checker(self):
        """Проверка покупок каждые 30 секунд"""
        while self.running:
            try:
                await self.check_new_purchases()
            except Exception as e:
                logging.error(f"Ошибка проверки покупок: {e}")
            await asyncio.sleep(30)
    
    async def check_new_purchases(self):
        """Проверка новых покупок с обработкой проблем хоста"""
        if not await self.ensure_ggsel_auth():
            logging.debug("Пропускаем проверку покупок - нет соединения с API")
            return
        
        loop = asyncio.get_event_loop()
        try:
            sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 10)
        except Exception as e:
            logging.error(f"Ошибка получения продаж (проблемы хоста): {e}")
            return
        
        if not sales_data or sales_data.get('retval') != 0:
            logging.debug("Не удалось получить данные о продажах")
            return
        
        for sale in sales_data.get('sales', []):
            invoice_id = sale.get('invoice_id')
            if invoice_id and not self.purchase_manager.is_purchase_processed(invoice_id):
                await self.process_new_purchase(invoice_id)
    
    async def process_new_purchase(self, invoice_id: int):
        """Обработка новой покупки"""
        try:
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data:
                dummy = type('Purchase', (), {
                    'invoice_id': invoice_id, 'buyer_email': '', 'buyer_account': '',
                    'name': 'Unknown', 'amount': 0, 'currency_type': 'USD',
                    'purchase_date': '', 'date_pay': '', 'buyer_phone': '',
                    'buyer_ip': '', 'payment_method': '', 'processed_at': datetime.now().isoformat()
                })()
                self.purchase_manager.add_purchase(dummy)
                return
            
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase and self.purchase_manager.add_purchase(purchase):
                logging.info(f"Покупка: {purchase.invoice_id} - {purchase.buyer_email}")
                await self.create_topic_for_purchase(purchase)
                
        except Exception as e:
            logging.error(f"Ошибка обработки покупки {invoice_id}: {e}")
    
    async def create_topic_for_purchase(self, purchase: Purchase, skip_greeting: bool = False):
        """Создание топика для покупки с улучшенной обработкой ошибок"""
        try:
            if self.flood_control_until and datetime.now() < self.flood_control_until:
                logging.info(f"Flood control, добавляем в очередь: {purchase.invoice_id}")
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                return
            self.flood_control_until = None
            
            # Проверяем по invoice_id, не по email (мультизаказность)
            topic_key = f"purchase_{purchase.invoice_id}"
            if self.topic_manager.get_all_topics().get(topic_key):
                logging.info(f"Топик для покупки {purchase.invoice_id} уже существует")
                return
            
            customer_id = purchase.buyer_email or purchase.buyer_account or f"Customer_{purchase.invoice_id}"
            
            # Название: ID | email (ограничиваем длину)
            topic_name = f"💬 {purchase.invoice_id} | {customer_id}"
            if len(topic_name) > 128:
                # Обрезаем email если слишком длинный
                max_email_len = 128 - len(f"💬 {purchase.invoice_id} | ") - 3
                if len(customer_id) > max_email_len:
                    customer_id = customer_id[:max_email_len] + "..."
                topic_name = f"💬 {purchase.invoice_id} | {customer_id}"
            
            # Задержка перед созданием топика (антифлуд)
            await asyncio.sleep(2)
            
            logging.info(f"Создаём топик для покупки {purchase.invoice_id}: {topic_name}")
            topic_id, cooldown = await self.telegram_bot.create_topic(topic_name)
            
            if topic_id is not None and topic_id > 0:
                # Успешно создан топик
                chat_ids = await self.find_chats_for_customer(customer_id)
                self.topic_manager.add_topic_for_purchase(purchase, topic_id, topic_name, chat_ids)
                
                # Форматирование даты
                date_str = ""
                if purchase.purchase_date:
                    try:
                        dt = datetime.fromisoformat(purchase.purchase_date.replace('+03:00', ''))
                        date_str = dt.strftime('%d.%m.%Y %H:%M')
                    except:
                        date_str = purchase.purchase_date
                
                msg = f"🛒 {'Восстановлен топик' if skip_greeting else 'Новая покупка'}\n\n"
                msg += f"🧾 Invoice: {purchase.invoice_id}\n"
                msg += f"📦 {purchase.name}\n"
                msg += f"💰 {purchase.amount} {purchase.currency_type}\n"
                msg += f"📧 {purchase.buyer_email or 'N/A'}\n"
                if purchase.buyer_account:
                    msg += f"👤 {purchase.buyer_account}\n"
                if purchase.payment_method:
                    msg += f"💳 {purchase.payment_method}\n"
                if date_str:
                    msg += f"📅 {date_str}\n"
                
                # Получаем опции покупки
                options_text, options_list = await self.get_purchase_options_with_list(purchase.invoice_id)
                if options_text:
                    msg += f"\n⚙️ Опции:\n{options_text}\n"
                
                msg += f"\n{'✅ Чатов: ' + str(len(chat_ids)) if chat_ids else '⚠️ Чаты не найдены'}"
                
                await self.send_message_with_cooldown(msg, topic_id)
                logging.info(f"Создан топик {topic_id} для {purchase.invoice_id}")
                
                # Проверяем режим ЧСВ
                if options_list and not skip_greeting:
                    await self.process_csv_rules(purchase.invoice_id, topic_id, options_list)
                
                # Отправляем приветствие в чат покупки (invoice_id = id_i чата)
                # НЕ отправляем при пересоздании топика (skip_greeting=True)
                if not skip_greeting and self.autoresponder.should_send_first_message():
                    greeting = self.autoresponder.get_first_message_text()
                    if greeting:
                        loop = asyncio.get_event_loop()
                        try:
                            await loop.run_in_executor(
                                None, 
                                lambda cid=purchase.invoice_id, g=greeting: self.ggsel_api.send_message(cid, g)
                            )
                            # И в топик
                            await self.send_message_with_cooldown(f"📤 {greeting}", topic_id)
                            logging.info(f"Приветствие отправлено в чат {purchase.invoice_id}")
                        except Exception as e:
                            logging.error(f"Ошибка отправки приветствия: {e}")
                    
            elif topic_id == -1:
                # Группа не является форумом
                logging.error("Группа не является форумом! Проверьте настройки группы в Telegram.")
                
            elif cooldown:
                # Flood control
                self.flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                logging.warning(f"Flood control {cooldown}s, в очередь: {purchase.invoice_id}")
                
            else:
                # Другая ошибка
                logging.error(f"Не удалось создать топик для {purchase.invoice_id}")
                # Добавляем в очередь для повторной попытки
                self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
                self._save_pending_topics()
                
        except Exception as e:
            logging.error(f"Ошибка создания топика покупки {purchase.invoice_id}: {e}")
            # Добавляем в очередь для повторной попытки
            self.pending_topics.append({'purchase': purchase, 'timestamp': datetime.now(), 'skip_greeting': skip_greeting})
            self._save_pending_topics()
    
    async def load_chat_history(self, chat_ids: List[int], topic_id: int):
        """Загрузка истории сообщений из GGSel и отправка в топик"""
        try:
            all_messages = []
            loop = asyncio.get_event_loop()
            
            # Собираем сообщения из всех чатов
            for chat_id in chat_ids:
                messages_data = await loop.run_in_executor(
                    None, self.ggsel_api.get_chat_messages, chat_id
                )
                
                if messages_data:
                    for msg in messages_data:
                        msg['_chat_id'] = chat_id
                        all_messages.append(msg)
            
            # Если история пустая - отправляем приветствие
            if not all_messages and self.autoresponder.should_send_first_message():
                greeting = self.autoresponder.get_first_message_text()
                if greeting and chat_ids:
                    # Отправляем в GGSel
                    for chat_id in chat_ids:
                        await loop.run_in_executor(
                            None, 
                            lambda cid=chat_id, g=greeting: self.ggsel_api.send_message(cid, g)
                        )
                    # И в топик (без пометки)
                    await self.send_message_with_cooldown(greeting, topic_id)
                return
            
            if not all_messages:
                return
            
            # Сортируем по времени (от старых к новым)
            def get_timestamp(msg):
                ts = msg.get('timestamp', msg.get('created_at', msg.get('date', msg.get('time', ''))))
                if not ts:
                    return datetime.min
                try:
                    # Пробуем разные форматы
                    ts = str(ts).replace('Z', '+00:00').replace('+03:00', '')
                    return datetime.fromisoformat(ts)
                except:
                    try:
                        return datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except:
                        return datetime.min
            
            all_messages.sort(key=get_timestamp)
            
            logging.info(f"Загружено {len(all_messages)} сообщений для топика {topic_id}")
            
            # Отправляем сообщения в топик
            for msg in all_messages:
                message_id = str(msg.get('id', ''))
                content = msg.get('message', msg.get('text', msg.get('content', '')))
                chat_id = msg.get('_chat_id')
                timestamp = get_timestamp(msg)
                
                if not content:
                    continue
                
                # Проверяем, не обработано ли уже
                if self.message_manager.is_message_processed(chat_id, message_id):
                    continue
                
                # Просто текст сообщения
                await self.send_message_with_cooldown(content, topic_id, chat_id, message_id)
                
                # Помечаем как обработанное
                self.message_manager.add_processed_message(chat_id, message_id, content, timestamp)
                
                await asyncio.sleep(0.5)
                
        except Exception as e:
            logging.error(f"Ошибка загрузки истории: {e}")
    
    async def process_pending_topics(self):
        """Обработка отложенных топиков"""
        if not self.pending_topics:
            # Но всё равно проверяем очередь истории
            await self.process_pending_history_loads()
            return
        
        if self.flood_control_until and datetime.now() < self.flood_control_until:
            remaining = (self.flood_control_until - datetime.now()).seconds
            logging.debug(f"Flood control активен, ждём {remaining}s, в очереди {len(self.pending_topics)} топиков")
            return
        self.flood_control_until = None
        
        topics = self.pending_topics.copy()
        self.pending_topics.clear()
        self._save_pending_topics()  # Очищаем файл
        
        logging.info(f"Обрабатываем {len(topics)} отложенных топиков")
        
        for i, data in enumerate(topics):
            skip_greeting = data.get('skip_greeting', False)
            await self.create_topic_for_purchase(data['purchase'], skip_greeting=skip_greeting)
            if self.flood_control_until:
                # Возвращаем необработанные топики в очередь
                remaining_topics = topics[i+1:]
                for t in remaining_topics:
                    if not any(p['purchase'].invoice_id == t['purchase'].invoice_id for p in self.pending_topics):
                        self.pending_topics.append(t)
                self._save_pending_topics()
                logging.info(f"Flood control, {len(remaining_topics)} топиков вернулись в очередь")
                break
            await asyncio.sleep(3)  # Увеличенная задержка
        
        # Загружаем историю для созданных топиков
        await self.process_pending_history_loads()
    
    async def monitor_messages(self):
        """Мониторинг сообщений с hot/cold системой и параллельной проверкой"""
        logging.info("Запуск мониторинга сообщений")
        cold_counter = 0
        sync_counter = 0
        unlinked_counter = 0
        review_counter = 0
        
        # Синхронизация в фоне, не блокируем основной цикл
        asyncio.create_task(self.sync_topics_with_purchases())
        
        while self.running:
            try:
                await self.process_pending_messages()
                await self.process_pending_topics()
                
                if not await self.ensure_ggsel_auth():
                    await asyncio.sleep(2)
                    continue
                
                all_topics = self.topic_manager.get_all_topics()
                purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
                
                # Сортируем по invoice_id (новые = больший ID)
                sorted_keys = sorted(purchase_topics.keys(), 
                                    key=lambda k: purchase_topics[k].get('invoice_id', 0), 
                                    reverse=True)
                
                # Hot = последние 25, Cold = остальные
                hot_keys = sorted_keys[:self.hot_topics_max]
                cold_keys = sorted_keys[self.hot_topics_max:]
                
                # Горячие топики - каждый цикл (2 сек)
                hot_topics = {k: v for k, v in purchase_topics.items() if k in hot_keys}
                if hot_topics:
                    await self.check_topics_parallel(hot_topics)
                
                # Холодные топики - каждые 30 циклов (~1 минута)
                cold_counter += 1
                if cold_counter >= 30 and cold_keys:
                    cold_counter = 0
                    cold_topics = {k: v for k, v in purchase_topics.items() if k in cold_keys}
                    if cold_topics:
                        asyncio.create_task(self.check_topics_parallel(cold_topics))
                
                # Проверяем новые чаты без топиков каждые 30 циклов (~1 минута)
                unlinked_counter += 1
                if unlinked_counter >= 30:
                    unlinked_counter = 0
                    asyncio.create_task(self.check_unlinked_chats())
                
                # Проверяем отзывы каждые 30 циклов (~1 минута)
                review_counter += 1
                if review_counter >= 30:
                    review_counter = 0
                    asyncio.create_task(self.check_new_reviews())
                
                # Синхронизация топиков раз в день (43200 циклов * 2 сек = 24 часа)
                sync_counter += 1
                if sync_counter >= 43200:
                    sync_counter = 0
                    asyncio.create_task(self.sync_topics_with_purchases())
                
            except Exception as e:
                logging.error(f"Ошибка мониторинга: {e}")
            
            await asyncio.sleep(2)  # 2 секунды между циклами
    
    async def check_topics_parallel(self, topics: Dict):
        """Параллельная проверка топиков"""
        tasks = []
        
        for topic_key, topic_info in topics.items():
            chat_ids = topic_info.get('chat_ids', [])
            topic_id = topic_info.get('topic_id')
            invoice_id = topic_info.get('invoice_id')
            
            if topic_id:
                # Если chat_ids пустой, используем invoice_id как chat_id
                if not chat_ids and invoice_id:
                    chat_ids = [invoice_id]
                
                for chat_id in chat_ids:
                    tasks.append(self._check_single_chat(chat_id, topic_id))
        
        if tasks:
            # Все запросы параллельно (GGSel API не имеет документированного rate limit)
            await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _check_single_chat(self, chat_id: int, topic_id: int):
        """Проверка одного чата"""
        try:
            await self.check_chat_messages(chat_id, topic_id)
        except Exception as e:
            logging.error(f"Ошибка проверки чата {chat_id}: {e}")
    
    async def check_chat_messages(self, chat_id: int, topic_id: int) -> bool:
        """Проверка сообщений чата. Возвращает True если было новое сообщение"""
        try:
            loop = asyncio.get_event_loop()
            messages_data = await loop.run_in_executor(
                None, self.ggsel_api.get_chat_messages, chat_id
            )
            
            if not messages_data:
                return False
            
            has_new = False
            for msg_data in messages_data:
                if await self.process_single_message_check(chat_id, topic_id, msg_data):
                    has_new = True
            
            return has_new
                
        except Exception as e:
            logging.error(f"Ошибка проверки чата {chat_id}: {e}")
            return False
    
    async def process_single_message_check(self, chat_id: int, topic_id: int, msg_data: Dict) -> bool:
        """Обработка сообщения"""
        try:
            message_id = str(msg_data.get('id', ''))
            content = msg_data.get('message', msg_data.get('text', msg_data.get('content', '')))
            timestamp_str = msg_data.get('timestamp', msg_data.get('created_at', ''))
            
            if not message_id or not content:
                return False
            
            # Проверяем по глобальному message_id (без chat_id) чтобы избежать дублей
            if self.message_manager.is_message_processed(0, message_id):
                return False
            
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00').replace('+03:00', '')) if timestamp_str else datetime.now()
            except:
                timestamp = datetime.now()
            
            # Сохраняем с chat_id=0 для глобальной уникальности
            if self.message_manager.add_processed_message(0, message_id, content, timestamp):
                message = Message(chat_id=chat_id, message_id=message_id, content=content, timestamp=timestamp)
                self.database.save_message(message)
                
                logging.info(f"Новое сообщение в чате {chat_id}: {content[:50]}...")
                
                # Просто текст
                await self.send_message_with_cooldown(content, topic_id, chat_id, message_id)
                
                # Проверяем автоответ на триггеры
                try:
                    auto_result = self.autoresponder.find_response(content)
                    if auto_result:
                        response_text = auto_result.get("response", "")
                        notify_group = auto_result.get("notify_group", False)
                        
                        if response_text:
                            # Отправляем автоответ в GGSel
                            loop = asyncio.get_event_loop()
                            await loop.run_in_executor(
                                None, 
                                lambda rt=response_text: self.ggsel_api.send_message(chat_id, rt)
                            )
                            # И в топик
                            await self.send_message_with_cooldown(response_text, topic_id)
                        
                        # Уведомление в тот же топик
                        if notify_group:
                            topic_info = None
                            for key, info in self.topic_manager.get_all_topics().items():
                                if info.get('topic_id') == topic_id:
                                    topic_info = info
                                    break
                            
                            trigger_notify_text = auto_result.get("notify_text", "")
                            notify_msg = trigger_notify_text or "🔔 Требуется ответ!"
                            if topic_info:
                                notify_msg += f"\n📧 {topic_info.get('email', 'N/A')}"
                                notify_msg += f"\n🆔 {topic_info.get('invoice_id', 'N/A')}"
                            
                            await self.send_message_with_cooldown(notify_msg, topic_id)
                except Exception as e:
                    logging.error(f"Ошибка автоответа: {e}")
                
                return True
            
            return False
                
        except Exception as e:
            logging.error(f"Ошибка обработки сообщения: {e}")
            return False
    
    async def stop(self):
        """Остановка бота"""
        logging.info("Остановка бота...")
        self.running = False
        
        tasks = [t for t in asyncio.all_tasks() if not t.done()]
        for task in tasks:
            task.cancel()
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        await self.telegram_bot.stop()
        logging.info("Бот остановлен")
    
    async def send_message_with_cooldown(self, text: str, topic_id: int, chat_id: int = None, message_id: str = None) -> bool:
        """Отправка с учетом кулдауна"""
        try:
            if self.message_flood_control_until and datetime.now() < self.message_flood_control_until:
                self.pending_messages.append({
                    'text': text, 'topic_id': topic_id,
                    'chat_id': chat_id, 'message_id': message_id,
                    'timestamp': datetime.now()
                })
                return False
            self.message_flood_control_until = None
            
            success, cooldown = await self.telegram_bot.send_message(text, topic_id)
            
            if success:
                if chat_id and message_id:
                    self.message_manager.mark_message_sent(chat_id, message_id)
                    self.database.mark_message_sent(message_id)
                return True
                
            elif cooldown:
                self.message_flood_control_until = datetime.now() + timedelta(seconds=cooldown + 5)
                self.pending_messages.append({
                    'text': text, 'topic_id': topic_id,
                    'chat_id': chat_id, 'message_id': message_id,
                    'timestamp': datetime.now()
                })
            return False
                
        except Exception as e:
            logging.error(f"Ошибка отправки: {e}")
            return False
    
    async def process_pending_messages(self):
        """Обработка отложенных сообщений"""
        if not self.pending_messages:
            return
        
        if self.message_flood_control_until and datetime.now() < self.message_flood_control_until:
            return
        self.message_flood_control_until = None
        
        messages = self.pending_messages.copy()
        self.pending_messages.clear()
        
        for msg in messages:
            success = await self.send_message_with_cooldown(
                msg['text'], msg['topic_id'], msg.get('chat_id'), msg.get('message_id')
            )
            if not success and self.message_flood_control_until:
                break
            await asyncio.sleep(1)
    
    async def find_chats_for_customer(self, customer_id: str) -> List[int]:
        """Поиск чатов по email"""
        try:
            matching = []
            loop = asyncio.get_event_loop()
            
            chats_data = await loop.run_in_executor(
                None, lambda: self.ggsel_api.get_chats_by_email(customer_id, 100, 1)
            )
            
            if chats_data:
                chats = self.ggsel_api.parse_chats_response(chats_data)
                customer_lower = customer_id.lower()
                for chat in chats:
                    if chat.email and chat.email.lower() == customer_lower and chat.id_i:
                        if chat.id_i not in matching:
                            matching.append(chat.id_i)
                
                if matching:
                    return matching
            
            page = 1
            while page <= 100:
                chats_data = await loop.run_in_executor(
                    None, lambda p=page: self.ggsel_api.get_chats(pagesize=100, page=p)
                )
                
                if not chats_data:
                    break
                
                chats = self.ggsel_api.parse_chats_response(chats_data)
                if not chats:
                    break
                
                customer_lower = customer_id.lower()
                for chat in chats:
                    if chat.email and chat.email.lower() == customer_lower and chat.id_i:
                        if chat.id_i not in matching:
                            matching.append(chat.id_i)
                
                if matching:
                    break
                
                if page >= chats_data.get('cnt_pages', 1):
                    break
                
                page += 1
                await asyncio.sleep(0.3)
            
            return matching
            
        except Exception as e:
            logging.error(f"Ошибка поиска чатов: {e}")
            return []
    
    async def link_existing_topics_with_chats(self):
        """Связывание топиков с чатами"""
        try:
            all_topics = self.topic_manager.get_all_topics()
            purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
            
            if not purchase_topics:
                return
            
            unlinked = [(k, v) for k, v in purchase_topics.items() if not v.get('chat_ids')]
            
            if not unlinked:
                return
            
            logging.info(f"Связываем {len(unlinked)} топиков...")
            
            linked = 0
            for key, info in unlinked:
                email = info.get('email')
                if email:
                    chat_ids = await self.find_chats_for_customer(email)
                    if chat_ids:
                        self.topic_manager.update_topic_chat_ids(key, chat_ids)
                        linked += 1
            
            logging.info(f"Связано {linked} топиков")
            
        except Exception as e:
            logging.error(f"Ошибка связывания: {e}")
    
    async def reauth_scheduler(self):
        """Переавторизация каждые 15 минут"""
        while self.running:
            await asyncio.sleep(self.auth_interval)
            if self.running:
                await self.ensure_ggsel_auth()
    
    def stop_sync(self):
        """Синхронная остановка"""
        self.running = False

    # ==================== АВТООТВЕТЫ И INLINE КНОПКИ ====================
    
    async def handle_callback(self, data: str, update, context):
        """Обработка inline кнопок"""
        query = update.callback_query
        message = query.message
        chat_id = message.chat.id
        message_id = message.message_id
        
        # Меню автоответов
        if data == "auto_menu":
            await self.show_auto_menu(chat_id, message_id)
        
        # Переключить автоответы
        elif data == "auto_toggle":
            enabled = self.autoresponder.toggle_enabled()
            await self.show_auto_menu(chat_id, message_id)
        
        # Переключить приветствие
        elif data == "auto_first_toggle":
            self.autoresponder.toggle_first_message()
            await self.show_auto_menu(chat_id, message_id)
        
        # Изменить текст приветствия
        elif data == "auto_first_edit":
            self.awaiting_input[chat_id] = {"type": "first_message"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Отправьте новый текст приветствия:",
                [[InlineKeyboardButton("❌ Отмена", callback_data="auto_menu")]]
            )
        
        # Изменить текст уведомления (удалено - теперь для каждого триггера отдельно)
        
        # Список триггеров
        elif data == "auto_triggers":
            await self.show_triggers_menu(chat_id, message_id)
        
        # Добавить триггер
        elif data == "auto_add_trigger":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Отправьте фразу-триггер (на что реагировать):",
                [[InlineKeyboardButton("❌ Отмена", callback_data="auto_triggers")]]
            )
        
        # Добавить триггер с уведомлением
        elif data == "auto_add_trigger_notify":
            self.awaiting_input[chat_id] = {"type": "trigger_phrase", "notify_group": True}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Отправьте фразу-триггер (с уведомлением):",
                [[InlineKeyboardButton("❌ Отмена", callback_data="auto_triggers")]]
            )
        
        # Редактировать триггер
        elif data.startswith("auto_trigger_edit_"):
            idx = int(data.replace("auto_trigger_edit_", ""))
            await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        # Переключить уведомление триггера
        elif data.startswith("auto_trigger_notify_"):
            idx = int(data.replace("auto_trigger_notify_", ""))
            self.autoresponder.toggle_trigger_notify(idx)
            await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        # Переключить точное совпадение триггера
        elif data.startswith("auto_trigger_exact_"):
            idx = int(data.replace("auto_trigger_exact_", ""))
            self.autoresponder.toggle_trigger_exact_match(idx)
            await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        # Переключить триггер
        elif data.startswith("auto_trigger_toggle_"):
            idx = int(data.replace("auto_trigger_toggle_", ""))
            self.autoresponder.toggle_trigger(idx)
            await self.show_trigger_edit_menu(chat_id, message_id, idx)
        
        # Изменить фразу триггера
        elif data.startswith("auto_trigger_phrase_"):
            idx = int(data.replace("auto_trigger_phrase_", ""))
            self.awaiting_input[chat_id] = {"type": "edit_trigger_phrase", "trigger_idx": idx}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Отправьте новую фразу-триггер:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"auto_trigger_edit_{idx}")]]
            )
        
        # Изменить ответ триггера
        elif data.startswith("auto_trigger_response_"):
            idx = int(data.replace("auto_trigger_response_", ""))
            self.awaiting_input[chat_id] = {"type": "edit_trigger_response", "trigger_idx": idx}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Отправьте новый текст ответа:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"auto_trigger_edit_{idx}")]]
            )
        
        # Изменить текст уведомления триггера
        elif data.startswith("auto_trigger_notifytext_"):
            idx = int(data.replace("auto_trigger_notifytext_", ""))
            self.awaiting_input[chat_id] = {"type": "edit_trigger_notify_text", "trigger_idx": idx}
            trigger = self.autoresponder.get_trigger(idx)
            current = trigger.get('notify_text', '🔔 Требуется ответ!') if trigger else ''
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущий текст уведомления:\n{current or '(не задан)'}\n\n✏️ Отправьте новый текст:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"auto_trigger_edit_{idx}")]]
            )
        
        # Удалить триггер
        elif data.startswith("auto_trigger_del_"):
            idx = int(data.replace("auto_trigger_del_", ""))
            self.autoresponder.remove_trigger(idx)
            await self.show_triggers_menu(chat_id, message_id)
        
        # === Меню ответов на отзывы ===
        elif data == "auto_reviews":
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_toggle":
            self.autoresponder.toggle_review_responses()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_good_toggle":
            self.autoresponder.toggle_good_review_response()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_bad_toggle":
            self.autoresponder.toggle_bad_review_response()
            await self.show_reviews_menu(chat_id, message_id)
        
        elif data == "auto_reviews_good_edit":
            self.awaiting_input[chat_id] = {"type": "edit_good_review_text"}
            current = self.autoresponder.get_good_review_text()
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущий текст:\n{current}\n\n✏️ Отправьте новый текст ответа на хороший отзыв:",
                [[InlineKeyboardButton("❌ Отмена", callback_data="auto_reviews")]]
            )
        
        elif data == "auto_reviews_bad_edit":
            self.awaiting_input[chat_id] = {"type": "edit_bad_review_text"}
            current = self.autoresponder.get_bad_review_text()
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущий текст:\n{current}\n\n✏️ Отправьте новый текст ответа на плохой отзыв:",
                [[InlineKeyboardButton("❌ Отмена", callback_data="auto_reviews")]]
            )
        
        # === Меню режима ЧСВ ===
        elif data == "csv_menu":
            await self.show_csv_menu(chat_id, message_id)
        
        elif data == "csv_toggle":
            self.autoresponder.toggle_csv_mode()
            await self.show_csv_menu(chat_id, message_id)
        
        elif data == "csv_add_rule":
            self.awaiting_input[chat_id] = {"type": "csv_option_name"}
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                "✏️ Введите название опции (как в заказе):\n\nПример: Чай",
                [[InlineKeyboardButton("❌ Отмена", callback_data="csv_menu")]]
            )
        
        elif data.startswith("csv_rule_"):
            idx = int(data.replace("csv_rule_", ""))
            await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_toggle_"):
            idx = int(data.replace("csv_toggle_", ""))
            self.autoresponder.toggle_csv_rule(idx)
            await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_case_"):
            idx = int(data.replace("csv_case_", ""))
            self.autoresponder.toggle_csv_rule_case_sensitive(idx)
            await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_touser_"):
            idx = int(data.replace("csv_touser_", ""))
            self.autoresponder.toggle_csv_rule_send_to_user(idx)
            await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_totopic_"):
            idx = int(data.replace("csv_totopic_", ""))
            self.autoresponder.toggle_csv_rule_send_to_topic(idx)
            await self.show_csv_rule_menu(chat_id, message_id, idx)
        
        elif data.startswith("csv_usermsg_"):
            idx = int(data.replace("csv_usermsg_", ""))
            self.awaiting_input[chat_id] = {"type": "csv_user_message", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            current = rule.get("user_message", "") if rule else ""
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущее сообщение юзеру:\n{current or '(не задано)'}\n\n✏️ Введите сообщение для юзера:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"csv_rule_{idx}")]]
            )
        
        elif data.startswith("csv_topicmsg_"):
            idx = int(data.replace("csv_topicmsg_", ""))
            self.awaiting_input[chat_id] = {"type": "csv_topic_message", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            current = rule.get("topic_message", "") if rule else ""
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущее сообщение в топик:\n{current or '(не задано)'}\n\n✏️ Введите сообщение для топика:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"csv_rule_{idx}")]]
            )
        
        elif data.startswith("csv_name_"):
            idx = int(data.replace("csv_name_", ""))
            self.awaiting_input[chat_id] = {"type": "csv_edit_name", "rule_idx": idx}
            rule = self.autoresponder.get_csv_rule(idx)
            current = rule.get("option_name", "") if rule else ""
            await self.telegram_bot.edit_message(
                message_id, chat_id,
                f"Текущее название опции:\n{current}\n\n✏️ Введите новое название:",
                [[InlineKeyboardButton("❌ Отмена", callback_data=f"csv_rule_{idx}")]]
            )
        
        elif data.startswith("csv_del_"):
            idx = int(data.replace("csv_del_", ""))
            self.autoresponder.remove_csv_rule(idx)
            await self.show_csv_menu(chat_id, message_id)
        
        # Статистика
        elif data == "stats":
            topics = self.topic_manager.get_all_topics()
            purchases = len([k for k in topics if k.startswith('purchase_')])
            
            text = f"📊 Статистика\n\n"
            text += f"📝 Топиков: {len(topics)}\n"
            text += f"🛒 Покупок: {purchases}\n"
            text += f"💬 Сообщений: {len(self.message_manager.processed_messages)}\n"
            text += f"🤖 Автоответы: {'✅' if self.autoresponder.is_enabled() else '❌'}"
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")]]
            await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_auto_menu(self, chat_id: int, message_id: int):
        """Показать меню автоответов"""
        enabled = self.autoresponder.is_enabled()
        first_enabled = self.autoresponder.is_first_message_enabled()
        triggers_count = len(self.autoresponder.get_triggers())
        review_enabled = self.autoresponder.is_review_responses_enabled()
        csv_enabled = self.autoresponder.is_csv_mode_enabled()
        csv_rules_count = len(self.autoresponder.get_csv_rules())
        
        text = f"⚙️ Настройки автоответов\n\n"
        text += f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}\n"
        text += f"Приветствие: {'✅' if first_enabled else '❌'}\n"
        text += f"Триггеров: {triggers_count}\n"
        text += f"Ответы на отзывы: {'✅' if review_enabled else '❌'}\n"
        text += f"Режим ЧСВ: {'✅' if csv_enabled else '❌'} ({csv_rules_count})"
        
        keyboard = [
            [InlineKeyboardButton(
                f"{'🔴 Выключить' if enabled else '🟢 Включить'}", 
                callback_data="auto_toggle"
            )],
            [InlineKeyboardButton(
                f"👋 Приветствие {'✅' if first_enabled else '❌'}", 
                callback_data="auto_first_toggle"
            )],
            [InlineKeyboardButton("✏️ Текст приветствия", callback_data="auto_first_edit")],
            [InlineKeyboardButton(f"📝 Триггеры ({triggers_count})", callback_data="auto_triggers")],
            [InlineKeyboardButton(f"⭐ Ответы на отзывы {'✅' if review_enabled else '❌'}", callback_data="auto_reviews")],
            [InlineKeyboardButton(f"🎯 Режим ЧСВ {'✅' if csv_enabled else '❌'} ({csv_rules_count})", callback_data="csv_menu")],
            [InlineKeyboardButton("📊 Статистика", callback_data="stats")],
            [InlineKeyboardButton("❌ Закрыть", callback_data="close")]
        ]
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_triggers_menu(self, chat_id: int, message_id: int):
        """Показать меню триггеров"""
        triggers = self.autoresponder.get_triggers()
        
        text = "📝 Триггеры автоответов\n\n"
        
        keyboard = []
        
        for i, trigger in enumerate(triggers):
            phrase = trigger.get('phrase', '')[:15]
            enabled = trigger.get('enabled', True)
            notify = trigger.get('notify_group', False)
            status = "✅" if enabled else "❌"
            notify_icon = "🔔" if notify else ""
            
            text += f"{i+1}. {status}{notify_icon} \"{phrase}\"\n"
            
            # Кнопка для редактирования триггера
            keyboard.append([
                InlineKeyboardButton(f"{status}{notify_icon} {phrase}", callback_data=f"auto_trigger_edit_{i}"),
                InlineKeyboardButton("🗑", callback_data=f"auto_trigger_del_{i}")
            ])
        
        if not triggers:
            text += "Пусто. Добавьте триггер."
        
        keyboard.append([InlineKeyboardButton("➕ Добавить триггер", callback_data="auto_add_trigger")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_trigger_edit_menu(self, chat_id: int, message_id: int, idx: int):
        """Показать меню редактирования триггера"""
        trigger = self.autoresponder.get_trigger(idx)
        if not trigger:
            await self.show_triggers_menu(chat_id, message_id)
            return
        
        phrase = trigger.get('phrase', '')
        response = trigger.get('response', '')
        enabled = trigger.get('enabled', True)
        notify = trigger.get('notify_group', False)
        notify_text = trigger.get('notify_text', '')
        exact_match = trigger.get('exact_match', False)
        
        text = f"⚙️ Триггер #{idx+1}\n\n"
        text += f"📝 Фраза: {phrase}\n"
        text += f"💬 Ответ: {response[:50]}{'...' if len(response) > 50 else ''}\n"
        text += f"Статус: {'✅ Вкл' if enabled else '❌ Выкл'}\n"
        text += f"🎯 Режим: {'Точное совпадение' if exact_match else 'Вхождение в текст'}\n"
        text += f"🔔 Уведомление: {'✅ Вкл' if notify else '❌ Выкл'}\n"
        if notify:
            text += f"📢 Текст: {notify_text or '(по умолчанию)'}\n"
        
        keyboard = [
            [InlineKeyboardButton(
                f"{'🔴 Выключить' if enabled else '🟢 Включить'}", 
                callback_data=f"auto_trigger_toggle_{idx}"
            )],
            [InlineKeyboardButton("✏️ Изменить фразу", callback_data=f"auto_trigger_phrase_{idx}")],
            [InlineKeyboardButton("✏️ Изменить ответ", callback_data=f"auto_trigger_response_{idx}")],
            [InlineKeyboardButton(
                f"🎯 {'Точное' if exact_match else 'Вхождение'}", 
                callback_data=f"auto_trigger_exact_{idx}"
            )],
            [InlineKeyboardButton(
                f"🔔 Уведомление: {'✅' if notify else '❌'}", 
                callback_data=f"auto_trigger_notify_{idx}"
            )],
        ]
        
        if notify:
            keyboard.append([InlineKeyboardButton("📢 Текст уведомления", callback_data=f"auto_trigger_notifytext_{idx}")])
        
        keyboard.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"auto_trigger_del_{idx}")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="auto_triggers")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_reviews_menu(self, chat_id: int, message_id: int):
        """Показать меню автоответов на отзывы"""
        enabled = self.autoresponder.is_review_responses_enabled()
        good_enabled = self.autoresponder.is_good_review_response_enabled()
        bad_enabled = self.autoresponder.is_bad_review_response_enabled()
        good_text = self.autoresponder.get_good_review_text()
        bad_text = self.autoresponder.get_bad_review_text()
        
        text = f"⭐ Автоответы на отзывы\n\n"
        text += f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}\n\n"
        text += f"👍 На хорошие: {'✅' if good_enabled else '❌'}\n"
        text += f"Текст: {good_text[:50]}{'...' if len(good_text) > 50 else ''}\n\n"
        text += f"👎 На плохие: {'✅' if bad_enabled else '❌'}\n"
        text += f"Текст: {bad_text[:50]}{'...' if len(bad_text) > 50 else ''}"
        
        keyboard = [
            [InlineKeyboardButton(
                f"{'🔴 Выключить' if enabled else '🟢 Включить'}", 
                callback_data="auto_reviews_toggle"
            )],
            [InlineKeyboardButton(
                f"👍 Хорошие: {'✅' if good_enabled else '❌'}", 
                callback_data="auto_reviews_good_toggle"
            )],
            [InlineKeyboardButton("✏️ Текст для хороших", callback_data="auto_reviews_good_edit")],
            [InlineKeyboardButton(
                f"👎 Плохие: {'✅' if bad_enabled else '❌'}", 
                callback_data="auto_reviews_bad_toggle"
            )],
            [InlineKeyboardButton("✏️ Текст для плохих", callback_data="auto_reviews_bad_edit")],
            [InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")]
        ]
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_csv_menu(self, chat_id: int, message_id: int):
        """Показать меню режима ЧСВ"""
        enabled = self.autoresponder.is_csv_mode_enabled()
        rules = self.autoresponder.get_csv_rules()
        
        text = f"🎯 Режим ЧСВ\n\n"
        text += f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}\n\n"
        text += "Реагирует на опции в заказе.\n"
        text += "Если название опции совпадает - отправляет сообщение.\n\n"
        
        if rules:
            text += f"📋 Правила ({len(rules)}):\n"
            for i, rule in enumerate(rules):
                status = "✅" if rule.get("enabled", True) else "❌"
                name = rule.get("option_name", "")[:20]
                case = "🔤" if rule.get("case_sensitive", False) else "🔡"
                to_user = "👤" if rule.get("send_to_user", False) else ""
                to_topic = "💬" if rule.get("send_to_topic", True) else ""
                text += f"{i+1}. {status} {case} {name} {to_user}{to_topic}\n"
        else:
            text += "Правил нет. Добавьте первое!"
        
        keyboard = [
            [InlineKeyboardButton(
                f"{'🔴 Выключить' if enabled else '🟢 Включить'}", 
                callback_data="csv_toggle"
            )],
        ]
        
        # Кнопки для каждого правила
        for i, rule in enumerate(rules):
            status = "✅" if rule.get("enabled", True) else "❌"
            name = rule.get("option_name", "")[:15]
            keyboard.append([
                InlineKeyboardButton(f"{status} {name}", callback_data=f"csv_rule_{i}"),
                InlineKeyboardButton("🗑", callback_data=f"csv_del_{i}")
            ])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить правило", callback_data="csv_add_rule")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def show_csv_rule_menu(self, chat_id: int, message_id: int, idx: int):
        """Показать меню редактирования правила ЧСВ"""
        rule = self.autoresponder.get_csv_rule(idx)
        if not rule:
            await self.show_csv_menu(chat_id, message_id)
            return
        
        enabled = rule.get("enabled", True)
        option_name = rule.get("option_name", "")
        case_sensitive = rule.get("case_sensitive", False)
        send_to_user = rule.get("send_to_user", False)
        user_message = rule.get("user_message", "")
        send_to_topic = rule.get("send_to_topic", True)
        topic_message = rule.get("topic_message", "")
        
        text = f"🎯 Правило ЧСВ #{idx+1}\n\n"
        text += f"📝 Опция: {option_name}\n"
        text += f"Статус: {'✅ Вкл' if enabled else '❌ Выкл'}\n"
        text += f"🔤 Регистр: {'Строго' if case_sensitive else 'Любой'}\n\n"
        text += f"👤 Юзеру: {'✅' if send_to_user else '❌'}\n"
        if send_to_user and user_message:
            text += f"   └ {user_message[:50]}{'...' if len(user_message) > 50 else ''}\n"
        text += f"💬 В топик: {'✅' if send_to_topic else '❌'}\n"
        if send_to_topic and topic_message:
            text += f"   └ {topic_message[:50]}{'...' if len(topic_message) > 50 else ''}\n"
        
        keyboard = [
            [InlineKeyboardButton(
                f"{'🔴 Выключить' if enabled else '🟢 Включить'}", 
                callback_data=f"csv_toggle_{idx}"
            )],
            [InlineKeyboardButton("✏️ Название опции", callback_data=f"csv_name_{idx}")],
            [InlineKeyboardButton(
                f"🔤 Регистр: {'Строго' if case_sensitive else 'Любой'}", 
                callback_data=f"csv_case_{idx}"
            )],
            [InlineKeyboardButton(
                f"👤 Юзеру: {'✅' if send_to_user else '❌'}", 
                callback_data=f"csv_touser_{idx}"
            )],
        ]
        
        if send_to_user:
            keyboard.append([InlineKeyboardButton("✏️ Сообщение юзеру", callback_data=f"csv_usermsg_{idx}")])
        
        keyboard.append([InlineKeyboardButton(
            f"💬 В топик: {'✅' if send_to_topic else '❌'}", 
            callback_data=f"csv_totopic_{idx}"
        )])
        
        if send_to_topic:
            keyboard.append([InlineKeyboardButton("✏️ Сообщение в топик", callback_data=f"csv_topicmsg_{idx}")])
        
        keyboard.append([InlineKeyboardButton("🗑 Удалить", callback_data=f"csv_del_{idx}")])
        keyboard.append([InlineKeyboardButton("◀️ Назад", callback_data="csv_menu")])
        
        await self.telegram_bot.edit_message(message_id, chat_id, text, keyboard)
    
    async def handle_text_input(self, chat_id: int, text: str):
        """Обработка текстового ввода для настроек"""
        if chat_id not in self.awaiting_input:
            return False
        
        input_type = self.awaiting_input[chat_id].get("type")
        
        if input_type == "first_message":
            self.autoresponder.set_first_message_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст приветствия обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        elif input_type == "trigger_phrase":
            notify_group = self.awaiting_input[chat_id].get("notify_group", False)
            self.awaiting_input[chat_id] = {"type": "trigger_response", "phrase": text, "notify_group": notify_group}
            
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="auto_triggers")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"Триггер: \"{text}\"\n\n✏️ Теперь отправьте текст ответа:", keyboard, None
            )
            return True
        
        elif input_type == "trigger_response":
            phrase = self.awaiting_input[chat_id].get("phrase", "")
            notify_group = self.awaiting_input[chat_id].get("notify_group", False)
            idx = self.autoresponder.add_trigger(phrase, text, notify_group)
            del self.awaiting_input[chat_id]
            
            if notify_group:
                # Если с уведомлением - спрашиваем текст уведомления
                self.awaiting_input[chat_id] = {"type": "new_trigger_notify_text", "trigger_idx": idx}
                keyboard = [[InlineKeyboardButton("⏭ Пропустить", callback_data=f"auto_trigger_edit_{idx}")]]
                await self.telegram_bot.send_message_with_keyboard(
                    f"✅ Триггер добавлен!\n\n✏️ Отправьте текст уведомления (или пропустите):", keyboard, None
                )
            else:
                keyboard = [[InlineKeyboardButton("◀️ К триггерам", callback_data="auto_triggers")]]
                await self.telegram_bot.send_message_with_keyboard(
                    f"✅ Триггер добавлен!\n\nФраза: \"{phrase}\"\nОтвет: \"{text}\"", keyboard, None
                )
            return True
        
        # Редактирование триггера - фраза
        elif input_type == "edit_trigger_phrase":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, phrase=text.lower())
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Фраза обновлена: \"{text}\"", keyboard, None
            )
            return True
        
        # Редактирование триггера - ответ
        elif input_type == "edit_trigger_response":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, response=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Ответ обновлён: \"{text}\"", keyboard, None
            )
            return True
        
        # Редактирование триггера - текст уведомления
        elif input_type == "edit_trigger_notify_text":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, notify_text=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"auto_trigger_edit_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст уведомления обновлён: \"{text}\"", keyboard, None
            )
            return True
        
        # Новый триггер - текст уведомления
        elif input_type == "new_trigger_notify_text":
            idx = self.awaiting_input[chat_id].get("trigger_idx")
            self.autoresponder.update_trigger(idx, notify_text=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ К триггерам", callback_data="auto_triggers")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст уведомления установлен!", keyboard, None
            )
            return True
        
        # Текст ответа на хороший отзыв
        elif input_type == "edit_good_review_text":
            self.autoresponder.set_good_review_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_reviews")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст для хороших отзывов обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        # Текст ответа на плохой отзыв
        elif input_type == "edit_bad_review_text":
            self.autoresponder.set_bad_review_text(text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data="auto_reviews")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Текст для плохих отзывов обновлён:\n\n{text}", keyboard, None
            )
            return True
        
        # === ЧСВ режим ===
        
        # Название опции для нового правила
        elif input_type == "csv_option_name":
            self.awaiting_input[chat_id] = {"type": "csv_topic_message_new", "option_name": text}
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"Опция: {text}\n\n✏️ Введите сообщение для топика (или отправьте - чтобы пропустить):",
                keyboard, None
            )
            return True
        
        # Сообщение в топик для нового правила
        elif input_type == "csv_topic_message_new":
            option_name = self.awaiting_input[chat_id].get("option_name", "")
            topic_msg = text if text != "-" else ""
            self.awaiting_input[chat_id] = {"type": "csv_user_message_new", "option_name": option_name, "topic_message": topic_msg}
            keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✏️ Введите сообщение для юзера (или отправьте - чтобы пропустить):",
                keyboard, None
            )
            return True
        
        # Сообщение юзеру для нового правила
        elif input_type == "csv_user_message_new":
            option_name = self.awaiting_input[chat_id].get("option_name", "")
            topic_msg = self.awaiting_input[chat_id].get("topic_message", "")
            user_msg = text if text != "-" else ""
            
            # Создаём правило
            send_to_topic = bool(topic_msg)
            send_to_user = bool(user_msg)
            
            idx = self.autoresponder.add_csv_rule(
                option_name=option_name,
                case_sensitive=False,
                send_to_user=send_to_user,
                user_message=user_msg,
                send_to_topic=send_to_topic,
                topic_message=topic_msg
            )
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ К правилам", callback_data="csv_menu")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Правило ЧСВ добавлено!\n\n📝 Опция: {option_name}\n💬 В топик: {'✅' if send_to_topic else '❌'}\n👤 Юзеру: {'✅' if send_to_user else '❌'}",
                keyboard, None
            )
            return True
        
        # Редактирование названия опции
        elif input_type == "csv_edit_name":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, option_name=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Название опции обновлено: {text}", keyboard, None
            )
            return True
        
        # Редактирование сообщения юзеру
        elif input_type == "csv_user_message":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, user_message=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Сообщение юзеру обновлено!", keyboard, None
            )
            return True
        
        # Редактирование сообщения в топик
        elif input_type == "csv_topic_message":
            idx = self.awaiting_input[chat_id].get("rule_idx")
            self.autoresponder.update_csv_rule(idx, topic_message=text)
            del self.awaiting_input[chat_id]
            
            keyboard = [[InlineKeyboardButton("◀️ Назад", callback_data=f"csv_rule_{idx}")]]
            await self.telegram_bot.send_message_with_keyboard(
                f"✅ Сообщение в топик обновлено!", keyboard, None
            )
            return True
        
        return False

    async def sync_topics_with_purchases(self):
        """Синхронизация топиков с покупками"""
        logging.info("Запуск синхронизации топиков с покупками...")
        
        if not await self.ensure_ggsel_auth():
            logging.error("Синхронизация: ошибка авторизации")
            return
        
        # Получаем последние покупки
        loop = asyncio.get_event_loop()
        sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 30)
        
        if not sales_data or sales_data.get('retval') != 0:
            logging.warning("Не удалось получить список покупок")
            return
        
        # Получаем существующие топики из JSON
        all_topics = self.topic_manager.get_all_topics()
        logging.info(f"Синхронизация топиков...")
        
        # Собираем invoice_id из API
        api_invoice_ids = set()
        for sale in sales_data.get('sales', []):
            invoice_id = sale.get('invoice_id')
            if invoice_id:
                api_invoice_ids.add(invoice_id)
        
        # Считаем топики для проверки
        purchase_topics = {k: v for k, v in all_topics.items() if k.startswith('purchase_')}
        logging.info(f"Найдено {len(purchase_topics)} существующих топиков, {len(api_invoice_ids)} покупок")
        
        # Список восстановленных топиков для загрузки истории
        recreated_topics = []
        
        # Проверяем существующие топики - пересоздаём удалённые
        # Ограничиваем до 10 проверок за раз чтобы не грузить сервер
        recreated_count = 0
        checked_count = 0
        max_checks_per_sync = 10
        
        for key, info in list(purchase_topics.items()):
            if checked_count >= max_checks_per_sync:
                break
            
            topic_id = info.get('topic_id')
            invoice_id = info.get('invoice_id')
            
            if not topic_id or not invoice_id:
                continue
            
            checked_count += 1
            
            # Проверяем существует ли топик (с задержкой чтобы не словить flood)
            topic_exists = await self.telegram_bot.check_topic_exists(topic_id)
            await asyncio.sleep(3)  # Увеличенная задержка между проверками
            
            if not topic_exists:
                # Топик удалён - пересоздаём
                logging.info(f"Топик {key} удалён из Telegram, пересоздаём...")
                
                # Получаем данные покупки
                purchase_data = await loop.run_in_executor(
                    None, self.ggsel_api.get_purchase_info, invoice_id
                )
                
                if purchase_data:
                    purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
                    if purchase:
                        # Удаляем старую запись
                        del self.topic_manager.topics[key]
                        self.topic_manager.save_topics()
                        
                        # Создаём новый топик БЕЗ приветствия
                        await self.create_topic_for_purchase(purchase, skip_greeting=True)
                        recreated_count += 1
                        
                        # Запоминаем для загрузки истории
                        new_topic_info = self.topic_manager.topics.get(f"purchase_{invoice_id}")
                        if new_topic_info:
                            recreated_topics.append({
                                'topic_id': new_topic_info.get('topic_id'),
                                'chat_ids': new_topic_info.get('chat_ids', []),
                                'invoice_id': invoice_id
                            })
                        
                        if self.flood_control_until:
                            break
                        
                        await asyncio.sleep(3)
        
        # Пересчитываем существующие invoice_id
        existing_invoice_ids = set()
        for key in self.topic_manager.topics.keys():
            if key.startswith('purchase_'):
                try:
                    existing_invoice_ids.add(int(key.replace('purchase_', '')))
                except:
                    pass
        
        logging.info(f"В базе: {len(existing_invoice_ids)} топиков, в API: {len(api_invoice_ids)} покупок")
        
        # Создаём топики для новых покупок
        missing_invoice_ids = api_invoice_ids - existing_invoice_ids
        
        missing_count = 0
        for invoice_id in missing_invoice_ids:
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data:
                continue
            
            purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
            if purchase:
                self.purchase_manager.add_purchase(purchase)
                await self.create_topic_for_purchase(purchase)
                missing_count += 1
                
                if self.flood_control_until:
                    break
                    
                await asyncio.sleep(3)
        
        if recreated_count > 0 or missing_count > 0:
            logging.info(f"Синхронизация: пересоздано {recreated_count}, создано {missing_count} топиков")
        
        # Загружаем историю для восстановленных топиков
        if recreated_topics:
            logging.info(f"Загрузка истории для {len(recreated_topics)} восстановленных топиков...")
            for topic_data in recreated_topics:
                topic_id = topic_data['topic_id']
                chat_ids = topic_data['chat_ids']
                invoice_id = topic_data['invoice_id']
                
                if topic_id and chat_ids:
                    try:
                        await self.load_chat_history(chat_ids, topic_id)
                        logging.info(f"История загружена для топика {invoice_id}")
                    except Exception as e:
                        logging.error(f"Ошибка загрузки истории для {invoice_id}: {e}")
                    await asyncio.sleep(2)
        
        # Загружаем историю для созданных топиков
        await self.process_pending_history_loads()
    
    async def ensure_topic_for_chat(self, chat_id: int, email: str) -> Optional[int]:
        """Убедиться что топик существует для чата, создать если нет"""
        # Ищем топик по email
        all_topics = self.topic_manager.get_all_topics()
        
        for key, info in all_topics.items():
            if chat_id in info.get('chat_ids', []):
                return info.get('topic_id')
        
        # Топик не найден - ищем покупку по email и создаём
        loop = asyncio.get_event_loop()
        
        # Получаем последние покупки и ищем по email
        sales_data = await loop.run_in_executor(None, self.ggsel_api.get_last_sales, 50)
        
        if not sales_data or sales_data.get('retval') != 0:
            return None
        
        for sale in sales_data.get('sales', []):
            invoice_id = sale.get('invoice_id')
            if not invoice_id:
                continue
            
            # Получаем инфо о покупке
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data:
                continue
            
            buyer_email = purchase_data.get('buyer_email', '')
            if buyer_email and buyer_email.lower() == email.lower():
                # Нашли покупку - создаём топик если его нет
                topic_key = f"purchase_{invoice_id}"
                if topic_key not in all_topics:
                    purchase = self.purchase_manager.parse_purchase_response(purchase_data, invoice_id)
                    if purchase:
                        self.purchase_manager.add_purchase(purchase)
                        await self.create_topic_for_purchase(purchase)
                        
                        # Возвращаем topic_id
                        updated_topics = self.topic_manager.get_all_topics()
                        if topic_key in updated_topics:
                            return updated_topics[topic_key].get('topic_id')
                break
        
        return None

    async def check_unlinked_chats(self):
        """Проверка чатов без топиков - создание топиков при новых сообщениях"""
        try:
            loop = asyncio.get_event_loop()
            
            # Получаем последние чаты
            chats_data = await loop.run_in_executor(
                None, lambda: self.ggsel_api.get_chats(pagesize=20, page=1)
            )
            
            if not chats_data:
                return
            
            chats = self.ggsel_api.parse_chats_response(chats_data)
            if not chats:
                return
            
            # Получаем все chat_ids из существующих топиков
            all_topics = self.topic_manager.get_all_topics()
            linked_chat_ids = set()
            for info in all_topics.values():
                for cid in info.get('chat_ids', []):
                    linked_chat_ids.add(cid)
            
            # Проверяем чаты без топиков
            for chat in chats:
                if not chat.id_i or chat.id_i in linked_chat_ids:
                    continue
                
                # Чат без топика - проверяем есть ли новые сообщения
                messages_data = await loop.run_in_executor(
                    None, self.ggsel_api.get_chat_messages, chat.id_i
                )
                
                if not messages_data:
                    continue
                
                # Есть сообщения - ищем покупку и создаём топик
                if chat.email:
                    topic_id = await self.ensure_topic_for_chat(chat.id_i, chat.email)
                    if topic_id:
                        logging.info(f"Создан топик для чата {chat.id_i}")
                        await asyncio.sleep(2)
                        
        except Exception as e:
            logging.error(f"Ошибка проверки чатов: {e}")

    async def check_new_reviews(self):
        """Проверка новых отзывов с обработкой проблем хоста"""
        try:
            if not await self.ensure_ggsel_auth():
                logging.debug("Пропускаем проверку отзывов - нет соединения с API")
                return
                
            logging.debug("Проверка новых отзывов...")
            loop = asyncio.get_event_loop()
            
            try:
                reviews_data = await loop.run_in_executor(
                    None, lambda: self.ggsel_api.get_reviews(50)
                )
            except Exception as e:
                logging.error(f"Ошибка получения отзывов (проблемы хоста): {e}")
                return
            
            if not reviews_data:
                logging.debug("Не удалось получить данные отзывов (проблемы хоста)")
                return
            
            reviews = reviews_data.get('reviews', [])
            if not reviews:
                logging.debug("Отзывов не найдено")
                return
            
            logging.info(f"Получено {len(reviews)} отзывов для проверки")
            all_topics = self.topic_manager.get_all_topics()
            
            new_reviews_count = 0
            
            for review in reviews:
                # Получаем ID отзыва
                review_id = review.get('id')
                if not review_id:
                    logging.debug("Отзыв без ID, пропускаем")
                    continue
                    
                if review_id in self.processed_reviews:
                    continue
                
                # Помечаем как обработанный
                self.processed_reviews.add(review_id)
                new_reviews_count += 1
                
                # Ищем топик по invoice_id
                invoice_id = review.get('invoice_id')
                if not invoice_id:
                    logging.debug(f"Отзыв {review_id} без invoice_id")
                    continue
                
                topic_key = f"purchase_{invoice_id}"
                topic_info = all_topics.get(topic_key)
                
                if not topic_info:
                    logging.debug(f"Топик не найден для invoice {invoice_id}")
                    continue
                
                topic_id = topic_info.get('topic_id')
                if not topic_id:
                    logging.debug(f"Нет topic_id для invoice {invoice_id}")
                    continue
                
                # Формируем сообщение об отзыве
                review_type = review.get('type', 'good')
                info = review.get('info', '')
                name = review.get('name', '')
                date = review.get('date', '')
                rating = review.get('rating', '')
                
                emoji = "👍" if review_type == 'good' else "👎"
                msg = f"{emoji} Новый отзыв!\n\n"
                if name:
                    msg += f"📦 {name}\n"
                if rating:
                    msg += f"⭐ Оценка: {rating}\n"
                if info:
                    msg += f"💬 {info}\n"
                if date:
                    msg += f"📅 {date}"
                
                await self.send_message_with_cooldown(msg, topic_id)
                logging.info(f"Отзыв {review_id} для invoice {invoice_id} отправлен в топик {topic_id}")
                
                # Автоответ на отзыв юзеру (только если API работает)
                auto_response = self.autoresponder.get_review_response(review_type)
                if auto_response:
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda cid=invoice_id, txt=auto_response: self.ggsel_api.send_message(cid, txt)
                        )
                        await self.send_message_with_cooldown(f"📤 {auto_response}", topic_id)
                        logging.info(f"Автоответ на отзыв {review_id} отправлен в чат {invoice_id}")
                    except Exception as e:
                        logging.error(f"Ошибка отправки автоответа на отзыв {review_id} (проблемы хоста): {e}")
            
            if new_reviews_count > 0:
                logging.info(f"Обработано {new_reviews_count} новых отзывов")
            else:
                logging.debug("Новых отзывов не найдено")
                
        except Exception as e:
            logging.error(f"Ошибка проверки отзывов: {e}")

    async def process_pending_history_loads(self):
        """Загрузка истории сообщений для созданных топиков"""
        if not self.pending_history_loads:
            return
        
        logging.info(f"Загружаем историю для {len(self.pending_history_loads)} топиков")
        
        loads = self.pending_history_loads.copy()
        self.pending_history_loads.clear()
        
        for item in loads:
            try:
                await self.load_chat_history(item['chat_ids'], item['topic_id'])
                await asyncio.sleep(1)
            except Exception as e:
                logging.error(f"Ошибка загрузки истории для {item.get('invoice_id')}: {e}")

    async def handle_history_command(self, topic_id: int):
        """Обработка команды /history - загрузка истории в топик"""
        try:
            # Ищем топик по topic_id
            all_topics = self.topic_manager.get_all_topics()
            target_topic = None
            
            for key, info in all_topics.items():
                if info.get('topic_id') == topic_id:
                    target_topic = info
                    break
            
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден в базе", topic_id)
                return
            
            chat_ids = target_topic.get('chat_ids', [])
            
            if not chat_ids:
                # Пробуем найти чаты по email
                email = target_topic.get('email')
                if email:
                    chat_ids = await self.find_chats_for_customer(email)
                    if chat_ids:
                        # Обновляем топик
                        for key, info in all_topics.items():
                            if info.get('topic_id') == topic_id:
                                self.topic_manager.update_topic_chat_ids(key, chat_ids)
                                break
            
            if not chat_ids:
                await self.telegram_bot.send_message("❌ Нет связанных чатов", topic_id)
                return
            
            # Сбрасываем обработанные сообщения для этих чатов чтобы загрузить заново
            for chat_id in chat_ids:
                if chat_id in self.message_manager.processed_messages:
                    del self.message_manager.processed_messages[chat_id]
            
            # Загружаем историю
            await self.load_chat_history(chat_ids, topic_id)
            await self.telegram_bot.send_message("✅ История загружена", topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка загрузки истории: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)

    async def get_purchase_options(self, invoice_id: int) -> Optional[str]:
        """Получить опции покупки в виде текста"""
        text, _ = await self.get_purchase_options_with_list(invoice_id)
        return text
    
    async def get_purchase_options_with_list(self, invoice_id: int) -> tuple:
        """Получить опции покупки в виде текста и списка"""
        try:
            loop = asyncio.get_event_loop()
            purchase_data = await loop.run_in_executor(
                None, self.ggsel_api.get_purchase_info, invoice_id
            )
            
            if not purchase_data or purchase_data.get('retval') != 0:
                return None, []
            
            content = purchase_data.get('content', {})
            options = content.get('options', [])
            
            if not options:
                return None, []
            
            # Форматируем опции
            lines = []
            for opt in options:
                name = opt.get('name', '')
                user_data = opt.get('user_data', '')
                if name and user_data:
                    lines.append(f"• {name}: {user_data}")
            
            text = "\n".join(lines) if lines else None
            return text, options
            
        except Exception as e:
            logging.error(f"Ошибка получения опций: {e}")
            return None, []
    
    async def process_csv_rules(self, invoice_id: int, topic_id: int, options: list):
        """Обработка правил ЧСВ для опций покупки"""
        try:
            results = self.autoresponder.check_csv_options(options)
            
            if not results:
                return
            
            loop = asyncio.get_event_loop()
            
            for result in results:
                option = result.get("option", {})
                option_name = option.get("name", "")
                option_value = option.get("user_data", "")
                
                # Отправляем в топик
                if result.get("send_to_topic") and result.get("topic_message"):
                    topic_msg = result["topic_message"]
                    # Подставляем переменные
                    topic_msg = topic_msg.replace("{option}", option_name)
                    topic_msg = topic_msg.replace("{value}", option_value)
                    topic_msg = topic_msg.replace("{sum}", option_value)  # алиас для {value}
                    await self.send_message_with_cooldown(f"🎯 {topic_msg}", topic_id)
                    logging.info(f"ЧСВ: отправлено в топик для опции '{option_name}'")
                
                # Отправляем юзеру
                if result.get("send_to_user") and result.get("user_message"):
                    user_msg = result["user_message"]
                    # Подставляем переменные
                    user_msg = user_msg.replace("{option}", option_name)
                    user_msg = user_msg.replace("{value}", option_value)
                    user_msg = user_msg.replace("{sum}", option_value)  # алиас для {value}
                    try:
                        await loop.run_in_executor(
                            None,
                            lambda cid=invoice_id, msg=user_msg: self.ggsel_api.send_message(cid, msg)
                        )
                        await self.send_message_with_cooldown(f"📤 {user_msg}", topic_id)
                        logging.info(f"ЧСВ: отправлено юзеру для опции '{option_name}'")
                    except Exception as e:
                        logging.error(f"ЧСВ: ошибка отправки юзеру: {e}")
                        
        except Exception as e:
            logging.error(f"Ошибка обработки ЧСВ: {e}")
    
    async def handle_options_command(self, topic_id: int):
        """Обработка команды /options - показать опции покупки"""
        try:
            all_topics = self.topic_manager.get_all_topics()
            target_topic = None
            
            for key, info in all_topics.items():
                if info.get('topic_id') == topic_id:
                    target_topic = info
                    break
            
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден", topic_id)
                return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.telegram_bot.send_message("❌ Нет invoice_id", topic_id)
                return
            
            options_text = await self.get_purchase_options(invoice_id)
            
            if options_text:
                msg = f"⚙️ Опции покупки #{invoice_id}:\n\n{options_text}"
            else:
                msg = f"ℹ️ Нет опций для покупки #{invoice_id}"
            
            await self.telegram_bot.send_message(msg, topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка команды /options: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)
            if not target_topic:
                await self.telegram_bot.send_message("❌ Топик не найден", topic_id)
                return
            
            invoice_id = target_topic.get('invoice_id')
            if not invoice_id:
                await self.telegram_bot.send_message("❌ Нет invoice_id", topic_id)
                return
            
            options_text = await self.get_purchase_options(invoice_id)
            
            if options_text:
                msg = f"⚙️ Опции покупки #{invoice_id}:\n\n{options_text}"
            else:
                msg = f"ℹ️ Нет опций для покупки #{invoice_id}"
            
            await self.telegram_bot.send_message(msg, topic_id)
            
        except Exception as e:
            logging.error(f"Ошибка команды /options: {e}")
            await self.telegram_bot.send_message(f"❌ Ошибка: {e}", topic_id)