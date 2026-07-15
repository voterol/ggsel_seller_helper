import json
import os

class LocaleManager:
    def __init__(self, file_path="bot_lang.json"):
        self.file_path = file_path
        self.lang = self.load_lang()
        self.translations = {
            "ru": {
                "menu_title": "🤖 Меню управления ботом", "btn_auto": "⚙️ Автоответы", "btn_stats": "📊 Статистика",
                "btn_lang": "🇷🇺 Язык: RU", "btn_close": "❌ Закрыть", "stats_title": "📊 Статистика",
                "stats_topics": "📝 Топиков:", "stats_purchases": "🛒 Покупок:", "stats_msgs": "💬 Сообщений:",
                "stats_auto": "🤖 Автоответы:", "btn_back": "◀️ Назад", "auto_title": "⚙️ Настройки автоответов",
                "auto_status": "Статус:", "auto_greeting": "Приветствие:", "auto_triggers": "Триггеров:",
                "auto_reviews": "Ответы на отзывы:", "auto_csv": "Режим ЧСВ:", "btn_turn_on": "🟢 Включить",
                "btn_turn_off": "🔴 Выключить", "btn_greeting": "👋 Приветствие", "btn_greeting_text": "✏️ Текст приветствия",
                "btn_triggers": "📝 Триггеры", "btn_reviews": "⭐ Ответы на отзывы", "btn_csv": "🎯 Режим ЧСВ",
                "enabled": "Включено", "disabled": "Выключено", "csv_title": "🎯 Режим ЧСВ", "csv_desc": "Реагирует на опции в заказе.\nПеременные: {option}, {value}, {sum}",
                "csv_rules_empty": "Правил нет. Добавьте первое!", "btn_add_rule": "➕ Добавить правило",
                "csv_rule_title": "🎯 Правило ЧСВ", "csv_option": "📝 Опция:", "csv_value": "📊 Значение:",
                "csv_type": "🔎 Тип:", "csv_register": "🔤 Регистр:", "csv_to_user": "👤 Юзеру:", "csv_to_topic": "💬 В топик:",
                "csv_type_name": "📝 Только по названию", "csv_type_value": "🎯 По названию и значению", "csv_type_contains": "🔍 Значение содержит",
                "csv_reg_strict": "Строго", "csv_reg_any": "Любой", "btn_edit_name": "✏️ Название опции",
                "btn_edit_value": "✏️ Значение опции", "btn_edit_user_msg": "✏️ Сообщение юзеру", "btn_edit_topic_msg": "✏️ Сообщение в топик",
                "btn_delete": "🗑 Удалить", "triggers_title": "📝 Триггеры автоответов", "triggers_empty": "Пусто. Добавьте триггер.",
                "btn_add_trigger": "➕ Добавить триггер", "trigger_edit_title": "⚙️ Триггер", "trigger_phrase": "📝 Фраза:",
                "trigger_answer": "💬 Ответ:", "trigger_mode": "🎯 Режим:", "trigger_mode_exact": "Точное совпадение",
                "trigger_mode_contain": "Вхождение в текст", "trigger_notify": "🔔 Уведомление:", "trigger_notify_text": "📢 Текст:",
                "btn_edit_phrase": "✏️ Изменить фразу", "btn_edit_answer": "✏️ Изменить ответ", "btn_edit_notify_text": "📢 Текст уведомления",
                "reviews_title": "⭐ Автоответы на отзывы", "reviews_good": "👍 На хорошие:", "reviews_bad": "👎 На плохие:",
                "reviews_text": "Текст:", "btn_good_edit": "✏️ Текст для хороших", "btn_bad_edit": "✏️ Текст для плохих",
                "btn_cancel": "❌ Отмена", "prompt_greeting": "✏️ Отправьте новый текст приветствия:",
                "prompt_trigger_phrase": "✏️ Отправьте фразу-триггер (на что реагировать):", "prompt_trigger_notify": "✏️ Отправьте фразу-триггер (с уведомлением):",
                "prompt_edit_phrase": "✏️ Отправьте новую фразу-триггер:", "prompt_edit_answer": "✏️ Отправьте новый текст ответа:",
                "prompt_notify_text": "Текущий текст уведомления:\n{current}\n\n✏️ Отправьте новый текст:",
                "prompt_good_review": "Текущий текст:\n{current}\n\n✏️ Отправьте новый текст ответа на хороший отзыв:",
                "prompt_bad_review": "Текущий текст:\n{current}\n\n✏️ Отправьте новый текст ответа на плохой отзыв:",
                "prompt_csv_option": "✏️ Введите название опции (как в заказе):\n\nПример: Чай",
                "prompt_csv_value": "Текущее значение:\n{current}\n\n✏️ Введите значение опции (user_data):\n\nПример: 20р\n\nОтправьте - чтобы очистить",
                "prompt_csv_user": "Текущее сообщение юзеру:\n{current}\n\n✏️ Введите сообщение для юзера:",
                "prompt_csv_topic": "Текущее сообщение в топик:\n{current}\n\n✏️ Введите сообщение для топика:",
                "prompt_csv_name": "Текущее название опции:\n{current}\n\n✏️ Введите новое название:",
                "btn_balance": "💰 Мой кошелек",
                "btn_refresh": "🔄 Обновить",
                "balance_header": "🏛 **Баланс продавца GGSel**\n" + "—" * 15,
                "balance_body": "\n💵 **Всего:** `{total} {curr}`\n✅ **Доступно:** `{avail} {curr}`\n⏳ **Удержано:** `{hold} {curr}`\n\nℹ️ *Средства переходят в 'Доступно' после завершения срока холда.*",
                "balance_error": "❌ **Ошибка:** Не удалось получить данные от GGSel.",
                "balance_released_alert": (
                    "📈 **ИЗМЕНЕНИЕ БАЛАНСА**\n"
                    "🟢 **ПОПОЛНЕНИЕ:** `+{amount} {curr}`\n"
                    "💰 **Текущий баланс:**\n"
                    "• Доступно: `{avail} {curr}`\n"
                    "• Заблокировано: `{hold} {curr}`\n"
                    "• Лимит: `0.00 {curr}`\n"
                    "• Всего: `{total} {curr}`\n"
                    "📊 Предыдущий баланс: `{prev} {curr}`\n"
                    "🕒 Время проверки: `{time}`"
                ),
                # NEW NOTIFICATION KEYS (EMOJIS REMOVED TO PREVENT DOUBLING)
                "noti_new_purchase": "🛒 <b>НОВЫЙ ЗАКАЗ!</b>",
                "noti_restored": "🛒 <b>ВОССТАНОВЛЕН ТОПИК!</b>",
                "noti_product": "📦 Товар:",
                "noti_item_id": "🆔 ID товара:",
                "noti_invoice": "🧾 Номер заказа:",
                "noti_date": "📅 Дата заказа:",
                "noti_prices": "Цены:",
                "noti_details": "Детали заказа:",
                "noti_total": "• Сумма:",
                "noti_status": "• Состояние:",
                "noti_profit": "• Прибыль продавца:",
                "noti_buyer_info": "Информация о покупателе:",
                "noti_payment": "• Способ оплаты:",
                "noti_account": "• Аккаунт:",
                "noti_email": "• Email:",
                "noti_aggregator": "• Платёжный агрегатор:",
                "noti_options": "Детали товара:",
                "noti_status_processing": "⚙️ В процессе",
                "noti_status_done": "✅ Выполнен",
                "btn_go_to_order": "Перейти к заказу"
            },
            "en": {
                "menu_title": "🤖 Bot Control Menu", "btn_auto": "⚙️ Auto-replies", "btn_stats": "📊 Statistics",
                "btn_lang": "🇬🇧 Language: EN", "btn_close": "❌ Close", "stats_title": "📊 Statistics",
                "stats_topics": "📝 Topics:", "stats_purchases": "🛒 Purchases:", "stats_msgs": "💬 Messages:",
                "stats_auto": "🤖 Auto-replies:", "btn_back": "◀️ Back", "auto_title": "⚙️ Auto-reply Settings",
                "auto_status": "Status:", "auto_greeting": "Greeting:", "auto_triggers": "Triggers:",
                "auto_reviews": "Review Replies:", "auto_csv": "CSV Mode:", "btn_turn_on": "🟢 Turn On",
                "btn_turn_off": "🔴 Turn Off", "btn_greeting": "👋 Greeting", "btn_greeting_text": "✏️ Greeting Text",
                "btn_triggers": "📝 Triggers", "btn_reviews": "⭐ Review Replies", "btn_csv": "🎯 CSV Mode",
                "enabled": "Enabled", "disabled": "Disabled", "csv_title": "🎯 CSV Mode", "csv_desc": "Reacts to order options.\nVariables: {option}, {value}, {sum}",
                "csv_rules_empty": "No rules. Add the first one!", "btn_add_rule": "➕ Add rule",
                "csv_rule_title": "🎯 CSV Rule", "csv_option": "📝 Option:", "csv_value": "📊 Value:",
                "csv_type": "🔎 Type:", "csv_register": "🔤 Case sensitive:", "csv_to_user": "👤 To User:", "csv_to_topic": "💬 To Topic:",
                "csv_type_name": "📝 Name only", "csv_type_value": "🎯 Name & Value", "csv_type_contains": "🔍 Value contains",
                "csv_reg_strict": "Strict", "csv_reg_any": "Any", "btn_edit_name": "✏️ Option Name",
                "btn_edit_value": "✏️ Option Value", "btn_edit_user_msg": "✏️ User Message", "btn_edit_topic_msg": "✏️ Topic Message",
                "btn_delete": "🗑 Delete", "triggers_title": "📝 Auto-reply Triggers", "triggers_empty": "Empty. Add a trigger.",
                "btn_add_trigger": "➕ Add trigger", "trigger_edit_title": "⚙️ Trigger", "trigger_phrase": "📝 Phrase:",
                "trigger_answer": "💬 Reply:", "trigger_mode": "🎯 Mode:", "trigger_mode_exact": "Exact match",
                "trigger_mode_contain": "Contains text", "trigger_notify": "🔔 Notification:", "trigger_notify_text": "📢 Text:",
                "btn_edit_phrase": "✏️ Edit Phrase", "btn_edit_answer": "✏️ Edit Reply", "btn_edit_notify_text": "📢 Notify Text",
                "reviews_title": "⭐ Review Auto-replies", "reviews_good": "👍 For good:", "reviews_bad": "👎 For bad:",
                "reviews_text": "Text:", "btn_good_edit": "✏️ Text for good", "btn_bad_edit": "✏️ Text for bad",
                "btn_cancel": "❌ Cancel", "prompt_greeting": "✏️ Send new greeting text:",
                "prompt_trigger_phrase": "✏️ Send trigger phrase (what to react to):", "prompt_trigger_notify": "✏️ Send trigger phrase (with notification):",
                "prompt_edit_phrase": "✏️ Send new trigger phrase:", "prompt_edit_answer": "✏️ Send new reply text:",
                "prompt_notify_text": "Current notification text:\n{current}\n\n✏️ Send new text:",
                "prompt_good_review": "Current text:\n{current}\n\n✏️ Send new reply text for good reviews:",
                "prompt_bad_review": "Current text:\n{current}\n\n✏️ Send new reply text for bad reviews:",
                "prompt_csv_option": "✏️ Enter option name (as in the order):\n\nExample: Tea",
                "prompt_csv_value": "Current value:\n{current}\n\n✏️ Enter option value (user_data):\n\nExample: 20$\n\nSend - to clear",
                "prompt_csv_user": "Current user message:\n{current}\n\n✏️ Enter message for the user:",
                "prompt_csv_topic": "Current topic message:\n{current}\n\n✏️ Enter message for the topic:",
                "prompt_csv_name": "Current option name:\n{current}\n\n✏️ Enter new name:",
                "btn_balance": "💰 My Wallet",
                "btn_refresh": "🔄 Refresh",
                "balance_header": "🏛 **GGSel Seller Balance**\n" + "—" * 15,
                "balance_body": "\n💵 **Total:** `{total} {curr}`\n✅ **Available:** `{avail} {curr}`\n⏳ **On Hold:** `{hold} {curr}`\n\nℹ️ *Funds move to 'Available' once the protection period ends.*",
                "balance_error": "❌ **Error:** Could not connect to GGSel API.",
                "balance_released_alert": (
                    "📈 **BALANCE CHANGE**\n"
                    "🟢 **INCREASE:** `+{amount} {curr}`\n"
                    "💰 **Current balance:**\n"
                    "• Available: `{avail} {curr}`\n"
                    "• Blocked: `{hold} {curr}`\n"
                    "• Limit: `0.00 {curr}`\n"
                    "• Total: `{total} {curr}`\n"
                    "📊 Previous balance: `{prev} {curr}`\n"
                    "🕒 Verification time: `{time}`"
                ),
                # NEW NOTIFICATION KEYS (EMOJIS REMOVED TO PREVENT DOUBLING)
                "noti_new_purchase": "🛒 <b>NEW ORDER!</b>",
                "noti_restored": "🛒 <b>TOPIC RESTORED!</b>",
                "noti_product": "📦 Product:",
                "noti_item_id": "🆔 Product ID:",
                "noti_invoice": "🧾 Order number:",
                "noti_date": "📅 Order date:",
                "noti_prices": "Prices:",
                "noti_details": "Order details:",
                "noti_total": "• Total:",
                "noti_status": "• Status:",
                "noti_profit": "• Seller profit:",
                "noti_buyer_info": "Buyer information:",
                "noti_payment": "• Payment method:",
                "noti_account": "• Account:",
                "noti_email": "• Email:",
                "noti_aggregator": "• Payment aggregator:",
                "noti_options": "Product details:",
                "noti_status_processing": "⚙️ In progress",
                "noti_status_done": "✅ Completed",
                "btn_go_to_order": "Go to order"
            }
        }

    def load_lang(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    return json.load(f).get("lang", "ru")
            except: pass
        return "ru"

    def save_lang(self):
        with open(self.file_path, 'w') as f:
            json.dump({"lang": self.lang}, f)

    def toggle(self):
        self.lang = "en" if self.lang == "ru" else "ru"
        self.save_lang()
        return self.lang

    def get(self, key):
        return self.translations.get(self.lang, self.translations["ru"]).get(key, key)

locales = LocaleManager()
_ = locales.get