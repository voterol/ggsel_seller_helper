# 🤖 GGSel Telegram Bot

An automatic bot for GGSel sellers — monitoring purchases and two-way communication with buyers through Telegram.

## ✨ Features

| Function                     | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| 📦 **Purchase monitoring**   | Automatically creates topics for new orders    |
| 💬 **Two-way communication** | Messages from GGSel ↔ Telegram in real time    |
| 🤖 **Auto-replies**          | Greetings and keyword trigger responses        |
| ⭐ **Review replies**         | Automatic replies to positive/negative reviews |
| 🎯 **CSV mode**              | Reacts to purchase options with conditions     |


---

## 🚀 Installation

### 1. Clone the repository

```bash
git clone https://github.com/paparei/ggsel_seller_helper
cd ggsel_seller_helper/
```

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the .env file

```bash
cp .env.example .env
```

Edit .env and fill in your details:

```env
# GGSel API (from your seller dashboard)
GGSEL_SELLER_ID=1234567
GGSEL_API_KEY=your_api_key_here

# Telegram
TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
TELEGRAM_GROUP_ID=-1001234567890
```

### 5. Настройте Telegram

1. Create a bot via [@BotFather](https://t.me/BotFather) → get the token
2. Create a group and enable Topics in group settings
3. Add the bot to the group
4. Make the bot an administrator with permissions:
   - ✅ Manage topics
   - ✅ Send messages
   - ✅ Delete messages

5. Get the group ID::
   - Add [@userinfobot](https://t.me/userinfobot) to the group
   - Or forward a message from the group to [@userinfobot](https://t.me/userinfobot)
   - The ID will look like `-100xxxxxxxxxx`

### 6. Run the bot

```bash
python main.py
```

---

## ⚙️ Команды бота

| Command    | Where to use | Description                      |
| ---------- | ------------ | -------------------------------- |
| `/menu`    | In group     | Main menu |
| `/history` | In topic     | Load message history             |
| `/options` | In topic     | Show purchase options            |

---

## 🎯 Режим ЧСВ

Автоматическая реакция на опции в заказе.

### Типы сопоставления

| Тип | Описание | Пример |
|-----|----------|--------|
| 📝 `name` | Только по названию | Опция "Чай" — любое значение |
| 🎯 `value` | По названию И значению | Опция "Чай" = "20р" |
| 🔍 `contains` | Значение содержит | Опция "Чай" содержит "20" |

### Variables in messages

- `{option}` — option name
- `{value}` — option value
- `{sum}` — alias for {value}

**Example:** If the buyer selected “Tea: 20₽”, the bot can send: `Thanks for the tea for {sum}! ☕`

---

## 📊 Как это работает

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   GGSel     │────▶│    Bot      │────▶│  Telegram   │
│    API      │◀────│             │◀────│   Группа    │
└─────────────┘     └─────────────┘     └─────────────┘
```

1. The bot checks new purchases through the GGSel API
2. A Telegram topic is created for each purchase
3. Buyer messages are forwarded to the topic
4. Your replies in the topic are sent back to the buyer

---

## 🔧 Run as a service (Linux)

Create `/etc/systemd/system/ggsel-bot.service`:

```ini
[Unit]
Description=GGSel Telegram Bot
After=network.target

[Service]
Type=simple
User=your_user
WorkingDirectory=/path/to/ggsel_bot
ExecStart=/path/to/venv/bin/python main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable ggsel-bot
sudo systemctl start ggsel-bot
```

---

## 📁 Structure

```
ggsel_bot/
├── main.py              # Точка входа
├── bot_service.py       # Основная логика
├── telegram_bot.py      # Telegram API
├── ggsel_api.py         # GGSel API
├── autoresponder.py     # Автоответы
├── .env                 # Секреты (не коммитить!)
└── requirements.txt     # Зависимости
```

---

## ❓ FAQ

**Bot doesn’t create topics**
- Make sure the group is a forum (topics enabled)
- Check bot permissions (admin + manage topics)

**Messages are not sent**
- Check `GGSEL_API_KEY` and `GGSEL_SELLER_ID`
- Look at logs in `ggsel_bot.log`

**How to get a GGSel API key?**
- Seller dashboard → Settings → API

---

## 📝 License

MIT


