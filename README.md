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
git clone https://github.com/voterol/ggsel_seller_helper.git
cd ggsel_seller_helper/
```

### Optional verified automatic updates

Automatic updating is disabled unless it is explicitly opted into. Updates are
accepted only from a named release tag whose downloaded ZIP matches a pinned
SHA-256 digest. The updater never downloads the mutable `main.zip` archive.

```env
AUTO_UPDATE=true
UPDATE_VERSION=1.2.3
UPDATE_SHA256=<64 lowercase or uppercase hexadecimal characters>
```

Obtain the checksum for the exact release asset before enabling it, for example:

```bash
curl -fL -o ggsel-update.zip \
  https://github.com/voterol/ggsel_seller_helper/archive/refs/tags/1.2.3.zip
shasum -a 256 ggsel-update.zip
```

Installation is attempted **only during process startup**, before file logging,
`BotService` construction, database migration, or any SQLite connection. The
two-hour periodic task is notification-only: it checks the pinned tag but never
downloads, extracts, swaps files, or restarts the running bot. A newly noticed
release is therefore downloaded, hash-verified, and installed on a later
service startup, not while SQLite is active.

On successful startup installation, the previous application directory is
retained beside the install as `ggsel_seller_helper.update-backup`, and the
process exits so the service manager starts the new code cleanly. Runtime state
is preserved by this explicit allowlist:

- `.env`;
- `venv/` and `.venv/`;
- `bot_lang.json`, `orders.json`, `autoresponder.json`, and
  `autoresponder_config.json`;
- `topics.json`, `pending_topics.json`, `processed_reviews.json`,
  `processed_purchases.json`, and `processed_messages.json`;
- `ggsel_bot.log`;
- the configured in-tree `DATABASE_PATH` plus SQLite `-wal`, `-shm`, and
  `-journal` sidecars.

An absolute database path outside the deploy directory is not touched. Relative
database paths that escape the deploy directory, symlinked in-tree database
paths, and paths inside updater backup directories are rejected before swap.
Keep independent backups; this allowlist intentionally does not preserve
arbitrary files added to the application directory.

### 2. Create a virtual environment (recommended)

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

For a production service with automatic updates, prefer a virtual environment
outside the replaceable deploy directory, for example `/opt/ggsel-venv`. The
updater preserves an existing in-tree directory named `venv` for compatibility,
but keeping the interpreter outside the deploy makes the service lifecycle
independent of an application-directory swap.

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Create the .env file

For a new installation, use the interactive setup. It asks for the required
GGSel and Telegram values, a non-empty operator allowlist, and an optional HTTP
or SOCKS5 proxy. Secret values and proxy passwords are entered without echo.
The resulting file is replaced atomically with mode `0600`; an existing `.env`
is not replaced without explicit confirmation.

```bash
python setup.py
```

Alternatively, create it manually:

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
TELEGRAM_ALLOWED_USER_IDS=123456789,987654321
# Optional; omit for a direct connection:
# TELEGRAM_PROXY_URL=socks5://username:password@127.0.0.1:1080
```

`TELEGRAM_ALLOWED_USER_IDS` is a comma- or space-separated allowlist of
positive personal Telegram user IDs. If this setting is missing or empty, the
bot starts but denies every operator action. In the configured group, use
`/id` or `/myid` to display your personal numeric ID, then add it to the
allowlist and restart the bot. These commands only display an ID and never
grant access. Group membership or administrator status alone never grants
operator access.

`TELEGRAM_PROXY_URL` is optional, so existing `.env` files continue to work.
Only `http://` and `socks5://` URLs with an explicit host and port are accepted.
When set, the proxy is used for both polling (`getUpdates`) and all other Bot
API calls. Percent-encode special characters in credentials and never put the
URL in logs or support messages because it may contain a password.

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
| `/id`, `/myid` | In group | Show your personal Telegram user ID |
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
ExecStart=/opt/ggsel-venv/bin/python main.py
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


