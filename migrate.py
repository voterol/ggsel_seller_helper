import sqlite3
import json
import os

DB_PATH = "ggsel_bot.db"

def migrate():
    print("Starting Bulletproof Migration...")
    
    # Force delete any corrupted database
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"🗑️ Deleted old {DB_PATH}")
        
    with sqlite3.connect(DB_PATH) as conn:
        # 1. FORCE CREATE EXACT NEW SCHEMA
        conn.execute('''CREATE TABLE IF NOT EXISTS chats (
            id_i INTEGER PRIMARY KEY, email TEXT, product INTEGER,
            last_message TEXT, cnt_msg INTEGER, cnt_new INTEGER,
            telegram_topic_id INTEGER, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )''')
        conn.execute('''CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER, message_id TEXT UNIQUE,
            content TEXT, timestamp TIMESTAMP, is_sent_to_telegram BOOLEAN DEFAULT FALSE,
            FOREIGN KEY (chat_id) REFERENCES chats (id_i)
        )''')
        conn.execute('CREATE TABLE IF NOT EXISTS topics (key TEXT PRIMARY KEY, data TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS purchases (invoice_id TEXT PRIMARY KEY, data TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS processed_reviews (review_id TEXT PRIMARY KEY, hash TEXT)')
        conn.execute('CREATE TABLE IF NOT EXISTS pending_topics (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)')
        
        print("✅ New Schema Created.")

        # 2. MIGRATE PURCHASES
        if os.path.exists("processed_purchases.json"):
            with open("processed_purchases.json", "r", encoding="utf-8") as f:
                purchases = json.load(f)
                count = 0
                for inv_id, data in purchases.items():
                    conn.execute("INSERT OR IGNORE INTO purchases (invoice_id, data) VALUES (?, ?)", (str(inv_id), json.dumps(data)))
                    count += 1
                print(f"✅ Migrated {count} purchases.")

        # 3. MIGRATE TOPICS
        if os.path.exists("topics.json"):
            with open("topics.json", "r", encoding="utf-8") as f:
                topics = json.load(f)
                count = 0
                for key, data in topics.items():
                    conn.execute("INSERT OR IGNORE INTO topics (key, data) VALUES (?, ?)", (str(key), json.dumps(data)))
                    count += 1
                print(f"✅ Migrated {count} topics.")

        # 4. MIGRATE MESSAGES
        if os.path.exists("processed_messages.json"):
            with open("processed_messages.json", "r", encoding="utf-8") as f:
                msgs = json.load(f)
                count = 0
                for key, data in msgs.items():
                    chat_id = data.get("chat_id", 0)
                    msg_id = str(data.get("message_id", ""))
                    content = data.get("content", "")
                    ts = data.get("timestamp", data.get("processed_at", ""))
                    sent = data.get("sent_to_telegram", data.get("is_sent_to_telegram", True))
                    
                    conn.execute("INSERT OR IGNORE INTO chats (id_i) VALUES (?)", (chat_id,))
                    conn.execute("INSERT OR IGNORE INTO messages (chat_id, message_id, content, timestamp, is_sent_to_telegram) VALUES (?, ?, ?, ?, ?)", 
                                 (chat_id, msg_id, content, ts, sent))
                    count += 1
                print(f"✅ Migrated {count} messages.")

        # 5. MIGRATE REVIEWS
        if os.path.exists("processed_reviews.json"):
            with open("processed_reviews.json", "r", encoding="utf-8") as f:
                reviews = json.load(f)
                count = 0
                if isinstance(reviews, list):
                    for rev_id in reviews:
                        conn.execute("INSERT OR IGNORE INTO processed_reviews (review_id, hash) VALUES (?, ?)", (str(rev_id), ""))
                        count += 1
                else:
                    for rev_id, hash_val in reviews.items():
                        conn.execute("INSERT OR IGNORE INTO processed_reviews (review_id, hash) VALUES (?, ?)", (str(rev_id), str(hash_val)))
                        count += 1
                print(f"✅ Migrated {count} reviews.")

    print("🎉 Migration complete! You can now delete the JSON files.")

if __name__ == "__main__":
    migrate()
