#!/usr/bin/env python3
"""
Скрипт для исправления дублированных записей в processed_messages.json
после исправления бага с chat_id=0
"""

import json
import os
from datetime import datetime

def fix_processed_messages():
    """Исправляет дублированные записи в processed_messages.json"""
    
    messages_file = "processed_messages.json"
    backup_file = f"processed_messages_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    if not os.path.exists(messages_file):
        print(f"❌ Файл {messages_file} не найден")
        return
    
    # Создаем бэкап
    try:
        with open(messages_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Создан бэкап: {backup_file}")
    except Exception as e:
        print(f"❌ Ошибка создания бэкапа: {e}")
        return
    
    # Анализируем данные
    print(f"\n📊 Анализ данных:")
    print(f"Всего записей: {len(data)}")
    
    # Ищем записи с chat_id=0
    zero_chat_records = {}
    normal_records = {}
    
    for key, record in data.items():
        chat_id = record.get('chat_id', 0)
        message_id = record.get('message_id', '')
        
        if chat_id == 0:
            zero_chat_records[key] = record
        else:
            normal_records[key] = record
    
    print(f"Записей с chat_id=0: {len(zero_chat_records)}")
    print(f"Нормальных записей: {len(normal_records)}")
    
    if not zero_chat_records:
        print("✅ Записей с chat_id=0 не найдено, исправление не требуется")
        return
    
    # Удаляем записи с chat_id=0
    cleaned_data = normal_records.copy()
    
    # Сохраняем исправленные данные
    try:
        with open(messages_file, 'w', encoding='utf-8') as f:
            json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Исправлено!")
        print(f"Удалено записей с chat_id=0: {len(zero_chat_records)}")
        print(f"Осталось записей: {len(cleaned_data)}")
        
    except Exception as e:
        print(f"❌ Ошибка сохранения: {e}")
        # Восстанавливаем из бэкапа
        try:
            with open(backup_file, 'r', encoding='utf-8') as f:
                backup_data = json.load(f)
            with open(messages_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)
            print(f"🔄 Восстановлено из бэкапа")
        except:
            print(f"💥 Критическая ошибка! Восстановите вручную из {backup_file}")

def show_statistics():
    """Показывает статистику по сообщениям"""
    
    messages_file = "processed_messages.json"
    
    if not os.path.exists(messages_file):
        print(f"❌ Файл {messages_file} не найден")
        return
    
    try:
        with open(messages_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"❌ Ошибка чтения файла: {e}")
        return
    
    print(f"\n📈 Статистика processed_messages.json:")
    print(f"Всего записей: {len(data)}")
    
    # Группируем по chat_id
    chat_stats = {}
    sent_count = 0
    
    for key, record in data.items():
        chat_id = record.get('chat_id', 0)
        sent_to_telegram = record.get('sent_to_telegram', False)
        
        if chat_id not in chat_stats:
            chat_stats[chat_id] = 0
        chat_stats[chat_id] += 1
        
        if sent_to_telegram:
            sent_count += 1
    
    print(f"Отправлено в Telegram: {sent_count}")
    print(f"Не отправлено: {len(data) - sent_count}")
    
    print(f"\nПо чатам:")
    for chat_id, count in sorted(chat_stats.items()):
        if chat_id == 0:
            print(f"  Chat ID 0 (ПРОБЛЕМА!): {count} записей")
        else:
            print(f"  Chat ID {chat_id}: {count} записей")

if __name__ == "__main__":
    print("🔧 Исправление дублированных записей в GGSel Bot")
    print("=" * 50)
    
    show_statistics()
    
    print("\n" + "=" * 50)
    response = input("Исправить записи с chat_id=0? (y/N): ").strip().lower()
    
    if response in ['y', 'yes', 'да']:
        fix_processed_messages()
        print("\n" + "=" * 50)
        show_statistics()
    else:
        print("❌ Отменено")