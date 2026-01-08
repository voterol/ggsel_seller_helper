"""Модуль автообновления бота"""
import os
import sys
import re
import logging
import zipfile
import shutil
import tempfile
import asyncio
import aiohttp
from typing import Optional, Tuple

# URL для проверки версии и скачивания
VERSION_URL = "https://raw.githubusercontent.com/voterol/ggsel_seller_helper/main/__init__.py"
REPO_ZIP_URL = "https://github.com/voterol/ggsel_seller_helper/archive/refs/heads/main.zip"

# Текущая директория бота
BOT_DIR = os.path.dirname(os.path.abspath(__file__))


def get_current_version() -> str:
    """Получить текущую версию из __init__.py"""
    try:
        init_file = os.path.join(BOT_DIR, "__init__.py")
        if os.path.exists(init_file):
            with open(init_file, 'r', encoding='utf-8') as f:
                content = f.read()
                match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
                if match:
                    return match.group(1)
    except Exception as e:
        logging.error(f"Ошибка чтения версии: {e}")
    return "0.0.0"


async def get_remote_version() -> Optional[str]:
    """Получить версию с GitHub"""
    try:
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(VERSION_URL) as resp:
                if resp.status == 200:
                    content = await resp.text()
                    match = re.search(r'__version__\s*=\s*["\']([^"\']+)["\']', content)
                    if match:
                        return match.group(1)
    except Exception as e:
        logging.error(f"Ошибка получения версии с GitHub: {e}")
    return None


async def download_and_extract_update() -> bool:
    """Скачать и распаковать обновление"""
    try:
        timeout = aiohttp.ClientTimeout(total=120)
        
        # Создаём временную директорию
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "update.zip")
            
            # Скачиваем архив
            logging.info("Скачивание обновления...")
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(REPO_ZIP_URL) as resp:
                    if resp.status != 200:
                        logging.error(f"Ошибка скачивания: HTTP {resp.status}")
                        return False
                    
                    with open(zip_path, 'wb') as f:
                        while True:
                            chunk = await resp.content.read(8192)
                            if not chunk:
                                break
                            f.write(chunk)
            
            logging.info("Распаковка обновления...")
            
            # Распаковываем
            extract_dir = os.path.join(temp_dir, "extracted")
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            # Находим папку с исходниками (обычно repo-main/ggsel_bot или просто repo-main)
            extracted_items = os.listdir(extract_dir)
            if not extracted_items:
                logging.error("Архив пустой")
                return False
            
            source_dir = os.path.join(extract_dir, extracted_items[0])
            
            # Если внутри есть папка ggsel_bot - используем её
            ggsel_bot_dir = os.path.join(source_dir, "ggsel_bot")
            if os.path.exists(ggsel_bot_dir):
                source_dir = ggsel_bot_dir
            
            logging.info("Обновление файлов...")
            
            # Файлы которые НЕ нужно перезаписывать
            skip_files = {
                '.env',
                '.env.example',
                'readme.md',
                'README.md',
                'topics.json',
                'processed_reviews.json',
                'processed_purchases.json',
                'processed_messages.json',
                'pending_topics.json',
                'autoresponder.json',
                'ggsel_bot.db'
            }
            
            # Копируем все файлы кроме исключённых
            for item in os.listdir(source_dir):
                if item in skip_files or item.lower() in skip_files:
                    continue
                
                src = os.path.join(source_dir, item)
                dst = os.path.join(BOT_DIR, item)
                
                try:
                    if os.path.isfile(src):
                        shutil.copy2(src, dst)
                        logging.debug(f"Обновлён: {item}")
                    elif os.path.isdir(src):
                        if os.path.exists(dst):
                            shutil.rmtree(dst)
                        shutil.copytree(src, dst)
                        logging.debug(f"Обновлена папка: {item}")
                except Exception as e:
                    logging.error(f"Ошибка копирования {item}: {e}")
            
            logging.info("Обновление завершено!")
            return True
            
    except Exception as e:
        logging.error(f"Ошибка обновления: {e}")
        return False


async def check_and_update(auto_update_enabled: bool = True) -> Tuple[bool, str]:
    """
    Проверить обновления и установить если есть
    
    Returns:
        (needs_restart, message)
    """
    if not auto_update_enabled:
        return False, "Автообновление отключено"
    
    current = get_current_version()
    logging.info(f"Текущая версия: {current}")
    
    remote = await get_remote_version()
    if not remote:
        return False, "Не удалось проверить обновления"
    
    logging.info(f"Версия на GitHub: {remote}")
    
    if current == remote:
        return False, f"Версия актуальна: {current}"
    
    logging.info(f"Доступно обновление: {current} -> {remote}")
    
    # Скачиваем и устанавливаем
    if await download_and_extract_update():
        return True, f"Обновлено: {current} -> {remote}"
    
    return False, "Ошибка установки обновления"


def restart_bot():
    """Перезапуск бота через exit code 1"""
    logging.info("Перезапуск бота...")
    sys.exit(1)
