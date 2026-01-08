import asyncio
import logging
import signal
import sys
import os

# Добавляем текущую директорию в путь для импортов
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from bot_service import BotService
from auto_updater import check_and_update, restart_bot, get_current_version

# Интервал принудительного рестарта (1 час = 3600 секунд)
HARD_RESTART_INTERVAL = 3600
# Интервал проверки обновлений (2 часа)
UPDATE_CHECK_INTERVAL = 7200

async def hard_restart_scheduler():
    """Принудительный рестарт через час для очистки памяти и pm2"""
    await asyncio.sleep(HARD_RESTART_INTERVAL)
    logging.info(f"⚡ Принудительный рестарт через {HARD_RESTART_INTERVAL}с - pm2 поднимет бота")
    os._exit(1)  # Жесткий выход, pm2 перезапустит


async def update_checker(auto_update_enabled: bool):
    """Периодическая проверка обновлений каждые 2 часа"""
    while True:
        await asyncio.sleep(UPDATE_CHECK_INTERVAL)
        
        if not auto_update_enabled:
            continue
        
        try:
            logging.info("🔄 Периодическая проверка обновлений...")
            needs_restart, message = await check_and_update(auto_update_enabled)
            logging.info(f"📦 {message}")
            
            if needs_restart:
                logging.info("🔄 Перезапуск для применения обновления...")
                restart_bot()
        except Exception as e:
            logging.error(f"Ошибка проверки обновлений: {e}")

def setup_logging():
    """Настройка логирования"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ggsel_bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Отключаем спам от httpx и telegram
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('telegram.ext').setLevel(logging.WARNING)

async def main():
    """Главная функция"""
    setup_logging()
    
    try:
        # Загружаем конфигурацию
        config = Config.from_env()
        
        # Проверяем обновления при запуске
        logging.info(f"🤖 GGSel Bot v{get_current_version()}")
        
        if config.auto_update:
            logging.info("🔄 Проверка обновлений...")
            needs_restart, message = await check_and_update(config.auto_update)
            logging.info(f"📦 {message}")
            
            if needs_restart:
                logging.info("🔄 Перезапуск для применения обновления...")
                restart_bot()
        else:
            logging.info("⏸️ Автообновление отключено")
        
        # Проверяем обязательные параметры
        if not config.ggsel_api_key:
            logging.error("GGSEL_API_KEY не установлен")
            return
        
        if not config.telegram_bot_token:
            logging.error("TELEGRAM_BOT_TOKEN не установлен")
            return
        
        if not config.telegram_group_id:
            logging.error("TELEGRAM_GROUP_ID не установлен")
            return
        
        # Создаем и запускаем бот
        bot_service = BotService(config)
        
        # Обработка сигналов для корректного завершения
        def signal_handler(signum, frame):
            logging.info(f"Получен сигнал {signum}, завершение работы...")
            bot_service.stop_sync()
            # Принудительно завершаем через 5 секунд если не остановился
            asyncio.get_event_loop().call_later(5, lambda: os._exit(0))
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Запускаем бота
        try:
            # Запускаем таймер принудительного рестарта
            asyncio.create_task(hard_restart_scheduler())
            logging.info(f"⏰ Запланирован рестарт через {HARD_RESTART_INTERVAL}с")
            
            # Запускаем периодическую проверку обновлений
            asyncio.create_task(update_checker(config.auto_update))
            
            await bot_service.start()
        except KeyboardInterrupt:
            logging.info("Получен KeyboardInterrupt")
        except Exception as e:
            logging.error(f"Ошибка в работе бота: {e}")
        finally:
            await bot_service.stop()
        
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())