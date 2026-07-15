import asyncio
import logging
import signal
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config
from bot_service import BotService
from auto_updater import check_and_update, get_current_version

# How often to check GitHub for updates (2 hours)
UPDATE_CHECK_INTERVAL = 7200

async def update_checker(auto_update_enabled: bool):
    """Update check loop"""
    if not auto_update_enabled:
        return
    while True:
        await asyncio.sleep(UPDATE_CHECK_INTERVAL)
        try:
            logging.info("🔄 Checking for updates...")
            needs_restart, message = await check_and_update(auto_update_enabled)
            logging.info(f"ℹ️ {message}")
            if needs_restart:
                logging.info("🔄 Restarting for update...")
                sys.exit(1)
        except Exception as e:
            logging.error(f"Update check error: {e}")

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('ggsel_bot.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    # Silence noisy libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('telegram.ext').setLevel(logging.WARNING)
    # --- Add this line below to silence the GGSel connection drops ---
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

async def main():
    setup_logging()
    try:
        config = Config.from_env()
        logging.info(f"🚀 GGSel Bot v{get_current_version()}")
        
        # Initial update check on boot
        if config.auto_update:
            needs_restart, message = await check_and_update(config.auto_update)
            if needs_restart:
                sys.exit(1)

        if not all([config.ggsel_api_key, config.telegram_bot_token, config.telegram_group_id]):
            logging.error("Missing required config parameters. Check your .env file.")
            sys.exit(1)

        bot_service = BotService(config)
        
        def signal_handler(signum, frame):
            logging.info("Shutting down safely...")
            bot_service.stop_sync()
            asyncio.get_event_loop().call_later(5, lambda: sys.exit(0))
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        try:
            # Start background tasks
            asyncio.create_task(update_checker(config.auto_update))
            
            # Start main bot service
            await bot_service.start()
        except KeyboardInterrupt:
            pass
        finally:
            await bot_service.stop()
            
    except Exception as e:
        logging.error(f"Critical error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())