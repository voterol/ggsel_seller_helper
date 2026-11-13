import asyncio
import logging
import signal
import sys
import os
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import setup
from config import Config, reload_env
from bot_service import BotService
from auto_updater import check_and_update, check_update_available, get_current_version

# How often to check GitHub for updates (2 hours)
UPDATE_CHECK_INTERVAL = 7200


def ensure_config() -> Optional[Config]:
    """Return a validated Config, launching the setup wizard when .env is missing.

    In non-interactive environments the wizard cannot ask questions, so we only
    log instructions and return None for the caller to exit.
    """
    try:
        return Config.from_env()
    except ValueError as e:
        logging.error(f"Конфигурация не найдена или неполна: {e}")

    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        logging.error(
            "Запустите 'python setup.py' в интерактивном терминале или создайте "
            ".env по образцу .env.example."
        )
        return None

    try:
        result = setup.main()
    except (KeyboardInterrupt, EOFError, ValueError):
        # Ctrl+C/Ctrl+D и ошибки ввода мастера (например, невалидный proxy host)
        # не должны выглядеть как сбой приложения.
        logging.error("Мастер настройки прерван. Создайте .env по образцу .env.example и перезапустите бота.")
        return None
    if result == 0 and setup.ENV_PATH.is_file():
        # override=True: файл только что написан пользователем интерактивно,
        # его значения авторитетнее устаревших переменных окружения (даже пустых).
        reload_env(override=True)
        try:
            return Config.from_env()
        except ValueError as e:
            logging.error(f"После мастера настройки конфигурация всё ещё неполна: {e}")
            return None
    logging.error("Мастер настройки не завершился. Создайте .env по образцу .env.example и перезапустите бота.")
    return None


async def update_checker(auto_update_enabled: bool, version: str, sha256: str):
    """Periodically notify only; never download, install, or restart."""
    if not auto_update_enabled:
        return
    while True:
        await asyncio.sleep(UPDATE_CHECK_INTERVAL)
        try:
            logging.info("🔄 Checking for updates...")
            message = await check_update_available(auto_update_enabled, version, sha256)
            logging.info(f"ℹ️ {message}")
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
    # The log contains buyer emails; keep it owner-only when possible.
    try:
        os.chmod('ggsel_bot.log', 0o600)
    except OSError as e:
        logging.warning(f"Could not restrict log file permissions (ggsel_bot.log): {e}")
    # Silence noisy libraries
    logging.getLogger('httpx').setLevel(logging.WARNING)
    logging.getLogger('telegram').setLevel(logging.WARNING)
    logging.getLogger('telegram.ext').setLevel(logging.WARNING)
    # --- Add this line below to silence the GGSel connection drops ---
    logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)

async def main():
    try:
        # Setup must complete before any network or update operation.
        config = ensure_config()
        if config is None:
            sys.exit(1)
        config.validate()
        logging.info(f"🚀 GGSel Bot v{get_current_version()}")

        # Deny by default even with legacy Config defaults: AUTO_UPDATE must be
        # explicitly present and true, and the release must be pinned + hashed.
        explicit_auto_update = os.getenv('AUTO_UPDATE', '').strip().lower() in ('true', '1', 'yes')
        auto_update_enabled = bool(config.auto_update and explicit_auto_update)
        update_version = os.getenv('UPDATE_VERSION', '').strip()
        update_sha256 = os.getenv('UPDATE_SHA256', '').strip()
        
        # Initial update check on boot
        if auto_update_enabled:
            needs_restart, message = await check_and_update(
                auto_update_enabled, update_version, update_sha256, config.database_path
            )
            if needs_restart:
                sys.exit(1)

        # File logging and service construction happen only after the startup
        # installer has finished. In particular, BotService.__init__ opens and
        # migrates SQLite, so it must remain below this boundary.
        setup_logging()
        logging.info(f"ℹ️ {message}" if auto_update_enabled else "ℹ️ Automatic updates are disabled")

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
            asyncio.create_task(update_checker(
                auto_update_enabled, update_version, update_sha256
            ))
            
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
