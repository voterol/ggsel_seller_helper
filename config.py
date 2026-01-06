import os
from dataclasses import dataclass
from typing import Optional

# Попытка загрузить .env файл только из папки ggsel_bot
try:
    from dotenv import load_dotenv
    import os
    
    # Ищем .env файл только в текущей директории (ggsel_bot/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, '.env')
    
    load_dotenv(env_path)
        
except ImportError:
    # Если python-dotenv не установлен, продолжаем без него
    pass

@dataclass
class Config:
    # GGSel API (обязательные поля)
    ggsel_seller_id: int
    ggsel_api_key: str
    telegram_bot_token: str
    telegram_group_id: int
    
    # Опциональные поля с значениями по умолчанию
    ggsel_base_url: str = "https://seller.ggsel.net/api_sellers/api"
    ggsel_fallback_urls: list = None  # Резервные URL для API
    database_path: str = "ggsel_bot.db"
    poll_interval: int = 15  # секунды для проверки сообщений
    chat_check_interval: int = 40  # секунды для проверки новых чатов
    
    # Настройки таймаутов и повторных попыток
    telegram_timeout: int = 30  # таймаут для Telegram API
    max_retries: int = 3  # максимальное количество повторных попыток
    retry_delay: int = 5  # задержка между попытками в секундах
    
    # Мониторинг заказов (опционально)
    orders_group_id: Optional[int] = None  # ID группы для мониторинга заказов
    
    @classmethod
    def from_env(cls) -> 'Config':
        # Резервные URL для API (на случай проблем с DNS на хосте)
        fallback_urls = [
            "https://seller.ggsel.net/api_sellers/api",
            # Используем IP адреса вместо доменов для обхода DNS проблем
            "https://185.178.208.140/api_sellers/api",  # IP GGSel (нужно узнать реальный)
            "https://5.188.84.8/api_sellers/api",       # Альтернативный IP
            # Публичные DNS серверы через прокси (если есть)
            "https://api.ggsel.net/api_sellers/api",
            "https://ggsel.net/api_sellers/api"
        ]
        
        return cls(
            ggsel_seller_id=int(os.getenv('GGSEL_SELLER_ID', '0')),
            ggsel_api_key=os.getenv('GGSEL_API_KEY', ''),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            telegram_group_id=int(os.getenv('TELEGRAM_GROUP_ID', '0')),
            ggsel_fallback_urls=fallback_urls,
            database_path=os.getenv('DATABASE_PATH', 'ggsel_bot.db'),
            poll_interval=int(os.getenv('POLL_INTERVAL', '15')),
            chat_check_interval=int(os.getenv('CHAT_CHECK_INTERVAL', '40')),
            telegram_timeout=int(os.getenv('TELEGRAM_TIMEOUT', '30')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '5')),
            orders_group_id=int(os.getenv('ORDERS_GROUP_ID', '0')) if os.getenv('ORDERS_GROUP_ID') else None
        )