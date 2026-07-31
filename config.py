import os
from dataclasses import dataclass
from typing import FrozenSet, Optional
from urllib.parse import unquote, urlsplit

# Попытка загрузить .env файл только из папки ggsel_bot
try:
    from dotenv import load_dotenv
    # Ищем .env файл только в текущей директории (ggsel_bot/)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(current_dir, '.env')
    
    # Credentials are literal values; do not expand ${NAME} inside secrets.
    load_dotenv(env_path, interpolate=False)
        
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
    ggsel_base_url: str = "https://seller.ggsel.com/api_sellers/api"
    database_path: str = "ggsel_bot.db"
    poll_interval: int = 15  # секунды для проверки сообщений
    chat_check_interval: int = 40  # секунды для проверки новых чатов
    
    # Настройки таймаутов и повторных попыток
    telegram_timeout: int = 30  # таймаут для Telegram API
    max_retries: int = 3  # максимальное количество повторных попыток
    retry_delay: int = 5  # задержка между попытками в секундах
    
    # Автообновление
    auto_update: bool = True  # автоматическое обновление с GitHub

    # Kept after existing optional fields to preserve positional construction.
    # Empty intentionally authorizes nobody; group membership is insufficient.
    telegram_allowed_user_ids: FrozenSet[int] = frozenset()
    # Seller API transport settings. Kept at the end for positional callers.
    ggsel_connect_timeout: float = 5.0
    ggsel_read_timeout: float = 30.0
    # Optional proxy used only for Telegram Bot API traffic.
    telegram_proxy_url: Optional[str] = None

    def validate(self) -> None:
        """Raise a useful startup error rather than failing later in a worker."""
        missing = []
        if self.ggsel_seller_id <= 0:
            missing.append('GGSEL_SELLER_ID (positive integer)')
        if not self.ggsel_api_key.strip():
            missing.append('GGSEL_API_KEY')
        if not self.telegram_bot_token.strip():
            missing.append('TELEGRAM_BOT_TOKEN')
        if self.telegram_group_id == 0:
            missing.append('TELEGRAM_GROUP_ID (non-zero integer)')
        if not isinstance(self.ggsel_base_url, str) or not self.ggsel_base_url.strip():
            missing.append('GGSEL_BASE_URL (HTTPS URL)')
        if not 0 < self.ggsel_connect_timeout <= 300:
            missing.append('GGSEL_CONNECT_TIMEOUT (0-300 seconds)')
        if not 0 < self.ggsel_read_timeout <= 300:
            missing.append('GGSEL_READ_TIMEOUT (0-300 seconds)')
        if self.telegram_proxy_url is not None:
            self._validate_telegram_proxy_url(self.telegram_proxy_url)
        if missing:
            raise ValueError("Missing or invalid required configuration: " + ", ".join(missing))

    @staticmethod
    def _validate_telegram_proxy_url(value: str) -> None:
        """Validate without ever copying proxy credentials into an error."""
        error = "Invalid TELEGRAM_PROXY_URL: expected http:// or socks5:// with host and port"
        if not value or value != value.strip() or any(char.isspace() for char in value):
            raise ValueError("Invalid TELEGRAM_PROXY_URL")
        try:
            parsed = urlsplit(value)
            # Accessing port performs urllib's numeric/range validation.
            port = parsed.port
            hostname = parsed.hostname
            username = parsed.username
            password = parsed.password
        except (ValueError, UnicodeError) as exc:
            raise ValueError("Invalid TELEGRAM_PROXY_URL") from exc
        if (
            parsed.scheme.lower() not in {"http", "socks5"}
            or not hostname
            or port is None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(error)

        # urllib accepts malformed percent escapes. Reject them explicitly so
        # credentials have one unambiguous representation at the HTTP client.
        for credential in (username, password):
            if credential is None:
                continue
            index = 0
            while index < len(credential):
                if credential[index] == "%":
                    if index + 2 >= len(credential) or any(
                        char not in "0123456789abcdefABCDEF"
                        for char in credential[index + 1:index + 3]
                    ):
                        raise ValueError(error)
                    index += 3
                else:
                    index += 1
            try:
                decoded = unquote(credential, errors="strict")
            except UnicodeDecodeError as exc:
                raise ValueError(error) from exc
            if any(ord(char) < 32 or ord(char) == 127 for char in decoded):
                raise ValueError(error)

    @staticmethod
    def _required_int(name: str) -> int:
        value = os.getenv(name, '').strip()
        if not value:
            raise ValueError(f"Missing required configuration: {name}")
        try:
            return int(value)
        except ValueError as exc:
            raise ValueError(f"Invalid {name}: expected an integer, got {value!r}") from exc

    @staticmethod
    def _allowed_user_ids(value: str) -> FrozenSet[int]:
        if not value.strip():
            return frozenset()
        values = value.replace(',', ' ').split()
        try:
            ids = frozenset(int(item) for item in values)
        except ValueError as exc:
            raise ValueError(
                "Invalid TELEGRAM_ALLOWED_USER_IDS: expected comma- or space-separated integers"
            ) from exc
        if any(user_id <= 0 for user_id in ids):
            raise ValueError("Invalid TELEGRAM_ALLOWED_USER_IDS: user IDs must be positive")
        return ids
    
    @classmethod
    def from_env(cls) -> 'Config':
        config = cls(
            ggsel_seller_id=cls._required_int('GGSEL_SELLER_ID'),
            ggsel_api_key=os.getenv('GGSEL_API_KEY', ''),
            telegram_bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            telegram_group_id=cls._required_int('TELEGRAM_GROUP_ID'),
            telegram_allowed_user_ids=cls._allowed_user_ids(
                os.getenv('TELEGRAM_ALLOWED_USER_IDS', '')
            ),
            telegram_proxy_url=os.getenv('TELEGRAM_PROXY_URL', '').strip() or None,
            ggsel_base_url=os.getenv(
                'GGSEL_BASE_URL', 'https://seller.ggsel.com/api_sellers/api'
            ),
            ggsel_connect_timeout=float(os.getenv('GGSEL_CONNECT_TIMEOUT', '5')),
            ggsel_read_timeout=float(os.getenv('GGSEL_READ_TIMEOUT', '30')),
            database_path=os.getenv('DATABASE_PATH', 'ggsel_bot.db'),
            poll_interval=int(os.getenv('POLL_INTERVAL', '15')),
            chat_check_interval=int(os.getenv('CHAT_CHECK_INTERVAL', '40')),
            telegram_timeout=int(os.getenv('TELEGRAM_TIMEOUT', '30')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay=int(os.getenv('RETRY_DELAY', '5')),
            auto_update=os.getenv('AUTO_UPDATE', 'true').lower() in ('true', '1', 'yes')
        )
        config.validate()
        return config
