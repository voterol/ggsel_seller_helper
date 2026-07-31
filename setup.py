"""Interactive, dependency-free initial .env setup."""

from getpass import getpass
import os
from pathlib import Path
import tempfile
from typing import Dict, Optional
from urllib.parse import quote

from config import Config


ENV_PATH = Path(__file__).resolve().with_name(".env")


def _required(prompt: str, *, secret: bool = False) -> str:
    reader = getpass if secret else input
    while True:
        value = reader(prompt).strip()
        if value and "\n" not in value and "\r" not in value:
            return value
        print("Значение обязательно и должно занимать одну строку.")


def _integer(prompt: str, *, positive: bool, nonzero: bool = False) -> str:
    while True:
        value = _required(prompt)
        try:
            number = int(value)
        except ValueError:
            print("Введите целое число.")
            continue
        if (positive and number <= 0) or (nonzero and number == 0):
            print("Недопустимое числовое значение.")
            continue
        return str(number)


def _yes_no(prompt: str, *, default: bool = False) -> bool:
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        answer = input(prompt + suffix).strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes", "д", "да"}:
            return True
        if answer in {"n", "no", "н", "нет"}:
            return False
        print("Ответьте yes или no.")


def _allowed_ids() -> str:
    while True:
        value = _required("Разрешённые Telegram user ID (через запятую): ")
        try:
            ids = Config._allowed_user_ids(value)
        except ValueError as exc:
            print(str(exc))
            continue
        if ids:
            return ",".join(str(item) for item in sorted(ids))


def _proxy_url() -> Optional[str]:
    if not _yes_no("Нужен Telegram proxy?"):
        return None
    while True:
        kind = input("Тип proxy (HTTP/SOCKS): ").strip().lower()
        if kind in {"http", "socks", "socks5"}:
            scheme = "http" if kind == "http" else "socks5"
            break
        print("Выберите HTTP или SOCKS.")
    host = _required("Proxy host: ")
    if any(char in host for char in "/@?#"):
        raise ValueError("Proxy host должен быть именем хоста или IP-адресом")
    port = _integer("Proxy port: ", positive=True)
    if int(port) > 65535:
        raise ValueError("Proxy port должен быть в диапазоне 1-65535")

    auth = ""
    if _yes_no("Proxy требует логин/пароль?"):
        username = _required("Proxy username: ")
        password = getpass("Proxy password (может быть пустым): ")
        if "\n" in password or "\r" in password:
            raise ValueError("Proxy password должен занимать одну строку")
        auth = f"{quote(username, safe='')}:{quote(password, safe='')}@"
    url = f"{scheme}://{auth}{host}:{port}"
    Config._validate_telegram_proxy_url(url)
    return url


def _env_value(value: str) -> str:
    """Double-quote dotenv values to prevent comments or expansion."""
    if "\n" in value or "\r" in value:
        raise ValueError(".env values must be single-line")
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$") + '"'


def write_env_atomic(path: Path, values: Dict[str, str]) -> None:
    content = "".join(f"{key}={_env_value(value)}\n" for key, value in values.items())
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
        os.chmod(path, 0o600)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def main() -> int:
    if ENV_PATH.exists() and not _yes_no(".env уже существует. Перезаписать его?"):
        print("Существующий .env не изменён.")
        return 0

    values = {
        "GGSEL_SELLER_ID": _integer("GGSel seller ID: ", positive=True),
        "GGSEL_API_KEY": _required("GGSel API key: ", secret=True),
        "TELEGRAM_BOT_TOKEN": _required("Telegram bot token: ", secret=True),
        "TELEGRAM_GROUP_ID": _integer("Telegram group ID: ", positive=False, nonzero=True),
        "TELEGRAM_ALLOWED_USER_IDS": _allowed_ids(),
    }
    proxy_url = _proxy_url()
    if proxy_url:
        values["TELEGRAM_PROXY_URL"] = proxy_url
    write_env_atomic(ENV_PATH, values)
    print(f"Настройка сохранена в {ENV_PATH} с правами 0600.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
