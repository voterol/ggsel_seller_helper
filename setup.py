"""Interactive, dependency-free initial .env setup."""

from getpass import getpass
import os
from pathlib import Path
import tempfile
from typing import Dict, Optional
from urllib.parse import quote

from config import Config
from locales import locales, _, LANGUAGE_NAMES


ENV_PATH = Path(__file__).resolve().with_name(".env")


def choose_language() -> Optional[str]:
    """Ask for the wizard language; the choice persists for the bot UI too."""
    codes = locales.available_languages()
    print("🌐 Language / Язык / Мова / Idioma / Sprache:")
    for number, code in enumerate(codes, start=1):
        print(f"{number}) {LANGUAGE_NAMES.get(code, code)}")
    while True:
        try:
            number = int(input().strip())
        except ValueError:
            number = 0
        if 1 <= number <= len(codes):
            code = codes[number - 1]
            locales.lang = code
            locales.save_lang()
            return code
        print(_("setup_choose_lang_invalid"))


def _required(prompt: str, *, secret: bool = False) -> str:
    reader = getpass if secret else input
    while True:
        value = reader(prompt).strip()
        if value and "\n" not in value and "\r" not in value:
            return value
        print(_("setup_value_required"))


def _integer(prompt: str, *, positive: bool, nonzero: bool = False) -> str:
    while True:
        value = _required(prompt)
        try:
            number = int(value)
        except ValueError:
            print(_("setup_enter_integer"))
            continue
        if (positive and number <= 0) or (nonzero and number == 0):
            print(_("setup_invalid_number"))
            continue
        return str(number)


def _yes_no(prompt: str, *, default: bool = False) -> bool:
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        answer = input(prompt + suffix).strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes", "д", "да", "s", "sí", "j", "ja", "т", "так"}:
            return True
        if answer in {"n", "no", "н", "нет", "nein", "ні"}:
            return False
        print(_("setup_yes_no_hint"))


def _allowed_ids() -> str:
    while True:
        value = _required(_("setup_allowed_ids"))
        try:
            ids = Config._allowed_user_ids(value)
        except ValueError as exc:
            print(str(exc))
            continue
        if ids:
            return ",".join(str(item) for item in sorted(ids))


def _proxy_url() -> Optional[str]:
    if not _yes_no(_("setup_proxy_ask")):
        return None
    while True:
        kind = input(_("setup_proxy_type")).strip().lower()
        if kind in {"http", "socks", "socks5"}:
            scheme = "http" if kind == "http" else "socks5"
            break
        print(_("setup_proxy_type_invalid"))
    while True:
        host = _required(_("setup_proxy_host"))
        if not any(char in host for char in "/@?#"):
            break
        print(_("setup_proxy_host_invalid"))
    while True:
        port = _integer(_("setup_proxy_port"), positive=True)
        if int(port) <= 65535:
            break
        print(_("setup_proxy_port_range"))

    auth = ""
    if _yes_no(_("setup_proxy_auth")):
        username = _required(_("setup_proxy_user"))
        while True:
            password = getpass(_("setup_proxy_pass"))
            if "\n" not in password and "\r" not in password:
                break
            print(_("setup_proxy_pass_invalid"))
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


def main(lang: Optional[str] = None) -> int:
    if lang is None:
        lang = choose_language()
    else:
        locales.lang = lang
        locales.save_lang()
    print(_("setup_missing_notice"))

    if ENV_PATH.exists() and not _yes_no(_("setup_env_exists")):
        print(_("setup_env_kept"))
        return 0

    values = {
        "GGSEL_SELLER_ID": _integer(_("setup_seller_id"), positive=True),
        "GGSEL_API_KEY": _required(_("setup_api_key"), secret=True),
        "TELEGRAM_BOT_TOKEN": _required(_("setup_token"), secret=True),
        "TELEGRAM_GROUP_ID": _integer(_("setup_group_id"), positive=False, nonzero=True),
        "TELEGRAM_ALLOWED_USER_IDS": _allowed_ids(),
    }
    proxy_url = _proxy_url()
    if proxy_url:
        values["TELEGRAM_PROXY_URL"] = proxy_url
    write_env_atomic(ENV_PATH, values)
    print(_("setup_saved").replace("{path}", str(ENV_PATH)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
