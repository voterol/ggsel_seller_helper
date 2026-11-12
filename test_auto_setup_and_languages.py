import asyncio
import json
import os
import string
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, patch

import main
from config import Config
from locales import LocaleManager
from telegram_bot import TelegramBot

TRANSLATIONS_DIR = Path(__file__).resolve().parent / "translations"


def load_table(code):
    with open(TRANSLATIONS_DIR / f"{code}.json", encoding="utf-8") as stream:
        return json.load(stream)


def placeholders(value):
    return {name for _, name, _, _ in string.Formatter().parse(value) if name is not None}


class TranslationFileTests(unittest.TestCase):
    def test_every_language_has_exactly_the_russian_key_set(self):
        files = sorted(TRANSLATIONS_DIR.glob("*.json"))
        self.assertTrue(files, "translations/ directory is empty or missing")
        reference = load_table("ru")
        for path in files:
            with self.subTest(file=path.name):
                table = load_table(path.stem)
                missing = sorted(set(reference) - set(table))
                extra = sorted(set(table) - set(reference))
                self.assertEqual(
                    (missing, extra),
                    ([], []),
                    f"{path.name}: missing keys {missing}, extra keys {extra}",
                )

    def test_picker_keys_are_present_in_every_language(self):
        for path in sorted(TRANSLATIONS_DIR.glob("*.json")):
            with self.subTest(file=path.name):
                table = load_table(path.stem)
                for key in ("btn_lang", "lang_menu_title", "menu_title", "btn_back"):
                    self.assertIn(key, table)
                self.assertTrue(table["btn_lang"].strip())
                self.assertTrue(table["lang_menu_title"].startswith("🌐"))

    def test_placeholders_are_identical_across_languages(self):
        reference = load_table("ru")
        files = sorted(TRANSLATIONS_DIR.glob("*.json"))
        for key in sorted(reference):
            expected = placeholders(reference[key])
            if not expected:
                continue
            for path in files:
                with self.subTest(file=path.name, key=key):
                    self.assertEqual(placeholders(load_table(path.stem)[key]), expected)

    def test_html_and_markdown_markup_survives_translation(self):
        for path in sorted(TRANSLATIONS_DIR.glob("*.json")):
            table = load_table(path.stem)
            with self.subTest(file=path.name):
                self.assertIn("<b>", table["noti_new_purchase"])
                self.assertIn("**", table["balance_body"])
                self.assertIn("`{total} {curr}`", table["balance_body"])


class LocaleManagerTests(unittest.TestCase):
    def test_default_manager_loads_all_bundled_languages(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = LocaleManager(file_path=str(Path(directory) / "bot_lang.json"))
            bundled = sorted(path.stem for path in TRANSLATIONS_DIR.glob("*.json"))
            # Фиксируем только обязательные языки; полный список берём из
            # каталога, чтобы добавление нового перевода не ломало тест.
            self.assertTrue({"ru", "en"}.issubset(set(bundled)))
            self.assertEqual(manager.available_languages(), bundled)

    def test_set_lang_roundtrip_persists_and_translates(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "bot_lang.json"
            manager = LocaleManager(file_path=str(state), translations_dir=TRANSLATIONS_DIR)
            self.assertEqual(manager.lang, "ru")

            self.assertTrue(manager.set_lang("es"))
            self.assertEqual(manager.lang, "es")
            self.assertEqual(json.loads(state.read_text(encoding="utf-8")), {"lang": "es"})
            self.assertEqual(manager.get("menu_title"), load_table("es")["menu_title"])

            self.assertFalse(manager.set_lang("bad"))
            self.assertEqual(manager.lang, "es")
            self.assertEqual(manager.get("menu_title"), load_table("es")["menu_title"])

            reloaded = LocaleManager(file_path=str(state), translations_dir=TRANSLATIONS_DIR)
            self.assertEqual(reloaded.lang, "es")

    def test_unknown_stored_language_falls_back_to_russian_then_key(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "bot_lang.json"
            state.write_text(json.dumps({"lang": "fr"}), encoding="utf-8")
            manager = LocaleManager(file_path=str(state), translations_dir=TRANSLATIONS_DIR)
            self.assertEqual(manager.lang, "fr")
            self.assertEqual(manager.get("menu_title"), load_table("ru")["menu_title"])
            self.assertEqual(manager.get("no_such_key"), "no_such_key")

    def test_corrupt_lang_file_defaults_to_russian(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "bot_lang.json"
            state.write_text("{not json", encoding="utf-8")
            manager = LocaleManager(file_path=str(state), translations_dir=TRANSLATIONS_DIR)
            self.assertEqual(manager.lang, "ru")

    def test_missing_translations_dir_degrades_to_raw_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = LocaleManager(
                file_path=str(Path(directory) / "bot_lang.json"),
                translations_dir=Path(directory) / "does_not_exist",
            )
            self.assertEqual(manager.available_languages(), [])
            self.assertEqual(manager.get("menu_title"), "menu_title")
            self.assertEqual(manager.get("btn_lang"), "btn_lang")


class EnsureConfigTests(unittest.TestCase):
    ENV_LINES = (
        "GGSEL_SELLER_ID=1\n"
        "GGSEL_API_KEY=key\n"
        "TELEGRAM_BOT_TOKEN=123:token\n"
        "TELEGRAM_GROUP_ID=-1001\n"
        "TELEGRAM_ALLOWED_USER_IDS=42\n"
    )

    @staticmethod
    def _load_from(path):
        for line in Path(path).read_text(encoding="utf-8").splitlines():
            name, _, value = line.partition("=")
            os.environ[name] = value

    def test_wizard_success_returns_config(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"

            def fake_wizard():
                env_path.write_text(self.ENV_LINES, encoding="utf-8")
                return 0

            reload_calls = []

            def fake_reload(**kwargs):
                reload_calls.append(kwargs)
                self._load_from(env_path)

            with patch.dict(os.environ, {}, clear=True), \
                 patch.object(main.setup, "main", side_effect=fake_wizard) as wizard, \
                 patch.object(main.setup, "ENV_PATH", env_path), \
                 patch.object(main, "reload_env", side_effect=fake_reload), \
                 patch.object(main.sys.stdin, "isatty", return_value=True), \
                 patch.object(main.sys.stdout, "isatty", return_value=True):
                config = main.ensure_config()

            self.assertIsNotNone(config)
            self.assertEqual(config.ggsel_seller_id, 1)
            self.assertEqual(config.telegram_group_id, -1001)
            wizard.assert_called_once_with()
            # Свежий .env от мастера должен перекрывать унаследованные переменные.
            self.assertEqual(reload_calls, [{"override": True}])
            self.assertFalse((Path(__file__).resolve().parent / ".env").exists())

    def test_non_interactive_terminal_returns_none_without_wizard(self):
        with patch.dict(os.environ, {}, clear=True), \
             patch.object(main.sys.stdin, "isatty", return_value=False), \
             patch.object(main.sys.stdout, "isatty", return_value=True), \
             patch.object(main.setup, "main") as wizard:
            self.assertIsNone(main.ensure_config())
        wizard.assert_not_called()

    def test_wizard_declined_or_failed_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            with patch.dict(os.environ, {}, clear=True), \
                 patch.object(main.setup, "main", return_value=1), \
                 patch.object(main.setup, "ENV_PATH", env_path), \
                 patch.object(main.sys.stdin, "isatty", return_value=True), \
                 patch.object(main.sys.stdout, "isatty", return_value=True):
                self.assertIsNone(main.ensure_config())

    def test_incomplete_wizard_result_returns_none(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"

            def fake_wizard():
                env_path.write_text("GGSEL_SELLER_ID=1\n", encoding="utf-8")
                return 0

            with patch.dict(os.environ, {}, clear=True), \
                 patch.object(main.setup, "main", side_effect=fake_wizard), \
                 patch.object(main.setup, "ENV_PATH", env_path), \
                 patch.object(main, "reload_env", side_effect=lambda **kwargs: self._load_from(env_path)), \
                 patch.object(main.sys.stdin, "isatty", return_value=True), \
                 patch.object(main.sys.stdout, "isatty", return_value=True):
                self.assertIsNone(main.ensure_config())

    def test_valid_environment_skips_wizard_entirely(self):
        env = {
            "GGSEL_SELLER_ID": "1",
            "GGSEL_API_KEY": "key",
            "TELEGRAM_BOT_TOKEN": "123:token",
            "TELEGRAM_GROUP_ID": "-1001",
            "TELEGRAM_ALLOWED_USER_IDS": "42",
        }
        with patch.dict(os.environ, env, clear=True), \
             patch.object(main.setup, "main") as wizard:
            config = main.ensure_config()
        self.assertIsNotNone(config)
        wizard.assert_not_called()


class TelegramBotLanguageMenuTests(unittest.TestCase):
    @staticmethod
    def _make_bot():
        return TelegramBot(
            Config(
                ggsel_seller_id=1,
                ggsel_api_key="key",
                telegram_bot_token="123456:token",
                telegram_group_id=-1001,
                telegram_allowed_user_ids=frozenset({42}),
            )
        )

    @staticmethod
    def _run_callback(bot, data):
        update = SimpleNamespace(callback_query=SimpleNamespace(data=data, answer=AsyncMock()))
        with patch.object(TelegramBot, "_is_authorized", return_value=True):
            asyncio.run(bot._handle_callback(update, None))
        return update

    def test_language_menu_lists_every_language_and_marks_the_active_one(self):
        bot = self._make_bot()
        with tempfile.TemporaryDirectory() as directory:
            manager = LocaleManager(
                file_path=str(Path(directory) / "bot_lang.json"),
                translations_dir=TRANSLATIONS_DIR,
            )
            self.assertTrue(manager.set_lang("uk"))
            with patch("telegram_bot.locales", manager):
                rows = bot._language_menu_markup().inline_keyboard

        self.assertEqual(len(rows), len(manager.available_languages()) + 1)
        labels = [button.text for row in rows for button in row]
        self.assertEqual(labels.count("✅ 🇺🇦 Українська"), 1)
        self.assertEqual(labels.count("🇷🇺 Русский"), 1)
        self.assertEqual(labels.count("🇬🇧 English"), 1)
        self.assertEqual(labels.count("🇪🇸 Español"), 1)
        self.assertEqual(labels.count("🇩🇪 Deutsch"), 1)
        codes = {row[0].callback_data for row in rows[:-1]}
        self.assertEqual(codes, {f"lang_select_{code}" for code in manager.available_languages()})
        self.assertEqual(rows[-1][0].callback_data, "main_menu")

    def test_lang_toggle_opens_the_language_menu(self):
        bot = self._make_bot()
        bot.callback_handler = AsyncMock()
        with patch.object(TelegramBot, "_show_language_menu", new_callable=AsyncMock) as show:
            self._run_callback(bot, "lang_toggle")
        show.assert_awaited_once()
        bot.callback_handler.assert_not_called()

    def test_lang_select_switches_language_and_rerenders_the_menu(self):
        bot = self._make_bot()
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "bot_lang.json"
            manager = LocaleManager(file_path=str(state), translations_dir=TRANSLATIONS_DIR)
            with patch("telegram_bot.locales", manager), patch.object(
                TelegramBot, "_show_language_menu", new_callable=AsyncMock
            ) as show:
                self._run_callback(bot, "lang_select_es")
                self.assertEqual(manager.lang, "es")
                self.assertEqual(json.loads(state.read_text(encoding="utf-8")), {"lang": "es"})
                show.assert_awaited_once()

                show.reset_mock()
                self._run_callback(bot, "lang_select_bad")
                self.assertEqual(manager.lang, "es")
                show.assert_awaited_once()

    def test_unrelated_callbacks_still_reach_the_bot_service_handler(self):
        bot = self._make_bot()
        bot.callback_handler = AsyncMock()
        with patch.object(TelegramBot, "_show_language_menu", new_callable=AsyncMock) as show:
            update = self._run_callback(bot, "main_menu")
        bot.callback_handler.assert_awaited_once_with("main_menu", update, None)
        show.assert_not_called()


if __name__ == "__main__":
    unittest.main()
