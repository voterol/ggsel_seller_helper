import json
import logging
import os
from pathlib import Path

TRANSLATIONS_DIR = Path(__file__).resolve().parent / "translations"

# Display names for the language picker; unknown codes fall back to the code.
LANGUAGE_NAMES = {
    "ru": "🇷🇺 Русский",
    "en": "🇬🇧 English",
    "uk": "🇺🇦 Українська",
    "es": "🇪🇸 Español",
    "de": "🇩🇪 Deutsch",
}

class LocaleManager:
    def __init__(self, file_path="bot_lang.json", translations_dir=None):
        self.file_path = file_path
        self.translations_dir = Path(translations_dir) if translations_dir is not None else TRANSLATIONS_DIR
        self.lang = self.load_lang()
        self.translations = self._load_translations()

    def _load_translations(self):
        translations = {}
        try:
            paths = sorted(self.translations_dir.glob("*.json"))
        except OSError:
            return translations
        for path in paths:
            try:
                # utf-8-sig прозрачно принимает файлы как с BOM (частый случай
                # на Windows), так и без него.
                with open(path, "r", encoding="utf-8-sig") as stream:
                    data = json.load(stream)
                if isinstance(data, dict):
                    translations[path.stem] = data
            except (OSError, ValueError):
                logging.warning(f"Не удалось загрузить файл переводов: {path}")
        return translations

    def load_lang(self):
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, 'r') as f:
                    return json.load(f).get("lang", "ru")
            except: pass
        return "ru"

    def save_lang(self):
        with open(self.file_path, 'w') as f:
            json.dump({"lang": self.lang}, f)

    def available_languages(self):
        return sorted(self.translations)

    def set_lang(self, code):
        if code not in self.translations:
            return False
        self.lang = code
        self.save_lang()
        return True

    def get(self, key):
        table = self.translations.get(self.lang)
        if table is None or key not in table:
            table = self.translations.get("ru", {})
        return table.get(key, key)

locales = LocaleManager()
_ = locales.get
