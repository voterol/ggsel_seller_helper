import os
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch

from config import Config
import setup


class ProxyConfigTests(unittest.TestCase):
    def test_old_environment_without_proxy_remains_supported(self):
        env = {
            "GGSEL_SELLER_ID": "1",
            "GGSEL_API_KEY": "key",
            "TELEGRAM_BOT_TOKEN": "123:token",
            "TELEGRAM_GROUP_ID": "-1001",
            "TELEGRAM_ALLOWED_USER_IDS": "42",
        }
        with patch.dict(os.environ, env, clear=True):
            self.assertIsNone(Config.from_env().telegram_proxy_url)

    def test_supported_proxy_urls_are_accepted(self):
        for value in ("http://proxy.local:8080", "socks5://u:p@127.0.0.1:1080"):
            config = Config(1, "key", "token", -1001, telegram_proxy_url=value)
            config.validate()

    def test_invalid_proxy_error_does_not_disclose_credentials(self):
        secret = "do-not-disclose"
        with self.assertRaises(ValueError) as raised:
            Config._validate_telegram_proxy_url(f"ftp://user:{secret}@host:21")
        self.assertNotIn(secret, str(raised.exception))


class SetupFileTests(unittest.TestCase):
    def test_atomic_writer_quotes_values_and_sets_private_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            setup.write_env_atomic(path, {"TOKEN": 'a#b$c"d'})

            self.assertEqual(path.read_text(), 'TOKEN="a#b\\$c\\"d"\n')
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_proxy_builder_percent_encodes_credentials(self):
        answers = iter(["yes", "SOCKS", "proxy.local", "1080", "yes", "user@name"])
        with patch("builtins.input", side_effect=lambda _prompt: next(answers)), patch(
            "setup.getpass", return_value="p:a/ss"
        ):
            value = setup._proxy_url()
        self.assertEqual(value, "socks5://user%40name:p%3Aa%2Fss@proxy.local:1080")


if __name__ == "__main__":
    unittest.main()
