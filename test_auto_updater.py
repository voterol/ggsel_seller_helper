import asyncio
import hashlib
import importlib
import tempfile
import unittest
import zipfile
import os
import stat
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, patch

import auto_updater


class AutoUpdaterTests(unittest.TestCase):
    @staticmethod
    def _import_main_without_optional_runtime_dependencies():
        fake_service_module = ModuleType("bot_service")
        fake_service_module.BotService = object
        with patch.dict(sys.modules, {"bot_service": fake_service_module}):
            sys.modules.pop("main", None)
            return importlib.import_module("main")

    def test_disabled_by_default(self):
        result = asyncio.run(auto_updater.check_and_update())
        self.assertEqual((False, "Automatic updates are disabled"), result)

    def test_enabled_update_requires_version_and_digest(self):
        result = asyncio.run(auto_updater.check_and_update(True))
        self.assertFalse(result[0])
        self.assertIn("Unsafe update configuration", result[1])

    def test_current_pinned_version_does_not_download(self):
        with patch.object(auto_updater, "get_current_version", return_value="1.2.3"), patch.object(
            auto_updater, "download_and_extract_update", new_callable=AsyncMock
        ) as download:
            result = asyncio.run(auto_updater.check_and_update(True, "1.2.3", "a" * 64))
        self.assertFalse(result[0])
        download.assert_not_awaited()

    def test_periodic_check_cannot_download_extract_or_swap(self):
        with patch.object(auto_updater, "get_current_version", return_value="1.0.0"), patch.object(
            auto_updater, "get_remote_version", new=AsyncMock(return_value="1.2.3")
        ) as remote, patch.object(
            auto_updater, "download_and_extract_update", new_callable=AsyncMock
        ) as download, patch.object(auto_updater, "_install_staged") as install:
            message = asyncio.run(
                auto_updater.check_update_available(True, "1.2.3", "a" * 64)
            )
        self.assertIn("verified at next startup", message)
        remote.assert_awaited_once_with("1.2.3")
        download.assert_not_awaited()
        install.assert_not_called()

    def test_periodic_loop_is_wired_only_to_check_only_function(self):
        main = self._import_main_without_optional_runtime_dependencies()

        async def exercise_loop():
            with patch.object(main.asyncio, "sleep", new=AsyncMock(side_effect=[None, asyncio.CancelledError])), patch.object(
                main, "check_update_available", new=AsyncMock(return_value="notification")
            ) as check_only, patch.object(
                main, "check_and_update", new_callable=AsyncMock
            ) as installer:
                with self.assertRaises(asyncio.CancelledError):
                    await main.update_checker(True, "1.2.3", "a" * 64)
                check_only.assert_awaited_once_with(True, "1.2.3", "a" * 64)
                installer.assert_not_awaited()

        asyncio.run(exercise_loop())

    def test_successful_startup_install_exits_before_service_or_database_construction(self):
        main = self._import_main_without_optional_runtime_dependencies()

        config = SimpleNamespace(
            auto_update=True,
            database_path="state/bot.db",
            ggsel_api_key="key",
            telegram_bot_token="token",
            telegram_group_id=-1,
            validate=lambda: None,
        )
        with patch.object(main.Config, "from_env", return_value=config), patch.dict(
            os.environ,
            {"AUTO_UPDATE": "true", "UPDATE_VERSION": "1.2.3", "UPDATE_SHA256": "a" * 64},
            clear=False,
        ), patch.object(
            main, "check_and_update", new=AsyncMock(return_value=(True, "updated"))
        ) as installer, patch.object(main, "BotService") as service, patch.object(
            main, "setup_logging"
        ) as logging_setup:
            with self.assertRaises(SystemExit):
                asyncio.run(main.main())
        installer.assert_awaited_once_with(True, "1.2.3", "a" * 64, "state/bot.db")
        service.assert_not_called()
        logging_setup.assert_not_called()

    def test_safe_extract_rejects_traversal(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_path = Path(temp) / "bad.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("../outside", "bad")
            with zipfile.ZipFile(archive_path) as archive:
                with self.assertRaises(ValueError):
                    auto_updater._safe_extract(archive, Path(temp) / "extract")

    def test_safe_extract_regular_file(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_path = Path(temp) / "good.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr("release/main.py", "pass\n")
            destination = Path(temp) / "extract"
            destination.mkdir()
            with zipfile.ZipFile(archive_path) as archive:
                auto_updater._safe_extract(archive, destination)
            self.assertEqual("pass\n", (destination / "release/main.py").read_text())

    def test_safe_extract_rejects_symbolic_link(self):
        with tempfile.TemporaryDirectory() as temp:
            archive_path = Path(temp) / "link.zip"
            link = zipfile.ZipInfo("release/link")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr(link, "../../outside")
            with zipfile.ZipFile(archive_path) as archive:
                with self.assertRaises(ValueError):
                    auto_updater._safe_extract(archive, Path(temp) / "extract")

    def test_parameter_validation_normalizes_digest(self):
        version, digest = auto_updater._validate_update_parameters("1.2.3", "A" * 64)
        self.assertEqual("1.2.3", version)
        self.assertEqual("a" * 64, digest)

    def test_archive_url_is_pinned_to_tag(self):
        url = auto_updater._archive_url("1.2.3")
        self.assertEqual(
            "https://github.com/voterol/ggsel_seller_helper/archive/refs/tags/1.2.3.zip",
            url,
        )
        self.assertNotIn("heads/main", url)

    def test_staged_install_preserves_runtime_data_and_keeps_backup(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed = root / "app"
            source = root / "source"
            work = root / "work"
            installed.mkdir()
            source.mkdir()
            work.mkdir()
            (installed / "main.py").write_text("old")
            (installed / ".env").write_text("SECRET=value")
            (source / "main.py").write_text("new")
            with patch.object(auto_updater, "BOT_DIR", installed):
                auto_updater._install_staged(source, work)
            self.assertEqual("new", (installed / "main.py").read_text())
            self.assertEqual("SECRET=value", (installed / ".env").read_text())
            self.assertEqual("old", (root / "app.update-backup/main.py").read_text())

    def test_staged_install_preserves_explicit_runtime_state_allowlist(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            (source / "main.py").write_text("new")
            expected = {
                ".env": "environment",
                "bot_lang.json": "language",
                "orders.json": "orders",
                "autoresponder_config.json": "autoresponder-config",
                "autoresponder.json": "autoresponder",
                "topics.json": "topics",
                "processed_reviews.json": "reviews",
                "processed_purchases.json": "purchases",
                "processed_messages.json": "messages",
                "pending_topics.json": "pending",
                "ggsel_bot.log": "existing-log",
            }
            for name, contents in expected.items():
                (installed / name).write_text(contents)
                (source / name).write_text("release-must-not-replace-state")
            for directory in ("venv", ".venv"):
                (installed / directory).mkdir()
                (installed / directory / "marker").write_text(directory)
            with patch.object(auto_updater, "BOT_DIR", installed):
                auto_updater._install_staged(source, work)
            for name, contents in expected.items():
                self.assertEqual(contents, (installed / name).read_text(), name)
            for directory in ("venv", ".venv"):
                self.assertEqual(directory, (installed / directory / "marker").read_text())

    def test_staged_install_preserves_in_tree_venv(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            (installed / "venv/bin").mkdir(parents=True)
            (installed / "venv/bin/python").write_text("venv-placeholder")
            (source / "main.py").write_text("new")
            with patch.object(auto_updater, "BOT_DIR", installed):
                auto_updater._install_staged(source, work)
            self.assertEqual("venv-placeholder", (installed / "venv/bin/python").read_text())

    def test_staged_install_preserves_nested_relative_database_and_sidecar(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            (installed / "data/sqlite").mkdir(parents=True)
            (installed / "data/sqlite/bot.db").write_text("database")
            (installed / "data/sqlite/bot.db-wal").write_text("wal")
            (installed / "data/sqlite/bot.db-shm").write_text("shm")
            (installed / "data/sqlite/bot.db-journal").write_text("journal")
            (source / "main.py").write_text("new")
            with patch.object(auto_updater, "BOT_DIR", installed), patch.dict(
                os.environ, {"DATABASE_PATH": "data/sqlite/bot.db"}
            ):
                auto_updater._install_staged(source, work)
            self.assertEqual("database", (installed / "data/sqlite/bot.db").read_text())
            self.assertEqual("wal", (installed / "data/sqlite/bot.db-wal").read_text())
            self.assertEqual("shm", (installed / "data/sqlite/bot.db-shm").read_text())
            self.assertEqual("journal", (installed / "data/sqlite/bot.db-journal").read_text())

    def test_staged_install_leaves_absolute_external_database_untouched(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            database = root / "state/nested/bot.db"
            database.parent.mkdir(parents=True)
            database.write_text("database")
            (source / "main.py").write_text("new")
            with patch.object(auto_updater, "BOT_DIR", installed), patch.dict(
                os.environ, {"DATABASE_PATH": str(database)}
            ):
                auto_updater._install_staged(source, work)
            self.assertEqual("database", database.read_text())

    def test_database_path_in_updater_backup_is_rejected_before_swap(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            (installed / "main.py").write_text("old")
            (source / "main.py").write_text("new")
            unsafe = root / "app.update-backup/data/bot.db"
            with patch.object(auto_updater, "BOT_DIR", installed):
                with self.assertRaises(ValueError):
                    auto_updater._install_staged(source, work, str(unsafe))
            self.assertEqual("old", (installed / "main.py").read_text())

    def test_relative_database_path_cannot_escape_install(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed, source, work = root / "app", root / "source", root / "work"
            installed.mkdir(); source.mkdir(); work.mkdir()
            (installed / "main.py").write_text("old")
            (source / "main.py").write_text("new")
            with patch.object(auto_updater, "BOT_DIR", installed):
                with self.assertRaises(ValueError):
                    auto_updater._install_staged(source, work, "../state/bot.db")
            self.assertEqual("old", (installed / "main.py").read_text())

    def test_failed_atomic_swap_rolls_back_installed_directory(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            installed = root / "app"
            source = root / "source"
            work = root / "work"
            installed.mkdir()
            source.mkdir()
            work.mkdir()
            (installed / "main.py").write_text("old")
            (source / "main.py").write_text("new")
            real_replace = os.replace
            calls = 0

            def fail_stage_swap(src, dst):
                nonlocal calls
                calls += 1
                if calls == 2:
                    raise OSError("simulated swap failure")
                return real_replace(src, dst)

            with patch.object(auto_updater, "BOT_DIR", installed), patch.object(
                auto_updater.os, "replace", side_effect=fail_stage_swap
            ):
                with self.assertRaises(OSError):
                    auto_updater._install_staged(source, work)
            self.assertEqual("old", (installed / "main.py").read_text())


if __name__ == "__main__":
    unittest.main()
