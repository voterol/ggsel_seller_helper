import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from bot_service import BotService
from config import Config
from database import Database


def make_config(database_path):
    return Config(
        ggsel_seller_id=1,
        ggsel_api_key="key",
        telegram_bot_token="123:token",
        telegram_group_id=-1001,
        telegram_allowed_user_ids=frozenset({42}),
        database_path=database_path,
    )


class SyncControlTests(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.addCleanup(self.loop.close)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = str(Path(self.temp.name) / "state.db")

    def make_service(self):
        with patch("bot_service.GGSelAPI", return_value=Mock()), patch(
            "bot_service.TelegramBot", return_value=Mock()
        ):
            return BotService(make_config(self.path))

    def test_sync_defaults_enabled_and_stop_state_survives_restart(self):
        service = self.make_service()
        self.assertTrue(service.sync_enabled)

        self.assertIn("stopped", self.loop.run_until_complete(service.pause_sync()))
        self.assertFalse(service.sync_enabled)
        self.assertEqual(Database(self.path).get_setting("ggsel_sync_enabled"), "false")

        restarted = self.make_service()
        self.assertFalse(restarted.sync_enabled)
        self.assertIn("already stopped", self.loop.run_until_complete(restarted.pause_sync()))

    def test_installation_time_is_created_once_and_survives_restart(self):
        service = self.make_service()
        installed_at = Database(self.path).get_setting("bot_installed_at")

        restarted = self.make_service()

        self.assertIsNotNone(installed_at)
        self.assertEqual(Database(self.path).get_setting("bot_installed_at"), installed_at)
        self.assertEqual(restarted.installed_at, service.installed_at)

    def test_start_is_idempotent_and_persisted(self):
        service = self.make_service()
        self.loop.run_until_complete(service.pause_sync())

        with patch.object(service, "_background_boot_sequence", new=AsyncMock()):
            self.assertIn("started", self.loop.run_until_complete(service.start_sync()))
            self.assertIn("already running", self.loop.run_until_complete(service.start_sync()))

        self.assertTrue(service.sync_enabled)
        self.assertEqual(Database(self.path).get_setting("ggsel_sync_enabled"), "true")

    def test_no_message_mode_is_persisted_and_start_restores_messages(self):
        service = self.make_service()

        result = self.loop.run_until_complete(service.start_sync_without_messages())

        self.assertIn("disabled", result)
        self.assertTrue(service.sync_enabled)
        self.assertFalse(service.automatic_customer_messages_enabled)
        restarted = self.make_service()
        self.assertTrue(restarted.sync_enabled)
        self.assertFalse(restarted.automatic_customer_messages_enabled)

        with patch.object(restarted, "_background_boot_sequence", new=AsyncMock()):
            self.assertIn("started", self.loop.run_until_complete(restarted.start_sync()))

        normal_restart = self.make_service()
        self.assertTrue(normal_restart.sync_enabled)
        self.assertTrue(normal_restart.automatic_customer_messages_enabled)

    def test_no_message_mode_suppresses_automatic_but_allows_manual_messages(self):
        service = self.make_service()
        service.ggsel_api.send_message.return_value = True
        service.ggsel_api.last_failure = None
        self.loop.run_until_complete(service.start_sync_without_messages())

        automatic = self.loop.run_until_complete(
            service._send_customer_message(123, "automatic", automatic=True)
        )
        manual = self.loop.run_until_complete(
            service._send_customer_message(123, "manual")
        )

        self.assertEqual(automatic, (False, "suppressed"))
        self.assertEqual(manual, (True, None))
        service.ggsel_api.send_message.assert_called_once_with(123, "manual")

    def test_no_message_transition_waits_for_admitted_customer_write(self):
        service = self.make_service()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def scenario():
            async def admitted_write():
                async with service._customer_write_lock:
                    entered.set()
                    await release.wait()

            write_task = asyncio.create_task(admitted_write())
            await entered.wait()
            transition_task = asyncio.create_task(service.start_sync_without_messages())
            await asyncio.sleep(0)
            self.assertFalse(transition_task.done())
            release.set()
            await write_task
            self.assertIn("disabled", await transition_task)

        self.loop.run_until_complete(scenario())

    def test_stopped_mode_blocks_customer_writes(self):
        service = self.make_service()
        self.loop.run_until_complete(service.pause_sync())

        result = self.loop.run_until_complete(service._send_customer_message(123, "hello"))

        self.assertEqual(result, (False, None))
        service.ggsel_api.send_message.assert_not_called()

    def test_stop_waits_for_admitted_customer_write(self):
        service = self.make_service()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def scenario():
            async def admitted_write():
                async with service._customer_write_lock:
                    entered.set()
                    await release.wait()

            write_task = asyncio.create_task(admitted_write())
            await entered.wait()
            stop_task = asyncio.create_task(service.pause_sync())
            await asyncio.sleep(0)
            self.assertFalse(stop_task.done())
            release.set()
            await write_task
            self.assertIn("stopped", await stop_task)

        self.loop.run_until_complete(scenario())


if __name__ == "__main__":
    unittest.main()
