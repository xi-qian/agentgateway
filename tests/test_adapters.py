"""Tests for platform adapter interface, mock adapter, Telegram adapter, and server integration."""
import asyncio
import pytest
from gateway.adapters.base import PlatformAdapter, AdapterMessageEvent, resolve_group_id
from gateway.adapters.mock import MockAdapter


class TestAdapterMessageEvent:
    def test_create_event(self):
        event = AdapterMessageEvent(
            text="hello",
            group_id="tg:1",
            sender_id="123",
            sender_name="Alice",
            message_id="msg_1",
            platform="mock",
            media_urls=["http://example.com/img.jpg"],
            reply_to_message_id="msg_0",
        )
        assert event.text == "hello"
        assert event.group_id == "tg:1"
        assert event.sender_id == "123"
        assert len(event.media_urls) == 1

    def test_default_values(self):
        event = AdapterMessageEvent(text="hi", group_id="g1")
        assert event.sender_id == ""
        assert event.sender_name == ""
        assert event.media_urls == []
        assert event.media_types == []
        assert event.reply_to_message_id == ""
        assert event.reply_to_text == ""


class TestPlatformAdapterInterface:
    @pytest.mark.asyncio
    async def test_mock_adapter_lifecycle(self):
        adapter = MockAdapter()
        assert adapter.name == "mock"
        assert adapter.platform == "mock"

        started = await adapter.start()
        assert started is True
        assert adapter.is_running is True

        received = []
        adapter.on_message = lambda event: received.append(event)

        await adapter.send_test_message("tg:1", "hello", "123")
        await asyncio.sleep(0.05)
        assert len(received) == 1
        assert received[0].text == "hello"

        responses = []
        adapter.on_send = lambda msg: responses.append(msg)

        await adapter.send_response("tg:1", "response text")
        await asyncio.sleep(0.05)
        assert len(responses) == 1
        assert responses[0] == {"group_id": "tg:1", "text": "response text"}

        await adapter.stop()
        assert adapter.is_running is False

    @pytest.mark.asyncio
    async def test_dispatch_without_handler(self):
        adapter = MockAdapter()
        await adapter.start()
        # Should not raise even with no handler
        await adapter.send_test_message("g1", "test")
        await adapter.stop()

    @pytest.mark.asyncio
    async def test_send_edit_and_typing_noops(self):
        """Base class send_edit and send_typing are noops."""
        adapter = MockAdapter()
        await adapter.start()
        await adapter.send_edit("g1", "edited text")
        await adapter.send_typing("g1")
        await adapter.stop()

    @pytest.mark.asyncio
    async def test_send_test_message_with_media(self):
        adapter = MockAdapter()
        await adapter.start()
        received = []
        adapter.on_message = lambda event: received.append(event)

        await adapter.send_test_message(
            "g1", "check this", media_urls=["http://img.png"],
            reply_to_message_id="msg_prev",
        )
        await asyncio.sleep(0.05)
        assert len(received) == 1
        assert received[0].media_urls == ["http://img.png"]
        assert received[0].reply_to_message_id == "msg_prev"
        await adapter.stop()


class TestGroupIDResolution:
    def test_resolve_telegram(self):
        group_id = resolve_group_id("telegram", {"chat_id": "-1001234567890"})
        assert group_id == "telegram:group:-1001234567890"

    def test_resolve_telegram_private(self):
        group_id = resolve_group_id("telegram", {"chat_id": "987654321"})
        assert group_id == "telegram:group:987654321"

    def test_resolve_discord(self):
        group_id = resolve_group_id("discord", {"channel_id": "1234567890"})
        assert group_id == "discord:channel:1234567890"

    def test_resolve_slack(self):
        group_id = resolve_group_id("slack", {"channel_id": "C01ABCDE"})
        assert group_id == "slack:channel:C01ABCDE"

    def test_resolve_unknown_with_id(self):
        group_id = resolve_group_id("unknown", {"id": "123"})
        assert group_id == "unknown:123"

    def test_resolve_unknown_with_chat_id(self):
        group_id = resolve_group_id("unknown", {"chat_id": "456"})
        assert group_id == "unknown:456"


class TestTelegramAdapter:
    def test_telegram_adapter_properties(self):
        from gateway.adapters.telegram import TelegramAdapter
        adapter = TelegramAdapter()
        assert adapter.name == "telegram"
        assert adapter.platform == "telegram"

    def test_telegram_adapter_start_without_token(self):
        from gateway.adapters.telegram import TelegramAdapter
        adapter = TelegramAdapter({"token": ""})
        # Sync check -- start() returns coroutine, need to await
        assert adapter._token == ""

    @pytest.mark.asyncio
    async def test_telegram_adapter_start_fails_without_token(self):
        from gateway.adapters.telegram import TelegramAdapter
        adapter = TelegramAdapter({"token": ""})
        started = await adapter.start()
        assert started is False
        assert adapter.is_running is False

    def test_check_requirements(self):
        from gateway.adapters.telegram import TelegramAdapter, HAS_TELEGRAM
        assert TelegramAdapter.check_requirements() == HAS_TELEGRAM


class TestAdapterServerIntegration:
    """Test that the Gateway loads and uses adapters."""

    def test_load_adapters_from_config(self):
        from gateway.server import load_adapters_from_config
        from gateway.adapters.mock import MockAdapter

        config = {"adapters": {"mock": {"enabled": True}}}
        adapters = load_adapters_from_config(config)
        assert len(adapters) == 1
        assert isinstance(adapters[0], MockAdapter)

    def test_disabled_adapter_not_loaded(self):
        from gateway.server import load_adapters_from_config
        config = {"adapters": {"mock": {"enabled": False}}}
        adapters = load_adapters_from_config(config)
        assert len(adapters) == 0

    def test_empty_config_returns_empty_list(self):
        from gateway.server import load_adapters_from_config
        adapters = load_adapters_from_config({})
        assert adapters == []

    def test_unknown_adapter_skipped(self):
        from gateway.server import load_adapters_from_config
        config = {"adapters": {"nonexistent": {"enabled": True}}}
        adapters = load_adapters_from_config(config)
        assert adapters == []
