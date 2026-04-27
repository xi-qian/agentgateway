"""Tests for platform adapter interface, mock adapter, Telegram adapter, and server integration."""
import asyncio
import json
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

    def test_resolve_feishu(self):
        group_id = resolve_group_id("feishu", {"chat_id": "oc_abc123"})
        assert group_id == "feishu:chat:oc_abc123"

    def test_resolve_dingtalk(self):
        group_id = resolve_group_id("dingtalk", {"conversation_id": "cid123"})
        assert group_id == "dingtalk:conversation:cid123"

    def test_resolve_wecom(self):
        group_id = resolve_group_id("wecom", {"chat_id": "userId123"})
        assert group_id == "wecom:chat:userId123"

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


class TestDingTalkAdapter:
    def test_dingtalk_adapter_properties(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter()
        assert adapter.name == "dingtalk"
        assert adapter.platform == "dingtalk"

    def test_dingtalk_adapter_config(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter({
            "client_id": "test_id",
            "client_secret": "test_secret",
        })
        assert adapter._client_id == "test_id"
        assert adapter._client_secret == "test_secret"

    @pytest.mark.asyncio
    async def test_dingtalk_start_fails_without_deps(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter({
            "client_id": "test_id",
            "client_secret": "test_secret",
        })
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        assert isinstance(DingTalkAdapter.check_requirements(), bool)


class TestWeComAdapter:
    def test_wecom_adapter_properties(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter()
        assert adapter.name == "wecom"
        assert adapter.platform == "wecom"

    def test_wecom_adapter_config(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter({
            "bot_id": "test_bot",
            "secret": "test_secret",
        })
        assert adapter._bot_id == "test_bot"
        assert adapter._secret == "test_secret"

    @pytest.mark.asyncio
    async def test_wecom_start_fails_without_creds(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter({"bot_id": "", "secret": ""})
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.wecom import WeComAdapter
        assert isinstance(WeComAdapter.check_requirements(), bool)


class TestFeishuAdapter:
    def test_feishu_adapter_properties(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter()
        assert adapter.name == "feishu"
        assert adapter.platform == "feishu"

    def test_feishu_adapter_config(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter({
            "app_id": "cli_test",
            "app_secret": "secret_test",
        })
        assert adapter._app_id == "cli_test"
        assert adapter._app_secret == "secret_test"

    @pytest.mark.asyncio
    async def test_feishu_start_fails_without_deps(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter({
            "app_id": "cli_test",
            "app_secret": "secret_test",
        })
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.feishu import FeishuAdapter
        assert isinstance(FeishuAdapter.check_requirements(), bool)

    def test_feishu_post_parsing(self):
        from gateway.adapters.feishu import parse_feishu_post_content
        raw = json.dumps({
            "zh_cn": {
                "title": "Hello",
                "content": [
                    [{"tag": "text", "text": "world"}]
                ]
            }
        })
        result = parse_feishu_post_content(raw)
        assert "Hello" in result.text_content
        assert "world" in result.text_content

    def test_feishu_message_normalization_text(self):
        from gateway.adapters.feishu import normalize_feishu_message
        msg = normalize_feishu_message(message_type="text", raw_content='{"text": "hello"}')
        assert msg.text_content == "hello"
        assert msg.raw_type == "text"


class TestAdapterRegistryIntegration:
    """Test that new adapters are registered and loadable."""

    def test_registry_contains_china_adapters(self):
        from gateway.server import ADAPTER_REGISTRY
        assert "feishu" in ADAPTER_REGISTRY
        assert "dingtalk" in ADAPTER_REGISTRY
        assert "wecom" in ADAPTER_REGISTRY

    def test_load_feishu_adapter(self):
        from gateway.server import load_adapters_from_config
        config = {"adapters": {"feishu": {"enabled": True, "app_id": "x", "app_secret": "y"}}}
        adapters = load_adapters_from_config(config)
        assert len(adapters) == 1
        assert adapters[0].name == "feishu"

    def test_load_dingtalk_adapter(self):
        from gateway.server import load_adapters_from_config
        config = {"adapters": {"dingtalk": {"enabled": True, "client_id": "x", "client_secret": "y"}}}
        adapters = load_adapters_from_config(config)
        assert len(adapters) == 1
        assert adapters[0].name == "dingtalk"

    def test_load_wecom_adapter(self):
        from gateway.server import load_adapters_from_config
        config = {"adapters": {"wecom": {"enabled": True, "bot_id": "x", "secret": "y"}}}
        adapters = load_adapters_from_config(config)
        assert len(adapters) == 1
        assert adapters[0].name == "wecom"
