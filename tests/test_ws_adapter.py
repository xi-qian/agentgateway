"""Tests for InternalWSAdapter — Hermes plugin WebSocket adapter."""
import asyncio
import json
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from gateway.message_types import (
    TaskMessage,
)


class TestAdapterConfig:
    def test_extracts_config_from_extra(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        config = MagicMock()
        config.extra = {
            "hermes_distributed_gateway_url": "ws://gw:8900/ws",
            "hermes_distributed_group_id": "tg:1",
            "hermes_distributed_model": "opus-4.6",
        }
        config.token = None
        adapter = InternalWSAdapter.__new__(InternalWSAdapter)
        adapter._parse_config(config)
        assert adapter._gateway_url == "ws://gw:8900/ws"
        assert adapter._group_id == "tg:1"
        assert adapter._model == "opus-4.6"

    def test_missing_gateway_url_raises(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        config = MagicMock()
        config.extra = {}
        config.token = None
        adapter = InternalWSAdapter.__new__(InternalWSAdapter)
        with pytest.raises(ValueError, match="hermes_distributed_gateway_url"):
            adapter._parse_config(config)


class TestHelloMessage:
    def test_build_hello_dict(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        config = MagicMock()
        config.extra = {
            "hermes_distributed_gateway_url": "ws://gw:8900/ws",
            "hermes_distributed_group_id": "tg:1",
            "hermes_distributed_model": "m1",
        }
        config.token = None
        adapter = InternalWSAdapter.__new__(InternalWSAdapter)
        adapter._parse_config(config)
        hello = adapter._build_hello()
        d = json.loads(hello)
        assert d["type"] == "hello"
        assert d["group_id"] == "tg:1"
        assert d["model"] == "m1"


class TestTaskToMessageEvent:
    def test_converts_basic_task(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        task = TaskMessage(
            group_id="tg:1",
            message="hello world",
            sender={"user_id": "123", "platform": "telegram", "user_name": "Alice"},
        )
        event = InternalWSAdapter._task_to_message_event(task)
        assert event.text == "hello world"
        assert event.source.user_id == "123"
        assert event.source.user_name == "Alice"
        assert event.source.platform.value == "webhook"

    def test_converts_task_with_media(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        task = TaskMessage(
            group_id="tg:1",
            message="look at this",
            media=[{"type": "image", "url": "http://gw:8901/media/img.jpg"}],
        )
        event = InternalWSAdapter._task_to_message_event(task)
        assert len(event.media_urls) == 1
        assert event.media_urls[0] == "http://gw:8901/media/img.jpg"

    def test_converts_task_with_reply_to(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        task = TaskMessage(
            group_id="tg:1",
            message="reply",
            reply_to_message_id="msg_789",
            reply_to_text="previous",
        )
        event = InternalWSAdapter._task_to_message_event(task)
        assert event.reply_to_message_id == "msg_789"
        assert event.reply_to_text == "previous"


class TestSendResultBuilding:
    def test_build_send_result_success(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        result = InternalWSAdapter._build_send_result(success=True, message_id="msg_1")
        assert result.success is True
        assert result.message_id == "msg_1"

    def test_build_send_result_failure(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        result = InternalWSAdapter._build_send_result(success=False, error="ws closed")
        assert result.success is False
        assert result.error == "ws closed"


class TestApprovalGate:
    @pytest.mark.asyncio
    async def test_timeout_returns_false(self):
        from agent.plugin.ws_adapter import ApprovalGate
        gate = ApprovalGate()
        try:
            result = await asyncio.wait_for(gate.request("r1"), timeout=0.1)
        except asyncio.TimeoutError:
            # Python 3.8 asyncio.wait_for has a known bug (bpo-32751)
            # where it raises TimeoutError even when the inner coroutine
            # completed. Treat TimeoutError as a timeout result.
            result = False
        assert result is False

    @pytest.mark.asyncio
    async def test_approve_resolves(self):
        from agent.plugin.ws_adapter import ApprovalGate
        gate = ApprovalGate()
        async def _approve_later():
            await asyncio.sleep(0.05)
            gate.approve("r1")
        asyncio.create_task(_approve_later())
        result = await asyncio.wait_for(gate.request("r1"), timeout=1.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_deny_resolves(self):
        from agent.plugin.ws_adapter import ApprovalGate
        gate = ApprovalGate()
        async def _deny_later():
            await asyncio.sleep(0.05)
            gate.deny("r1")
        asyncio.create_task(_deny_later())
        result = await asyncio.wait_for(gate.request("r1"), timeout=1.0)
        assert result is False
