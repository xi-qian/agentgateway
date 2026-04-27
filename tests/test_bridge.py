"""Unit tests for gateway.bridge.Bridge."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.bridge import Bridge
from gateway.message_types import (
    StreamMessage,
    CompleteMessage,
    ErrorMessage,
    ApprovalRequestMessage,
    ProgressMessage,
    HeartbeatMessage,
)


@pytest.fixture
def bridge():
    return Bridge()


class TestStreamHandling:
    @pytest.mark.asyncio
    async def test_stream_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = StreamMessage(group_id="tg:1", token="hello")
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_stream_no_handler(self, bridge):
        msg = StreamMessage(group_id="tg:1", token="hello")
        await bridge.handle_agent_message(msg, "tg:1")


class TestCompleteHandling:
    @pytest.mark.asyncio
    async def test_complete_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = CompleteMessage(group_id="tg:1", final_response="done", api_calls=2)
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_complete_no_handler(self, bridge):
        msg = CompleteMessage(group_id="tg:1", final_response="done")
        await bridge.handle_agent_message(msg, "tg:1")


class TestApprovalHandling:
    @pytest.mark.asyncio
    async def test_approval_request_with_callback(self, bridge):
        callback = AsyncMock()
        bridge.register_approval_callback("tg:1", callback)
        msg = ApprovalRequestMessage(
            group_id="tg:1",
            request_id="r1",
            command="rm -rf /tmp/x",
            reason="cleanup",
        )
        await bridge.handle_agent_message(msg, "tg:1")
        callback.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_approval_request_no_callback(self, bridge):
        msg = ApprovalRequestMessage(
            group_id="tg:1",
            request_id="r1",
            command="rm -rf /tmp/x",
        )
        await bridge.handle_agent_message(msg, "tg:1")


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = HeartbeatMessage()
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_not_awaited()


class TestProgressHandling:
    @pytest.mark.asyncio
    async def test_progress_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = ProgressMessage(group_id="tg:1", tool="terminal", preview="pip install x")
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_error_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = ErrorMessage(group_id="tg:1", message="API rate limit", fatal=False)
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_error_no_handler(self, bridge):
        msg = ErrorMessage(group_id="tg:1", message="crash", fatal=True)
        await bridge.handle_agent_message(msg, "tg:1")
