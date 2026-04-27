"""Tests for agent.agent_service — message building helpers, ApprovalGate, InterruptFlag."""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.message_types import (
    HelloMessage,
    TaskMessage,
    CompleteMessage,
    StreamMessage,
    ErrorMessage,
    ProgressMessage,
    ApprovalRequestMessage,
    parse_gateway_message,
)


class TestBuildHello:
    """Test the build_hello helper."""

    def test_basic_hello_structure(self):
        from agent.agent_service import build_hello
        hello = build_hello(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes",
            model="opus-4.6",
            toolsets=["terminal", "file"],
        )
        d = json.loads(hello)
        assert d["type"] == "hello"
        assert d["version"] == 1
        assert d["group_id"] == "telegram:group:1001"
        assert d["profile"] == "/home/user/.hermes"
        assert d["model"] == "opus-4.6"
        assert d["toolsets"] == ["terminal", "file"]
        assert "streaming" in d["capabilities"]
        assert "approval" in d["capabilities"]
        assert "interrupt" in d["capabilities"]

    def test_empty_toolsets(self):
        from agent.agent_service import build_hello
        hello = build_hello(
            group_id="g:1", profile="", model="m1", toolsets=[],
        )
        d = json.loads(hello)
        assert d["toolsets"] == []

    def test_is_valid_json_string(self):
        from agent.agent_service import build_hello
        hello = build_hello("g:1", "/p", "m1", ["t"])
        assert isinstance(hello, str)
        # Should not raise
        json.loads(hello)


class TestBuildAgentMessage:
    """Test the build_agent_message serialization helper."""

    def test_stream_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(StreamMessage(group_id="tg:1", token="hi"))
        d = json.loads(msg)
        assert d["type"] == "stream"
        assert d["token"] == "hi"
        assert d["group_id"] == "tg:1"

    def test_complete_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(CompleteMessage(
            group_id="tg:1", final_response="done",
            api_calls=3, tokens={"input": 100, "output": 50},
        ))
        d = json.loads(msg)
        assert d["type"] == "complete"
        assert d["api_calls"] == 3
        assert d["tokens"] == {"input": 100, "output": 50}
        assert d["final_response"] == "done"

    def test_error_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(ErrorMessage(
            group_id="tg:1", message="boom", fatal=True,
        ))
        d = json.loads(msg)
        assert d["type"] == "error"
        assert d["fatal"] is True
        assert d["message"] == "boom"

    def test_progress_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(ProgressMessage(
            group_id="tg:1", tool="bash", preview="ls -la",
        ))
        d = json.loads(msg)
        assert d["type"] == "progress"
        assert d["tool"] == "bash"

    def test_approval_request_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(ApprovalRequestMessage(
            group_id="tg:1", request_id="req_1",
            command="rm -rf /", reason="cleanup",
        ))
        d = json.loads(msg)
        assert d["type"] == "approval_request"
        assert d["request_id"] == "req_1"
        assert d["command"] == "rm -rf /"

    def test_hello_message(self):
        from agent.agent_service import build_agent_message
        msg = build_agent_message(HelloMessage(
            group_id="tg:1", profile="/p", model="m1",
        ))
        d = json.loads(msg)
        assert d["type"] == "hello"
        assert d["group_id"] == "tg:1"


class TestApprovalGate:
    """Test the ApprovalGate async synchronization primitive."""

    @pytest.mark.asyncio
    async def test_approve_resolves_future(self):
        from agent.agent_service import ApprovalGate
        gate = ApprovalGate()

        async def wait_for_approval():
            return await gate.request("req_1")

        task = asyncio.create_task(wait_for_approval())
        await asyncio.sleep(0)  # Let the task start
        gate.approve("req_1")
        result = await task
        assert result is True

    @pytest.mark.asyncio
    async def test_deny_resolves_future(self):
        from agent.agent_service import ApprovalGate
        gate = ApprovalGate()

        async def wait_for_approval():
            return await gate.request("req_2")

        task = asyncio.create_task(wait_for_approval())
        await asyncio.sleep(0)
        gate.deny("req_2")
        result = await task
        assert result is False

    @pytest.mark.asyncio
    async def test_unknown_request_id_no_crash(self):
        from agent.agent_service import ApprovalGate
        gate = ApprovalGate()
        # Approving/denying non-existent requests should not raise
        gate.approve("nonexistent")
        gate.deny("nonexistent")


class TestInterruptFlag:
    """Test the InterruptFlag thread-safe flag."""

    def test_initial_state_is_clear(self):
        from agent.agent_service import InterruptFlag
        flag = InterruptFlag()
        assert flag.is_set() is False

    def test_set_and_check(self):
        from agent.agent_service import InterruptFlag
        flag = InterruptFlag()
        flag.set()
        assert flag.is_set() is True

    def test_clear(self):
        from agent.agent_service import InterruptFlag
        flag = InterruptFlag()
        flag.set()
        flag.clear()
        assert flag.is_set() is False

    def test_set_multiple_times(self):
        from agent.agent_service import InterruptFlag
        flag = InterruptFlag()
        flag.set()
        flag.set()
        assert flag.is_set() is True


class TestParseTaskMessage:
    """Test parsing task messages from Gateway — integration with message_types."""

    def test_parses_task_message(self):
        raw = {
            "version": 1, "type": "task",
            "group_id": "tg:1",
            "message": "write a hello world",
            "sender": {"user_id": "123", "platform": "telegram"},
        }
        msg = parse_gateway_message(raw)
        assert isinstance(msg, TaskMessage)
        assert msg.message == "write a hello world"
        assert msg.sender["user_id"] == "123"
        assert msg.group_id == "tg:1"

    def test_parse_with_media(self):
        raw = {
            "version": 1, "type": "task",
            "group_id": "tg:2",
            "message": "what is this?",
            "media": [{"type": "image", "url": "http://example.com/img.png"}],
        }
        msg = parse_gateway_message(raw)
        assert len(msg.media) == 1
        assert msg.media[0]["type"] == "image"
