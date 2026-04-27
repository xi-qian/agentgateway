import json
import pytest

from gateway.message_types import (
    HelloMessage, TaskMessage, StreamMessage, CompleteMessage,
    ErrorMessage, InterruptMessage, ApprovalRequestMessage,
    ApprovedMessage, DeniedMessage, HeartbeatMessage,
    ProgressMessage, parse_agent_message, parse_gateway_message,
    ProtocolError,
)


class TestHelloMessage:
    def test_serialize(self):
        msg = HelloMessage(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes/profiles/grp-1001",
            model="opus-4.6",
            toolsets=["terminal", "file"],
            capabilities=["streaming"],
        )
        d = msg.to_dict()
        assert d["version"] == 1
        assert d["type"] == "hello"
        assert d["group_id"] == "telegram:group:1001"

    def test_round_trip(self):
        msg = HelloMessage(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes",
            model="sonnet-4.6",
            toolsets=["terminal"],
            capabilities=["streaming", "approval"],
        )
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert isinstance(parsed, HelloMessage)
        assert parsed.group_id == "telegram:group:1001"
        assert parsed.model == "sonnet-4.6"


class TestTaskMessage:
    def test_serialize(self):
        msg = TaskMessage(
            group_id="telegram:group:1001",
            message="hello",
            sender={"user_id": "123", "platform": "telegram", "user_name": "Alice"},
            media=[{"type": "image", "url": "http://gw:8901/media/img.jpg"}],
        )
        d = msg.to_dict()
        assert d["type"] == "task"
        assert d["message"] == "hello"
        assert len(d["media"]) == 1
        assert "context_prompt" not in d
        assert "history" not in d

    def test_serialize_with_history(self):
        msg = TaskMessage(
            group_id="tg:1",
            message="hi",
            context_prompt="You are helpful",
            history=[{"role": "user", "content": "prev"}],
        )
        d = msg.to_dict()
        assert d["context_prompt"] == "You are helpful"
        assert d["history"] == [{"role": "user", "content": "prev"}]

    def test_parse(self):
        d = {
            "version": 1,
            "type": "task",
            "group_id": "tg:1",
            "message": "hi",
            "sender": {"user_id": "1", "platform": "tg"},
        }
        msg = parse_gateway_message(d)
        assert isinstance(msg, TaskMessage)
        assert msg.message == "hi"
        assert msg.context_prompt == ""
        assert msg.history == []

    def test_parse_with_history(self):
        d = {
            "version": 1,
            "type": "task",
            "group_id": "tg:1",
            "message": "hi",
            "context_prompt": "system prompt",
            "history": [{"role": "assistant", "content": "prev response"}],
        }
        msg = parse_gateway_message(d)
        assert isinstance(msg, TaskMessage)
        assert msg.context_prompt == "system prompt"
        assert len(msg.history) == 1


class TestStreamMessage:
    def test_serialize(self):
        msg = StreamMessage(group_id="tg:1", token="hello")
        d = msg.to_dict()
        assert d["type"] == "stream"
        assert d["token"] == "hello"

    def test_parse(self):
        msg = parse_agent_message({"version": 1, "type": "stream", "group_id": "tg:1", "token": "x"})
        assert isinstance(msg, StreamMessage)


class TestCompleteMessage:
    def test_full(self):
        msg = CompleteMessage(
            group_id="tg:1",
            final_response="done",
            api_calls=3,
            tokens={"input": 100, "output": 50},
            cost_usd=0.001,
        )
        d = msg.to_dict()
        assert d["tokens"]["input"] == 100
        parsed = parse_agent_message(d)
        assert parsed.api_calls == 3
        assert parsed.cost_usd == 0.001
        assert parsed.interrupted is False
        assert "interrupted" not in d

    def test_interrupted(self):
        msg = CompleteMessage(
            group_id="tg:1",
            final_response="",
            interrupted=True,
        )
        d = msg.to_dict()
        assert d["interrupted"] is True
        parsed = parse_agent_message(d)
        assert parsed.interrupted is True

    def test_parse_interrupted_default(self):
        msg = parse_agent_message({
            "version": 1, "type": "complete", "group_id": "tg:1",
        })
        assert isinstance(msg, CompleteMessage)
        assert msg.interrupted is False


class TestErrorMessage:
    def test_fatal(self):
        msg = ErrorMessage(group_id="tg:1", message="crash", fatal=True)
        assert msg.fatal is True

    def test_non_fatal(self):
        msg = ErrorMessage(group_id="tg:1", message="rate limit")
        assert msg.fatal is False


class TestApprovalMessages:
    def test_request(self):
        msg = ApprovalRequestMessage(
            group_id="tg:1", request_id="r1",
            command="rm -rf /tmp/x", reason="cleanup",
        )
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert parsed.command == "rm -rf /tmp/x"

    def test_approved(self):
        msg = parse_gateway_message({"version": 1, "type": "approved", "group_id": "tg:1", "request_id": "r1"})
        assert isinstance(msg, ApprovedMessage)

    def test_denied(self):
        msg = parse_gateway_message({"version": 1, "type": "denied", "group_id": "tg:1", "request_id": "r1", "reason": "danger"})
        assert isinstance(msg, DeniedMessage)
        assert msg.reason == "danger"


class TestInterruptMessage:
    def test_parse(self):
        msg = parse_gateway_message({"version": 1, "type": "interrupt", "group_id": "tg:1"})
        assert isinstance(msg, InterruptMessage)


class TestHeartbeatMessage:
    def test_bidirectional(self):
        # Agent -> Gateway
        a = parse_agent_message({"version": 1, "type": "heartbeat"})
        assert isinstance(a, HeartbeatMessage)
        # Gateway -> Agent
        g = parse_gateway_message({"version": 1, "type": "heartbeat"})
        assert isinstance(g, HeartbeatMessage)


class TestProgressMessage:
    def test_serialize(self):
        msg = ProgressMessage(group_id="tg:1", tool="terminal", preview="pip install x", emoji="📦")
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert parsed.tool == "terminal"


class TestParseErrors:
    def test_unknown_type_raises(self):
        with pytest.raises(ProtocolError, match="Unknown agent message type"):
            parse_agent_message({"version": 1, "type": "unknown_type"})

    def test_missing_type_raises(self):
        with pytest.raises(ProtocolError, match="missing 'type'"):
            parse_agent_message({"version": 1})

    def test_missing_version_passes(self):
        # version is optional for forward compat
        msg = parse_agent_message({"type": "hello", "group_id": "tg:1", "profile": "/", "model": "m", "toolsets": [], "capabilities": []})
        assert isinstance(msg, HelloMessage)
