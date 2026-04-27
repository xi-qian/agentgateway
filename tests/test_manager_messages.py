"""Tests for daemon <-> Manager protocol message types (Link 3)."""
import pytest
from gateway.message_types import (
    RegisterMessage, DaemonHeartbeatMessage,
    AgentStartedMessage, AgentStoppedMessage, LogMessage,
    StartAgentMessage, StopAgentMessage,
    parse_daemon_message, parse_manager_message,
    ProtocolError,
)


class TestRegisterMessage:
    def test_serialize(self):
        msg = RegisterMessage(
            server_id="server-a", hostname="gpu-node-1",
            cpu_cores=8, mem_total_gb=32, mem_available_gb=24,
        )
        d = msg.to_dict()
        assert d["type"] == "register"
        assert d["server_id"] == "server-a"
        assert d["cpu_cores"] == 8

    def test_round_trip(self):
        msg = RegisterMessage(
            server_id="srv-1", hostname="host-1",
            cpu_cores=4, mem_total_gb=16, mem_available_gb=12,
        )
        parsed = parse_daemon_message(msg.to_dict())
        assert isinstance(parsed, RegisterMessage)
        assert parsed.hostname == "host-1"


class TestDaemonHeartbeatMessage:
    def test_with_resources(self):
        msg = DaemonHeartbeatMessage(
            running_agents=[{"group_id": "tg:1", "pid": 1234, "mem_mb": 280}],
            mem_used_gb=3.2, cpu_percent=45,
        )
        d = msg.to_dict()
        assert d["type"] == "heartbeat"
        assert d["mem_used_gb"] == 3.2
        assert len(d["running_agents"]) == 1

    def test_round_trip(self):
        msg = DaemonHeartbeatMessage(
            running_agents=[], mem_used_gb=1.5, cpu_percent=10,
        )
        parsed = parse_daemon_message(msg.to_dict())
        assert isinstance(parsed, DaemonHeartbeatMessage)
        assert parsed.cpu_percent == 10


class TestAgentStartedMessage:
    def test_round_trip(self):
        msg = AgentStartedMessage(group_id="tg:1", pid=12345, status="running")
        parsed = parse_daemon_message(msg.to_dict())
        assert parsed.pid == 12345
        assert parsed.status == "running"


class TestAgentStoppedMessage:
    def test_with_reason(self):
        msg = AgentStoppedMessage(
            group_id="tg:1", pid=12345, exit_code=0, reason="idle_timeout",
        )
        parsed = parse_daemon_message(msg.to_dict())
        assert parsed.reason == "idle_timeout"
        assert parsed.exit_code == 0

    def test_no_reason(self):
        msg = AgentStoppedMessage(group_id="tg:2", pid=99, exit_code=1)
        parsed = parse_daemon_message(msg.to_dict())
        assert parsed.reason == ""


class TestLogMessage:
    def test_stdout(self):
        msg = LogMessage(group_id="tg:1", pid=12345, stream="stdout", line="Loading tools...")
        d = msg.to_dict()
        assert d["stream"] == "stdout"
        assert d["line"] == "Loading tools..."
        parsed = parse_daemon_message(d)
        assert isinstance(parsed, LogMessage)

    def test_stderr(self):
        msg = LogMessage(group_id="tg:1", pid=12345, stream="stderr", line="Error: timeout")
        parsed = parse_daemon_message(msg.to_dict())
        assert parsed.stream == "stderr"


class TestStartAgentMessage:
    def test_round_trip(self):
        msg = StartAgentMessage(
            group_id="tg:1", profile="research",
            gateway_url="ws://gateway:8900/ws",
            hermes_home="/home/user/.hermes",
        )
        parsed = parse_manager_message(msg.to_dict())
        assert isinstance(parsed, StartAgentMessage)
        assert parsed.profile == "research"
        assert parsed.gateway_url == "ws://gateway:8900/ws"


class TestStopAgentMessage:
    def test_graceful(self):
        msg = StopAgentMessage(group_id="tg:1", force=False)
        parsed = parse_manager_message(msg.to_dict())
        assert parsed.force is False

    def test_force(self):
        msg = StopAgentMessage(group_id="tg:1", force=True)
        parsed = parse_manager_message(msg.to_dict())
        assert parsed.force is True


class TestParseErrors:
    def test_unknown_daemon_type(self):
        with pytest.raises(ProtocolError, match="Unknown daemon message type"):
            parse_daemon_message({"version": 1, "type": "bogus"})

    def test_unknown_manager_type(self):
        with pytest.raises(ProtocolError, match="Unknown manager message type"):
            parse_manager_message({"version": 1, "type": "bogus"})
