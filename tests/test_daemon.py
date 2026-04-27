"""Tests for agent.daemon -- process manager logic."""
import json
import pytest
from unittest.mock import MagicMock

from gateway.message_types import (
    StartAgentMessage,
    StopAgentMessage,
)


class TestDaemonConfig:
    def test_build_register(self):
        from agent.daemon import build_register
        reg = build_register(
            server_id="srv-1", hostname="host-1",
            cpu_cores=8, mem_total_gb=32, mem_available_gb=24,
        )
        d = json.loads(reg)
        assert d["type"] == "register"
        assert d["server_id"] == "srv-1"
        assert d["cpu_cores"] == 8

    def test_build_heartbeat(self):
        from agent.daemon import build_heartbeat
        hb = build_heartbeat(
            running_agents=[{"group_id": "tg:1", "pid": 12345}],
            mem_used_gb=3.2, mem_available_gb=12.8, cpu_percent=45,
        )
        d = json.loads(hb)
        assert d["type"] == "heartbeat"
        assert len(d["running_agents"]) == 1
        assert d["mem_used_gb"] == 3.2
        assert d["mem_available_gb"] == 12.8
        assert d["cpu_percent"] == 45


class TestChildRegistry:
    def test_add_and_remove(self):
        from agent.daemon import ChildRegistry
        reg = ChildRegistry()
        proc = MagicMock()
        proc.pid = 12345
        proc.returncode = None
        proc.poll.return_value = None

        reg.add("tg:1", proc)
        assert reg.get("tg:1") is proc
        assert reg.list_running() == ["tg:1"]
        assert reg.get_pid("tg:1") == 12345

        reg.remove("tg:1")
        assert reg.get("tg:1") is None
        assert reg.list_running() == []

    def test_get_running_agents_info(self):
        from agent.daemon import ChildRegistry
        reg = ChildRegistry()
        proc = MagicMock()
        proc.pid = 111
        proc.poll.return_value = None
        reg.add("tg:1", proc)

        proc2 = MagicMock()
        proc2.pid = 222
        proc2.poll.return_value = None
        reg.add("tg:2", proc2)

        info = reg.get_running_agents_info()
        assert len(info) == 2
        assert info[0]["group_id"] == "tg:1"
        assert info[0]["pid"] == 111


class TestParseManagerCommands:
    def test_parse_start_agent(self):
        from agent.daemon import parse_manager_command
        cmd = StartAgentMessage(
            group_id="tg:1", profile="research",
            gateway_url="ws://gw:8900/ws",
        ).to_dict()
        msg = parse_manager_command(cmd)
        assert msg["type"] == "start_agent"
        assert msg["group_id"] == "tg:1"

    def test_parse_stop_agent(self):
        from agent.daemon import parse_manager_command
        cmd = StopAgentMessage(group_id="tg:1", force=True).to_dict()
        msg = parse_manager_command(cmd)
        assert msg["type"] == "stop_agent"
        assert msg["force"] is True


class TestCpuPercent:
    def setup_method(self):
        """Reset module-level CPU state before each test."""
        import agent.daemon as dm
        dm._cpu_prev_total = None
        dm._cpu_prev_idle = None

    def test_first_call_returns_zero(self):
        from agent.daemon import _get_cpu_percent
        # First call has no baseline, should return 0.0
        assert _get_cpu_percent() == 0.0

    def test_non_linux_returns_zero(self):
        import agent.daemon as dm
        import platform
        orig = platform.system
        platform.system = lambda: "Darwin"
        try:
            assert dm._get_cpu_percent() == 0.0
        finally:
            platform.system = orig

    def test_two_calls_give_delta(self):
        import agent.daemon as dm
        # Simulate /proc/stat readings: first call stores baseline,
        # second call computes delta.
        stat_data_first = "cpu  100 5 50 200 0 0 0\n"
        stat_data_second = "cpu  200 10 100 300 0 0 0\n"

        import tempfile, os
        with tempfile.NamedTemporaryFile(mode="w", suffix=".stat", delete=False) as f:
            f.write(stat_data_first)
            tmp_path = f.name

        try:
            # Monkey-patch open to return our fake /proc/stat
            original_open = open

            def fake_open(path, *a, **kw):
                if str(path).endswith("/proc/stat") or path == "/proc/stat":
                    return original_open(tmp_path, *a, **kw)
                return original_open(path, *a, **kw)

            import builtins
            builtins.open = fake_open
            try:
                dm._cpu_prev_total = None
                dm._cpu_prev_idle = None
                r1 = dm._get_cpu_percent()  # stores baseline
                assert r1 == 0.0

                # Update file content for second reading
                with original_open(tmp_path, "w") as f:
                    f.write(stat_data_second)

                r2 = dm._get_cpu_percent()  # computes delta
                # delta_total = 650 - 355 = 295, delta_idle = 300 - 200 = 100
                # cpu = (1 - 100/295) * 100 = 66.1%
                assert r2 > 0.0
                assert r2 < 100.0
            finally:
                builtins.open = original_open
        finally:
            os.unlink(tmp_path)
