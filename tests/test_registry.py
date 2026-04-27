"""Tests for manager.registry.AgentRegistry — SQLite state storage."""
import asyncio

import pytest
import pytest_asyncio

from manager.registry import AgentRegistry


@pytest_asyncio.fixture
async def registry():
    reg = AgentRegistry(db_path=":memory:")
    await reg.init()
    yield reg
    await reg.close()


def _srv(server_id="srv-1", hostname="host-1", cpu_cores=4, mem_total_gb=16, mem_available_gb=12):
    """Shorthand for register_server keyword args."""
    return dict(server_id=server_id, hostname=hostname, cpu_cores=cpu_cores,
                mem_total_gb=mem_total_gb, mem_available_gb=mem_available_gb)


class TestServerRegistration:
    @pytest.mark.asyncio
    async def test_register_server(self, registry):
        await registry.register_server(
            server_id="srv-1", hostname="host-1",
            cpu_cores=8, mem_total_gb=32, mem_available_gb=24,
        )
        servers = await registry.list_servers()
        assert len(servers) == 1
        assert servers[0]["server_id"] == "srv-1"
        assert servers[0]["hostname"] == "host-1"
        assert servers[0]["status"] == "online"

    @pytest.mark.asyncio
    async def test_update_heartbeat(self, registry):
        await registry.register_server(**_srv())
        await registry.update_heartbeat("srv-1", mem_available_gb=10, mem_used_gb=6.0, cpu_percent=45)
        servers = await registry.list_servers()
        assert servers[0]["mem_available_gb"] == 10
        assert servers[0]["mem_used_gb"] == 6.0
        assert servers[0]["cpu_percent"] == 45

    @pytest.mark.asyncio
    async def test_mark_server_lost(self, registry):
        await registry.register_server(**_srv())
        await registry.mark_server_lost("srv-1")
        servers = await registry.list_servers()
        assert servers[0]["status"] == "lost"


class TestAgentLifecycle:
    @pytest.mark.asyncio
    async def test_create_agent(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(
            group_id="tg:1", server_id="srv-1",
            profile="research", model="opus-4.6",
        )
        agent = await registry.get_agent("tg:1")
        assert agent is not None
        assert agent["server_id"] == "srv-1"
        assert agent["status"] == "starting"

    @pytest.mark.asyncio
    async def test_agent_started(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.agent_started("tg:1", pid=12345)
        agent = await registry.get_agent("tg:1")
        assert agent["pid"] == 12345
        assert agent["status"] == "running"

    @pytest.mark.asyncio
    async def test_agent_stopped(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.agent_started("tg:1", pid=12345)
        await registry.agent_stopped("tg:1", exit_code=0, reason="idle_timeout")
        agent = await registry.get_agent("tg:1")
        assert agent["status"] == "stopped"
        assert agent["exit_code"] == 0

    @pytest.mark.asyncio
    async def test_agent_lost(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.agent_started("tg:1", pid=12345)
        await registry.mark_server_lost("srv-1")
        agent = await registry.get_agent("tg:1")
        assert agent["status"] == "lost"

    @pytest.mark.asyncio
    async def test_touch_agent_activity(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.agent_started("tg:1", pid=12345)
        await registry.touch_agent_activity("tg:1")
        agent = await registry.get_agent("tg:1")
        assert agent["last_active_at"] is not None

    @pytest.mark.asyncio
    async def test_list_agents(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="p1", model="m1")
        await registry.create_agent(group_id="tg:2", server_id="srv-1", profile="p2", model="m2")
        agents = await registry.list_agents()
        assert len(agents) == 2

    @pytest.mark.asyncio
    async def test_delete_agent(self, registry):
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.delete_agent("tg:1")
        agent = await registry.get_agent("tg:1")
        assert agent is None

    @pytest.mark.asyncio
    async def test_get_idle_agents(self, registry):
        from datetime import datetime, timedelta
        await registry.register_server(**_srv())
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="default", model="m1")
        await registry.agent_started("tg:1", pid=12345)
        old_time = (datetime.utcnow() - timedelta(minutes=31)).isoformat()
        await registry._execute(
            "UPDATE agents SET last_active_at = ? WHERE group_id = ?",
            (old_time, "tg:1"),
        )
        idle = await registry.get_idle_agents(timeout_minutes=30)
        assert len(idle) == 1
        assert idle[0]["group_id"] == "tg:1"

    @pytest.mark.asyncio
    async def test_get_timedout_servers(self, registry):
        from datetime import datetime, timedelta
        await registry.register_server(**_srv())
        old_time = (datetime.utcnow() - timedelta(seconds=91)).isoformat()
        await registry._execute(
            "UPDATE servers SET last_heartbeat = ? WHERE server_id = ?",
            (old_time, "srv-1"),
        )
        timedout = await registry.get_timedout_servers(timeout_seconds=90)
        assert len(timedout) == 1
        assert timedout[0]["server_id"] == "srv-1"

    @pytest.mark.asyncio
    async def test_select_best_server(self, registry):
        await registry.register_server(**_srv())
        await registry.register_server(server_id="srv-2", hostname="host-2", cpu_cores=8, mem_total_gb=32, mem_available_gb=28)
        await registry.create_agent(group_id="tg:1", server_id="srv-1", profile="p1", model="m1")
        await registry.agent_started("tg:1", pid=111)
        best = await registry.select_best_server()
        assert best == "srv-2"

    @pytest.mark.asyncio
    async def test_select_best_server_all_busy(self, registry):
        best = await registry.select_best_server()
        assert best is None


class TestLogStorage:
    @pytest.mark.asyncio
    async def test_append_log(self, registry):
        await registry.append_log(
            group_id="tg:1", pid=12345, stream="stdout", line="Loading tools...",
        )
        logs = await registry.get_logs("tg:1")
        assert len(logs) == 1
        assert logs[0]["line"] == "Loading tools..."

    @pytest.mark.asyncio
    async def test_get_logs_with_tail(self, registry):
        for i in range(10):
            await registry.append_log(group_id="tg:1", pid=12345, stream="stdout", line="line %d" % i)
        logs = await registry.get_logs("tg:1", tail=3)
        assert len(logs) == 3
        assert logs[0]["line"] == "line 7"

    @pytest.mark.asyncio
    async def test_get_logs_nonexistent_group(self, registry):
        logs = await registry.get_logs("nonexistent")
        assert logs == []

    @pytest.mark.asyncio
    async def test_delete_old_logs(self, registry):
        from datetime import datetime, timedelta
        await registry.append_log(group_id="tg:1", pid=1, stream="stdout", line="old log")
        await registry.append_log(group_id="tg:1", pid=1, stream="stdout", line="recent log")
        # Backdate the first log
        old_time = (datetime.utcnow() - timedelta(days=8)).isoformat()
        await registry._execute(
            "UPDATE logs SET timestamp = ? WHERE id = 1", (old_time,),
        )
        deleted = await registry.delete_old_logs(retention_days=7)
        assert deleted == 1
        logs = await registry.get_logs("tg:1")
        assert len(logs) == 1
        assert logs[0]["line"] == "recent log"
