"""Full integration test: Manager + daemon + Gateway cold path."""
import asyncio
import json
import pytest
from aiohttp import WSMsgType
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from gateway.message_types import (
    RegisterMessage,
    AgentStartedMessage,
    AgentStoppedMessage,
    DaemonHeartbeatMessage,
)


class TestFullColdPath(AioHTTPTestCase):
    async def get_application(self):
        from manager.server import create_manager_app
        return await create_manager_app()

    @unittest_run_loop
    async def test_cold_path_end_to_end(self):
        """Simulate full cold path: daemon registers, agent created, lifecycle."""
        # Step 1: Daemon registers via WebSocket
        daemon_ws = await self.client.ws_connect("/ws")
        await daemon_ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Step 2: Create agent via REST
        resp = await self.client.post(
            "/api/v1/agents",
            json={"group_id": "tg:new", "profile": "research"},
        )
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "starting"
        assert data["server_id"] == "srv-1"

        # Step 3: Daemon receives start_agent
        msg = await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)
        assert msg["type"] == "start_agent"
        assert msg["group_id"] == "tg:new"
        assert msg["profile"] == "research"

        # Step 4: Agent reports started
        await daemon_ws.send_json(
            AgentStartedMessage(
                group_id="tg:new",
                pid=12345,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Step 5: Verify agent status via REST
        resp = await self.client.get("/api/v1/agents/tg:new")
        agent_data = await resp.json()
        assert agent_data["status"] == "running"
        assert agent_data["pid"] == 12345

        # Step 6: Heartbeat with resource info
        await daemon_ws.send_json(
            DaemonHeartbeatMessage(
                running_agents=[{"group_id": "tg:new", "pid": 12345}],
                mem_used_gb=2.5,
                cpu_percent=30,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Verify server resource info updated
        resp = await self.client.get("/api/v1/servers")
        servers = (await resp.json())["servers"]
        assert len(servers) == 1
        assert servers[0]["server_id"] == "srv-1"
        assert servers[0]["cpu_percent"] == 30
        assert servers[0]["mem_used_gb"] == 2.5

        await daemon_ws.close()

    @unittest_run_loop
    async def test_delete_agent_sends_stop_command(self):
        """Delete agent via REST sends stop_agent to daemon."""
        # Register daemon
        daemon_ws = await self.client.ws_connect("/ws")
        await daemon_ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Create agent
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:1"})
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "starting"

        # Consume start_agent from daemon
        msg = await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)
        assert msg["type"] == "start_agent"
        assert msg["group_id"] == "tg:1"

        # Agent reports started
        await daemon_ws.send_json(
            AgentStartedMessage(group_id="tg:1", pid=111).to_dict()
        )
        await asyncio.sleep(0.1)

        # Verify agent is running
        resp = await self.client.get("/api/v1/agents/tg:1")
        assert (await resp.json())["status"] == "running"

        # Delete agent via REST
        resp = await self.client.delete("/api/v1/agents/tg:1")
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "deleted"

        # Daemon should receive stop_agent (delete sends graceful stop)
        msg = await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)
        assert msg["type"] == "stop_agent"
        assert msg["group_id"] == "tg:1"
        assert msg["force"] is False

        # Agent should no longer exist
        resp = await self.client.get("/api/v1/agents/tg:1")
        assert resp.status == 404

        await daemon_ws.close()

    @unittest_run_loop
    async def test_disconnect_agent_sends_stop_command(self):
        """Disconnect agent sends stop_agent with force from request body."""
        # Register daemon
        daemon_ws = await self.client.ws_connect("/ws")
        await daemon_ws.send_json(
            RegisterMessage(
                server_id="srv-2",
                hostname="host-2",
                cpu_cores=8,
                mem_total_gb=32,
                mem_available_gb=28,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Create and start agent
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:2"})
        assert resp.status == 201
        await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)  # consume start_agent
        await daemon_ws.send_json(
            AgentStartedMessage(group_id="tg:2", pid=222).to_dict()
        )
        await asyncio.sleep(0.1)

        # Disconnect agent (non-force)
        resp = await self.client.post(
            "/api/v1/agents/tg:2/disconnect",
            json={"force": False},
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "disconnected"

        # Daemon should receive stop_agent with force=False
        msg = await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)
        assert msg["type"] == "stop_agent"
        assert msg["group_id"] == "tg:2"
        assert msg["force"] is False

        await daemon_ws.close()

    @unittest_run_loop
    async def test_agent_stopped_updates_status(self):
        """Agent stopped message updates agent status in registry."""
        # Register daemon
        daemon_ws = await self.client.ws_connect("/ws")
        await daemon_ws.send_json(
            RegisterMessage(
                server_id="srv-3",
                hostname="host-3",
                cpu_cores=2,
                mem_total_gb=8,
                mem_available_gb=6,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Create agent
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:3"})
        assert resp.status == 201
        await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)  # consume start_agent

        # Agent reports started
        await daemon_ws.send_json(
            AgentStartedMessage(group_id="tg:3", pid=333).to_dict()
        )
        await asyncio.sleep(0.1)
        assert (await (await self.client.get("/api/v1/agents/tg:3")).json())["status"] == "running"

        # Agent reports stopped
        await daemon_ws.send_json(
            AgentStoppedMessage(
                group_id="tg:3",
                pid=333,
                exit_code=0,
                reason="completed",
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Verify agent status is stopped
        resp = await self.client.get("/api/v1/agents/tg:3")
        agent_data = await resp.json()
        assert agent_data["status"] == "stopped"
        assert agent_data["exit_code"] == 0

        await daemon_ws.close()

    @unittest_run_loop
    async def test_create_agent_duplicate_returns_existing(self):
        """Creating an agent that already exists returns existing record."""
        # Register daemon
        daemon_ws = await self.client.ws_connect("/ws")
        await daemon_ws.send_json(
            RegisterMessage(
                server_id="srv-4",
                hostname="host-4",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Create agent first time
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:dup"})
        assert resp.status == 201
        await asyncio.wait_for(daemon_ws.receive_json(), timeout=2)  # consume start_agent

        # Create agent second time -- should return existing (status 200, not 201)
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:dup"})
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "starting"
        assert data["server_id"] == "srv-4"
        assert "message" in data

        await daemon_ws.close()

    @unittest_run_loop
    async def test_no_servers_returns_503(self):
        """Creating an agent with no registered servers returns 503."""
        resp = await self.client.post(
            "/api/v1/agents",
            json={"group_id": "tg:noserver"},
        )
        assert resp.status == 503
        data = await resp.json()
        assert "error" in data
