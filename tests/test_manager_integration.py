"""Integration tests: Manager REST API + daemon WebSocket round-trip."""
import asyncio
import json

from aiohttp import WSMsgType
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from gateway.message_types import (
    RegisterMessage,
    DaemonHeartbeatMessage,
    AgentStartedMessage,
    AgentStoppedMessage,
    LogMessage,
)


class TestManagerDaemonIntegration(AioHTTPTestCase):
    async def get_application(self):
        from manager.server import create_manager_app
        return await create_manager_app()

    @unittest_run_loop
    async def test_daemon_register_and_list_servers(self):
        ws = await self.client.ws_connect("/ws")
        reg = RegisterMessage(
            server_id="srv-1",
            hostname="host-1",
            cpu_cores=8,
            mem_total_gb=32,
            mem_available_gb=24,
        )
        await ws.send_json(reg.to_dict())
        await asyncio.sleep(0.1)

        resp = await self.client.get("/api/v1/servers")
        data = await resp.json()
        assert len(data["servers"]) == 1
        assert data["servers"][0]["server_id"] == "srv-1"
        assert data["servers"][0]["status"] == "online"
        await ws.close()

    @unittest_run_loop
    async def test_create_agent_sends_start_agent(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        resp = await self.client.post(
            "/api/v1/agents",
            json={"group_id": "tg:1", "profile": "research", "model": "opus-4.6"},
        )
        assert resp.status == 201
        data = await resp.json()
        assert data["status"] == "starting"
        assert data["server_id"] == "srv-1"

        # Daemon should receive start_agent command
        msg = await asyncio.wait_for(ws.receive_json(), timeout=2)
        assert msg["type"] == "start_agent"
        assert msg["group_id"] == "tg:1"
        assert msg["profile"] == "research"
        await ws.close()

    @unittest_run_loop
    async def test_agent_lifecycle_via_ws(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Create agent via REST
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:1"})
        assert resp.status == 201
        await ws.receive_json()  # consume start_agent command

        # Daemon reports agent started
        await ws.send_json(AgentStartedMessage(group_id="tg:1", pid=12345).to_dict())
        await asyncio.sleep(0.1)

        resp = await self.client.get("/api/v1/agents/tg:1")
        data = await resp.json()
        assert data["status"] == "running"
        assert data["pid"] == 12345

        # Daemon reports agent stopped
        await ws.send_json(
            AgentStoppedMessage(
                group_id="tg:1", pid=12345, exit_code=0, reason="idle_timeout",
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        resp = await self.client.get("/api/v1/agents/tg:1")
        data = await resp.json()
        assert data["status"] == "stopped"
        await ws.close()

    @unittest_run_loop
    async def test_daemon_disconnect_marks_server_lost(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        # Close the WebSocket to simulate daemon disconnect
        await ws.close()

        # Poll until server status becomes "lost" (avoid fixed-sleep flakiness)
        for _ in range(20):
            await asyncio.sleep(0.05)
            resp = await self.client.get("/api/v1/servers")
            data = await resp.json()
            if data["servers"] and data["servers"][0]["status"] == "lost":
                break
        else:
            resp = await self.client.get("/api/v1/servers")
            data = await resp.json()
        assert data["servers"][0]["status"] == "lost"

    @unittest_run_loop
    async def test_log_collection(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        await ws.send_json(
            LogMessage(
                group_id="tg:1", pid=12345, stream="stdout", line="Loading tools...",
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        resp = await self.client.get("/api/v1/logs/tg:1")
        data = await resp.json()
        assert len(data["logs"]) == 1
        assert data["logs"][0]["line"] == "Loading tools..."
        await ws.close()

    @unittest_run_loop
    async def test_no_register_closes_connection(self):
        ws = await self.client.ws_connect("/ws")
        # Send a valid daemon message that is NOT register
        await ws.send_json(
            DaemonHeartbeatMessage(running_agents=[], mem_used_gb=1.0).to_dict()
        )
        msg = await ws.receive()
        assert msg.type == WSMsgType.CLOSE

    @unittest_run_loop
    async def test_select_best_server_load_balancing(self):
        ws1 = await self.client.ws_connect("/ws")
        await ws1.send_json(
            RegisterMessage(
                server_id="srv-1",
                hostname="host-1",
                cpu_cores=4,
                mem_total_gb=16,
                mem_available_gb=12,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        ws2 = await self.client.ws_connect("/ws")
        await ws2.send_json(
            RegisterMessage(
                server_id="srv-2",
                hostname="host-2",
                cpu_cores=8,
                mem_total_gb=32,
                mem_available_gb=28,
            ).to_dict()
        )
        await asyncio.sleep(0.1)

        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:1"})
        assert resp.status == 201
        server1 = (await resp.json())["server_id"]

        # Consume start_agent on the selected ws
        if server1 == "srv-1":
            await ws1.receive_json()
        else:
            await ws2.receive_json()

        # Second agent should go to the OTHER server
        resp = await self.client.post("/api/v1/agents", json={"group_id": "tg:2"})
        assert resp.status == 201
        server2 = (await resp.json())["server_id"]
        assert server2 != server1

        await ws1.close()
        await ws2.close()
