"""Tests for Manager REST API endpoints."""
import pytest
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop


class TestManagerRestAPI(AioHTTPTestCase):
    async def get_application(self):
        from manager.server import create_manager_app
        return await create_manager_app()

    @unittest_run_loop
    async def test_list_agents_empty(self):
        resp = await self.client.get("/api/v1/agents")
        assert resp.status == 200
        data = await resp.json()
        assert data["agents"] == []

    @unittest_run_loop
    async def test_create_agent_no_servers(self):
        resp = await self.client.post(
            "/api/v1/agents",
            json={"group_id": "tg:1", "profile": "research"},
        )
        assert resp.status == 503
        data = await resp.json()
        assert "error" in data

    @unittest_run_loop
    async def test_list_servers_empty(self):
        resp = await self.client.get("/api/v1/servers")
        assert resp.status == 200
        data = await resp.json()
        assert data["servers"] == []

    @unittest_run_loop
    async def test_get_nonexistent_agent(self):
        resp = await self.client.get("/api/v1/agents/nonexistent")
        assert resp.status == 404

    @unittest_run_loop
    async def test_get_logs_empty(self):
        resp = await self.client.get("/api/v1/logs/tg:1")
        assert resp.status == 200
        data = await resp.json()
        assert data["logs"] == []

    @unittest_run_loop
    async def test_delete_nonexistent_agent(self):
        resp = await self.client.delete("/api/v1/agents/nonexistent")
        assert resp.status == 404

    @unittest_run_loop
    async def test_disconnect_nonexistent_agent(self):
        resp = await self.client.post(
            "/api/v1/agents/nonexistent/disconnect",
            json={"reason": "closed"},
        )
        assert resp.status == 404
