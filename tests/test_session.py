"""Tests for session persistence and restore."""
import pytest
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop


class TestSessionMessage:
    def test_serialize(self):
        from gateway.message_types import SessionRestoreMessage
        msg = SessionRestoreMessage(
            group_id="tg:1",
            history=[{"role": "user", "content": "hello"}],
            session_id="sess_abc123",
        )
        d = msg.to_dict()
        assert d["type"] == "session_restore"
        assert d["group_id"] == "tg:1"
        assert len(d["history"]) == 1

    def test_round_trip(self):
        from gateway.message_types import SessionRestoreMessage, parse_gateway_message
        msg = SessionRestoreMessage(
            group_id="tg:1",
            history=[
                {"role": "user", "content": "hello"},
                {"role": "assistant", "content": "hi there"},
            ],
        )
        parsed = parse_gateway_message(msg.to_dict())
        assert isinstance(parsed, SessionRestoreMessage)
        assert len(parsed.history) == 2
        assert parsed.history[0]["role"] == "user"


class TestSessionRegistry:
    @pytest.mark.asyncio
    async def test_save_and_load_session(self):
        from manager.registry import AgentRegistry
        registry = AgentRegistry(db_path=":memory:")
        await registry.init()

        history = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
        await registry.save_session("tg:1", history=history, session_id="sess_1")

        session = await registry.load_session("tg:1")
        assert session is not None
        assert session["history"] == history
        assert session["session_id"] == "sess_1"

    @pytest.mark.asyncio
    async def test_load_nonexistent_session(self):
        from manager.registry import AgentRegistry
        registry = AgentRegistry(db_path=":memory:")
        await registry.init()

        session = await registry.load_session("nonexistent")
        assert session is None

    @pytest.mark.asyncio
    async def test_save_session_overwrites(self):
        from manager.registry import AgentRegistry
        registry = AgentRegistry(db_path=":memory:")
        await registry.init()

        await registry.save_session("tg:1", [{"role": "user", "content": "a"}], "s1")
        await registry.save_session("tg:1", [{"role": "user", "content": "b"}], "s2")

        session = await registry.load_session("tg:1")
        assert session["history"][0]["content"] == "b"
        assert session["session_id"] == "s2"


class TestSessionRestAPI(AioHTTPTestCase):
    async def get_application(self):
        from manager.server import create_manager_app
        return await create_manager_app()

    @unittest_run_loop
    async def test_save_session(self):
        resp = await self.client.post(
            "/api/v1/sessions",
            json={
                "group_id": "tg:1",
                "history": [{"role": "user", "content": "hello"}],
                "session_id": "sess_1",
            },
        )
        assert resp.status == 200
        data = await resp.json()
        assert data["status"] == "saved"

    @unittest_run_loop
    async def test_save_session_missing_group_id(self):
        resp = await self.client.post(
            "/api/v1/sessions",
            json={"history": []},
        )
        assert resp.status == 400

    @unittest_run_loop
    async def test_get_session(self):
        # Save first
        await self.client.post(
            "/api/v1/sessions",
            json={
                "group_id": "tg:2",
                "history": [{"role": "user", "content": "hi"}],
                "session_id": "sess_2",
            },
        )
        # Then retrieve
        resp = await self.client.get("/api/v1/sessions/tg:2")
        assert resp.status == 200
        data = await resp.json()
        assert data["group_id"] == "tg:2"
        assert len(data["history"]) == 1
        assert data["session_id"] == "sess_2"

    @unittest_run_loop
    async def test_get_session_not_found(self):
        resp = await self.client.get("/api/v1/sessions/nonexistent")
        assert resp.status == 404
