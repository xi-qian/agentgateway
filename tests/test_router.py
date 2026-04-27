"""Unit tests for gateway.router.Router."""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock

from gateway.router import Router


@pytest.fixture
def router():
    return Router()


class TestAgentRegistration:
    def test_register_and_retrieve(self, router):
        ws = object()
        router.register_agent("tg:1", ws)
        assert router.has_agent("tg:1")
        assert router.get_agent("tg:1") is ws

    def test_remove(self, router):
        ws = object()
        router.register_agent("tg:1", ws)
        router.remove_agent("tg:1")
        assert not router.has_agent("tg:1")
        assert router.get_agent("tg:1") is None

    def test_overwrite(self, router):
        ws1 = MagicMock()
        ws2 = MagicMock()
        router.register_agent("tg:1", ws1)
        router.register_agent("tg:1", ws2)
        assert router.get_agent("tg:1") is ws2
        ws1.close.assert_called_once()

    def test_overwrite_same_ws_no_close(self, router):
        ws = MagicMock()
        router.register_agent("tg:1", ws)
        router.register_agent("tg:1", ws)
        ws.close.assert_not_called()

    def test_multiple_groups(self, router):
        router.register_agent("tg:1", object())
        router.register_agent("tg:2", object())
        assert router.has_agent("tg:1")
        assert router.has_agent("tg:2")
        assert not router.has_agent("tg:3")


@pytest.mark.asyncio
class TestTaskDispatch:
    async def test_dispatch_success(self, router):
        ws = AsyncMock()
        router.register_agent("tg:1", ws)
        task = {"type": "task", "message": "hello"}
        result = await router.dispatch_task("tg:1", task)
        assert result is True
        ws.send_json.assert_awaited_once_with(task)

    async def test_dispatch_no_agent(self, router):
        task = {"type": "task", "message": "hello"}
        result = await router.dispatch_task("tg:99", task)
        assert result is False

    async def test_dispatch_send_error_removes_agent(self, router):
        ws = AsyncMock()
        ws.send_json.side_effect = RuntimeError("broken")
        router.register_agent("tg:1", ws)
        result = await router.dispatch_task("tg:1", {"type": "task"})
        assert result is False
        assert not router.has_agent("tg:1")


class TestPendingQueue:
    def test_get_queue(self, router):
        q = router.get_pending_queue("tg:1")
        assert q is not None

    def test_same_queue_on_repeated_get(self, router):
        q1 = router.get_pending_queue("tg:1")
        q2 = router.get_pending_queue("tg:1")
        assert q1 is q2

    def test_remove_queue(self, router):
        router.get_pending_queue("tg:1")
        q = router.remove_pending_queue("tg:1")
        assert q is not None
        assert router.get_pending_queue("tg:1") is not q
