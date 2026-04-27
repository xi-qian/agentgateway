"""Tests for gateway.manager_client and cold-path integration."""
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from gateway.server import GatewayServer
from gateway.message_types import HelloMessage


def _make_mock_session(post_result=None, post_side_effect=None):
    """Build a mock aiohttp.ClientSession with proper async context manager support."""
    # Build mock response that works as async context manager
    mock_response = MagicMock()
    mock_response.status = 201
    mock_response.json = AsyncMock(return_value={"status": "ok"})
    if isinstance(post_result, dict):
        mock_response.json = AsyncMock(return_value=post_result)
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    # Build mock session that works as async context manager
    mock_session = MagicMock()
    if post_side_effect:
        mock_session.post = MagicMock(return_value=post_side_effect)
    else:
        mock_session.post = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    return mock_session, mock_response


class TestManagerClient:
    """Unit tests for the REST client that calls Manager."""

    @pytest.mark.asyncio
    async def test_create_agent_calls_post(self):
        from gateway.manager_client import ManagerClient
        client = ManagerClient(base_url="http://localhost:8800")

        mock_session, mock_response = _make_mock_session(
            post_result={"status": "starting", "server_id": "srv-1"},
        )
        mock_response.status = 201

        with patch("gateway.manager_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.create_agent("tg:1", profile="research")
            assert result["status"] == "starting"
            mock_session.post.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_agent_returns_none_on_connection_error(self):
        from gateway.manager_client import ManagerClient
        client = ManagerClient(base_url="http://localhost:8800")

        mock_session = MagicMock()
        mock_session.post = MagicMock(side_effect=ConnectionError("refused"))
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)

        with patch("gateway.manager_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.create_agent("tg:1")
            assert result is None

    @pytest.mark.asyncio
    async def test_notify_disconnect(self):
        from gateway.manager_client import ManagerClient
        client = ManagerClient(base_url="http://localhost:8800")

        mock_session, mock_response = _make_mock_session(
            post_result={"status": "notified"},
        )
        mock_response.status = 200

        with patch("gateway.manager_client.aiohttp.ClientSession", return_value=mock_session):
            result = await client.notify_disconnect("tg:1", reason="closed")
            assert result["status"] == "notified"


class TestColdPathIntegration(AioHTTPTestCase):
    """Test that Gateway calls Manager when no agent is available."""

    async def get_application(self):
        server = GatewayServer()
        return server.create_app()

    @unittest_run_loop
    async def test_dispatch_task_no_agent_no_manager(self):
        task = {"type": "task", "message": "hello"}
        sent = await self.app["router"].dispatch_task("tg:99", task)
        assert sent is False

    @unittest_run_loop
    async def test_cold_path_with_manager_configured(self):
        mock_client = AsyncMock()
        mock_client.create_agent = AsyncMock(return_value={"status": "starting", "server_id": "srv-1"})

        self.app["router"].set_manager_client(mock_client)

        task = {"type": "task", "message": "hello"}
        sent = await self.app["router"].dispatch_task("tg:new", task)
        assert sent is False
        mock_client.create_agent.assert_awaited_once_with("tg:new")
