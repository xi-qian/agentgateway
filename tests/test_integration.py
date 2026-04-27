"""Integration tests -- Gateway + Agent Service end-to-end over WebSocket."""
import asyncio
from unittest.mock import AsyncMock

from aiohttp import WSMsgType
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from gateway.message_types import (
    HelloMessage,
    StreamMessage,
    CompleteMessage,
    HeartbeatMessage,
    TaskMessage,
)
from gateway.server import GatewayServer


class TestGatewayAgentIntegration(AioHTTPTestCase):
    """End-to-end tests using real aiohttp server and WebSocket connections."""

    async def get_application(self):
        server = GatewayServer()
        return server.create_app()

    @unittest_run_loop
    async def test_agent_hello_registration(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="telegram:group:1001",
                profile="/home/user/.hermes",
                model="opus-4.6",
                toolsets=["terminal"],
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        assert self.app["router"].has_agent("telegram:group:1001")
        await ws.close()

    @unittest_run_loop
    async def test_no_hello_closes_connection(self):
        ws = await self.client.ws_connect("/ws")
        # Send a valid-but-not-hello message (heartbeat has no required fields)
        await ws.send_json(HeartbeatMessage().to_dict())
        msg = await ws.receive()
        # aiohttp delivers a CLOSE message when the server closes the socket
        assert msg.type == WSMsgType.CLOSE
        assert msg.data == 4003

    @unittest_run_loop
    async def test_agent_sends_stream(self):
        received = []

        async def handler(msg):
            received.append(msg)

        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="tg:1", profile="/p", model="m", toolsets=[]
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        self.app["bridge"].register_response_handler("tg:1", handler)
        await ws.send_json(
            StreamMessage(group_id="tg:1", token="hello").to_dict()
        )
        await asyncio.sleep(0.1)
        assert len(received) == 1
        assert isinstance(received[0], StreamMessage)
        assert received[0].token == "hello"
        await ws.close()

    @unittest_run_loop
    async def test_agent_sends_complete(self):
        received = []

        async def handler(msg):
            received.append(msg)

        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="tg:1", profile="/p", model="m", toolsets=[]
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        self.app["bridge"].register_response_handler("tg:1", handler)
        await ws.send_json(
            CompleteMessage(
                group_id="tg:1",
                final_response="done",
                api_calls=1,
                tokens={"input": 50, "output": 30},
                cost_usd=0.001,
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        assert len(received) == 1
        assert isinstance(received[0], CompleteMessage)
        assert received[0].final_response == "done"
        assert received[0].tokens["input"] == 50
        await ws.close()

    @unittest_run_loop
    async def test_agent_disconnect_removes_from_router(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="tg:1", profile="/p", model="m", toolsets=[]
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        assert self.app["router"].has_agent("tg:1")
        await ws.close()
        await asyncio.sleep(0.1)
        assert not self.app["router"].has_agent("tg:1")

    @unittest_run_loop
    async def test_heartbeat_no_error(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="tg:1", profile="/p", model="m", toolsets=[]
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        handler = AsyncMock()
        self.app["bridge"].register_response_handler("tg:1", handler)
        await ws.send_json(HeartbeatMessage().to_dict())
        await asyncio.sleep(0.1)
        handler.assert_not_awaited()
        await ws.close()

    @unittest_run_loop
    async def test_dispatch_task_to_agent(self):
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(
            HelloMessage(
                group_id="tg:1", profile="/p", model="m", toolsets=[]
            ).to_dict()
        )
        await asyncio.sleep(0.1)
        task = TaskMessage(group_id="tg:1", message="hello world").to_dict()
        sent = await self.app["router"].dispatch_task("tg:1", task)
        assert sent is True
        msg = await ws.receive_json()
        assert msg["type"] == "task"
        assert msg["message"] == "hello world"
        await ws.close()
