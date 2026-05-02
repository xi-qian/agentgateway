"""Gateway server -- main entry point.

Starts an aiohttp server with:
- WebSocket endpoint at /ws for Agent Service connections
- Message router (group_id -> agent_ws)
- Response bridge (agent responses -> platform adapters)
- Platform adapters (Telegram, Discord, etc.)
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
from typing import List, Optional

from aiohttp import web, WSMsgType

from gateway.config import GatewayConfig, load_config
from gateway.router import Router
from gateway.bridge import Bridge

logger = logging.getLogger("gateway.server")


ADAPTER_REGISTRY = {
    "mock": "gateway.adapters.mock:MockAdapter",
    "telegram": "gateway.adapters.telegram:TelegramAdapter",
    "feishu": "gateway.adapters.feishu:FeishuAdapter",
    "dingtalk": "gateway.adapters.dingtalk:DingTalkAdapter",
    "wecom": "gateway.adapters.wecom:WeComAdapter",
}


def load_adapters_from_config(config: dict) -> list:
    """Load enabled adapters from config or GW_ADAPTERS_CONFIG env var.

    Supports two sources:
    1. config["adapters"] dict (from YAML): each key is an adapter name
       with an "enabled" flag.
    2. GW_ADAPTERS_CONFIG env var: JSON string like
       '{"feishu": {"app_id": "x", "app_secret": "y"}, "mock": {}}'.
       Adapters listed here are auto-enabled (no "enabled" key needed).

    Env var takes precedence if both are present.
    """
    import json
    import os

    adapters = []

    # Check GW_ADAPTERS_CONFIG env var first
    env_config = os.environ.get("GW_ADAPTERS_CONFIG", "").strip()
    if env_config:
        try:
            adapter_configs = json.loads(env_config)
            if not isinstance(adapter_configs, dict):
                logger.warning("GW_ADAPTERS_CONFIG must be a JSON object")
                adapter_configs = {}
            else:
                # Auto-enable all adapters from env var
                adapter_configs = {
                    name: dict(cfg, enabled=True) if isinstance(cfg, dict) else {"enabled": True}
                    for name, cfg in adapter_configs.items()
                }
        except json.JSONDecodeError as e:
            logger.warning("GW_ADAPTERS_CONFIG is not valid JSON: %s", e)
            adapter_configs = config.get("adapters", {})
    else:
        adapter_configs = config.get("adapters", {})

    for name, cfg in adapter_configs.items():
        if not cfg.get("enabled", False):
            continue
        module_path = ADAPTER_REGISTRY.get(name)
        if not module_path:
            logger.warning("Unknown adapter: %s", name)
            continue
        try:
            module_name, class_name = module_path.rsplit(":", 1)
            module = importlib.import_module(module_name)
            cls = getattr(module, class_name)
            adapter = cls(cfg)
            adapters.append(adapter)
            logger.info("Loaded adapter: %s", name)
        except Exception as e:
            logger.error("Failed to load adapter %s: %s", name, e)
    return adapters


class GatewayServer:
    """aiohttp-based WebSocket gateway server.

    Accepts agent WebSocket connections at ``/ws``. Each agent must send
    a ``HelloMessage`` as its first message. The server then routes tasks
    to agents and bridges responses back to platform adapters.
    """

    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig()
        self.router = Router(self.config)
        self.bridge = Bridge(self.router)
        self._app: Optional[web.Application] = None
        self._adapters: List = []
        if self.config.manager_url:
            from gateway.manager_client import ManagerClient
            self._manager_client = ManagerClient(self.config.manager_url)
            self.router.set_manager_client(self._manager_client)
        else:
            self._manager_client = None

    async def _on_platform_message(self, event) -> None:
        """Handle a message from a platform adapter and dispatch to agent."""
        from gateway.message_types import TaskMessage
        from gateway.adapters.base import AdapterMessageEvent

        if not isinstance(event, AdapterMessageEvent):
            logger.warning("Unexpected event type: %s", type(event))
            return

        task = TaskMessage(
            group_id=event.group_id,
            message=event.text,
            sender={
                "user_id": event.sender_id,
                "user_name": event.sender_name,
                "platform": event.platform,
            },
            media=[],
            reply_to_message_id=event.reply_to_message_id,
            reply_to_text=event.reply_to_text,
        )

        logger.info("Dispatching task from %s to group_id=%s", event.platform, event.group_id)
        await self.router.dispatch_task(event.group_id, task.to_dict())

    def _setup_adapter_handlers(self) -> None:
        """Set up message handlers for all loaded adapters."""
        for adapter in self._adapters:
            adapter.on_message = self._on_platform_message

    def _find_adapter_for_platform(self, platform: str) -> Optional[Any]:
        """Find the adapter that handles the given platform prefix."""
        for adapter in self._adapters:
            adapter_name = adapter.__class__.__name__.lower().replace("adapter", "")
            if adapter_name == platform or adapter_name.startswith(platform):
                return adapter
        # Fallback: check adapter's platform attribute
        for adapter in self._adapters:
            if hasattr(adapter, 'platform') and adapter.platform == platform:
                return adapter
        return None

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Handle an incoming WebSocket connection from an agent."""
        ws = web.WebSocketResponse(
            heartbeat=self.config.heartbeat_interval_seconds
        )
        await ws.prepare(request)
        logger.info("Agent WebSocket connected from %s", request.remote)

        # Expect a hello message within 10 seconds
        try:
            first_msg = await asyncio.wait_for(ws.receive_json(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning(
                "Agent connected but did not send hello within 10s"
            )
            await ws.close(code=4001, message=b"no hello")
            return ws

        from gateway.message_types import parse_agent_message, HelloMessage

        try:
            msg = parse_agent_message(first_msg)
        except Exception as e:
            logger.warning("Invalid hello message: %s", e)
            await ws.close(code=4002, message=b"invalid hello")
            return ws

        if not isinstance(msg, HelloMessage):
            logger.warning("First message was not hello, got: %s", msg)
            await ws.close(code=4003, message=b"expected hello")
            return ws

        # Register the agent
        self.router.register_agent(msg.group_id, ws)
        logger.info(
            "Agent registered: group_id=%s, model=%s, toolsets=%s",
            msg.group_id,
            msg.model,
            msg.toolsets,
        )

        # Register response handler to send replies back to platform
        platform_prefix = msg.group_id.split(":")[0] if ":" in msg.group_id else ""
        adapter = self._find_adapter_for_platform(platform_prefix)
        if adapter:
            async def _response_handler(msg):
                if hasattr(msg, 'final_response'):
                    await adapter.send_response(msg.group_id, msg.final_response)
                elif hasattr(msg, 'token'):
                    await adapter.send_edit(msg.group_id, msg.token)
            self.bridge.register_response_handler(msg.group_id, _response_handler)

        # Drain any pending tasks
        await self.bridge.on_agent_connected(msg.group_id)

        # Auto-restore session from Manager if available
        if self._manager_client:
            try:
                import aiohttp
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=5)) as s:
                    async with s.get(f"{self._manager_client.base_url}/api/v1/sessions/{msg.group_id}") as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            if data.get("history"):
                                from gateway.message_types import SessionRestoreMessage
                                restore = SessionRestoreMessage(
                                    group_id=msg.group_id,
                                    history=data["history"],
                                    session_id=data.get("session_id", ""),
                                )
                                await ws.send_json(restore.to_dict())
                                logger.info("Sent session restore for %s (%d messages)",
                                            msg.group_id, len(data["history"]))
            except Exception as e:
                logger.debug("No session to restore for %s: %s", msg.group_id, e)

        # Message loop
        try:
            async for ws_msg in ws:
                if ws_msg.type == WSMsgType.TEXT:
                    await self._handle_agent_message(
                        ws_msg.json(), msg.group_id
                    )
                elif ws_msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self.router.remove_agent(msg.group_id)
            self.router.remove_pending_queue(msg.group_id)
            self.bridge.cleanup_agent(msg.group_id)
            logger.info("Agent disconnected: group_id=%s", msg.group_id)
            if self._manager_client:
                await self._manager_client.notify_disconnect(msg.group_id)

        return ws

    async def _handle_agent_message(self, data: dict, group_id: str) -> None:
        """Parse and dispatch a single message from an agent."""
        from gateway.message_types import parse_agent_message

        try:
            msg = parse_agent_message(data)
            await self.bridge.handle_agent_message(msg, group_id)
        except Exception as e:
            logger.error("Error handling agent message: %s", e)

    def create_app(self) -> web.Application:
        """Build and return the aiohttp Application."""
        app = web.Application()
        app.add_routes([web.get("/ws", self._ws_handler)])
        app["gateway"] = self
        app["router"] = self.router
        app["bridge"] = self.bridge
        app["adapters"] = self._adapters
        return app

    async def start(self) -> None:
        """Start the gateway server."""
        self._app = self.create_app()
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(
            runner, self.config.ws_host, self.config.ws_port
        )
        await site.start()
        logger.info(
            "Gateway listening on ws://%s:%d/ws",
            self.config.ws_host,
            self.config.ws_port,
        )

    async def stop(self) -> None:
        """Gracefully shut down the gateway server."""
        if self._app:
            await self._app.shutdown()
            logger.info("Gateway stopped")


def main():
    """CLI entry point for the gateway server."""
    parser = argparse.ArgumentParser(
        description="Hermes Distributed Gateway"
    )
    parser.add_argument(
        "--config", "-c", default=None, help="Path to config.yaml"
    )
    parser.add_argument(
        "--host", default=None, help="WebSocket bind host"
    )
    parser.add_argument(
        "--port", type=int, default=None, help="WebSocket bind port"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    config = load_config(args.config)
    if args.host:
        config.ws_host = args.host
    if args.port:
        config.ws_port = args.port

    # Load adapters from config
    import yaml
    config_data = {}
    if args.config:
        from pathlib import Path
        p = Path(args.config)
        if p.exists():
            with open(p) as f:
                config_data = yaml.safe_load(f) or {}
    adapters = load_adapters_from_config(config_data)

    server = GatewayServer(config)
    server._adapters = adapters
    server._setup_adapter_handlers()

    async def _run():
        # Start adapters
        for adapter in adapters:
            try:
                if hasattr(adapter, 'start'):
                    await adapter.start()
                    logger.info("Started adapter: %s", adapter.__class__.__name__)
            except Exception as e:
                logger.error("Failed to start adapter %s: %s", adapter.__class__.__name__, e)

        await server.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            # Stop adapters
            for adapter in adapters:
                try:
                    if hasattr(adapter, 'stop'):
                        await adapter.stop()
                except Exception as e:
                    logger.error("Failed to stop adapter %s: %s", adapter.__class__.__name__, e)
            await server.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
