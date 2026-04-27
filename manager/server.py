"""Manager server with REST API, WebSocket daemon handler, and background scheduler."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from typing import Any, Dict, Optional

from aiohttp import web

from manager.config import ManagerConfig, load_manager_config
from manager.registry import AgentRegistry
from gateway.message_types import (
    parse_daemon_message,
    StartAgentMessage,
    StopAgentMessage,
)

logger = logging.getLogger(__name__)


class ManagerServer:
    """Core Manager service: REST API + WebSocket daemon connections + scheduler."""

    def __init__(self, config: Optional[ManagerConfig] = None) -> None:
        self.config = config or ManagerConfig()
        self.registry = AgentRegistry(db_path=self.config.db_path)
        self._daemon_ws: Dict[str, web.WebSocketResponse] = {}
        self._scheduler_task: Optional[asyncio.Task] = None
        self._app: Optional[web.Application] = None
        self._site: Optional[web.TCPSite] = None

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Initialize the registry (create tables)."""
        await self.registry.init()

    # ------------------------------------------------------------------
    # REST handlers
    # ------------------------------------------------------------------

    async def _list_agents(self, request: web.Request) -> web.Response:
        agents = await self.registry.list_agents()
        return web.json_response({"agents": agents})

    async def _get_agent(self, request: web.Request) -> web.Response:
        group_id = request.match_info["group_id"]
        agent = await self.registry.get_agent(group_id)
        if agent is None:
            return web.json_response({"error": "agent not found"}, status=404)
        return web.json_response(agent)

    async def _create_agent(self, request: web.Request) -> web.Response:
        body = await request.json()
        group_id = body.get("group_id")
        profile = body.get("profile", "default")
        model = body.get("model", "")

        if not group_id:
            return web.json_response({"error": "group_id is required"}, status=400)

        # Check if an agent already exists in starting/running state
        existing = await self.registry.get_agent(group_id)
        if existing is not None and existing["status"] in ("starting", "running"):
            return web.json_response(
                {
                    "status": existing["status"],
                    "server_id": existing["server_id"],
                    "message": "agent already exists",
                },
                status=200,
            )

        # Select the best server
        server_id = await self.registry.select_best_server()
        if server_id is None:
            return web.json_response(
                {"error": "no available servers"},
                status=503,
            )

        # Create agent record in registry
        await self.registry.create_agent(
            group_id=group_id,
            server_id=server_id,
            profile=profile,
            model=model,
        )

        # Send start_agent to the daemon via WebSocket
        ws = self._daemon_ws.get(server_id)
        if ws is not None and not ws.closed:
            msg = StartAgentMessage(
                group_id=group_id,
                profile=profile,
                gateway_url=self.config.gateway_url or "",
                hermes_home="",
                model=model,
            )
            await ws.send_str(json.dumps(msg.to_dict()))
            logger.info("Sent start_agent for %s to server %s", group_id, server_id)
        else:
            logger.warning(
                "No WebSocket connection to server %s, start_agent not sent",
                server_id,
            )

        return web.json_response(
            {"status": "starting", "server_id": server_id},
            status=201,
        )

    async def _delete_agent(self, request: web.Request) -> web.Response:
        group_id = request.match_info["group_id"]
        agent = await self.registry.get_agent(group_id)
        if agent is None:
            return web.json_response({"error": "agent not found"}, status=404)

        server_id = agent["server_id"]

        # Send stop_agent to the daemon
        ws = self._daemon_ws.get(server_id)
        if ws is not None and not ws.closed:
            msg = StopAgentMessage(group_id=group_id, force=False)
            await ws.send_str(json.dumps(msg.to_dict()))
            logger.info("Sent stop_agent for %s to server %s", group_id, server_id)

        await self.registry.delete_agent(group_id)
        return web.json_response({"status": "deleted"})

    async def _disconnect_agent(self, request: web.Request) -> web.Response:
        group_id = request.match_info["group_id"]
        agent = await self.registry.get_agent(group_id)
        if agent is None:
            return web.json_response({"error": "agent not found"}, status=404)

        server_id = agent["server_id"]

        # Send stop_agent to the daemon
        ws = self._daemon_ws.get(server_id)
        if ws is not None and not ws.closed:
            body = await request.json() if request.body_exists else {}
            msg = StopAgentMessage(
                group_id=group_id,
                force=body.get("force", False),
            )
            await ws.send_str(json.dumps(msg.to_dict()))
            logger.info("Sent disconnect for %s to server %s", group_id, server_id)

        return web.json_response({"status": "disconnected"})

    async def _list_servers(self, request: web.Request) -> web.Response:
        servers = await self.registry.list_servers()
        return web.json_response({"servers": servers})

    async def _get_logs(self, request: web.Request) -> web.Response:
        group_id = request.match_info["group_id"]
        tail_str = request.query.get("tail")
        since = request.query.get("since")
        tail: Optional[int] = int(tail_str) if tail_str else None
        logs = await self.registry.get_logs(group_id, tail=tail, since=since)
        return web.json_response({"logs": logs})

    async def _save_session(self, request: web.Request) -> web.Response:
        body = await request.json()
        group_id = body.get("group_id")
        if not group_id:
            return web.json_response({"error": "group_id required"}, status=400)
        history = body.get("history", [])
        session_id = body.get("session_id", "")
        await self.registry.save_session(group_id, history, session_id)
        return web.json_response({"status": "saved"})

    async def _get_session(self, request: web.Request) -> web.Response:
        group_id = request.match_info["group_id"]
        session = await self.registry.load_session(group_id)
        if session is None:
            return web.json_response({"error": "session not found"}, status=404)
        return web.json_response(session)

    # ------------------------------------------------------------------
    # WebSocket daemon handler
    # ------------------------------------------------------------------

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        server_id: Optional[str] = None

        try:
            # Wait for register message within 10 seconds
            msg_text = await asyncio.wait_for(ws.receive_str(), timeout=10.0)
            data = json.loads(msg_text)
            msg = parse_daemon_message(data)

            if data.get("type") != "register":
                await ws.close(code=4003, message="expected register message")
                return ws

            # Register server in registry
            server_id = msg.server_id
            await self.registry.register_server(
                server_id=server_id,
                hostname=msg.hostname,
                cpu_cores=msg.cpu_cores,
                mem_total_gb=msg.mem_total_gb,
                mem_available_gb=msg.mem_available_gb,
            )
            self._daemon_ws[server_id] = ws
            logger.info(
                "Daemon registered: %s (%s) with %d cores, %.1f GB RAM",
                server_id,
                msg.hostname,
                msg.cpu_cores,
                msg.mem_total_gb,
            )

            # Message loop
            async for msg_text in ws:
                if msg_text.type == web.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg_text.data)
                        await self._handle_daemon_message(server_id, data)
                    except Exception:
                        logger.exception(
                            "Error handling daemon message from %s",
                            server_id,
                        )
                elif msg_text.type in (
                    web.WSMsgType.ERROR,
                    web.WSMsgType.CLOSE,
                ):
                    break

        except asyncio.TimeoutError:
            logger.warning("WebSocket connection timed out waiting for register")
            await ws.close(code=4003, message="registration timeout")
        except Exception:
            logger.exception("WebSocket handler error for server %s", server_id)
        finally:
            if server_id is not None:
                self._daemon_ws.pop(server_id, None)
                await self.registry.mark_server_lost(server_id)
                logger.info("Daemon disconnected: %s", server_id)

        return ws

    async def _handle_daemon_message(
        self, server_id: str, data: Dict[str, Any]
    ) -> None:
        """Dispatch an incoming daemon message."""
        msg_type = data.get("type")

        if msg_type == "heartbeat":
            await self.registry.update_heartbeat(
                server_id,
                mem_available_gb=data.get("mem_available_gb", 0.0),
                mem_used_gb=data.get("mem_used_gb", 0.0),
                cpu_percent=data.get("cpu_percent", 0.0),
            )

        elif msg_type == "agent_started":
            group_id = data.get("group_id", "")
            pid = data.get("pid", 0)
            await self.registry.agent_started(group_id, pid=pid)
            logger.info("Agent started: %s (pid=%d) on %s", group_id, pid, server_id)

        elif msg_type == "agent_stopped":
            group_id = data.get("group_id", "")
            exit_code = data.get("exit_code", 0)
            reason = data.get("reason", "")
            await self.registry.agent_stopped(
                group_id, exit_code=exit_code, reason=reason
            )
            logger.info(
                "Agent stopped: %s (exit_code=%d, reason=%s) on %s",
                group_id,
                exit_code,
                reason,
                server_id,
            )

        elif msg_type == "log":
            group_id = data.get("group_id", "")
            pid = data.get("pid", 0)
            stream = data.get("stream", "stdout")
            line = data.get("line", "")
            await self.registry.append_log(
                group_id=group_id,
                pid=pid,
                stream=stream,
                line=line,
            )

        else:
            logger.warning("Unknown daemon message type from %s: %s", server_id, msg_type)

    # ------------------------------------------------------------------
    # Background scheduler
    # ------------------------------------------------------------------

    async def _scheduler_loop(self) -> None:
        """Periodic check for timed-out servers and idle agents."""
        try:
            while True:
                await asyncio.sleep(self.config.scheduler_interval_seconds)

                # Check for timed-out servers
                timedout = await self.registry.get_timedout_servers(
                    timeout_seconds=self.config.heartbeat_timeout_seconds,
                )
                for server in timedout:
                    sid = server["server_id"]
                    await self.registry.mark_server_lost(sid)
                    self._daemon_ws.pop(sid, None)
                    logger.warning("Server timed out and marked lost: %s", sid)

                # Check for idle agents
                idle = await self.registry.get_idle_agents(
                    timeout_minutes=self.config.idle_timeout_minutes,
                )
                for agent in idle:
                    group_id = agent["group_id"]
                    server_id = agent["server_id"]
                    ws = self._daemon_ws.get(server_id)
                    if ws is not None and not ws.closed:
                        msg = StopAgentMessage(group_id=group_id, force=True)
                        await ws.send_str(json.dumps(msg.to_dict()))
                        logger.info(
                            "Stopped idle agent %s on server %s",
                            group_id,
                            server_id,
                        )

                # Clean up old logs
                if self.config.log_retention_days > 0:
                    deleted = await self.registry.delete_old_logs(
                        retention_days=self.config.log_retention_days,
                    )
                    if deleted > 0:
                        logger.info("Deleted %d expired log entries", deleted)

        except asyncio.CancelledError:
            logger.info("Scheduler cancelled")
            raise

    # ------------------------------------------------------------------
    # Application factory and lifecycle
    # ------------------------------------------------------------------

    def create_app(self) -> web.Application:
        """Create the aiohttp Application with all routes wired."""
        app = web.Application()
        app["manager"] = self
        app["registry"] = self.registry

        app.router.add_get("/api/v1/agents", self._list_agents)
        app.router.add_get("/api/v1/agents/{group_id}", self._get_agent)
        app.router.add_post("/api/v1/agents", self._create_agent)
        app.router.add_delete("/api/v1/agents/{group_id}", self._delete_agent)
        app.router.add_post(
            "/api/v1/agents/{group_id}/disconnect", self._disconnect_agent
        )
        app.router.add_get("/api/v1/servers", self._list_servers)
        app.router.add_get("/api/v1/logs/{group_id}", self._get_logs)
        app.router.add_post("/api/v1/sessions", self._save_session)
        app.router.add_get("/api/v1/sessions/{group_id}", self._get_session)

        # WebSocket endpoint for daemon connections
        app.router.add_get(self.config.ws_path, self._ws_handler)

        self._app = app
        return app

    async def start(self) -> None:
        """Initialize registry, create and start the HTTP/WS server."""
        await self.init()
        app = self.create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        self._site = web.TCPSite(runner, self.config.rest_host, self.config.rest_port)
        await self._site.start()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        logger.info(
            "Manager started on %s:%d (ws: %s)",
            self.config.rest_host,
            self.config.rest_port,
            self.config.ws_path,
        )

    async def stop(self) -> None:
        """Shut down the scheduler, HTTP server, and registry."""
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            try:
                await self._scheduler_task
            except asyncio.CancelledError:
                pass
            self._scheduler_task = None

        if self._site is not None:
            await self._site.stop()
            self._site = None

        if self._app is not None:
            await self._app.shutdown()
            await self._app.cleanup()
            self._app = None

        await self.registry.close()
        logger.info("Manager stopped")


# ------------------------------------------------------------------
# Helper factory
# ------------------------------------------------------------------


async def create_manager_app(config: Optional[ManagerConfig] = None) -> web.Application:
    """Create and return an initialized aiohttp Application for testing."""
    server = ManagerServer(config)
    await server.init()
    return server.create_app()


# ------------------------------------------------------------------
# CLI entry point
# ------------------------------------------------------------------


async def _run(config: ManagerConfig) -> None:
    """Run the Manager server until interrupted."""
    server = ManagerServer(config)
    try:
        await server.start()
        # Run forever until cancelled
        while True:
            await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass
    finally:
        await server.stop()


def main() -> None:
    """CLI entry point for the Manager service."""
    parser = argparse.ArgumentParser(description="Hermes Distributed Manager")
    parser.add_argument("--config", "-c", default=None, help="Path to config YAML file")
    parser.add_argument("--host", default=None, help="REST API bind host")
    parser.add_argument("--port", type=int, default=None, help="REST API bind port")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    config = load_manager_config(args.config)
    if args.host is not None:
        config.rest_host = args.host
    if args.port is not None:
        config.rest_port = args.port

    logger.info("Starting Hermes Manager on %s:%d", config.rest_host, config.rest_port)
    asyncio.run(_run(config))


if __name__ == "__main__":
    main()
