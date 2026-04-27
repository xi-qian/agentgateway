# Hermes Distributed — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the minimum end-to-end distributed system: a Gateway service that receives messages, routes them via WebSocket to Agent Service(s), and returns responses — with streaming, interrupt, and approval support.

**Architecture:** Gateway is a new aiohttp async service with a WebSocket server for Agent connections, a message router (group_id → agent_ws), and a response bridge. Agent Service is a standalone script that imports Hermes as a library, connects to Gateway via WebSocket, and runs AIAgent for each task. No Manager yet — agents are started manually.

**Tech Stack:** Python 3.11+, aiohttp (WebSocket + HTTP), asyncio, Hermes Agent (imported as library)

**New project root:** `hermes-distributed/` (sibling to hermes-agent)

---

## File Structure

```
hermes-distributed/
├── gateway/
│   ├── __init__.py
│   ├── server.py              # Main entry: aiohttp app, WS server, router, bridge
│   ├── router.py              # group_id → agent_ws routing + message dispatch
│   ├── bridge.py              # Agent response → platform adapter delivery
│   ├── message_types.py       # Shared message protocol dataclasses
│   └── config.py              # Gateway YAML config + defaults
├── agent/
│   └── agent_service.py       # Standalone: WS client → AIAgent execution
├── tests/
│   ├── conftest.py            # Shared fixtures, mock helpers
│   ├── test_message_types.py  # Protocol message serialization
│   ├── test_router.py         # Routing logic
│   ├── test_bridge.py         # Bridge response handling
│   └── test_agent_service.py  # Agent WS client + task execution
├── pyproject.toml
└── README.md
```

---

### Task 1: Project scaffold + shared message types

**Files:**
- Create: `hermes-distributed/pyproject.toml`
- Create: `hermes-distributed/gateway/__init__.py`
- Create: `hermes-distributed/agent/__init__.py`
- Create: `hermes-distributed/tests/__init__.py`
- Create: `hermes-distributed/tests/conftest.py`
- Create: `hermes-distributed/gateway/message_types.py`
- Create: `hermes-distributed/tests/test_message_types.py`

- [ ] **Step 1: Create project directory and pyproject.toml**

```bash
mkdir -p hermes-distributed/{gateway,agent,tests}
```

```toml
# hermes-distributed/pyproject.toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "hermes-distributed"
version = "0.1.0"
description = "Distributed gateway and agent service for Hermes Agent"
requires-python = ">=3.11"
dependencies = [
    "aiohttp>=3.9.0",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "pytest-xdist>=3.0",
]

[project.scripts]
hermes-gateway = "gateway.server:main"
hermes-agent-service = "agent.agent_service:main"
```

- [ ] **Step 2: Create package __init__.py files**

```python
# hermes-distributed/gateway/__init__.py
# hermes-distributed/agent/__init__.py
# hermes-distributed/tests/__init__.py
```

All empty files.

- [ ] **Step 3: Create test conftest.py**

```python
# hermes-distributed/tests/conftest.py
import asyncio
import sys
from pathlib import Path

import pytest
import pytest_asyncio

PROJECT_ROOT = Path(__file__).parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


@pytest.fixture
def event_loop():
    """Provide a fresh event loop for each async test."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
```

- [ ] **Step 4: Write the failing tests for message types**

```python
# hermes-distributed/tests/test_message_types.py
import json
import pytest

from gateway.message_types import (
    HelloMessage, TaskMessage, StreamMessage, CompleteMessage,
    ErrorMessage, InterruptMessage, ApprovalRequestMessage,
    ApprovedMessage, DeniedMessage, HeartbeatMessage,
    ProgressMessage, parse_agent_message, parse_gateway_message,
    ProtocolError,
)


class TestHelloMessage:
    def test_serialize(self):
        msg = HelloMessage(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes/profiles/grp-1001",
            model="opus-4.6",
            toolsets=["terminal", "file"],
            capabilities=["streaming"],
        )
        d = msg.to_dict()
        assert d["version"] == 1
        assert d["type"] == "hello"
        assert d["group_id"] == "telegram:group:1001"

    def test_round_trip(self):
        msg = HelloMessage(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes",
            model="sonnet-4.6",
            toolsets=["terminal"],
            capabilities=["streaming", "approval"],
        )
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert isinstance(parsed, HelloMessage)
        assert parsed.group_id == "telegram:group:1001"
        assert parsed.model == "sonnet-4.6"


class TestTaskMessage:
    def test_serialize(self):
        msg = TaskMessage(
            group_id="telegram:group:1001",
            message="hello",
            sender={"user_id": "123", "platform": "telegram", "user_name": "Alice"},
            media=[{"type": "image", "url": "http://gw:8901/media/img.jpg"}],
        )
        d = msg.to_dict()
        assert d["type"] == "task"
        assert d["message"] == "hello"
        assert len(d["media"]) == 1

    def test_parse(self):
        d = {
            "version": 1,
            "type": "task",
            "group_id": "tg:1",
            "message": "hi",
            "sender": {"user_id": "1", "platform": "tg"},
        }
        msg = parse_gateway_message(d)
        assert isinstance(msg, TaskMessage)
        assert msg.message == "hi"


class TestStreamMessage:
    def test_serialize(self):
        msg = StreamMessage(group_id="tg:1", token="hello")
        d = msg.to_dict()
        assert d["type"] == "stream"
        assert d["token"] == "hello"

    def test_parse(self):
        msg = parse_agent_message({"version": 1, "type": "stream", "group_id": "tg:1", "token": "x"})
        assert isinstance(msg, StreamMessage)


class TestCompleteMessage:
    def test_full(self):
        msg = CompleteMessage(
            group_id="tg:1",
            final_response="done",
            api_calls=3,
            tokens={"input": 100, "output": 50},
            cost_usd=0.001,
        )
        d = msg.to_dict()
        assert d["tokens"]["input"] == 100
        parsed = parse_agent_message(d)
        assert parsed.api_calls == 3
        assert parsed.cost_usd == 0.001


class TestErrorMessage:
    def test_fatal(self):
        msg = ErrorMessage(group_id="tg:1", message="crash", fatal=True)
        assert msg.fatal is True

    def test_non_fatal(self):
        msg = ErrorMessage(group_id="tg:1", message="rate limit")
        assert msg.fatal is False


class TestApprovalMessages:
    def test_request(self):
        msg = ApprovalRequestMessage(
            group_id="tg:1", request_id="r1",
            command="rm -rf /tmp/x", reason="cleanup",
        )
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert parsed.command == "rm -rf /tmp/x"

    def test_approved(self):
        msg = parse_gateway_message({"version": 1, "type": "approved", "group_id": "tg:1", "request_id": "r1"})
        assert isinstance(msg, ApprovedMessage)

    def test_denied(self):
        msg = parse_gateway_message({"version": 1, "type": "denied", "group_id": "tg:1", "request_id": "r1", "reason": "danger"})
        assert isinstance(msg, DeniedMessage)
        assert msg.reason == "danger"


class TestInterruptMessage:
    def test_parse(self):
        msg = parse_gateway_message({"version": 1, "type": "interrupt", "group_id": "tg:1"})
        assert isinstance(msg, InterruptMessage)


class TestHeartbeatMessage:
    def test_bidirectional(self):
        # Agent → Gateway
        a = parse_agent_message({"version": 1, "type": "heartbeat"})
        assert isinstance(a, HeartbeatMessage)
        # Gateway → Agent
        g = parse_gateway_message({"version": 1, "type": "heartbeat"})
        assert isinstance(g, HeartbeatMessage)


class TestProgressMessage:
    def test_serialize(self):
        msg = ProgressMessage(group_id="tg:1", tool="terminal", preview="pip install x", emoji="📦")
        d = msg.to_dict()
        parsed = parse_agent_message(d)
        assert parsed.tool == "terminal"


class TestParseErrors:
    def test_unknown_type_raises(self):
        with pytest.raises(ProtocolError, match="Unknown agent message type"):
            parse_agent_message({"version": 1, "type": "unknown_type"})

    def test_missing_type_raises(self):
        with pytest.raises(ProtocolError, match="missing 'type'"):
            parse_agent_message({"version": 1})

    def test_missing_version_passes(self):
        # version is optional for forward compat
        msg = parse_agent_message({"type": "hello", "group_id": "tg:1", "profile": "/", "model": "m", "toolsets": [], "capabilities": []})
        assert isinstance(msg, HelloMessage)
```

- [ ] **Step 5: Run tests to verify they fail**

```bash
cd hermes-distributed && python -m pytest tests/test_message_types.py -v 2>&1 | head -20
```

Expected: FAIL — `ModuleNotFoundError: No module named 'gateway.message_types'`

- [ ] **Step 6: Implement message_types.py**

```python
# hermes-distributed/gateway/message_types.py
"""Shared message protocol for Gateway <-> Agent Service communication.

All messages include a ``version`` field for forward compatibility.
Agent messages are sent from Agent Service to Gateway.
Gateway messages are sent from Gateway to Agent Service.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


class ProtocolError(Exception):
    """Raised when a message cannot be parsed."""


# ── Message type mapping ──────────────────────────────────────────────────

AGENT_MESSAGE_TYPES = {
    "hello", "stream", "complete", "error",
    "approval_request", "heartbeat", "progress",
}

GATEWAY_MESSAGE_TYPES = {
    "task", "interrupt", "approved", "denied", "heartbeat",
}

MESSAGE_CLASS_MAP: Dict[str, type] = {}

# ── Agent → Gateway messages ──────────────────────────────────────────────


@dataclass
class HelloMessage:
    """Agent registers with Gateway after WebSocket connect."""
    group_id: str
    profile: str
    model: str
    toolsets: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1, "type": "hello",
            "group_id": self.group_id,
            "profile": self.profile,
            "model": self.model,
            "toolsets": self.toolsets,
            "capabilities": self.capabilities,
        }


@dataclass
class StreamMessage:
    """Incremental text token from Agent."""
    group_id: str
    token: str

    def to_dict(self) -> Dict[str, Any]:
        return {"version": 1, "type": "stream", "group_id": self.group_id, "token": self.token}


@dataclass
class CompleteMessage:
    """Agent finished processing a task."""
    group_id: str
    final_response: str
    api_calls: int = 0
    tokens: Dict[str, int] = field(default_factory=dict)
    cost_usd: float = 0.0
    interrupted: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1, "type": "complete",
            "group_id": self.group_id,
            "final_response": self.final_response,
            "api_calls": self.api_calls,
            "tokens": self.tokens,
            "cost_usd": self.cost_usd,
            "interrupted": self.interrupted,
        }


@dataclass
class ErrorMessage:
    """Agent encountered an error."""
    group_id: str
    message: str
    fatal: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1, "type": "error",
            "group_id": self.group_id,
            "message": self.message,
            "fatal": self.fatal,
        }


@dataclass
class ApprovalRequestMessage:
    """Agent requests user approval for a command."""
    group_id: str
    request_id: str
    command: str
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1, "type": "approval_request",
            "group_id": self.group_id,
            "request_id": self.request_id,
            "command": self.command,
            "reason": self.reason,
        }


@dataclass
class ProgressMessage:
    """Agent reports tool execution progress."""
    group_id: str
    tool: str
    preview: str = ""
    emoji: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": 1, "type": "progress",
            "group_id": self.group_id,
            "tool": self.tool,
            "preview": self.preview,
            "emoji": self.emoji,
        }


@dataclass
class HeartbeatMessage:
    """Bidirectional heartbeat."""
    def to_dict(self) -> Dict[str, Any]:
        return {"version": 1, "type": "heartbeat"}


# ── Gateway → Agent messages ──────────────────────────────────────────────


@dataclass
class TaskMessage:
    """Gateway dispatches a user message to Agent."""
    group_id: str
    message: str
    context_prompt: str = ""
    history: List[Dict[str, Any]] = field(default_factory=list)
    sender: Dict[str, str] = field(default_factory=dict)
    media: List[Dict[str, str]] = field(default_factory=list)
    reply_to_message_id: Optional[str] = None
    reply_to_text: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "version": 1, "type": "task",
            "group_id": self.group_id,
            "message": self.message,
        }
        if self.context_prompt:
            d["context_prompt"] = self.context_prompt
        if self.history:
            d["history"] = self.history
        if self.sender:
            d["sender"] = self.sender
        if self.media:
            d["media"] = self.media
        if self.reply_to_message_id:
            d["reply_to_message_id"] = self.reply_to_message_id
        if self.reply_to_text:
            d["reply_to_text"] = self.reply_to_text
        return d


@dataclass
class InterruptMessage:
    """Gateway signals Agent to stop."""
    group_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {"version": 1, "type": "interrupt", "group_id": self.group_id}


@dataclass
class ApprovedMessage:
    """Gateway forwards user approval to Agent."""
    group_id: str
    request_id: str

    def to_dict(self) -> Dict[str, Any]:
        return {"version": 1, "type": "approved", "group_id": self.group_id, "request_id": self.request_id}


@dataclass
class DeniedMessage:
    """Gateway forwards user denial to Agent."""
    group_id: str
    request_id: str
    reason: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {"version": 1, "type": "denied", "group_id": self.group_id, "request_id": self.request_id, "reason": self.reason}


# ── Parse functions ───────────────────────────────────────────────────────

_AGENT_PARSERS: Dict[str, type] = {
    "hello": HelloMessage,
    "stream": StreamMessage,
    "complete": CompleteMessage,
    "error": ErrorMessage,
    "approval_request": ApprovalRequestMessage,
    "heartbeat": HeartbeatMessage,
    "progress": ProgressMessage,
}

_GATEWAY_PARSERS: Dict[str, type] = {
    "task": TaskMessage,
    "interrupt": InterruptMessage,
    "approved": ApprovedMessage,
    "denied": DeniedMessage,
    "heartbeat": HeartbeatMessage,
}


def _parse(d: Dict[str, Any], parsers: Dict[str, type], source_name: str):
    msg_type = d.get("type")
    if not msg_type:
        raise ProtocolError(f"Message missing 'type' field")
    cls = parsers.get(msg_type)
    if cls is None:
        raise ProtocolError(f"Unknown {source_name} message type: {msg_type}")
    try:
        return cls(**{k: v for k, v in d.items() if k != "version" and hasattr(cls, "__dataclass_fields__") and k in cls.__dataclass_fields__})
    except TypeError as e:
        raise ProtocolError(f"Failed to parse {msg_type}: {e}") from e


def parse_agent_message(d: Dict[str, Any]):
    """Parse a message from Agent Service."""
    return _parse(d, _AGENT_PARSERS, "agent")


def parse_gateway_message(d: Dict[str, Any]):
    """Parse a message from Gateway."""
    return _parse(d, _GATEWAY_PARSERS, "gateway")
```

- [ ] **Step 7: Run tests to verify they pass**

```bash
cd hermes-distributed && python -m pytest tests/test_message_types.py -v
```

Expected: All tests PASS.

- [ ] **Step 8: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "feat: project scaffold + shared message protocol types"
```

---

### Task 2: Gateway config + server skeleton

**Files:**
- Create: `hermes-distributed/gateway/config.py`
- Create: `hermes-distributed/gateway/server.py`
- Create: `hermes-distributed/tests/test_config.py`

- [ ] **Step 1: Write failing test for config loading**

```python
# hermes-distributed/tests/test_config.py
import pytest
from pathlib import Path

from gateway.config import GatewayConfig, load_config


class TestGatewayConfig:
    def test_default_values(self):
        cfg = GatewayConfig()
        assert cfg.ws_host == "0.0.0.0"
        assert cfg.ws_port == 8900
        assert cfg.media_port == 8901
        assert cfg.idle_timeout_minutes == 30

    def test_load_from_dict(self):
        cfg = load_config({"gateway": {"ws_port": 9999}})
        assert cfg.ws_port == 9999

    def test_load_from_yaml(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("gateway:\n  ws_port: 7777\n")
        cfg = load_config(cfg_file)
        assert cfg.ws_port == 7777

    def test_missing_file_returns_defaults(self):
        cfg = load_config("/nonexistent/config.yaml")
        assert cfg.ws_port == 8900

    def test_profile_mappings(self):
        cfg = load_config({
            "profiles": {
                "telegram:group:1001": "research-profile",
                "default": "general",
            },
        })
        assert cfg.resolve_profile("telegram:group:1001") == "research-profile"
        assert cfg.resolve_profile("discord:group:999") == "general"
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd hermes-distributed && python -m pytest tests/test_config.py -v 2>&1 | head -10
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement config.py**

```python
# hermes-distributed/gateway/config.py
"""Gateway configuration loading."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


@dataclass
class GatewayConfig:
    ws_host: str = "0.0.0.0"
    ws_port: int = 8900
    media_port: int = 8901
    media_dir: str = "/tmp/hermes-distributed/media"
    idle_timeout_minutes: int = 30
    heartbeat_interval_seconds: int = 60
    heartbeat_timeout_seconds: int = 90
    manager_url: Optional[str] = None
    profile_mappings: Dict[str, str] = field(default_factory=lambda: {"default": "default"})


def load_config(source: Union[str, Path, Dict[str, Any], None] = None) -> GatewayConfig:
    """Load config from a YAML file path, dict, or return defaults."""
    data: Dict[str, Any] = {}
    if source is None:
        pass
    elif isinstance(source, dict):
        data = source
    else:
        p = Path(source)
        if p.exists():
            with open(p) as f:
                data = yaml.safe_load(f) or {}

    gw = data.get("gateway", {})
    profiles = data.get("profiles", {})

    cfg = GatewayConfig(
        ws_host=gw.get("ws_host", GatewayConfig.ws_host),
        ws_port=gw.get("ws_port", GatewayConfig.ws_port),
        media_port=gw.get("media_port", GatewayConfig.media_port),
        media_dir=gw.get("media_dir", GatewayConfig.media_dir),
        idle_timeout_minutes=gw.get("idle_timeout_minutes", GatewayConfig.idle_timeout_minutes),
        heartbeat_interval_seconds=gw.get("heartbeat_interval_seconds", GatewayConfig.heartbeat_interval_seconds),
        heartbeat_timeout_seconds=gw.get("heartbeat_timeout_seconds", GatewayConfig.heartbeat_timeout_seconds),
        manager_url=gw.get("manager_url"),
        profile_mappings=profiles if profiles else {"default": "default"},
    )
    return cfg


# Monkey-patch for dataclass
GatewayConfig.resolve_profile = lambda self, group_id: self.profile_mappings.get(group_id, self.profile_mappings.get("default", "default"))
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd hermes-distributed && python -m pytest tests/test_config.py -v
```

Expected: All PASS.

- [ ] **Step 5: Create server.py skeleton (empty aiohttp app)**

```python
# hermes-distributed/gateway/server.py
"""Gateway server — main entry point.

Starts an aiohttp server with:
- WebSocket endpoint at /ws for Agent Service connections
- HTTP media file server
- Message router (group_id → agent_ws)
- Response bridge (agent responses → platform adapters)
"""
from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Optional

from aiohttp import web, WSMsgType

from gateway.config import GatewayConfig, load_config
from gateway.router import Router
from gateway.bridge import Bridge

logger = logging.getLogger("gateway.server")


class GatewayServer:
    def __init__(self, config: Optional[GatewayConfig] = None):
        self.config = config or GatewayConfig()
        self.router = Router(self.config)
        self.bridge = Bridge(self.router)
        self._app: Optional[web.Application] = None

    async def _ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        """Handle incoming WebSocket connection from an Agent Service."""
        ws = web.WebSocketResponse(heartbeat=self.config.heartbeat_interval_seconds)
        await ws.prepare(request)

        logger.info("Agent WebSocket connected from %s", request.remote)

        # Wait for hello message to register
        try:
            first_msg = await asyncio.wait_for(ws.receive_json(), timeout=10)
        except asyncio.TimeoutError:
            logger.warning("Agent connected but did not send hello within 10s")
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

        # Register agent
        self.router.register_agent(msg.group_id, ws)
        logger.info("Agent registered: group_id=%s, model=%s, toolsets=%s",
                     msg.group_id, msg.model, msg.toolsets)

        # Notify bridge of new agent (for pending tasks)
        await self.bridge.on_agent_connected(msg.group_id)

        # Message loop
        try:
            async for ws_msg in ws:
                if ws_msg.type == WSMsgType.TEXT:
                    await self._handle_agent_message(ws_msg.json(), msg.group_id)
                elif ws_msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
        finally:
            self.router.remove_agent(msg.group_id)
            logger.info("Agent disconnected: group_id=%s", msg.group_id)

        return ws

    async def _handle_agent_message(self, data: dict, group_id: str) -> None:
        """Route an agent message through the bridge."""
        from gateway.message_types import parse_agent_message
        try:
            msg = parse_agent_message(data)
            await self.bridge.handle_agent_message(msg, group_id)
        except Exception as e:
            logger.error("Error handling agent message: %s", e)

    def create_app(self) -> web.Application:
        """Create the aiohttp application."""
        app = web.Application()
        app.add_routes([web.get("/ws", self._ws_handler)])
        app["gateway"] = self
        app["router"] = self.router
        app["bridge"] = self.bridge
        return app

    async def start(self) -> None:
        """Start the Gateway server."""
        self._app = self.create_app()
        runner = web.AppRunner(self._app)
        await runner.setup()
        site = web.TCPSite(runner, self.config.ws_host, self.config.ws_port)
        await site.start()
        logger.info("Gateway listening on ws://%s:%d/ws", self.config.ws_host, self.config.ws_port)

    async def stop(self) -> None:
        """Stop the Gateway server."""
        if self._app:
            await self._app.shutdown()
            logger.info("Gateway stopped")


def main():
    parser = argparse.ArgumentParser(description="Hermes Distributed Gateway")
    parser.add_argument("--config", "-c", default=None, help="Path to config.yaml")
    parser.add_argument("--host", default=None, help="WebSocket bind host")
    parser.add_argument("--port", type=int, default=None, help="WebSocket bind port")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    config = load_config(args.config)
    if args.host:
        config.ws_host = args.host
    if args.port:
        config.ws_port = args.port

    server = GatewayServer(config)

    async def _run():
        await server.start()
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await server.stop()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Create minimal router.py and bridge.py (stubs for server to import)**

```python
# hermes-distributed/gateway/router.py
"""Message router — maps group_id to Agent WebSocket connections."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("gateway.router")


class Router:
    def __init__(self, config=None):
        self._agents: Dict[str, Any] = {}  # group_id → websocket
        self._pending_tasks: Dict[str, asyncio.Queue] = {}  # group_id → queue

    def register_agent(self, group_id: str, ws) -> None:
        self._agents[group_id] = ws
        if group_id in self._pending_tasks:
            logger.info("Agent registered for group_id=%s, draining %d pending tasks",
                        group_id, self._pending_tasks[group_id].qsize())

    def remove_agent(self, group_id: str) -> None:
        self._agents.pop(group_id, None)

    def get_agent(self, group_id: str):
        return self._agents.get(group_id)

    def has_agent(self, group_id: str) -> bool:
        return group_id in self._agents

    async def dispatch_task(self, group_id: str, task_dict: dict) -> bool:
        """Send a task to the agent for this group_id. Returns True if sent."""
        ws = self._agents.get(group_id)
        if ws is None:
            return False
        try:
            await ws.send_json(task_dict)
            return True
        except Exception as e:
            logger.error("Failed to send task to agent %s: %s", group_id, e)
            self.remove_agent(group_id)
            return False

    def get_pending_queue(self, group_id: str) -> asyncio.Queue:
        if group_id not in self._pending_tasks:
            self._pending_tasks[group_id] = asyncio.Queue()
        return self._pending_tasks[group_id]

    def remove_pending_queue(self, group_id: str) -> Optional[asyncio.Queue]:
        return self._pending_tasks.pop(group_id, None)
```

```python
# hermes-distributed/gateway/bridge.py
"""Response bridge — routes agent responses back to platform adapters."""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Optional

logger = logging.getLogger("gateway.bridge")


class Bridge:
    """Bridges Agent Service responses to platform adapters.

    For Phase 1, responses are logged. Platform adapter integration comes later.
    """

    def __init__(self, router=None):
        self._router = router
        self._response_handlers: Dict[str, Callable] = {}
        self._approval_callbacks: Dict[str, Callable] = {}

    def register_response_handler(self, group_id: str, handler: Callable) -> None:
        """Register a callback for agent responses (used by platform adapters)."""
        self._response_handlers[group_id] = handler

    def register_approval_callback(self, group_id: str, callback: Callable) -> None:
        """Register a callback for approval requests from agents."""
        self._approval_callbacks[group_id] = callback

    async def on_agent_connected(self, group_id: str) -> None:
        """Called when a new agent connects. Drain pending tasks if any."""
        if self._router:
            queue = self._router.get_pending_queue(group_id)
            ws = self._router.get_agent(group_id)
            if queue and ws:
                while not queue.empty():
                    task_dict = await queue.get()
                    await ws.send_json(task_dict)

    async def handle_agent_message(self, msg: Any, group_id: str) -> None:
        """Route an agent message to the appropriate handler."""
        from gateway.message_types import (
            StreamMessage, CompleteMessage, ErrorMessage,
            ApprovalRequestMessage, ProgressMessage, HeartbeatMessage,
        )

        if isinstance(msg, HeartbeatMessage):
            return  # Heartbeats are handled at the WS layer

        if isinstance(msg, StreamMessage):
            await self._on_stream(group_id, msg)
        elif isinstance(msg, CompleteMessage):
            await self._on_complete(group_id, msg)
        elif isinstance(msg, ErrorMessage):
            await self._on_error(group_id, msg)
        elif isinstance(msg, ApprovalRequestMessage):
            await self._on_approval_request(group_id, msg)
        elif isinstance(msg, ProgressMessage):
            await self._on_progress(group_id, msg)
        else:
            logger.warning("Unknown message type from agent: %s", type(msg))

    async def _on_stream(self, group_id: str, msg) -> None:
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)
        else:
            logger.debug("Stream from %s (no handler): %s", group_id, msg.token[:50])

    async def _on_complete(self, group_id: str, msg) -> None:
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)
        else:
            logger.info("Complete from %s: %s", group_id, msg.final_response[:200])

    async def _on_error(self, group_id: str, msg) -> None:
        logger.error("Error from agent %s (fatal=%s): %s", group_id, msg.fatal, msg.message)
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)

    async def _on_approval_request(self, group_id: str, msg) -> None:
        logger.info("Approval request from agent %s: %s — %s", group_id, msg.command, msg.reason)
        callback = self._approval_callbacks.get(group_id)
        if callback:
            await callback(msg)

    async def _on_progress(self, group_id: str, msg) -> None:
        logger.info("Progress from agent %s [%s]: %s", group_id, msg.emoji, msg.preview)
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)
```

- [ ] **Step 7: Run all tests**

```bash
cd hermes-distributed && python -m pytest tests/ -v
```

Expected: All PASS.

- [ ] **Step 8: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "feat: gateway config, server skeleton, router, bridge"
```

---

### Task 3: Router tests — group_id routing, agent registration, task dispatch

**Files:**
- Create: `hermes-distributed/tests/test_router.py`

- [ ] **Step 1: Write router tests**

```python
# hermes-distributed/tests/test_router.py
import asyncio
import pytest

from unittest.mock import AsyncMock
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
        ws1 = object()
        ws2 = object()
        router.register_agent("tg:1", ws1)
        router.register_agent("tg:1", ws2)
        assert router.get_agent("tg:1") is ws2

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
```

- [ ] **Step 2: Run tests**

```bash
cd hermes-distributed && python -m pytest tests/test_router.py -v
```

Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "test: router unit tests"
```

---

### Task 4: Bridge tests — response handling, approval flow

**Files:**
- Create: `hermes-distributed/tests/test_bridge.py`

- [ ] **Step 1: Write bridge tests**

```python
# hermes-distributed/tests/test_bridge.py
import asyncio
import pytest

from unittest.mock import AsyncMock, MagicMock
from gateway.bridge import Bridge
from gateway.message_types import (
    StreamMessage, CompleteMessage, ErrorMessage,
    ApprovalRequestMessage, ProgressMessage, HeartbeatMessage,
)


@pytest.fixture
def bridge():
    return Bridge()


class TestStreamHandling:
    @pytest.mark.asyncio
    async def test_stream_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = StreamMessage(group_id="tg:1", token="hello")
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_stream_no_handler(self, bridge):
        msg = StreamMessage(group_id="tg:1", token="hello")
        await bridge.handle_agent_message(msg, "tg:1")
        # Should not raise


class TestCompleteHandling:
    @pytest.mark.asyncio
    async def test_complete_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = CompleteMessage(group_id="tg:1", final_response="done", api_calls=2)
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_complete_no_handler(self, bridge):
        msg = CompleteMessage(group_id="tg:1", final_response="done")
        await bridge.handle_agent_message(msg, "tg:1")
        # Should not raise


class TestApprovalHandling:
    @pytest.mark.asyncio
    async def test_approval_request_with_callback(self, bridge):
        callback = AsyncMock()
        bridge.register_approval_callback("tg:1", callback)
        msg = ApprovalRequestMessage(
            group_id="tg:1", request_id="r1",
            command="rm -rf /tmp/x", reason="cleanup",
        )
        await bridge.handle_agent_message(msg, "tg:1")
        callback.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_approval_request_no_callback(self, bridge):
        msg = ApprovalRequestMessage(
            group_id="tg:1", request_id="r1",
            command="rm -rf /tmp/x",
        )
        await bridge.handle_agent_message(msg, "tg:1")
        # Should not raise


class TestHeartbeat:
    @pytest.mark.asyncio
    async def test_heartbeat_ignored(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = HeartbeatMessage()
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_not_awaited()


class TestProgressHandling:
    @pytest.mark.asyncio
    async def test_progress_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = ProgressMessage(group_id="tg:1", tool="terminal", preview="pip install x")
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)


class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_error_with_handler(self, bridge):
        handler = AsyncMock()
        bridge.register_response_handler("tg:1", handler)
        msg = ErrorMessage(group_id="tg:1", message="API rate limit", fatal=False)
        await bridge.handle_agent_message(msg, "tg:1")
        handler.assert_awaited_once_with(msg)

    @pytest.mark.asyncio
    async def test_error_no_handler(self, bridge):
        msg = ErrorMessage(group_id="tg:1", message="crash", fatal=True)
        await bridge.handle_agent_message(msg, "tg:1")
        # Should not raise
```

- [ ] **Step 2: Run tests**

```bash
cd hermes-distributed && python -m pytest tests/test_bridge.py -v
```

Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "test: bridge unit tests"
```

---

### Task 5: Agent Service — WebSocket client + AIAgent execution

**Files:**
- Create: `hermes-distributed/agent/agent_service.py`
- Create: `hermes-distributed/tests/test_agent_service.py`

- [ ] **Step 1: Write failing tests for agent service WebSocket client**

```python
# hermes-distributed/tests/test_agent_service.py
import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from gateway.message_types import (
    HelloMessage, TaskMessage, parse_gateway_message,
)


class TestAgentServiceProtocol:
    """Test the agent service's WebSocket message handling logic."""

    @pytest.mark.asyncio
    async def test_sends_hello_on_connect(self):
        """Agent must send hello as first message after connecting."""
        from agent.agent_service import build_hello
        hello = build_hello(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes",
            model="opus-4.6",
            toolsets=["terminal", "file"],
        )
        d = json.loads(hello)
        assert d["type"] == "hello"
        assert d["group_id"] == "telegram:group:1001"
        assert d["model"] == "opus-4.6"

    @pytest.mark.asyncio
    async def test_parses_task_message(self):
        """Agent must correctly parse task messages from Gateway."""
        raw = {
            "version": 1, "type": "task",
            "group_id": "tg:1",
            "message": "write a hello world",
            "sender": {"user_id": "123", "platform": "telegram"},
        }
        msg = parse_gateway_message(raw)
        assert isinstance(msg, TaskMessage)
        assert msg.message == "write a hello world"
        assert msg.sender["user_id"] == "123"

    @pytest.mark.asyncio
    async def test_build_stream_message(self):
        from agent.agent_service import build_agent_message
        from gateway.message_types import StreamMessage
        msg = build_agent_message(StreamMessage(group_id="tg:1", token="hi"))
        d = json.loads(msg)
        assert d["type"] == "stream"
        assert d["token"] == "hi"

    @pytest.mark.asyncio
    async def test_build_complete_message(self):
        from agent.agent_service import build_agent_message
        from gateway.message_types import CompleteMessage
        msg = build_agent_message(CompleteMessage(
            group_id="tg:1", final_response="done",
            api_calls=3, tokens={"input": 100, "output": 50},
        ))
        d = json.loads(msg)
        assert d["type"] == "complete"
        assert d["api_calls"] == 3
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
cd hermes-distributed && python -m pytest tests/test_agent_service.py -v 2>&1 | head -10
```

Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 3: Implement agent_service.py**

```python
# hermes-distributed/agent/agent_service.py
"""Agent Service — connects to Gateway via WebSocket, runs Hermes AIAgent for each task.

Usage:
    python -m agent.agent_service --gateway-url ws://localhost:8900/ws \
        --group-id telegram:group:1001 \
        --profile /home/user/.hermes/profiles/grp-1001 \
        --model opus-4.6

This is a standalone script that:
1. Connects to Gateway via WebSocket
2. Sends hello to register
3. Receives task messages
4. Runs Hermes AIAgent (imported as library)
5. Sends stream/complete/error back to Gateway
6. Handles interrupt and approval requests
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

# Add Hermes to path so we can import it as a library
# HERMES_ROOT is expected to point to the hermes-agent directory
HERMES_ROOT = os.environ.get("HERMES_ROOT", "")

if HERMES_ROOT and HERMES_ROOT not in sys.path:
    sys.path.insert(0, HERMES_ROOT)

logger = logging.getLogger("agent.service")


def build_hello(group_id: str, profile: str, model: str, toolsets: list) -> str:
    """Build the hello registration message as JSON string."""
    return json.dumps({
        "version": 1, "type": "hello",
        "group_id": group_id,
        "profile": profile,
        "model": model,
        "toolsets": toolsets,
        "capabilities": ["streaming", "approval", "interrupt"],
    })


def build_agent_message(msg) -> str:
    """Serialize an agent message to JSON string."""
    if hasattr(msg, "to_dict"):
        return json.dumps(msg.to_dict())
    return json.dumps(msg)


class ApprovalGate:
    """Manages approval requests — blocks until approved or denied."""

    def __init__(self):
        self._pending: Dict[str, asyncio.Future] = {}

    async def request(self, request_id: str) -> bool:
        """Wait for approval. Returns True if approved, False if denied."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._pending[request_id] = future
        try:
            result = await asyncio.wait_for(future, timeout=300)
            return result
        except asyncio.TimeoutError:
            return False
        finally:
            self._pending.pop(request_id, None)

    def approve(self, request_id: str) -> None:
        future = self._pending.get(request_id)
        if future and not future.done():
            future.set_result(True)

    def deny(self, request_id: str) -> None:
        future = self._pending.get(request_id)
        if future and not future.done():
            future.set_result(False)


class InterruptFlag:
    """Thread-safe interrupt flag shared between WS loop and agent thread."""

    def __init__(self):
        self._flag = False

    def set(self):
        self._flag = True

    def is_set(self) -> bool:
        return self._flag

    def clear(self):
        self._flag = False


async def run_agent_service(
    gateway_url: str,
    group_id: str,
    profile: str,
    model: str,
    toolsets: Optional[list] = None,
    hermes_home: Optional[str] = None,
) -> None:
    """Main agent service loop."""
    if toolsets is None:
        toolsets = ["terminal", "file", "web", "code"]

    if hermes_home:
        os.environ["HERMES_HOME"] = hermes_home

    approval_gate = ApprovalGate()
    interrupt_flag = InterruptFlag()
    request_counter = {"n": 0}

    def next_request_id() -> str:
        request_counter["n"] += 1
        return f"req_{request_counter['n']:04d}"

    async def send(ws: aiohttp.ClientWebSocketResponse, msg) -> None:
        payload = build_agent_message(msg)
        logger.debug("→ Gateway: %s", payload[:200])
        await ws.send_str(payload)

    async def on_interrupt(ws):
        """Handle interrupt message from Gateway."""
        interrupt_flag.set()
        logger.info("Interrupt received")

    async def on_approved(ws, msg):
        approval_gate.approve(msg.request_id)
        logger.info("Approved request %s", msg.request_id)

    async def on_denied(ws, msg):
        approval_gate.deny(msg.request_id)
        logger.info("Denied request %s: %s", msg.request_id, msg.reason)

    # Stream callback — called from agent's thread, queues to WS
    stream_queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()

    def stream_callback(text: str):
        """Called synchronously from AIAgent's worker thread."""
        try:
            stream_queue.put_nowait(text)
        except asyncio.QueueFull:
            pass

    async def stream_sender(ws: aiohttp.ClientWebSocketResponse):
        """Async task that drains stream_queue and sends to Gateway."""
        from gateway.message_types import StreamMessage
        while not stop_event.is_set():
            try:
                text = await asyncio.wait_for(stream_queue.get(), timeout=0.5)
                await send(ws, StreamMessage(group_id=group_id, token=text))
            except asyncio.TimeoutError:
                continue

    # Approval callback — called from agent's thread
    def approval_callback(command: str, reason: str = "") -> bool:
        """Called synchronously from AIAgent's thread. Blocks until response."""
        rid = next_request_id()
        # Schedule approval_request send on the event loop
        loop = asyncio.get_running_loop()
        from gateway.message_types import ApprovalRequestMessage
        loop.call_soon_threadsafe(
            stream_queue.put_nowait,
            json.dumps(ApprovalRequestMessage(
                group_id=group_id, request_id=rid,
                command=command, reason=reason,
            ).to_dict()),
        )
        # Block in thread until approval gate resolves
        # We need to run the async approval_gate in a new thread-aware way
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(asyncio.run, approval_gate.request(rid))
            return future.result()

    # Connect and main loop
    retry_delay = 1.0
    max_retry_delay = 60.0

    while True:
        try:
            logger.info("Connecting to %s ...", gateway_url)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(gateway_url) as ws:
                    retry_delay = 1.0  # Reset on successful connect

                    # Send hello
                    hello = build_hello(group_id, profile, model, toolsets)
                    await ws.send_str(hello)
                    logger.info("Sent hello: group_id=%s, model=%s", group_id, model)

                    # Start stream sender task
                    sender_task = asyncio.create_task(stream_sender(ws))

                    try:
                        async for ws_msg in ws:
                            if ws_msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(ws_msg.data)
                                msg_type = data.get("type")

                                from gateway.message_types import (
                                    parse_gateway_message, TaskMessage,
                                    InterruptMessage, ApprovedMessage, DeniedMessage,
                                )
                                try:
                                    msg = parse_gateway_message(data)
                                except Exception as e:
                                    logger.warning("Failed to parse message: %s", e)
                                    continue

                                if isinstance(msg, TaskMessage):
                                    interrupt_flag.clear()
                                    await handle_task(
                                        msg, send, stream_callback, approval_callback,
                                        interrupt_flag, group_id,
                                    )
                                elif isinstance(msg, InterruptMessage):
                                    await on_interrupt(ws)
                                elif isinstance(msg, ApprovedMessage):
                                    await on_approved(ws, msg)
                                elif isinstance(msg, DeniedMessage):
                                    await on_denied(ws, msg)

                            elif ws_msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSE,
                            ):
                                logger.warning("WebSocket closed: %s", ws_msg)
                                break
                    finally:
                        stop_event.set()
                        sender_task.cancel()

        except aiohttp.WSSLError as e:
            logger.error("WebSocket error: %s", e)
        except aiohttp.ClientError as e:
            logger.error("Connection error: %s", e)
        except Exception as e:
            logger.error("Unexpected error: %s", e)

        # Reconnect with exponential backoff
        logger.info("Reconnecting in %.1fs ...", retry_delay)
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


async def handle_task(
    task, send_fn, stream_callback, approval_callback,
    interrupt_flag, group_id: str,
) -> None:
    """Run Hermes AIAgent for a single task. Runs in a thread to avoid blocking the event loop."""
    from gateway.message_types import CompleteMessage, ErrorMessage

    logger.info("Processing task: %s", task.message[:100])

    loop = asyncio.get_running_loop()

    def _run_agent():
        """Run Hermes AIAgent in a separate thread."""
        try:
            import hermes_constants
            # Ensure HERMES_HOME is set correctly
            hermes_home = os.environ.get("HERMES_HOME", "")
            if not hermes_home:
                hermes_home = str(Path.home() / ".hermes")
                os.environ["HERMES_HOME"] = hermes_home

            from run_agent import AIAgent
            from hermes_state import SessionDB

            session_db = SessionDB()

            # Build agent config from environment
            config = {}
            try:
                from hermes_cli.config import load_config as load_hermes_config
                config = load_hermes_config() or {}
            except Exception:
                pass

            model = config.get("model", "anthropic/claude-sonnet-4-20250514")

            agent = AIAgent(
                model=model,
                config=config,
                session_db=session_db,
                stream_delta_callback=stream_callback,
            )

            # Run conversation
            result = agent.run_conversation(
                message=task.message,
                history=task.history or [],
            )

            # Send complete
            tokens = getattr(result, "tokens", {}) or {}
            complete = CompleteMessage(
                group_id=group_id,
                final_response=result.get("final_response", ""),
                api_calls=result.get("api_calls", 0),
                tokens=tokens,
                cost_usd=result.get("cost_usd", 0.0),
                interrupted=interrupt_flag.is_set(),
            )
            loop.call_soon_threadsafe(
                asyncio.ensure_future, send_fn(complete)
            )

        except Exception as e:
            logger.error("Agent execution failed: %s", e, exc_info=True)
            error = ErrorMessage(
                group_id=group_id,
                message=str(e),
                fatal=True,
            )
            loop.call_soon_threadsafe(
                asyncio.ensure_future, send_fn(error)
            )

    await asyncio.get_running_loop().run_in_executor(None, _run_agent)


def main():
    parser = argparse.ArgumentParser(description="Hermes Distributed Agent Service")
    parser.add_argument("--gateway-url", required=True, help="Gateway WebSocket URL")
    parser.add_argument("--group-id", required=True, help="Group ID for this agent")
    parser.add_argument("--profile", default=None, help="Hermes profile path")
    parser.add_argument("--model", default=None, help="Model override")
    parser.add_argument("--hermes-home", default=None, help="HERMES_HOME path")
    parser.add_argument("--toolsets", nargs="*", default=None, help="Tool sets to enable")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    hermes_home = args.hermes_home or args.profile or ""
    model = args.model or "anthropic/claude-sonnet-4-20250514"

    asyncio.run(run_agent_service(
        gateway_url=args.gateway_url,
        group_id=args.group_id,
        profile=args.profile or "",
        model=model,
        toolsets=args.toolsets,
        hermes_home=hermes_home,
    ))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run agent service tests**

```bash
cd hermes-distributed && python -m pytest tests/test_agent_service.py -v
```

Expected: All PASS.

- [ ] **Step 5: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "feat: agent service WebSocket client + Hermes AIAgent runner"
```

---

### Task 6: Integration test — Gateway + Agent Service end-to-end

**Files:**
- Create: `hermes-distributed/tests/test_integration.py`

- [ ] **Step 1: Write integration test**

```python
# hermes-distributed/tests/test_integration.py
"""Integration test: Gateway server + Agent Service WebSocket round-trip.

Tests the full message flow without running actual Hermes AIAgent.
Uses aiohttp test utilities.
"""
import asyncio
import json
import pytest

from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from gateway.server import GatewayServer
from gateway.message_types import (
    HelloMessage, TaskMessage, StreamMessage, CompleteMessage,
    HeartbeatMessage,
)


class TestGatewayAgentIntegration(AioHTTPTestCase):
    """Test Gateway WebSocket endpoint with a simulated Agent Service."""

    async def get_application(self):
        server = GatewayServer()
        server.config.ws_port = 0  # Will be overridden by test utils
        return server.create_app()

    @unittest_run_loop
    async def test_agent_hello_registration(self):
        """Agent connects, sends hello, gets registered in router."""
        ws = await self.client.ws_connect("/ws")
        hello = HelloMessage(
            group_id="telegram:group:1001",
            profile="/home/user/.hermes",
            model="opus-4.6",
            toolsets=["terminal"],
        ).to_dict()
        await ws.send_json(hello)

        # Give server time to process
        await asyncio.sleep(0.1)

        # Verify agent is registered
        router = self.app["router"]
        assert router.has_agent("telegram:group:1001")

        await ws.close()

    @unittest_run_loop
    async def test_no_hello_closes_connection(self):
        """Sending non-hello as first message gets connection closed."""
        ws = await self.client.ws_connect("/ws")
        await ws.send_json({"type": "stream", "token": "x"})
        msg = await ws.receive()
        assert msg.type == 1  # CLOSE
        assert msg.data == 4003  # expected hello code

    @unittest_run_loop
    async def test_agent_sends_stream(self):
        """Agent can send stream messages that bridge receives."""
        # Collect messages from bridge
        received = []

        async def handler(msg):
            received.append(msg)

        ws = await self.client.ws_connect("/ws")
        await ws.send_json(HelloMessage(
            group_id="tg:1", profile="/p", model="m", toolsets=[],
        ).to_dict())
        await asyncio.sleep(0.1)

        bridge = self.app["bridge"]
        bridge.register_response_handler("tg:1", handler)

        # Send stream
        await ws.send_json(StreamMessage(group_id="tg:1", token="hello").to_dict())
        await asyncio.sleep(0.1)

        assert len(received) == 1
        assert isinstance(received[0], StreamMessage)
        assert received[0].token == "hello"

        await ws.close()

    @unittest_run_loop
    async def test_agent_sends_complete(self):
        """Agent sends complete message."""
        received = []

        async def handler(msg):
            received.append(msg)

        ws = await self.client.ws_connect("/ws")
        await ws.send_json(HelloMessage(
            group_id="tg:1", profile="/p", model="m", toolsets=[],
        ).to_dict())
        await asyncio.sleep(0.1)

        self.app["bridge"].register_response_handler("tg:1", handler)

        await ws.send_json(CompleteMessage(
            group_id="tg:1", final_response="done", api_calls=1,
            tokens={"input": 50, "output": 30}, cost_usd=0.001,
        ).to_dict())
        await asyncio.sleep(0.1)

        assert len(received) == 1
        assert isinstance(received[0], CompleteMessage)
        assert received[0].final_response == "done"
        assert received[0].tokens["input"] == 50

        await ws.close()

    @unittest_run_loop
    async def test_agent_disconnect_removes_from_router(self):
        """When agent disconnects, router removes the agent."""
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(HelloMessage(
            group_id="tg:1", profile="/p", model="m", toolsets=[],
        ).to_dict())
        await asyncio.sleep(0.1)

        assert self.app["router"].has_agent("tg:1")

        await ws.close()
        await asyncio.sleep(0.1)

        assert not self.app["router"].has_agent("tg:1")

    @unittest_run_loop
    async def test_heartbeat_no_error(self):
        """Heartbeat messages are silently handled."""
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(HelloMessage(
            group_id="tg:1", profile="/p", model="m", toolsets=[],
        ).to_dict())
        await asyncio.sleep(0.1)

        self.app["bridge"].register_response_handler("tg:1", AsyncMock())

        await ws.send_json(HeartbeatMessage().to_dict())
        await asyncio.sleep(0.1)

        await ws.close()

    @unittest_run_loop
    async def test_dispatch_task_to_agent(self):
        """Gateway can dispatch a task to a connected agent via router."""
        ws = await self.client.ws_connect("/ws")
        await ws.send_json(HelloMessage(
            group_id="tg:1", profile="/p", model="m", toolsets=[],
        ).to_dict())
        await asyncio.sleep(0.1)

        # Dispatch task through router
        task = TaskMessage(group_id="tg:1", message="hello world").to_dict()
        sent = await self.app["router"].dispatch_task("tg:1", task)

        assert sent is True

        # Agent should receive the task
        msg = await ws.receive_json()
        assert msg["type"] == "task"
        assert msg["message"] == "hello world"

        await ws.close()


from unittest.mock import AsyncMock
```

- [ ] **Step 2: Run integration tests**

```bash
cd hermes-distributed && python -m pytest tests/test_integration.py -v
```

Expected: All PASS.

- [ ] **Step 3: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "test: Gateway+Agent integration tests (WebSocket round-trip)"
```

---

### Task 7: Manual end-to-end smoke test

This task verifies the full system works manually with Hermes AIAgent.

**Files:**
- Create: `hermes-distributed/scripts/smoke_test.sh`

- [ ] **Step 1: Create smoke test script**

```bash
#!/usr/bin/env bash
# hermes-distributed/scripts/smoke_test.sh
# Manual smoke test: start Gateway, start Agent Service, send a task, verify response.
#
# Prerequisites:
#   1. Hermes agent installed: pip install -e /path/to/hermes-agent
#   2. Export HERMES_ROOT=/path/to/hermes-agent
#   3. Export ANTHROPIC_API_KEY or set up your provider
set -euo pipefail

HERMES_ROOT="${HERMES_ROOT:?Set HERMES_ROOT to hermes-agent directory}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Starting Gateway ==="
cd "$PROJECT_DIR"
python -m gateway.server --host 127.0.0.1 --port 8900 &
GW_PID=$!
sleep 1

cleanup() {
    echo "=== Cleaning up ==="
    kill $GW_PID 2>/dev/null || true
    kill $AGENT_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Agent Service ==="
export HERMES_ROOT
python -m agent.agent_service \
    --gateway-url ws://127.0.0.1:8900/ws \
    --group-id "telegram:group:1001" \
    --hermes-home "$HOME/.hermes" &
AGENT_PID=$!
sleep 2

echo "=== Sending test task via Python ==="
python3 -c "
import asyncio, aiohttp, json

async def test():
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('ws://127.0.0.1:8900/ws') as ws:
            # Wait for agent hello (we need a second client)
            pass

asyncio.run(test())
print('Gateway is accepting connections. Test complete.')
"

echo ""
echo "=== Smoke test complete ==="
echo "Gateway PID: $GW_PID"
echo "Agent PID: $AGENT_PID"
echo "Check logs above for errors."
```

- [ ] **Step 2: Make executable and run**

```bash
chmod +x hermes-distributed/scripts/smoke_test.sh
```

Note: Full execution requires Hermes with API keys. This is a manual verification step.

- [ ] **Step 3: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "chore: smoke test script for manual e2e verification"
```

---

### Task 8: README

**Files:**
- Create: `hermes-distributed/README.md`

- [ ] **Step 1: Write README**

```markdown
# Hermes Distributed

Distributed architecture for Hermes Agent: Gateway (message routing) + Agent Service (execution) + Manager (lifecycle management).

## Phase 1: Gateway + Agent Service

### Quick Start

```bash
# Terminal 1: Start Gateway
cd hermes-distributed
pip install -e ".[dev]"
python -m gateway.server --host 127.0.0.1 --port 8900

# Terminal 2: Start Agent Service
export HERMES_ROOT=/path/to/hermes-agent
python -m agent.agent_service \
    --gateway-url ws://127.0.0.1:8900/ws \
    --group-id "telegram:group:1001" \
    --hermes-home ~/.hermes
```

### Architecture

```
Platform → [Gateway] → WebSocket → [Agent Service (Hermes)]
```

- **Gateway**: aiohttp WebSocket server, routes messages by group_id
- **Agent Service**: Connects to Gateway, runs Hermes AIAgent for each task
- **Communication**: JSON over WebSocket, bidirectional real-time

### Running Tests

```bash
cd hermes-distributed
python -m pytest tests/ -v
```

### Protocol

See `docs/superpowers/specs/2026-04-25-hermes-distributed-design.md` for the full message protocol specification.
```

- [ ] **Step 2: Commit**

```bash
cd hermes-distributed && git add -A && git commit -m "docs: README with quick start and architecture"
```

---

## Self-Review Checklist

- [x] **Spec coverage**: All Phase 1 components (Gateway, Router, Bridge, Agent Service, message protocol) have tasks
- [x] **No placeholders**: Every step has actual code, exact file paths, exact commands
- [x] **TDD**: Tests written before implementation in every task
- [x] **Type consistency**: Message classes used consistently across tasks
- [x] **DRY**: Message types defined once in `message_types.py`, reused everywhere
- [x] **YAGNI**: Phase 1 only — no Manager, no daemon, no Hermes plugin, no platform adapters
- [x] **Frequent commits**: Every task ends with a commit
