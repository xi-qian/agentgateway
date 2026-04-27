# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## What is AgentGateway

A distributed agent service infrastructure that decouples AI agent execution from message routing. Provides three independent services (Gateway, Manager, Daemon) that communicate via WebSocket, plus a Hermes Agent plugin for transparent integration.

## Architecture

Three services + one plugin:

- **Gateway** (port 8900): WebSocket message routing, group_id → agent mapping, platform adapters
- **Manager** (port 8800): Agent lifecycle management, REST API, SQLite state, scheduler (idle reclaim + health check)
- **Daemon**: Process manager running on each agent server, manages agent_service.py child processes
- **Plugin** (`agent/plugin/`): Monkey-patches Hermes `GatewayRunner._create_adapter()` for transparent distributed mode

Communication: JSON over WebSocket (3 links: Agent↔Gateway, Gateway→Manager REST, Daemon→Manager WS)

## Running Tests

```bash
PYTHONPATH=. python3 -m pytest tests/ -v
```

185 tests covering all phases. Uses pytest + pytest-asyncio + aiohttp.test_utils.

## Running the Services

```bash
# Gateway
PYTHONPATH=. python3 -m gateway.server --host 0.0.0.0 --port 8900

# Manager
PYTHONPATH=. python3 -m manager.server --host 0.0.0.0 --port 8800

# Daemon (on agent server)
PYTHONPATH=. python3 -m agent.daemon --manager-url ws://manager:8800/ws --server-id srv-1

# Agent Service (standalone, needs Hermes installed)
PYTHONPATH=. python3 -m agent.agent_service --gateway-url ws://gateway:8900/ws --group-id "telegram:group:1001"
```

## Key Files

| File | Purpose |
|------|---------|
| `gateway/message_types.py` | All protocol messages (14 types with to_dict/from_dict) |
| `gateway/router.py` | group_id → agent WebSocket routing + cold path |
| `gateway/bridge.py` | Agent response → platform adapter dispatch |
| `gateway/server.py` | aiohttp WebSocket server + adapter loading |
| `manager/registry.py` | SQLite backend (servers, agents, logs, sessions tables) |
| `manager/server.py` | REST API + daemon WS handler + scheduler |
| `agent/daemon.py` | Process manager, heartbeat with CPU/mem metrics |
| `agent/agent_service.py` | Agent entry point, ApprovalGate, thread-safe WS send |
| `agent/plugin/ws_adapter.py` | InternalWSAdapter bridging Hermes ↔ Gateway protocol |

## Python Compatibility

Targets Python 3.8+. Constraints:
- No walrus operator (`:=`)
- No PEP 604 union types (`X | Y`)
- `datetime.utcnow()` (not `datetime.now(timezone.utc)`)
- sqlite3 via `run_in_executor` (not aiosqlite)

## Message Protocol

All messages defined in `gateway/message_types.py` as dataclasses with `to_dict()`/`from_dict()`.

Link 1 (Gateway ↔ Agent): hello, task, stream, progress, complete, error, approval_request, approved, denied, interrupt, heartbeat, session_restore
Link 3 (Daemon ↔ Manager): register, heartbeat, agent_started, agent_stopped, log, start_agent, stop_agent

## Code Style

- Code comments in English
- Dataclasses for messages and config
- `from __future__ import annotations` in all files
- `asyncio.get_running_loop()` (not `get_event_loop()`) in async methods
- `asyncio.run_coroutine_threadsafe()` for thread → loop coroutine scheduling
