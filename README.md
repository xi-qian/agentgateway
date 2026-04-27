# AgentGateway

Distributed agent service infrastructure: Gateway (message routing) + Agent Service (execution) + Manager (lifecycle management) + Daemon (process manager). Decoupled from any specific AI agent framework — connects to Hermes Agent via plugin.

## Phase 1: Gateway + Agent Service

### Quick Start

```bash
# Terminal 1: Start Gateway
cd hermes-distributed
pip install aiohttp pyyaml
PYTHONPATH=. python -m gateway.server --host 127.0.0.1 --port 8900

# Terminal 2: Start Agent Service
export HERMES_ROOT=/path/to/hermes-agent
PYTHONPATH=. python -m agent.agent_service \
    --gateway-url ws://127.0.0.1:8900/ws \
    --group-id "telegram:group:1001" \
    --hermes-home ~/.hermes
```

### Architecture

```
Platform -> [Gateway] -> WebSocket -> [Agent Service (Hermes)]
```

- **Gateway**: aiohttp WebSocket server, routes messages by group_id
- **Agent Service**: Connects to Gateway, runs Hermes AIAgent for each task
- **Communication**: JSON over WebSocket, bidirectional real-time

### Running Tests

```bash
cd hermes-distributed
PYTHONPATH=. python -m pytest tests/ -v
```

## Phase 2: Manager + Daemon

### Quick Start

```bash
# Terminal 1: Start Manager
cd hermes-distributed
python -m manager.server --host 127.0.0.1 --port 8800

# Terminal 2: Start Daemon (on agent server)
python -m agent.daemon --manager-url ws://127.0.0.1:8800/ws \
    --server-id server-a --hermes-root /path/to/hermes-agent

# Terminal 3: Start Gateway (with manager_url configured)
python -m gateway.server --host 127.0.0.1 --port 8900
```

### Architecture

```
Platform → [Gateway:8900] → Manager REST → [Manager:8800]
                                                ↓ WebSocket
                                         [Daemon] → fork → [Agent Service]
```

- **Manager**: REST API + WebSocket server for daemon connections, SQLite state storage, background scheduler (idle reclaim + health check)
- **Daemon**: Process manager, connects to Manager, manages agent_service.py child processes, log collection, heartbeat
- **Cold Path**: Gateway → POST /api/v1/agents → Manager → daemon start_agent → Agent Service connects to Gateway
- **REST API**: `/api/v1/agents` (CRUD), `/api/v1/servers`, `/api/v1/logs/{group_id}`

### Protocol

See `docs/superpowers/specs/2026-04-25-hermes-distributed-design.md` for the full message protocol specification.

## Phase 3: Hermes Plugin Integration

### Quick Start

```bash
# Install plugin into Hermes
pip install -e /path/to/hermes-distributed

# Configure Hermes for distributed mode
cat >> ~/.hermes/config.yaml << 'EOF'
platforms:
  webhook:
    enabled: true
    extra:
      hermes_distributed_gateway_url: "ws://gateway-host:8900/ws"
      hermes_distributed_group_id: "telegram:group:1001"
      hermes_distributed_model: "opus-4.6"
plugins:
  enabled:
    - hermes-distributed
EOF

# Start Hermes gateway (plugin loads automatically)
hermes gateway
```

### How It Works

The plugin monkey-patches `GatewayRunner._create_adapter()` to return
`InternalWSAdapter` when `hermes_distributed_gateway_url` is configured.
The adapter bridges the Hermes platform adapter interface with the
distributed Gateway WebSocket protocol.

## Phase 4: Multi-Platform Adapters

### Supported Platforms

- **Telegram** -- Full support (requires python-telegram-bot)
- **Mock** -- For testing

### Configuration

```yaml
adapters:
  telegram:
    enabled: true
    token: "YOUR_BOT_TOKEN"
  mock:
    enabled: false
```

### Adding a New Adapter

1. Create `gateway/adapters/<platform>.py` implementing `PlatformAdapter`
2. Register in `ADAPTER_REGISTRY` in `gateway/server.py`
3. Configure in config.yaml

### Architecture

```
Telegram/Discord/... → [Platform Adapter] → Gateway → Agent Service
                              ↑
                         send_response()
```

Each platform adapter receives messages, normalizes them into `AdapterMessageEvent`,
and dispatches them through the Gateway. Responses flow back via `send_response()`.

## Phase 5: Session Migration

### Overview

Agent sessions are persisted in the Manager's SQLite database. When an agent
reconnects (or is recreated on a different server), the Gateway automatically
sends the session history for restoration.

### API

```
POST /api/v1/sessions    → {"group_id": "...", "history": [...], "session_id": "..."}
GET  /api/v1/sessions/{group_id}  → {"group_id": "...", "history": [...], "session_id": "..."}
```

### Flow

1. Agent sends `complete` → Gateway forwards session history to Manager
2. Agent disconnects → Manager stores session
3. Agent reconnects → Gateway fetches session from Manager → sends `session_restore`
4. Agent restores session → continues conversation
