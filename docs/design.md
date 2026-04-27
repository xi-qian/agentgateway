# Hermes Distributed Architecture Design

## Context

Hermes 当前的 Gateway 和 AIAgent 是同进程耦合的。本设计将其拆分为三个独立服务：

1. **Gateway** — 消息路由层，接收平台消息，按 group_id 路由到 Agent Service
2. **Agent Service** — 执行层，运行 Hermes 进程 + WebSocket 插件，连接 Gateway 接收任务
3. **Manager** — 管理层，管理 Agent Service 实例生命周期、负载均衡、日志收集

通信方式：WebSocket（长连接 + JSON 双向实时，飞书 SDK 模式）。
隔离粒度：按群组（group_id）隔离，同一群组内所有用户共享 Agent 实例和 session。
实例管理：daemon 容器（常驻）+ fork 子进程，通过 Manager 按需创建/回收。

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Gateway (新建独立服务)                            │
│                                                              │
│  平台适配器 (从 Hermes 复制 + 简化)                          │
│  ┌──────────┐  ┌──────────┐  ┌───────────┐                   │
│  │ Telegram │  │ Discord  │  │ Slack    │  ...               │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘                   │
│       └──────────┬───┘             └───┬────────┘            │
│                  ↓                     │                      │
│  ┌───────────────────────────────────┐  │                     │
│  │ Message Router                   │  │  REST Client          │
│  │ group_id → agent_ws 查找          │  │  (创建/状态/销毁)   │
│  │ affinity: group_id → agent       │──┤                      │
│  └──────────────┬────────────────┘  │                      │
│                 │                    │                      │
│  ┌──────────────▼────────────────┐  │                      │
│  │ Response Bridge                   │  │                      │
│  │ stream/complete/approval → 平台  │  │                      │
│  └───────────────────────────────┘  │                      │
│                                     │                      │
│  ┌────────────────────────────────┐  │                      │
│  │ Media File Server (HTTP)       │  │                      │
│  │ 附件缓存 → http://g:8901/media│  │                      │
│  └───────────────────────────────┘  │                      │
└─────────────────────────────────────┼──────────────────────┘
                                      │ REST
┌─────────────────────────────────────▼──────────────────────┐
│              Manager (新建独立服务)                            │
│                                                              │
│  REST Server (Gateway 调用) + WebSocket Server (daemon)       │
│  ─────────────────────────────────────────────────────────   │
│  • Agent 实例 CRUD (路由表: group_id → server → agent)       │
│  • 服务器资源池 + 负载均衡 (选 running_agents 最少的服务器)    │
│  • 健康检查 (daemon 心跳超时 90s → 标记失联)               │
│  • idle 回收 (last_active > 30min → 通知 daemon 停止)        │
│  • 日志存储 (SQLite, 按 group_id 索引)                      │
└─────────────────────────────────────┬──────────────────────┘
                                      │ WebSocket (daemon 连入)
                     ┌────────────────┼────────────────┐
                     ↓                 ↓                 ↓
       ┌──────────────────┐  ┌──────────────┐  ┌──────────────┐
       │ Agent Server A    │  │ Server B     │  │ Server C     │
       │ agent_daemon      │  │ agent_daemon │  │ agent_daemon │
       │  ├─ Hermes(g1)   │  │  ├─ Hermes   │  │  ├─ Hermes   │
       │  │  + WS plugin  │  │  │  + WS     │  │  │  + WS      │
       │  └─ Hermes(g2)   │  │  └─ Hermes   │  │  └─ Hermes   │
       │     + WS plugin  │  │     + WS     │  │     + WS      │
       └──────────────────┘  └──────────────┘  └──────────────┘
```

## Communication Links

| # | Link | Direction | Protocol | Purpose |
|---|------|----------|----------|---------|
| 1 | Agent Service → Gateway | Agent (Hermes plugin) 主动连 | WebSocket | task, response, stream, interrupt, approval |
| 2 | Gateway → Manager | Gateway 请求 | REST | create/destroy/query agents, disconnect notify |
| 3 | daemon → Manager | daemon 主动连 | WebSocket | register, heartbeat, receive start/stop, send logs |

## Component Responsibilities

### Gateway (`gateway/`)

New independent service. Copies Hermes platform adapters (simplified), adds routing and agent bridge.

**`server.py`** — Main entry. Starts platform adapters + router + bridge + media server.

**`adapters/`** — Platform adapters copied from Hermes (`gateway/platforms/telegram.py`, `discord.py`, etc.), simplified:
- Keep: message receive/send, file upload/download, platform SDK calls
- Remove: burst protection, media batching, rich text rendering, comment rules
- Replace: message handler → Router, session store → bridge passthrough

**`router.py`** — Message routing:
- `resolve_group_id(source) → "telegram:group:1001"` (from platform + chat_id)
- DM messages (no group) are silently dropped — distributed mode only supports group conversations
- Lookup `agent_ws[group_id]` → if found, dispatch task
- If not found → call Manager REST to create → wait for Agent connect → dispatch
- Profile resolution: Gateway config maps group_id patterns to profiles (default: `"default"`)

**`bridge.py`** — Response bridging (Agent → platform):
- stream messages → stream consumer (platform adapter edit message)
- complete → platform adapter send
- approval_request → platform adapter send prompt → wait for response → forward back
- progress → platform adapter send
- Also handles media file URL conversion for cross-server deployments

**`media_server.py`** — Attachment HTTP file server:
- aiohttp static file server on port 8901
- Serves `/media/*` from Gateway's Hermes cache directory
- Agent downloads attachments via HTTP URL

**`manager_client.py`** — REST client for Manager communication:
- `POST /api/v1/agents {group_id}` → create instance
- `DELETE /api/v1/agents/{group_id}` → destroy instance
- `GET /api/v1/agents/{group_id}` → query status
- `POST /api/v1/agents/{group_id}/disconnect` → notify disconnect

### Manager (`manager/`)

New independent service. Manages Agent Service instances.

**`server.py`** — REST + WebSocket server:
- REST on port 8800 (aiohttp): Gateway calls
- WebSocket on `/ws` (aiohttp): daemon connects

**`registry.py`** — SQLite state storage:
```
servers:    server_id, hostname, status, cpu_cores, mem_total_gb, registered_at, last_heartbeat
agents:     group_id, server_id, pid, status, profile, model, created_at, last_active_at, ended_at
logs:       group_id, timestamp, stream, line
```

**`scheduler.py`** — Scheduling:
- Load balance: select server with fewest running_agents + lowest mem_used_gb
- Idle回收: scan agents table every 60s, last_active_at > 30min → send stop_agent
- Health check: daemon heartbeat timeout 90s → mark server lost → mark all agents lost

### Agent Daemon (`agent/daemon.py`)

Runs on each Agent server. Long-lived process, forks Hermes child processes.

- Startup: WebSocket connect to Manager → send register (server_id, hostname, CPU, MEM)
- Receive start_agent: fork `hermes gateway --profile <profile> --hermes-distributed-gateway-url ws://... --hermes-distributed-group <group_id>`
- Receive stop_agent: SIGTERM child, wait 10s, SIGKILL if needed
- Monitor: child process exit → auto-report agent_stopped to Manager
- Collect logs: read child stdout/stderr line-by-line → send log messages to Manager
- Heartbeat: every 30s send running_agents list + resource usage

### Agent Service (Hermes + Plugin)

Hermes Gateway process with a plugin that adds WebSocket connectivity.

**`plugin/plugin.yaml`**:
```yaml
name: hermes-distributed
description: Connect Hermes Gateway to distributed Gateway via WebSocket
version: 0.1.0
provides_hooks:
  - gateway:startup
```

**`plugin/__init__.py`** — Plugin register function:
1. Monkey-patch `GatewayRunner._create_adapter()` to inject `InternalWSAdapter` when platform == "internal"
2. No other Hermes code changes needed

**`plugin/ws_adapter.py`** — WebSocket "platform adapter":
- Inherits from `BasePlatformAdapter`
- `connect()`: WebSocket connect to Gateway → send hello (group_id, profile, model, toolsets)
- Message loop: receive task → build `MessageEvent` → call `self._message_handler(event)` → goes through full Hermes pipeline (context building, session management, AIAgent execution, streaming, approval)
- `send()`: Hermes response → WebSocket send back to Gateway (stream/complete/approval_request)
- `send_image()`: Hermes image response → upload to media server or send via Gateway
- `send_typing()`: forward to Gateway (optional)
- Reconnect: exponential backoff on WebSocket disconnect
- Gateway side: on Agent disconnect, remove from `agent_ws` and notify Manager (POST /disconnect)

**Agent Service config.yaml**:
```yaml
platforms:
  internal:
    enabled: true
    gateway_url: ws://gateway-host:8900
    group_id: telegram:group:1001
plugins:
  enabled:
    - hermes-distributed
```

## Message Protocol

All messages include `"version": 1` for forward compatibility.

### Link 1: Gateway ↔ Agent Service (WebSocket)

```jsonc
// Agent Service → Gateway (registration)
{"version": 1, "type": "hello",
 "group_id": "telegram:group:1001",
 "profile": "/home/user/.hermes/profiles/grp-telegram-1001",
 "model": "opus-4.6",
 "toolsets": ["terminal", "file", "web", "code", "browser"],
 "capabilities": ["streaming", "approval", "interrupt"]}

// Gateway → Agent Service (task dispatch)
{"version": 1, "type": "task",
 "group_id": "telegram:group:1001",
 "message": "帮我写一个 web scraper",
 "context_prompt": "...",
 "history": [...],                    // only on first message or migration
 "sender": {"user_id": "456", "platform": "telegram", "user_name": "Alice"},
 "media": [{"type": "image", "url": "http://gateway:8901/media/img_xxx.jpg"}],
 "reply_to_message_id": "msg_789",
 "reply_to_text": "上一条消息"}

// Agent Service → Gateway (streaming)
{"version": 1, "type": "stream",
 "group_id": "telegram:group:1001", "token": "好的，我来"}

// Agent Service → Gateway (tool progress)
{"version": 1, "type": "progress",
 "group_id": "telegram:group:1001", "tool": "terminal",
 "preview": "pip install beautifulsoup4", "emoji": "📦"}

// Agent Service → Gateway (approval request)
{"version": 1, "type": "approval_request",
 "group_id": "telegram:group:1001", "request_id": "req_001",
 "command": "rm -rf /tmp/build", "reason": "清理构建产物"}

// Gateway → Agent Service (approval response)
{"version": 1, "type": "approved",
 "group_id": "telegram:group:1001", "request_id": "req_001"}
{"version": 1, "type": "denied",
 "group_id": "telegram:group:1001", "request_id": "req_001",
 "reason": "路径看起来危险"}

// Gateway → Agent Service (interrupt)
{"version": 1, "type": "interrupt",
 "group_id": "telegram:group:1001"}

// Agent Service → Gateway (completion)
{"version": 1, "type": "complete",
 "group_id": "telegram:group:1001",
 "final_response": "完整回复...",
 "api_calls": 5,
 "tokens": {"input": 2340, "output": 1567, "cache_read": 500},
 "cost_usd": 0.0034, "interrupted": false}

// Agent Service → Gateway (error)
{"version": 1, "type": "error",
 "group_id": "telegram:group:1001", "message": "API rate limit", "fatal": false}

// Bidirectional heartbeat (every 60s)
{"version": 1, "type": "heartbeat"}
```

### Link 2: Gateway → Manager (REST)

```
POST   /api/v1/agents              →  {"group_id": "...", "profile": "default"} →  {"status": "starting", "server_id": "..."}
DELETE /api/v1/agents/{group_id}   →  {"force": false}              →  {"status": "stopping"}
GET    /api/v1/agents              →                               →  [{"group_id": "...", "status": "running", ...}]
GET    /api/v1/agents/{group_id}   →                               →  {"status": "running", "server_id": "...", "pid": 12345, ...}
POST   /api/v1/agents/{group_id}/disconnect  →  {"reason": "closed"}   →  {"status": "notified"}
GET    /api/v1/servers             →                               →  [{"server_id": "...", "status": "online", "running_agents": 2, ...}]
GET    /api/v1/logs/{group_id}     →  ?tail=100&since=...        →  {"logs": [{"timestamp": "...", "stream": "stdout", "line": "..."}]}
```

### Link 3: daemon → Manager (WebSocket)

```jsonc
// daemon → Manager (registration)
{"version": 1, "type": "register",
 "server_id": "server-a", "hostname": "gpu-node-1",
 "cpu_cores": 8, "mem_total_gb": 32, "mem_available_gb": 24}

// Manager → daemon (start agent)
{"version": 1, "type": "start_agent",
 "group_id": "telegram:group:1001", "profile": "grp-telegram-1001",
 "gateway_url": "ws://gateway:8900",
 "hermes_home": "/home/user/.hermes"}

// daemon → Manager (agent started)
{"version": 1, "type": "agent_started",
 "group_id": "telegram:group:1001", "pid": 12345, "status": "running"}

// Manager → daemon (stop agent)
{"version": 1, "type": "stop_agent",
 "group_id": "telegram:group:1001", "force": false}

// daemon → Manager (agent stopped)
{"version": 1, "type": "agent_stopped",
 "group_id": "telegram:group:1001", "pid": 12345,
 "exit_code": 0, "reason": "idle_timeout"}

// daemon → Manager (log line)
{"version": 1, "type": "log",
 "group_id": "telegram:group:1001", "pid": 12345,
 "stream": "stdout", "line": "Loading tools..."}

// daemon → Manager (heartbeat, every 30s)
{"version": 1, "type": "heartbeat",
 "running_agents": [{"group_id": "...", "pid": 12345, "mem_mb": 280}],
 "mem_used_gb": 3.2, "cpu_percent": 45}
```

## Message Flows

### Hot Path: Agent Online (~0ms extra latency)

```
Platform message → Adapter → MessageEvent
  → resolve group_id "telegram:group:1001"
  → agent_ws[group_id] found → ws.send(task)
  → Agent receives task → Hermes processes → ws.send(stream/complete)
  → Gateway bridge → platform adapter send
```

### Cold Path: First Message (1-2s for Hermes startup)

```
Platform message → Adapter → MessageEvent
  → resolve group_id → agent_ws not found
  → POST /api/v1/agents to Manager
    → Manager selects server-b (least loaded)
    → Manager ws.send(start_agent) to server-b daemon
    → daemon forks: hermes gateway --profile ... --gateway-url ws://...
    → daemon sends agent_started
    → Manager returns 200 {status: "starting"}
  → Gateway waits for "hello" WebSocket from new Agent
  → Agent hello received → registered in agent_ws
  → ws.send(task) → Agent processes → response
```

### Approval Flow

```
Agent executes dangerous command (e.g., rm -rf /tmp/build)
  → Agent ws.send(approval_request)
  → Gateway bridge → platform adapter send "⚠️ 需要审批: rm -rf /tmp/build"
  → User sends /approve
  → Platform adapter receives → Gateway
  → Gateway ws.send(approved) to Agent
  → Agent resumes execution
```

### Idle Reclaim

```
Agent last_active > 30 minutes
  → Manager scheduler detects idle
  → Manager ws.send(stop_agent, force=false) to daemon
  → daemon SIGTERM child process
  → Child exits → daemon ws.send(agent_stopped)
  → Manager updates agents table
  → Agent WebSocket disconnects → Gateway removes from agent_ws
  → Next message for this group → cold path (create new instance)
```

### Attachment Flow

```
User sends image via Telegram
  → Platform adapter downloads to local cache (/tmp/hermes_cache/img_xxx.jpg)
  → Gateway builds task with media: [{type: "image", url: "http://gateway:8901/media/img_xxx.jpg"}]
  → ws.send(task) to Agent
  → Agent (Hermes) receives media url
  → Agent downloads via HTTP (same machine) → cache_image_from_bytes()
  → Hermes vision tool processes image → response
  → (Cross-server: Agent downloads via HTTP URL from Gateway's media server)
```

## Project Structure

```
hermes-distributed/
├── gateway/
│   ├── __init__.py
│   ├── server.py                  # Main entry: platform adapters + router + bridge
│   ├── adapters/                  # From Hermes (copied + simplified)
│   │   ├── base.py              # BasePlatformAdapter interface
│   │   ├── telegram.py          # Telegram adapter
│   │   ├── discord.py           # Discord adapter
│   │   └── ...
│   ├── router.py                  # group_id routing + agent_ws registry
│   ├── bridge.py                  # Response bridge (stream/complete/approval → platform)
│   ├── manager_client.py           # Gateway → Manager REST client
│   └── media_server.py            # Attachment HTTP file server
│
├── manager/
│   ├── __init__.py
│   ├── server.py                  # REST (aiohttp:8800) + WebSocket (/ws)
│   ├── registry.py                # SQLite state storage
│   └── scheduler.py               # Load balance + idle reclaim + health check
│
├── agent/
│   ├── daemon.py                  # Process manager: connect Manager, fork Hermes children
│   └── plugin/                    # Hermes plugin (installed to ~/.hermes/plugins/)
│       ├── plugin.yaml
│       ├── __init__.py           # register() → inject InternalWSAdapter
│       └── ws_adapter.py         # WebSocket "platform adapter" (BasePlatformAdapter subclass)
│
├── pyproject.toml
└── README.md
```

## Hermes Code Changes

**Zero changes to Hermes codebase.** All integration is through:
1. Hermes plugin system (`~/.hermes/plugins/hermes-distributed/`) — injects WebSocket adapter
2. Hermes config.yaml — adds `platforms.internal` config
3. Hermes is used as a library by Agent Service (imports AIAgent, tools, config, etc.)

## Implementation Phases

### Phase 1: Gateway + Agent Service (end-to-end)
- Gateway: minimal server with one hardcoded platform adapter + WebSocket server + router
- Agent Service: agent_service.py (standalone script, imports Hermes as library) + manual start
- Verify: message round-trip, streaming, interrupt, approval

### Phase 2: Manager + daemon
- Manager: REST + WebSocket server, registry, scheduler
- Daemon: connect Manager, fork Hermes processes, log collection
- Verify: instance creation, idle reclaim, log query, health check

### Phase 3: Hermes plugin integration
- Plugin: InternalWSAdapter + monkey-patch injection
- Agent Service: run as `hermes gateway` with plugin instead of standalone script
- Verify: plugin loads, WebSocket adapter works, full Hermes pipeline intact

### Phase 4: Multi-platform adapters
- Copy + simplify Hermes adapters (Telegram, Discord, etc.)
- Verify: each platform sends/receives messages correctly

### Phase 5: Session migration + cross-server
- Session history persistence to shared storage (OSS/MinIO)
- Agent migration: recreate on different server, restore session
- Verify: agent crash → new server → session restored

## Verification

### Phase 1
1. Start Gateway + one Agent Service → verify WebSocket connection
2. Send test message → verify task dispatch → Agent processes → response returns
3. Verify streaming (incremental tokens)
4. Verify interrupt (/interrupt signal → Agent stops)
5. Verify approval (Agent requests → approve → continues)

### Phase 2
1. Start Manager + two daemons on different servers
2. Send message for unknown group → verify Manager creates instance → daemon forks → Agent connects
3. Wait 30 min idle → verify Agent reclaimed
4. GET /api/v1/logs → verify log entries

### Phase 3
1. Install plugin to Hermes → start `hermes gateway` with internal platform
2. Verify Hermes plugin loads at startup
3. Full pipeline: platform → Gateway → Agent (Hermes with plugin) → response

### End-to-end
1. Multi-platform: Telegram + Discord messages → different groups → different Agent Services
2. Agent crash → Manager detects → new instance → session continues
3. Attachments: image → vision processing, audio → STT transcription
