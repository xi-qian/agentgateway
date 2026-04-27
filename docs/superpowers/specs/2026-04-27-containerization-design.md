# AgentGateway 容器化部署设计

## 概述

AgentGateway 采用多镜像架构，通过 Docker + docker-compose 支持开发测试和生产分布式部署。Gateway 单副本运行（飞书 WebSocket 约束），Manager 单副本运行（有状态），Daemon 在多台 Agent 服务器上横向扩展。

## 镜像设计

### 三镜像架构

| 镜像名 | 内容 | 基础镜像 | 端口 |
|--------|------|----------|------|
| `agentgateway-gateway` | Gateway + 平台适配器（飞书、Telegram等） | python:3.11-slim | 8900 |
| `agentgateway-manager` | Manager + SQLite/PostgreSQL 驱动 | python:3.11-slim | 8800 |
| `agentgateway-daemon` | Daemon + Agent Service + Hermes 动态安装 | python:3.11-slim | 无固定端口 |

### 构建策略

三个独立 Dockerfile，位于 `docker/` 目录：

```
docker/
├── Dockerfile.gateway      # Gateway 镜像
├── Dockerfile.manager      # Manager 镜像
├── Dockerfile.daemon       # Daemon 镜像
└── Dockerfile.base         # 公共基础镜像（可选，减少重复）
```

### Daemon 镜像特殊处理

Daemon 启动时动态安装 Hermes Agent：
- 环境变量 `DA_HERMES_PIP_SOURCE` 指定安装源
- 支持本地路径（挂载 volume）或 Git URL
- Agent Service 子进程继承环境

## 配置管理

### 全部通过环境变量

| 服务 | 环境变量 | 说明 | 默认值 |
|------|----------|------|--------|
| **Gateway** | `GW_HOST` | 监听地址 | `0.0.0.0` |
| | `GW_PORT` | 监听端口 | `8900` |
| | `GW_MANAGER_URL` | Manager REST URL | 无 |
| | `GW_ADAPTERS_CONFIG` | JSON 适配器配置 | `{}` |
| **Manager** | `MG_HOST` | 监听地址 | `0.0.0.0` |
| | `MG_PORT` | 监听端口 | `8800` |
| | `MG_DB_TYPE` | 数据库类型 | `sqlite` |
| | `MG_DB_PATH` | SQLite 文件路径 | `/data/manager.db` |
| | `MG_DB_URL` | PostgreSQL 连接串 | 无 |
| | `MG_IDLE_TIMEOUT` | 空闲回收超时秒数 | `1800` |
| | `MG_HEALTH_TIMEOUT` | 健康检查超时秒数 | `90` |
| **Daemon** | `DA_MANAGER_URL` | Manager WebSocket URL | 无 |
| | `DA_SERVER_ID` | 服务器标识 | 无 |
| | `DA_HERMES_PIP_SOURCE` | Hermes 安装源 | 无 |
| | `DA_HERMES_HOME` | Hermes 配置目录 | `/root/.hermes` |
| | `DA_GATEWAY_URL` | Agent Service 连接的 Gateway URL | 无 |
| | `ANTHROPIC_API_KEY` | AI API 密钥 | 无 |

### 适配器配置格式

`GW_ADAPTERS_CONFIG` 为 JSON 字符串：

```json
{
  "feishu": {"app_id": "xxx", "app_secret": "xxx"},
  "telegram": {"token": "xxx"},
  "mock": {"enabled": true}
}
```

### 敏感信息处理

- API Key 通过环境变量传入，不写入配置文件
- PostgreSQL 密码包含在 `MG_DB_URL` 连接串中
- 生产环境使用 `.env` 文件（不提交 git）或 secrets 管理

## 数据持久化

### 挂载点

| 服务 | 挂载点 | 内容 |
|------|--------|------|
| Manager | `/data` | SQLite 数据库文件 |
| Daemon | `/root/.hermes` | Hermes 配置目录 |
| Daemon | `/logs` | Agent Service 日志输出（可选） |

### 数据库选择

- **开发/测试**：SQLite，数据持久化到 volume
- **生产**：PostgreSQL，外部数据库或 docker-compose 内 postgres 服务

## 网络设计

### 开发测试（单机 docker-compose）

- 专用网络 `agentgateway-net`
- 服务名作为主机名：`gateway`、`manager`、`daemon`
- Gateway 对外暴露端口 8900

### 生产（分布式部署）

- 核心服务器固定 IP 或域名
- Agent 服务器通过网络连接：
  - Gateway WebSocket: `ws://<core-server>:8900/ws`
  - Manager WebSocket: `ws://<core-server>:8800/ws`

## 部署架构

### 开发测试（单机）

所有服务在一台主机上运行：

```
docker-compose up -d  # 启动 gateway + manager + daemon
```

架构：

```
[Platform] → Gateway:8900 → Manager:8800
                ↓                ↓
           Agent Service ← Daemon
                ↓
           Gateway:8900/ws
```

### 生产（分布式）

**核心服务器** 运行 Gateway + Manager + PostgreSQL（可选）：

```
┌─────────────────────────────────────────┐
│         核心服务器 (core.example.com)    │
│                                         │
│  docker-compose.prod.yml:               │
│    - gateway (端口 8900)                │
│    - manager (端口 8800)                │
│    - postgres (可选，端口 5432)         │
│                                         │
└─────────────────────────────────────────┘
```

**Agent 服务器池** 运行 Daemon：

```
┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│ Server A│  │ Server B│  │ Server C│  │ Server D│
│ Daemon  │  │ Daemon  │  │ Daemon  │  │ Daemon  │
│ + Agents│  │ + Agents│  │ + Agents│  │ + Agents│
└─────────┘  └─────────┘  └─────────┘  └─────────┘
     │            │            │            │
     └────────────┴────────────┴────────────┘
              连接 core.example.com
              - gateway:8900/ws
              - manager:8800/ws
```

### Gateway 单副本约束

飞书 WebSocket 适配器只能单实例连接，Gateway 整体运行单副本：

- `replicas: 1`
- Pod 重启时飞书连接短暂断开（SDK 应支持断线重连）
- 不使用 K8s Leader Election 或其他复杂方案

## 文件结构

```
agentgateway/
├── docker/
│   ├── Dockerfile.base         # 公共基础镜像（Python 3.11 + aiohttp）
│   ├── Dockerfile.gateway      # Gateway 镜像
│   ├── Dockerfile.manager      # Manager 镜像
│   └── Dockerfile.daemon       # Daemon 镜像
├── docker-compose.yml          # 开发测试（全部服务）
├── docker-compose.prod.yml     # 生产核心服务器
├── docker-compose.agent.yml    # 生产 Agent 服务器
└── deploy/
    ├── .env.example            # 环境变量示例
    └── README.md               # 部署说明文档
```

## docker-compose 文件示例

### 开发测试 docker-compose.yml

```yaml
services:
  gateway:
    build:
      context: .
      dockerfile: docker/Dockerfile.gateway
    ports:
      - "8900:8900"
    environment:
      GW_MANAGER_URL: http://manager:8800
      GW_ADAPTERS_CONFIG: '{"mock":{"enabled":true}}'
    depends_on:
      - manager
    networks:
      - agentgateway-net

  manager:
    build:
      context: .
      dockerfile: docker/Dockerfile.manager
    ports:
      - "8800:8800"
    volumes:
      - manager-data:/data
    environment:
      MG_DB_TYPE: sqlite
      MG_DB_PATH: /data/manager.db
    networks:
      - agentgateway-net

  daemon:
    build:
      context: .
      dockerfile: docker/Dockerfile.daemon
    volumes:
      - hermes-home:/root/.hermes
    environment:
      DA_MANAGER_URL: ws://manager:8800/ws
      DA_SERVER_ID: dev-server-1
      DA_HERMES_PIP_SOURCE: /hermes
      DA_GATEWAY_URL: ws://gateway:8900/ws
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    depends_on:
      - gateway
      - manager
    networks:
      - agentgateway-net

volumes:
  manager-data:
  hermes-home:

networks:
  agentgateway-net:
```

### 生产核心服务器 docker-compose.prod.yml

```yaml
services:
  gateway:
    image: agentgateway-gateway:latest
    ports:
      - "8900:8900"
    environment:
      GW_MANAGER_URL: http://manager:8800
      GW_ADAPTERS_CONFIG: ${GW_ADAPTERS_CONFIG}
    restart: always
    depends_on:
      - manager

  manager:
    image: agentgateway-manager:latest
    ports:
      - "8800:8800"
    volumes:
      - manager-data:/data
    environment:
      MG_DB_TYPE: postgresql
      MG_DB_URL: postgresql://${PG_USER}:${PG_PASS}@postgres:5432/agentgateway
      MG_IDLE_TIMEOUT: ${MG_IDLE_TIMEOUT:-1800}
      MG_HEALTH_TIMEOUT: ${MG_HEALTH_TIMEOUT:-90}
    restart: always
    depends_on:
      - postgres

  postgres:
    image: postgres:15-alpine
    volumes:
      - postgres-data:/var/lib/postgresql/data
    environment:
      POSTGRES_USER: ${PG_USER}
      POSTGRES_PASSWORD: ${PG_PASS}
      POSTGRES_DB: agentgateway
    restart: always

volumes:
  manager-data:
  postgres-data:
```

### 生产 Agent 服务器 docker-compose.agent.yml

```yaml
services:
  daemon:
    image: agentgateway-daemon:latest
    volumes:
      - hermes-home:/root/.hermes
      - agent-logs:/logs
    environment:
      DA_MANAGER_URL: ws://${CORE_SERVER}:8800/ws
      DA_SERVER_ID: ${DA_SERVER_ID}
      DA_HERMES_PIP_SOURCE: ${DA_HERMES_PIP_SOURCE}
      DA_HERMES_HOME: /root/.hermes
      DA_GATEWAY_URL: ws://${CORE_SERVER}:8900/ws
      ANTHROPIC_API_KEY: ${ANTHROPIC_API_KEY}
    restart: always

volumes:
  hermes-home:
  agent-logs:
```

## 升级与维护

### 镜像构建

```bash
# 构建所有镜像
docker build -t agentgateway-gateway:latest -f docker/Dockerfile.gateway .
docker build -t agentgateway-manager:latest -f docker/Dockerfile.manager .
docker build -t agentgateway-daemon:latest -f docker/Dockerfile.daemon .

# 或使用 docker-compose 构建
docker-compose build
```

### 升级流程

**核心服务器升级：**

```bash
# 拉取最新镜像
docker-compose -f docker-compose.prod.yml pull

# 重启服务（Gateway 短暂断开飞书连接）
docker-compose -f docker-compose.prod.yml up -d
```

**Agent 服务器升级：**

```bash
# 拉取最新镜像
docker-compose -f docker-compose.agent.yml pull

# 重启 Daemon（Agent Service 进程由 Daemon 管理）
docker-compose -f docker-compose.agent.yml up -d
```

### 扩容流程

新增 Agent 服务器：

1. 安装 Docker
2. 复制 `docker-compose.agent.yml` 和 `.env` 到新服务器
3. 配置 `DA_SERVER_ID` 为唯一标识
4. 启动：`docker-compose -f docker-compose.agent.yml up -d`

## 设计决策摘要

| 决策项 | 选择 | 原因 |
|--------|------|------|
| 镜像架构 | 三镜像独立 | 按需部署，Daemon 可横向扩展 |
| 配置方式 | 全环境变量 | 简单，适合 docker-compose |
| 数据库 | SQLite + PostgreSQL | 开发简单，生产可靠 |
| 适配器 | 镜像内置 | 配置启用，无需额外部署 |
| Hermes 安装 | 动态 pip | 灵活切换版本 |
| Gateway 副本 | 单副本 | 飞书 WebSocket 约束 |
| 部署方式 | Docker + docker-compose | 简单实用，K8s 过度复杂 |