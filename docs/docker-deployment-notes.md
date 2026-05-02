# Docker 部署调试记录

本文档记录 AgentGateway 在 Docker 环境下部署过程中遇到的问题及其解决方案。

## 部署环境

- **平台**: 飞书（WebSocket 模式，国内版）
- **LLM**: DeepSeek API
- **Hermes**: 从 GitHub 动态安装 (git+https://github.com/NousResearch/hermes-agent.git)

## 问题与解决方案

### 1. Hermes 模块名冲突

**问题**: 项目目录 `agent/` 与 Hermes 安装后的 `agent/` 模块冲突，导致 import 错误。

**解决方案**: 将项目目录重命名为 `agentgw/`，并更新所有 Dockerfile 和 Python 导入路径。

```diff
- COPY agent/ agent/
+ COPY agentgw/ agentgw/

- CMD ["python3", "-m", "agent.daemon"]
+ CMD ["python3", "-m", "agentgw.daemon"]
```

### 2. Hermes LLM Provider 未检测到

**问题**: Agent 启动时报错 `No LLM provider configured`，尽管环境变量 `OPENAI_API_KEY` 和 `OPENAI_BASE_URL` 已设置。

**根因**: Hermes 的 provider 检测链优先读取 `config.yaml`，而不是环境变量。`resolve_provider()` 函数在检测到 `OPENAI_API_KEY` 时返回 `"openrouter"`，但 `resolve_runtime_provider()` 需要正确的 config.yaml 配置才能使用自定义端点。

**解决方案**: 创建 `entrypoint-daemon.sh` 在容器启动时生成 Hermes 配置文件：

```bash
# /root/.hermes/config.yaml
model:
  provider: custom
  base_url: "${OPENAI_BASE_URL}"
  default: ${HERMES_MODEL:-deepseek-chat}

# /root/.hermes/.env
OPENAI_API_KEY=${OPENAI_API_KEY}
OPENAI_BASE_URL=${OPENAI_BASE_URL}
```

### 3. Hermes API 参数变更

**问题**: AIAgent 构造函数和 `run_conversation()` 方法的参数签名与项目代码不匹配。

**错误信息**:
- `AIAgent.__init__() got unexpected keyword argument 'config'`
- `AIAgent.run_conversation() got an unexpected keyword argument 'message'`

**解决方案**: 更新 `agentgw/agent_service.py` 适配最新 Hermes API：

```python
# 旧代码
agent = AIAgent(model=model, config=config, ...)
result = agent.run_conversation(message=task.message)

# 新代码
agent = AIAgent(model=model, ...)  # 移除 config 参数
result = agent.run_conversation(user_message=task.message)  # 参数名改为 user_message
```

### 4. Model 配置解析错误

**问题**: Hermes `config.yaml` 中的 `model` 字段是字典格式，而非字符串。

**错误信息**: `'dict' object has no attribute 'lower'`

**解决方案**: 在读取 config 时正确处理字典格式：

```python
model_cfg = hermes_config.get("model", {})
if isinstance(model_cfg, dict):
    model = model_cfg.get("default") or model_cfg.get("name") or model
elif isinstance(model_cfg, str) and model_cfg:
    model = model_cfg
```

### 5. HERMES_HOME 未传递给子进程

**问题**: Daemon 启动 agent_service 子进程时未正确传递 HERMES_HOME 参数，导致 Agent 使用默认路径而非 profile 目录。

**解决方案**: 
1. Manager 在 `start_agent` 消息中生成 profile 目录路径
2. Daemon 在启动子进程前创建 profile 目录并复制配置
3. 通过 `--hermes-home` 参数和环境变量传递给子进程

```python
# manager/server.py
hermes_home_path = f"{base_hermes_root}/profiles/{group_id}"
msg = StartAgentMessage(hermes_home=hermes_home_path, ...)

# agentgw/daemon.py
cmd.extend(["--hermes-home", hermes_home_path])
env["HERMES_HOME"] = hermes_home_path
```

### 6. 飞书 Response Handler 未注册

**问题**: Gateway 收到 Agent 的 `complete` 消息后未转发给飞书，因为 response handler 未正确注册。

**解决方案**: 在 Agent 注册时动态创建并注册 response handler：

```python
# gateway/server.py - _ws_handler()
platform_prefix = msg.group_id.split(":")[0] if ":" in msg.group_id else ""
adapter = self._find_adapter_for_platform(platform_prefix)
if adapter:
    async def _response_handler(msg):
        if hasattr(msg, 'final_response'):
            await adapter.send_response(msg.group_id, msg.final_response)
        elif hasattr(msg, 'token'):
            await adapter.send_edit(msg.group_id, msg.token)
    self.bridge.register_response_handler(msg.group_id, _response_handler)
```

### 7. Manager 缺少 os 模块导入

**问题**: Manager 创建 agent 时报 `NameError: name 'os' is not defined`。

**解决方案**: 在 `manager/server.py` 开头添加 `import os`。

## Profile 隔离架构

每个 group_id 使用独立的 Hermes profile 目录实现隔离：

```
/root/.hermes/profiles/<group_id>//
├── config.yaml      # 独立的模型和 provider 配置
├── .env             # 独立的 API keys
├── sessions/        # 独立的会话历史
├── logs/            # 独立的日志
├── state.db         # 独立的状态数据库
└── memories/        # 独立的记忆存储
```

这确保了：
- 不同飞书群聊有独立的对话上下文
- 终端/VM 环境隔离（通过 Hermes 的 terminal sandbox）
- 配置可以按 group_id 定制

## 最终配置文件

### docker-compose.yml

```yaml
services:
  gateway:
    environment:
      GW_ADAPTERS_CONFIG: '{"feishu": {"enabled": true, "app_id": "...", "app_secret": "...", "connection_mode": "websocket", "domain": "feishu"}}'

  daemon:
    environment:
      DA_HERMES_PIP_SOURCE: git+https://github.com/NousResearch/hermes-agent.git
      OPENAI_API_KEY: ${OPENAI_API_KEY}
      OPENAI_BASE_URL: ${OPENAI_BASE_URL}
      HERMES_MODEL: ${HERMES_MODEL:-deepseek-chat}
```

### .env

```
DA_HERMES_PIP_SOURCE=git+https://github.com/NousResearch/hermes-agent.git
OPENAI_API_KEY=sk-xxx
OPENAI_BASE_URL=https://api.deepseek.com
HERMES_MODEL=deepseek-chat
```

## 验证流程

1. 飞书发送消息 → Gateway 收到 `Inbound message`
2. Gateway `Dispatching task` → Router `Task sent to agent`
3. Daemon 创建 profile 目录 → 启动 agent_service 子进程
4. Agent 注册 → Gateway `Agent registered`
5. Agent 处理消息 → DeepSeek API 响应
6. Gateway 收到 `complete` → 飞书收到回复

## 相关文件

- `docker/entrypoint-daemon.sh`: Hermes 配置生成脚本
- `gateway/server.py`: 飞书 response handler 注册
- `manager/server.py`: Profile 目录路径生成
- `agentgw/daemon.py`: Profile 目录创建和子进程启动
- `agentgw/agent_service.py`: Hermes API 适配

## 参考链接

- Hermes Agent: https://github.com/NousResearch/hermes-agent
- DeepSeek Hermes 集成: https://api-docs.deepseek.com/zh-cn/quick_start/agent_integrations/hermes