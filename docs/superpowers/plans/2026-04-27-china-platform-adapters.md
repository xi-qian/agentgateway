# 补全国内平台适配器（飞书、钉钉、企业微信）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 hermes-agent 的飞书、钉钉、企业微信三个适配器代码复制到 agentgateway，修改为 `PlatformAdapter` 接口，使 Gateway 支持国内三大平台。

**Architecture:** 从 hermes-agent 复制适配器源码，保留 SDK 集成逻辑和代码结构。替换 hermes-agent 框架部分（`BasePlatformAdapter`/`MessageEvent`/`SendResult`/`PlatformConfig`/`handle_message`）为 agentgateway 的 `PlatformAdapter`/`AdapterMessageEvent`/`_dispatch`。每个适配器继承 `PlatformAdapter`，实现 `start()`/`stop()`/`send_response()`。

**Tech Stack:** Python 3.11+, aiohttp, dingtalk-stream, lark_oapi, httpx

---

## Key Interface Mapping

Hermes-agent 的适配器需要映射到 agentgateway 的接口：

| Hermes-agent | agentgateway | 说明 |
|---|---|---|
| `BasePlatformAdapter` | `PlatformAdapter` | 基类 |
| `PlatformConfig` dataclass | `config: Dict[str, Any]` | 配置来源 |
| `MessageEvent(text, source, ...)` | `AdapterMessageEvent(text, group_id, ...)` | 消息事件 |
| `self.build_source(chat_id=...)` | 直接设置 `group_id` 字段 | 来源构造 |
| `handle_message(event)` | `self._dispatch(event)` | 消息分发 |
| `send(chat_id, content) -> SendResult` | `send_response(group_id, text)` | 发送响应 |
| `connect() -> bool` | `start() -> bool` | 启动 |
| `disconnect()` | `stop()` | 停止 |
| `_mark_connected()` / `_mark_disconnected()` | `self._running = True/False` | 状态标记 |
| `_set_fatal_error(...)` | `logger.error(...)` | 错误处理 |
| `Platform.FEISHU` enum | `"feishu"` 字符串 | 平台标识 |

## group_id 格式

| 平台 | 格式 | 示例 |
|---|---|---|
| feishu | `feishu:chat:{chat_id}` | `feishu:chat:oc_abc123` |
| dingtalk | `dingtalk:conversation:{conversation_id}` | `dingtalk:conversation:cid123` |
| wecom | `wecom:chat:{chat_id}` | `wecom:chat:userId123` |

## Files

| File | Action | Purpose |
|---|---|---|
| `gateway/adapters/dingtalk.py` | Create | 钉钉适配器（复制自 hermes-agent，~300行） |
| `gateway/adapters/wecom.py` | Create | 企业微信适配器（复制自 hermes-agent，~800行） |
| `gateway/adapters/feishu.py` | Create | 飞书适配器（复制自 hermes-agent，~2000行，精简版） |
| `gateway/adapters/base.py` | Modify | 添加 feishu/dingtalk/wecom 的 resolve_group_id |
| `gateway/server.py` | Modify | 更新 ADAPTER_REGISTRY |
| `pyproject.toml` | Modify | 添加可选依赖 |
| `tests/test_adapters.py` | Modify | 添加新适配器测试 |

---

### Task 1: Update base.py — add resolve_group_id for new platforms

**Files:**
- Modify: `gateway/adapters/base.py:76-93`

- [ ] **Step 1: Write the failing test**

在 `tests/test_adapters.py` 的 `TestGroupIDResolution` 类中添加：

```python
def test_resolve_feishu(self):
    group_id = resolve_group_id("feishu", {"chat_id": "oc_abc123"})
    assert group_id == "feishu:chat:oc_abc123"

def test_resolve_dingtalk(self):
    group_id = resolve_group_id("dingtalk", {"conversation_id": "cid123"})
    assert group_id == "dingtalk:conversation:cid123"

def test_resolve_wecom(self):
    group_id = resolve_group_id("wecom", {"chat_id": "userId123"})
    assert group_id == "wecom:chat:userId123"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestGroupIDResolution -v`
Expected: FAIL — `resolve_group_id` 没有 feishu/dingtalk/wecom 分支

- [ ] **Step 3: Update resolve_group_id**

在 `gateway/adapters/base.py` 的 `resolve_group_id()` 中添加三个平台分支：

```python
elif platform == "feishu":
    return f"feishu:chat:{source_info.get('chat_id', '')}"
elif platform == "dingtalk":
    return f"dingtalk:conversation:{source_info.get('conversation_id', '')}"
elif platform == "wecom":
    return f"wecom:chat:{source_info.get('chat_id', '')}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestGroupIDResolution -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add gateway/adapters/base.py tests/test_adapters.py
git commit -m "feat: add feishu/dingtalk/wecom group_id resolution"
```

---

### Task 2: Create DingTalk adapter

**Files:**
- Create: `gateway/adapters/dingtalk.py`
- Source: `/data/user/qianxi/hermes-agent/gateway/platforms/dingtalk.py` (340 lines)

钉钉适配器最简单（340行），仅使用 `dingtalk-stream` SDK WebSocket + `httpx` session webhook 回复。

- [ ] **Step 1: Write the failing test**

在 `tests/test_adapters.py` 中添加：

```python
class TestDingTalkAdapter:
    def test_dingtalk_adapter_properties(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter()
        assert adapter.name == "dingtalk"
        assert adapter.platform == "dingtalk"

    def test_dingtalk_adapter_config(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter({
            "client_id": "test_id",
            "client_secret": "test_secret",
        })
        assert adapter._client_id == "test_id"
        assert adapter._client_secret == "test_secret"

    @pytest.mark.asyncio
    async def test_dingtalk_start_fails_without_deps(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        adapter = DingTalkAdapter({
            "client_id": "test_id",
            "client_secret": "test_secret",
        })
        # Will fail because dingtalk-stream is not installed in test env
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.dingtalk import DingTalkAdapter
        assert isinstance(DingTalkAdapter.check_requirements(), bool)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestDingTalkAdapter -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create dingtalk.py**

复制 hermes-agent 的 `dingtalk.py`，做以下修改：

1. **替换导入**：删除 `from gateway.config import Platform, PlatformConfig` 和 `from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult`，改为：
```python
from gateway.adapters.base import (
    PlatformAdapter, AdapterMessageEvent, resolve_group_id,
)
```

2. **修改类定义**：`class DingTalkAdapter(BasePlatformAdapter)` → `class DingTalkAdapter(PlatformAdapter)`，添加类属性 `name = "dingtalk"` 和 `platform = "dingtalk"`

3. **修改 `__init__`**：`(self, config: PlatformConfig)` → `(self, config: Dict[str, Any] = None)`，`super().__init__(config, Platform.DINGTALK)` → `super().__init__(config)`，`extra = config.extra or {}` → 从 `self._config` 直接读取

4. **修改 `connect` → `start`**：方法名改为 `start`，删除 `self._mark_connected()` 调用，改为 `self._running = True`

5. **修改 `disconnect` → `stop`**：方法名改为 `stop`，删除 `self._mark_disconnected()` 调用

6. **修改 `_on_message`**：删除 `self.build_source(...)` 调用，删除 `MessageEvent(...)` 构造，改为构建 `AdapterMessageEvent` 并调用 `self._dispatch(event)`：
```python
group_id = resolve_group_id("dingtalk", {"conversation_id": chat_id})
event = AdapterMessageEvent(
    text=text,
    group_id=group_id,
    sender_id=sender_id,
    sender_name=sender_nick,
    message_id=msg_id,
    platform="dingtalk",
)
await self._dispatch(event)
```
删除 `timestamp` 构造和 `raw_message`。

7. **修改 `send` → `send_response`**：方法签名改为 `(self, group_id: str, text: str)`，从 `group_id` 解析 `chat_id`（取最后一段），不再返回 `SendResult`，异常时只 `logger.error`。

8. **删除不需要的方法**：`send_typing`、`get_chat_info`

9. **添加 `check_requirements` 静态方法**：`return DINGTALK_STREAM_AVAILABLE and HTTPX_AVAILABLE`

10. **保留**：`_IncomingHandler` 类、`_extract_text`、`_is_duplicate`、`_run_stream`、所有常量

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestDingTalkAdapter -v`
Expected: PASS

- [ ] **Step 5: Run all tests to verify no regressions**

Run: `PYTHONPATH=. python3 -m pytest tests/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add gateway/adapters/dingtalk.py tests/test_adapters.py
git commit -m "feat: add DingTalk platform adapter (adapted from hermes-agent)"
```

---

### Task 3: Create WeCom (Enterprise WeChat) adapter

**Files:**
- Create: `gateway/adapters/wecom.py`
- Source: `/data/user/qianxi/hermes-agent/gateway/platforms/wecom.py` (1343 lines)

企业微信适配器使用纯 aiohttp WebSocket 连接到 `wss://openws.work.weixin.qq.com`，有完整的认证、心跳、重连机制。

- [ ] **Step 1: Write the failing test**

在 `tests/test_adapters.py` 中添加：

```python
class TestWeComAdapter:
    def test_wecom_adapter_properties(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter()
        assert adapter.name == "wecom"
        assert adapter.platform == "wecom"

    def test_wecom_adapter_config(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter({
            "bot_id": "test_bot",
            "secret": "test_secret",
        })
        assert adapter._bot_id == "test_bot"
        assert adapter._secret == "test_secret"

    @pytest.mark.asyncio
    async def test_wecom_start_fails_without_deps(self):
        from gateway.adapters.wecom import WeComAdapter
        adapter = WeComAdapter({
            "bot_id": "test_bot",
            "secret": "test_secret",
        })
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.wecom import WeComAdapter
        assert isinstance(WeComAdapter.check_requirements(), bool)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestWeComAdapter -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create wecom.py**

复制 hermes-agent 的 `wecom.py`，做以下修改：

1. **替换导入**：删除 hermes-agent 特有导入，改为：
```python
from gateway.adapters.base import (
    PlatformAdapter, AdapterMessageEvent, resolve_group_id,
)
```

2. **删除不需要的导入**：删除 `cache_document_from_bytes`、`cache_image_from_bytes`（媒体缓存函数来自 hermes-agent base）

3. **类定义**：`class WeComAdapter(BasePlatformAdapter)` → `class WeComAdapter(PlatformAdapter)`，添加 `name = "wecom"` 和 `platform = "wecom"`

4. **修改 `__init__`**：`(self, config: PlatformConfig)` → `(self, config: Dict[str, Any] = None)`，`super().__init__(config, Platform.WECOM)` → `super().__init__(config)`，`extra = config.extra or {}` → 从 `self._config` 直接读取

5. **修改 `connect` → `start`**：删除 `self._mark_connected()` 和 `self._set_fatal_error(...)` 调用，改为 `self._running = True` 和 `logger.error(...)`

6. **修改 `disconnect` → `stop`**：删除 `self._mark_disconnected()`

7. **修改 `_on_message`**：删除 `self.build_source(...)` 和 `MessageEvent(...)`，改为：
```python
group_id = resolve_group_id("wecom", {"chat_id": chat_id})
event = AdapterMessageEvent(
    text=text,
    group_id=group_id,
    sender_id=sender_id,
    message_id=msg_id,
    platform="wecom",
    media_urls=media_urls,
    media_types=media_types,
    reply_to_message_id=f"quote:{msg_id}" if has_reply_context else "",
    reply_to_text=reply_text if has_reply_context else "",
)
await self._dispatch(event)
```

8. **修改 `send` → `send_response`**：方法签名改为 `(self, group_id: str, text: str)`，从 `group_id` 解析 `chat_id`，不再返回 `SendResult`。

9. **删除**：`send_image`、`send_image_file`、`send_document`、`send_voice`、`send_video`、`send_typing`、`get_chat_info`、`_download_remote_bytes`（依赖 `tools.url_safety`）、`_load_outbound_media`、`_prepare_outbound_media`、`_send_media_source`、`_send_media_message`、`_send_reply_media_message`、`_send_followup_markdown`、`_upload_media_bytes`、`_cache_media`、`_extract_media`（所有媒体上传/下载/缓存功能）

10. **保留**：WebSocket 连接逻辑（`_open_connection`、`_listen_loop`、`_heartbeat_loop`、`_dispatch_payload`）、认证逻辑（`_wait_for_handshake`）、请求-响应机制（`_send_request`、`_send_reply_request`、`_pending_responses`）、消息去重（`_is_duplicate`）、策略管理（`_is_dm_allowed`、`_is_group_allowed`）、所有常量、`_IncomingHandler` 不需要

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestWeComAdapter -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `PYTHONPATH=. python3 -m pytest tests/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add gateway/adapters/wecom.py tests/test_adapters.py
git commit -m "feat: add WeCom platform adapter (adapted from hermes-agent)"
```

---

### Task 4: Create Feishu (Lark) adapter

**Files:**
- Create: `gateway/adapters/feishu.py`
- Source: `/data/user/qianxi/hermes-agent/gateway/platforms/feishu.py` (3589 lines, 精简为 ~2000行)

飞书适配器最复杂，支持双模式（WebSocket + Webhook）、富文本解析、卡片交互、消息去重。精简时保留核心消息收发，删除高级媒体功能。

- [ ] **Step 1: Write the failing test**

在 `tests/test_adapters.py` 中添加：

```python
class TestFeishuAdapter:
    def test_feishu_adapter_properties(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter()
        assert adapter.name == "feishu"
        assert adapter.platform == "feishu"

    def test_feishu_adapter_config(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter({
            "app_id": "cli_test",
            "app_secret": "secret_test",
        })
        assert adapter._app_id == "cli_test"
        assert adapter._app_secret == "secret_test"

    @pytest.mark.asyncio
    async def test_feishu_start_fails_without_deps(self):
        from gateway.adapters.feishu import FeishuAdapter
        adapter = FeishuAdapter({
            "app_id": "cli_test",
            "app_secret": "secret_test",
        })
        started = await adapter.start()
        assert started is False

    def test_check_requirements(self):
        from gateway.adapters.feishu import FeishuAdapter
        assert isinstance(FeishuAdapter.check_requirements(), bool)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestFeishuAdapter -v`
Expected: FAIL — module not found

- [ ] **Step 3: Create feishu.py**

复制 hermes-agent 的 `feishu.py`，做以下修改：

1. **替换导入**：删除 hermes-agent 特有导入，改为：
```python
from gateway.adapters.base import (
    PlatformAdapter, AdapterMessageEvent, resolve_group_id,
)
```
删除 `from gateway.status import ...` 和 `from hermes_constants import ...`

2. **类定义**：添加 `name = "feishu"` 和 `platform = "feishu"`

3. **修改 `__init__`**：与钉钉/企微相同模式，从 `self._config` 读取 `app_id`、`app_secret`、`connection_mode` 等

4. **修改 `connect` → `start`**：删除 `_mark_connected()`、`_set_fatal_error()` 调用

5. **修改 `disconnect` → `stop`**：删除 `_mark_disconnected()` 调用

6. **修改消息处理**：删除 `MessageEvent` 构造，改为 `AdapterMessageEvent` + `self._dispatch()`

7. **修改 `send` → `send_response`**：从 `group_id` 解析 `chat_id`，不再返回 `SendResult`

8. **删除不需要的**：`cache_document_from_bytes`、`cache_image_from_url`、`cache_audio_from_bytes`、`cache_image_from_bytes` 引用，`acquire_scoped_lock`/`release_scoped_lock` 引用，`get_hermes_home` 引用，`send_image`/`send_document` 等媒体发送方法，`get_chat_info`

9. **保留**：WebSocket 连接逻辑、Webhook 模式、消息去重（`_seen_message_ids`）、文本提取和富文本解析、卡片按钮事件、正则表达式常量、所有常量定义

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestFeishuAdapter -v`
Expected: PASS

- [ ] **Step 5: Run all tests**

Run: `PYTHONPATH=. python3 -m pytest tests/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add gateway/adapters/feishu.py tests/test_adapters.py
git commit -m "feat: add Feishu platform adapter (adapted from hermes-agent)"
```

---

### Task 5: Update ADAPTER_REGISTRY and pyproject.toml

**Files:**
- Modify: `gateway/server.py:26-29`
- Modify: `pyproject.toml:15-20`

- [ ] **Step 1: Write the failing test**

在 `tests/test_adapters.py` 的 `TestAdapterServerIntegration` 类中添加：

```python
def test_load_feishu_adapter(self):
    from gateway.server import load_adapters_from_config
    config = {"adapters": {"feishu": {"enabled": True, "app_id": "x", "app_secret": "y"}}}
    adapters = load_adapters_from_config(config)
    assert len(adapters) == 1
    assert adapters[0].name == "feishu"

def test_load_dingtalk_adapter(self):
    from gateway.server import load_adapters_from_config
    config = {"adapters": {"dingtalk": {"enabled": True, "client_id": "x", "client_secret": "y"}}}
    adapters = load_adapters_from_config(config)
    assert len(adapters) == 1
    assert adapters[0].name == "dingtalk"

def test_load_wecom_adapter(self):
    from gateway.server import load_adapters_from_config
    config = {"adapters": {"wecom": {"enabled": True, "bot_id": "x", "secret": "y"}}}
    adapters = load_adapters_from_config(config)
    assert len(adapters) == 1
    assert adapters[0].name == "wecom"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. python3 -m pytest tests/test_adapters.py::TestAdapterServerIntegration::test_load_feishu_adapter -v`
Expected: FAIL — adapter not in registry

- [ ] **Step 3: Update ADAPTER_REGISTRY**

在 `gateway/server.py` 的 `ADAPTER_REGISTRY` 中添加：

```python
ADAPTER_REGISTRY = {
    "mock": "gateway.adapters.mock:MockAdapter",
    "telegram": "gateway.adapters.telegram:TelegramAdapter",
    "feishu": "gateway.adapters.feishu:FeishuAdapter",
    "dingtalk": "gateway.adapters.dingtalk:DingTalkAdapter",
    "wecom": "gateway.adapters.wecom:WeComAdapter",
}
```

- [ ] **Step 4: Update pyproject.toml**

添加可选依赖：

```toml
[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-asyncio>=0.21",
    "pytest-xdist>=3.0",
]
feishu = ["lark_oapi>=1.0"]
dingtalk = ["dingtalk-stream>=1.0", "httpx>=0.24"]
wecom = ["httpx>=0.24"]
all-china = ["lark_oapi>=1.0", "dingtalk-stream>=1.0", "httpx>=0.24"]
```

- [ ] **Step 5: Run all tests**

Run: `PYTHONPATH=. python3 -m pytest tests/ -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add gateway/server.py pyproject.toml tests/test_adapters.py
git commit -m "feat: register feishu/dingtalk/wecom adapters and add optional deps"
```

---

### Task 6: Final verification

- [ ] **Step 1: Run full test suite**

Run: `PYTHONPATH=. python3 -m pytest tests/ -v`
Expected: All tests PASS, no regressions

- [ ] **Step 2: Verify adapter imports work**

```bash
python3 -c "from gateway.adapters.feishu import FeishuAdapter; print('feishu OK')"
python3 -c "from gateway.adapters.dingtalk import DingTalkAdapter; print('dingtalk OK')"
python3 -c "from gateway.adapters.wecom import WeComAdapter; print('wecom OK')"
```
Expected: All print OK

- [ ] **Step 3: Verify Gateway starts**

```bash
PYTHONPATH=. python3 -m gateway.server --host 127.0.0.1 --port 8900 &
GW_PID=$!
sleep 2
kill $GW_PID 2>/dev/null
```
Expected: Gateway starts without import errors
