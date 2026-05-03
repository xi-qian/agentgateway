"""Agent Service -- connects to Gateway via WebSocket, runs Hermes AIAgent for each task.

Usage:
    python -m agent.agent_service --gateway-url ws://localhost:8900/ws \
        --group-id telegram:group:1001 \
        --profile /home/user/.hermes/profiles/grp-1001 \
        --model opus-4.6
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp

HERMES_ROOT = os.environ.get("HERMES_ROOT", "")
if HERMES_ROOT and HERMES_ROOT not in sys.path:
    sys.path.insert(0, HERMES_ROOT)

logger = logging.getLogger("agentgw.service")


# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------


def build_hello(group_id: str, profile: str, model: str, toolsets: List[str]) -> str:
    """Build the JSON hello message sent immediately after WebSocket connect."""
    return json.dumps({
        "version": 1,
        "type": "hello",
        "group_id": group_id,
        "profile": profile,
        "model": model,
        "toolsets": toolsets,
        "capabilities": ["streaming", "approval", "interrupt"],
    })


def build_agent_message(msg) -> str:
    """Serialize any message dataclass (with ``to_dict()``) to a JSON string."""
    if hasattr(msg, "to_dict"):
        return json.dumps(msg.to_dict())
    return json.dumps(msg)


# ---------------------------------------------------------------------------
# Approval gate -- allows the agent thread to block until user approves/denies
# ---------------------------------------------------------------------------


class ApprovalGate:
    """Manages pending approval requests.

    The agent calls ``request(rid)`` from a background thread and blocks until
    the gateway forwards the user's decision (approve/deny).
    """

    def __init__(self) -> None:
        self._pending: Dict[str, asyncio.Future] = {}

    async def request(self, request_id: str) -> bool:
        """Block until ``request_id`` is approved or denied.  Returns True/False.

        Times out after 300 seconds and returns False.
        """
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
        """Resolve a pending approval with True."""
        future = self._pending.get(request_id)
        if future and not future.done():
            future.set_result(True)

    def deny(self, request_id: str) -> None:
        """Resolve a pending approval with False."""
        future = self._pending.get(request_id)
        if future and not future.done():
            future.set_result(False)


# ---------------------------------------------------------------------------
# Interrupt flag -- simple boolean flag for cooperative cancellation
# ---------------------------------------------------------------------------


class InterruptFlag:
    """Thread-safe cooperative interrupt flag."""

    def __init__(self) -> None:
        self._flag = False

    def set(self) -> None:
        self._flag = True

    def is_set(self) -> bool:
        return self._flag

    def clear(self) -> None:
        self._flag = False


# ---------------------------------------------------------------------------
# Main agent service loop
# ---------------------------------------------------------------------------


async def run_agent_service(
    gateway_url: str,
    group_id: str,
    profile: str,
    model: str,
    toolsets: Optional[List[str]] = None,
    hermes_home: Optional[str] = None,
) -> None:
    """Connect to the Gateway WebSocket and process tasks indefinitely.

    Reconnects with exponential backoff on disconnect.
    """
    if toolsets is None:
        toolsets = ["terminal", "file", "web", "feishu_doc", "feishu_drive"]
    if hermes_home:
        os.environ["HERMES_HOME"] = hermes_home

    approval_gate = ApprovalGate()
    interrupt_flag = InterruptFlag()
    request_counter: Dict[str, int] = {"n": 0}
    main_loop = asyncio.get_running_loop()

    def next_request_id() -> str:
        request_counter["n"] += 1
        return f"req_{request_counter['n']:04d}"

    # ---- helpers -----------------------------------------------------------

    async def send(ws: aiohttp.ClientWebSocketResponse, msg: Any) -> None:
        payload = build_agent_message(msg)
        logger.debug("-> Gateway: %s", payload[:200])
        await ws.send_str(payload)

    stream_queue: asyncio.Queue = asyncio.Queue()
    stop_event = asyncio.Event()

    def stream_callback(text: str) -> None:
        """Called from the agent thread to push a streaming token."""
        try:
            stream_queue.put_nowait(text)
        except asyncio.QueueFull:
            pass

    async def stream_sender(ws: aiohttp.ClientWebSocketResponse) -> None:
        """Drain the stream queue and send tokens to the gateway."""
        from gateway.message_types import StreamMessage

        while not stop_event.is_set():
            try:
                text = await asyncio.wait_for(stream_queue.get(), timeout=0.5)
                await send(ws, StreamMessage(group_id=group_id, token=text))
            except asyncio.TimeoutError:
                continue

    # Approval callback: runs inside the agent thread (via run_in_executor).
    # Schedules the approval request directly onto the event loop for sending,
    # then blocks on the ApprovalGate future.
    def approval_callback(command: str, reason: str = "") -> bool:
        rid = next_request_id()
        from gateway.message_types import ApprovalRequestMessage

        req_msg = ApprovalRequestMessage(
            group_id=group_id,
            request_id=rid,
            command=command,
            reason=reason,
        )

        # Schedule direct send on the event loop (not via stream_queue)
        def _send_approval():
            # Capture ws from outer scope at schedule time
            asyncio.ensure_future(send_fn(_current_ws, req_msg))
        main_loop.call_soon_threadsafe(_send_approval)

        # Block this thread until the gateway resolves the future on the main loop
        done_event = threading.Event()
        result_holder = {"value": False}

        def _schedule_request():
            coro = approval_gate.request(rid)
            future = asyncio.ensure_future(coro)

            def _set_result(fut):
                try:
                    result_holder["value"] = fut.result()
                except Exception:
                    result_holder["value"] = False
                done_event.set()

            future.add_done_callback(_set_result)

        main_loop.call_soon_threadsafe(_schedule_request)
        done_event.wait()
        return result_holder["value"]

    # ---- connect / reconnect -----------------------------------------------

    retry_delay = 1.0
    max_retry_delay = 60.0
    _current_ws = None  # set on each (re)connect for approval_callback

    while True:
        try:
            logger.info("Connecting to %s ...", gateway_url)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(gateway_url) as ws:
                    retry_delay = 1.0
                    _current_ws = ws

                    hello = build_hello(group_id, profile, model, toolsets)
                    await ws.send_str(hello)
                    logger.info("Sent hello: group_id=%s, model=%s", group_id, model)

                    sender_task = asyncio.create_task(stream_sender(ws))
                    try:
                        async for ws_msg in ws:
                            if ws_msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(ws_msg.data)
                                from gateway.message_types import (  # noqa: WPS433
                                    parse_gateway_message,
                                    TaskMessage,
                                    InterruptMessage,
                                    ApprovedMessage,
                                    DeniedMessage,
                                )
                                try:
                                    msg = parse_gateway_message(data)
                                except Exception as exc:
                                    logger.warning("Failed to parse message: %s", exc)
                                    continue

                                if isinstance(msg, TaskMessage):
                                    interrupt_flag.clear()
                                    await handle_task(
                                        msg, ws, send, stream_callback,
                                        approval_callback, interrupt_flag,
                                        group_id, toolsets,
                                    )
                                elif isinstance(msg, InterruptMessage):
                                    interrupt_flag.set()
                                    logger.info("Interrupt received")
                                elif isinstance(msg, ApprovedMessage):
                                    approval_gate.approve(msg.request_id)
                                elif isinstance(msg, DeniedMessage):
                                    approval_gate.deny(msg.request_id)

                            elif ws_msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSE,
                            ):
                                logger.warning("WebSocket closed: %s", ws_msg)
                                break
                    finally:
                        stop_event.set()
                        sender_task.cancel()

        except (aiohttp.WSSLError, aiohttp.ClientError) as exc:
            logger.error("Connection error: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error: %s", exc)

        logger.info("Reconnecting in %.1fs ...", retry_delay)
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


# ---------------------------------------------------------------------------
# Task handler
# ---------------------------------------------------------------------------


async def handle_task(
    task,
    ws,
    send_fn,
    stream_callback,
    approval_callback,
    interrupt_flag: InterruptFlag,
    group_id: str,
    toolsets: List[str],
) -> None:
    """Run the Hermes AIAgent in a thread pool executor for a single task."""
    from gateway.message_types import CompleteMessage, ErrorMessage

    logger.info("Received task: %s (message: %s)", group_id, task.message[:100] if task.message else "")
    loop = asyncio.get_running_loop()

    def _run_agent() -> None:
        try:
            hermes_home = os.environ.get("HERMES_HOME", "")
            if not hermes_home:
                hermes_home = str(Path.home() / ".hermes")
                os.environ["HERMES_HOME"] = hermes_home

            # Inject Feishu client into tool thread-locals for feishu_doc/feishu_drive tools
            # Use Hermes get_env_value() to read from .env file if not in os.environ
            try:
                from hermes_cli.config import get_env_value
            except ImportError:
                get_env_value = lambda k: os.environ.get(k, "")

            feishu_app_id = get_env_value("FEISHU_APP_ID")
            if feishu_app_id:
                try:
                    import lark_oapi as lark
                    from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN
                    from tools.feishu_doc_tool import set_client as set_doc_client
                    from tools.feishu_drive_tool import set_client as set_drive_client

                    domain = get_env_value("FEISHU_DOMAIN") or "feishu"
                    domain_const = FEISHU_DOMAIN if domain != "lark" else LARK_DOMAIN
                    client = (
                        lark.Client.builder()
                        .app_id(feishu_app_id)
                        .app_secret(get_env_value("FEISHU_APP_SECRET") or "")
                        .domain(domain_const)
                        .log_level(lark.LogLevel.WARNING)
                        .build()
                    )
                    set_doc_client(client)
                    set_drive_client(client)
                    logger.info("Injected Feishu client into tool thread-locals (app_id=%s)", feishu_app_id[:8])
                except ImportError as e:
                    logger.warning("lark_oapi not installed, skipping Feishu client injection: %s", e)
                except Exception as e:
                    logger.warning("Failed to inject Feishu client: %s", e)

            from run_agent import AIAgent
            from hermes_state import SessionDB

            session_db = SessionDB()
            model = os.environ.get("HERMES_MODEL", "deepseek-chat")
            try:
                from hermes_cli.config import load_config as load_hermes_config
                hermes_config = load_hermes_config() or {}
                model_cfg = hermes_config.get("model", {})
                # model_cfg can be a string (model name) or a dict with 'default' key
                if isinstance(model_cfg, dict):
                    model = model_cfg.get("default") or model_cfg.get("name") or model
                elif isinstance(model_cfg, str) and model_cfg:
                    model = model_cfg
            except Exception:
                pass

            agent = AIAgent(
                model=model,
                session_db=session_db,
                stream_delta_callback=stream_callback,
                enabled_toolsets=toolsets,
            )
            result = agent.run_conversation(
                user_message=task.message,
            )
            if not result or not hasattr(result, "get"):
                result = {}
            tokens = getattr(result, "tokens", {}) or {}
            complete = CompleteMessage(
                group_id=group_id,
                final_response=result.get("final_response", ""),
                api_calls=result.get("api_calls", 0),
                tokens=tokens,
                cost_usd=result.get("cost_usd", 0.0),
                interrupted=interrupt_flag.is_set(),
            )
            asyncio.run_coroutine_threadsafe(send_fn(ws, complete), loop)
        except Exception as exc:
            logger.error("Agent execution failed: %s", exc, exc_info=True)
            error = ErrorMessage(
                group_id=group_id, message=str(exc), fatal=True,
            )
            try:
                fut = asyncio.run_coroutine_threadsafe(send_fn(ws, error), loop)
                fut.result(timeout=5)
            except Exception:
                logger.debug("Failed to send error message to gateway", exc_info=True)

    await loop.run_in_executor(None, _run_agent)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hermes Distributed Agent Service",
    )
    parser.add_argument(
        "--gateway-url", required=True,
        help="Gateway WebSocket URL",
    )
    parser.add_argument(
        "--group-id", required=True,
        help="Group ID for this agent",
    )
    parser.add_argument(
        "--profile", default=None,
        help="Hermes profile path",
    )
    parser.add_argument(
        "--model", default=None,
        help="Model override",
    )
    parser.add_argument(
        "--hermes-home", default=None,
        help="HERMES_HOME path",
    )
    parser.add_argument(
        "--toolsets", nargs="*", default=None,
        help="Tool sets to enable",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

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
