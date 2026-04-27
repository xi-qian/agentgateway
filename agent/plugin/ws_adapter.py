"""InternalWSAdapter — bridges Hermes BasePlatformAdapter to the distributed Gateway.

This adapter runs inside the Hermes process.  It connects to the distributed
Gateway over WebSocket and translates between Hermes's
``BasePlatformAdapter`` interface (connect / disconnect / send / edit_message /
handle_message) and the distributed Gateway's message protocol (hello / task /
stream / complete / approved / denied / interrupt).
"""
from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger("hermes_distributed.ws_adapter")


# ---------------------------------------------------------------------------
# Fallback stubs used when Hermes is not importable (e.g. Python < 3.10).
# Tests set these at the class level before calling static helpers.
# ---------------------------------------------------------------------------


class _StubPlatform:
    """Minimal Platform-like stub for testing.

    Callable so that ``Platform("telegram")`` creates a new instance
    (mimics enum behaviour).
    """

    def __call__(self, value):
        return _StubPlatform(value)

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class _StubMessageType:
    """Minimal MessageType-like stub for testing.

    Callable so that ``MessageType("text")`` creates a new instance.
    """

    def __call__(self, value):
        return _StubMessageType(value)

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value


class _StubSendResult:
    """Minimal SendResult-like stub for testing."""
    def __init__(self, success, message_id=None, error=None):
        self.success = success
        self.message_id = message_id
        self.error = error


class _StubSessionSource:
    """Minimal SessionSource-like stub for testing."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


class _StubMessageEvent:
    """Minimal MessageEvent-like stub for testing."""
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


# ---------------------------------------------------------------------------
# Approval gate
# ---------------------------------------------------------------------------


class ApprovalGate:
    """Manages pending approval requests.

    The agent calls ``request(rid)`` and blocks until the gateway forwards
    the user's decision (approve/deny).
    """

    def __init__(self) -> None:
        self._pending: Dict[str, asyncio.Future] = {}

    async def request(self, request_id: str) -> bool:
        """Block until *request_id* is approved or denied.

        Returns ``True`` on approve, ``False`` on deny or timeout (300 s).
        """
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        self._pending[request_id] = future
        try:
            done, _pending_set = await asyncio.wait(
                {future}, timeout=300,
            )
            if done:
                return future.result()
            return False
        except asyncio.CancelledError:
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


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class InternalWSAdapter:
    """WebSocket adapter bridging Hermes <-> distributed Gateway.

    Subclasses :class:`~gateway.platforms.base.BasePlatformAdapter` and
    implements the required interface so that Hermes's ``GatewayRunner`` can
    treat the distributed Gateway like any other messaging platform.

    When Hermes classes are importable (Python >= 3.10 with Hermes on
    ``sys.path``), they are loaded via :meth:`_ensure_hermes_imports`.
    Otherwise lightweight stubs are used so that unit tests can run on any
    Python version.
    """

    # Cached Hermes classes — populated by _ensure_hermes_imports() or
    # set manually in tests.
    _BasePlatformAdapter = None  # type: ignore[assignment]
    _Platform = None  # type: ignore[assignment]
    _PlatformConfig = None  # type: ignore[assignment]
    _MessageEvent = None  # type: ignore[assignment]
    _SendResult = None  # type: ignore[assignment]
    _SessionSource = None  # type: ignore[assignment]
    _MessageType = None  # type: ignore[assignment]

    @classmethod
    def _ensure_hermes_imports(cls) -> None:
        """Attempt to import Hermes classes; fall back to stubs."""
        if cls._BasePlatformAdapter is not None:
            return
        try:
            from gateway.platforms.base import (  # type: ignore[no-redef]
                BasePlatformAdapter,
                MessageEvent,
                MessageType,
                SendResult,
            )
            from gateway.config import Platform, PlatformConfig  # type: ignore[no-redef]
            from gateway.session import SessionSource  # type: ignore[no-redef]

            cls._BasePlatformAdapter = BasePlatformAdapter
            cls._MessageEvent = MessageEvent
            cls._MessageType = MessageType
            cls._SendResult = SendResult
            cls._Platform = Platform
            cls._PlatformConfig = PlatformConfig
            cls._SessionSource = SessionSource
        except Exception:
            # Hermes not available — install lightweight stubs so unit
            # tests can run on Python < 3.10.
            cls._Platform = _StubPlatform
            cls._MessageEvent = _StubMessageEvent
            cls._SendResult = _StubSendResult
            cls._SessionSource = _StubSessionSource
            cls._MessageType = _StubMessageType

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, config) -> None:  # type: ignore[type-arg]
        self._ensure_hermes_imports()

        # Parse distributed-specific settings from config.extra (before super
        # init, because we need _gateway_url etc. for config validation tests
        # that use __new__ to bypass Hermes imports).
        self._parse_config(config)

        # Only call super().__init__ if Hermes BasePlatformAdapter is real.
        # Tests using __new__ skip this entirely.
        if self._BasePlatformAdapter is not None:
            super(InternalWSAdapter, self).__init__(config, self._Platform.WEBHOOK)  # type: ignore[misc]

        # Internal state
        self._ws = None
        self._session = None
        self._approval_gate = ApprovalGate()
        self._stream_queue: asyncio.Queue = asyncio.Queue()
        self._stop_event = asyncio.Event()
        self._receive_task = None  # type: Optional[asyncio.Task]
        self._stream_task = None  # type: Optional[asyncio.Task]
        self._request_counter = 0

    # ------------------------------------------------------------------
    # Config parsing
    # ------------------------------------------------------------------

    def _parse_config(self, config) -> None:  # type: ignore[type-arg]
        """Extract distributed-Gateway settings from *config.extra*."""
        extra = getattr(config, "extra", {}) or {}
        self._gateway_url = extra.get("hermes_distributed_gateway_url", "")
        self._group_id = extra.get("hermes_distributed_group_id", "")
        self._model = extra.get("hermes_distributed_model", "")

        if not self._gateway_url:
            raise ValueError(
                "hermes_distributed_gateway_url is required in config.extra"
            )

    # ------------------------------------------------------------------
    # Hello message
    # ------------------------------------------------------------------

    def _build_hello(self) -> str:
        """Build the JSON hello message sent immediately after connect."""
        return json.dumps({
            "version": 1,
            "type": "hello",
            "group_id": self._group_id,
            "profile": "",
            "model": self._model,
            "toolsets": [],
            "capabilities": ["streaming", "approval", "interrupt"],
        })

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> bool:
        """Connect to the distributed Gateway and start receive/stream tasks."""
        import aiohttp

        try:
            self._session = aiohttp.ClientSession()
            self._ws = await self._session.ws_connect(self._gateway_url)

            # Send hello
            hello = self._build_hello()
            await self._ws.send_str(hello)
            logger.info(
                "Connected to distributed Gateway: %s (group=%s, model=%s)",
                self._gateway_url,
                self._group_id,
                self._model,
            )

            # Start background tasks
            self._stop_event.clear()
            self._stream_task = asyncio.create_task(self._stream_sender())
            self._receive_task = asyncio.create_task(self._receive_loop())

            self._mark_connected()  # type: ignore[attr-defined]
            return True
        except Exception as exc:
            logger.error("Failed to connect to distributed Gateway: %s", exc)
            return False

    async def disconnect(self) -> None:
        """Stop background tasks and close the WebSocket."""
        self._stop_event.set()

        if self._stream_task and not self._stream_task.done():
            self._stream_task.cancel()
        if self._receive_task and not self._receive_task.done():
            self._receive_task.cancel()

        # Await cancellation
        for task in (self._stream_task, self._receive_task):
            if task is not None:
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

        self._ws = None
        self._session = None
        self._stream_task = None
        self._receive_task = None

        self._mark_disconnected()  # type: ignore[attr-defined]
        logger.info("Disconnected from distributed Gateway")

    # ------------------------------------------------------------------
    # Outbound: send / edit / typing
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Send a CompleteMessage to the Gateway."""
        from gateway.message_types import CompleteMessage

        if self._ws is None or self._ws.closed:
            return self._build_send_result(
                success=False, error="WebSocket not connected"
            )

        msg = CompleteMessage(
            group_id=self._group_id,
            final_response=content,
        )
        try:
            await self._ws.send_str(json.dumps(msg.to_dict()))
            return self._build_send_result(success=True, message_id=str(uuid.uuid4()))
        except Exception as exc:
            return self._build_send_result(success=False, error=str(exc))

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ):
        """Handle edit_message.

        If ``finalize`` is True, this is the last edit in a streaming sequence
        -- no further action needed.

        Otherwise, queue the content for the stream sender to forward as a
        StreamMessage token.
        """
        if finalize:
            return self._build_send_result(success=True, message_id=message_id)

        # Queue streaming token
        try:
            self._stream_queue.put_nowait(content)
        except asyncio.QueueFull:
            logger.warning("Stream queue full, dropping edit chunk")

        return self._build_send_result(success=True, message_id=message_id)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Send a ProgressMessage to indicate the agent is working."""
        from gateway.message_types import ProgressMessage

        if self._ws is None or self._ws.closed:
            return

        msg = ProgressMessage(
            group_id=self._group_id,
            tool="thinking",
            preview="...",
        )
        try:
            await self._ws.send_str(json.dumps(msg.to_dict()))
        except Exception:
            pass

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        """Return minimal chat info."""
        return {"name": self._group_id, "type": "group"}

    # ------------------------------------------------------------------
    # Approval
    # ------------------------------------------------------------------

    async def request_approval(self, command: str, reason: str = "") -> bool:
        """Send an ApprovalRequestMessage and wait for the user's decision."""
        from gateway.message_types import ApprovalRequestMessage

        self._request_counter += 1
        request_id = "req_{:04d}".format(self._request_counter)

        msg = ApprovalRequestMessage(
            group_id=self._group_id,
            request_id=request_id,
            command=command,
            reason=reason,
        )

        if self._ws and not self._ws.closed:
            try:
                await self._ws.send_str(json.dumps(msg.to_dict()))
            except Exception:
                return False

        return await self._approval_gate.request(request_id)

    # ------------------------------------------------------------------
    # Internal: stream sender
    # ------------------------------------------------------------------

    async def _stream_sender(self) -> None:
        """Drain the stream queue and send StreamMessages to the Gateway."""
        from gateway.message_types import StreamMessage

        while not self._stop_event.is_set():
            try:
                text = await asyncio.wait_for(self._stream_queue.get(), timeout=0.5)
                if self._ws and not self._ws.closed:
                    msg = StreamMessage(group_id=self._group_id, token=text)
                    await self._ws.send_str(json.dumps(msg.to_dict()))
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("Stream sender error: %s", exc)

    # ------------------------------------------------------------------
    # Internal: receive loop
    # ------------------------------------------------------------------

    async def _receive_loop(self) -> None:
        """Receive messages from the Gateway and dispatch them."""
        import aiohttp
        from gateway.message_types import (
            parse_gateway_message,
            TaskMessage,
            InterruptMessage,
            ApprovedMessage,
            DeniedMessage,
        )

        if self._ws is None:
            return

        async for ws_msg in self._ws:
            if self._stop_event.is_set():
                break

            if ws_msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(ws_msg.data)
                    msg = parse_gateway_message(data)
                except Exception as exc:
                    logger.warning("Failed to parse Gateway message: %s", exc)
                    continue

                if isinstance(msg, TaskMessage):
                    event = self._task_to_message_event(msg)
                    if self._message_handler is not None:  # type: ignore[attr-defined]
                        self.handle_message(event)  # type: ignore[attr-defined]

                elif isinstance(msg, InterruptMessage):
                    logger.info("Interrupt received for group %s", self._group_id)
                    for key, event in self._active_sessions.items():  # type: ignore[attr-defined]
                        event.set()

                elif isinstance(msg, ApprovedMessage):
                    self._approval_gate.approve(msg.request_id)

                elif isinstance(msg, DeniedMessage):
                    self._approval_gate.deny(msg.request_id)

            elif ws_msg.type in (
                aiohttp.WSMsgType.ERROR,
                aiohttp.WSMsgType.CLOSE,
            ):
                logger.warning("Gateway WebSocket closed: %s", ws_msg)
                break

    # ------------------------------------------------------------------
    # Static helpers — use class-level cached Hermes classes
    # ------------------------------------------------------------------

    @staticmethod
    def _task_to_message_event(task):
        """Convert a distributed :class:`TaskMessage` to a Hermes :class:`MessageEvent`."""
        cls = InternalWSAdapter
        MessageEvent = cls._MessageEvent
        MessageType = cls._MessageType
        SessionSource = cls._SessionSource
        Platform = cls._Platform

        # Extract sender info
        sender = task.sender or {}
        user_id = sender.get("user_id", "unknown")
        user_name = sender.get("user_name", "User")

        # This adapter always presents itself as the WEBHOOK platform
        # to Hermes, regardless of the original sender's platform.
        platform = Platform("webhook")

        source = SessionSource(
            platform=platform,
            chat_id=task.group_id,
            chat_name=task.group_id,
            chat_type="group",
            user_id=user_id,
            user_name=user_name,
        )

        # Extract media URLs
        media_urls = []
        media_types = []
        for media_item in (task.media or []):
            url = media_item.get("url", "")
            if url:
                media_urls.append(url)
                media_types.append(media_item.get("type", "photo"))

        event = MessageEvent(
            text=task.message,
            message_type=MessageType("text"),
            source=source,
            raw_message=task,
            message_id=None,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=task.reply_to_message_id,
            reply_to_text=task.reply_to_text,
        )
        return event

    @staticmethod
    def _build_send_result(success, message_id=None, error=None):
        """Build a Hermes :class:`SendResult`."""
        return InternalWSAdapter._SendResult(
            success=success,
            message_id=message_id,
            error=error,
        )


def check_webhook_requirements() -> bool:
    """Check whether the environment satisfies plugin requirements.

    Returns ``True`` when aiohttp is importable (the only runtime
    dependency beyond the Hermes gateway itself).
    """
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


# Ensure stubs are loaded at module import time so tests work without
# calling _ensure_hermes_imports explicitly.
InternalWSAdapter._ensure_hermes_imports()
