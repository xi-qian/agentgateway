"""Response bridge -- routes agent responses back to platform adapters."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Dict

logger = logging.getLogger("gateway.bridge")


class Bridge:
    """Routes agent messages back to the appropriate platform adapter.

    Platform adapters register response handlers per group_id. When an
    agent sends a message (stream, complete, error, approval_request,
    progress), the bridge dispatches it to the registered handler.
    """

    def __init__(self, router=None):
        self._router = router
        self._response_handlers: Dict[str, Callable] = {}
        self._approval_callbacks: Dict[str, Callable] = {}

    def register_response_handler(self, group_id: str, handler: Callable) -> None:
        """Register a coroutine handler for agent responses on a group_id."""
        self._response_handlers[group_id] = handler

    def register_approval_callback(self, group_id: str, callback: Callable) -> None:
        """Register a coroutine callback for approval requests on a group_id."""
        self._approval_callbacks[group_id] = callback

    def cleanup_agent(self, group_id: str) -> None:
        """Remove all handlers and callbacks for a disconnected agent."""
        self._response_handlers.pop(group_id, None)
        self._approval_callbacks.pop(group_id, None)

    async def on_agent_connected(self, group_id: str) -> None:
        """Called when a new agent connects; drains any pending task queue."""
        if self._router:
            queue = self._router.get_pending_queue(group_id)
            ws = self._router.get_agent(group_id)
            if queue and ws:
                while True:
                    try:
                        task_dict = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    await ws.send_json(task_dict)

    async def handle_agent_message(self, msg: Any, group_id: str) -> None:
        """Dispatch an agent message to the appropriate handler."""
        from gateway.message_types import (
            StreamMessage,
            CompleteMessage,
            ErrorMessage,
            ApprovalRequestMessage,
            ProgressMessage,
            HeartbeatMessage,
        )

        if isinstance(msg, HeartbeatMessage):
            return
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
            logger.debug(
                "Stream from %s (no handler): %s",
                group_id,
                msg.token[:50],
            )

    async def _on_complete(self, group_id: str, msg) -> None:
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)
        else:
            logger.info(
                "Complete from %s: %s", group_id, msg.final_response[:200]
            )

    async def _on_error(self, group_id: str, msg) -> None:
        logger.error(
            "Error from agent %s (fatal=%s): %s",
            group_id,
            msg.fatal,
            msg.message,
        )
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)

    async def _on_approval_request(self, group_id: str, msg) -> None:
        logger.info(
            "Approval request from agent %s: %s -- %s",
            group_id,
            msg.command,
            msg.reason,
        )
        callback = self._approval_callbacks.get(group_id)
        if callback:
            await callback(msg)

    async def _on_progress(self, group_id: str, msg) -> None:
        logger.info(
            "Progress from agent %s [%s]: %s",
            group_id,
            msg.emoji,
            msg.preview,
        )
        handler = self._response_handlers.get(group_id)
        if handler:
            await handler(msg)
