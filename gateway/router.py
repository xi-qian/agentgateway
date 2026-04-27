"""Message router -- maps group_id to Agent WebSocket connections."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("gateway.router")


class Router:
    """Routes tasks to the correct agent WebSocket by group_id.

    Each agent registers with a group_id when it connects. The router
    maintains a mapping and dispatches tasks. If no agent is available,
    tasks can be queued for later delivery.
    """

    def __init__(self, config=None):
        self._agents: Dict[str, Any] = {}
        self._pending_tasks: Dict[str, asyncio.Queue] = {}
        self._manager_client = None

    def register_agent(self, group_id: str, ws) -> None:
        """Register a WebSocket connection for a group_id."""
        old_ws = self._agents.get(group_id)
        if old_ws is not None and old_ws is not ws:
            logger.warning(
                "Replacing existing agent for group_id=%s", group_id,
            )
            try:
                old_ws.close()
            except Exception:
                pass
        self._agents[group_id] = ws
        if group_id in self._pending_tasks:
            logger.info(
                "Agent registered for group_id=%s, draining %d pending tasks",
                group_id,
                self._pending_tasks[group_id].qsize(),
            )

    def remove_agent(self, group_id: str) -> None:
        """Remove the agent mapping for a group_id."""
        self._agents.pop(group_id, None)

    def get_agent(self, group_id: str):
        """Return the WebSocket for a group_id, or None."""
        return self._agents.get(group_id)

    def set_manager_client(self, client):
        """Set the Manager REST client for cold-path agent creation."""
        self._manager_client = client

    def has_agent(self, group_id: str) -> bool:
        """Check whether an agent is registered for a group_id."""
        return group_id in self._agents

    async def dispatch_task(self, group_id: str, task_dict: dict) -> bool:
        """Send a task to the agent for group_id.

        If no agent is available and a manager client is configured, request
        the manager to create one (cold path). Returns True if the task was
        sent directly, False otherwise.
        """
        ws = self._agents.get(group_id)
        if ws is not None:
            try:
                await ws.send_json(task_dict)
                return True
            except Exception as e:
                logger.error("Failed to send task to agent %s: %s", group_id, e)
                self.remove_agent(group_id)

        # Cold path: no agent available
        if self._manager_client is not None:
            logger.info("No agent for %s, requesting from Manager", group_id)
            result = await self._manager_client.create_agent(group_id)
            if result:
                queue = self.get_pending_queue(group_id)
                try:
                    queue.put_nowait(task_dict)
                    logger.info("Task queued for %s (Manager status: %s)", group_id, result.get("status"))
                except asyncio.QueueFull:
                    logger.warning("Pending queue full for %s, dropping task", group_id)
                    return False

        return False

    def get_pending_queue(self, group_id: str, maxsize: int = 10) -> asyncio.Queue:
        """Get or create a pending task queue for a group_id."""
        if group_id not in self._pending_tasks:
            self._pending_tasks[group_id] = asyncio.Queue(maxsize=maxsize)
        return self._pending_tasks[group_id]

    def remove_pending_queue(self, group_id: str) -> Optional[asyncio.Queue]:
        """Remove and return the pending queue for a group_id."""
        return self._pending_tasks.pop(group_id, None)
