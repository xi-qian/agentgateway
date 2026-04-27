"""REST client for Gateway to communicate with Manager."""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger("gateway.manager_client")


class ManagerClient:
    """Async REST client for Manager API calls."""

    def __init__(self, base_url: str, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)

    async def create_agent(
        self,
        group_id: str,
        profile: str = "default",
        model: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Request Manager to create an agent. Returns response dict or None."""
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    f"{self.base_url}/api/v1/agents",
                    json={"group_id": group_id, "profile": profile, "model": model},
                ) as resp:
                    if resp.status in (200, 201):
                        return await resp.json()
                    logger.warning("Manager create_agent returned %d for %s", resp.status, group_id)
                    return None
        except Exception as e:
            logger.error("Failed to contact Manager: %s", e)
            return None

    async def notify_disconnect(
        self, group_id: str, reason: str = "closed"
    ) -> Optional[Dict[str, Any]]:
        """Notify Manager that an agent disconnected."""
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.post(
                    f"{self.base_url}/api/v1/agents/{group_id}/disconnect",
                    json={"reason": reason},
                ) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    return None
        except Exception as e:
            logger.error("Failed to notify Manager about disconnect: %s", e)
            return None
