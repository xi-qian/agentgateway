"""Platform adapter interface for distributed Gateway."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("gateway.adapters")


@dataclass
class AdapterMessageEvent:
    """A message received from a platform adapter."""
    text: str
    group_id: str
    sender_id: str = ""
    sender_name: str = ""
    message_id: str = ""
    platform: str = ""
    media_urls: List[str] = field(default_factory=list)
    media_types: List[str] = field(default_factory=list)
    reply_to_message_id: str = ""
    reply_to_text: str = ""


class PlatformAdapter:
    """Base class for Gateway platform adapters."""

    name: str = "base"
    platform: str = "base"

    def __init__(self, config: Dict[str, Any] = None):
        self._config = config or {}
        self._running = False
        self.on_message: Optional[Callable] = None
        self.on_send: Optional[Callable] = None

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> bool:
        """Start the adapter. Return True on success."""
        self._running = True
        return True

    async def stop(self) -> None:
        """Stop the adapter."""
        self._running = False

    async def send_response(self, group_id: str, text: str) -> None:
        """Send a text response to a group. Override for real adapters."""
        if self.on_send:
            self.on_send({"group_id": group_id, "text": text})
        logger.debug("[%s] Response to %s: %s", self.name, group_id, text[:100])

    async def send_edit(self, group_id: str, text: str) -> None:
        """Edit the last message (streaming). Override for real adapters."""
        pass

    async def send_typing(self, group_id: str) -> None:
        """Send typing indicator. Override for real adapters."""
        pass

    async def _dispatch(self, event: AdapterMessageEvent) -> None:
        """Dispatch a received message to the handler."""
        if self.on_message:
            result = self.on_message(event)
            if asyncio.iscoroutine(result):
                await result
        else:
            logger.warning("[%s] Message received but no handler set", self.name)


def resolve_group_id(platform: str, source_info: Dict[str, Any]) -> str:
    """Resolve a group_id from platform-specific source info.

    Platform-specific logic:
    - Telegram: strip "-" prefix from chat_id -> "telegram:group:{id}"
    - Discord: "discord:channel:{channel_id}"
    - Slack: "slack:channel:{channel_id}"
    - Default: "{platform}:{id}"
    """
    if platform == "telegram":
        chat_id = str(source_info.get("chat_id", ""))
        return f"telegram:group:{chat_id}"
    elif platform == "discord":
        return f"discord:channel:{source_info.get('channel_id', '')}"
    elif platform == "slack":
        return f"slack:channel:{source_info.get('channel_id', '')}"
    else:
        return f"{platform}:{source_info.get('id', source_info.get('chat_id', ''))}"
