"""Mock platform adapter for testing."""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict

from gateway.adapters.base import PlatformAdapter, AdapterMessageEvent

logger = logging.getLogger("gateway.adapters.mock")


class MockAdapter(PlatformAdapter):
    """Test adapter that allows programmatic message injection."""

    name = "mock"
    platform = "mock"

    async def send_test_message(
        self, group_id: str, text: str, sender_id: str = "test",
        sender_name: str = "TestUser", message_id: str = "msg_test",
        media_urls: list = None, reply_to_message_id: str = "",
    ) -> None:
        """Inject a test message into the adapter."""
        event = AdapterMessageEvent(
            text=text,
            group_id=group_id,
            sender_id=sender_id,
            sender_name=sender_name,
            message_id=message_id,
            platform=self.platform,
            media_urls=media_urls or [],
            reply_to_message_id=reply_to_message_id,
        )
        await self._dispatch(event)
