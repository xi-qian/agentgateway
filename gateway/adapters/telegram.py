"""Telegram platform adapter for distributed Gateway.

Simplified version of Hermes's Telegram adapter. Uses python-telegram-bot
for receiving messages and sending responses.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from gateway.adapters.base import (
    PlatformAdapter, AdapterMessageEvent, resolve_group_id,
)

logger = logging.getLogger("gateway.adapters.telegram")

try:
    import telegram
    from telegram import Update
    from telegram.ext import Application, MessageHandler, filters, ContextTypes
    HAS_TELEGRAM = True
except ImportError:
    HAS_TELEGRAM = False
    logger.warning("python-telegram-bot not installed -- Telegram adapter disabled")


class TelegramAdapter(PlatformAdapter):
    """Telegram platform adapter."""

    name = "telegram"
    platform = "telegram"

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._token = self._config.get("token", "")
        self._app: Optional[Any] = None

    async def start(self) -> bool:
        if not HAS_TELEGRAM or not self._token:
            logger.warning("Telegram adapter: missing token or library")
            return False
        self._app = Application.builder().token(self._token).build()
        self._app.add_handler(MessageHandler(
            filters.ALL & ~filters.COMMAND, self._handle_message,
        ))
        await self._app.initialize()
        await self._app.start()
        self._running = True
        logger.info("Telegram adapter started")
        return True

    async def stop(self) -> None:
        if self._app:
            await self._app.stop()
            await self._app.shutdown()
        self._running = False

    async def _handle_message(self, update: Update, context: ContextTypes) -> None:
        """Handle incoming Telegram message."""
        msg = update.effective_message
        if not msg:
            return

        group_id = resolve_group_id("telegram", {"chat_id": str(msg.chat.id)})

        media_urls = []
        media_types = []
        if msg.photo:
            photo = msg.photo[-1]  # highest resolution
            file = await photo.get_file()
            media_urls.append(file.file_path)
            media_types.append("photo")
        elif msg.document:
            file = await msg.document.get_file()
            media_urls.append(file.file_path)
            media_types.append("document")

        event = AdapterMessageEvent(
            text=msg.text or "",
            group_id=group_id,
            sender_id=str(msg.from_user.id) if msg.from_user else "",
            sender_name=msg.from_user.full_name if msg.from_user else "",
            message_id=str(msg.message_id),
            platform=self.platform,
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=str(msg.reply_to_message.message_id) if msg.reply_to_message else "",
            reply_to_text=msg.reply_to_message.text if msg.reply_to_message else "",
        )
        await self._dispatch(event)

    async def send_response(self, group_id: str, text: str) -> None:
        """Send a text response to the Telegram chat."""
        if not self._app:
            return
        # Extract chat_id from group_id
        parts = group_id.split(":")
        chat_id = int(parts[-1]) if len(parts) > 1 else int(group_id)
        try:
            await self._app.bot.send_message(chat_id=chat_id, text=text)
        except Exception as e:
            logger.error("Failed to send Telegram response: %s", e)

    async def send_typing(self, group_id: str) -> None:
        if not self._app:
            return
        parts = group_id.split(":")
        chat_id = int(parts[-1]) if len(parts) > 1 else int(group_id)
        try:
            await self._app.bot.send_chat_action(chat_id=chat_id, action="typing")
        except Exception as e:
            logger.error("Failed to send typing: %s", e)

    @staticmethod
    def check_requirements() -> bool:
        return HAS_TELEGRAM
