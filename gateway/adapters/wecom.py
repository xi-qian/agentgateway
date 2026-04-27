"""
WeCom (Enterprise WeChat) platform adapter.

Uses the WeCom AI Bot WebSocket gateway for inbound and outbound messages.
Core gateway path: authenticate, receive callbacks, send markdown messages.

Configuration:
    bot_id: WeCom bot ID (or WECOM_BOT_ID env var)
    secret: WeCom secret (or WECOM_SECRET env var)
    websocket_url: Override WS URL (default: wss://openws.work.weixin.qq.com)
    dm_policy: open | allowlist | disabled
    allow_from: List of allowed DM user IDs
    group_policy: open | allowlist | disabled
    group_allow_from: List of allowed group IDs
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None  # type: ignore[assignment]

from gateway.adapters.base import (
    PlatformAdapter,
    AdapterMessageEvent,
    resolve_group_id,
)

logger = logging.getLogger(__name__)

DEFAULT_WS_URL = "wss://openws.work.weixin.qq.com"

APP_CMD_SUBSCRIBE = "aibot_subscribe"
APP_CMD_CALLBACK = "aibot_msg_callback"
APP_CMD_LEGACY_CALLBACK = "aibot_callback"
APP_CMD_EVENT_CALLBACK = "aibot_event_callback"
APP_CMD_SEND = "aibot_send_msg"
APP_CMD_RESPONSE = "aibot_respond_msg"
APP_CMD_PING = "ping"

CALLBACK_COMMANDS = {APP_CMD_CALLBACK, APP_CMD_LEGACY_CALLBACK}
NON_RESPONSE_COMMANDS = CALLBACK_COMMANDS | {APP_CMD_EVENT_CALLBACK}

MAX_MESSAGE_LENGTH = 4000
CONNECT_TIMEOUT_SECONDS = 20.0
REQUEST_TIMEOUT_SECONDS = 15.0
HEARTBEAT_INTERVAL_SECONDS = 30.0
RECONNECT_BACKOFF = [2, 5, 10, 30, 60]

DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 1000


def _coerce_list(value: Any) -> List[str]:
    """Coerce config values into a trimmed string list."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()] if str(value).strip() else []


def _normalize_entry(raw: str) -> str:
    """Normalize allowlist entries such as ``wecom:user:foo``."""
    value = str(raw).strip()
    value = re.sub(r"^wecom:", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^(user|group):", "", value, flags=re.IGNORECASE)
    return value.strip()


def _entry_matches(entries: List[str], target: str) -> bool:
    """Case-insensitive allowlist match with ``*`` support."""
    normalized_target = str(target).strip().lower()
    for entry in entries:
        normalized = _normalize_entry(entry).lower()
        if normalized == "*" or normalized == normalized_target:
            return True
    return False


class WeComAdapter(PlatformAdapter):
    """WeCom AI Bot adapter backed by a persistent WebSocket connection."""

    name = "wecom"
    platform = "wecom"
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._bot_id = str(self._config.get("bot_id") or os.getenv("WECOM_BOT_ID", "")).strip()
        self._secret = str(self._config.get("secret") or os.getenv("WECOM_SECRET", "")).strip()
        self._ws_url = str(
            self._config.get("websocket_url")
            or self._config.get("websocketUrl")
            or os.getenv("WECOM_WEBSOCKET_URL", DEFAULT_WS_URL)
        ).strip() or DEFAULT_WS_URL

        self._dm_policy = str(self._config.get("dm_policy") or os.getenv("WECOM_DM_POLICY", "open")).strip().lower()
        self._allow_from = _coerce_list(self._config.get("allow_from") or self._config.get("allowFrom"))

        self._group_policy = str(self._config.get("group_policy") or os.getenv("WECOM_GROUP_POLICY", "open")).strip().lower()
        self._group_allow_from = _coerce_list(self._config.get("group_allow_from") or self._config.get("groupAllowFrom"))
        self._groups = self._config.get("groups") if isinstance(self._config.get("groups"), dict) else {}

        self._session: Optional["aiohttp.ClientSession"] = None
        self._ws: Optional["aiohttp.ClientWebSocketResponse"] = None
        self._listen_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._pending_responses: Dict[str, asyncio.Future] = {}
        self._seen_messages: Dict[str, float] = {}
        self._reply_req_ids: Dict[str, str] = {}

    @staticmethod
    def check_requirements() -> bool:
        """Check if WeCom runtime dependencies are available."""
        return AIOHTTP_AVAILABLE

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        """Connect to the WeCom AI Bot gateway."""
        if not AIOHTTP_AVAILABLE:
            logger.warning("[%s] aiohttp not installed. Run: pip install aiohttp", self.name)
            return False
        if not self._bot_id or not self._secret:
            logger.warning("[%s] WECOM_BOT_ID and WECOM_SECRET are required", self.name)
            return False

        try:
            await self._open_connection()
            self._running = True
            self._listen_task = asyncio.create_task(self._listen_loop())
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("[%s] Connected to %s", self.name, self._ws_url)
            return True
        except Exception as exc:
            logger.error("[%s] Failed to connect: %s", self.name, exc, exc_info=True)
            await self._cleanup_ws()
            return False

    async def stop(self) -> None:
        """Disconnect from WeCom."""
        self._running = False

        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
            self._heartbeat_task = None

        self._fail_pending_responses(RuntimeError("WeCom adapter disconnected"))
        await self._cleanup_ws()
        self._seen_messages.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _cleanup_ws(self) -> None:
        """Close the live websocket/session, if any."""
        if self._ws and not self._ws.closed:
            await self._ws.close()
        self._ws = None

        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _open_connection(self) -> None:
        """Open and authenticate a websocket connection."""
        await self._cleanup_ws()
        self._session = aiohttp.ClientSession()
        self._ws = await self._session.ws_connect(
            self._ws_url,
            heartbeat=HEARTBEAT_INTERVAL_SECONDS * 2,
            timeout=CONNECT_TIMEOUT_SECONDS,
        )

        req_id = self._new_req_id("subscribe")
        await self._send_json(
            {
                "cmd": APP_CMD_SUBSCRIBE,
                "headers": {"req_id": req_id},
                "body": {"bot_id": self._bot_id, "secret": self._secret},
            }
        )

        auth_payload = await self._wait_for_handshake(req_id)
        errcode = auth_payload.get("errcode", 0)
        if errcode not in (0, None):
            errmsg = auth_payload.get("errmsg", "authentication failed")
            raise RuntimeError(f"{errmsg} (errcode={errcode})")

    async def _wait_for_handshake(self, req_id: str) -> Dict[str, Any]:
        """Wait for the subscribe acknowledgement."""
        if not self._ws:
            raise RuntimeError("WebSocket not initialized")

        deadline = asyncio.get_running_loop().time() + CONNECT_TIMEOUT_SECONDS
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise TimeoutError("Timed out waiting for WeCom subscribe acknowledgement")

            msg = await asyncio.wait_for(self._ws.receive(), timeout=remaining)
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = self._parse_json(msg.data)
                if not payload:
                    continue
                if payload.get("cmd") == APP_CMD_PING:
                    continue
                if self._payload_req_id(payload) == req_id:
                    return payload
                logger.debug("[%s] Ignoring pre-auth payload: %s", self.name, payload.get("cmd"))
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("WeCom websocket closed during authentication")

    async def _listen_loop(self) -> None:
        """Read websocket events forever, reconnecting on errors."""
        backoff_idx = 0
        while self._running:
            try:
                await self._read_events()
                backoff_idx = 0
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                logger.warning("[%s] WebSocket error: %s", self.name, exc)
                self._fail_pending_responses(RuntimeError("WeCom connection interrupted"))

                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                backoff_idx += 1
                await asyncio.sleep(delay)

                try:
                    await self._open_connection()
                    backoff_idx = 0
                    logger.info("[%s] Reconnected", self.name)
                except Exception as reconnect_exc:
                    logger.warning("[%s] Reconnect failed: %s", self.name, reconnect_exc)

    async def _read_events(self) -> None:
        """Read websocket frames until the connection closes."""
        if not self._ws:
            raise RuntimeError("WebSocket not connected")

        while self._running and self._ws and not self._ws.closed:
            msg = await self._ws.receive()
            if msg.type == aiohttp.WSMsgType.TEXT:
                payload = self._parse_json(msg.data)
                if payload:
                    await self._dispatch_payload(payload)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                raise RuntimeError("WeCom websocket closed")

    async def _heartbeat_loop(self) -> None:
        """Send lightweight application-level pings."""
        try:
            while self._running:
                await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)
                if not self._ws or self._ws.closed:
                    continue
                try:
                    await self._send_json(
                        {
                            "cmd": APP_CMD_PING,
                            "headers": {"req_id": self._new_req_id("ping")},
                            "body": {},
                        }
                    )
                except Exception as exc:
                    logger.debug("[%s] Heartbeat send failed: %s", self.name, exc)
        except asyncio.CancelledError:
            pass

    async def _dispatch_payload(self, payload: Dict[str, Any]) -> None:
        """Route inbound websocket payloads."""
        req_id = self._payload_req_id(payload)
        cmd = str(payload.get("cmd") or "")

        if req_id and req_id in self._pending_responses and cmd not in NON_RESPONSE_COMMANDS:
            future = self._pending_responses.get(req_id)
            if future and not future.done():
                future.set_result(payload)
            return

        if cmd in CALLBACK_COMMANDS:
            await self._on_message(payload)
            return
        if cmd in {APP_CMD_PING, APP_CMD_EVENT_CALLBACK}:
            return

        logger.debug("[%s] Ignoring websocket payload: %s", self.name, cmd or payload)

    def _fail_pending_responses(self, exc: Exception) -> None:
        """Fail all outstanding request futures."""
        for req_id, future in list(self._pending_responses.items()):
            if not future.done():
                future.set_exception(exc)
            self._pending_responses.pop(req_id, None)

    async def _send_json(self, payload: Dict[str, Any]) -> None:
        """Send a raw JSON frame over the active websocket."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")
        await self._ws.send_json(payload)

    async def _send_request(self, cmd: str, body: Dict[str, Any], timeout: float = REQUEST_TIMEOUT_SECONDS) -> Dict[str, Any]:
        """Send a JSON request and await the correlated response."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")

        req_id = self._new_req_id(cmd)
        future = asyncio.get_running_loop().create_future()
        self._pending_responses[req_id] = future
        try:
            await self._send_json({"cmd": cmd, "headers": {"req_id": req_id}, "body": body})
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        finally:
            self._pending_responses.pop(req_id, None)

    async def _send_reply_request(
        self,
        reply_req_id: str,
        body: Dict[str, Any],
        cmd: str = APP_CMD_RESPONSE,
        timeout: float = REQUEST_TIMEOUT_SECONDS,
    ) -> Dict[str, Any]:
        """Send a reply frame correlated to an inbound callback req_id."""
        if not self._ws or self._ws.closed:
            raise RuntimeError("WeCom websocket is not connected")

        normalized_req_id = str(reply_req_id or "").strip()
        if not normalized_req_id:
            raise ValueError("reply_req_id is required")

        future = asyncio.get_running_loop().create_future()
        self._pending_responses[normalized_req_id] = future
        try:
            await self._send_json(
                {"cmd": cmd, "headers": {"req_id": normalized_req_id}, "body": body}
            )
            response = await asyncio.wait_for(future, timeout=timeout)
            return response
        finally:
            self._pending_responses.pop(normalized_req_id, None)

    @staticmethod
    def _new_req_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex}"

    @staticmethod
    def _payload_req_id(payload: Dict[str, Any]) -> str:
        headers = payload.get("headers")
        if isinstance(headers, dict):
            return str(headers.get("req_id") or "")
        return ""

    @staticmethod
    def _parse_json(raw: Any) -> Optional[Dict[str, Any]]:
        try:
            payload = json.loads(raw)
        except Exception:
            logger.debug("Failed to parse WeCom payload: %r", raw)
            return None
        return payload if isinstance(payload, dict) else None

    # ------------------------------------------------------------------
    # Inbound message parsing
    # ------------------------------------------------------------------

    async def _on_message(self, payload: Dict[str, Any]) -> None:
        """Process an inbound WeCom message callback event."""
        body = payload.get("body")
        if not isinstance(body, dict):
            return

        msg_id = str(body.get("msgid") or self._payload_req_id(payload) or uuid.uuid4().hex)
        if self._is_duplicate(msg_id):
            logger.debug("[%s] Duplicate message %s ignored", self.name, msg_id)
            return
        self._remember_reply_req_id(msg_id, self._payload_req_id(payload))

        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        sender_id = str(sender.get("userid") or "").strip()
        chat_id = str(body.get("chatid") or sender_id).strip()
        if not chat_id:
            logger.debug("[%s] Missing chat id, skipping message", self.name)
            return

        is_group = str(body.get("chattype") or "").lower() == "group"
        if is_group:
            if not self._is_group_allowed(chat_id, sender_id):
                logger.debug("[%s] Group %s / sender %s blocked by policy", self.name, chat_id, sender_id)
                return
        elif not self._is_dm_allowed(sender_id):
            logger.debug("[%s] DM sender %s blocked by policy", self.name, sender_id)
            return

        text, reply_text = self._extract_text(body)
        has_reply_context = bool(reply_text and text)

        if not text and reply_text:
            text = reply_text

        if not text:
            logger.debug("[%s] Empty WeCom message skipped", self.name)
            return

        group_id = resolve_group_id("wecom", {"chat_id": chat_id})

        event = AdapterMessageEvent(
            text=text,
            group_id=group_id,
            sender_id=sender_id,
            message_id=msg_id,
            platform="wecom",
            reply_to_message_id=f"quote:{msg_id}" if has_reply_context else "",
            reply_to_text=reply_text if has_reply_context else "",
        )

        await self._dispatch(event)

    @staticmethod
    def _extract_text(body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        """Extract plain text and quoted text from a callback payload."""
        text_parts: List[str] = []
        reply_text: Optional[str] = None
        msgtype = str(body.get("msgtype") or "").lower()

        if msgtype == "mixed":
            mixed = body.get("mixed") if isinstance(body.get("mixed"), dict) else {}
            items = mixed.get("msg_item") if isinstance(mixed.get("msg_item"), list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("msgtype") or "").lower() == "text":
                    text_block = item.get("text") if isinstance(item.get("text"), dict) else {}
                    content = str(text_block.get("content") or "").strip()
                    if content:
                        text_parts.append(content)
        else:
            text_block = body.get("text") if isinstance(body.get("text"), dict) else {}
            content = str(text_block.get("content") or "").strip()
            if content:
                text_parts.append(content)

            if msgtype == "voice":
                voice_block = body.get("voice") if isinstance(body.get("voice"), dict) else {}
                voice_text = str(voice_block.get("content") or "").strip()
                if voice_text:
                    text_parts.append(voice_text)

        quote = body.get("quote") if isinstance(body.get("quote"), dict) else {}
        quote_type = str(quote.get("msgtype") or "").lower()
        if quote_type == "text":
            quote_text = quote.get("text") if isinstance(quote.get("text"), dict) else {}
            reply_text = str(quote_text.get("content") or "").strip() or None
        elif quote_type == "voice":
            quote_voice = quote.get("voice") if isinstance(quote.get("voice"), dict) else {}
            reply_text = str(quote_voice.get("content") or "").strip() or None

        return "\n".join(part for part in text_parts if part).strip(), reply_text

    # ------------------------------------------------------------------
    # Policy helpers
    # ------------------------------------------------------------------

    def _is_dm_allowed(self, sender_id: str) -> bool:
        if self._dm_policy == "disabled":
            return False
        if self._dm_policy == "allowlist":
            return _entry_matches(self._allow_from, sender_id)
        return True

    def _is_group_allowed(self, chat_id: str, sender_id: str) -> bool:
        if self._group_policy == "disabled":
            return False
        if self._group_policy == "allowlist" and not _entry_matches(self._group_allow_from, chat_id):
            return False

        group_cfg = self._resolve_group_cfg(chat_id)
        sender_allow = _coerce_list(group_cfg.get("allow_from") or group_cfg.get("allowFrom"))
        if sender_allow:
            return _entry_matches(sender_allow, sender_id)
        return True

    def _resolve_group_cfg(self, chat_id: str) -> Dict[str, Any]:
        if not isinstance(self._groups, dict):
            return {}
        if chat_id in self._groups and isinstance(self._groups[chat_id], dict):
            return self._groups[chat_id]
        lowered = chat_id.lower()
        for key, value in self._groups.items():
            if isinstance(key, str) and key.lower() == lowered and isinstance(value, dict):
                return value
        wildcard = self._groups.get("*")
        return wildcard if isinstance(wildcard, dict) else {}

    def _is_duplicate(self, msg_id: str) -> bool:
        now = time.time()
        if len(self._seen_messages) > DEDUP_MAX_SIZE:
            cutoff = now - DEDUP_WINDOW_SECONDS
            self._seen_messages = {
                key: ts for key, ts in self._seen_messages.items() if ts > cutoff
            }
            if self._reply_req_ids:
                self._reply_req_ids = {
                    key: value for key, value in self._reply_req_ids.items() if key in self._seen_messages
                }

        if msg_id in self._seen_messages:
            return True

        self._seen_messages[msg_id] = now
        return False

    def _remember_reply_req_id(self, message_id: str, req_id: str) -> None:
        normalized_message_id = str(message_id or "").strip()
        normalized_req_id = str(req_id or "").strip()
        if not normalized_message_id or not normalized_req_id:
            return
        self._reply_req_ids[normalized_message_id] = normalized_req_id
        while len(self._reply_req_ids) > DEDUP_MAX_SIZE:
            self._reply_req_ids.pop(next(iter(self._reply_req_ids)))

    def _reply_req_id_for_message(self, reply_to: Optional[str]) -> Optional[str]:
        normalized = str(reply_to or "").strip()
        if not normalized or normalized.startswith("quote:"):
            return None
        return self._reply_req_ids.get(normalized)

    # ------------------------------------------------------------------
    # Outbound messaging
    # ------------------------------------------------------------------

    async def send_response(self, group_id: str, text: str) -> None:
        """Send markdown to a WeCom chat via aibot_send_msg."""
        # Extract chat_id from group_id (last segment after :)
        chat_id = group_id.rsplit(":", 1)[-1] if ":" in group_id else group_id

        if not chat_id:
            logger.error("[%s] Cannot send: no chat_id", self.name)
            return

        try:
            response = await self._send_request(
                APP_CMD_SEND,
                {
                    "chatid": chat_id,
                    "msgtype": "markdown",
                    "markdown": {"content": text[:self.MAX_MESSAGE_LENGTH]},
                },
            )
            errcode = response.get("errcode", 0)
            if errcode not in (0, None):
                errmsg = response.get("errmsg", "unknown error")
                logger.warning("[%s] Send failed: errcode=%s errmsg=%s", self.name, errcode, errmsg)
        except asyncio.TimeoutError:
            logger.error("[%s] Timeout sending message to WeCom", self.name)
        except Exception as exc:
            logger.error("[%s] Send failed: %s", self.name, exc)
