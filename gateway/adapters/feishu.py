"""
Feishu/Lark platform adapter.

Supports:
- WebSocket long connection and Webhook transport
- Direct-message and group @mention-gated text receive/send
- Inbound rich text (post) parsing
- Card button-click events routed as synthetic text events
- Message deduplication

Configuration:
    app_id: Feishu app ID (or FEISHU_APP_ID env var)
    app_secret: Feishu app secret (or FEISHU_APP_SECRET env var)
    connection_mode: websocket | webhook (default: websocket)
    webhook_host: Webhook bind host (default: 127.0.0.1)
    webhook_port: Webhook bind port (default: 8765)
    webhook_path: Webhook URL path (default: /feishu/webhook)
    domain: feishu | lark (default: feishu)
    bot_open_id: Bot open ID for mention detection
    bot_user_id: Bot user ID for mention detection
    bot_name: Bot name for mention detection
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import aiohttp
    from aiohttp import web
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    web = None  # type: ignore[assignment]

try:
    import websockets
except ImportError:
    websockets = None  # type: ignore[assignment]

try:
    import lark_oapi as lark
    from lark_oapi.api.im.v1 import (
        CreateMessageRequest,
        CreateMessageRequestBody,
        ReplyMessageRequest,
        ReplyMessageRequestBody,
    )
    from lark_oapi.core.const import FEISHU_DOMAIN, LARK_DOMAIN
    from lark_oapi.event.callback.model.p2_card_action_trigger import P2CardActionTriggerResponse
    from lark_oapi.event.dispatcher_handler import EventDispatcherHandler
    from lark_oapi.ws import Client as FeishuWSClient

    FEISHU_AVAILABLE = True
except ImportError:
    FEISHU_AVAILABLE = False
    lark = None  # type: ignore[assignment]
    P2CardActionTriggerResponse = None  # type: ignore[assignment]
    EventDispatcherHandler = None  # type: ignore[assignment]
    FeishuWSClient = None  # type: ignore[assignment]
    FEISHU_DOMAIN = None  # type: ignore[assignment]
    LARK_DOMAIN = None  # type: ignore[assignment]

FEISHU_WEBSOCKET_AVAILABLE = websockets is not None
FEISHU_WEBHOOK_AVAILABLE = aiohttp is not None

from gateway.adapters.base import (
    PlatformAdapter,
    AdapterMessageEvent,
    resolve_group_id,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_MESSAGE_LENGTH = 8000
FEISHU_CONNECT_ATTEMPTS = 3
FEISHU_SEND_ATTEMPTS = 3
DEDUP_CACHE_SIZE = 2048
DEDUP_TTL_SECONDS = 24 * 60 * 60
DEFAULT_WEBHOOK_HOST = "127.0.0.1"
DEFAULT_WEBHOOK_PORT = 8765
DEFAULT_WEBHOOK_PATH = "/feishu/webhook"
CARD_ACTION_DEDUP_TTL_SECONDS = 15 * 60
FEISHU_REPLY_FALLBACK_CODES = frozenset({230011, 231003})

FALLBACK_POST_TEXT = "[Rich text message]"
FALLBACK_FORWARD_TEXT = "[Merged forward message]"
FALLBACK_SHARE_CHAT_TEXT = "[Shared chat]"
FALLBACK_INTERACTIVE_TEXT = "[Interactive message]"
FALLBACK_IMAGE_TEXT = "[Image]"
FALLBACK_ATTACHMENT_TEXT = "[Attachment]"

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

_MARKDOWN_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_MARKDOWN_SPECIAL_CHARS_RE = re.compile(r"([\\`*_{}\[\]()#+\-!|>~])")
_MENTION_PLACEHOLDER_RE = re.compile(r"@_user_\d+")
_WHITESPACE_RE = re.compile(r"\s+")
_MULTISPACE_RE = re.compile(r"[ \t]{2,}")
_POST_CONTENT_INVALID_RE = re.compile(r"content format of the post type is incorrect", re.IGNORECASE)

_PREFERRED_LOCALES = ("zh_cn", "en_us")
_SUPPORTED_CARD_TEXT_KEYS = (
    "title", "text", "content", "label", "value", "name",
    "summary", "subtitle", "description", "placeholder", "hint",
)
_SKIP_TEXT_KEYS = {
    "tag", "type", "msg_type", "message_type", "chat_id", "open_chat_id",
    "share_chat_id", "file_key", "image_key", "user_id", "open_id",
    "union_id", "url", "href", "link", "token", "template", "locale",
}

# ---------------------------------------------------------------------------
# Post parsing dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FeishuPostMediaRef:
    file_key: str
    file_name: str = ""
    resource_type: str = "file"


@dataclass(frozen=True)
class FeishuPostParseResult:
    text_content: str
    image_keys: List[str] = field(default_factory=list)
    media_refs: List[FeishuPostMediaRef] = field(default_factory=list)
    mentioned_ids: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class FeishuNormalizedMessage:
    raw_type: str
    text_content: str
    preferred_message_type: str = "text"
    image_keys: List[str] = field(default_factory=list)
    media_refs: List[FeishuPostMediaRef] = field(default_factory=list)
    mentioned_ids: List[str] = field(default_factory=list)
    relation_kind: str = "plain"
    metadata: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Markdown rendering helpers
# ---------------------------------------------------------------------------


def _escape_markdown_text(text: str) -> str:
    return _MARKDOWN_SPECIAL_CHARS_RE.sub(r"\\\1", text)


def _is_style_enabled(style: Optional[Dict[str, Any]], key: str) -> bool:
    if not style:
        return False
    return style.get(key) is True or style.get(key) == 1 or style.get(key) == "true"


def _wrap_inline_code(text: str) -> str:
    max_run = max([0, *[len(run) for run in re.findall(r"`+", text)]])
    fence = "`" * (max_run + 1)
    body = f" {text} " if text.startswith("`") or text.endswith("`") else text
    return f"{fence}{body}{fence}"


def _render_text_element(element: Dict[str, Any]) -> str:
    text = str(element.get("text", "") or "")
    style = element.get("style")
    style_dict = style if isinstance(style, dict) else None

    if _is_style_enabled(style_dict, "code"):
        return _wrap_inline_code(text)

    rendered = _escape_markdown_text(text)
    if not rendered:
        return ""
    if _is_style_enabled(style_dict, "bold"):
        rendered = f"**{rendered}**"
    if _is_style_enabled(style_dict, "italic"):
        rendered = f"*{rendered}*"
    if _is_style_enabled(style_dict, "underline"):
        rendered = f"<u>{rendered}</u>"
    if _is_style_enabled(style_dict, "strikethrough"):
        rendered = f"~~{rendered}~~"
    return rendered


def _strip_markdown_to_plain_text(text: str) -> str:
    plain = text.replace("\r\n", "\n")
    plain = _MARKDOWN_LINK_RE.sub(lambda m: f"{m.group(1)} ({m.group(2).strip()})", plain)
    plain = re.sub(r"^#{1,6}\s+", "", plain, flags=re.MULTILINE)
    plain = re.sub(r"```(?:[^\n]*\n)?([\s\S]*?)```", lambda m: m.group(1).strip("\n"), plain)
    plain = re.sub(r"`([^`\n]+)`", r"\1", plain)
    plain = re.sub(r"\*\*([^*\n]+)\*\*", r"\1", plain)
    plain = re.sub(r"\*([^*\n]+)\*", r"\1", plain)
    plain = re.sub(r"~~([^~\n]+)~~", r"\1", plain)
    plain = re.sub(r"<u>([\s\S]*?)</u>", r"\1", plain)
    plain = re.sub(r"\n{3,}", "\n\n", plain)
    return plain.strip()


def _normalize_feishu_text(text: str) -> str:
    cleaned = _MENTION_PLACEHOLDER_RE.sub(" ", text or "")
    cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
    cleaned = "\n".join(_WHITESPACE_RE.sub(" ", line).strip() for line in cleaned.split("\n"))
    cleaned = "\n".join(line for line in cleaned.split("\n") if line)
    cleaned = _MULTISPACE_RE.sub(" ", cleaned)
    return cleaned.strip()


def _unique_lines(lines: List[str]) -> List[str]:
    seen = set()  # type: ignore[var-annotated]
    unique: List[str] = []
    for line in lines:
        if not line or line in seen:
            continue
        seen.add(line)
        unique.append(line)
    return unique


# ---------------------------------------------------------------------------
# Post payload parsers
# ---------------------------------------------------------------------------


def _build_markdown_post_payload(content: str) -> str:
    return json.dumps(
        {
            "zh_cn": {
                "content": [
                    [{"tag": "md", "text": content}]
                ]
            }
        },
        ensure_ascii=False,
    )


def parse_feishu_post_content(raw_content: str) -> FeishuPostParseResult:
    try:
        parsed = json.loads(raw_content) if raw_content else {}
    except json.JSONDecodeError:
        return FeishuPostParseResult(text_content=FALLBACK_POST_TEXT)
    return parse_feishu_post_payload(parsed)


def parse_feishu_post_payload(payload: Any) -> FeishuPostParseResult:
    resolved = _resolve_post_payload(payload)
    if not resolved:
        return FeishuPostParseResult(text_content=FALLBACK_POST_TEXT)

    image_keys: List[str] = []
    media_refs: List[FeishuPostMediaRef] = []
    mentioned_ids: List[str] = []
    parts: List[str] = []

    title = _normalize_feishu_text(str(resolved.get("title", "")).strip())
    if title:
        parts.append(title)

    for row in resolved.get("content", []) or []:
        if not isinstance(row, list):
            continue
        row_text = _normalize_feishu_text(
            "".join(_render_post_element(item, image_keys, media_refs, mentioned_ids) for item in row)
        )
        if row_text:
            parts.append(row_text)

    return FeishuPostParseResult(
        text_content="\n".join(parts).strip() or FALLBACK_POST_TEXT,
        image_keys=image_keys,
        media_refs=media_refs,
        mentioned_ids=mentioned_ids,
    )


def _resolve_post_payload(payload: Any) -> Optional[Dict[str, Any]]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return None

    wrapped = payload.get("post")
    wrapped_direct = _resolve_locale_payload(wrapped)
    if wrapped_direct:
        return wrapped_direct
    return _resolve_locale_payload(payload)


def _resolve_locale_payload(payload: Any) -> Optional[Dict[str, Any]]:
    direct = _to_post_payload(payload)
    if direct:
        return direct
    if not isinstance(payload, dict):
        return None

    for key in _PREFERRED_LOCALES:
        candidate = _to_post_payload(payload.get(key))
        if candidate:
            return candidate
    for value in payload.values():
        candidate = _to_post_payload(value)
        if candidate:
            return candidate
    return None


def _to_post_payload(candidate: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(candidate, dict):
        return None
    content = candidate.get("content")
    if not isinstance(content, list):
        return None
    return {
        "title": str(candidate.get("title", "") or ""),
        "content": content,
    }


def _render_post_element(
    element: Any,
    image_keys: List[str],
    media_refs: List[FeishuPostMediaRef],
    mentioned_ids: List[str],
) -> str:
    if isinstance(element, str):
        return element
    if not isinstance(element, dict):
        return ""

    tag = str(element.get("tag", "")).strip().lower()
    if tag == "text":
        return _render_text_element(element)
    if tag == "a":
        href = str(element.get("href", "")).strip()
        label = str(element.get("text", href) or "").strip()
        if not label:
            return ""
        escaped_label = _escape_markdown_text(label)
        return f"[{escaped_label}]({href})" if href else escaped_label
    if tag == "at":
        mentioned_id = (
            str(element.get("open_id", "")).strip()
            or str(element.get("user_id", "")).strip()
        )
        if mentioned_id and mentioned_id not in mentioned_ids:
            mentioned_ids.append(mentioned_id)
        display_name = (
            str(element.get("user_name", "")).strip()
            or str(element.get("name", "")).strip()
            or str(element.get("text", "")).strip()
            or mentioned_id
        )
        return f"@{_escape_markdown_text(display_name)}" if display_name else "@"
    if tag in {"img", "image"}:
        image_key = str(element.get("image_key", "")).strip()
        if image_key and image_key not in image_keys:
            image_keys.append(image_key)
        alt = str(element.get("text", "")).strip() or str(element.get("alt", "")).strip()
        return f"[Image: {alt}]" if alt else "[Image]"
    if tag in {"media", "file", "audio", "video"}:
        file_key = str(element.get("file_key", "")).strip()
        file_name = (
            str(element.get("file_name", "")).strip()
            or str(element.get("title", "")).strip()
            or str(element.get("text", "")).strip()
        )
        if file_key:
            media_refs.append(
                FeishuPostMediaRef(
                    file_key=file_key,
                    file_name=file_name,
                    resource_type=tag if tag in {"audio", "video"} else "file",
                )
            )
        return f"[Attachment: {file_name}]" if file_name else "[Attachment]"
    if tag in {"emotion", "emoji"}:
        label = str(element.get("text", "")).strip() or str(element.get("emoji_type", "")).strip()
        return f":{_escape_markdown_text(label)}:" if label else "[Emoji]"
    if tag == "br":
        return "\n"
    if tag in {"hr", "divider"}:
        return "\n\n---\n\n"
    if tag == "code":
        code = str(element.get("text", "") or "") or str(element.get("content", "") or "")
        return _wrap_inline_code(code) if code else ""

    # Fallback: try to extract from nested fields
    nested_parts: List[str] = []
    for key in ("text", "title", "content", "children", "elements"):
        value = element.get(key)
        extracted = _render_nested_post(value, image_keys, media_refs, mentioned_ids)
        if extracted:
            nested_parts.append(extracted)
    return " ".join(part for part in nested_parts if part)


def _render_nested_post(
    value: Any,
    image_keys: List[str],
    media_refs: List[FeishuPostMediaRef],
    mentioned_ids: List[str],
) -> str:
    if isinstance(value, str):
        return _escape_markdown_text(value)
    if isinstance(value, list):
        return " ".join(
            part
            for item in value
            for part in [_render_nested_post(item, image_keys, media_refs, mentioned_ids)]
            if part
        )
    if isinstance(value, dict):
        direct = _render_post_element(value, image_keys, media_refs, mentioned_ids)
        if direct:
            return direct
        return " ".join(
            part
            for item in value.values()
            for part in [_render_nested_post(item, image_keys, media_refs, mentioned_ids)]
            if part
        )
    return ""


# ---------------------------------------------------------------------------
# Message normalization
# ---------------------------------------------------------------------------


def normalize_feishu_message(*, message_type: str, raw_content: str) -> FeishuNormalizedMessage:
    normalized_type = str(message_type or "").strip().lower()
    payload = _load_feishu_payload(raw_content)

    if normalized_type == "text":
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=_normalize_feishu_text(str(payload.get("text", "") or "")),
        )
    if normalized_type == "post":
        parsed_post = parse_feishu_post_payload(payload)
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=parsed_post.text_content,
            image_keys=list(parsed_post.image_keys),
            media_refs=list(parsed_post.media_refs),
            mentioned_ids=list(parsed_post.mentioned_ids),
            relation_kind="post",
        )
    if normalized_type == "image":
        image_key = str(payload.get("image_key", "") or "").strip()
        alt_text = _normalize_feishu_text(
            str(payload.get("text", "") or "")
            or str(payload.get("alt", "") or "")
            or FALLBACK_IMAGE_TEXT
        )
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=alt_text if alt_text != FALLBACK_IMAGE_TEXT else "",
            preferred_message_type="photo",
            image_keys=[image_key] if image_key else [],
            relation_kind="image",
        )
    if normalized_type in {"file", "audio", "media"}:
        file_key = str(payload.get("file_key", "") or "").strip()
        file_name = str(payload.get("file_name", "") or payload.get("title", "") or "").strip()
        return FeishuNormalizedMessage(
            raw_type=normalized_type,
            text_content=f"[Attachment: {file_name}]" if file_name else FALLBACK_ATTACHMENT_TEXT,
            preferred_message_type="audio" if normalized_type == "audio" else "document",
            media_refs=[FeishuPostMediaRef(file_key=file_key, file_name=file_name)] if file_key else [],
            relation_kind=normalized_type,
        )
    if normalized_type == "merge_forward":
        return _normalize_merge_forward_message(payload)
    if normalized_type == "share_chat":
        return _normalize_share_chat_message(payload)
    if normalized_type in {"interactive", "card"}:
        return _normalize_interactive_message(normalized_type, payload)

    return FeishuNormalizedMessage(raw_type=normalized_type, text_content="")


def _load_feishu_payload(raw_content: str) -> Dict[str, Any]:
    try:
        parsed = json.loads(raw_content) if raw_content else {}
    except json.JSONDecodeError:
        return {"text": raw_content}
    return parsed if isinstance(parsed, dict) else {"content": parsed}


def _normalize_merge_forward_message(payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    title = _first_non_empty_text(
        payload.get("title"),
        payload.get("summary"),
    )
    entries = _collect_forward_entries(payload)
    lines: List[str] = []
    if title:
        lines.append(title)
    lines.extend(entries[:8])
    text_content = "\n".join(lines).strip() or FALLBACK_FORWARD_TEXT
    return FeishuNormalizedMessage(
        raw_type="merge_forward",
        text_content=text_content,
        relation_kind="merge_forward",
    )


def _normalize_share_chat_message(payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    chat_name = _first_non_empty_text(
        payload.get("chat_name"),
        payload.get("name"),
        payload.get("title"),
    )
    lines = []
    if chat_name:
        lines.append(f"Shared chat: {chat_name}")
    else:
        lines.append(FALLBACK_SHARE_CHAT_TEXT)
    text_content = "\n".join(lines)
    return FeishuNormalizedMessage(
        raw_type="share_chat",
        text_content=text_content,
        relation_kind="share_chat",
    )


def _normalize_interactive_message(message_type: str, payload: Dict[str, Any]) -> FeishuNormalizedMessage:
    card_payload = payload.get("card") if isinstance(payload.get("card"), dict) else payload
    title = _first_non_empty_text(
        _find_header_title(card_payload),
        payload.get("title"),
    )
    body_lines = _collect_card_lines(card_payload)
    actions = _collect_action_labels(card_payload)

    lines: List[str] = []
    if title:
        lines.append(title)
    for line in body_lines:
        if line != title:
            lines.append(line)
    if actions:
        lines.append(f"Actions: {', '.join(actions)}")

    text_content = "\n".join(lines[:12]).strip() or FALLBACK_INTERACTIVE_TEXT
    return FeishuNormalizedMessage(
        raw_type=message_type,
        text_content=text_content,
        relation_kind="interactive",
        metadata={"title": title, "actions": actions},
    )


# ---------------------------------------------------------------------------
# Card/forward text extraction utilities
# ---------------------------------------------------------------------------


def _collect_forward_entries(payload: Dict[str, Any]) -> List[str]:
    candidates: List[Any] = []
    for key in ("messages", "items", "message_list", "records", "content"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    entries: List[str] = []
    for item in candidates:
        if isinstance(item, dict):
            sender = _first_non_empty_text(
                item.get("sender_name"), item.get("user_name"), item.get("sender"), item.get("name"),
            )
            body = _first_non_empty_text(
                item.get("text"), item.get("summary"), item.get("preview"), item.get("content"),
            )
            body = _normalize_feishu_text(body)
            if sender and body:
                entries.append(f"- {sender}: {body}")
            elif body:
                entries.append(f"- {body}")
        else:
            text = _normalize_feishu_text(str(item or ""))
            if text:
                entries.append(f"- {text}")
    return _unique_lines(entries)


def _collect_card_lines(payload: Any) -> List[str]:
    lines = _collect_text_segments(payload, in_rich_block=False)
    normalized = [_normalize_feishu_text(line) for line in lines]
    return _unique_lines([line for line in normalized if line])


def _collect_action_labels(payload: Any) -> List[str]:
    labels: List[str] = []
    for item in _walk_nodes(payload):
        if not isinstance(item, dict):
            continue
        tag = str(item.get("tag", "") or item.get("type", "")).strip().lower()
        if tag not in {"button", "select_static", "overflow", "date_picker", "picker"}:
            continue
        label = _first_non_empty_text(
            item.get("text"),
            item.get("name"),
            item.get("value"),
        )
        if label:
            labels.append(label)
    return _unique_lines(labels)


def _collect_text_segments(value: Any, *, in_rich_block: bool) -> List[str]:
    if isinstance(value, str):
        return [_normalize_feishu_text(value)] if in_rich_block else []
    if isinstance(value, list):
        segments: List[str] = []
        for item in value:
            segments.extend(_collect_text_segments(item, in_rich_block=in_rich_block))
        return segments
    if not isinstance(value, dict):
        return []

    tag = str(value.get("tag", "") or value.get("type", "")).strip().lower()
    next_in_rich_block = in_rich_block or tag in {
        "plain_text", "lark_md", "markdown", "note", "div",
        "column_set", "column", "action", "button", "select_static", "date_picker",
    }

    segments: List[str] = []
    for key in _SUPPORTED_CARD_TEXT_KEYS:
        item = value.get(key)
        if isinstance(item, str) and next_in_rich_block:
            normalized = _normalize_feishu_text(item)
            if normalized:
                segments.append(normalized)

    for key, item in value.items():
        if key in _SKIP_TEXT_KEYS:
            continue
        segments.extend(_collect_text_segments(item, in_rich_block=next_in_rich_block))
    return segments


def _find_header_title(payload: Any) -> str:
    if not isinstance(payload, dict):
        return ""
    header = payload.get("header")
    if not isinstance(header, dict):
        return ""
    title = header.get("title")
    if isinstance(title, dict):
        return _first_non_empty_text(title.get("content"), title.get("text"), title.get("name"))
    return _normalize_feishu_text(str(title or ""))


def _walk_nodes(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from _walk_nodes(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_nodes(item)


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        if isinstance(value, str):
            normalized = _normalize_feishu_text(value)
            if normalized:
                return normalized
        elif value is not None and not isinstance(value, (dict, list)):
            normalized = _normalize_feishu_text(str(value))
            if normalized:
                return normalized
    return ""


# ---------------------------------------------------------------------------
# WS thread runner
# ---------------------------------------------------------------------------


def _run_official_feishu_ws_client(ws_client: Any, adapter: Any) -> None:
    """Run the official Lark WS client in its own thread-local event loop."""
    import lark_oapi.ws.client as ws_client_module

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    ws_client_module.loop = loop
    adapter._ws_thread_loop = loop

    try:
        ws_client.start()
    except Exception:
        pass
    finally:
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for task in pending:
            task.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        try:
            loop.close()
        except Exception:
            pass
        adapter._ws_thread_loop = None


# ---------------------------------------------------------------------------
# Adapter class
# ---------------------------------------------------------------------------


class FeishuAdapter(PlatformAdapter):
    """Feishu/Lark bot adapter."""

    name = "feishu"
    platform = "feishu"
    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH

    def __init__(self, config: Dict[str, Any] = None):
        super().__init__(config)
        self._app_id = str(self._config.get("app_id") or os.getenv("FEISHU_APP_ID", "")).strip()
        self._app_secret = str(self._config.get("app_secret") or os.getenv("FEISHU_APP_SECRET", "")).strip()
        self._domain_name = str(self._config.get("domain") or os.getenv("FEISHU_DOMAIN", "feishu")).strip().lower()
        self._connection_mode = str(
            self._config.get("connection_mode") or os.getenv("FEISHU_CONNECTION_MODE", "websocket")
        ).strip().lower()
        self._encrypt_key = os.getenv("FEISHU_ENCRYPT_KEY", "").strip()
        self._verification_token = os.getenv("FEISHU_VERIFICATION_TOKEN", "").strip()
        self._bot_open_id = str(self._config.get("bot_open_id") or os.getenv("FEISHU_BOT_OPEN_ID", "")).strip()
        self._bot_user_id = str(self._config.get("bot_user_id") or os.getenv("FEISHU_BOT_USER_ID", "")).strip()
        self._bot_name = str(self._config.get("bot_name") or os.getenv("FEISHU_BOT_NAME", "")).strip()
        self._webhook_host = str(
            self._config.get("webhook_host") or os.getenv("FEISHU_WEBHOOK_HOST", DEFAULT_WEBHOOK_HOST)
        ).strip()
        self._webhook_port = int(
            self._config.get("webhook_port") or os.getenv("FEISHU_WEBHOOK_PORT", str(DEFAULT_WEBHOOK_PORT))
        )
        self._webhook_path = (
            str(self._config.get("webhook_path") or os.getenv("FEISHU_WEBHOOK_PATH", DEFAULT_WEBHOOK_PATH)).strip()
            or DEFAULT_WEBHOOK_PATH
        )

        self._client: Optional[Any] = None
        self._ws_client: Optional[Any] = None
        self._ws_future: Optional[asyncio.Future] = None
        self._ws_thread_loop: Optional[asyncio.AbstractEventLoop] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._webhook_runner: Optional[Any] = None
        self._webhook_site: Optional[Any] = None
        self._event_handler: Optional[Any] = None

        # Deduplication
        self._seen_message_ids: Dict[str, float] = {}
        self._seen_message_order: List[str] = []

        # Card action dedup
        self._card_action_tokens: Dict[str, float] = {}

    @staticmethod
    def check_requirements() -> bool:
        """Check if Feishu/Lark dependencies are available."""
        return FEISHU_AVAILABLE

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        """Connect to Feishu/Lark."""
        if not FEISHU_AVAILABLE:
            logger.error("[Feishu] lark-oapi not installed")
            return False
        if not self._app_id or not self._app_secret:
            logger.error("[Feishu] FEISHU_APP_ID or FEISHU_APP_SECRET not set")
            return False
        if self._connection_mode not in {"websocket", "webhook"}:
            logger.error("[Feishu] Unsupported connection_mode=%s", self._connection_mode)
            return False

        try:
            self._loop = asyncio.get_running_loop()
            await self._connect_with_retry()
            self._running = True
            logger.info("[Feishu] Connected in %s mode (%s)", self._connection_mode, self._domain_name)
            return True
        except Exception as exc:
            logger.error("[Feishu] Failed to connect: %s", exc, exc_info=True)
            return False

    async def stop(self) -> None:
        """Disconnect from Feishu/Lark."""
        self._running = False
        self._disable_websocket_auto_reconnect()
        await self._stop_webhook_server()

        ws_thread_loop = self._ws_thread_loop
        if ws_thread_loop is not None and not ws_thread_loop.is_closed():
            def cancel_all_tasks() -> None:
                tasks = [t for t in asyncio.all_tasks(ws_thread_loop) if not t.done()]
                for task in tasks:
                    task.cancel()
                ws_thread_loop.call_later(0.1, ws_thread_loop.stop)
            ws_thread_loop.call_soon_threadsafe(cancel_all_tasks)

        ws_future = self._ws_future
        if ws_future is not None:
            try:
                await asyncio.wait_for(asyncio.shield(ws_future), timeout=10.0)
            except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
                pass

        self._ws_future = None
        self._ws_thread_loop = None
        self._loop = None
        self._event_handler = None
        self._seen_message_ids.clear()
        self._seen_message_order.clear()
        logger.info("[Feishu] Disconnected")

    async def _connect_with_retry(self) -> None:
        for attempt in range(FEISHU_CONNECT_ATTEMPTS):
            try:
                if self._connection_mode == "websocket":
                    await self._connect_websocket()
                else:
                    await self._connect_webhook()
                return
            except Exception as exc:
                self._running = False
                self._disable_websocket_auto_reconnect()
                self._ws_future = None
                await self._stop_webhook_server()
                if attempt >= FEISHU_CONNECT_ATTEMPTS - 1:
                    raise
                wait_seconds = 2 ** attempt
                logger.warning("[Feishu] Connect attempt %d/%d failed; retrying in %ds: %s",
                               attempt + 1, FEISHU_CONNECT_ATTEMPTS, wait_seconds, exc)
                await asyncio.sleep(wait_seconds)

    async def _connect_websocket(self) -> None:
        if not FEISHU_WEBSOCKET_AVAILABLE:
            raise RuntimeError("websockets not installed; websocket mode unavailable")
        domain = FEISHU_DOMAIN if self._domain_name != "lark" else LARK_DOMAIN
        self._client = self._build_lark_client(domain)
        self._event_handler = self._build_event_handler()
        if self._event_handler is None:
            raise RuntimeError("failed to build Feishu event handler")
        loop = self._loop
        if loop is None or loop.is_closed():
            raise RuntimeError("adapter loop is not ready")
        self._ws_client = FeishuWSClient(
            app_id=self._app_id,
            app_secret=self._app_secret,
            log_level=lark.LogLevel.INFO,
            event_handler=self._event_handler,
            domain=domain,
        )
        self._ws_future = loop.run_in_executor(
            None,
            _run_official_feishu_ws_client,
            self._ws_client,
            self,
        )

    async def _connect_webhook(self) -> None:
        if not FEISHU_WEBHOOK_AVAILABLE:
            raise RuntimeError("aiohttp not installed; webhook mode unavailable")
        domain = FEISHU_DOMAIN if self._domain_name != "lark" else LARK_DOMAIN
        self._client = self._build_lark_client(domain)
        self._event_handler = self._build_event_handler()
        if self._event_handler is None:
            raise RuntimeError("failed to build Feishu event handler")
        app = web.Application()
        app.router.add_post(self._webhook_path, self._handle_webhook_request)
        self._webhook_runner = web.AppRunner(app)
        await self._webhook_runner.setup()
        self._webhook_site = web.TCPSite(self._webhook_runner, self._webhook_host, self._webhook_port)
        await self._webhook_site.start()

    def _build_lark_client(self, domain: Any) -> Any:
        return (
            lark.Client.builder()
            .app_id(self._app_id)
            .app_secret(self._app_secret)
            .domain(domain)
            .log_level(lark.LogLevel.WARNING)
            .build()
        )

    def _build_event_handler(self) -> Any:
        if EventDispatcherHandler is None:
            return None
        return (
            EventDispatcherHandler.builder(
                self._encrypt_key,
                self._verification_token,
            )
            .register_p2_im_message_receive_v1(self._on_message_event)
            .register_p2_card_action_trigger(self._on_card_action_trigger)
            .build()
        )

    def _disable_websocket_auto_reconnect(self) -> None:
        if self._ws_client is None:
            return
        try:
            setattr(self._ws_client, "_auto_reconnect", False)
        except Exception:
            pass
        finally:
            self._ws_client = None

    async def _stop_webhook_server(self) -> None:
        if self._webhook_runner is None:
            return
        try:
            await self._webhook_runner.cleanup()
        finally:
            self._webhook_runner = None
            self._webhook_site = None

    # ------------------------------------------------------------------
    # Inbound message handling
    # ------------------------------------------------------------------

    def _on_message_event(self, data: Any) -> None:
        """Normalize Feishu inbound events."""
        loop = self._loop
        if loop is None or bool(getattr(loop, "is_closed", lambda: False)()):
            return
        future = asyncio.run_coroutine_threadsafe(
            self._handle_message_event_data(data),
            loop,
        )
        future.add_done_callback(self._log_background_failure)

    @staticmethod
    def _log_background_failure(future: asyncio.Future) -> None:
        exc = future.exception()
        if exc:
            logger.error("[Feishu] Background task failed: %s", exc, exc_info=exc)

    async def _handle_message_event_data(self, data: Any) -> None:
        """Shared inbound message handling for websocket and webhook transports."""
        event = getattr(data, "event", None)
        message = getattr(event, "message", None)
        sender = getattr(event, "sender", None)
        sender_id = getattr(sender, "sender_id", None)
        if not message or not sender_id:
            return

        message_id = getattr(message, "message_id", None)
        if not message_id or self._is_duplicate(message_id):
            return
        if getattr(sender, "sender_type", "") == "bot":
            return

        chat_type = getattr(message, "chat_type", "p2p")
        chat_id = getattr(message, "chat_id", "") or ""
        if chat_type != "p2p" and not self._should_accept_group_message(message, sender_id, chat_id):
            return

        # Extract text content
        raw_content = getattr(message, "content", "") or ""
        raw_type = getattr(message, "message_type", "") or ""
        normalized = normalize_feishu_message(message_type=raw_type, raw_content=raw_content)
        text = normalized.text_content

        if not text:
            logger.debug("[Feishu] Empty message skipped: id=%s type=%s", message_id, raw_type)
            return

        # Resolve sender info
        open_id = getattr(sender_id, "open_id", None) or ""
        user_id = getattr(sender_id, "user_id", None) or ""

        group_id = resolve_group_id("feishu", {"chat_id": chat_id})

        reply_to_message_id = (
            getattr(message, "parent_id", None)
            or getattr(message, "upper_message_id", None)
            or ""
        )

        event = AdapterMessageEvent(
            text=text,
            group_id=group_id,
            sender_id=str(open_id or user_id),
            message_id=message_id,
            platform="feishu",
            reply_to_message_id=str(reply_to_message_id) if reply_to_message_id else "",
        )

        logger.info("[Feishu] Inbound message: id=%s type=%s chat_id=%s text=%r",
                     message_id, raw_type, chat_id, text[:80])
        await self._dispatch(event)

    def _on_card_action_trigger(self, data: Any) -> Any:
        """Schedule Feishu card actions on the adapter loop."""
        loop = self._loop
        if loop is None or bool(getattr(loop, "is_closed", lambda: False)()):
            return None
        future = asyncio.run_coroutine_threadsafe(
            self._handle_card_action_event(data),
            loop,
        )
        future.add_done_callback(self._log_background_failure)
        if P2CardActionTriggerResponse is None:
            return None
        return P2CardActionTriggerResponse()

    async def _handle_card_action_event(self, data: Any) -> None:
        """Route card button clicks as synthetic text events."""
        event = getattr(data, "event", None)
        token = str(getattr(event, "token", "") or "")
        if token and self._is_card_action_duplicate(token):
            return

        context = getattr(event, "context", None)
        chat_id = str(getattr(context, "open_chat_id", "") or "")
        operator = getattr(event, "operator", None)
        open_id = str(getattr(operator, "open_id", "") or "")
        if not chat_id or not open_id:
            return

        action = getattr(event, "action", None)
        action_tag = str(getattr(action, "tag", "") or "button")
        action_value = getattr(action, "value", {}) or {}

        synthetic_text = f"/card {action_tag}"
        if action_value:
            try:
                synthetic_text += " " + json.dumps(action_value, ensure_ascii=False)
            except Exception:
                pass

        group_id = resolve_group_id("feishu", {"chat_id": chat_id})

        event_obj = AdapterMessageEvent(
            text=synthetic_text,
            group_id=group_id,
            sender_id=open_id,
            platform="feishu",
        )
        await self._dispatch(event_obj)

    # ------------------------------------------------------------------
    # Webhook handler
    # ------------------------------------------------------------------

    async def _handle_webhook_request(self, request: Any) -> Any:
        """Handle inbound webhook HTTP request from Feishu."""
        if web is None:
            return None
        try:
            body = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)

        # Handle URL verification challenge
        if body.get("type") == "url_verification":
            challenge = body.get("challenge", "")
            return web.json_response({"challenge": challenge})

        # Dispatch through the event handler
        if self._event_handler is not None:
            try:
                self._event_handler.do(body)
            except Exception as exc:
                logger.error("[Feishu] Webhook event dispatch error: %s", exc)

        return web.json_response({"code": 0})

    # ------------------------------------------------------------------
    # Group message filtering
    # ------------------------------------------------------------------

    def _should_accept_group_message(self, message: Any, sender_id: Any, chat_id: str) -> bool:
        """Require an explicit @mention before group messages enter the agent."""
        raw_content = getattr(message, "content", "") or ""
        if "@_all" in raw_content:
            return True
        mentions = getattr(message, "mentions", None) or []
        if mentions:
            return self._message_mentions_bot(mentions)
        normalized = normalize_feishu_message(
            message_type=getattr(message, "message_type", "") or "",
            raw_content=raw_content,
        )
        if normalized.mentioned_ids:
            return self._post_mentions_bot(normalized.mentioned_ids)
        return False

    def _message_mentions_bot(self, mentions: List[Any]) -> bool:
        for mention in mentions:
            mention_id = getattr(mention, "id", None)
            mention_open_id = getattr(mention_id, "open_id", None)
            mention_user_id = getattr(mention_id, "user_id", None)
            mention_name = (getattr(mention, "name", None) or "").strip()

            if self._bot_open_id and mention_open_id == self._bot_open_id:
                return True
            if self._bot_user_id and mention_user_id == self._bot_user_id:
                return True
            if self._bot_name and mention_name == self._bot_name:
                return True
        return False

    def _post_mentions_bot(self, mentioned_ids: List[str]) -> bool:
        if not mentioned_ids:
            return False
        if self._bot_open_id and self._bot_open_id in mentioned_ids:
            return True
        if self._bot_user_id and self._bot_user_id in mentioned_ids:
            return True
        return False

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _is_duplicate(self, message_id: str) -> bool:
        now = time.time()
        if message_id in self._seen_message_ids:
            return True
        self._seen_message_ids[message_id] = now
        self._seen_message_order.append(message_id)

        # Evict old entries
        while len(self._seen_message_ids) > DEDUP_CACHE_SIZE:
            oldest = self._seen_message_order.pop(0)
            self._seen_message_ids.pop(oldest, None)
        return False

    def _is_card_action_duplicate(self, token: str) -> bool:
        now = time.time()
        expired = [t for t, ts in self._card_action_tokens.items() if now - ts > CARD_ACTION_DEDUP_TTL_SECONDS]
        for t in expired:
            del self._card_action_tokens[t]
        if token in self._card_action_tokens:
            return True
        self._card_action_tokens[token] = now
        return False

    # ------------------------------------------------------------------
    # Outbound messaging
    # ------------------------------------------------------------------

    def _build_outbound_payload(self, content: str):
        """Build Feishu message payload. Try markdown post first, fallback to text."""
        return "post", _build_markdown_post_payload(content)

    async def send_response(self, group_id: str, text: str) -> None:
        """Send a text message to a Feishu chat."""
        # Extract chat_id from group_id (last segment after :)
        chat_id = group_id.rsplit(":", 1)[-1] if ":" in group_id else group_id

        if not self._client:
            logger.error("[Feishu] Cannot send: not connected")
            return

        msg_type, payload = self._build_outbound_payload(text)

        try:
            body = CreateMessageRequestBody.builder() \
                .receive_id(chat_id) \
                .msg_type(msg_type) \
                .content(payload) \
                .build()
            request = CreateMessageRequest.builder() \
                .receive_id_type("chat_id") \
                .request_body(body) \
                .build()

            response = await asyncio.to_thread(self._client.im.v1.message.create, request)

            if not response or not getattr(response, "success", lambda: False)():
                code = getattr(response, "code", "unknown")
                msg = getattr(response, "msg", "unknown error")
                if msg_type == "post" and _POST_CONTENT_INVALID_RE.search(str(msg)):
                    # Fallback to plain text
                    fallback_payload = json.dumps({"text": _strip_markdown_to_plain_text(text)}, ensure_ascii=False)
                    body = CreateMessageRequestBody.builder() \
                        .receive_id(chat_id) \
                        .msg_type("text") \
                        .content(fallback_payload) \
                        .build()
                    request = CreateMessageRequest.builder() \
                        .receive_id_type("chat_id") \
                        .request_body(body) \
                        .build()
                    response = await asyncio.to_thread(self._client.im.v1.message.create, request)
                    if not response or not getattr(response, "success", lambda: False)():
                        code = getattr(response, "code", "unknown")
                        msg = getattr(response, "msg", "unknown error")
                        logger.warning("[Feishu] Send failed after fallback: [%s] %s", code, msg)
                else:
                    logger.warning("[Feishu] Send failed: [%s] %s", code, msg)
        except Exception as exc:
            logger.error("[Feishu] Send error: %s", exc, exc_info=True)
