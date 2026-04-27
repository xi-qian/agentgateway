"""Shared message protocol types for hermes-distributed.

Defines all message types exchanged between the gateway service and agent
service. Each message is a simple dataclass with a ``to_dict()`` method for
serialization and class-level ``from_dict()`` for deserialization.

Protocol version: 1
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, fields
from typing import Any, Dict, List, Literal, Optional

# ---------------------------------------------------------------------------
# Protocol version
# ---------------------------------------------------------------------------

PROTOCOL_VERSION = 1

# ---------------------------------------------------------------------------
# Custom exception
# ---------------------------------------------------------------------------


class ProtocolError(Exception):
    """Raised when a message cannot be parsed or is invalid."""


# ---------------------------------------------------------------------------
# Agent -> Gateway messages
# ---------------------------------------------------------------------------


@dataclass
class HelloMessage:
    """Sent by agent on first connection to announce its identity."""

    group_id: str
    profile: str
    model: str
    toolsets: List[str] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "hello",
            "group_id": self.group_id,
            "profile": self.profile,
            "model": self.model,
            "toolsets": self.toolsets,
            "capabilities": self.capabilities,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> HelloMessage:
        return cls(
            group_id=d["group_id"],
            profile=d["profile"],
            model=d["model"],
            toolsets=d.get("toolsets", []),
            capabilities=d.get("capabilities", []),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class StreamMessage:
    """Incremental text token streamed from the agent."""

    group_id: str
    token: str
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "stream",
            "group_id": self.group_id,
            "token": self.token,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StreamMessage:
        return cls(
            group_id=d["group_id"],
            token=d["token"],
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class ProgressMessage:
    """Agent is executing a tool -- gateway can show a status indicator."""

    group_id: str
    tool: str
    preview: str = ""
    emoji: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "progress",
            "group_id": self.group_id,
            "tool": self.tool,
            "preview": self.preview,
            "emoji": self.emoji,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ProgressMessage:
        return cls(
            group_id=d["group_id"],
            tool=d["tool"],
            preview=d.get("preview", ""),
            emoji=d.get("emoji", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class CompleteMessage:
    """Agent has finished processing the task."""

    group_id: str
    final_response: str = ""
    api_calls: int = 0
    tokens: Dict[str, int] = field(default_factory=dict)
    cost_usd: float = 0.0
    interrupted: bool = False
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "version": self.version,
            "type": "complete",
            "group_id": self.group_id,
            "final_response": self.final_response,
            "api_calls": self.api_calls,
            "tokens": self.tokens,
            "cost_usd": self.cost_usd,
        }
        if self.interrupted:
            d["interrupted"] = self.interrupted
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> CompleteMessage:
        return cls(
            group_id=d["group_id"],
            final_response=d.get("final_response", ""),
            api_calls=d.get("api_calls", 0),
            tokens=d.get("tokens", {}),
            cost_usd=d.get("cost_usd", 0.0),
            interrupted=d.get("interrupted", False),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class ErrorMessage:
    """Agent encountered an error."""

    group_id: str
    message: str
    fatal: bool = False
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "error",
            "group_id": self.group_id,
            "message": self.message,
            "fatal": self.fatal,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ErrorMessage:
        return cls(
            group_id=d["group_id"],
            message=d["message"],
            fatal=d.get("fatal", False),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class ApprovalRequestMessage:
    """Agent asks the gateway/user to approve a dangerous command."""

    group_id: str
    request_id: str
    command: str
    reason: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "approval_request",
            "group_id": self.group_id,
            "request_id": self.request_id,
            "command": self.command,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ApprovalRequestMessage:
        return cls(
            group_id=d["group_id"],
            request_id=d["request_id"],
            command=d["command"],
            reason=d.get("reason", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class HeartbeatMessage:
    """Bidirectional keep-alive ping."""

    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "heartbeat",
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> HeartbeatMessage:
        return cls(
            version=d.get("version", PROTOCOL_VERSION),
        )


# ---------------------------------------------------------------------------
# Gateway -> Agent messages
# ---------------------------------------------------------------------------


@dataclass
class TaskMessage:
    """Gateway dispatches a new user task to the agent."""

    group_id: str
    message: str
    sender: Dict[str, Any] = field(default_factory=dict)
    media: List[Dict[str, Any]] = field(default_factory=list)
    reply_to_message_id: Optional[str] = None
    reply_to_text: Optional[str] = None
    context_prompt: str = ""
    history: List[Dict[str, Any]] = field(default_factory=list)
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "version": self.version,
            "type": "task",
            "group_id": self.group_id,
            "message": self.message,
            "sender": self.sender,
            "media": self.media,
        }
        if self.reply_to_message_id is not None:
            d["reply_to_message_id"] = self.reply_to_message_id
        if self.reply_to_text is not None:
            d["reply_to_text"] = self.reply_to_text
        if self.context_prompt:
            d["context_prompt"] = self.context_prompt
        if self.history:
            d["history"] = self.history
        return d

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> TaskMessage:
        return cls(
            group_id=d["group_id"],
            message=d["message"],
            sender=d.get("sender", {}),
            media=d.get("media", []),
            reply_to_message_id=d.get("reply_to_message_id"),
            reply_to_text=d.get("reply_to_text"),
            context_prompt=d.get("context_prompt", ""),
            history=d.get("history", []),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class InterruptMessage:
    """Gateway asks the agent to stop processing a task."""

    group_id: str
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "interrupt",
            "group_id": self.group_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> InterruptMessage:
        return cls(
            group_id=d["group_id"],
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class ApprovedMessage:
    """Gateway/user approved an approval request."""

    group_id: str
    request_id: str
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "approved",
            "group_id": self.group_id,
            "request_id": self.request_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> ApprovedMessage:
        return cls(
            group_id=d["group_id"],
            request_id=d["request_id"],
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class DeniedMessage:
    """Gateway/user denied an approval request."""

    group_id: str
    request_id: str
    reason: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "denied",
            "group_id": self.group_id,
            "request_id": self.request_id,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DeniedMessage:
        return cls(
            group_id=d["group_id"],
            request_id=d["request_id"],
            reason=d.get("reason", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class SessionRestoreMessage:
    """Gateway sends session history to Agent for restoration."""

    group_id: str
    history: List[Dict[str, Any]] = field(default_factory=list)
    session_id: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "session_restore",
            "group_id": self.group_id,
            "history": self.history,
            "session_id": self.session_id,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> SessionRestoreMessage:
        return cls(
            group_id=d["group_id"],
            history=d.get("history", []),
            session_id=d.get("session_id", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


# ---------------------------------------------------------------------------
# Daemon -> Manager messages  (Link 3 — daemon <-> manager protocol)
# ---------------------------------------------------------------------------


@dataclass
class RegisterMessage:
    """Daemon registers itself with the manager on first connection."""

    server_id: str
    hostname: str
    cpu_cores: int
    mem_total_gb: float
    mem_available_gb: float
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "register",
            "server_id": self.server_id,
            "hostname": self.hostname,
            "cpu_cores": self.cpu_cores,
            "mem_total_gb": self.mem_total_gb,
            "mem_available_gb": self.mem_available_gb,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> RegisterMessage:
        return cls(
            server_id=d["server_id"],
            hostname=d["hostname"],
            cpu_cores=d["cpu_cores"],
            mem_total_gb=d["mem_total_gb"],
            mem_available_gb=d["mem_available_gb"],
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class DaemonHeartbeatMessage:
    """Periodic heartbeat with resource usage from daemon to manager."""

    running_agents: List[Dict[str, Any]] = field(default_factory=list)
    mem_used_gb: float = 0.0
    cpu_percent: float = 0.0
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "heartbeat",
            "running_agents": self.running_agents,
            "mem_used_gb": self.mem_used_gb,
            "cpu_percent": self.cpu_percent,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> DaemonHeartbeatMessage:
        return cls(
            running_agents=d.get("running_agents", []),
            mem_used_gb=d.get("mem_used_gb", 0.0),
            cpu_percent=d.get("cpu_percent", 0.0),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class AgentStartedMessage:
    """Daemon notifies the manager that an agent process has started."""

    group_id: str
    pid: int
    status: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "agent_started",
            "group_id": self.group_id,
            "pid": self.pid,
            "status": self.status,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AgentStartedMessage:
        return cls(
            group_id=d["group_id"],
            pid=d["pid"],
            status=d.get("status", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class AgentStoppedMessage:
    """Daemon notifies the manager that an agent process has exited."""

    group_id: str
    pid: int
    exit_code: int = 0
    reason: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "agent_stopped",
            "group_id": self.group_id,
            "pid": self.pid,
            "exit_code": self.exit_code,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> AgentStoppedMessage:
        return cls(
            group_id=d["group_id"],
            pid=d["pid"],
            exit_code=d.get("exit_code", 0),
            reason=d.get("reason", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class LogMessage:
    """Daemon forwards agent stdout/stderr lines to the manager."""

    group_id: str
    pid: int
    stream: str = "stdout"
    line: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "log",
            "group_id": self.group_id,
            "pid": self.pid,
            "stream": self.stream,
            "line": self.line,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> LogMessage:
        return cls(
            group_id=d["group_id"],
            pid=d["pid"],
            stream=d.get("stream", "stdout"),
            line=d.get("line", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


# ---------------------------------------------------------------------------
# Manager -> Daemon messages  (Link 3)
# ---------------------------------------------------------------------------


@dataclass
class StartAgentMessage:
    """Manager instructs the daemon to launch an agent process."""

    group_id: str
    profile: str = ""
    gateway_url: str = ""
    hermes_home: str = ""
    model: str = ""
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "start_agent",
            "group_id": self.group_id,
            "profile": self.profile,
            "gateway_url": self.gateway_url,
            "hermes_home": self.hermes_home,
            "model": self.model,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StartAgentMessage:
        return cls(
            group_id=d["group_id"],
            profile=d.get("profile", ""),
            gateway_url=d.get("gateway_url", ""),
            hermes_home=d.get("hermes_home", ""),
            model=d.get("model", ""),
            version=d.get("version", PROTOCOL_VERSION),
        )


@dataclass
class StopAgentMessage:
    """Manager instructs the daemon to stop an agent process."""

    group_id: str
    force: bool = False
    version: int = PROTOCOL_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "type": "stop_agent",
            "group_id": self.group_id,
            "force": self.force,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> StopAgentMessage:
        return cls(
            group_id=d["group_id"],
            force=d.get("force", False),
            version=d.get("version", PROTOCOL_VERSION),
        )


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

# Message types that can originate from the agent  (Link 1)
_AGENT_MESSAGE_TYPES: Dict[str, type] = {
    "hello": HelloMessage,
    "stream": StreamMessage,
    "progress": ProgressMessage,
    "complete": CompleteMessage,
    "error": ErrorMessage,
    "approval_request": ApprovalRequestMessage,
    "heartbeat": HeartbeatMessage,
}

# Message types that can originate from the gateway  (Link 1)
_GATEWAY_MESSAGE_TYPES: Dict[str, type] = {
    "task": TaskMessage,
    "interrupt": InterruptMessage,
    "approved": ApprovedMessage,
    "denied": DeniedMessage,
    "heartbeat": HeartbeatMessage,
    "session_restore": SessionRestoreMessage,
}

# Message types that can originate from the daemon  (Link 3)
_DAEMON_MESSAGE_TYPES: Dict[str, type] = {
    "register": RegisterMessage,
    "heartbeat": DaemonHeartbeatMessage,
    "agent_started": AgentStartedMessage,
    "agent_stopped": AgentStoppedMessage,
    "log": LogMessage,
}

# Message types that can originate from the manager  (Link 3)
_MANAGER_MESSAGE_TYPES: Dict[str, type] = {
    "start_agent": StartAgentMessage,
    "stop_agent": StopAgentMessage,
}


def parse_agent_message(d: Dict[str, Any]):
    """Parse a dict received from an agent into the appropriate message type."""
    msg_type = d.get("type")
    if msg_type is None:
        raise ProtocolError("Message missing 'type' field")
    cls = _AGENT_MESSAGE_TYPES.get(msg_type)
    if cls is None:
        raise ProtocolError(f"Unknown agent message type: {msg_type!r}")
    return cls.from_dict(d)


def parse_gateway_message(d: Dict[str, Any]):
    """Parse a dict received from the gateway into the appropriate message type."""
    msg_type = d.get("type")
    if msg_type is None:
        raise ProtocolError("Message missing 'type' field")
    cls = _GATEWAY_MESSAGE_TYPES.get(msg_type)
    if cls is None:
        raise ProtocolError(f"Unknown gateway message type: {msg_type!r}")
    return cls.from_dict(d)


def parse_daemon_message(d: Dict[str, Any]):
    """Parse a dict received from a daemon into the appropriate message type."""
    msg_type = d.get("type")
    if msg_type is None:
        raise ProtocolError("Message missing 'type' field")
    cls = _DAEMON_MESSAGE_TYPES.get(msg_type)
    if cls is None:
        raise ProtocolError(f"Unknown daemon message type: {msg_type!r}")
    return cls.from_dict(d)


def parse_manager_message(d: Dict[str, Any]):
    """Parse a dict received from the manager into the appropriate message type."""
    msg_type = d.get("type")
    if msg_type is None:
        raise ProtocolError("Message missing 'type' field")
    cls = _MANAGER_MESSAGE_TYPES.get(msg_type)
    if cls is None:
        raise ProtocolError(f"Unknown manager message type: {msg_type!r}")
    return cls.from_dict(d)
