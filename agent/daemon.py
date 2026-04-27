"""Agent Daemon -- process manager that runs on each Agent server.

Connects to the Manager via WebSocket, receives start/stop commands,
manages child processes (agent_service.py) via subprocess, collects
stdout/stderr, and sends heartbeats.

Usage:
    python -m agent.daemon --manager-url ws://manager:8800/ws \
        --server-id agent-1 --hermes-root /path/to/hermes
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import platform
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional, TextIO

import aiohttp

from gateway.message_types import PROTOCOL_VERSION

logger = logging.getLogger("agent.daemon")

# ---------------------------------------------------------------------------
# Message builders
# ---------------------------------------------------------------------------


def build_register(
    server_id: str,
    hostname: str,
    cpu_cores: int,
    mem_total_gb: float,
    mem_available_gb: float,
) -> str:
    """Build the JSON register message sent on first WebSocket connection."""
    return json.dumps({
        "version": PROTOCOL_VERSION,
        "type": "register",
        "server_id": server_id,
        "hostname": hostname,
        "cpu_cores": cpu_cores,
        "mem_total_gb": mem_total_gb,
        "mem_available_gb": mem_available_gb,
    })


def build_heartbeat(
    running_agents: Optional[List[Dict[str, Any]]] = None,
    mem_used_gb: float = 0.0,
    mem_available_gb: float = 0.0,
    cpu_percent: float = 0.0,
) -> str:
    """Build the JSON heartbeat message with resource usage info."""
    if running_agents is None:
        running_agents = []
    return json.dumps({
        "version": PROTOCOL_VERSION,
        "type": "heartbeat",
        "running_agents": running_agents,
        "mem_used_gb": mem_used_gb,
        "mem_available_gb": mem_available_gb,
        "cpu_percent": cpu_percent,
    })


# ---------------------------------------------------------------------------
# Child process registry
# ---------------------------------------------------------------------------


class ChildRegistry:
    """Tracks running child agent processes keyed by group_id."""

    def __init__(self) -> None:
        self._children: Dict[str, subprocess.Popen] = {}

    def add(self, group_id: str, proc: subprocess.Popen) -> None:
        """Register a child process under the given group_id."""
        self._children[group_id] = proc

    def remove(self, group_id: str) -> Optional[subprocess.Popen]:
        """Remove and return the child process for the given group_id."""
        return self._children.pop(group_id, None)

    def get(self, group_id: str) -> Optional[subprocess.Popen]:
        """Return the child process for the given group_id, or None."""
        return self._children.get(group_id)

    def get_pid(self, group_id: str) -> Optional[int]:
        """Return the PID of the child process, or None."""
        proc = self._children.get(group_id)
        if proc is not None:
            return proc.pid
        return None

    def list_running(self) -> List[str]:
        """Return list of group_ids whose processes are still running."""
        running = []
        for gid, proc in list(self._children.items()):
            if proc.poll() is None:
                running.append(gid)
        return running

    def get_running_agents_info(self) -> List[Dict[str, Any]]:
        """Return info dicts for all running agent processes."""
        info = []
        for gid, proc in list(self._children.items()):
            if proc.poll() is None:
                info.append({"group_id": gid, "pid": proc.pid})
        return info


# ---------------------------------------------------------------------------
# Parse manager commands
# ---------------------------------------------------------------------------


def parse_manager_command(data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate and return a manager command dict.

    Accepts only 'start_agent' and 'stop_agent' types.  Returns the input
    dict if valid, raises ``ValueError`` otherwise.
    """
    msg_type = data.get("type")
    if msg_type not in ("start_agent", "stop_agent"):
        raise ValueError(f"Unknown manager command type: {msg_type!r}")
    return data


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------


async def start_child_process(
    group_id: str,
    gateway_url: str,
    profile: str,
    hermes_root: str,
    hermes_home: str,
    model: str,
    children: ChildRegistry,
    ws: aiohttp.ClientWebSocketResponse,
) -> None:
    """Launch an agent_service child process and wire up stdout/stderr/monitor."""
    from gateway.message_types import AgentStartedMessage

    cmd = [sys.executable, "-m", "agent.agent_service"]
    cmd.extend([
        "--gateway-url", gateway_url,
        "--group-id", group_id,
    ])
    if profile:
        cmd.extend(["--profile", profile])
    if hermes_home:
        cmd.extend(["--hermes-home", hermes_home])
    if model:
        cmd.extend(["--model", model])

    env = os.environ.copy()
    if hermes_root:
        env["HERMES_ROOT"] = hermes_root
    if hermes_home:
        env["HERMES_HOME"] = hermes_home

    logger.info("Launching child process: %s", " ".join(cmd))
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )

    children.add(group_id, proc)
    pid = proc.pid

    logger.info("Child process started: group_id=%s pid=%d", group_id, pid)

    await ws.send_str(json.dumps(
        AgentStartedMessage(group_id=group_id, pid=pid).to_dict(),
    ))

    # Start background tasks for stdout, stderr, and exit monitoring
    loop = asyncio.get_running_loop()
    if proc.stdout:
        loop.create_task(_read_stream(
            group_id, pid, proc.stdout, "stdout", ws, children,
        ))
    if proc.stderr:
        loop.create_task(_read_stream(
            group_id, pid, proc.stderr, "stderr", ws, children,
        ))
    loop.create_task(_monitor_exit(group_id, pid, proc, ws, children))


async def _read_stream(
    group_id: str,
    pid: int,
    stream: TextIO,
    stream_name: str,
    ws: aiohttp.ClientWebSocketResponse,
    children: ChildRegistry,
) -> None:
    """Read lines from a child process stream and forward as LogMessage."""
    from gateway.message_types import LogMessage

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, stream.readline)
        if not line:
            break
        text = line.decode("utf-8", errors="replace").rstrip("\n\r")
        try:
            await ws.send_str(json.dumps(
                LogMessage(
                    group_id=group_id, pid=pid,
                    stream=stream_name, line=text,
                ).to_dict(),
            ))
        except Exception as exc:
            logger.debug("Failed to send log line: %s", exc)


async def _monitor_exit(
    group_id: str,
    pid: int,
    proc: subprocess.Popen,
    ws: aiohttp.ClientWebSocketResponse,
    children: ChildRegistry,
) -> None:
    """Wait for a child process to exit and notify the manager."""
    from gateway.message_types import AgentStoppedMessage

    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, proc.wait)

    exit_code = proc.returncode if proc.returncode is not None else -1
    children.remove(group_id)

    if exit_code == 0:
        reason = "exited normally"
    elif exit_code < 0:
        reason = f"killed by signal {-exit_code}"
    else:
        reason = f"exited with code {exit_code}"

    logger.info("Child process exited: group_id=%s pid=%d exit_code=%d reason=%s",
                group_id, pid, exit_code, reason)

    try:
        await ws.send_str(json.dumps(
            AgentStoppedMessage(
                group_id=group_id, pid=pid,
                exit_code=exit_code, reason=reason,
            ).to_dict(),
        ))
    except Exception as exc:
        logger.debug("Failed to send agent_stopped: %s", exc)


def stop_child_process(
    group_id: str,
    children: ChildRegistry,
    force: bool = False,
) -> bool:
    """Stop a child process.  Returns True if a process was found."""
    proc = children.get(group_id)
    if proc is None:
        return False
    if proc.poll() is not None:
        # Already exited
        children.remove(group_id)
        return True
    if force:
        proc.kill()
    else:
        proc.terminate()
    return True


# ---------------------------------------------------------------------------
# System info helpers
# ---------------------------------------------------------------------------


def get_cpu_cores() -> int:
    """Return the number of CPU cores, minimum 1."""
    count = os.cpu_count()
    return count if count else 1


def get_mem_info_gb() -> tuple:
    """Return (total_gb, available_gb) from /proc/meminfo.

    Falls back to (0.0, 0.0) on non-Linux platforms.
    """
    if platform.system() != "Linux":
        return (0.0, 0.0)
    try:
        meminfo: Dict[str, int] = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split()
                if len(parts) >= 2:
                    key = parts[0].rstrip(":")
                    val = int(parts[1])  # in kB
                    meminfo[key] = val
        total_kb = meminfo.get("MemTotal", 0)
        available_kb = meminfo.get("MemAvailable", 0)
        kb_to_gb = 1024 * 1024
        return (total_kb / kb_to_gb, available_kb / kb_to_gb)
    except Exception:
        return (0.0, 0.0)


# Module-level state for interval-based CPU measurement
_cpu_prev_total: Optional[int] = None
_cpu_prev_idle: Optional[int] = None


def _get_cpu_percent() -> float:
    """Return CPU usage percentage since the last call (interval-based).

    Uses delta between consecutive /proc/stat readings.
    Returns 0.0 on first call (no baseline), non-Linux, or errors.
    """
    global _cpu_prev_total, _cpu_prev_idle
    if platform.system() != "Linux":
        return 0.0
    try:
        with open("/proc/stat", "r") as f:
            line = f.readline()
        parts = line.split()
        if len(parts) < 5 or parts[0] != "cpu":
            return 0.0
        user = int(parts[1])
        nice = int(parts[2])
        system = int(parts[3])
        idle = int(parts[4])
        iowait = int(parts[5]) if len(parts) > 5 else 0
        irq = int(parts[6]) if len(parts) > 6 else 0
        softirq = int(parts[7]) if len(parts) > 7 else 0
        total = user + nice + system + idle + iowait + irq + softirq

        if _cpu_prev_total is None:
            # First reading -- store baseline, return 0
            _cpu_prev_total = total
            _cpu_prev_idle = idle
            return 0.0

        delta_total = total - _cpu_prev_total
        delta_idle = idle - _cpu_prev_idle
        _cpu_prev_total = total
        _cpu_prev_idle = idle

        if delta_total <= 0:
            return 0.0
        return (1.0 - delta_idle / delta_total) * 100.0
    except Exception:
        return 0.0


# ---------------------------------------------------------------------------
# Main daemon loop
# ---------------------------------------------------------------------------


async def run_daemon(
    manager_url: str,
    server_id: str,
    hermes_root: str,
    heartbeat_interval: int = 30,
) -> None:
    """Connect to the Manager WebSocket, register, and handle commands.

    Reconnects with exponential backoff on disconnect.
    """
    children = ChildRegistry()
    hostname = platform.node() or "unknown"
    cpu_cores = get_cpu_cores()
    mem_total_gb, _ = get_mem_info_gb()

    retry_delay = 1.0
    max_retry_delay = 60.0

    while True:
        try:
            logger.info("Connecting to manager at %s ...", manager_url)
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(manager_url) as ws:
                    retry_delay = 1.0

                    # Send register
                    _, mem_available_gb = get_mem_info_gb()
                    reg = build_register(
                        server_id=server_id,
                        hostname=hostname,
                        cpu_cores=cpu_cores,
                        mem_total_gb=mem_total_gb,
                        mem_available_gb=mem_available_gb,
                    )
                    await ws.send_str(reg)
                    logger.info("Registered: server_id=%s", server_id)

                    # Start heartbeat task
                    heartbeat_task = asyncio.create_task(
                        _heartbeat_loop(ws, children, heartbeat_interval),
                    )

                    try:
                        async for ws_msg in ws:
                            if ws_msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(ws_msg.data)
                                except json.JSONDecodeError:
                                    logger.warning("Invalid JSON from manager")
                                    continue

                                try:
                                    cmd = parse_manager_command(data)
                                except ValueError as exc:
                                    logger.warning("Bad command: %s", exc)
                                    continue

                                if cmd["type"] == "start_agent":
                                    asyncio.create_task(
                                        _handle_start(cmd, hermes_root, children, ws),
                                    )
                                elif cmd["type"] == "stop_agent":
                                    group_id = cmd["group_id"]
                                    force = cmd.get("force", False)
                                    stopped = stop_child_process(
                                        group_id, children, force=force,
                                    )
                                    if not stopped:
                                        logger.warning(
                                            "No running agent for %s", group_id,
                                        )

                            elif ws_msg.type in (
                                aiohttp.WSMsgType.ERROR,
                                aiohttp.WSMsgType.CLOSE,
                            ):
                                logger.warning("WebSocket closed: %s", ws_msg)
                                break
                    finally:
                        heartbeat_task.cancel()

        except (aiohttp.WSSLError, aiohttp.ClientError) as exc:
            logger.error("Connection error: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error: %s", exc)

        # Clean up orphaned child processes before reconnecting
        running = children.list_running()
        if running:
            logger.info("Stopping %d orphaned agents before reconnect: %s", len(running), running)
            for gid in running:
                stop_child_process(gid, children, force=True)

        logger.info("Reconnecting in %.1fs ...", retry_delay)
        await asyncio.sleep(retry_delay)
        retry_delay = min(retry_delay * 2, max_retry_delay)


async def _heartbeat_loop(
    ws: aiohttp.ClientWebSocketResponse,
    children: ChildRegistry,
    interval: int,
) -> None:
    """Send periodic heartbeat messages with running agent info and resource usage."""
    try:
        while True:
            await asyncio.sleep(interval)
            agents_info = children.get_running_agents_info()
            mem_total_gb, mem_available_gb = get_mem_info_gb()
            mem_used_gb = max(0.0, mem_total_gb - mem_available_gb)
            cpu_percent = _get_cpu_percent()
            hb = build_heartbeat(
                running_agents=agents_info,
                mem_used_gb=round(mem_used_gb, 2),
                mem_available_gb=round(mem_available_gb, 2),
                cpu_percent=round(cpu_percent, 1),
            )
            await ws.send_str(hb)
            logger.debug("Heartbeat sent: %d running agents, %.1f GB used, %.0f%% CPU",
                         len(agents_info), mem_used_gb, cpu_percent)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        logger.debug("Heartbeat error: %s", exc)


async def _handle_start(
    cmd: Dict[str, Any],
    hermes_root: str,
    children: ChildRegistry,
    ws: aiohttp.ClientWebSocketResponse,
) -> None:
    """Handle a start_agent command from the manager."""
    group_id = cmd["group_id"]
    if children.get(group_id) is not None:
        logger.warning("Agent already running for %s", group_id)
        return

    await start_child_process(
        group_id=group_id,
        gateway_url=cmd.get("gateway_url", ""),
        profile=cmd.get("profile", ""),
        hermes_root=hermes_root,
        hermes_home=cmd.get("hermes_home", ""),
        model=cmd.get("model", ""),
        children=children,
        ws=ws,
    )


# ---------------------------------------------------------------------------
# Dynamic Hermes installation
# ---------------------------------------------------------------------------


def _install_hermes(source: str) -> None:
    """Install Hermes Agent from a pip source.

    Args:
        source: A pip-installable target. Can be:
            - Local path: /hermes or ./hermes-agent
            - Git URL: git+https://github.com/user/hermes-agent.git
            - PyPI package: hermes-agent or hermes-agent==1.0
    """
    logger.info("Installing Hermes from: %s", source)
    cmd = [sys.executable, "-m", "pip", "install", "--quiet", source]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        logger.error("Failed to install Hermes from %s:\nstdout: %s\nstderr: %s",
                      source, result.stdout, result.stderr)
        raise RuntimeError(f"pip install failed (exit code {result.returncode})")
    logger.info("Hermes installed successfully from: %s", source)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Hermes Distributed Agent Daemon",
    )
    parser.add_argument(
        "--manager-url", default="",
        help="Manager WebSocket URL (ws://host:port/ws), or DA_MANAGER_URL env var",
    )
    parser.add_argument(
        "--server-id", default="",
        help="Unique server ID, or DA_SERVER_ID env var",
    )
    parser.add_argument(
        "--hermes-root", default="",
        help="Root directory of the Hermes installation",
    )
    parser.add_argument(
        "--heartbeat-interval", type=int, default=0,
        help="Heartbeat interval in seconds, or DA_HEARTBEAT_INTERVAL env var (default: 30)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    # Resolve from env vars if CLI args not provided
    manager_url = args.manager_url or os.environ.get("DA_MANAGER_URL", "")
    server_id = args.server_id or os.environ.get("DA_SERVER_ID", "")
    heartbeat_interval = args.heartbeat_interval or int(
        os.environ.get("DA_HEARTBEAT_INTERVAL", "30")
    )

    if not manager_url or not server_id:
        parser.error("--manager-url (or DA_MANAGER_URL) and --server-id (or DA_SERVER_ID) are required")

    # Dynamic Hermes installation
    pip_source = os.environ.get("DA_HERMES_PIP_SOURCE", "").strip()
    if pip_source:
        _install_hermes(pip_source)

    hermes_root = args.hermes_root or os.environ.get("DA_HERMES_ROOT", "")
    if hermes_root:
        os.environ["HERMES_ROOT"] = hermes_root

    asyncio.run(run_daemon(
        manager_url=manager_url,
        server_id=server_id,
        hermes_root=hermes_root,
        heartbeat_interval=heartbeat_interval,
    ))


if __name__ == "__main__":
    main()
