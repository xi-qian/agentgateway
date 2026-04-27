"""SQLite-backed agent registry for the Manager service."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


_SCHEMA = """
CREATE TABLE IF NOT EXISTS servers (
    server_id TEXT PRIMARY KEY,
    hostname TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'online',
    cpu_cores INTEGER NOT NULL DEFAULT 0,
    mem_total_gb REAL NOT NULL DEFAULT 0,
    mem_available_gb REAL NOT NULL DEFAULT 0,
    mem_used_gb REAL NOT NULL DEFAULT 0,
    cpu_percent REAL NOT NULL DEFAULT 0,
    registered_at TEXT NOT NULL,
    last_heartbeat TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    group_id TEXT PRIMARY KEY,
    server_id TEXT NOT NULL,
    pid INTEGER,
    status TEXT NOT NULL DEFAULT 'starting',
    profile TEXT NOT NULL DEFAULT 'default',
    model TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    last_active_at TEXT NOT NULL,
    ended_at TEXT,
    exit_code INTEGER,
    FOREIGN KEY (server_id) REFERENCES servers(server_id)
);

CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_id TEXT NOT NULL,
    pid INTEGER NOT NULL,
    timestamp TEXT NOT NULL,
    stream TEXT NOT NULL,
    line TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    group_id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL DEFAULT '',
    history TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL
);
"""


class AgentRegistry:
    """Async SQLite registry for servers, agents, and logs."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self._db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    async def init(self) -> None:
        """Create tables if they don't exist."""
        def _do() -> None:
            self._get_conn().executescript(_SCHEMA)

        await asyncio.get_event_loop().run_in_executor(None, _do)

    async def _execute(self, sql: str, params: tuple = ()) -> None:
        def _do() -> None:
            self._get_conn().execute(sql, params)

        await asyncio.get_event_loop().run_in_executor(None, _do)

    async def _fetchall(self, sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
        def _do() -> List[Dict[str, Any]]:
            rows = self._get_conn().execute(sql, params).fetchall()
            return [dict(r) for r in rows]

        return await asyncio.get_event_loop().run_in_executor(None, _do)

    async def _fetchone(self, sql: str, params: tuple = ()) -> Optional[Dict[str, Any]]:
        def _do() -> Optional[Dict[str, Any]]:
            row = self._get_conn().execute(sql, params).fetchone()
            return dict(row) if row else None

        return await asyncio.get_event_loop().run_in_executor(None, _do)

    async def close(self) -> None:
        def _do() -> None:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

        await asyncio.get_event_loop().run_in_executor(None, _do)

    # ------------------------------------------------------------------
    # Server operations
    # ------------------------------------------------------------------

    async def register_server(
        self,
        *,
        server_id: str,
        hostname: str,
        cpu_cores: int = 0,
        mem_total_gb: float = 0,
        mem_available_gb: float = 0,
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            """INSERT OR REPLACE INTO servers
               (server_id, hostname, status, cpu_cores, mem_total_gb,
                mem_available_gb, mem_used_gb, cpu_percent,
                registered_at, last_heartbeat)
               VALUES (?, ?, 'online', ?, ?, ?, 0, 0, ?, ?)""",
            (server_id, hostname, cpu_cores, mem_total_gb,
             mem_available_gb, now, now),
        )

    async def update_heartbeat(
        self,
        server_id: str,
        *,
        mem_available_gb: float,
        mem_used_gb: float,
        cpu_percent: float,
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            """UPDATE servers
               SET mem_available_gb = ?, mem_used_gb = ?, cpu_percent = ?,
                   last_heartbeat = ?, status = 'online'
               WHERE server_id = ?""",
            (mem_available_gb, mem_used_gb, cpu_percent, now, server_id),
        )

    async def mark_server_lost(self, server_id: str) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            "UPDATE servers SET status = 'lost' WHERE server_id = ?",
            (server_id,),
        )
        # Also mark all agents on this server as lost
        await self._execute(
            """UPDATE agents SET status = 'lost', ended_at = ?
               WHERE server_id = ? AND status IN ('starting', 'running')""",
            (now, server_id),
        )

    async def list_servers(self) -> List[Dict[str, Any]]:
        return await self._fetchall("SELECT * FROM servers")

    async def get_timedout_servers(self, *, timeout_seconds: int) -> List[Dict[str, Any]]:
        cutoff = (
            datetime.utcnow()
            .isoformat(timespec="seconds")
        )
        # We need to compare as datetime. SQLite compares ISO strings lexicographically,
        # which works for ISO-8601 UTC format.  Subtract timeout from now.
        from datetime import timedelta
        cutoff_dt = datetime.utcnow() - timedelta(seconds=timeout_seconds)
        cutoff_str = cutoff_dt.isoformat()
        return await self._fetchall(
            "SELECT * FROM servers WHERE status = 'online' AND last_heartbeat < ?",
            (cutoff_str,),
        )

    async def select_best_server(self) -> Optional[str]:
        """Return server_id with fewest active agents (starting/running)."""
        row = await self._fetchone(
            """SELECT s.server_id, COUNT(a.group_id) AS agent_count
               FROM servers s
               LEFT JOIN agents a
                   ON a.server_id = s.server_id
                   AND a.status IN ('starting', 'running')
               WHERE s.status = 'online'
               GROUP BY s.server_id
               ORDER BY agent_count ASC, s.mem_used_gb ASC
               LIMIT 1"""
        )
        if row is None or row.get("server_id") is None:
            return None
        return row["server_id"]

    # ------------------------------------------------------------------
    # Agent operations
    # ------------------------------------------------------------------

    async def create_agent(
        self,
        *,
        group_id: str,
        server_id: str,
        profile: str = "default",
        model: str = "",
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            """INSERT OR REPLACE INTO agents
               (group_id, server_id, pid, status, profile, model,
                created_at, last_active_at)
               VALUES (?, ?, NULL, 'starting', ?, ?, ?, ?)""",
            (group_id, server_id, profile, model, now, now),
        )

    async def agent_started(self, group_id: str, *, pid: int) -> None:
        await self._execute(
            "UPDATE agents SET pid = ?, status = 'running' WHERE group_id = ?",
            (pid, group_id),
        )

    async def agent_stopped(
        self,
        group_id: str,
        *,
        exit_code: int,
        reason: Optional[str] = None,
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            "UPDATE agents SET status = 'stopped', ended_at = ?, exit_code = ? WHERE group_id = ?",
            (now, exit_code, group_id),
        )

    async def touch_agent_activity(self, group_id: str) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            "UPDATE agents SET last_active_at = ? WHERE group_id = ?",
            (now, group_id),
        )

    async def get_agent(self, group_id: str) -> Optional[Dict[str, Any]]:
        return await self._fetchone(
            "SELECT * FROM agents WHERE group_id = ?",
            (group_id,),
        )

    async def list_agents(self) -> List[Dict[str, Any]]:
        return await self._fetchall("SELECT * FROM agents")

    async def delete_agent(self, group_id: str) -> None:
        await self._execute("DELETE FROM agents WHERE group_id = ?", (group_id,))

    async def get_idle_agents(self, *, timeout_minutes: int) -> List[Dict[str, Any]]:
        from datetime import timedelta
        cutoff_dt = datetime.utcnow() - timedelta(minutes=timeout_minutes)
        cutoff_str = cutoff_dt.isoformat()
        return await self._fetchall(
            """SELECT * FROM agents
               WHERE status IN ('running', 'starting') AND last_active_at < ?""",
            (cutoff_str,),
        )

    # ------------------------------------------------------------------
    # Log operations
    # ------------------------------------------------------------------

    async def append_log(
        self,
        *,
        group_id: str,
        pid: int,
        stream: str,
        line: str,
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            """INSERT INTO logs (group_id, pid, timestamp, stream, line)
               VALUES (?, ?, ?, ?, ?)""",
            (group_id, pid, now, stream, line),
        )

    async def get_logs(
        self,
        group_id: str,
        *,
        tail: Optional[int] = None,
        since: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if tail is not None:
            # Fetch newest `tail` rows in DESC order, then reverse for chronological.
            rows = await self._fetchall(
                """SELECT * FROM logs WHERE group_id = ?
                   ORDER BY id DESC LIMIT ?""",
                (group_id, tail),
            )
            rows.reverse()
            return rows

        where = "WHERE group_id = ?"
        params: list = [group_id]
        if since is not None:
            where += " AND timestamp >= ?"
            params.append(since)

        return await self._fetchall(
            f"SELECT * FROM logs {where} ORDER BY id ASC",
            tuple(params),
        )

    async def delete_old_logs(self, *, retention_days: int) -> int:
        """Delete log entries older than retention_days. Returns count deleted."""
        from datetime import timedelta
        cutoff_dt = datetime.utcnow() - timedelta(days=retention_days)
        cutoff_str = cutoff_dt.isoformat()
        def _do() -> int:
            cursor = self._get_conn().execute(
                "DELETE FROM logs WHERE timestamp < ?", (cutoff_str,),
            )
            return cursor.rowcount
        return await asyncio.get_event_loop().run_in_executor(None, _do)

    # ------------------------------------------------------------------
    # Session operations
    # ------------------------------------------------------------------

    async def save_session(
        self,
        group_id: str,
        history: list,
        session_id: str = "",
    ) -> None:
        now = datetime.utcnow().isoformat()
        await self._execute(
            """INSERT OR REPLACE INTO sessions (group_id, session_id, history, updated_at)
               VALUES (?, ?, ?, ?)""",
            (group_id, session_id, json.dumps(history), now),
        )

    async def load_session(self, group_id: str) -> Optional[Dict[str, Any]]:
        row = await self._fetchone(
            "SELECT * FROM sessions WHERE group_id = ?", (group_id,)
        )
        if row is None:
            return None
        return {
            "group_id": row["group_id"],
            "session_id": row["session_id"],
            "history": json.loads(row["history"]),
            "updated_at": row["updated_at"],
        }
