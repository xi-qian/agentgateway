"""Manager configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


@dataclass
class ManagerConfig:
    rest_host: str = "0.0.0.0"
    rest_port: int = 8800
    ws_path: str = "/ws"
    heartbeat_timeout_seconds: int = 90
    idle_timeout_minutes: int = 30
    scheduler_interval_seconds: int = 60
    db_path: str = ":memory:"
    log_retention_days: int = 7
    gateway_url: Optional[str] = None


def load_manager_config(
    source: Union[str, Path, Dict[str, Any], None] = None,
) -> ManagerConfig:
    """Load config from env vars, YAML path, or dict.

    Priority: env var > YAML > defaults.

    Env vars: MG_HOST, MG_PORT, MG_DB_PATH, MG_GATEWAY_URL,
              MG_IDLE_TIMEOUT, MG_HEALTH_TIMEOUT, MG_SCHEDULER_INTERVAL
    """
    data: Dict[str, Any] = {}
    if source is None:
        pass
    elif isinstance(source, dict):
        data = source
    else:
        p = Path(source)
        if p.exists():
            with open(p) as f:
                data = yaml.safe_load(f) or {}

    mgr = data.get("manager", {})

    def _env(name: str, default: Optional[str] = None) -> Optional[str]:
        return os.environ.get(name, default)

    def _env_int(name: str, default: int) -> int:
        val = os.environ.get(name)
        if val is not None:
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
        return default

    return ManagerConfig(
        rest_host=_env("MG_HOST", mgr.get("rest_host", ManagerConfig.rest_host)),
        rest_port=_env_int("MG_PORT", mgr.get("rest_port", ManagerConfig.rest_port)),
        ws_path=_env("MG_WS_PATH", mgr.get("ws_path", ManagerConfig.ws_path)),
        heartbeat_timeout_seconds=_env_int("MG_HEALTH_TIMEOUT", mgr.get(
            "heartbeat_timeout_seconds", ManagerConfig.heartbeat_timeout_seconds
        )),
        idle_timeout_minutes=_env_int("MG_IDLE_TIMEOUT", mgr.get(
            "idle_timeout_minutes", ManagerConfig.idle_timeout_minutes
        )),
        scheduler_interval_seconds=_env_int("MG_SCHEDULER_INTERVAL", mgr.get(
            "scheduler_interval_seconds", ManagerConfig.scheduler_interval_seconds
        )),
        db_path=_env("MG_DB_PATH", mgr.get("db_path", ManagerConfig.db_path)),
        log_retention_days=_env_int("MG_LOG_RETENTION_DAYS", mgr.get(
            "log_retention_days", ManagerConfig.log_retention_days
        )),
        gateway_url=_env("MG_GATEWAY_URL", mgr.get("gateway_url")),
    )
