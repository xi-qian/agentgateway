"""Manager configuration."""
from __future__ import annotations

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
    """Load config from YAML path, dict, or return defaults."""
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
    return ManagerConfig(
        rest_host=mgr.get("rest_host", ManagerConfig.rest_host),
        rest_port=mgr.get("rest_port", ManagerConfig.rest_port),
        ws_path=mgr.get("ws_path", ManagerConfig.ws_path),
        heartbeat_timeout_seconds=mgr.get(
            "heartbeat_timeout_seconds", ManagerConfig.heartbeat_timeout_seconds
        ),
        idle_timeout_minutes=mgr.get(
            "idle_timeout_minutes", ManagerConfig.idle_timeout_minutes
        ),
        scheduler_interval_seconds=mgr.get(
            "scheduler_interval_seconds", ManagerConfig.scheduler_interval_seconds
        ),
        db_path=mgr.get("db_path", ManagerConfig.db_path),
        log_retention_days=mgr.get(
            "log_retention_days", ManagerConfig.log_retention_days
        ),
        gateway_url=mgr.get("gateway_url"),
    )
