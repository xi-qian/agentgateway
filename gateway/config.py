"""Gateway configuration loading."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml


@dataclass
class GatewayConfig:
    ws_host: str = "0.0.0.0"
    ws_port: int = 8900
    media_port: int = 8901
    media_dir: str = "/tmp/hermes-distributed/media"
    idle_timeout_minutes: int = 30
    heartbeat_interval_seconds: int = 60
    heartbeat_timeout_seconds: int = 90
    manager_url: Optional[str] = None
    profile_mappings: Dict[str, str] = field(default_factory=lambda: {"default": "default"})

    def resolve_profile(self, group_id: str) -> str:
        """Look up the profile for a group_id, falling back to the 'default' key."""
        return self.profile_mappings.get(
            group_id, self.profile_mappings.get("default", "default")
        )


def load_config(source: Union[str, Path, Dict[str, Any], None] = None) -> GatewayConfig:
    """Load config from env vars, YAML file, or dict.

    Priority: env var > YAML > defaults.

    Env vars: GW_HOST, GW_PORT, GW_MANAGER_URL,
              GW_IDLE_TIMEOUT, GW_HEARTBEAT_INTERVAL, GW_HEARTBEAT_TIMEOUT
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

    gw = data.get("gateway", {})
    profiles = data.get("profiles", {})

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

    cfg = GatewayConfig(
        ws_host=_env("GW_HOST", gw.get("ws_host", GatewayConfig.ws_host)),
        ws_port=_env_int("GW_PORT", gw.get("ws_port", GatewayConfig.ws_port)),
        media_port=_env_int("GW_MEDIA_PORT", gw.get("media_port", GatewayConfig.media_port)),
        media_dir=_env("GW_MEDIA_DIR", gw.get("media_dir", GatewayConfig.media_dir)),
        idle_timeout_minutes=_env_int("GW_IDLE_TIMEOUT", gw.get(
            "idle_timeout_minutes", GatewayConfig.idle_timeout_minutes
        )),
        heartbeat_interval_seconds=_env_int("GW_HEARTBEAT_INTERVAL", gw.get(
            "heartbeat_interval_seconds", GatewayConfig.heartbeat_interval_seconds
        )),
        heartbeat_timeout_seconds=_env_int("GW_HEARTBEAT_TIMEOUT", gw.get(
            "heartbeat_timeout_seconds", GatewayConfig.heartbeat_timeout_seconds
        )),
        manager_url=_env("GW_MANAGER_URL", gw.get("manager_url")),
        profile_mappings=profiles if profiles else {"default": "default"},
    )
    return cfg
