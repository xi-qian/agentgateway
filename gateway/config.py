"""Gateway configuration loading."""
from __future__ import annotations

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
    """Load config from a YAML file path, dict, or return defaults.

    Args:
        source: A YAML file path (str/Path), a dict with config keys, or None
                for all-default configuration.

    Returns:
        A GatewayConfig instance.
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

    cfg = GatewayConfig(
        ws_host=gw.get("ws_host", GatewayConfig.ws_host),
        ws_port=gw.get("ws_port", GatewayConfig.ws_port),
        media_port=gw.get("media_port", GatewayConfig.media_port),
        media_dir=gw.get("media_dir", GatewayConfig.media_dir),
        idle_timeout_minutes=gw.get(
            "idle_timeout_minutes", GatewayConfig.idle_timeout_minutes
        ),
        heartbeat_interval_seconds=gw.get(
            "heartbeat_interval_seconds", GatewayConfig.heartbeat_interval_seconds
        ),
        heartbeat_timeout_seconds=gw.get(
            "heartbeat_timeout_seconds", GatewayConfig.heartbeat_timeout_seconds
        ),
        manager_url=gw.get("manager_url"),
        profile_mappings=profiles if profiles else {"default": "default"},
    )
    return cfg
