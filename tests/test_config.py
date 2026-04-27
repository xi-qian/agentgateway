import pytest
from pathlib import Path

from gateway.config import GatewayConfig, load_config


class TestGatewayConfig:
    def test_default_values(self):
        cfg = GatewayConfig()
        assert cfg.ws_host == "0.0.0.0"
        assert cfg.ws_port == 8900
        assert cfg.media_port == 8901
        assert cfg.idle_timeout_minutes == 30

    def test_load_from_dict(self):
        cfg = load_config({"gateway": {"ws_port": 9999}})
        assert cfg.ws_port == 9999

    def test_load_from_yaml(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("gateway:\n  ws_port: 7777\n")
        cfg = load_config(cfg_file)
        assert cfg.ws_port == 7777

    def test_missing_file_returns_defaults(self):
        cfg = load_config("/nonexistent/config.yaml")
        assert cfg.ws_port == 8900

    def test_profile_mappings(self):
        cfg = load_config({
            "profiles": {
                "telegram:group:1001": "research-profile",
                "default": "general",
            },
        })
        assert cfg.resolve_profile("telegram:group:1001") == "research-profile"
        assert cfg.resolve_profile("discord:group:999") == "general"

    def test_resolve_profile_defaults_to_default_key(self):
        cfg = GatewayConfig()
        assert cfg.resolve_profile("unknown:group") == "default"

    def test_load_config_none_returns_defaults(self):
        cfg = load_config(None)
        assert cfg.ws_host == "0.0.0.0"
        assert cfg.ws_port == 8900
