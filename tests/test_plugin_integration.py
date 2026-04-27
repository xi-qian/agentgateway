"""Test that the plugin loads and registers correctly."""
import pytest


class TestPluginRegistration:
    def test_register_function_exists(self):
        from agent.plugin import register
        assert callable(register)

    def test_plugin_yaml_valid(self):
        import yaml
        from pathlib import Path
        plugin_dir = Path(__file__).parent.parent / "agent" / "plugin"
        manifest = yaml.safe_load((plugin_dir / "plugin.yaml").read_text())
        assert manifest["name"] == "hermes-distributed"
        assert "version" in manifest

    def test_ws_adapter_module_exists(self):
        from agent.plugin.ws_adapter import InternalWSAdapter
        assert InternalWSAdapter is not None

    def test_check_webhook_requirements_returns_true(self):
        from agent.plugin.ws_adapter import check_webhook_requirements
        assert check_webhook_requirements() is True
