"""Hermes Distributed plugin — bridges Hermes Gateway to the distributed Gateway."""


def register(ctx) -> None:
    """Patch GatewayRunner._create_adapter to inject InternalWSAdapter.

    When the Hermes Gateway is configured with a WEBHOOK platform whose
    ``extra`` dict contains ``hermes_distributed_gateway_url``, this plugin
    replaces the default WebhookAdapter with our InternalWSAdapter.
    """
    from gateway.run import GatewayRunner
    from gateway.config import Platform

    _original_create = GatewayRunner._create_adapter

    def _patched_create_adapter(self, platform, config):
        if platform == Platform.WEBHOOK:
            extra = getattr(config, "extra", {}) or {}
            if extra.get("hermes_distributed_gateway_url"):
                try:
                    from agent.plugin.ws_adapter import InternalWSAdapter
                    return InternalWSAdapter(config)
                except Exception as e:
                    import logging
                    logging.getLogger("hermes_distributed.plugin").error(
                        "Failed to create InternalWSAdapter: %s", e,
                    )
        return _original_create(self, platform, config)

    GatewayRunner._create_adapter = _patched_create_adapter
