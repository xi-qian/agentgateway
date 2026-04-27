"""Static file server for media attachments.

Serves files from a configurable directory (e.g. /tmp/hermes-distributed/media)
via HTTP, so that agent services can reference uploaded media by URL.

Usage:
    python -m gateway.media_server --dir /tmp/media --port 8901
"""
from __future__ import annotations

import argparse
import logging
import os
from typing import Optional

from aiohttp import web

logger = logging.getLogger("gateway.media")


class MediaServer:
    """Simple HTTP server that serves static files from a directory."""

    def __init__(self, directory: str, host: str = "0.0.0.0", port: int = 8901) -> None:
        self.directory = os.path.abspath(directory)
        self.host = host
        self.port = port

    def create_app(self) -> web.Application:
        os.makedirs(self.directory, exist_ok=True)
        app = web.Application()
        app.router.add_static("/media", self.directory, follow_symlinks=False)
        app.router.add_get("/health", self._health)
        return app

    async def _health(self, request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def start(self) -> None:
        app = self.create_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.port)
        await site.start()
        logger.info("Media server on http://%s:%d/media (dir: %s)", self.host, self.port, self.directory)


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Distributed Media Server")
    parser.add_argument("--dir", default="/tmp/hermes-distributed/media", help="Media directory")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8901, help="Bind port")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    async def _run():
        server = MediaServer(directory=args.dir, host=args.host, port=args.port)
        await server.start()
        try:
            while True:
                import asyncio
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass

    import asyncio
    asyncio.run(_run())


if __name__ == "__main__":
    main()
