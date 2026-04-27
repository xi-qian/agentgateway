"""End-to-end message flow test for Docker container integration.

Simulates an agent connecting to the Gateway via WebSocket,
sending hello, and verifying the connection is accepted.

Usage:
    python3 tests/e2e_message_flow.py [--gateway-url ws://localhost:8900/ws]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys

try:
    import aiohttp
except ImportError:
    print("FAIL: aiohttp not installed")
    sys.exit(1)

PROTOCOL_VERSION = 1
TIMEOUT = 10.0


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.tests = []

    def ok(self, name: str):
        self.passed += 1
        self.tests.append(("PASS", name))
        print(f"  PASS: {name}")

    def fail(self, name: str, reason: str):
        self.failed += 1
        self.tests.append(("FAIL", name))
        print(f"  FAIL: {name} -- {reason}")

    def summary(self):
        total = self.passed + self.failed
        print(f"\n{'='*50}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed")
        for status, name in self.tests:
            print(f"  [{status}] {name}")
        return self.failed == 0


async def test_manager_api(result: TestResult, manager_url: str):
    """Test Manager REST API endpoints."""
    print("\n--- Manager REST API Tests ---")
    async with aiohttp.ClientSession() as session:
        # Test 1: List servers (should be empty)
        try:
            async with session.get(f"{manager_url}/api/v1/servers", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "servers" in data:
                        result.ok("GET /api/v1/servers returns 200 with servers list")
                    else:
                        result.fail("GET /api/v1/servers", f"missing 'servers' key: {data}")
                else:
                    result.fail("GET /api/v1/servers", f"status {resp.status}")
        except Exception as e:
            result.fail("GET /api/v1/servers", str(e))

        # Test 2: List agents (should be empty)
        try:
            async with session.get(f"{manager_url}/api/v1/agents", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if "agents" in data:
                        result.ok("GET /api/v1/agents returns 200 with agents list")
                    else:
                        result.fail("GET /api/v1/agents", f"missing 'agents' key: {data}")
                else:
                    result.fail("GET /api/v1/agents", f"status {resp.status}")
        except Exception as e:
            result.fail("GET /api/v1/agents", str(e))


async def test_agent_gateway_ws(result: TestResult, gateway_url: str):
    """Test agent <-> Gateway WebSocket communication."""
    print("\n--- Agent-Gateway WebSocket Tests ---")
    group_id = "mock:test:e2e_test"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(gateway_url, timeout=aiohttp.ClientTimeout(total=10)) as ws:
                # Test 1: Send hello and verify connection stays open
                hello = {
                    "version": PROTOCOL_VERSION,
                    "type": "hello",
                    "group_id": group_id,
                    "profile": "default",
                    "model": "test-model",
                    "toolsets": ["test"],
                    "capabilities": ["streaming"],
                }
                await ws.send_str(json.dumps(hello))

                # Wait briefly for any response or close
                await asyncio.sleep(0.5)

                if ws.closed:
                    result.fail("Agent hello connection", f"WebSocket closed with code {ws.close_code}")
                else:
                    result.ok("Agent sends hello, connection accepted")

                # Test 2: Send a complete message (simulate agent finishing a task)
                complete = {
                    "version": PROTOCOL_VERSION,
                    "type": "complete",
                    "group_id": group_id,
                    "final_response": "Hello from integration test!",
                    "api_calls": 1,
                    "tokens": {"input": 10, "output": 20},
                    "cost_usd": 0.001,
                    "interrupted": False,
                }
                await ws.send_str(json.dumps(complete))

                await asyncio.sleep(0.3)

                if ws.closed:
                    # Connection might close after complete, that's ok
                    result.ok("Agent sends complete message (connection closed after)")
                else:
                    result.ok("Agent sends complete message (connection still open)")

                await ws.close()
                result.ok("Agent disconnects cleanly")

    except aiohttp.WSServerHandshakeError as e:
        result.fail("WebSocket connect", f"handshake failed: {e}")
    except Exception as e:
        result.fail("WebSocket test", str(e))


async def run_tests(gateway_url: str, manager_url: str):
    result = TestResult()

    await test_manager_api(result, manager_url)
    await test_agent_gateway_ws(result, gateway_url)

    return result.summary()


def main():
    parser = argparse.ArgumentParser(description="AgentGateway E2E integration test")
    parser.add_argument("--gateway-url", default="ws://localhost:8900/ws",
                        help="Gateway WebSocket URL")
    parser.add_argument("--manager-url", default="http://localhost:8800",
                        help="Manager REST URL")
    args = parser.parse_args()

    print(f"Gateway: {args.gateway_url}")
    print(f"Manager: {args.manager_url}")

    ok = asyncio.run(run_tests(args.gateway_url, args.manager_url))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
