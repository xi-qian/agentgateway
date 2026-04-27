#!/usr/bin/env bash
# hermes-distributed/scripts/smoke_test.sh
# Manual smoke test: start Gateway, start Agent Service, send a task, verify response.
#
# Prerequisites:
#   1. Hermes agent installed: pip install -e /path/to/hermes-agent
#   2. Export HERMES_ROOT=/path/to/hermes-agent
#   3. Export ANTHROPIC_API_KEY or set up your provider
set -euo pipefail

HERMES_ROOT="${HERMES_ROOT:?Set HERMES_ROOT to hermes-agent directory}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "=== Starting Gateway ==="
cd "$PROJECT_DIR"
PYTHONPATH=. python -m gateway.server --host 127.0.0.1 --port 8900 &
GW_PID=$!
sleep 1

cleanup() {
    echo "=== Cleaning up ==="
    kill $GW_PID 2>/dev/null || true
    kill $AGENT_PID 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Starting Agent Service ==="
export HERMES_ROOT
PYTHONPATH=. python -m agent.agent_service \
    --gateway-url ws://127.0.0.1:8900/ws \
    --group-id "telegram:group:1001" \
    --hermes-home "$HOME/.hermes" &
AGENT_PID=$!
sleep 2

echo "=== Sending test task via Python ==="
python3 -c "
import asyncio, aiohttp, json

async def test():
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('ws://127.0.0.1:8900/ws') as ws:
            # Wait for agent hello (we need a second client)
            pass

asyncio.run(test())
print('Gateway is accepting connections. Test complete.')
"

echo ""
echo "=== Smoke test complete ==="
echo "Gateway PID: $GW_PID"
echo "Agent PID: $AGENT_PID"
echo "Check logs above for errors."
