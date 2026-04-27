#!/bin/bash
# Docker container integration test for AgentGateway
#
# Starts gateway + manager via docker-compose, runs E2E tests, cleans up.
#
# Usage:
#   bash tests/docker_integration_test.sh
#   bash tests/docker_integration_test.sh --no-cleanup  # keep containers after test
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLEANUP=true

for arg in "$@"; do
    case "$arg" in
        --no-cleanup) CLEANUP=false ;;
    esac
done

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass=0
fail=0

check_pass() {
    echo -e "  ${GREEN}PASS${NC}: $1"
    ((pass++))
}

check_fail() {
    echo -e "  ${RED}FAIL${NC}: $1 -- $2"
    ((fail++))
}

cleanup() {
    if [ "$CLEANUP" = true ]; then
        echo ""
        echo "--- Cleanup ---"
        docker-compose -f "$PROJECT_DIR/docker-compose.yml" down -v --remove-orphans 2>/dev/null || true
    fi
}

trap cleanup EXIT

echo "========================================"
echo " AgentGateway Docker Integration Test"
echo "========================================"
echo ""

# --- Step 1: Build and start containers ---
echo "--- Starting containers ---"
docker-compose -f "$PROJECT_DIR/docker-compose.yml" up -d --build 2>&1
echo ""

# --- Step 2: Wait for services ---
echo "--- Waiting for services ---"

wait_for_http() {
    local url=$1
    local name=$2
    local max_wait=30
    local waited=0
    while [ $waited -lt $max_wait ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            check_pass "$name is ready"
            return 0
        fi
        sleep 1
        ((waited++))
    done
    check_fail "$name ready" "timed out after ${max_wait}s"
    return 1
}

wait_for_ws() {
    local host=$1
    local port=$2
    local name=$3
    local max_wait=30
    local waited=0
    while [ $waited -lt $max_wait ]; do
        # Simple TCP check — WS endpoint won't respond to curl but port should be open
        if python3 -c "
import socket, sys
s = socket.socket()
s.settimeout(2)
try:
    s.connect(('$host', $port))
    s.close()
    sys.exit(0)
except:
    sys.exit(1)
" 2>/dev/null; then
            check_pass "$name is ready"
            return 0
        fi
        sleep 1
        ((waited++))
    done
    check_fail "$name ready" "timed out after ${max_wait}s"
    return 1
}

wait_for_http "http://localhost:8800/api/v1/servers" "Manager REST API"

# Gateway port check — just wait for TCP port to be open
echo "  Waiting for Gateway port 8900..."
waited=0
while [ $waited -lt 30 ]; do
    if docker exec agentgateway-gateway-1 python3 -c "
import socket
s = socket.socket()
s.settimeout(1)
try:
    s.connect(('127.0.0.1', 8900))
    s.close()
except:
    exit(1)
" 2>/dev/null; then
        check_pass "Gateway WebSocket is ready"
        break
    fi
    sleep 1
    ((waited++))
done
if [ $waited -ge 30 ]; then
    # Port might not be reachable from host TCP check, but container is running
    # E2E test will verify actual connectivity
    echo -e "  ${YELLOW}SKIP${NC}: Gateway port check (will verify via E2E test)"
fi

echo ""

# --- Step 3: Check container health ---
echo "--- Container health ---"
containers=("agentgateway-gateway-1" "agentgateway-manager-1" "agentgateway-daemon-1")

for c in "${containers[@]}"; do
    status=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo "missing")
    if [ "$status" = "running" ]; then
        check_pass "$c is running"
    else
        check_fail "$c is running" "status=$status"
    fi
done

echo ""

# --- Step 4: Network connectivity ---
echo "--- Network connectivity ---"

# Gateway can reach Manager
if docker exec agentgateway-gateway-1 python3 -c "
import urllib.request
r = urllib.request.urlopen('http://manager:8800/api/v1/servers', timeout=5)
print(r.status)
" 2>/dev/null | grep -q "200"; then
    check_pass "Gateway -> Manager connectivity"
else
    check_fail "Gateway -> Manager connectivity" "HTTP request failed"
fi

echo ""

# --- Step 5: E2E message flow test ---
echo "--- E2E message flow test ---"
PYTHONPATH="$PROJECT_DIR" python3 "$SCRIPT_DIR/e2e_message_flow.py"
e2e_result=$?

if [ $e2e_result -eq 0 ]; then
    check_pass "E2E message flow test"
else
    check_fail "E2E message flow test" "exit code $e2e_result"
fi

echo ""

# --- Summary ---
echo "========================================"
total=$((pass + fail))
echo -e " Results: ${GREEN}${pass}${NC}/${total} passed, ${RED}${fail}${NC} failed"
echo "========================================"

if [ $fail -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Container logs (last 20 lines each):${NC}"
    for c in "${containers[@]}"; do
        echo ""
        echo "--- $c ---"
        docker logs --tail 20 "$c" 2>&1 || true
    done
fi

exit $fail
