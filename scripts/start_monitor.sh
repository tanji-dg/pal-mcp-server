#!/bin/bash
# Start the PAL MCP Monitor Coordinator
#
# Usage: ./scripts/start_monitor.sh [background]
#   background: If "bg" or "background" is provided, runs in background

set -e

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Check for virtual environment
if [ -d ".pal_venv" ]; then
    source .pal_venv/bin/activate
elif [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "Error: Virtual environment not found (.pal_venv or venv)"
    exit 1
fi

# Load environment variables if .env exists
if [ -f ".env" ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Default settings from .env or defaults
MONITOR_TRANSPORT=${MONITOR_TRANSPORT:-unix}
MONITOR_SOCKET_PATH=${MONITOR_SOCKET_PATH:-/tmp/pal-monitor.sock}
MONITOR_WS_PORT=${MONITOR_WS_PORT:-9876}
LOG_FILE="/tmp/pal-monitor.log"

# Determine flags based on transport
# If transport is unix or dual, we force dual mode for coordinator
# to ensure dashboard (HTTP) is available alongside Unix socket
COORD_ARGS=""
if [[ "$MONITOR_TRANSPORT" == "unix" || "$MONITOR_TRANSPORT" == "dual" ]]; then
    COORD_ARGS="--dual --socket $MONITOR_SOCKET_PATH"
elif [[ "$MONITOR_TRANSPORT" == "http" ]]; then
    COORD_ARGS="--transport http --port $MONITOR_WS_PORT"
fi

echo "Starting PAL MCP Monitor Coordinator..."
echo "  Transport: $MONITOR_TRANSPORT"
echo "  Dashboard: http://localhost:$MONITOR_WS_PORT"
if [[ "$MONITOR_TRANSPORT" != "http" ]]; then
    echo "  Socket:    $MONITOR_SOCKET_PATH"
fi

# Run
if [[ "$1" == "bg" || "$1" == "background" ]]; then
    # Background mode
    nohup python monitor/run_coordinator.py $COORD_ARGS > "$LOG_FILE" 2>&1 &
    PID=$!
    echo "Coordinator started in background (PID: $PID)"
    echo "Logs: $LOG_FILE"
    echo "Stop with: kill $PID"
else
    # Foreground mode
    python monitor/run_coordinator.py $COORD_ARGS
fi
