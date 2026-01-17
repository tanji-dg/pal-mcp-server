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
MONITOR_TRANSPORT=${MONITOR_TRANSPORT:-dual}
MONITOR_SOCKET_PATH=${MONITOR_SOCKET_PATH:-/tmp/pal-monitor.sock}
MONITOR_WS_PORT=${MONITOR_WS_PORT:-9876}
LOG_FILE="/tmp/pal-monitor.log"

echo "Starting PAL MCP Monitor Coordinator..."
echo "  Transport: $MONITOR_TRANSPORT"
echo "  Dashboard: http://localhost:$MONITOR_WS_PORT"
echo "  Socket:    $MONITOR_SOCKET_PATH"

# Run
if [[ "$1" == "bg" || "$1" == "background" ]]; then
    # Background mode
    nohup python monitor/run_coordinator.py > "$LOG_FILE" 2>&1 &
    PID=$!
    echo "Coordinator started in background (PID: $PID)"
    echo "Logs: $LOG_FILE"
    echo "Stop with: kill $PID"
else
    # Foreground mode
    python monitor/run_coordinator.py
fi
