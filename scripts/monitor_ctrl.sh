#!/bin/bash
# PAL MCP Monitor Control Script
#
# Usage: ./scripts/monitor_ctrl.sh [start|stop|restart|status]

set -e

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PID_FILE="/tmp/pal-monitor.pid"
LOG_FILE="/tmp/pal-monitor.log"
SOCKET_FILE="/tmp/pal-monitor.sock"

function usage() {
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
}

function get_pid() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE"
    fi
}

function is_running() {
    local pid=$(get_pid)
    if [ -n "$pid" ] && ps -p "$pid" > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

function start() {
    if is_running; then
        echo "Monitor is already running (PID: $(get_pid))"
        return 0
    fi

    echo "Starting monitor..."
    
    if [ -d ".pal_venv" ]; then
        source .pal_venv/bin/activate
    elif [ -d "venv" ]; then
        source venv/bin/activate
    fi
    
    if [ -f ".env" ]; then
        # Load env vars safely
        set -a
        [ -f .env ] && . .env
        set +a
    fi
    
    MONITOR_TRANSPORT=${MONITOR_TRANSPORT:-unix}
    MONITOR_SOCKET_PATH=${MONITOR_SOCKET_PATH:-/tmp/pal-monitor.sock}
    MONITOR_WS_PORT=${MONITOR_WS_PORT:-9876}
    
    COORD_ARGS=""
    if [[ "$MONITOR_TRANSPORT" == "unix" || "$MONITOR_TRANSPORT" == "dual" ]]; then
        COORD_ARGS="--dual --socket $MONITOR_SOCKET_PATH"
    elif [[ "$MONITOR_TRANSPORT" == "http" ]]; then
        COORD_ARGS="--transport http --port $MONITOR_WS_PORT"
    fi
    
    nohup python monitor/run_coordinator.py $COORD_ARGS > "$LOG_FILE" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    
    echo "Monitor started with PID: $PID"
    echo "Logs: $LOG_FILE"
}

function stop() {
    local pid=$(get_pid)
    
    # If no PID file, try to find process by port
    if [ -z "$pid" ]; then
        pid=$(ss -tulnp | grep ":9876" | grep -oP "pid=\K[0-9]+")
    fi

    if [ -z "$pid" ] || ! ps -p "$pid" > /dev/null 2>&1; then
        echo "Monitor is not running"
        [ -f "$PID_FILE" ] && rm "$PID_FILE"
        [ -S "$SOCKET_FILE" ] && rm "$SOCKET_FILE"
        return 0
    fi

    echo "Stopping monitor (PID: $pid)..."
    kill "$pid"
    
    # Wait for it to stop
    local timeout=10
    while ps -p "$pid" > /dev/null 2>&1 && [ $timeout -gt 0 ]; do
        sleep 1
        timeout=$((timeout - 1))
    done
    
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Monitor did not stop gracefully, killing forcefully..."
        kill -9 "$pid"
        # Also kill any other processes on the same port just in case
        fuser -k 9876/tcp > /dev/null 2>&1 || true
    fi
    
    rm -f "$PID_FILE"
    [ -S "$SOCKET_FILE" ] && rm "$SOCKET_FILE"
    
    echo "Monitor stopped."
}

function status() {
    if is_running; then
        echo "Monitor is RUNNING (PID: $(get_pid))"
        echo "Log tail:"
        tail -n 5 "$LOG_FILE"
    else
        echo "Monitor is STOPPED"
        if [ -f "$PID_FILE" ]; then
            echo "Warning: PID file exists but process is dead"
        fi
    fi
}

# No arguments provided
if [ -z "$1" ]; then
    usage
fi

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 1
        start
        ;;
    status)
        status
        ;;
    *)
        usage
        ;;
esac