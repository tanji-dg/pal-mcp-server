#!/bin/bash
# PAL MCP Monitor Control Script
#
# Usage: ./scripts/monitor_ctrl.sh {start|stop|restart|status|logs|fg}

set -e

# Get project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

PID_FILE="/tmp/pal-monitor.pid"
LOG_FILE="logs/monitor.log" # Use logs directory in project root
SOCKET_FILE="/tmp/pal-monitor.sock"

# Ensure logs directory exists
mkdir -p logs

function usage() {
    echo "Usage: $0 {start|stop|restart|status|logs|fg}"
    echo "  start   : Start monitor in background"
    echo "  stop    : Stop monitor"
    echo "  restart : Restart monitor"
    echo "  status  : Check monitor status"
    echo "  logs    : Tail monitor logs"
    echo "  fg      : Start monitor in foreground"
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

function setup_env() {
    # Check for virtual environment
    if [ -d ".pal_venv" ]; then
        source .pal_venv/bin/activate
    elif [ -d "venv" ]; then
        source venv/bin/activate
    fi
    
    # Load environment variables if .env exists
    if [ -f ".env" ]; then
        set -a
        source .env
        set +a
    fi
    
    # Set PYTHONPATH to project root
    export PYTHONPATH=$PYTHONPATH:$PROJECT_ROOT
}

function get_coord_args() {
    local transport=${MONITOR_TRANSPORT:-unix}
    local socket_path=${MONITOR_SOCKET_PATH:-/tmp/pal-monitor.sock}
    local ws_port=${MONITOR_WS_PORT:-9876}
    
    if [[ "$transport" == "unix" || "$transport" == "dual" ]]; then
        echo "--dual --socket $socket_path"
    elif [[ "$transport" == "http" ]]; then
        echo "--transport http --port $ws_port"
    fi
}

function start() {
    if is_running; then
        echo "Monitor is already running (PID: $(get_pid))"
        return 0
    fi

    setup_env
    local args=$(get_coord_args)
    
    echo "Starting PAL MCP Monitor in background..."
    nohup python monitor/run_coordinator.py $args > "$LOG_FILE" 2>&1 &
    PID=$!
    echo $PID > "$PID_FILE"
    
    echo "Monitor started with PID: $PID"
    echo "Logs: $LOG_FILE"
}

function start_fg() {
    if is_running; then
        echo "Monitor is already running in background (PID: $(get_pid))"
        exit 1
    fi

    setup_env
    local args=$(get_coord_args)
    
    echo "Starting PAL MCP Monitor in foreground..."
    python monitor/run_coordinator.py $args
}

function stop() {
    local pid=$(get_pid)
    
    if [ -z "$pid" ]; then
        # Try to find by port 9876 if PID file is missing
        pid=$(lsof -t -i:9876 2>/dev/null || true)
    fi

    if [ -z "$pid" ] || ! ps -p "$pid" > /dev/null 2>&1; then
        echo "Monitor is not running"
        [ -f "$PID_FILE" ] && rm "$PID_FILE"
        return 0
    fi

    echo "Stopping monitor (PID: $pid)..."
    kill "$pid" 2>/dev/null || true
    
    # Wait for it to stop
    local timeout=5
    while ps -p "$pid" > /dev/null 2>&1 && [ $timeout -gt 0 ]; do
        sleep 1
        timeout=$((timeout - 1))
    done
    
    if ps -p "$pid" > /dev/null 2>&1; then
        echo "Forcing stop..."
        kill -9 "$pid" 2>/dev/null || true
    fi
    
    # Final cleanup of any remaining processes on the port
    fuser -k 9876/tcp 2>/dev/null || true
    
    # Cleanup
    rm -f "$PID_FILE"
    [ -S "$SOCKET_FILE" ] && rm "$SOCKET_FILE"
    
    echo "Monitor stopped."
}

function status() {
    if is_running; then
        local pid=$(get_pid)
        echo "Monitor is RUNNING (PID: $pid)"
        local port=${MONITOR_WS_PORT:-9876}
        echo "Dashboard: http://localhost:$port"
    else
        echo "Monitor is STOPPED"
    fi
}

function logs() {
    if [ -f "$LOG_FILE" ]; then
        tail -f "$LOG_FILE"
    else
        echo "Log file not found: $LOG_FILE"
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
    fg)
        start_fg
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
    logs)
        logs
        ;;
    *)
        usage
        ;;
esac
