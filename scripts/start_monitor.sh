#!/bin/bash
# Wrapper for monitor_ctrl.sh to start the monitor
# Usage: ./scripts/start_monitor.sh [background]

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CTRL_SCRIPT="$PROJECT_ROOT/scripts/monitor_ctrl.sh"

if [[ "$1" == "bg" || "$1" == "background" ]]; then
    "$CTRL_SCRIPT" start
else
    "$CTRL_SCRIPT" fg
fi