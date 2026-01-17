#!/usr/bin/env python3
"""
Run the PAL MCP Monitor Coordinator.

This script starts the monitoring coordinator server that:
- Receives status events from MCP server instances
- Maintains aggregated state in memory
- Serves WebSocket connections for dashboards

Supports both HTTP and Unix socket transports.

Usage:
    # HTTP mode (default)
    python monitor/run_coordinator.py
    python monitor/run_coordinator.py --transport http --host 0.0.0.0 --port 9876

    # Unix socket mode
    python monitor/run_coordinator.py --transport unix --socket /tmp/pal-monitor.sock

Environment Variables:
    MONITOR_TRANSPORT: Transport type ("http" or "unix", default: http)
    MONITOR_WS_HOST: WebSocket host (default: 0.0.0.0)
    MONITOR_WS_PORT: WebSocket port (default: 9876)
    MONITOR_SOCKET_PATH: Unix socket path (default: /tmp/pal-monitor.sock)
    LOG_LEVEL: Logging level (default: INFO)
"""

import argparse
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def main():
    """Run the coordinator server."""
    parser = argparse.ArgumentParser(
        description="PAL MCP Monitor Coordinator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--transport",
        choices=["http", "unix"],
        default=os.environ.get("MONITOR_TRANSPORT", "http"),
        help="Transport type: http or unix (default: http)",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MONITOR_WS_HOST", "0.0.0.0"),
        help="Host to bind to (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MONITOR_WS_PORT", "9876")),
        help="Port to listen on (default: 9876)",
    )
    parser.add_argument(
        "--socket",
        default=os.environ.get("MONITOR_SOCKET_PATH", "/tmp/pal-monitor.sock"),
        help="Unix socket path (default: /tmp/pal-monitor.sock)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    logger = logging.getLogger(__name__)

    # Import and run with appropriate transport
    try:
        import uvicorn
    except ImportError:
        logger.error(
            "uvicorn is required to run the coordinator. "
            "Install it with: pip install uvicorn"
        )
        sys.exit(1)

    if args.transport == "unix":
        # Unix socket mode
        logger.info(f"Starting coordinator with Unix socket: {args.socket}")
        logger.info(f"WebSocket for dashboards: ws://{args.host}:{args.port}/ws")

        # Remove existing socket file if present
        if os.path.exists(args.socket):
            os.unlink(args.socket)

        uvicorn.run(
            "monitor.coordinator:app",
            uds=args.socket,
            reload=args.reload,
            log_level=args.log_level.lower(),
        )
    else:
        # HTTP mode
        logger.info(f"Starting coordinator on http://{args.host}:{args.port}")
        logger.info(f"WebSocket endpoint: ws://{args.host}:{args.port}/ws")

        uvicorn.run(
            "monitor.coordinator:app",
            host=args.host,
            port=args.port,
            reload=args.reload,
            log_level=args.log_level.lower(),
        )


if __name__ == "__main__":
    main()
