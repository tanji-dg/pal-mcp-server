#!/usr/bin/env python3
"""
Run the PAL MCP Monitor Coordinator.

This script starts the monitoring coordinator server that:
- Receives status events from MCP server instances (via HTTP or Unix socket)
- Maintains aggregated state in memory
- Serves WebSocket connections and dashboard for browsers (via HTTP)

Architecture:
    MCP Servers → Unix Socket (internal) → Coordinator
                                               ↓
    Browser → HTTP (external) ← Dashboard + WebSocket

Usage:
    # HTTP only (simple mode)
    python monitor/run_coordinator.py

    # Dual mode: HTTP for dashboard + Unix socket for MCP events
    python monitor/run_coordinator.py --dual --socket /tmp/pal-monitor.sock

Environment Variables:
    MONITOR_TRANSPORT: "http", "unix", or "dual" (default: http)
    MONITOR_WS_HOST: HTTP host (default: 0.0.0.0)
    MONITOR_WS_PORT: HTTP port (default: 9876)
    MONITOR_SOCKET_PATH: Unix socket path (default: /tmp/pal-monitor.sock)
    LOG_LEVEL: Logging level (default: INFO)
"""

import argparse
import asyncio
import logging
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_http_only(host: str, port: int, log_level: str, reload: bool = False):
    """Run coordinator with HTTP only."""
    import uvicorn

    uvicorn.run(
        "monitor.coordinator:app",
        host=host,
        port=port,
        reload=reload,
        log_level=log_level,
    )


def run_unix_only(socket_path: str, log_level: str, reload: bool = False):
    """Run coordinator with Unix socket only."""
    import uvicorn

    if os.path.exists(socket_path):
        os.unlink(socket_path)

    uvicorn.run(
        "monitor.coordinator:app",
        uds=socket_path,
        reload=reload,
        log_level=log_level,
    )


async def run_dual_mode(host: str, port: int, socket_path: str, log_level: str):
    """Run coordinator with both HTTP and Unix socket simultaneously."""
    import uvicorn

    logger = logging.getLogger(__name__)

    # Remove existing socket
    if os.path.exists(socket_path):
        os.unlink(socket_path)

    # Create configs for both servers
    http_config = uvicorn.Config(
        "monitor.coordinator:app",
        host=host,
        port=port,
        log_level=log_level,
    )
    unix_config = uvicorn.Config(
        "monitor.coordinator:app",
        uds=socket_path,
        log_level=log_level,
    )

    # Create server instances
    http_server = uvicorn.Server(http_config)
    unix_server = uvicorn.Server(unix_config)

    logger.info(f"Starting dual-mode coordinator:")
    logger.info(f"  HTTP: http://{host}:{port} (dashboard + WebSocket)")
    logger.info(f"  Unix: {socket_path} (MCP events)")

    # Run both servers concurrently
    # Note: Both share the same 'app' and thus the same global coordinator instance
    await asyncio.gather(
        http_server.serve(),
        unix_server.serve(),
    )


def main():
    """Run the coordinator server."""
    parser = argparse.ArgumentParser(
        description="PAL MCP Monitor Coordinator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--transport",
        choices=["http", "unix", "dual"],
        default=os.environ.get("MONITOR_TRANSPORT", "http"),
        help="Transport: http, unix, or dual (default: http)",
    )
    parser.add_argument(
        "--dual",
        action="store_true",
        help="Enable dual mode (HTTP + Unix socket)",
    )
    parser.add_argument(
        "--host",
        default=os.environ.get("MONITOR_WS_HOST", "0.0.0.0"),
        help="HTTP host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MONITOR_WS_PORT", "9876")),
        help="HTTP port (default: 9876)",
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

    # Determine transport mode
    transport = "dual" if args.dual else args.transport

    try:
        import uvicorn
    except ImportError:
        logger.error(
            "uvicorn is required. Install with: pip install uvicorn"
        )
        sys.exit(1)

    if transport == "dual":
        logger.info("Starting in dual mode (HTTP + Unix socket)")
        asyncio.run(run_dual_mode(
            host=args.host,
            port=args.port,
            socket_path=args.socket,
            log_level=args.log_level.lower(),
        ))
    elif transport == "unix":
        logger.info(f"Starting with Unix socket: {args.socket}")
        run_unix_only(
            socket_path=args.socket,
            log_level=args.log_level.lower(),
            reload=args.reload,
        )
    else:
        logger.info(f"Starting with HTTP: http://{args.host}:{args.port}")
        run_http_only(
            host=args.host,
            port=args.port,
            log_level=args.log_level.lower(),
            reload=args.reload,
        )


if __name__ == "__main__":
    main()
