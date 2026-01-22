"""
Event Publisher for PAL MCP Server monitoring.

This module provides a non-blocking event publisher that MCP server instances
use to report their status to the monitoring coordinator.

Key Features:
- Non-blocking: Events are queued and sent asynchronously
- Graceful degradation: If coordinator is unavailable, events are logged only
- Auto-reconnection: Attempts to reconnect on connection failure
- Queue-based: Buffers events during brief network interruptions
- Dual transport: Supports both HTTP and Unix socket communication

Usage:
    from monitor.publisher import get_publisher

    publisher = get_publisher()
    await publisher.start()

    # Publish events
    await publisher.tool_start("chat")
    await publisher.tool_end("chat", duration_ms=1234)

    await publisher.stop()
"""

import asyncio
import atexit
import logging
import os
import socket
import time
from typing import Any, Optional

import httpx

from monitor.models import ToolEvent, ToolEventType

logger = logging.getLogger(__name__)

# Configuration defaults
DEFAULT_COORDINATOR_URL = "http://localhost:9876"
DEFAULT_SOCKET_PATH = "/tmp/pal-monitor.sock"
EVENT_QUEUE_SIZE = 1000
RECONNECT_INTERVAL = 5.0  # seconds
HEARTBEAT_INTERVAL = 5.0  # seconds
SEND_TIMEOUT = 5.0  # seconds


def get_instance_id() -> str:
    """Generate a unique instance identifier from PID and hostname."""
    pid = os.getpid()
    try:
        hostname = socket.gethostname()
    except Exception:
        hostname = "unknown"
    return f"{pid}@{hostname}"


class MonitorPublisher:
    """
    Non-blocking event publisher for MCP server monitoring.

    This class manages the connection to the monitoring coordinator and
    provides methods to publish tool execution events. All operations are
    non-blocking and fail gracefully if the coordinator is unavailable.

    Supports both HTTP and Unix socket transports.
    """

    def __init__(
        self,
        transport: str = "http",
        coordinator_url: Optional[str] = None,
        socket_path: Optional[str] = None,
        enabled: bool = True,
    ):
        """
        Initialize the publisher.

        Args:
            transport: "http" or "unix"
            coordinator_url: URL of the monitoring coordinator (for HTTP transport)
            socket_path: Path to Unix socket (for Unix transport)
            enabled: Whether monitoring is enabled
        """
        self.transport = transport.lower()
        self.coordinator_url = coordinator_url or DEFAULT_COORDINATOR_URL
        self.socket_path = socket_path or DEFAULT_SOCKET_PATH
        self.enabled = enabled
        self.instance_id = get_instance_id()
        self.start_time = time.time()

        self._client: Optional[httpx.AsyncClient] = None
        self._queue: asyncio.Queue[ToolEvent] = asyncio.Queue(maxsize=EVENT_QUEUE_SIZE)
        self._sender_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self._connected = False
        self._connection_error_logged = False
        self._last_error = None
        self._interrupted = False
        self._last_contact_time = 0.0

    @property
    def uptime_seconds(self) -> float:
        """Get current uptime in seconds."""
        return time.time() - self.start_time

    def is_interrupted(self) -> bool:
        """Check if an interruption has been requested by the coordinator."""
        return self._interrupted

    async def check_interruption(self) -> bool:
        """Force a status check with the coordinator if we've been silent."""
        if not self.enabled:
            return False
            
        now = time.time()
        # If we haven't talked to the coordinator in the last 2 seconds, send a heartbeat to poll status
        if now - self._last_contact_time > 2.0:
            logger.debug("Polling coordinator for interruption status...")
            try:
                await self._send_heartbeat()
            except Exception:
                pass # Heartbeat loop will handle reconnection
                
        return self._interrupted

    def reset_interruption(self):
        """Reset the interruption flag (e.g. before starting a new tool)."""
        if self._interrupted:
            logger.info(f"Resetting interruption state for {self.instance_id}")
            self._interrupted = False

    def _get_base_url(self) -> str:
        """Get the base URL for HTTP requests."""
        if self.transport == "unix":
            # For Unix socket, use a dummy URL - the actual socket is in transport
            return "http://localhost"
        return self.coordinator_url

    def _create_client(self) -> httpx.AsyncClient:
        """Create an HTTP client with appropriate transport."""
        if self.transport == "unix":
            # Use Unix socket transport
            transport = httpx.AsyncHTTPTransport(uds=self.socket_path)
            return httpx.AsyncClient(
                transport=transport,
                timeout=SEND_TIMEOUT,
                base_url="http://localhost",  # Dummy URL for UDS
            )
        else:
            # Use regular HTTP transport
            return httpx.AsyncClient(
                timeout=SEND_TIMEOUT,
                base_url=self.coordinator_url,
            )

    async def start(self):
        """Start the publisher and background tasks."""
        if not self.enabled:
            logger.debug("Monitor publisher disabled, not starting")
            return

        if self._running:
            return

        self._running = True
        self._client = self._create_client()

        # Start background tasks
        self._sender_task = asyncio.create_task(self._sender_loop())
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Register with coordinator
        await self._publish_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id=self.instance_id,
                uptime_seconds=self.uptime_seconds,
            )
        )

        transport_info = f"unix:{self.socket_path}" if self.transport == "unix" else self.coordinator_url
        logger.info(f"Monitor publisher started: {self.instance_id} via {transport_info}")

    async def stop(self):
        """Stop the publisher and cleanup."""
        if not self._running:
            return

        # Give a small window for queued events to be sent
        if not self._queue.empty():
            logger.debug("Monitor publisher: Waiting for queue to clear...")
            for _ in range(50):  # Max 5 seconds
                if self._queue.empty():
                    break
                await asyncio.sleep(0.1)

        self._running = False

        # Unregister from coordinator
        try:
            await self._send_event_directly(
                ToolEvent(
                    event_type=ToolEventType.UNREGISTER,
                    instance_id=self.instance_id,
                )
            )
        except Exception:
            pass  # Best effort unregistration

        # Cancel background tasks
        for task in [self._sender_task, self._heartbeat_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Close HTTP client
        if self._client:
            await self._client.aclose()
            self._client = None

        logger.info("Monitor publisher stopped")

    async def tool_start(self, tool_name: str, arguments: Optional[Any] = None, model_name: Optional[str] = None, session_id: Optional[str] = None):
        """Record that a tool has started execution."""
        if not self.enabled:
            return

        # Serialize arguments to string if present, stripping internal keys
        tool_input = None
        if arguments:
            import json

            # If already a string (e.g. pre-serialized JSON), use as is or attempt to load/strip
            if isinstance(arguments, str):
                tool_input = arguments
            elif isinstance(arguments, dict):
                # Create a clean version of arguments for monitoring (remove internal/non-serializable)
                clean_args = {
                    k: v for k, v in arguments.items()
                    if not k.startswith("_") and isinstance(v, (str, int, float, bool, list, dict, type(None)))
                }

                try:
                    tool_input = json.dumps(clean_args)
                except Exception:
                    # If even clean_args fails, try to stringify individual values
                    try:
                        tool_input = json.dumps({k: str(v) for k, v in clean_args.items()})
                    except Exception:
                        tool_input = str(clean_args)
            else:
                tool_input = str(arguments)

        event = ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id=self.instance_id,
            tool_name=tool_name,
            tool_input=tool_input,
            model_name=model_name,
            session_id=session_id,
            uptime_seconds=self.uptime_seconds,
        )
        await self._publish_event(event)

    async def tool_end(self, tool_name: str, duration_ms: int, tool_output: Optional[Any] = None, model_name: Optional[str] = None, session_id: Optional[str] = None):
        """Record that a tool has completed successfully."""
        if not self.enabled:
            return

        logger.debug(f"Publisher: tool_end called for {tool_name}")

        # Serialize result to string if present
        serialized_output = None
        if tool_output:
            import json

            try:
                # Handle Pydantic models or dicts
                if hasattr(tool_output, "model_dump"):
                    serialized_output = tool_output.model_dump_json()
                elif hasattr(tool_output, "to_dict"):
                    serialized_output = json.dumps(tool_output.to_dict())
                elif isinstance(tool_output, list):
                    # Handle list of objects (like TextContent)
                    try:
                        serialized_output = json.dumps([
                            item.model_dump() if hasattr(item, "model_dump") else item 
                            for item in tool_output
                        ])
                    except Exception:
                        serialized_output = json.dumps(tool_output) # Try default
                else:
                    serialized_output = json.dumps(tool_output)
            except Exception as e:
                logger.debug(f"Publisher: Serialization failed: {e}")
                serialized_output = str(tool_output)

        event = ToolEvent(
            event_type=ToolEventType.TOOL_END,
            instance_id=self.instance_id,
            tool_name=tool_name,
            tool_output=serialized_output,
            duration_ms=duration_ms,
            model_name=model_name,
            session_id=session_id,
            uptime_seconds=self.uptime_seconds,
        )
        await self._publish_event(event)

    async def tool_error(self, tool_name: str, duration_ms: int, error_message: str, model_name: Optional[str] = None, session_id: Optional[str] = None):
        """Record that a tool execution resulted in an error."""
        if not self.enabled:
            return

        event = ToolEvent(
            event_type=ToolEventType.TOOL_ERROR,
            instance_id=self.instance_id,
            tool_name=tool_name,
            duration_ms=duration_ms,
            model_name=model_name,
            session_id=session_id,
            error_message=error_message[:500],  # Truncate long error messages
            uptime_seconds=self.uptime_seconds,
        )
        await self._publish_event(event)

    async def tool_log(self, tool_name: str, log_message: str, session_id: Optional[str] = None):
        """Record a log message from a tool execution."""
        if not self.enabled:
            return

        event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=self.instance_id,
            tool_name=tool_name,
            log_data=log_message,
            session_id=session_id,
            uptime_seconds=self.uptime_seconds,
        )
        logger.debug(f"Publisher: Sent TOOL_LOG event: {event.model_dump_json()}")
        await self._publish_event(event)

    async def _publish_event(self, event: ToolEvent):
        """Add event to the send queue (non-blocking)."""
        if not self._running:
            logger.debug(f"Monitor publisher NOT RUNNING, dropping event: {event.event_type}")
            return

        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Monitor event queue full, dropping event")

    async def _send_event_directly(self, event: ToolEvent):
        """Send event directly without queuing (for critical events)."""
        if not self._client:
            return

        try:
            response = await self._client.post(
                "/event",
                json=event.model_dump(mode="json"),
            )
            response.raise_for_status()
            
            self._last_contact_time = time.time()
            
            # Check for interruption signal in response
            try:
                data = response.json()
                if isinstance(data, dict) and data.get("interrupted"):
                    if not self._interrupted:
                        logger.warning(f"Interruption signal detected for {self.instance_id}")
                    self._interrupted = True
            except Exception:
                pass

            if not self._connected and self._connection_error_logged:
                logger.info("Monitor coordinator connection restored")

            self._connected = True
            self._connection_error_logged = False
            self._last_error = None
        except Exception as e:
            self._connected = False
            self._last_error = str(e)
            raise

    async def _sender_loop(self):
        """Background task to send queued events."""
        batch: list[ToolEvent] = []
        batch_timeout = 0.5  # seconds

        # Continue as long as we are running OR there are events left in the queue
        while self._running or not self._queue.empty():
            try:
                # Collect events from queue
                try:
                    # Use a shorter timeout when stopping to drain quickly
                    wait_time = batch_timeout if self._running else 0.1
                    event = await asyncio.wait_for(self._queue.get(), timeout=wait_time)
                    batch.append(event)

                    # Drain queue for batching
                    while len(batch) < 10:
                        try:
                            event = self._queue.get_nowait()
                            batch.append(event)
                        except asyncio.QueueEmpty:
                            break

                except asyncio.TimeoutError:
                    pass

                # Send batch if we have events
                if batch and self._client:
                    try:
                        if len(batch) == 1:
                            await self._send_event_directly(batch[0])
                        else:
                            response = await self._client.post(
                                "/events",
                                json=[e.model_dump(mode="json") for e in batch],
                            )
                            response.raise_for_status()
                            
                            # Check for interruption signal in batch response
                            try:
                                data = response.json()
                                if isinstance(data, dict) and data.get("interrupted"):
                                    if not self._interrupted:
                                        logger.warning("Interruption signal received from coordinator (batch)")
                                    self._interrupted = True
                            except Exception:
                                pass

                        if not self._connected and self._connection_error_logged:
                            logger.info("Monitor coordinator connection restored")

                        self._connected = True
                        self._connection_error_logged = False
                        self._last_error = None
                        batch.clear()
                    except Exception as e:
                        if not self._connection_error_logged:
                            logger.warning(
                                f"Failed to connect to monitor coordinator: {e}. "
                                "Monitoring events will be queued and retried silently. "
                                "Check if monitor service is running (./scripts/start_monitor.sh)."
                            )
                            self._connection_error_logged = True

                        self._connected = False
                        self._last_error = str(e)
                        # Keep events in batch for retry
                        await asyncio.sleep(RECONNECT_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitor sender loop: {e}")
                await asyncio.sleep(1)

    async def _heartbeat_loop(self):
        """Background task to send periodic heartbeats."""
        while self._running:
            try:
                await asyncio.sleep(HEARTBEAT_INTERVAL)

                event = ToolEvent(
                    event_type=ToolEventType.HEARTBEAT,
                    instance_id=self.instance_id,
                    uptime_seconds=self.uptime_seconds,
                )
                await self._publish_event(event)

            except asyncio.CancelledError:
                break
            except Exception:
                # Silently fail, sender loop handles connection error logging
                pass


# Global publisher instance
_publisher: Optional[MonitorPublisher] = None


def get_publisher() -> MonitorPublisher:
    """Get or create the global publisher instance."""
    global _publisher
    if _publisher is None:
        # Import config here to avoid circular imports
        from utils.env import get_env

        # Use HTTP as default for stability, and enable by default
        transport = (get_env("MONITOR_TRANSPORT", "http") or "http").lower()
        coordinator_url = get_env("MONITOR_COORDINATOR_URL", DEFAULT_COORDINATOR_URL)
        socket_path = get_env("MONITOR_SOCKET_PATH", DEFAULT_SOCKET_PATH)
        enabled = (get_env("MONITOR_ENABLED", "true") or "true").lower() in (
            "true",
            "1",
            "yes",
        )

        _publisher = MonitorPublisher(
            transport=transport,
            coordinator_url=coordinator_url,
            socket_path=socket_path,
            enabled=enabled,
        )
    return _publisher


async def initialize_publisher():
    """Initialize and start the global publisher."""
    publisher = get_publisher()
    await publisher.start()
    return publisher


async def shutdown_publisher():
    """Stop and cleanup the global publisher."""
    global _publisher
    if _publisher:
        await _publisher.stop()
        _publisher = None


# Synchronous cleanup for atexit
def _sync_cleanup():
    """Synchronous cleanup wrapper for atexit."""
    global _publisher
    if _publisher and _publisher._running:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule cleanup in the running loop
                asyncio.create_task(shutdown_publisher())
            else:
                loop.run_until_complete(shutdown_publisher())
        except Exception:
            pass  # Best effort cleanup


atexit.register(_sync_cleanup)