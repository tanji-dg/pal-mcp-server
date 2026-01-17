"""
Logging handler for PAL MCP Server monitoring.

This module provides a logging handler that forwards log records to the
monitoring coordinator via the MonitorPublisher.
"""

import logging
import asyncio
from typing import Optional

from monitor.models import ToolEventType
from monitor.publisher import get_publisher, MonitorPublisher


class MonitorLogHandler(logging.Handler):
    """
    Logging handler that forwards logs to the monitoring coordinator.

    This handler is non-blocking and uses the MonitorPublisher to send
    log events asynchronously.
    """

    def __init__(self, publisher: Optional[MonitorPublisher] = None):
        super().__init__()
        self.publisher = publisher

    def emit(self, record: logging.LogRecord):
        """Emit a log record."""
        try:
            # Get publisher if not provided or initialized
            publisher = self.publisher or get_publisher()

            if not publisher or not publisher._running:
                return

            # Format the log message
            msg = self.format(record)

            # Send to publisher asynchronously
            # We use a helper method on the publisher to avoid import cycles
            # or complex async logic here
            try:
                loop = asyncio.get_running_loop()
                if loop.is_running():
                    loop.call_soon_threadsafe(
                        lambda: asyncio.create_task(
                            self._send_log(publisher, record.levelname, msg)
                        )
                    )
            except RuntimeError:
                # No running loop, skip log
                pass

        except Exception:
            self.handleError(record)

    async def _send_log(self, publisher: MonitorPublisher, level: str, message: str):
        """Send log event via publisher."""
        await publisher.log_event(level, message)
