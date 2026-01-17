"""
PAL MCP Server Monitoring Interface

This module provides real-time monitoring capabilities for multiple MCP server instances.
It implements a WebSocket-based pub/sub architecture where:
- MCP server instances publish status events to a coordinator
- Remote dashboards subscribe to aggregated status via WebSocket
- The coordinator maintains in-memory state and broadcasts updates

Architecture:
    MCP Servers → Publisher → Coordinator → WebSocket → Dashboards

Components:
    - models: Pydantic data models for status events and aggregated state
    - publisher: Non-blocking event publisher for MCP servers
    - coordinator: WebSocket server with instance management
"""

from monitor.models import (
    AggregatedState,
    InstanceStatus,
    ToolCall,
    ToolEvent,
    ToolEventType,
)
from monitor.publisher import MonitorPublisher, get_publisher

__all__ = [
    "AggregatedState",
    "InstanceStatus",
    "ToolCall",
    "ToolEvent",
    "ToolEventType",
    "MonitorPublisher",
    "get_publisher",
]
