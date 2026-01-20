"""
Data models for PAL MCP Server monitoring interface.

This module defines the Pydantic schemas used for:
- Instance status reporting
- Tool activity events
- Aggregated state for dashboard consumption
- WebSocket message formats

All models support JSON serialization for WebSocket transport.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


def utc_now():
    """Return current UTC time with timezone info."""
    return datetime.now(timezone.utc)


class ToolEventType(str, Enum):
    """Types of tool execution events."""

    TOOL_START = "tool_start"
    TOOL_END = "tool_end"
    TOOL_ERROR = "tool_error"
    TOOL_LOG = "tool_log"
    HEARTBEAT = "heartbeat"
    REGISTER = "register"
    UNREGISTER = "unregister"


class ToolCall(BaseModel):
    """Record of a completed tool call."""

    tool: str = Field(..., description="Name of the tool that was called")
    tool_input: Optional[str] = Field(None, description="Input arguments (JSON string)")
    tool_output: Optional[str] = Field(None, description="Output result (JSON string)")
    duration_ms: int = Field(..., description="Execution duration in milliseconds")
    status: str = Field(..., description="Execution status: 'success' or 'error'")
    model_name: Optional[str] = Field(default=None, description="AI model used for this call")
    timestamp: datetime = Field(default_factory=utc_now, description="When the call completed")


class InstanceStatus(BaseModel):
    """Status report from a single MCP server instance."""

    instance_id: str = Field(..., description="Unique identifier: PID@hostname")
    uptime_seconds: float = Field(..., description="Seconds since server startup")
    state: str = Field(
        default="idle",
        description="Current state: 'idle', 'busy', or 'offline'",
    )
    last_heartbeat: datetime = Field(
        default_factory=utc_now,
        description="Timestamp of last communication",
    )
    active_tool: Optional[str] = Field(default=None, description="Currently executing tool name, null if idle")
    session_id: Optional[str] = Field(default=None, description="Current session/conversation ID")
    model_name: Optional[str] = Field(default=None, description="AI model name currently in use")
    active_role: Optional[str] = Field(default=None, description="Active role name (e.g. for clink tool)")
    tool_start_time: Optional[datetime] = Field(default=None, description="When the active tool started execution")
    recent_calls: list[ToolCall] = Field(
        default_factory=list,
        description="Recent tool call history (newest first)",
    )
    error_rate_1m: float = Field(
        default=0.0,
        description="Error rate in the last window (0.0 to 1.0)",
    )
    avg_execution_time_1m: float = Field(
        default=0.0,
        description="Average tool execution time in ms over last window",
    )
    last_status: Optional[str] = Field(
        default=None,
        description="Most recent status message from logs or notifications",
    )
    total_calls: int = Field(
        default=0,
        description="Total number of tool calls since startup",
    )
    total_errors: int = Field(
        default=0,
        description="Total number of errors since startup",
    )

    def _format_dt(self, dt: datetime) -> str:
        """Format datetime to ISO string with Z suffix."""
        if not dt:
            return None
        # Convert to UTC if it has tzinfo, or just append Z if it's naive (we assume it's UTC)
        if dt.tzinfo:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def to_dict(self) -> dict:
        """Convert to dictionary with ISO format timestamps."""
        data = self.model_dump()
        data["last_heartbeat"] = self._format_dt(self.last_heartbeat)
        if self.tool_start_time:
            data["tool_start_time"] = self._format_dt(self.tool_start_time)
        
        data["recent_calls"] = [
            {
                **call,
                "timestamp": self._format_dt(call["timestamp"]) if isinstance(call["timestamp"], datetime) else call["timestamp"]
            }
            for call in data["recent_calls"]
        ]
        return data


class ToolEvent(BaseModel):
    """Event published by MCP server to coordinator."""

    event_type: ToolEventType = Field(..., description="Type of event")
    instance_id: str = Field(..., description="Source instance identifier")
    timestamp: datetime = Field(default_factory=utc_now, description="Event timestamp")
    tool_name: Optional[str] = Field(default=None, description="Tool name for tool events")
    session_id: Optional[str] = Field(default=None, description="Session ID (continuation_id) for log grouping")
    tool_input: Optional[str] = Field(default=None, description="Input arguments for TOOL_START")
    tool_output: Optional[str] = Field(default=None, description="Output result for TOOL_END")
    duration_ms: Optional[int] = Field(default=None, description="Duration for TOOL_END events")
    error_message: Optional[str] = Field(default=None, description="Error message for TOOL_ERROR events")
    log_data: Optional[str] = Field(default=None, description="Log content for TOOL_LOG events")
    model_name: Optional[str] = Field(default=None, description="Model name for TOOL_END events")
    uptime_seconds: Optional[float] = Field(default=None, description="Uptime for HEARTBEAT/REGISTER events")

    def to_json(self) -> str:
        """Serialize to JSON string."""
        # Use Pydantic's built-in serialization which handles datetime well, 
        # but we want to ensure Z suffix if possible.
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "ToolEvent":
        """Deserialize from JSON string."""
        return cls.model_validate_json(json_str)


class AggregatedState(BaseModel):
    """Complete aggregated state for dashboard consumption."""

    type: str = Field(default="state_update", description="Message type identifier")
    timestamp: datetime = Field(default_factory=utc_now, description="State snapshot timestamp")
    instances: list[InstanceStatus] = Field(default_factory=list, description="Status of all known instances")

    def _format_dt(self, dt: datetime) -> str:
        """Format datetime to ISO string with Z suffix."""
        if dt.tzinfo:
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    def to_json(self) -> str:
        """Serialize to JSON for WebSocket transmission."""
        data = {
            "type": self.type,
            "timestamp": self._format_dt(self.timestamp),
            "instances": [inst.to_dict() for inst in self.instances],
        }
        import json

        return json.dumps(data)

    @classmethod
    def from_instances(cls, instances: list[InstanceStatus]) -> "AggregatedState":
        """Create aggregated state from list of instance statuses."""
        return cls(instances=instances)


class WebSocketMessage(BaseModel):
    """Generic WebSocket message wrapper."""

    type: str = Field(..., description="Message type")
    payload: dict = Field(default_factory=dict, description="Message payload")
    timestamp: datetime = Field(default_factory=utc_now, description="Message timestamp")

    def to_json(self) -> str:
        """Serialize to JSON string."""
        data = self.model_dump()
        # Ensure UTC format
        if isinstance(data["timestamp"], datetime):
            if data["timestamp"].tzinfo:
                data["timestamp"] = data["timestamp"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                data["timestamp"] = data["timestamp"].strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        
        import json

        return json.dumps(data)

