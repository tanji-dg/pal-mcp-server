"""
Data models for PAL MCP Server monitoring interface.

This module defines the Pydantic schemas used for:
- Instance status reporting
- Tool activity events
- Aggregated state for dashboard consumption
- WebSocket message formats

All models support JSON serialization for WebSocket transport.
"""

from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


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
    timestamp: datetime = Field(default_factory=datetime.now, description="When the call completed")


class InstanceStatus(BaseModel):
    """Status report from a single MCP server instance."""

    instance_id: str = Field(..., description="Unique identifier: PID@hostname")
    uptime_seconds: float = Field(..., description="Seconds since server startup")
    state: str = Field(
        default="idle",
        description="Current state: 'idle', 'busy', or 'offline'",
    )
    last_heartbeat: datetime = Field(
        default_factory=datetime.now,
        description="Timestamp of last communication",
    )
    active_tool: Optional[str] = Field(default=None, description="Currently executing tool name, null if idle")
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
    total_calls: int = Field(
        default=0,
        description="Total number of tool calls since startup",
    )
    total_errors: int = Field(
        default=0,
        description="Total number of errors since startup",
    )

    def to_dict(self) -> dict:
        """Convert to dictionary with ISO format timestamps."""
        data = self.model_dump()
        data["last_heartbeat"] = self.last_heartbeat.isoformat()
        if self.tool_start_time:
            data["tool_start_time"] = self.tool_start_time.isoformat()
        data["recent_calls"] = [
            {
                **call,
                "timestamp": (
                    call["timestamp"].isoformat() if isinstance(call["timestamp"], datetime) else call["timestamp"]
                ),
            }
            for call in data["recent_calls"]
        ]
        return data


class ToolEvent(BaseModel):
    """Event published by MCP server to coordinator."""

    event_type: ToolEventType = Field(..., description="Type of event")
    instance_id: str = Field(..., description="Source instance identifier")
    timestamp: datetime = Field(default_factory=datetime.now, description="Event timestamp")
    tool_name: Optional[str] = Field(default=None, description="Tool name for tool events")
    tool_input: Optional[str] = Field(default=None, description="Input arguments for TOOL_START")
    tool_output: Optional[str] = Field(default=None, description="Output result for TOOL_END")
    duration_ms: Optional[int] = Field(default=None, description="Duration for TOOL_END events")
    error_message: Optional[str] = Field(default=None, description="Error message for TOOL_ERROR events")
    log_data: Optional[str] = Field(default=None, description="Log content for TOOL_LOG events")
    uptime_seconds: Optional[float] = Field(default=None, description="Uptime for HEARTBEAT/REGISTER events")

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "ToolEvent":
        """Deserialize from JSON string."""
        return cls.model_validate_json(json_str)


class AggregatedState(BaseModel):
    """Complete aggregated state for dashboard consumption."""

    type: str = Field(default="state_update", description="Message type identifier")
    timestamp: datetime = Field(default_factory=datetime.now, description="State snapshot timestamp")
    instances: list[InstanceStatus] = Field(default_factory=list, description="Status of all known instances")

    def to_json(self) -> str:
        """Serialize to JSON for WebSocket transmission."""
        data = {
            "type": self.type,
            "timestamp": self.timestamp.isoformat(),
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
    timestamp: datetime = Field(default_factory=datetime.now, description="Message timestamp")

    def to_json(self) -> str:
        """Serialize to JSON string."""
        data = self.model_dump()
        data["timestamp"] = self.timestamp.isoformat()
        import json

        return json.dumps(data)
