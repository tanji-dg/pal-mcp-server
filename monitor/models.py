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
from typing import Optional, List

from pydantic import BaseModel, Field


def utc_now():
    """Return current UTC time with timezone info."""
    return datetime.now(timezone.utc)


def format_dt_iso(dt: datetime) -> str:
    """Format datetime to ISO string."""
    if dt.tzinfo:
        # Standard ISO format with Z for UTC
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return dt.isoformat()


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
    duration_ms: int = Field(0, description="Execution duration in milliseconds")
    status: str = Field(..., description="Execution status: 'success' or 'error'")
    model_name: Optional[str] = Field(default=None, description="AI model used for this call")
    timestamp: datetime = Field(default_factory=utc_now, description="When the call completed")


class InstanceStatus(BaseModel):
    """Status report from a single MCP server instance."""

    instance_id: str = Field(..., description="Unique identifier: PID@hostname")
    uptime_seconds: float = Field(default=0.0, description="Seconds since server startup")
    state: str = Field(
        default="idle",
        description="Current state: 'idle', 'busy', or 'offline'",
    )
    last_heartbeat: datetime = Field(
        default_factory=utc_now,
        description="Timestamp of last communication",
    )
    active_tool: Optional[str] = Field(default=None, description="Currently executing tool name, null if idle")
    primary_tool: Optional[str] = Field(default=None, description="The top-level PAL tool that started the current session (e.g. clink, chat)")
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
    total_calls: int = Field(default=0)
    total_errors: int = Field(default=0)
    
    # Session breakdown metrics (current or last session)
    thinking_ms: int = Field(default=0, description="Time spent by the model reasoning in the current session")
    execution_ms: int = Field(default=0, description="Time spent executing sub-tools in the current session")

    # Lifetime metrics (cumulative since instance registration)
    total_thinking_ms: int = Field(default=0, description="Total time spent reasoning across all sessions")
    total_execution_ms: int = Field(default=0, description="Total time spent executing sub-tools across all sessions")
    total_session_ms: int = Field(default=0, description="Total duration of all primary tool sessions")

    # Token usage metrics
    input_tokens: int = Field(default=0)
    output_tokens: int = Field(default=0)
    cache_read_tokens: int = Field(default=0)
    cache_creation_tokens: int = Field(default=0)

    def to_dict(self) -> dict:
        """Convert to dictionary with ISO format timestamps."""
        data = self.model_dump()
        data["last_heartbeat"] = format_dt_iso(self.last_heartbeat)
        if self.tool_start_time:
            data["tool_start_time"] = format_dt_iso(self.tool_start_time)
        
        # Ensure lifetime metrics are preserved in dict (pydantic handles them, but being explicit is safer)
        data["total_thinking_ms"] = self.total_thinking_ms
        data["total_execution_ms"] = self.total_execution_ms
        data["total_session_ms"] = self.total_session_ms

        data["recent_calls"] = [
            {
                **call,
                "timestamp": format_dt_iso(call["timestamp"]) if isinstance(call["timestamp"], datetime) else call["timestamp"]
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
    primary_tool: Optional[str] = Field(default=None, description="The top-level PAL tool that started the session")
    is_primary: bool = Field(default=False, description="Whether this tool should be treated as the root tool for duration tracking")
    uptime_seconds: Optional[float] = Field(default=None, description="Uptime for HEARTBEAT/REGISTER events")

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str) -> "ToolEvent":
        """Deserialize from JSON string."""
        return cls.model_validate_json(json_str)


class EventResponse(BaseModel):
    """Response from coordinator to publisher."""

    status: str = Field(default="ok")
    interrupted: bool = Field(default=False)
    should_summarize: bool = Field(default=False)
    message: Optional[str] = Field(default=None)

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return self.model_dump_json()


class AggregatedState(BaseModel):
    """Complete aggregated state for dashboard consumption."""

    type: str = Field(default="state_update", description="Message type identifier")
    timestamp: datetime = Field(default_factory=utc_now, description="State snapshot timestamp")
    instances: List[InstanceStatus] = Field(default_factory=list, description="Status of all known instances")
    should_summarize: bool = False # Global flag for summarization
    
    # Aggregated metrics
    total_calls: int = 0
    total_errors: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_cache_read_tokens: int = 0
    total_cache_creation_tokens: int = 0
    
    # Global time breakdown (Lifetime totals across all instances)
    total_thinking_ms: int = 0
    total_execution_ms: int = 0
    total_session_ms: int = 0
    
    stats_reset_at: float = 0.0 # Snapshot of when stats were last reset

    def to_json(self) -> str:
        """Serialize to JSON for WebSocket transmission."""
        data = {
            "type": self.type,
            "timestamp": format_dt_iso(self.timestamp),
            "instances": [inst.to_dict() for inst in self.instances],
            "should_summarize": self.should_summarize,
            "total_calls": self.total_calls,
            "total_errors": self.total_errors,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
            "total_cache_read_tokens": self.total_cache_read_tokens,
            "total_cache_creation_tokens": self.total_cache_creation_tokens,
            "total_thinking_ms": self.total_thinking_ms,
            "total_execution_ms": self.total_execution_ms,
            "total_session_ms": self.total_session_ms,
            "stats_reset_at": self.stats_reset_at,
        }
        import json

        return json.dumps(data)

    @classmethod
    def from_instances(cls, instances: List[InstanceStatus], stats_reset_at: float = 0.0, should_summarize: bool = False) -> "AggregatedState":
        """Create aggregated state from list of instance statuses."""
        return cls(
            instances=instances,
            should_summarize=should_summarize,
            total_calls=sum((i.total_calls or 0) for i in instances),
            total_errors=sum((i.total_errors or 0) for i in instances),
            total_input_tokens=sum((i.input_tokens or 0) for i in instances),
            total_output_tokens=sum((i.output_tokens or 0) for i in instances),
            total_cache_read_tokens=sum((i.cache_read_tokens or 0) for i in instances),
            total_cache_creation_tokens=sum((i.cache_creation_tokens or 0) for i in instances),
            total_thinking_ms=sum((i.total_thinking_ms or 0) for i in instances),
            total_execution_ms=sum((i.total_execution_ms or 0) for i in instances),
            total_session_ms=sum((i.total_session_ms or 0) for i in instances),
            stats_reset_at=stats_reset_at,
        )


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
            data["timestamp"] = format_dt_iso(data["timestamp"])
        
        import json

        return json.dumps(data)