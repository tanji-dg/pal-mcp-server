"""
Unit tests for PAL MCP Server monitoring models.

Tests serialization, deserialization, and validation of monitoring data models.
"""

import json
from datetime import datetime

from monitor.models import (
    AggregatedState,
    InstanceStatus,
    ToolCall,
    ToolEvent,
    ToolEventType,
    WebSocketMessage,
)


class TestToolCall:
    """Tests for ToolCall model."""

    def test_create_tool_call(self):
        """Test creating a basic tool call."""
        call = ToolCall(tool="chat", duration_ms=1234, status="success")
        assert call.tool == "chat"
        assert call.duration_ms == 1234
        assert call.status == "success"
        assert isinstance(call.timestamp, datetime)

    def test_tool_call_with_custom_timestamp(self):
        """Test tool call with explicit timestamp."""
        ts = datetime(2026, 1, 18, 12, 0, 0)
        call = ToolCall(tool="debug", duration_ms=5000, status="error", timestamp=ts)
        assert call.timestamp == ts

    def test_tool_call_serialization(self):
        """Test tool call JSON serialization."""
        call = ToolCall(tool="analyze", duration_ms=2500, status="success")
        data = call.model_dump()
        assert data["tool"] == "analyze"
        assert data["duration_ms"] == 2500
        assert data["status"] == "success"


class TestInstanceStatus:
    """Tests for InstanceStatus model."""

    def test_create_instance_status(self):
        """Test creating basic instance status."""
        status = InstanceStatus(
            instance_id="12345@hostname",
            uptime_seconds=3600.0,
        )
        assert status.instance_id == "12345@hostname"
        assert status.uptime_seconds == 3600.0
        assert status.state == "idle"
        assert status.active_tool is None
        assert status.error_rate_1m == 0.0

    def test_instance_status_with_active_tool(self):
        """Test instance status with active tool."""
        ts = datetime.now()
        status = InstanceStatus(
            instance_id="12345@hostname",
            uptime_seconds=3600.0,
            state="busy",
            active_tool="codereview",
            tool_start_time=ts,
        )
        assert status.state == "busy"
        assert status.active_tool == "codereview"
        assert status.tool_start_time == ts

    def test_instance_status_with_recent_calls(self):
        """Test instance status with recent call history."""
        calls = [
            ToolCall(tool="chat", duration_ms=1000, status="success"),
            ToolCall(tool="debug", duration_ms=2000, status="success"),
        ]
        status = InstanceStatus(
            instance_id="12345@hostname",
            uptime_seconds=3600.0,
            recent_calls=calls,
        )
        assert len(status.recent_calls) == 2
        assert status.recent_calls[0].tool == "chat"

    def test_to_dict_iso_timestamps(self):
        """Test that to_dict produces ISO format timestamps."""
        ts = datetime(2026, 1, 18, 12, 0, 0)
        status = InstanceStatus(
            instance_id="12345@hostname",
            uptime_seconds=3600.0,
            last_heartbeat=ts,
            tool_start_time=ts,
            state="busy",
            active_tool="test",
        )
        data = status.to_dict()
        assert data["last_heartbeat"] == "2026-01-18T12:00:00"
        assert data["tool_start_time"] == "2026-01-18T12:00:00"


class TestToolEvent:
    """Tests for ToolEvent model."""

    def test_create_register_event(self):
        """Test creating a register event."""
        event = ToolEvent(
            event_type=ToolEventType.REGISTER,
            instance_id="12345@hostname",
            uptime_seconds=0.0,
        )
        assert event.event_type == ToolEventType.REGISTER
        assert event.instance_id == "12345@hostname"
        assert event.uptime_seconds == 0.0

    def test_create_tool_start_event(self):
        """Test creating a tool start event."""
        event = ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id="12345@hostname",
            tool_name="chat",
        )
        assert event.event_type == ToolEventType.TOOL_START
        assert event.tool_name == "chat"

    def test_create_tool_end_event(self):
        """Test creating a tool end event."""
        event = ToolEvent(
            event_type=ToolEventType.TOOL_END,
            instance_id="12345@hostname",
            tool_name="chat",
            duration_ms=1500,
        )
        assert event.event_type == ToolEventType.TOOL_END
        assert event.duration_ms == 1500

    def test_create_tool_error_event(self):
        """Test creating a tool error event."""
        event = ToolEvent(
            event_type=ToolEventType.TOOL_ERROR,
            instance_id="12345@hostname",
            tool_name="debug",
            duration_ms=500,
            error_message="API timeout",
        )
        assert event.event_type == ToolEventType.TOOL_ERROR
        assert event.error_message == "API timeout"

    def test_to_json_serialization(self):
        """Test JSON serialization."""
        event = ToolEvent(
            event_type=ToolEventType.HEARTBEAT,
            instance_id="12345@hostname",
            uptime_seconds=3600.0,
        )
        json_str = event.to_json()
        data = json.loads(json_str)
        assert data["event_type"] == "heartbeat"
        assert data["instance_id"] == "12345@hostname"

    def test_from_json_deserialization(self):
        """Test JSON deserialization."""
        original = ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id="12345@hostname",
            tool_name="analyze",
        )
        json_str = original.to_json()
        restored = ToolEvent.from_json(json_str)
        assert restored.event_type == original.event_type
        assert restored.instance_id == original.instance_id
        assert restored.tool_name == original.tool_name


class TestAggregatedState:
    """Tests for AggregatedState model."""

    def test_create_empty_state(self):
        """Test creating empty aggregated state."""
        state = AggregatedState()
        assert state.type == "state_update"
        assert len(state.instances) == 0

    def test_create_state_with_instances(self):
        """Test creating state with multiple instances."""
        instances = [
            InstanceStatus(instance_id="1@host1", uptime_seconds=100),
            InstanceStatus(instance_id="2@host2", uptime_seconds=200),
        ]
        state = AggregatedState(instances=instances)
        assert len(state.instances) == 2

    def test_from_instances_factory(self):
        """Test factory method for creating from instance list."""
        instances = [
            InstanceStatus(instance_id="1@host", uptime_seconds=100),
        ]
        state = AggregatedState.from_instances(instances)
        assert state.type == "state_update"
        assert len(state.instances) == 1

    def test_to_json_format(self):
        """Test JSON output format for WebSocket transmission."""
        instances = [
            InstanceStatus(
                instance_id="12345@hostname",
                uptime_seconds=3600,
                state="idle",
            )
        ]
        state = AggregatedState.from_instances(instances)
        json_str = state.to_json()
        data = json.loads(json_str)

        assert data["type"] == "state_update"
        assert "timestamp" in data
        assert len(data["instances"]) == 1
        assert data["instances"][0]["instance_id"] == "12345@hostname"


class TestWebSocketMessage:
    """Tests for WebSocketMessage model."""

    def test_create_message(self):
        """Test creating a WebSocket message."""
        msg = WebSocketMessage(type="ping", payload={"data": "test"})
        assert msg.type == "ping"
        assert msg.payload["data"] == "test"

    def test_to_json_serialization(self):
        """Test JSON serialization."""
        msg = WebSocketMessage(type="status", payload={"count": 5})
        json_str = msg.to_json()
        data = json.loads(json_str)
        assert data["type"] == "status"
        assert data["payload"]["count"] == 5


class TestToolEventType:
    """Tests for ToolEventType enum."""

    def test_event_types_exist(self):
        """Test all event types are defined."""
        assert ToolEventType.TOOL_START.value == "tool_start"
        assert ToolEventType.TOOL_END.value == "tool_end"
        assert ToolEventType.TOOL_ERROR.value == "tool_error"
        assert ToolEventType.HEARTBEAT.value == "heartbeat"
        assert ToolEventType.REGISTER.value == "register"
        assert ToolEventType.UNREGISTER.value == "unregister"

    def test_event_type_string_comparison(self):
        """Test string comparison for event types."""
        assert ToolEventType("tool_start") == ToolEventType.TOOL_START
        assert ToolEventType("heartbeat") == ToolEventType.HEARTBEAT
