"""
Unit tests for PAL MCP Server monitoring coordinator.

Tests coordinator logic including instance tracking, event processing,
and WebSocket client management.
"""

import asyncio
import time
from datetime import datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from monitor.coordinator import (
    INSTANCE_TIMEOUT,
    InstanceTracker,
    MonitorCoordinator,
    get_coordinator,
)
from monitor.models import (
    InstanceStatus,
    ToolEvent,
    ToolEventType,
)


class TestInstanceTracker:
    """Tests for InstanceTracker class."""

    def test_create_tracker(self):
        """Test creating a new instance tracker."""
        tracker = InstanceTracker("12345@hostname", uptime_seconds=100.0)
        assert tracker.instance_id == "12345@hostname"
        assert tracker.state == "idle"
        assert tracker.active_tool is None

    def test_update_heartbeat(self):
        """Test heartbeat update."""
        tracker = InstanceTracker("12345@hostname")
        old_heartbeat = tracker.last_heartbeat
        time.sleep(0.01)
        tracker.update_heartbeat()
        assert tracker.last_heartbeat > old_heartbeat

    def test_start_tool(self):
        """Test starting a tool execution."""
        tracker = InstanceTracker("12345@hostname")
        tracker.start_tool("chat")
        assert tracker.state == "busy"
        assert tracker.active_tool == "chat"
        assert tracker.tool_start_time is not None

    def test_end_tool_success(self):
        """Test ending a tool execution successfully."""
        tracker = InstanceTracker("12345@hostname")
        tracker.start_tool("analyze")
        tracker.end_tool(duration_ms=1500, is_error=False)

        assert tracker.state == "idle"
        assert tracker.active_tool is None
        assert tracker.tool_start_time is None
        assert len(tracker.recent_calls) == 1
        assert tracker.recent_calls[0].tool == "analyze"
        assert tracker.recent_calls[0].status == "success"

    def test_end_tool_error(self):
        """Test ending a tool execution with error."""
        tracker = InstanceTracker("12345@hostname")
        tracker.start_tool("debug")
        tracker.end_tool(duration_ms=500, is_error=True)

        assert len(tracker.recent_calls) == 1
        assert tracker.recent_calls[0].status == "error"

    def test_tool_io_tracking(self):
        """Test tracking tool input and output."""
        tracker = InstanceTracker("12345@hostname")
        tracker.start_tool("chat", tool_input='{"message": "hello"}')
        assert tracker.active_tool_input == '{"message": "hello"}'

        tracker.end_tool(duration_ms=100, is_error=False, tool_output='{"response": "hi"}')
        assert len(tracker.recent_calls) == 1
        call = tracker.recent_calls[0]
        assert call.tool_input == '{"message": "hello"}'
        assert call.tool_output == '{"response": "hi"}'

    def test_recent_calls_limit(self):
        """Test that recent calls are limited."""
        tracker = InstanceTracker("12345@hostname")

        # Execute more tools than the limit
        for i in range(25):
            tracker.start_tool(f"tool_{i}")
            tracker.end_tool(duration_ms=100)

        # Should only keep MAX_RECENT_CALLS (20)
        assert len(tracker.recent_calls) <= 20

    def test_get_uptime(self):
        """Test uptime calculation."""
        tracker = InstanceTracker("12345@hostname", uptime_seconds=100.0)
        time.sleep(0.1)
        uptime = tracker.get_uptime()
        assert uptime >= 100.1

    def test_error_rate_1m_no_calls(self):
        """Test error rate with no calls."""
        tracker = InstanceTracker("12345@hostname")
        assert tracker.get_error_rate_1m() == 0.0

    def test_error_rate_1m_with_errors(self):
        """Test error rate calculation with some errors."""
        tracker = InstanceTracker("12345@hostname")

        # 2 successes, 2 errors = 50% error rate
        for _ in range(2):
            tracker.start_tool("test")
            tracker.end_tool(100, is_error=False)
        for _ in range(2):
            tracker.start_tool("test")
            tracker.end_tool(100, is_error=True)

        error_rate = tracker.get_error_rate_1m()
        assert 0.49 <= error_rate <= 0.51

    def test_avg_execution_time_1m(self):
        """Test average execution time calculation."""
        tracker = InstanceTracker("12345@hostname")

        # 3 calls with durations 100, 200, 300 -> avg 200
        for duration in [100, 200, 300]:
            tracker.start_tool("test")
            tracker.end_tool(duration)

        avg_time = tracker.get_avg_execution_time_1m()
        assert 199 <= avg_time <= 201

    def test_is_timed_out(self):
        """Test timeout detection."""
        tracker = InstanceTracker("12345@hostname")
        assert not tracker.is_timed_out()

        # Manually set old heartbeat
        tracker.last_heartbeat = datetime.now() - timedelta(seconds=INSTANCE_TIMEOUT + 10)
        assert tracker.is_timed_out()

    def test_to_status_idle(self):
        """Test converting to InstanceStatus when idle."""
        tracker = InstanceTracker("12345@hostname", uptime_seconds=100.0)
        status = tracker.to_status()

        assert isinstance(status, InstanceStatus)
        assert status.instance_id == "12345@hostname"
        assert status.state == "idle"
        assert status.active_tool is None

    def test_to_status_busy(self):
        """Test converting to InstanceStatus when busy."""
        tracker = InstanceTracker("12345@hostname")
        tracker.start_tool("chat")
        status = tracker.to_status()

        assert status.state == "busy"
        assert status.active_tool == "chat"
        assert status.tool_start_time is not None

    def test_to_status_offline(self):
        """Test converting to InstanceStatus when timed out."""
        tracker = InstanceTracker("12345@hostname")
        tracker.last_heartbeat = datetime.now() - timedelta(seconds=INSTANCE_TIMEOUT + 10)
        status = tracker.to_status()

        assert status.state == "offline"
        assert status.active_tool is None  # Should be None when offline


class TestMonitorCoordinator:
    """Tests for MonitorCoordinator class."""

    @pytest.fixture
    def coordinator(self):
        """Create a fresh coordinator for each test."""
        return MonitorCoordinator()

    @pytest.mark.asyncio
    async def test_process_register_event(self, coordinator):
        """Test processing a registration event."""
        event = ToolEvent(
            event_type=ToolEventType.REGISTER,
            instance_id="12345@hostname",
            uptime_seconds=0.0,
        )
        await coordinator.process_event(event)

        assert "12345@hostname" in coordinator.instances
        assert coordinator.instances["12345@hostname"].instance_id == "12345@hostname"

    @pytest.mark.asyncio
    async def test_process_unregister_event(self, coordinator):
        """Test processing an unregistration event."""
        # First register
        register_event = ToolEvent(
            event_type=ToolEventType.REGISTER,
            instance_id="12345@hostname",
        )
        await coordinator.process_event(register_event)
        assert "12345@hostname" in coordinator.instances

        # Then unregister
        unregister_event = ToolEvent(
            event_type=ToolEventType.UNREGISTER,
            instance_id="12345@hostname",
        )
        await coordinator.process_event(unregister_event)
        assert "12345@hostname" not in coordinator.instances

    @pytest.mark.asyncio
    async def test_process_heartbeat_event(self, coordinator):
        """Test processing a heartbeat event."""
        # Register first
        register_event = ToolEvent(
            event_type=ToolEventType.REGISTER,
            instance_id="12345@hostname",
        )
        await coordinator.process_event(register_event)
        old_heartbeat = coordinator.instances["12345@hostname"].last_heartbeat

        # Send heartbeat
        await asyncio.sleep(0.01)
        heartbeat_event = ToolEvent(
            event_type=ToolEventType.HEARTBEAT,
            instance_id="12345@hostname",
            uptime_seconds=10.0,
        )
        await coordinator.process_event(heartbeat_event)

        new_heartbeat = coordinator.instances["12345@hostname"].last_heartbeat
        assert new_heartbeat > old_heartbeat

    @pytest.mark.asyncio
    async def test_process_tool_start_event(self, coordinator):
        """Test processing a tool start event."""
        # Register first
        register_event = ToolEvent(
            event_type=ToolEventType.REGISTER,
            instance_id="12345@hostname",
        )
        await coordinator.process_event(register_event)

        # Start tool
        start_event = ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id="12345@hostname",
            tool_name="chat",
        )
        await coordinator.process_event(start_event)

        tracker = coordinator.instances["12345@hostname"]
        assert tracker.state == "busy"
        assert tracker.active_tool == "chat"

    @pytest.mark.asyncio
    async def test_process_tool_end_event(self, coordinator):
        """Test processing a tool end event."""
        # Register and start tool
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id="12345@hostname",
            )
        )
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_START,
                instance_id="12345@hostname",
                tool_name="analyze",
            )
        )

        # End tool
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_END,
                instance_id="12345@hostname",
                tool_name="analyze",
                duration_ms=1500,
            )
        )

        tracker = coordinator.instances["12345@hostname"]
        assert tracker.state == "idle"
        assert tracker.active_tool is None
        assert len(tracker.recent_calls) == 1

    @pytest.mark.asyncio
    async def test_process_tool_error_event(self, coordinator):
        """Test processing a tool error event."""
        # Register and start tool
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id="12345@hostname",
            )
        )
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_START,
                instance_id="12345@hostname",
                tool_name="debug",
            )
        )

        # Tool error
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_ERROR,
                instance_id="12345@hostname",
                tool_name="debug",
                duration_ms=500,
                error_message="API timeout",
            )
        )

        tracker = coordinator.instances["12345@hostname"]
        assert tracker.state == "idle"
        assert len(tracker.recent_calls) == 1
        assert tracker.recent_calls[0].status == "error"

    @pytest.mark.asyncio
    async def test_process_tool_io_events(self, coordinator):
        """Test processing tool start/end with I/O data."""
        # Register
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id="12345@hostname",
            )
        )

        # Start with input
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_START,
                instance_id="12345@hostname",
                tool_name="chat",
                tool_input="input_data",
            )
        )

        tracker = coordinator.instances["12345@hostname"]
        assert tracker.active_tool_input == "input_data"

        # End with output
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_END,
                instance_id="12345@hostname",
                tool_name="chat",
                duration_ms=100,
                tool_output="output_data",
            )
        )

        call = tracker.recent_calls[0]
        assert call.tool_input == "input_data"
        assert call.tool_output == "output_data"

    @pytest.mark.asyncio
    async def test_auto_register_on_event(self, coordinator):
        """Test that instances are auto-registered on first event."""
        # Send tool start without prior registration
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.TOOL_START,
                instance_id="new_instance@host",
                tool_name="chat",
            )
        )

        # Should be auto-registered and tool started
        assert "new_instance@host" in coordinator.instances
        tracker = coordinator.instances["new_instance@host"]
        assert tracker.state == "busy"
        assert tracker.active_tool == "chat"

    @pytest.mark.asyncio
    async def test_get_aggregated_state(self, coordinator):
        """Test getting aggregated state."""
        # Register two instances
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id="inst1@host1",
                uptime_seconds=100.0,
            )
        )
        await coordinator.process_event(
            ToolEvent(
                event_type=ToolEventType.REGISTER,
                instance_id="inst2@host2",
                uptime_seconds=200.0,
            )
        )

        state = await coordinator.get_aggregated_state()
        assert state.type == "state_update"
        assert len(state.instances) == 2

    @pytest.mark.asyncio
    async def test_websocket_client_management(self, coordinator):
        """Test WebSocket client add/remove."""
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()

        await coordinator.add_websocket_client(mock_ws)
        assert mock_ws in coordinator.websocket_clients

        await coordinator.remove_websocket_client(mock_ws)
        assert mock_ws not in coordinator.websocket_clients


class TestGetCoordinator:
    """Tests for get_coordinator function."""

    def test_get_coordinator_returns_singleton(self):
        """Test that get_coordinator returns same instance."""
        # Reset the singleton for clean test
        import monitor.coordinator as coord_module

        coord_module._coordinator = None

        coord1 = get_coordinator()
        coord2 = get_coordinator()
        assert coord1 is coord2

        # Cleanup
        coord_module._coordinator = None

    def test_coordinator_has_required_attributes(self):
        """Test coordinator has required attributes."""
        import monitor.coordinator as coord_module

        coord_module._coordinator = None

        coordinator = get_coordinator()
        assert hasattr(coordinator, "instances")
        assert hasattr(coordinator, "websocket_clients")
        assert hasattr(coordinator, "process_event")
        assert hasattr(coordinator, "get_aggregated_state")

        # Cleanup
        coord_module._coordinator = None
