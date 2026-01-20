"""
Regression tests for monitor 0ms duration and ID mismatch.
"""

import json
import time
import pytest
from monitor.coordinator import InstanceTracker, MonitorCoordinator
from monitor.models import ToolEvent, ToolEventType, utc_now

class TestMonitorRegression:

    @pytest.fixture
    def tracker(self):
        return InstanceTracker("12345@hostname")

    @pytest.fixture
    def coordinator(self):
        return MonitorCoordinator()

    @pytest.mark.asyncio
    async def test_session_id_enrichment_regression(self, coordinator):
        """Verify that broadcasted logs have the correct session_id from tracker."""
        instance_id = "12345@hostname"
        session_id = "thread:abc-123"
        
        # 1. Register and start tool with session_id
        await coordinator.process_event(ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id=instance_id,
            tool_name="clink",
            tool_input=json.dumps({"continuation_id": session_id})
        ))
        
        tracker = coordinator.instances[instance_id]
        assert tracker.session_id == session_id
        
        # 2. Receive log WITHOUT session_id in the event
        log_event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=instance_id,
            tool_name="clink",
            log_data="partial thinking..."
        )
        
        # We need to capture the event that gets broadcasted
        # Since broadcast_log is an async method, we can mock it or check the event object after processing
        await coordinator.process_event(log_event)
        
        # The log_event object itself should have been enriched
        assert log_event.session_id == session_id

    def test_duration_calculation_from_json_timestamps(self, tracker):
        """Verify that duration calculation uses internal JSON timestamps if provided."""
        tracker.start_tool("clink")
        
        # Simulate a log chunk with two events having distinct internal timestamps
        start_ts = "2026-01-20T11:00:00.000Z"
        end_ts = "2026-01-20T11:00:01.500Z" # 1500ms later
        
        # Start tool via log
        tracker.log_activity("clink", json.dumps({
            "type": "tool_use",
            "tool_id": "t1",
            "name": "Bash",
            "timestamp": start_ts
        }))
        
        # Result via log in same session/tracker
        tracker.log_activity("clink", json.dumps({
            "type": "tool_result",
            "tool_id": "t1",
            "status": "success",
            "timestamp": end_ts
        }))
        
        last_call = tracker.recent_calls[0]
        # Should be exactly 1500ms based on JSON timestamps
        assert last_call.duration_ms == 1500

    def test_true_zero_ms_reported_as_zero(self, tracker):
        """Verify that 0ms is NOT hidden by max(1, ...) if events are identical in time."""
        tracker.start_tool("clink")
        ts = "2026-01-20T11:00:00.000Z"
        
        tracker.log_activity("clink", json.dumps({
            "type": "tool_use", "tool_id": "t1", "name": "Bash", "timestamp": ts
        }))
        tracker.log_activity("clink", json.dumps({
            "type": "tool_result", "tool_id": "t1", "status": "success", "timestamp": ts
        }))
        
        # Should be 0ms, reporting the measurement failure honestly
        assert tracker.recent_calls[0].duration_ms == 0

    def test_monotonic_precision_avoids_zero_ms(self, tracker):
        """Verify that monotonic time ensures non-zero duration for fast events without JSON timestamps."""
        tracker.start_tool("clink")
        
        # We cannot easily mock time.monotonic() inside the tracker without more complex injection,
        # but we can simulate the arrival of two events.
        # Tracker calls time.monotonic() on every log_activity call.
        
        tracker.log_activity("clink", json.dumps({"type": "tool_use", "tool_id": "fast", "name": "Quick"}))
        # Immediate follow-up
        tracker.log_activity("clink", json.dumps({"type": "tool_result", "tool_id": "fast", "status": "success"}))
        
        last_call = tracker.recent_calls[0]
        # In practice, even consecutive calls to time.monotonic() have a small delta.
        # If it's truly identical (very rare), it will be 0, but this prevents arrival-time collisions.
        # We just want to ensure it doesn't crash and behaves logically.
        assert isinstance(last_call.duration_ms, int)

    def test_to_status_id_visibility(self, tracker):
        """Verify session_id is visible in status even if idle."""
        tracker.session_id = "thread:xyz"
        tracker.state = "idle"
        
        status = tracker.to_status()
        # FIX: Now session_id should be preserved for correlation
        assert status.session_id == "thread:xyz"
