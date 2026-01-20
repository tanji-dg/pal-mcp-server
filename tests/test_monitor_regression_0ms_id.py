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

    def test_duration_calculation_non_zero(self, tracker):
        """Verify that duration calculation in log_activity doesn't result in 0ms if start time exists."""
        tracker.start_tool("clink")
        
        # Tool Use
        tracker.log_activity("clink", json.dumps({
            "type": "tool_use",
            "tool_id": "t1",
            "name": "Bash"
        }))
        
        # Simulate immediate result (delta < 1ms)
        # We keep the start time identical to 'now'
        start_time = tracker._tool_start_times["t1"]
        
        # Result
        tracker.log_activity("clink", json.dumps({
            "type": "tool_result",
            "tool_id": "t1",
            "status": "success"
        }), original_event=ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=tracker.instance_id,
            timestamp=start_time # Exact same time
        ))
        
        last_call = tracker.recent_calls[0]
        # Should be at least 1ms, not 0
        assert last_call.duration_ms >= 1
        
        # Check window metrics
        assert len(tracker._durations_1m) == 1
        assert tracker._durations_1m[0][1] >= 1
        assert tracker.get_avg_execution_time_1m() >= 1.0

    def test_to_status_id_visibility(self, tracker):
        """Verify session_id is visible in status even if idle."""
        tracker.session_id = "thread:xyz"
        tracker.state = "idle"
        
        status = tracker.to_status()
        # FIX: Now session_id should be preserved for correlation
        assert status.session_id == "thread:xyz"
