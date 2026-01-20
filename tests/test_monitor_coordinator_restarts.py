"""
Unit tests for monitor coordinator reliability during restarts and late logs.
"""

import json
import pytest
from datetime import timedelta
from monitor.coordinator import InstanceTracker
from monitor.models import ToolEvent, ToolEventType, utc_now

class TestMonitorCoordinatorRestartReliability:

    @pytest.fixture
    def tracker(self):
        """Create a fresh instance tracker (simulating monitor restart)."""
        return InstanceTracker("test_instance")

    def test_busy_on_first_log_after_restart(self, tracker):
        """Verify that a fresh tracker becomes BUSY on the first log, even without TOOL_START."""
        assert tracker.state == "idle"
        assert tracker.active_tool is None
        
        # Simulate receiving a log fragment mid-session
        log_event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            tool_name="clink",
            log_data='{"type": "message", "role": "assistant", "content": "Thinking...", "delta": true}',
            timestamp=utc_now()
        )
        
        tracker.log_activity("clink", log_event.log_data, original_event=log_event)
        
        # Should now be BUSY
        assert tracker.state == "busy"
        assert tracker.active_tool == "clink"
        assert tracker.last_status == "Thinking"

    def test_prevent_rebound_from_late_logs(self, tracker):
        """Verify that late logs from a previous session don't make an IDLE instance BUSY again."""
        # 1. Start and end a tool
        tracker.start_tool("clink")
        tracker.end_tool("clink", 1000)
        assert tracker.state == "idle"
        completion_time = tracker.last_completion_time
        
        # 2. Simulate a late log with an OLD timestamp (before completion)
        late_log_event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            tool_name="clink",
            log_data='{"type": "tool_use", "name": "Bash"}',
            timestamp=completion_time - timedelta(seconds=1)
        )
        
        tracker.log_activity("clink", late_log_event.log_data, original_event=late_log_event)
        
        # Should REMAIND IDLE because the log is older than the last completion
        assert tracker.state == "idle"
        assert tracker.active_tool is None

    def test_busy_on_new_start_event_after_completion(self, tracker):
        """Verify that a NEW tool start (after a previous completion) DOES make it BUSY."""
        # 1. Complete previous
        tracker.start_tool("clink")
        tracker.end_tool("clink", 1000)
        completion_time = tracker.last_completion_time
        
        # 2. Receive a NEW start event log (newer than completion)
        new_log_event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            tool_name="clink",
            log_data='{"type": "tool_use", "name": "Bash"}',
            timestamp=completion_time + timedelta(seconds=1)
        )
        
        tracker.log_activity("clink", new_log_event.log_data, original_event=new_log_event)
        
        # Should become BUSY
        assert tracker.state == "busy"
        assert tracker.last_status == "Calling Bash"

    def test_session_id_tracking(self, tracker):
        """Verify that session_id is captured from various event sources."""
        # 1. From tool start input
        args = json.dumps({"continuation_id": "session_123"})
        tracker.start_tool("clink", args)
        assert tracker.session_id == "session_123"
        
        # 2. From log data
        log_data = json.dumps({"type": "message", "session_id": "session_456"})
        tracker.log_activity("clink", log_data)
        assert tracker.session_id == "session_456"
        
        # 3. From original event object
        log_event = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test",
            tool_name="clink",
            session_id="session_789"
        )
        tracker.log_activity("clink", None, original_event=log_event)
        assert tracker.session_id == "session_789"
        
        # 4. Cleared on completion
        tracker.end_tool("clink", 100)
        assert tracker.session_id is None
