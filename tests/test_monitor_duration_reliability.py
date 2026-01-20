"""
Unit tests for verifying tool execution duration tracking in InstanceTracker.
Specifically targets Claude nested events and Codex events.
"""

import json
import time
import pytest
from datetime import timedelta
from monitor.coordinator import InstanceTracker
from monitor.models import utc_now

class TestMonitorDurationReliability:

    @pytest.fixture
    def tracker(self):
        """Create a fresh instance tracker."""
        return InstanceTracker("test_instance")

    def test_claude_nested_tool_duration(self, tracker):
        """Verify that duration is correctly tracked for Claude nested stream events."""
        tracker.start_tool("clink")
        
        # 1. Tool Start (Nested)
        start_log = json.dumps({
            "type": "stream_event",
            "event": {
                "type": "tool_use",
                "tool_name": "Bash",
                "tool_id": "toolu_1"
            }
        })
        tracker.log_activity("clink", start_log)
        
        # Manually backdate the start time for testing (simulating passage of time)
        tracker._tool_start_times["toolu_1"] = utc_now() - timedelta(seconds=2)
        
        # 2. Tool Result (Top-level or Nested)
        result_log = json.dumps({
            "type": "tool_result",
            "tool_id": "toolu_1",
            "content": "done"
        })
        tracker.log_activity("clink", result_log)
        
        # 3. Check Duration
        assert tracker.total_calls == 1
        last_call = tracker.recent_calls[0]
        assert last_call.tool == "Bash"
        # Duration should be around 2000ms
        assert 1900 <= last_call.duration_ms <= 2100
        assert tracker.get_avg_execution_time_1m() >= 1900

    def test_codex_item_duration(self, tracker):
        """Verify duration tracking for Codex item.started/completed events."""
        tracker.start_tool("clink")
        
        # 1. Item Started
        start_log = json.dumps({
            "type": "item.started",
            "item": {
                "id": "item_1",
                "type": "command_execution",
                "command": "ls -la"
            }
        })
        tracker.log_activity("clink", start_log)
        
        # Backdate
        tracker._tool_start_times["item_1"] = utc_now() - timedelta(milliseconds=500)
        
        # 2. Item Completed
        complete_log = json.dumps({
            "type": "item.completed",
            "item": {
                "id": "item_1",
                "type": "command_execution",
                "status": "success",
                "command": "ls -la"
            }
        })
        tracker.log_activity("clink", complete_log)
        
        # 3. Check
        assert tracker.total_calls == 1
        last_call = tracker.recent_calls[0]
        assert last_call.tool == "ls"
        assert 450 <= last_call.duration_ms <= 550
        assert tracker.get_avg_execution_time_1m() >= 450

    def test_average_execution_time_averaging(self, tracker):
        """Verify that avg_execution_time_1m correctly averages multiple calls."""
        # Add two manual durations
        now = time.time()
        tracker._durations_1m = [
            (now - 10, 1000), # 1000ms
            (now - 5, 2000),  # 2000ms
        ]
        
        assert tracker.get_avg_execution_time_1m() == 1500.0
