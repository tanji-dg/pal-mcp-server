"""
Unit tests for verifying tool execution duration tracking in InstanceTracker.
Specifically targets Claude nested events and Codex events.
"""

import json
import time
import pytest
from datetime import timedelta, datetime, timezone
from monitor.coordinator import InstanceTracker
from monitor.models import utc_now, ToolEvent, ToolEventType

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

    def test_claude_streaming_sequence_duration(self, tracker):
        """Verify duration calculation for a full Claude streaming tool sequence."""
        # 1. Content block start (Streaming start)
        t1 = datetime(2026, 1, 26, 12, 0, 0, tzinfo=timezone.utc)
        ev1 = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            timestamp=t1,
            log_data=json.dumps({
                "type": "stream_event",
                "event": {
                    "type": "content_block_start",
                    "index": 1,
                    "content_block": {
                        "type": "tool_use",
                        "id": "toolu_123",
                        "name": "test_tool"
                    }
                }
            })
        )
        tracker.log_activity("clink", ev1.log_data, original_event=ev1)
        
        assert "toolu_123" in tracker._tool_start_times
        assert tracker._tool_start_times["toolu_123"] == t1.timestamp()
        
        # 2. Assistant message (Final block) - Should NOT overwrite start time
        t2 = datetime(2026, 1, 26, 12, 0, 1, tzinfo=timezone.utc) # 1s later
        ev2 = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            timestamp=t2,
            log_data=json.dumps({
                "type": "assistant",
                "message": {
                    "content": [{
                        "type": "tool_use",
                        "id": "toolu_123",
                        "name": "test_tool"
                    }]
                }
            })
        )
        tracker.log_activity("clink", ev2.log_data, original_event=ev2)
        
        # Crucial: Start time should still be t1, not t2
        assert tracker._tool_start_times["toolu_123"] == t1.timestamp()
        
        # 3. User message (Tool result)
        t3 = datetime(2026, 1, 26, 12, 0, 3, tzinfo=timezone.utc) # 3s after t1
        ev3 = ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id="test_instance",
            timestamp=t3,
            log_data=json.dumps({
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{
                        "type": "tool_result",
                        "tool_use_id": "toolu_123",
                        "content": "success"
                    }]
                }
            })
        )
        tracker.log_activity("clink", ev3.log_data, original_event=ev3)
        
        # Duration should be t3 - t1 = 3 seconds = 3000ms
        assert len(tracker.recent_calls) == 1
        call = tracker.recent_calls[0]
        assert call.duration_ms == 3000
        assert tracker.execution_ms == 3000
        assert "toolu_123" not in tracker._tool_start_times
