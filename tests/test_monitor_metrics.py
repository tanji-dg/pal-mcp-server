"""
Unit tests for verifying metric counting logic in InstanceTracker.
Specifically ensures that internal tool calls detected via logs do NOT affect 
top-level metrics like total_calls.
"""

import json
import pytest
from monitor.coordinator import InstanceTracker

class TestInstanceTrackerMetrics:
    """Tests for metric aggregation in InstanceTracker."""

    @pytest.fixture
    def tracker(self):
        """Create a fresh instance tracker."""
        return InstanceTracker("test_instance")

    def test_total_calls_counts_log_tools(self, tracker):
        """
        Verify that total_calls IS incremented when tools are detected via logs.
        """
        assert tracker.total_calls == 0
        
        # 1. Start a top-level tool (MCP level)
        tracker.start_tool("clink")
        assert tracker.total_calls == 0  # Not incremented on start
        
        # 2. Simulate internal tool use via logs
        log_tool_use = json.dumps({
            "type": "tool_use",
            "tool_id": "call_internal_1",
            "name": "internal_search",
            "input": {}
        })
        tracker.log_activity("clink", log_tool_use)
        
        import time
        time.sleep(0.1) # Wait a bit to ensure duration > 0

        # 3. Simulate internal tool result via logs
        log_tool_result = json.dumps({
            "type": "tool_result",
            "tool_id": "call_internal_1",
            "status": "success",
            "content": "found results"
        })
        tracker.log_activity("clink", log_tool_result)
        
        # Status should update, AND count SHOULD increment
        assert tracker.last_status == "Result from internal_search"
        assert tracker.total_calls == 1
        
        # Verify sub-tool is in recent calls and has duration
        assert len(tracker.recent_calls) == 1
        call = tracker.recent_calls[0]
        assert call.tool == "internal_search"
        assert call.duration_ms >= 100 # Should be at least 100ms due to sleep
        
        # 4. End the top-level tool (MCP level)
        tracker.end_tool("clink", duration_ms=1000, is_error=False)
        
        # Count should increment again for the parent tool
        assert tracker.total_calls == 2
        
        # Recent calls should have both (newest first)
        assert len(tracker.recent_calls) == 2
        assert tracker.recent_calls[0].tool == "clink"
        assert tracker.recent_calls[1].tool == "internal_search"

    def test_total_errors_counts_log_errors(self, tracker):
        """
        Verify that total_errors IS incremented when internal tools fail.
        """
        assert tracker.total_errors == 0
        
        tracker.start_tool("clink")
        
        # Register tool name first
        log_tool_use = json.dumps({
            "type": "tool_use",
            "tool_id": "call_internal_2",
            "name": "flaky_tool",
            "input": {}
        })
        tracker.log_activity("clink", log_tool_use)

        # Simulate internal tool error via logs
        log_tool_error = json.dumps({
            "type": "tool_result",
            "tool_id": "call_internal_2",
            "name": "flaky_tool",
            "status": "error",
            "content": "Timeout"
        })
        tracker.log_activity("clink", log_tool_error)
        
        assert tracker.last_status == "Error in flaky_tool"
        assert tracker.total_errors == 1  # Should count for stats
        
        # End top level tool with success
        tracker.end_tool("clink", duration_ms=1000, is_error=False)
        assert tracker.total_calls == 2  # 1 sub-tool + 1 top-level
        assert tracker.total_errors == 1
        
    def test_total_errors_counts_mcp_errors(self, tracker):
        """
        Verify that total_errors IS incremented when MCP tool fails.
        """
        tracker.start_tool("clink")
        tracker.end_tool("clink", duration_ms=1000, is_error=True)
        
        assert tracker.total_calls == 1
        assert tracker.total_errors == 1
