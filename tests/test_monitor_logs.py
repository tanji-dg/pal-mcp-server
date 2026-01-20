"""
Unit tests for monitor log parsing logic in InstanceTracker.
"""

import json
import pytest
from monitor.coordinator import InstanceTracker

class TestInstanceTrackerLogs:
    """Tests for InstanceTracker.log_activity method."""

    @pytest.fixture
    def tracker(self):
        """Create a fresh instance tracker."""
        return InstanceTracker("test_instance")

    def test_log_activity_plain_text(self, tracker):
        """Test parsing plain text logs."""
        tracker.log_activity("tool_name", "Just some random log")
        # Should NOT change status if idle and not a trigger keyword
        assert tracker.state == "idle"

        # Force busy to test updates
        tracker.start_tool("test_tool")
        
        tracker.log_activity("test_tool", "Calculating something...")
        assert tracker.last_status == "Calculating something..."

        tracker.log_activity("test_tool", "thinking about life")
        assert tracker.last_status == "Thinking"

        tracker.log_activity("test_tool", "calling tool now")
        assert tracker.last_status == "Executing"

        tracker.log_activity("test_tool", "returning result")
        assert tracker.last_status == "Finishing"

    def test_log_activity_json_message(self, tracker):
        """Test parsing JSON message event."""
        tracker.start_tool("clink")
        
        log_data = json.dumps({
            "type": "message",
            "content": "I am thinking...",
            "delta": True
        })
        tracker.log_activity("clink", log_data)
        assert tracker.last_status == "Thinking"

    def test_log_activity_json_tool_use(self, tracker):
        """Test parsing JSON tool_use event."""
        tracker.start_tool("clink")
        
        log_data = json.dumps({
            "type": "tool_use",
            "tool_id": "call_123",
            "name": "search_web",
            "input": {"query": "python"}
        })
        tracker.log_activity("clink", log_data)
        
        assert tracker.last_status == "Calling search_web"
        assert tracker._tool_name_cache["call_123"] == "search_web"
        assert "call_123" in tracker._tool_start_times

    def test_log_activity_json_tool_result_success(self, tracker):
        """Test parsing JSON tool_result event (success)."""
        tracker.start_tool("clink")
        # Pre-populate cache
        tracker._tool_name_cache["call_123"] = "search_web"
        
        log_data = json.dumps({
            "type": "tool_result",
            "tool_id": "call_123",
            "status": "success",
            "content": "Found 10 results"
        })
        tracker.log_activity("clink", log_data)
        
        assert tracker.last_status == "Result from search_web"

    def test_log_activity_json_tool_result_error(self, tracker):
        """Test parsing JSON tool_result event (error)."""
        tracker.start_tool("clink")
        tracker._tool_name_cache["call_123"] = "search_web"
        
        log_data = json.dumps({
            "type": "tool_result",
            "tool_id": "call_123",
            "status": "error",
            "content": "Timeout occurred"
        })
        tracker.log_activity("clink", log_data)
        
        assert tracker.last_status == "Error in search_web"

    def test_log_activity_json_tool_result_content_error(self, tracker):
        """Test parsing JSON tool_result with error in content."""
        tracker.start_tool("clink")
        tracker._tool_name_cache["call_123"] = "search_web"
        
        log_data = json.dumps({
            "type": "tool_result",
            "tool_id": "call_123",
            "status": "success", 
            "content": "Error: API unavailable"
        })
        tracker.log_activity("clink", log_data)
        
        assert tracker.last_status == "Error in search_web"

    def test_log_activity_model_update(self, tracker):
        """Test model name update from logs."""
        tracker.start_tool("clink")
        tracker.model_name = "claude-3-5-sonnet" # High level name
        
        # 1. Low level model shouldn't overwrite high level one
        log_data = json.dumps({
            "type": "message",
            "model": "gemini-2.0-flash-lite"
        })
        tracker.log_activity("clink", log_data)
        assert tracker.model_name == "claude-3-5-sonnet"

        # 2. High level model SHOULD overwrite
        log_data = json.dumps({
            "type": "message",
            "model": "gpt-4-turbo"
        })
        tracker.log_activity("clink", log_data)
        assert tracker.model_name == "gpt-4-turbo"

        # 3. Model from metadata
        log_data = json.dumps({
            "type": "message",
            "metadata": {"model_used": "claude-3-opus"}
        })
        tracker.log_activity("clink", log_data)
        assert tracker.model_name == "claude-3-opus"

    def test_log_activity_codex_events(self, tracker):
        """Test Codex specific events."""
        tracker.start_tool("codex")
        
        # Item started
        log_data = json.dumps({
            "type": "item.started",
            "item": {
                "id": "item_1",
                "type": "command_execution",
                "command": "ls -la"
            }
        })
        tracker.log_activity("codex", log_data)
        assert tracker.last_status == "Executing ls -la"
        
        # Item completed error
        log_data = json.dumps({
            "type": "item.completed",
            "item": {
                "id": "item_1",
                "status": "failed",
                "command": "ls -la"
            }
        })
        tracker.log_activity("codex", log_data)
        assert tracker.last_status == "Error in ls -la"

    def test_log_activity_mixed_chunk(self, tracker):
        """Test parsing chunk with multiple JSON objects."""
        tracker.start_tool("clink")
        
        # Two JSON objects in one string (simulating stream chunk)
        log_data = (
            json.dumps({"type": "message", "content": "Thinking..."}) + "\n" +
            json.dumps({"type": "tool_use", "name": "calculator", "tool_id": "calc_1"})
        )
        
        tracker.log_activity("clink", log_data)
        
        # Should process both, last one wins for status
        assert tracker.last_status == "Calling calculator"
        assert tracker._tool_name_cache["calc_1"] == "calculator"
