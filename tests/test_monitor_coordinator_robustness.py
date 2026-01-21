"""
Regression tests for Monitor Coordinator to ensure robustness and correctness of token aggregation.
"""

import json
import pytest
import time
from monitor.coordinator import InstanceTracker, MonitorCoordinator
from monitor.models import ToolEvent, ToolEventType

class TestMonitorCoordinatorRobustness:

    @pytest.fixture
    def tracker(self):
        return InstanceTracker("test_instance@host")

    def test_module_import_and_init(self):
        """Regression test to ensure no SyntaxError or IndentationError in the module."""
        from monitor.coordinator import InstanceTracker, MonitorCoordinator
        tracker = InstanceTracker("test")
        assert tracker is not None
        coordinator = MonitorCoordinator()
        assert coordinator is not None

    def test_incremental_token_aggregation_streaming(self, tracker):
        """
        Regression test for double-counting tokens in streaming mode.
        Many AI CLIs report CUMULATIVE tokens in each stream chunk.
        We must only add the delta to the instance lifetime total.
        """
        # First chunk: 100 input tokens
        chunk1 = {"type": "usage", "usage": {"input_tokens": 100, "output_tokens": 10}}
        tracker.log_activity("clink", json.dumps(chunk1))
        
        assert tracker.input_tokens == 100
        assert tracker.output_tokens == 10
        
        # Second chunk: cumulative 150 input tokens (delta is 50)
        chunk2 = {"type": "usage", "usage": {"input_tokens": 150, "output_tokens": 25}}
        tracker.log_activity("clink", json.dumps(chunk2))
        
        # Total should be 150, NOT 100 + 150 = 250
        assert tracker.input_tokens == 150
        assert tracker.output_tokens == 25
        
        # Third chunk: another report for same values (should not change anything)
        tracker.log_activity("clink", json.dumps(chunk2))
        assert tracker.input_tokens == 150
        assert tracker.output_tokens == 25

    def test_token_reset_on_new_tool(self, tracker):
        """
        Verify that request-local token counters reset when a new tool starts,
        allowing independent delta calculations for the new request.
        """
        # First request
        tracker.start_tool("clink")
        tracker.log_activity("clink", json.dumps({"usage": {"input_tokens": 100}}))
        assert tracker.input_tokens == 100
        tracker.end_tool("clink", 1000)
        
        # Second request (e.g. user asks another question)
        tracker.start_tool("clink")
        # New request starts from 0 cumulative tokens
        tracker.log_activity("clink", json.dumps({"usage": {"input_tokens": 50}}))
        
        # Lifetime total should be 100 (first) + 50 (second) = 150
        assert tracker.input_tokens == 150
        
        # Cumulative update for second request (to 120 total for this request)
        tracker.log_activity("clink", json.dumps({"usage": {"input_tokens": 120}}))
        
        # Lifetime: 100 (first) + 120 (second) = 220
        assert tracker.input_tokens == 220

    def test_token_aggregation_multi_model(self, tracker):
        """Verify token delta logic works across multiple models in a single response."""
        tracker.start_tool("clink")
        
        # First report
        usage1 = {
            "type": "result",
            "modelUsage": {
                "model-a": {"inputTokens": 100, "outputTokens": 10},
                "model-b": {"inputTokens": 50, "outputTokens": 5}
            }
        }
        tracker.log_activity("clink", json.dumps(usage1))
        assert tracker.input_tokens == 150
        assert tracker.output_tokens == 15
        
        # Second report (cumulative increase in model-a)
        usage2 = {
            "type": "result",
            "modelUsage": {
                "model-a": {"inputTokens": 150, "outputTokens": 20},
                "model-b": {"inputTokens": 50, "outputTokens": 5}
            }
        }
        tracker.log_activity("clink", json.dumps(usage2))
        
        # Total: model-a (150) + model-b (50) = 200
        assert tracker.input_tokens == 200
        assert tracker.output_tokens == 25

    def test_nested_json_extraction_claude(self, tracker):
        """Verify tokens are extracted from Claude's nested structure using delta logic."""
        tracker.start_tool("clink")
        
        # Claude message_start
        event = {
            "type": "stream_event",
            "event": {
                "type": "message_start",
                "message": {"usage": {"input_tokens": 1000}}
            }
        }
        tracker.log_activity("clink", json.dumps(event))
        assert tracker.input_tokens == 1000
        
        # Claude message_delta (reports cumulative output tokens)
        delta = {
            "type": "stream_event",
            "event": {
                "type": "message_delta",
                "usage": {"output_tokens": 50}
            }
        }
        tracker.log_activity("clink", json.dumps(delta))
        assert tracker.output_tokens == 50
        
        # Another delta (cumulative output tokens 120)
        delta2 = {
            "type": "stream_event",
            "event": {
                "type": "message_delta",
                "usage": {"output_tokens": 120}
            }
        }
        tracker.log_activity("clink", json.dumps(delta2))
        assert tracker.output_tokens == 120

    def test_end_tool_with_text_content_list(self, tracker):
        """
        Regression test for extracting tokens from MCP-style list[TextContent].
        TextContent is often serialized as a list of dicts with a 'text' field 
        containing a JSON string of the actual ToolOutput.
        """
        tracker.start_tool("clink")
        
        # Simulated serialized TextContent list
        output = [
            {
                "type": "text",
                "text": json.dumps({
                    "status": "success",
                    "content": "Hello",
                    "metadata": {
                        "usage": {"input_tokens": 500, "output_tokens": 50}
                    }
                })
            }
        ]
        
        tracker.end_tool("clink", 1000, tool_output=json.dumps(output))
        
        assert tracker.input_tokens == 500
        assert tracker.output_tokens == 50

    def test_mixed_log_formats_resilience(self, tracker):
        """Ensure coordinator handles mixed JSON and raw text without losing state or crashing."""
        tracker.start_tool("clink")
        
        # Valid JSON usage
        tracker.log_activity("clink", json.dumps({"usage": {"input_tokens": 100}}))
        assert tracker.input_tokens == 100
        
        # Raw text log
        tracker.log_activity("clink", "Thinking about the problem...")
        assert tracker.last_status == "Thinking"
        
        # Malformed JSON (should be ignored safely)
        tracker.log_activity("clink", '{"type": "partial_json", ...}')
        assert tracker.input_tokens == 100
        
        # Subsequent valid JSON usage
        tracker.log_activity("clink", json.dumps({"usage": {"input_tokens": 150}}))
        assert tracker.input_tokens == 150

    def test_state_transition_reliability(self, tracker):
        """Verify that tracker transitions correctly between busy and idle even with late logs."""
        # Start tool
        tracker.start_tool("clink")
        assert tracker.state == "busy"
        
        # Logs arrive
        tracker.log_activity("clink", "Processing...")
        assert tracker.state == "busy"
        
        # Tool ends
        tracker.end_tool("clink", 1000)
        assert tracker.state == "idle"
        
        # LATE log arrives for the SAME tool after it ended
        tracker.log_activity("clink", "Finished cleanup")
        
        # Should NOT pull tracker back to busy
        assert tracker.state == "idle"
        
        # NEW tool start event in log (e.g. we missed the start_tool call)
        tracker.log_activity("clink", '{"type": "tool_use", "tool_name": "ls"}')
        assert tracker.state == "busy"
        assert tracker.active_tool == "ls"
