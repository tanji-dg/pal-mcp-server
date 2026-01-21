"""
Tests for real-time token usage extraction in Monitor Coordinator.
"""

import json
import pytest
from monitor.coordinator import InstanceTracker
from monitor.models import ToolEvent, ToolEventType

class TestMonitorTokenUsage:

    @pytest.fixture
    def tracker(self):
        return InstanceTracker("test_instance@host")

    def test_extract_tokens_from_claude_stream_event(self, tracker):
        """Verify token extraction from Claude's nested stream_event structure."""
        # Claude message_start event
        event_data = {
            "type": "stream_event",
            "event": {
                "type": "message_start",
                "message": {
                    "usage": {
                        "input_tokens": 1000,
                        "output_tokens": 0
                    }
                }
            }
        }
        
        tracker.log_activity("clink", json.dumps(event_data))
        
        assert tracker.input_tokens == 1000
        assert tracker.output_tokens == 0

        # Claude message_delta event (accumulating)
        delta_data = {
            "type": "stream_event",
            "event": {
                "type": "message_delta",
                "usage": {
                    "output_tokens": 500
                }
            }
        }
        
        tracker.log_activity("clink", json.dumps(delta_data))
        
        assert tracker.input_tokens == 1000
        assert tracker.output_tokens == 500

    def test_extract_tokens_from_gemini_result(self, tracker):
        """Verify token extraction from Gemini's stats structure."""
        # Gemini final result event
        event_data = {
            "type": "result",
            "status": "success",
            "stats": {
                "total_tokens": 5000,
                "input_tokens": 4500,
                "output_tokens": 500,
                "cached": 2000
            }
        }
        
        tracker.log_activity("clink", json.dumps(event_data))
        
        assert tracker.input_tokens == 4500
        assert tracker.output_tokens == 500
        # 'cached' in Gemini maps to cache_read_tokens based on current logic? 
        # Actually our logic looks for 'cache_read_input_tokens'. Let's verify and fix if needed.

    def test_extract_tokens_camel_case_support(self, tracker):
        """Verify support for camelCase keys used in some modelUsage reports."""
        event_data = {
            "type": "usage_report",
            "usage": {
                "inputTokens": 123,
                "outputTokens": 456,
                "cacheReadInputTokens": 789
            }
        }
        
        tracker.log_activity("clink", json.dumps(event_data))
        
        assert tracker.input_tokens == 123
        assert tracker.output_tokens == 456
        assert tracker.cache_read_tokens == 789

    def test_aggregate_model_usage(self, tracker):
        """Verify tokens are aggregated correctly when multiple models are reported."""
        event_data = {
            "type": "result",
            "modelUsage": {
                "gemini-3-flash": {
                    "inputTokens": 1000,
                    "outputTokens": 100
                },
                "gemini-2.0-flash-lite": {
                    "inputTokens": 500,
                    "outputTokens": 50
                }
            }
        }
        
        tracker.log_activity("clink", json.dumps(event_data))
        
        assert tracker.input_tokens == 1500
        assert tracker.output_tokens == 150

    def test_token_propagation_to_status(self, tracker):
        """Verify that token values are included in InstanceStatus."""
        tracker.input_tokens = 123
        tracker.output_tokens = 456
        tracker.cache_read_tokens = 789
        
        status = tracker.to_status()
        assert status.input_tokens == 123
        assert status.output_tokens == 456
        assert status.cache_read_tokens == 789
        
        d = status.to_dict()
        assert d["input_tokens"] == 123
        assert d["output_tokens"] == 456
        assert d["cache_read_tokens"] == 789
