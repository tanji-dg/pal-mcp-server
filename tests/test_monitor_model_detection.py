"""
Regression tests for model name detection logic in monitor/coordinator.py.
"""

import json
from monitor.coordinator import InstanceTracker

def test_monitor_recursive_model_detection():
    """Verify InstanceTracker can find model_name nested deep in log data."""
    tracker = InstanceTracker("test-instance")
    tracker.model_name = "gemini" # Generic start
    
    # 1. Nested JSON object
    log_data = json.dumps({
        "type": "result",
        "metadata": {
            "model_used": "gemini-3-pro-detailed"
        }
    })
    tracker.log_activity("clink", log_data)
    assert tracker.model_name == "gemini-3-pro-detailed"
    
    # 2. Even deeper nesting
    log_data = json.dumps({
        "type": "assistant",
        "message": {
            "model": "gemini-3-flash-final"
        }
    })
    tracker.log_activity("clink", log_data)
    assert tracker.model_name == "gemini-3-flash-final"

def test_monitor_recursive_string_json_detection():
    """Verify InstanceTracker can extract model info from JSON embedded in strings."""
    tracker = InstanceTracker("test-instance")
    tracker.model_name = "claude" # Generic
    
    # Simulate clink output where a tool result contains a JSON string of another tool output
    inner_json = json.dumps({
        "status": "success",
        "metadata": {
            "model_used": "claude-3-7-sonnet-20250219"
        }
    })
    
    outer_log = json.dumps({
        "type": "tool_result",
        "tool_name": "clink",
        "output": inner_json
    })
    
    tracker.log_activity("clink", outer_log)
    assert tracker.model_name == "claude-3-7-sonnet-20250219"

def test_monitor_model_name_priority():
    """Verify detailed model names overwrite generic ones, but not vice versa."""
    tracker = InstanceTracker("test-instance")
    
    # detailed should overwrite None
    tracker.log_activity("clink", json.dumps({"model_used": "gpt-4o-2024-08-06"}))
    assert tracker.model_name == "gpt-4o-2024-08-06"
    
    # generic should NOT overwrite detailed
    tracker.log_activity("clink", json.dumps({"model": "gpt-4"}))
    assert tracker.model_name == "gpt-4o-2024-08-06"
    
    # New detailed SHOULD overwrite old detailed
    tracker.log_activity("clink", json.dumps({"model": "gpt-4o-mini-latest"}))
    assert tracker.model_name == "gpt-4o-mini-latest"

if __name__ == "__main__":
    import pytest
    pytest.main([__file__])
