
import asyncio
import time
import pytest
from unittest.mock import MagicMock

from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEventType

@pytest.mark.asyncio
async def test_thinking_metrics_accumulation():
    """
    Verify that thinking time is correctly accumulated across events,
    especially ensuring the transition from Thinking -> Executing is captured.
    """
    coordinator = MonitorCoordinator()
    instance_id = "test_instance"
    
    # 1. Register instance
    await coordinator.process_event({
        "event_type": ToolEventType.REGISTER,
        "instance_id": instance_id,
        "uptime_seconds": 0.0
    })
    
    tracker = coordinator.instances[instance_id]
    
    # 2. Start Thinking (Event A)
    # Simulate a log indicating thinking started
    start_time = time.time()
    tracker._last_activity_time = start_time # Reset base time for test stability
    
    await coordinator.process_event({
        "event_type": ToolEventType.TOOL_LOG,
        "instance_id": instance_id,
        "tool_name": "clink",
        "log_data": '{"type": "message", "role": "assistant", "content": "Thinking about plan..."}'
    })
    
    assert tracker.last_status == "Thinking"
    initial_thinking = tracker.thinking_ms
    
    # 3. Simulate passage of time (2 seconds) while Thinking
    # We simulate this by advancing the next event's timestamp relative to the first
    # In the coordinator, it uses time.time(), so we'll mock the time or just sleep?
    # Sleeping is flaky. Better to rely on the coordinator's delta calculation.
    # The coordinator logic uses: now_ts = time.time(), delta = now_ts - tracker._last_activity_time
    
    # We will manually adjust _last_activity_time to simulate "2 seconds ago"
    # This tricks the coordinator into thinking 2 seconds passed when processing the next event.
    tracker._last_activity_time = time.time() - 2.0
    
    # 4. Continue Thinking (Event B) - e.g. a stream chunk
    # This should add ~2000ms to thinking_ms
    await coordinator.process_event({
        "event_type": ToolEventType.TOOL_LOG,
        "instance_id": instance_id,
        "tool_name": "clink",
        "log_data": '{"type": "content_block_delta", "delta": {"type": "thinking_delta", "thinking": "Still thinking..."}}'
    })
    
    assert tracker.thinking_ms >= 2000
    mid_thinking = tracker.thinking_ms
    
    # 5. Simulate more time passing (3 seconds) then Tool Execution (Event C)
    # This transition was PREVIOUSLY losing the 3 seconds of thinking time
    tracker._last_activity_time = time.time() - 3.0
    
    await coordinator.process_event({
        "event_type": ToolEventType.TOOL_LOG,
        "instance_id": instance_id,
        "tool_name": "clink",
        "log_data": '{"type": "tool_use", "name": "run_shell_command", "tool_id": "call_1"}'
    })
    
    # 6. Verify Total Thinking Time
    # Should be approx 2s (first gap) + 3s (second gap) = 5s total
    # Previous buggy logic would have stopped at 2s.
    
    assert tracker.last_status != "Thinking" # Should be "Calling run_shell_command"
    
    total_thinking = tracker.thinking_ms
    added_in_final_step = total_thinking - mid_thinking
    
    print(f"Initial thinking: {initial_thinking}ms")
    print(f"Mid thinking: {mid_thinking}ms")
    print(f"Total thinking: {total_thinking}ms")
    
    # Check if the final 3s segment was captured
    assert added_in_final_step >= 3000, f"Failed to capture final thinking segment! Added {added_in_final_step}ms instead of ~3000ms"
    assert total_thinking >= 5000
