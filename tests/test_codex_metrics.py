
import json
import time
import pytest
from datetime import datetime, timezone, timedelta
from monitor.coordinator import InstanceTracker, MonitorCoordinator
from monitor.models import ToolEventType, ToolEvent

class TestCodexMetrics:
    @pytest.mark.asyncio
    async def test_codex_thinking_and_execution_metrics(self):
        """
        Verify that Codex reasoning and command execution durations are correctly
        accumulated in thinking_ms and execution_ms.
        """
        coordinator = MonitorCoordinator()
        instance_id = "test_codex_instance"
        
        # 1. Register
        await coordinator.process_event({
            "event_type": ToolEventType.REGISTER,
            "instance_id": instance_id,
            "uptime_seconds": 0.0
        })
        
        # 2. Start tool
        t0 = datetime(2026, 1, 27, 12, 0, 0, tzinfo=timezone.utc)
        await coordinator.process_event(ToolEvent(
            event_type=ToolEventType.TOOL_START,
            instance_id=instance_id,
            tool_name="clink",
            timestamp=t0
        ))
        
        tracker = coordinator.instances[instance_id]
        
        # 3. Start Reasoning (Thinking)
        # This sets the state to "Thinking"
        t1 = t0 + timedelta(seconds=0) # +0s
        tracker._last_activity_time = t1.timestamp()
        
        log_reasoning = {
            "type": "item.started", 
            "item": {"id": "r1", "type": "reasoning"}
        }
        await coordinator.process_event(ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=instance_id,
            tool_name="clink",
            timestamp=t1,
            log_data=json.dumps(log_reasoning)
        ))
        
        # 4. Start Execution (Command) - 1s later
        # This transition should capture the 1s thinking time
        
        # Manually advance time to simulate 1s elapsed
        # In a real run, this happens naturally. Here we set timestamps.
        current_time = t1.timestamp() + 1.0
        
        # We need to simulate the coordinator receiving this event 1s later
        # The coordinator uses the event timestamp to calculate delta
        t_exec_start = datetime.fromtimestamp(current_time, tz=timezone.utc)
        
        log_exec_start = {
            "type": "item.started", 
            "item": {"id": "c1", "type": "command_execution", "command": "ls -la"}
        }
        await coordinator.process_event(ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=instance_id,
            tool_name="clink",
            timestamp=t_exec_start,
            log_data=json.dumps(log_exec_start)
        ))
        
        # Check thinking time: should be 1000ms
        print(f"Thinking MS: {tracker.thinking_ms}")
        assert tracker.thinking_ms == 1000
        
        # 5. Complete Execution - 1s later
        t_exec_end = datetime.fromtimestamp(current_time + 1.0, tz=timezone.utc)
        
        log_exec_end = {
            "type": "item.completed",
            "item": {"id": "c1", "type": "command_execution", "status": "success", "command": "ls -la"}
        }
        await coordinator.process_event(ToolEvent(
            event_type=ToolEventType.TOOL_LOG,
            instance_id=instance_id,
            tool_name="clink",
            timestamp=t_exec_end,
            log_data=json.dumps(log_exec_end)
        ))
        
        # Check execution time: should be 1000ms
        print(f"Execution MS: {tracker.execution_ms}")
        assert tracker.execution_ms == 1000
