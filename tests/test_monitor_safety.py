
import asyncio
import json
import pytest
from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEventType, ToolEvent

class TestMonitorSafety:
    @pytest.mark.asyncio
    async def test_interrupt_emits_log_with_harmful_check(self):
        """Verify that interrupt_instance emits a log and detects harmful commands."""
        coordinator = MonitorCoordinator()
        instance_id = "test_safety@host"
        
        # 1. Register and start a harmful tool
        await coordinator.process_event({
            "event_type": ToolEventType.REGISTER,
            "instance_id": instance_id,
        })
        
        # Simulate a harmful command
        harmful_cmd = "rm -rf /"
        await coordinator.process_event({
            "event_type": ToolEventType.TOOL_START,
            "instance_id": instance_id,
            "tool_name": "Bash",
            "tool_input": json.dumps({"command": harmful_cmd})
        })
        
        # 2. Mock broadcast_log to capture the emitted event
        coordinator.broadcast_log = AsyncMock()
        
        # 3. Trigger interrupt
        success = await coordinator.interrupt_instance(instance_id)
        assert success is True
        
        # 4. Verify the broadcasted log event
        assert coordinator.broadcast_log.called
        event = coordinator.broadcast_log.call_args[0][0]
        assert isinstance(event, ToolEvent)
        assert event.event_type == ToolEventType.TOOL_LOG
        
        log_data = json.loads(event.log_data)
        assert log_data["is_user_interruption"] is True
        assert log_data["is_harmful"] is True
        assert "rm " in log_data["harmful_reason"]
        assert "POTENTIALLY HARMFUL" in log_data["message"]

    @pytest.mark.asyncio
    async def test_interrupt_emits_log_for_safe_command(self):
        """Verify that interrupt_instance emits a log for safe commands without harmful flag."""
        coordinator = MonitorCoordinator()
        instance_id = "test_safe@host"
        
        await coordinator.process_event({
            "event_type": ToolEventType.REGISTER,
            "instance_id": instance_id,
        })
        
        # Simulate a safe command
        safe_cmd = "ls -la"
        await coordinator.process_event({
            "event_type": ToolEventType.TOOL_START,
            "instance_id": instance_id,
            "tool_name": "Bash",
            "tool_input": json.dumps({"command": safe_cmd})
        })
        
        coordinator.broadcast_log = AsyncMock()
        await coordinator.interrupt_instance(instance_id)
        
        assert coordinator.broadcast_log.called
        event = coordinator.broadcast_log.call_args[0][0]
        log_data = json.loads(event.log_data)
        
        assert log_data["is_user_interruption"] is True
        assert log_data["is_harmful"] is False
        assert "Interruption signal sent by user" in log_data["message"]

from unittest.mock import AsyncMock
