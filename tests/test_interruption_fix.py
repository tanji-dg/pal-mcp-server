"""
Tests for checking if interruption logs and status are correctly handled.
"""

import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEventType
from tools.clink import CLinkTool
from clink.agents.base import CLIAgentError

@pytest.mark.asyncio
async def test_coordinator_handles_interrupted_json_output():
    """Verify Coordinator detects 'interrupted' status from tool_output JSON."""
    coordinator = MonitorCoordinator()
    instance_id = "test_pid@host"
    
    # 1. Register and start tool
    await coordinator.process_event({
        "event_type": ToolEventType.REGISTER,
        "instance_id": instance_id,
    })
    await coordinator.process_event({
        "event_type": ToolEventType.TOOL_START,
        "instance_id": instance_id,
        "tool_name": "clink",
    })
    
    # 2. End tool with a JSON payload indicating interruption
    # This simulates the salvaging logic outputting a "status: interrupted" JSON
    salvaged_json = json.dumps({
        "status": "success", # Tool finished successfully (salvaged)
        "content": "Progress before stopping...",
        "metadata": {
            "status": "interrupted",
            "partial": True
        }
    })
    
    await coordinator.process_event({
        "event_type": ToolEventType.TOOL_END,
        "instance_id": instance_id,
        "tool_name": "clink",
        "tool_output": salvaged_json,
        "duration_ms": 1000
    })
    
    tracker = coordinator.instances[instance_id]
    
    # Verify that the status in the history record is 'interrupted'
    assert tracker.recent_calls[0].status == "interrupted"
    # Verify that last_status is human-friendly
    assert tracker.last_status == "Interrupted by user"
    # Verify content extraction
    assert tracker.recent_calls[0].tool_output == "Progress before stopping..."

@pytest.mark.asyncio
async def test_clink_tool_sends_interruption_log():
    """Verify CLinkTool sends a log message when interrupted."""
    # We'll use a mock publisher to verify the tool_log call
    mock_publisher = AsyncMock()
    mock_publisher.instance_id = "test-instance"
    
    # Create CLinkTool and mock its internal components
    tool = CLinkTool()
    tool._registry = MagicMock()
    
    # Mock CLI client/role
    mock_client = MagicMock()
    mock_client.name = "gemini"
    tool._registry.get_client.return_value = mock_client
    
    mock_role = MagicMock()
    mock_client.get_role.return_value = mock_role
    
    # Mock agent that raises CLIAgentError (interrupted)
    mock_agent = AsyncMock()
    exc = CLIAgentError("Task interrupted by user", returncode=-9, stdout="Partial output", stderr="")
    mock_agent.run.side_effect = exc
    
    # Mock role.prompt_path.read_text
    mock_role.prompt_path.read_text.return_value = "system prompt content"
    
    with patch('tools.clink.create_agent', return_value=mock_agent), \
         patch('tools.clink.get_publisher', return_value=mock_publisher), \
         patch('utils.conversation_memory.create_thread', return_value="thread-123"), \
         patch('utils.conversation_memory.add_turn'), \
         patch('utils.conversation_memory.update_current_turn'):
        
        # Mock handle_prompt_file_with_fallback
        tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
        arguments = {
            "prompt": "test",
            "cli_name": "gemini",
            "_request_context": MagicMock()
        }
        
        # Execute the tool - it will raise but we've salvaged it inside
        try:
            await tool.execute(arguments)
        except Exception:
            pass # We expect it to be handled or re-raised depending on logic
            
        # Verify tool_log was called with the interruption message
        # In our fix, we added: await publisher.tool_log(self.get_name(), status_msg, ...)
        found_interruption_log = False
        for call in mock_publisher.tool_log.call_args_list:
            if "interrupted" in call.args[1].lower():
                found_interruption_log = True
                break
        
        assert found_interruption_log is True, "Interruption log message was not sent to monitor"
