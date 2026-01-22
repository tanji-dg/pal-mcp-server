"""
Tests for the task interruption (cancellation) flow.
"""

import asyncio
import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch

from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEvent, ToolEventType, EventResponse
from monitor.publisher import MonitorPublisher
from clink.agents.base import BaseCLIAgent
from clink.models import ResolvedCLIClient, ResolvedCLIRole

class TestInterruptionFlow:

    @pytest.mark.asyncio
    async def test_coordinator_signals_interruption(self):
        """Verify Coordinator returns interrupted=True in response after an interrupt request."""
        coordinator = MonitorCoordinator()
        instance_id = "test_pid@host"
        
        # 1. Register instance (using dict to avoid pydantic attribute issues in test)
        await coordinator.process_event({
            "event_type": ToolEventType.REGISTER,
            "instance_id": instance_id,
            "uptime_seconds": 10.0
        })
        
        # 2. Initially, it should not be interrupted
        response = await coordinator.process_event({
            "event_type": ToolEventType.HEARTBEAT,
            "instance_id": instance_id,
            "uptime_seconds": 11.0
        })
        assert isinstance(response, EventResponse)
        assert response.interrupted is False
        
        # 3. Trigger interrupt
        await coordinator.interrupt_instance(instance_id)
        
        # 4. Next event should receive interruption signal
        response = await coordinator.process_event({
            "event_type": ToolEventType.TOOL_LOG,
            "instance_id": instance_id,
            "tool_name": "clink",
            "log_data": "still working...",
            "uptime_seconds": 12.0
        })
        
        assert response.interrupted is True

    @pytest.mark.asyncio
    async def test_publisher_updates_interrupted_state(self):
        """Verify Publisher updates its internal flag based on Coordinator response."""
        publisher = MonitorPublisher(enabled=True)
        # Mock the client to return an interrupted response
        mock_response = MagicMock()
        mock_response.status_code = 200
        # Simulate EventResponse JSON
        mock_response.json.return_value = {"status": "ok", "interrupted": True}
        
        publisher._client = AsyncMock()
        publisher._client.post.return_value = mock_response
        
        # Send an event directly (which calls _send_event_directly)
        event = ToolEvent(
            event_type=ToolEventType.HEARTBEAT, 
            instance_id=publisher.instance_id,
            uptime_seconds=1.0
        )
        
        await publisher._send_event_directly(event)
        
        assert publisher.is_interrupted() is True

    @pytest.mark.asyncio
    async def test_cli_agent_aborts_on_interruption(self):
        """Verify BaseCLIAgent kills the process when interruption flag is detected."""
        mock_client = MagicMock(spec=ResolvedCLIClient)
        mock_client.name = "test-cli"
        mock_client.parser = "gemini_json"
        mock_client.working_dir = None
        mock_client.output_to_file = None
        mock_client.default_total_timeout_seconds = 30
        mock_client.default_idle_timeout_seconds = 10
        mock_client.env = {}
        mock_client.executable = ["echo"]
        mock_client.internal_args = []
        mock_client.config_args = []
        
        mock_role = MagicMock(spec=ResolvedCLIRole)
        mock_role.name = "default"
        mock_role.command = ["sleep", "10"]
        mock_role.role_args = []
        mock_role.total_timeout_seconds = None
        mock_role.idle_timeout_seconds = None
        
        agent = BaseCLIAgent(mock_client)
        
        # Mock publisher
        mock_publisher = MagicMock(spec=MonitorPublisher)
        # Interrupt immediately
        mock_publisher.is_interrupted.return_value = True
        
        # Patch get_publisher in clink.agents.base
        with patch('clink.agents.base.get_publisher', return_value=mock_publisher):
            # mock_process setup
            mock_process = AsyncMock() 
            mock_process.kill = MagicMock()
            mock_process.returncode = None
            mock_process.pid = 1234
            mock_process.stdin = AsyncMock()
            mock_process.stdout = AsyncMock()
            mock_process.stderr = AsyncMock()
            
            # Streams
            mock_process.stdout.readline = AsyncMock(return_value=b"working...\n")
            mock_process.stderr.readline = AsyncMock(return_value=b"")
            
            # communicate() and wait() should be awaitable
            mock_process.communicate = AsyncMock(return_value=(b"", b""))
            mock_process.wait = AsyncMock(return_value=0)
            
            with patch('asyncio.create_subprocess_exec', return_value=mock_process):
                # The agent loop should check publisher.is_interrupted() and call process.kill()
                from clink.agents.base import CLIAgentError
                with pytest.raises(CLIAgentError) as excinfo:
                    await agent.run(role=mock_role, prompt="test", files=[], images=[])
                
                assert "interrupted" in str(excinfo.value).lower()
                mock_process.kill.assert_called()

    @pytest.mark.asyncio
    async def test_coordinator_handles_interrupted_json_output(self):
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
    async def test_clink_tool_sends_interruption_log(self):
        """Verify CLinkTool sends an early warning log to monitor when interrupted."""
        from tools.clink import CLinkTool, CLinkRequest
        from clink.agents.base import CLIAgentError
        
        tool = CLinkTool()
        mock_client = MagicMock(spec=ResolvedCLIClient)
        mock_client.name = "gemini"
        mock_client.parser = "gemini_json"
        tool._resolved_clients = {"gemini": mock_client}
        
        mock_role = MagicMock(spec=ResolvedCLIRole)
        mock_role.name = "default"
        # Properly mock Path object for prompt_path
        mock_path = MagicMock(spec=Path)
        mock_path.read_text.return_value = "system prompt content"
        mock_role.prompt_path = mock_path
        
        tool._resolved_roles = {"gemini": {"default": mock_role}}
        
        request = CLinkRequest(prompt="test", cli_name="gemini")
        
        # Mock publisher
        mock_publisher = AsyncMock(spec=MonitorPublisher)
        mock_publisher.instance_id = "test-instance"
        
        # Mock agent that raises CLIAgentError (interrupted)
        mock_agent = AsyncMock()
        exc = CLIAgentError("Task interrupted by user", returncode=-9, stdout="Partial output", stderr="")
        mock_agent.run.side_effect = exc
        
        with patch('tools.clink.create_agent', return_value=mock_agent), \
             patch('tools.clink.get_publisher', return_value=mock_publisher), \
             patch('utils.conversation_memory.create_thread', return_value="thread-123"), \
             patch('utils.conversation_memory.add_turn'), \
             patch('utils.conversation_memory.update_current_turn'):
            
            # Mock handle_prompt_file_with_fallback
            tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
            
            # Use standalone arguments to avoid context issues in simple test
            arguments = {
                "prompt": "test",
                "cli_name": "gemini",
                "_request_context": MagicMock() # Trigger notification path
            }
            
            await tool.execute(arguments)
            
            # Verify that tool_log was called with interruption message
            found_interruption_log = False
            for call in mock_publisher.tool_log.call_args_list:
                if "interrupted" in call.args[1].lower():
                    found_interruption_log = True
                    break
            
            assert found_interruption_log is True, "Interruption log message was not sent to monitor"
