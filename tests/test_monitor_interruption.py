"""
Tests for the task interruption (cancellation) flow.
"""

import asyncio
import json
import pytest
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
                with pytest.raises(InterruptedError) as excinfo:
                    await agent.run(role=mock_role, prompt="test", files=[], images=[])
                
                assert "interrupted" in str(excinfo.value).lower()
                mock_process.kill.assert_called()
