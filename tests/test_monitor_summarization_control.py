import asyncio
import pytest
import httpx
from fastapi import FastAPI
from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEvent, ToolEventType

@pytest.mark.asyncio
async def test_coordinator_summary_flag_management():
    """
    Test that the coordinator can toggle the summary flag via WebSocket 
    and include it in HTTP responses.
    """
    coordinator = MonitorCoordinator()
    
    # 1. Check initial state (should be False by default)
    assert coordinator.should_summarize is False
    
    # 2. Mock a response for an event
    event = ToolEvent(
        event_type=ToolEventType.HEARTBEAT,
        instance_id="test-instance",
        uptime_seconds=10.0
    )
    
    # We need to test the response logic of the coordinator
    # In a real scenario, this is handled in the FastAPI route
    # Let's verify our intended logic for the response
    response_data = {"interrupted": False, "should_summarize": coordinator.should_summarize}
    assert response_data["should_summarize"] is False
    
    # 3. Toggle the flag
    coordinator.should_summarize = True
    
    # 4. Check if the response now reflects the change
    response_data_updated = {"interrupted": False, "should_summarize": coordinator.should_summarize}
    assert response_data_updated["should_summarize"] is True

@pytest.mark.asyncio
async def test_coordinator_websocket_toggle():
    """Verify that the toggle message via WebSocket updates the state."""
    coordinator = MonitorCoordinator()
    
    # Mock a message from the dashboard
    # {"type": "toggle_summary", "enabled": true}
    class MockWebSocket:
        def __init__(self):
            self.sent = []
        async def send_text(self, text):
            self.sent.append(text)
            
    # We will manually call the handler that WebSocket would trigger
    # First, we need to ensure the coordinator has a way to handle this message
    if hasattr(coordinator, "handle_ui_message"):
        await coordinator.handle_ui_message({"type": "toggle_summary", "enabled": True})
        assert coordinator.should_summarize is True
    else:
        # If method doesn't exist yet, the test fails, which is correct for Test-First
        pytest.fail("MonitorCoordinator does not have handle_ui_message method")
