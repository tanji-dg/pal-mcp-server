import asyncio
import pytest
import json
from unittest.mock import MagicMock, AsyncMock, patch
from monitor.publisher import MonitorPublisher
from monitor.models import ToolEvent, ToolEventType

@pytest.mark.asyncio
async def test_publisher_extracts_summarization_flag():
    """
    Verify that MonitorPublisher updates its internal state based on 
    the coordinator's response to an event.
    """
    publisher = MonitorPublisher(enabled=True)
    
    # Mock the HTTP client
    mock_client = AsyncMock()
    publisher._client = mock_client
    
    # Define a response that includes should_summarize=True
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "ok",
        "interrupted": False,
        "should_summarize": True
    }
    mock_response.raise_for_status = MagicMock()
    
    mock_client.post.return_value = mock_response
    
    # 1. Initially, it should be False (or current state)
    # We need to ensure the method exists
    if not hasattr(publisher, "should_summarize"):
        pytest.fail("MonitorPublisher does not have should_summarize method")
        
    assert publisher.should_summarize() is False
    
    # 2. Send an event
    event = ToolEvent(
        event_type=ToolEventType.HEARTBEAT,
        instance_id="test-instance"
    )
    await publisher._send_event_directly(event)
    
    # 3. After receiving the response, should_summarize() should return True
    assert publisher.should_summarize() is True

@pytest.mark.asyncio
async def test_publisher_polling_updates_flag():
    """Verify that regular polling (check_interruption) also updates the flag."""
    publisher = MonitorPublisher(enabled=True)
    mock_client = AsyncMock()
    publisher._client = mock_client
    
    # Mock response for heartbeat
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "status": "ok",
        "should_summarize": True
    }
    mock_client.post.return_value = mock_response
    
    # Trigger a status poll
    await publisher.check_interruption()
    
    assert publisher.should_summarize() is True
