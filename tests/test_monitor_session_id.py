import asyncio
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from tools.clink import CLinkTool

@pytest.mark.asyncio
async def test_clink_sends_consistent_thread_id_to_monitor():
    """Verify that clink sends session IDs with 'thread:' prefix to the monitor consistently."""
    mock_publisher = AsyncMock()
    mock_publisher.instance_id = "test-instance"
    
    tool = CLinkTool()
    tool._registry = MagicMock()
    mock_client = MagicMock()
    mock_client.name = "gemini"
    tool._registry.get_client.return_value = mock_client
    
    mock_role = MagicMock()
    mock_client.get_role.return_value = mock_role
    mock_role.prompt_path.read_text.return_value = "system prompt"
    
    mock_agent = AsyncMock()
    mock_agent.run.return_value = MagicMock(
        parsed=MagicMock(content="Hello", thinking="", metadata={}),
        sanitized_command=[], returncode=0, stdout="", stderr="", duration_seconds=1.0
    )

    with patch('tools.clink.create_agent', return_value=mock_agent), \
         patch('tools.clink.get_publisher', return_value=mock_publisher), \
         patch('utils.conversation_memory.get_thread', return_value=MagicMock()), \
         patch('utils.conversation_memory.update_current_turn'):
        
        tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
        
        # Scenario 1: continuation_id WITHOUT prefix
        arguments = {
            "prompt": "test",
            "cli_name": "gemini",
            "continuation_id": "uuid-123",
            "_request_context": MagicMock()
        }
        
        await tool.execute(arguments)
        
        # Verify tool_start was called with thread:uuid-123
        start_call_args = mock_publisher.tool_start.call_args[0]
        monitor_args = start_call_args[1]
        assert monitor_args["continuation_id"] == "thread:uuid-123"
        
        # Scenario 2: notification callback uses the same consistent ID
        # We need to extract the callback from agent.run call
        callback = mock_agent.run.call_args[1]['output_callback']
        await callback('{"type":"message", "content":"thinking", "delta":true}')
        
        # Verify tool_log used the correct session_id
        log_call_args = mock_publisher.tool_log.call_args[1]
        assert log_call_args["session_id"] == "thread:uuid-123"

@pytest.mark.asyncio
async def test_publisher_tool_end_accepts_tool_output_keyword():
    """Verify that MonitorPublisher.tool_end correctly handles the tool_output keyword argument."""
    from monitor.publisher import MonitorPublisher
    
    # We use a real publisher instance but disable actual sending to avoid network issues
    publisher = MonitorPublisher(enabled=True)
    publisher._publish_event = AsyncMock() # Prevent actual queueing
    
    # This call should NOT raise TypeError: got an unexpected keyword argument 'tool_output'
    await publisher.tool_start("test_tool", arguments={"a": 1}, session_id="thread:123")
    await publisher.tool_end(
        "test_tool", 
        duration_ms=100, 
        tool_output='{"status": "success"}', 
        session_id="thread:123"
    )
    
    # Check that it was called correctly
    assert publisher._publish_event.called
    event = publisher._publish_event.call_args[0][0]
    # Note: MonitorPublisher serializes tool_output to JSON string if it's not None
    assert "success" in event.tool_output
    assert event.session_id == "thread:123"

