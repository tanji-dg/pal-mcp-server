"""
Tests for checking if thinking process is correctly salvaged during timeouts/errors in CLinkTool.
"""

import json
import pytest
import re
from unittest.mock import MagicMock, AsyncMock, patch

from tools.clink import CLinkTool
from clink.agents.base import CLIAgentError

@pytest.mark.asyncio
async def test_clink_salvages_thinking_on_timeout():
    """Verify CLinkTool preserves thinking content even if CLI times out with empty stdout."""
    mock_publisher = AsyncMock()
    mock_publisher.instance_id = "test-instance"
    
    tool = CLinkTool()
    tool._registry = MagicMock()
    
    # Mock CLI client/role
    mock_client = MagicMock()
    mock_client.name = "claude"
    tool._registry.get_client.return_value = mock_client
    
    mock_role = MagicMock()
    mock_client.get_role.return_value = mock_role
    mock_role.prompt_path.read_text.return_value = "system prompt"
    
    # Mock agent that raises timeout error
    mock_agent = AsyncMock()
    # Simulate a timeout where stdout is EMPTY (this is the critical case)
    exc = CLIAgentError("CLI 'claude' total timed out after 3600 seconds", returncode=1, stdout="", stderr="")
    mock_agent.run.side_effect = exc
    
    # Track calls to record_assistant_turn logic (via update_current_turn)
    recorded_content = []
    def mock_update_current_turn(thread_id, content, **kwargs):
        recorded_content.append(content)

    with patch('tools.clink.create_agent', return_value=mock_agent), \
         patch('tools.clink.get_publisher', return_value=mock_publisher), \
         patch('utils.conversation_memory.get_thread', return_value=MagicMock()), \
         patch('utils.conversation_memory.add_turn'), \
         patch('utils.conversation_memory.update_current_turn', side_effect=mock_update_current_turn):
        
        tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
        
        # Pre-populate some thinking in the internal state via callback
        # We need to access the callback passed to agent.run
        async def run_with_thinking(*args, **kwargs):
            callback = kwargs.get('output_callback')
            if callback:
                # Send some thinking chunks
                await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"I am thinking deeply..."}}}')
                await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":" about JIT cycles."}}}')
            raise exc

        mock_agent.run.side_effect = run_with_thinking

        arguments = {
            "prompt": "test",
            "cli_name": "claude",
            "continuation_id": "thread-123",
            "_request_context": MagicMock()
        }
        
        # Execute tool - it will return ToolOutput with error status (soft error)
        result = await tool.execute(arguments)
        
        # Verify the returned content
        content = result[0].text
        data = json.loads(content)
        assert data["status"] == "error"
        assert "<thinking>I am thinking deeply... about JIT cycles.</thinking>" in data["content"]
        assert "CLI 'claude' total timed out" in data["content"]
            
        # Verify the final recorded turn content contains the thinking
        # The last update_current_turn call should have the salvaged thinking
        assert len(recorded_content) > 0
        final_recorded = recorded_content[-1]
        
        assert "<thinking>I am thinking deeply... about JIT cycles.</thinking>" in final_recorded
        assert "CLI 'claude' total timed out" in final_recorded

@pytest.mark.asyncio
async def test_clink_salvages_thinking_and_logs_on_interruption():
    """Verify CLinkTool preserves both thinking and sub-tool logs during interruption."""
    mock_publisher = AsyncMock()
    
    tool = CLinkTool()
    tool._registry = MagicMock()
    
    mock_client = MagicMock()
    mock_client.name = "gemini"
    tool._registry.get_client.return_value = mock_client
    mock_role = MagicMock()
    mock_client.get_role.return_value = mock_role
    mock_role.prompt_path.read_text.return_value = "system prompt"
    
    mock_agent = AsyncMock()
    exc = CLIAgentError("Task interrupted", returncode=-9, stdout="Some stdout", stderr="")
    
    recorded_content = []
    def mock_update_current_turn(thread_id, content, **kwargs):
        recorded_content.append(content)

    with patch('tools.clink.create_agent', return_value=mock_agent), \
         patch('tools.clink.get_publisher', return_value=mock_publisher), \
         patch('utils.conversation_memory.get_thread', return_value=MagicMock()), \
         patch('utils.conversation_memory.add_turn'), \
         patch('utils.conversation_memory.update_current_turn', side_effect=mock_update_current_turn):
        
        tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
        
        async def run_with_activity(*args, **kwargs):
            callback = kwargs.get('output_callback')
            if callback:
                # 1. Thinking
                await callback('{"type":"message", "role":"assistant", "content":"Thinking chunk", "delta":true}')
                # 2. Tool use
                await callback('{"type":"tool_use", "tool_name":"read_file", "tool_id":"id1"}')
            raise exc

        mock_agent.run.side_effect = run_with_activity

        arguments = {
            "prompt": "test",
            "cli_name": "gemini",
            "continuation_id": "thread-123",
            "_request_context": MagicMock()
        }
        
        # Interruption is caught and returned as success (salvaged)
        result = await tool.execute(arguments)
        
        # Verify result contains thinking and log
        content = result[0].text
        # result is a JSON string of ToolOutput
        data = json.loads(content)
        salvaged_text = data["content"]
        
        assert "<thinking>Thinking chunk</thinking>" in salvaged_text
        assert "### 🔄 Progress Timeline" in salvaged_text
        assert '"type":"tool_use"' in salvaged_text
        assert '"tool_name":"read_file"' in salvaged_text
        assert "⚠️ **Task Interrupted**" in salvaged_text
