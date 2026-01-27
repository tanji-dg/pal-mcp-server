"""
Regression test for capturing raw HTTP error dumps in CLinkTool.
"""

import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from tools.clink import CLinkTool
from clink.agents.base import CLIAgentError

@pytest.mark.asyncio
async def test_clink_captures_raw_429_error_dump():
    """Verify CLinkTool captures raw HTTP 429 error dumps and formats them for the agent."""
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
    # Simulate a raw Gaxios error dump in stdout, then a process failure
    raw_error_dump = [
        "headers: {",
        "  status: 429,",
        "  statusText: 'Too Many Requests'",
        "}"
    ]
    
    exc = CLIAgentError("Process failed with 429", returncode=1, stdout="\n".join(raw_error_dump), stderr="")
    
    async def run_with_error_dump(*args, **kwargs):
        callback = kwargs.get('output_callback')
        if callback:
            # Send raw lines one by one
            for line in raw_error_dump:
                await callback(line)
        raise exc

    mock_agent.run.side_effect = run_with_error_dump

    with patch('tools.clink.create_agent', return_value=mock_agent), \
         patch('tools.clink.get_publisher', return_value=mock_publisher), \
         patch('utils.conversation_memory.get_thread', return_value=MagicMock()), \
         patch('utils.conversation_memory.add_turn'), \
         patch('utils.conversation_memory.update_current_turn'):
        
        tool.handle_prompt_file_with_fallback = MagicMock(return_value="user prompt")
        
        arguments = {
            "prompt": "test",
            "cli_name": "gemini",
            "continuation_id": "thread-429",
            "_request_context": MagicMock()
        }
        
        # Execute tool - it should salvage the raw error dump
        result = await tool.execute(arguments)
        
        # Verify result contains the warning and raw lines
        content_json = result[0].text
        data = json.loads(content_json)
        salvaged_text = data["content"]
        
        # 1. Check for the added warning
        assert "⚠️ API Rate Limit Detected (429)" in salvaged_text
        
        # 2. Check that raw lines are preserved in the timeline
        assert "status: 429," in salvaged_text
        assert "statusText: 'Too Many Requests'" in salvaged_text
        assert "### 🔄 Progress Timeline" in salvaged_text
        assert "❌ **Task Failed**" in salvaged_text

if __name__ == "__main__":
    pytest.main([__file__])