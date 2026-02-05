
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

from tools.clink import CLinkTool, MAX_RESPONSE_CHARS
from clink.agents.base import AgentOutput
from clink.parsers.base import ParsedCLIResponse

@pytest.mark.asyncio
async def test_dynamic_limit_by_client_budget():
    """
    Verify that providing a client_context_budget triggers offloading 
    at a lower threshold than the default MAX_RESPONSE_CHARS.
    """
    tool = CLinkTool()
    
    # 1. Content that is SMALLER than default MAX_RESPONSE_CHARS (40,000)
    # but LARGER than a small client budget hint.
    content_size = 15_000
    medium_content = "X" * content_size
    
    # Client says they only have 10,000 chars left.
    # PAL should calculate effective_limit = 10,000 * 0.8 = 8,000.
    # Since 15,000 > 8,000, it should offload.
    client_hint = 10_000
    
    mock_parsed = ParsedCLIResponse(
        content=medium_content,
        metadata={"is_error": False}
    )
    
    mock_result = AgentOutput(
        parsed=mock_parsed,
        sanitized_command=["test"],
        returncode=0,
        stdout="",
        stderr="",
        duration_seconds=0.5,
        parser_name="claude_json"
    )
    
    mock_agent = AsyncMock()
    mock_agent.run.return_value = mock_result
    
    mock_registry = MagicMock()
    mock_client = MagicMock()
    mock_client.name = "claude"
    mock_registry.get_client.return_value = mock_client
    
    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        # Execute tool WITH hint
        result = await tool.execute({
            "prompt": "test",
            "cli_name": "claude",
            "client_context_budget": client_hint
        })
        
        payload = json.loads(result[0].text)
        
        # VERIFICATION: Offloading occurred because of the hint
        assert payload["metadata"]["output_offloaded"] is True
        assert payload["metadata"]["output_limit"] == 8000 # 10,000 * 0.8
        assert "huge response" in payload["content"]
        
        # Cleanup
        Path(payload["metadata"]["output_file_path"]).unlink()

@pytest.mark.asyncio
async def test_dynamic_limit_clamping():
    """
    Verify that client_context_budget is clamped to safe bounds (min 5K, max 200K).
    """
    tool = CLinkTool()
    
    mock_parsed = ParsedCLIResponse(content="short", metadata={})
    mock_result = AgentOutput(parsed=mock_parsed, sanitized_command=["t"], returncode=0, stdout="", stderr="", duration_seconds=0.1, parser_name="t")
    mock_agent = AsyncMock()
    mock_agent.run.return_value = mock_result
    mock_registry = MagicMock()
    mock_registry.get_client.return_value.name = "claude"
    
    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        # Test MIN clamp (1,000 is too small, should clamp to 5,000)
        result = await tool.execute({"prompt": "test", "client_context_budget": 1000})
        payload = json.loads(result[0].text)
        assert payload["metadata"]["output_limit"] == 5000
        
        # Test MAX clamp (1,000,000 is too large, should clamp to 200,000)
        result = await tool.execute({"prompt": "test", "client_context_budget": 1_000_000})
        payload = json.loads(result[0].text)
        assert payload["metadata"]["output_limit"] == 200_000
