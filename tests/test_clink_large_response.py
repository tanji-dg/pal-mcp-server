
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from pathlib import Path

from tools.clink import CLinkTool, CLinkRequest, MAX_RESPONSE_CHARS
from clink.agents.base import AgentOutput
from clink.parsers.base import ParsedCLIResponse

@pytest.mark.asyncio
async def test_large_response_with_loop_detection_offloading():
    """
    Verify that a huge response containing 'Loop detected' is offloaded to a file
    but the warning message remains visible in the immediate MCP response.
    """
    tool = CLinkTool()
    
    # 1. Prepare huge content (larger than MAX_RESPONSE_CHARS)
    huge_content = "A" * (MAX_RESPONSE_CHARS + 5000)
    huge_thinking = "Thinking about loops... Loop detected here!"
    # Use keywords that the tool's callback recognizes for accumulation
    huge_logs = ["Executing tool: Log entry 1", "Executing tool: Log entry 2", "Executing tool: Loop detected in logs"]
    
    # 2. Mock AgentOutput and ParsedCLIResponse
    mock_parsed = ParsedCLIResponse(
        content=huge_content,
        thinking=huge_thinking,
        metadata={"is_error": True} 
    )
    
    mock_result = AgentOutput(
        parsed=mock_parsed,
        sanitized_command=["test", "cmd"],
        returncode=0,
        stdout="huge stdout",
        stderr="",
        duration_seconds=1.0,
        parser_name="gemini_json"
    )
    
    # 3. Mock dependencies
    async def mock_run_with_callback(*args, **kwargs):
        callback = kwargs.get("output_callback")
        if callback:
            # Simulate thinking accumulation via Gemini-style JSON message
            await callback(json.dumps({
                "type": "message",
                "role": "assistant",
                "content": huge_thinking,
                "delta": True
            }))
            # Simulate log accumulation via recognized text keywords
            for log in huge_logs:
                await callback(log)
        return mock_result

    mock_agent = AsyncMock()
    mock_agent.run.side_effect = mock_run_with_callback
    mock_agent._parser = MagicMock()
    
    mock_registry = MagicMock()
    mock_client = MagicMock()
    mock_client.name = "gemini"
    mock_registry.get_client.return_value = mock_client
    
    # 4. Execute tool
    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        from tools.shared.exceptions import ToolExecutionError
        with pytest.raises(ToolExecutionError) as excinfo:
            await tool.execute({
                "prompt": "test",
                "cli_name": "gemini",
                "_request_context": {"session_id": "test-session"}
            })
        
        # 5. Verify the error payload
        error_json = json.loads(str(excinfo.value))
        content = error_json["content"]
        
        # Check for critical warnings
        assert "⚠️ **CRITICAL: Loop detected, stopping execution.**" in content
        assert "[MANDATORY] CLI 'gemini' produced a huge response" in content
        
        # Check metadata
        metadata = error_json["metadata"]
        assert metadata["output_offloaded"] is True
        assert metadata["loop_detected"] is True
        
        # Verify the file actually exists and contains everything
        offloaded_path = Path(metadata["output_file_path"])
        assert offloaded_path.exists()
        file_content = offloaded_path.read_text()
        assert huge_thinking in file_content
        assert "Loop detected in logs" in file_content
        assert huge_content in file_content
        
        # Cleanup
        offloaded_path.unlink()

@pytest.mark.asyncio
async def test_large_success_response_offloading():
    """
    Verify that a huge SUCCESS response is also offloaded correctly.
    """
    tool = CLinkTool()
    huge_content = "SUCCESS CONTENT " * 2000 # ~30,000 chars
    
    mock_parsed = ParsedCLIResponse(
        content=huge_content,
        metadata={"is_error": False}
    )
    mock_result = AgentOutput(
        parsed=mock_parsed,
        sanitized_command=["test"],
        returncode=0,
        stdout="",
        stderr="",
        duration_seconds=0.5,
        parser_name="gemini_json"
    )
    
    mock_agent = AsyncMock()
    mock_agent.run.return_value = mock_result
    
    mock_registry = MagicMock()
    mock_registry.get_client.return_value.name = "gemini"
    
    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        result = await tool.execute({"prompt": "test"})
        payload = json.loads(result[0].text)
        
        assert payload["metadata"]["output_offloaded"] is True
        assert "huge response" in payload["content"]
        
        # Cleanup
        Path(payload["metadata"]["output_file_path"]).unlink()
