"""
Reproduction test for result-type error JSON.
"""

import json
import pytest
from clink.parsers.gemini import GeminiJSONParser
from clink.parsers.claude import ClaudeJSONParser

def test_gemini_repro_error_result():
    parser = GeminiJSONParser()
    # Exact JSON from user
    stdout = '{"type":"result","timestamp":"2026-01-20T11:26:13.055Z","status":"error","error":{"type":"Error","message":"[API Error: You have exhausted your capacity on this model. Your quota will reset after 3h5m30s.]"},"stats":{"total_tokens":0,"input_tokens":0,"output_tokens":0,"cached":0,"input":0,"duration_ms":0,"tool_calls":0}}'
    
    res = parser.parse(stdout, "")
    
    print(f"Gemini Result Content: {res.content}")
    assert "exhausted your capacity" in res.content
    assert res.metadata.get("is_error") is True

def test_claude_repro_error_result():
    parser = ClaudeJSONParser()
    # Exact JSON from user
    stdout = '{"type":"result","timestamp":"2026-01-20T11:26:13.055Z","status":"error","error":{"type":"Error","message":"[API Error: You have exhausted your capacity on this model. Your quota will reset after 3h5m30s.]"},"stats":{"total_tokens":0,"input_tokens":0,"output_tokens":0,"cached":0,"input":0,"duration_ms":0,"tool_calls":0}}'
    
    res = parser.parse(stdout, "")
    
    print(f"Claude Result Content: {res.content}")
    assert "exhausted your capacity" in res.content
    assert res.metadata.get("is_error") is True

@pytest.mark.asyncio
async def test_clink_tool_handles_parsed_error():
    from tools.clink import CLinkTool
    from clink.agents import AgentOutput
    from clink.parsers import ParsedCLIResponse
    from unittest.mock import MagicMock, patch, AsyncMock
    from tools.shared.exceptions import ToolExecutionError

    # Setup CLinkTool with mocked dependencies
    with patch("tools.clink.get_registry") as mock_registry:
        mock_client = MagicMock()
        mock_client.name = "gemini"
        mock_client.runner = "gemini"
        mock_role = MagicMock()
        mock_role.name = "default"
        mock_role.prompt_path.read_text.return_value = "System Prompt"
        mock_client.get_role.return_value = mock_role
        mock_registry.return_value.get_client.return_value = mock_client
        mock_registry.return_value.list_clients.return_value = ["gemini"]
        mock_registry.return_value.list_roles.return_value = ["default"]

        tool = CLinkTool()
        
        # Mock agent to return a SUCCESSFUL execution but with an ERROR payload
        mock_agent = AsyncMock()
        # Parsed result indicating error
        error_json = '{"type":"result","status":"error","error":{"message":"Resource exhausted"}}'
        parsed = ParsedCLIResponse(
            content="Resource exhausted",
            metadata={"is_error": True, "model_used": "test-model"}
        )
        mock_agent.run.return_value = AgentOutput(
            parsed=parsed,
            sanitized_command=["gemini"],
            returncode=0, # Process succeeded!
            stdout=error_json,
            stderr="",
            duration_seconds=1.0,
            parser_name="gemini_json"
        )
        
        with patch("tools.clink.create_agent", return_value=mock_agent):
            args = {
                "prompt": "Test",
                "cli_name": "gemini",
                "continuation_id": "test-thread"
            }
            
            # CLinkTool now returns a soft error instead of raising ToolExecutionError
            result = await tool.execute(args)
            assert isinstance(result, list)
            assert len(result) == 1
            
            output_json = result[0].text
            output_data = json.loads(output_json)
            
            assert output_data["status"] == "error"
            assert "Resource exhausted" in output_data["content"]
            print("CLinkTool correctly returned ToolOutput with error status for parsed error payload.")
