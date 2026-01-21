"""Unit tests for error/interrupt context preservation in ClinkTool."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from clink.agents import CLIAgentError
from clink.models import ResolvedCLIClient
from tools.clink import CLinkTool
from clink.parsers import ParsedCLIResponse

@pytest.fixture
def mock_registry():
    with patch("tools.clink.get_registry") as mock:
        registry = MagicMock()
        mock.return_value = registry
        
        # Setup mock client config
        client_config = MagicMock(spec=ResolvedCLIClient)
        client_config.name = "mock-cli"
        client_config.runner = "mock-runner"  # Fix: mock the runner attribute
        registry.get_client.return_value = client_config
        registry.list_clients.return_value = ["mock-cli"]
        
        yield registry

@pytest.fixture
def mock_agent():
    with patch("tools.clink.create_agent") as mock_create:
        agent = AsyncMock()
        # Explicitly set _parser to a MagicMock to handle synchronous calls correctly
        # and prevent AsyncMock from interfering with property access
        agent._parser = MagicMock()
        mock_create.return_value = agent
        yield agent

@pytest.fixture
def tool(mock_registry):
    return CLinkTool()

@pytest.mark.asyncio
async def test_clink_preserves_context_on_interrupt(tool, mock_agent):
    """Verify thinking content is saved when interrupted."""
    
    # Simulate an interruption error with partial output
    partial_stdout = '{"type":"message","thought":"I am thinking..."}\n'
    # Ensure "interrupted" is in the exception message so the handler catches it
    error = CLIAgentError("Task interrupted by user", stdout=partial_stdout, stderr="")
    mock_agent.run.side_effect = error
    
    # Mock the parser to return thinking content
    mock_agent._parser.parse.return_value = ParsedCLIResponse(
        content="Partial content",
        metadata={"model_used": "mock-model"},
        thinking="I am thinking..."
    )
    
    # Mock prompt preparation to avoid string join errors with mocks
    with patch.object(tool, "_prepare_prompt_for_role", return_value="mock prompt"):
        # Mock DB recording
        with patch.object(tool, "_record_assistant_turn") as mock_record:
            result = await tool.execute({
                "prompt": "test", 
                "cli_name": "mock-cli",
                "continuation_id": "test-session"
            })
            
            # Verify result is success (graceful interrupt)
            assert len(result) == 1
            assert "Task Interrupted" in result[0].text
            assert "I am thinking..." in result[0].text
            
            # Verify it was saved to history
            mock_record.assert_called()
            saved_content = mock_record.call_args[0][1]
            assert "<thinking>I am thinking...</thinking>" in saved_content
            assert "Task Interrupted" in saved_content

@pytest.mark.asyncio
async def test_clink_preserves_context_on_error(tool, mock_agent):
    """Verify thinking content is saved when a general error occurs."""
    
    # Simulate a crash error with partial output
    partial_stdout = '{"type":"message","thought":"Processing data..."}\n'
    error = CLIAgentError("Process crashed", returncode=1, stdout=partial_stdout, stderr="Fatal error")
    mock_agent.run.side_effect = error
    
    # Mock the parser
    mock_agent._parser.parse.return_value = ParsedCLIResponse(
        content="Partial work",
        metadata={"model_used": "mock-model"},
        thinking="Processing data..."
    )
    
    # Mock prompt preparation
    with patch.object(tool, "_prepare_prompt_for_role", return_value="mock prompt"):
        # Mock DB recording
        with patch.object(tool, "_record_assistant_turn") as mock_record:
            # Expect the tool to re-raise the error for the UI
            from tools.shared.exceptions import ToolExecutionError
            with pytest.raises(ToolExecutionError):
                await tool.execute({
                    "prompt": "test", 
                    "cli_name": "mock-cli",
                    "continuation_id": "test-session"
                })
            
        # Verify it was saved to history despite the error raise
        # Note: _record_assistant_turn might be called multiple times (salvaged content, then error msg)
        # We need to find the call with the salvaged content
        found_salvaged = False
        for call in mock_record.call_args_list:
            args = call[0]
            if len(args) > 1 and "<thinking>Processing data...</thinking>" in args[1]:
                content = args[1]
                assert "Task Failed" in content
                assert "Error Details" in content
                found_salvaged = True
                break
        
        assert found_salvaged, "Salvaged content was not recorded to history"
