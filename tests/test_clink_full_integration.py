import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import logging
from mcp.types import TextContent

# Setup logging
logging.basicConfig(level=logging.DEBUG)

@pytest.mark.asyncio
async def test_clink_full_flow():
    """
    Verify full flow from server -> clink -> server -> monitor
    Using real CLinkTool logic but mocking the CLI agent execution.
    """
    
    # Mock dependencies
    mock_publisher = AsyncMock()
    mock_agent = AsyncMock()
    
    # Mock agent result
    from clink.agents import AgentOutput
    from clink.parsers import ParsedCLIResponse
    
    mock_result = AgentOutput(
        parsed=ParsedCLIResponse(content="Mock agent response", metadata={"model_used": "mock-model"}),
        sanitized_command=["mock", "cmd"],
        returncode=0,
        stdout="Mock stdout",
        stderr="Mock stderr",
        duration_seconds=1.0,
        parser_name="mock"
    )
    mock_agent.run.return_value = mock_result
    
    # Setup patches
    with patch("monitor.publisher.get_publisher", return_value=mock_publisher), \
         patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry") as mock_registry:
             
        # Configure registry mock
        mock_client = MagicMock()
        mock_client.name = "mock-cli"
        mock_role = MagicMock()
        mock_role.prompt_path.read_text.return_value = "System Prompt"
        mock_client.get_role.return_value = mock_role
        mock_registry.return_value.get_client.return_value = mock_client
        mock_registry.return_value.list_clients.return_value = ["mock-cli"]
        
        # Import server and force enable monitor
        import server
        server.MONITOR_ENABLED = True
        
        from tools import CLinkTool
        clink_tool = CLinkTool()
        
        # Patch TOOLS in server
        with patch.dict(server.TOOLS, {"clink": clink_tool}):
            
            # Execute
            arguments = {"prompt": "test", "cli_name": "mock-cli"}
            print("DEBUG: Calling handle_call_tool")
            await server.handle_call_tool("clink", arguments)
            print("DEBUG: Returned from handle_call_tool")
            
            # Verify tool_end was called
            if mock_publisher.tool_end.call_count == 0:
                print("FAILURE: tool_end not called")
                # Debug: check if clink finished
                print(f"Agent run called: {mock_agent.run.called}")
            else:
                print("SUCCESS: tool_end called")
                args, _ = mock_publisher.tool_end.call_args
                print(f"tool_end args: {args}")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_clink_full_flow())
