import pytest
from unittest.mock import MagicMock, AsyncMock, patch
import logging
import sys

# Setup logging
logging.basicConfig(level=logging.DEBUG)

@pytest.mark.asyncio
async def test_clink_monitor_integration():
    """
    Verify that executing the clink tool via handle_call_tool results in
    the correct sequence of monitor events: start -> log -> end.
    """
    
    # Mock the publisher
    mock_publisher = AsyncMock()
    
    # Mock get_publisher to return our mock
    # We need to patch where it is IMPORTED in server.py (inside the function if local import)
    # But server.py imports it inside the 'if MONITOR_ENABLED' block usually?
    # Actually server.py does 'from monitor.publisher import get_publisher' inside the if block.
    
    # So we patch monitor.publisher.get_publisher
    with patch("monitor.publisher.get_publisher", return_value=mock_publisher):
        
        # Import server and force enable monitor
        import server
        server.MONITOR_ENABLED = True
        
        # Mock the CLinkTool
        from tools import CLinkTool
        from mcp.types import TextContent
        
        # Create a real CLinkTool instance but mock its execute method
        clink_tool = CLinkTool()
        
        async def mock_execute(arguments):
            print("DEBUG: Inside mock_execute")
            # Simulate clink sending a log
            # Note: clink.py imports get_publisher separately, so we rely on the patch above working globally
            pub = server.get_publisher() if hasattr(server, 'get_publisher') else mock_publisher
            await pub.tool_log("clink", "Simulated live log")
            return [TextContent(type="text", text='{"status": "success"}')]
            
        clink_tool.execute = mock_execute
        
        # Patch the TOOLS registry in server.py
        with patch.dict(server.TOOLS, {"clink": clink_tool}):
            
            # Execute handle_call_tool
            arguments = {"prompt": "test prompt"}
            print("DEBUG: Calling handle_call_tool")
            await server.handle_call_tool("clink", arguments)
            print("DEBUG: Returned from handle_call_tool")
            
            # Verify interactions
            
            # 1. tool_start should be called
            if mock_publisher.tool_start.call_count == 0:
                print("FAILURE: tool_start not called")
            else:
                args, _ = mock_publisher.tool_start.call_args
                assert args[0] == "clink"
                print("Verified: tool_start called")
            
            # 2. tool_log should be called
            if mock_publisher.tool_log.call_count == 0:
                print("FAILURE: tool_log not called")
            else:
                args, _ = mock_publisher.tool_log.call_args
                assert args[1] == "Simulated live log"
                print("Verified: tool_log called")
            
            # 3. tool_end should be called
            if mock_publisher.tool_end.call_count == 0:
                print("FAILURE: tool_end not called")
            else:
                args, _ = mock_publisher.tool_end.call_args
                assert args[0] == "clink"
                print("Verified: tool_end called")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_clink_monitor_integration())