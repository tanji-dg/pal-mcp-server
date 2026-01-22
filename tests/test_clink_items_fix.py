
import asyncio
import pytest
import json
from unittest.mock import MagicMock, patch
from tools.clink import CLinkTool
from mcp.types import TextContent

@pytest.mark.asyncio
async def test_clink_execute_with_string_arguments():
    """Regression test: verify clink handles cases where arguments is accidentally a string."""
    tool = CLinkTool()
    
    # Simulate the case where 'arguments' is a JSON string instead of a dict
    # (Sometimes happens due to nested tool calls or mis-serialization)
    bad_arguments = json.dumps({
        "prompt": "test",
        "cli_name": "claude",
        "role": "codereviewer"
    })
    
    # This should NOT raise 'str' object has no attribute 'items'
    # It might raise other errors (like validation errors), but we want to catch the 'items' one.
    try:
        await tool.execute(bad_arguments)
    except AttributeError as e:
        if "'str' object has no attribute 'items'" in str(e) or "attribute 'keys'" in str(e):
            pytest.fail(f"Caught the 'str' object attribute error: {e}")
    except Exception as e:
        print(f"Caught other expected exception: {type(e).__name__}: {e}")
        pass

@pytest.mark.asyncio
async def test_clink_execute_with_proper_dict():
    """Sanity check: proper dict should work."""
    tool = CLinkTool()
    # Mock internal registry to avoid real lookups
    tool._registry = MagicMock()
    
    arguments = {
        "prompt": "test",
        "cli_name": "claude",
        "_request_context": MagicMock()
    }
    
    # We just want to see if it reaches the point of using .items()
    # without crashing on the type check.
    with patch.object(tool, 'get_request_model', return_value=MagicMock()):
        try:
            await tool.execute(arguments)
        except Exception as e:
            assert "'str' object has no attribute" not in str(e)
