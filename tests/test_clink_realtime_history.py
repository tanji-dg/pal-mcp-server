"""Unit tests for real-time conversation history updates in ClinkTool."""

import json
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, call
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
        client_config.name = "claude" # Use claude to simplify capture logic
        client_config.runner = "claude"
        registry.get_client.return_value = client_config
        registry.list_clients.return_value = ["claude"]
        
        yield registry

@pytest.fixture
def mock_agent():
    with patch("tools.clink.create_agent") as mock_create:
        agent = AsyncMock()
        agent._parser = MagicMock()
        mock_create.return_value = agent
        yield agent

@pytest.fixture
def tool(mock_registry):
    return CLinkTool()

@pytest.mark.asyncio
async def test_clink_updates_history_in_realtime(tool, mock_agent):
    """Verify that history is updated periodically during execution."""
    
    # 1. Setup mock agent behavior
    async def side_effect(*args, **kwargs):
        callback = kwargs.get("output_callback")
        if callback:
            # Send thinking (Claude format)
            await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"I am thinking part 1"}}}')
            # Send tool use
            await callback('{"type":"tool_use","tool_name":"Read","tool_id":"t1"}')
            
        return MagicMock(
            parsed=ParsedCLIResponse(
                content="Final Answer",
                metadata={"model_used": "mock-model"},
                thinking="Final Thinking"
            ),
            sanitized_command=["mock"],
            returncode=0,
            stdout="final",
            stderr="",
            duration_seconds=1.0,
            parser_name="mock",
            output_file_content=None
        )
    
    mock_agent.run.side_effect = side_effect
    
    # 2. Patch memory functions
    with patch("utils.conversation_memory.add_turn") as mock_add_turn, \
         patch("utils.conversation_memory.update_current_turn") as mock_update_turn, \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "_prepare_prompt_for_role", return_value="mock prompt"):
        
        # We need a context to trigger notifications
        mock_context = MagicMock()
        mock_context.session = AsyncMock()
        
        await tool.execute({
            "prompt": "test", 
            "cli_name": "claude",
            "continuation_id": "test-session",
            "_request_context": mock_context
        })
        
        # Verify update_current_turn was called during execution
        # Check if any call has our thinking part
        found_thinking = any("I am thinking part 1" in call[0][1] for call in mock_update_turn.call_args_list)
        assert found_thinking, "Real-time update with thinking content was not triggered"

@pytest.mark.asyncio
async def test_clink_throttles_updates(tool, mock_agent):
    """Verify that thinking updates are throttled by time."""
    
    # Use a mutable object to track time so we can advance it
    time_state = {"now": 100.0}
    
    def get_time():
        return time_state["now"]
    
    async def side_effect(*args, **kwargs):
        callback = kwargs.get("output_callback")
        if callback:
            # Initial update triggers because 100 - 0 > 5
            await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"Thought 1"}}}')
            
            # Throttled (same time)
            await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"Thought 2"}}}')
            
            # Triggered by time jump
            time_state["now"] += 10.0
            await callback('{"type":"stream_event","event":{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"Thought 3"}}}')
            
        return MagicMock(
            parsed=ParsedCLIResponse(content="ok", metadata={}),
            duration_seconds=1.0, returncode=0, stdout="", stderr="", parser_name="mock"
        )
    
    mock_agent.run.side_effect = side_effect
    
    with patch("utils.conversation_memory.add_turn"), \
         patch("utils.conversation_memory.update_current_turn") as mock_update_turn, \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "_prepare_prompt_for_role", return_value="mock prompt"), \
         patch("time.monotonic", side_effect=get_time):
        
        mock_context = MagicMock()
        mock_context.session = AsyncMock()
        
        await tool.execute({
            "prompt": "test", 
            "cli_name": "claude",
            "continuation_id": "test-session",
            "_request_context": mock_context
        })
        
        # Check calls
        calls = [c[0][1] for c in mock_update_turn.call_args_list]
        
        found_initial = any("Thought 1" in c and "Thought 3" not in c for c in calls)
        found_final = any("Thought 3" in c for c in calls)
        
        assert found_initial, "Initial update was missed"
        assert found_final, "Throttled update after time jump was missed"
