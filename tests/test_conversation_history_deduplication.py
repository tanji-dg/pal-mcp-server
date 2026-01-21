"""
Tests for conversation history correctness and avoiding duplicate turns.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from tools.clink import CLinkTool
from tools.chat import ChatTool
from utils.conversation_memory import get_thread, create_thread, add_turn
from utils.storage_backend import get_storage_backend

@pytest.fixture
def storage():
    # Ensure fresh storage for each test
    s = get_storage_backend()
    s.flushall()
    return s

@pytest.mark.asyncio
async def test_clink_no_duplicate_turns(storage):
    """Verify that CLinkTool does not create duplicate user/assistant turns."""
    tool = CLinkTool()
    
    # Mock registry and agent
    mock_reg = MagicMock()
    mock_client = MagicMock()
    mock_client.name = "gemini"
    mock_role = MagicMock()
    mock_role.name = "default"
    mock_role.prompt_path.read_text.return_value = "System prompt"
    mock_role.role_args = []
    mock_client.get_role.return_value = mock_role
    mock_reg.get_client.return_value = mock_client
    
    with patch("tools.clink.get_registry", return_value=mock_reg), \
         patch("tools.clink.create_agent") as mock_create_agent:
        
        mock_agent = AsyncMock()
        mock_parsed = MagicMock()
        mock_parsed.content = "Final Answer"
        mock_parsed.thinking = "Thinking..."
        mock_parsed.metadata = {"model_used": "gemini-3"}
        mock_agent.run.return_value = MagicMock(parsed=mock_parsed, duration_seconds=1.0, sanitized_command="gemini ...", parser_name="gemini_json", returncode=0, stderr="", output_file_content=None)
        mock_create_agent.return_value = mock_agent
        
        # 1. New thread
        args = {"prompt": "Hello", "cli_name": "gemini"}
        await tool.execute(args)
        
        # Get thread ID from arguments (tool updates it)
        thread_id = args.get("continuation_id")
        assert thread_id is not None
        
        thread = get_thread(thread_id)
        # Should have exactly 2 turns: User, Assistant (placeholder updated to final)
        assert len(thread.turns) == 2
        assert thread.turns[0].role == "user"
        assert thread.turns[0].content == "Hello"
        assert thread.turns[1].role == "assistant"
        assert "Final Answer" in thread.turns[1].content
        
        # 2. Resuming thread (Simulate server.py adding user turn)
        next_prompt = "How are you?"
        # server.py's reconstruct_thread_context adds the user turn
        add_turn(thread_id, "user", next_prompt)
        
        # Now call execute with the continuation_id
        resumed_args = {
            "prompt": next_prompt, 
            "continuation_id": thread_id,
            "_request_context": MagicMock()
        }
        await tool.execute(resumed_args)
        
        thread = get_thread(thread_id)
        # Sequence:
        # Turn 1: User "Hello"
        # Turn 2: Assistant "Final Answer"
        # Turn 3: User "How are you?" (added by server.py)
        # Turn 4: Assistant (placeholder updated to final)
        assert len(thread.turns) == 4
        assert thread.turns[2].role == "user"
        assert thread.turns[2].content == next_prompt
        assert thread.turns[3].role == "assistant"
        assert "Final Answer" in thread.turns[3].content

@pytest.mark.asyncio
async def test_simple_tool_no_duplicate_turns(storage):
    """Verify that SimpleTool (ChatTool) avoids double user turns."""
    tool = ChatTool()
    
    # Mock model generation
    mock_response = MagicMock()
    mock_response.content = "I am fine."
    mock_response.usage = {}
    mock_response.metadata = {}
    
    with patch.object(tool, "_resolve_model_context") as mock_resolve:
        mock_provider = MagicMock()
        mock_provider.generate_content.return_value = mock_response
        mock_provider.get_provider_type.return_value.value = "google"
        
        mock_caps = MagicMock()
        mock_caps.context_window = 1000000
        
        mock_alloc = MagicMock()
        mock_alloc.file_tokens = 500000
        mock_alloc.history_tokens = 250000
        mock_alloc.content_tokens = 750000
        
        mock_ctx = MagicMock(provider=mock_provider, capabilities=mock_caps)
        mock_ctx.calculate_token_allocation.return_value = mock_alloc
        mock_ctx.estimate_tokens.return_value = 10
        mock_ctx.model_name = "gemini"
        
        mock_resolve.return_value = ("gemini", mock_ctx)
        
        # 1. New thread
        args = {"prompt": "Hi", "working_directory_absolute_path": "/tmp"}
        await tool.execute(args)
        
        thread_id = tool._current_arguments.get("continuation_id")
        thread = get_thread(thread_id)
        assert len(thread.turns) == 2
        
        # 2. Resuming thread
        next_prompt = "What time is it?"
        # server.py's reconstruct_thread_context adds the user turn
        add_turn(thread_id, "user", next_prompt)
        
        resumed_args = {
            "prompt": next_prompt,
            "continuation_id": thread_id,
            "working_directory_absolute_path": "/tmp",
            "_model_context": MagicMock(provider=mock_provider, capabilities=MagicMock()),
            "_resolved_model_name": "gemini"
        }
        # In this case, SimpleTool.execute is called with prompt already enhanced OR raw.
        # If raw, it should detect duplicate.
        await tool.execute(resumed_args)
        
        thread = get_thread(thread_id)
        # Turn 1: User "Hi"
        # Turn 2: Assistant "I am fine."
        # Turn 3: User "What time is it?" (added by server.py)
        # Turn 4: Assistant "I am fine."
        assert len(thread.turns) == 4
        assert thread.turns[2].content == next_prompt

@pytest.mark.asyncio
async def test_workflow_tool_initial_turn(storage):
    """Verify that WorkflowTool (ThinkDeepTool) records the initial user turn."""
    from tools.thinkdeep import ThinkDeepTool
    tool = ThinkDeepTool()
    
    # Mock model resolution
    with patch.object(tool, "_resolve_model_context") as mock_resolve:
        mock_resolve.return_value = ("gemini", MagicMock())
        
        # Step 1: New thread
        args = {
            "step": "Investigate bug",
            "step_number": 1,
            "total_steps": 3,
            "next_step_required": True,
            "findings": "Starting...",
            "confidence": "low"
        }
        await tool.execute(args)
        
        thread_id = tool._current_arguments.get("continuation_id")
        assert thread_id is not None
        
        thread = get_thread(thread_id)
        # Should have 2 turns: User (Investigate bug), Assistant (Base response)
        assert len(thread.turns) == 2
        assert thread.turns[0].role == "user"
        assert thread.turns[0].content == "Investigate bug"
        assert thread.turns[1].role == "assistant"
