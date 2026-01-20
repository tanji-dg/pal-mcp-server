"""
Reliability tests for conversation persistence.
Ensures that user turns and errors are saved even if tool execution fails.
"""

import pytest
import json
from unittest.mock import MagicMock, patch, AsyncMock
from tools import ChatTool, CLinkTool
from tools.shared.exceptions import ToolExecutionError
from utils.storage_backend import get_storage_backend

@pytest.fixture
def storage():
    """Get fresh SQLite storage."""
    s = get_storage_backend()
    # Clear existing data for test isolation
    with s._get_conn() as conn:
        conn.execute("DELETE FROM conversations")
    return s

class TestConversationPersistenceReliability:

    @pytest.mark.asyncio
    async def test_chat_tool_persistence_on_failure(self, storage):
        """Verify ChatTool saves user turn and error response when AI fails."""
        tool = ChatTool()
        
        # Mock provider to raise exception during generation
        mock_provider = MagicMock()
        mock_provider.generate_content.side_effect = Exception("AI Provider Down")
        mock_provider.get_provider_type.return_value = MagicMock(value="test-provider")
        
        # Mock ModelContext to return our mock provider
        mock_context = MagicMock()
        mock_context.provider = mock_provider
        mock_context.model_name = "test-model"
        mock_context.capabilities.supports_extended_thinking = False
        mock_context.calculate_token_allocation.return_value = MagicMock(content_tokens=100000)

        args = {
            "prompt": "Persistent Question",
            "working_directory_absolute_path": "/tmp",
            "_model_context": mock_context,
            "_resolved_model_name": "test-model"
        }

        # Execution should raise ToolExecutionError, but turns should be saved
        with pytest.raises(ToolExecutionError):
            await tool.execute(args)

        # 1. Check if a thread was created
        conversations = storage.list_all(include_expired=True)
        assert len(conversations) == 1
        
        thread_id = list(conversations.keys())[0]
        content = json.loads(conversations[thread_id][0])
        
        # 2. Verify turns
        turns = content["turns"]
        assert len(turns) == 2
        assert turns[0]["role"] == "user"
        assert turns[0]["content"] == "Persistent Question"
        
        assert turns[1]["role"] == "assistant"
        assert "AI Provider Down" in turns[1]["content"]
        assert turns[1].get("model_name") == "test-model"

    @pytest.mark.asyncio
    async def test_clink_tool_persistence_on_failure(self, storage):
        """Verify CLinkTool saves user turn and error response when CLI fails."""
        # Setup registry mock before instantiating CLinkTool
        from clink.agents import CLIAgentError
        
        with patch("tools.clink.get_registry") as mock_registry:
            # Setup registry mock
            mock_client = MagicMock()
            mock_client.name = "test-cli"
            mock_client.runner = "test-cli"
            mock_role = MagicMock()
            mock_role.name = "default"
            mock_role.prompt_path.read_text.return_value = "System Prompt"
            mock_client.get_role.return_value = mock_role
            mock_registry.return_value.get_client.return_value = mock_client
            mock_registry.return_value.list_clients.return_value = ["test-cli"]
            mock_registry.return_value.list_roles.return_value = ["default"]

            tool = CLinkTool()
            
            # Mock agent to raise CLIAgentError
            mock_agent = AsyncMock()
            mock_agent.run.side_effect = CLIAgentError("CLI Exploded", returncode=1, stderr="Fatal Stack Trace")
            
            with patch("tools.clink.create_agent", return_value=mock_agent):
                args = {
                    "prompt": "CLI Test Prompt",
                    "cli_name": "test-cli",
                    "continuation_id": None # New session
                }

                with pytest.raises(ToolExecutionError):
                    await tool.execute(args)

                # Check persistence
                conversations = storage.list_all(include_expired=True)
                assert len(conversations) == 1
                
                thread_id = list(conversations.keys())[0]
                content = json.loads(conversations[thread_id][0])
                
                turns = content["turns"]
                assert len(turns) == 2
                assert turns[0]["role"] == "user"
                assert "CLI Test Prompt" in turns[0]["content"]
                
                assert turns[1]["role"] == "assistant"
                assert "CLI Exploded" in turns[1]["content"]
                assert turns[1]["model_provider"] == "test-cli"
