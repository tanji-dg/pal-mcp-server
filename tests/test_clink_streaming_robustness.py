
import json
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import pytest

from clink.agents.base import AgentOutput
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from clink.parsers.gemini import GeminiJSONParser
from tools.clink import CLinkTool

@pytest.mark.asyncio
async def test_clink_robust_json_parsing(tmp_path):
    """Verify that _notification_callback handles concatenated JSON (}{) and init events."""
    
    # Combined line with }{
    robust_stdout = '{"type":"init","model":"gemini-test"}{"type":"message","role":"assistant","content":"Thinking...","delta":true}'
    
    mock_registry = MagicMock()
    mock_role = ResolvedCLIRole(
        name="default",
        role_args=[],
        prompt_path=tmp_path / "prompt.txt",
    )
    (tmp_path / "prompt.txt").write_text("System Prompt")

    mock_client = ResolvedCLIClient(
        name="gemini",
        executable=["echo"],
        env={},
        working_dir=tmp_path,
        default_total_timeout_seconds=5,
        default_idle_timeout_seconds=2,
        parser="gemini_json",
        roles={"default": mock_role},
        output_to_file=None,
    )

    mock_registry.get_client.return_value = mock_client
    mock_registry.list_clients.return_value = ["gemini"]
    mock_registry.list_roles.return_value = ["default"]

    mock_session = AsyncMock()
    mock_request_context = MagicMock()
    mock_request_context.session = mock_session

    with patch("tools.clink.get_registry", return_value=mock_registry):
        tool = CLinkTool()

        async def mock_agent_run(*args, output_callback=None, **kwargs):
            if output_callback:
                await output_callback(robust_stdout)

            from clink.parsers.base import ParsedCLIResponse
            return AgentOutput(
                parsed=ParsedCLIResponse(content="Thinking...", metadata={"model_used": "gemini-test"}),
                sanitized_command=["gemini", "test"],
                returncode=0,
                stdout=robust_stdout,
                stderr="",
                duration_seconds=0.1,
                parser_name="gemini_json",
            )

        mock_agent = AsyncMock()
        mock_agent.run.side_effect = mock_agent_run

        with patch("tools.clink.create_agent", return_value=mock_agent):
            arguments = {
                "prompt": "test",
                "cli_name": "gemini",
                "_request_context": mock_request_context,
            }

            await tool.execute(arguments)

            # Check notifications
            notification_data = [call.kwargs["data"] for call in mock_session.send_log_message.call_args_list]

            # Should have handled the message delta after splitting }{
            assert any("🧠 Thinking: Thinking..." in d for d in notification_data)
            
            # init event should be converted to human readable form
            assert any("🚀" in d and "gemini-test" in d for d in notification_data)
            
            # Raw JSON should NOT be in notifications
            assert not any('{"type":' in d for d in notification_data)
