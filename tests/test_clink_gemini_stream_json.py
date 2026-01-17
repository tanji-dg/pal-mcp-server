"Tests for Gemini CLI stream-json format support."

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from clink.agents.base import AgentOutput
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from clink.parsers.gemini import GeminiJSONParser
from tools.clink import CLinkTool


@pytest.fixture
def gemini_stream_stdout():
    """Simulate Gemini stream-json output with thinking, tool calls and final result."""
    events = [
        {"type": "init", "timestamp": "2025-01-01T00:00:00Z", "session_id": "test-session", "model": "gemini-2.0-flash"},
        {"type": "message", "role": "assistant", "content": "I will search for the capital of France.", "delta": True},
        {"type": "tool_use", "timestamp": "2025-01-01T00:00:01Z", "tool_name": "google_search", "tool_id": "call_123", "parameters": {}},
        {"type": "tool_result", "timestamp": "2025-01-01T00:00:02Z", "tool_id": "call_123", "status": "success", "output": "Paris"},
        {"type": "message", "role": "assistant", "content": "The capital is Paris.", "delta": True},
        {
            "type": "result",
            "timestamp": "2025-01-01T00:00:03Z",
            "status": "success",
            "stats": {
                "total_tokens": 100,
                "input_tokens": 40,
                "output_tokens": 60,
                "cached": 0,
                "input": 40,
                "duration_ms": 1500,
                "tool_calls": 1
            }
        }
    ]
    return "\n".join(json.dumps(e) for e in events)

def test_gemini_json_parser_stream_json(gemini_stream_stdout):
    """Verify that GeminiJSONParser extracts the final result from multiple JSON objects.
    Also tests accumulation of messages when result object lacks response field.
    """
    # Create stdout where result lacks response but messages have content
    # This also tests init event model capture
    parser = GeminiJSONParser()
    parsed = parser.parse(gemini_stream_stdout, stderr="")

    assert parsed.content == "I will search for the capital of France.The capital is Paris."
    assert parsed.metadata["model_used"] == "gemini-2.0-flash"
    assert parsed.metadata["raw"]["type"] == "result"
    assert parsed.metadata["stats"]["total_tokens"] == 100

def test_gemini_json_parser_legacy_format():
    """Verify that GeminiJSONParser still supports legacy format with 'response' field."""
    legacy_stdout = json.dumps({
        "response": "Hello from legacy!",
        "stats": {
            "models": {"gemini-pro": {"tokens": {"total": 50}}}
        }
    })

    parser = GeminiJSONParser()
    parsed = parser.parse(legacy_stdout, stderr="")

    assert parsed.content == "Hello from legacy!"
    assert parsed.metadata["model_used"] == "gemini-pro"

@pytest.mark.asyncio
async def test_clink_tool_gemini_notifications(tmp_path, gemini_stream_stdout):
    """Verify that CLinkTool correctly parses Gemini stream-json events and sends notifications."""

    # Mock setup similar to tests/test_clink_streaming.py
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

        # Mock agent to simulate streaming output
        async def mock_agent_run(*args, output_callback=None, **kwargs):
            if output_callback:
                for line in gemini_stream_stdout.split("\n"):
                    await output_callback(line)

            parser = GeminiJSONParser()
            return AgentOutput(
                parsed=parser.parse(gemini_stream_stdout, ""),
                sanitized_command=["gemini", "test"],
                returncode=0,
                stdout=gemini_stream_stdout,
                stderr="",
                duration_seconds=0.1,
                parser_name="gemini_json",
            )

        mock_agent = AsyncMock()
        mock_agent.run.side_effect = mock_agent_run

        with patch("tools.clink.create_agent", return_value=mock_agent):
            arguments = {
                "prompt": "What is the capital of France?",
                "cli_name": "gemini",
                "_request_context": mock_request_context
            }

            await tool.execute(arguments)

            # Check notifications
            notification_data = [call.kwargs["data"] for call in mock_session.send_log_message.call_args_list]

            assert any("🧠 Thinking: I will search for the capital of France." in d for d in notification_data)
            assert any("🛠️ Executing: google_search" in d for d in notification_data)
            assert any("✅ Executed: google_search" in d for d in notification_data)
            assert any("🧠 Thinking: The capital is Paris." in d for d in notification_data)

            # Ensure final response is NOT in notifications (it should be in tool output)
            assert not any("The capital of France is Paris." in d for d in notification_data)
