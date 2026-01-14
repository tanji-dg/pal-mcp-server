"Tests for Clink streaming capabilities and real-time notifications."

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# from mcp.types import LoggingLevel # Removed unused import
from clink.agents.base import AgentOutput, BaseCLIAgent
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from tools.clink import CLinkTool


@pytest.fixture
def mock_logger():
    """Mock logger to capture debug calls."""
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def mock_cli_client(tmp_path):
    """Create a mock ResolvedCLIClient."""
    return ResolvedCLIClient(
        name="test-cli",
        executable=["echo"],
        env={"TEST_ENV": "1"},
        working_dir=tmp_path,
        config_path=tmp_path / "config.json",
        timeout_seconds=5.0,
        parser="gemini-json",  # Using a known parser name
        roles={},  # Initialize with empty roles
    )


@pytest.fixture
def mock_cli_role(tmp_path):
    """Create a mock ResolvedCLIRole."""
    prompt_path = tmp_path / "prompt.txt"
    prompt_path.write_text("System Prompt", encoding="utf-8")
    return ResolvedCLIRole(
        name="default",
        role_args=["--role", "default"],
        prompt_path=prompt_path,
    )


class MockProcess:
    """Mock asyncio subprocess."""

    def __init__(self, stdout_lines=None, stderr_lines=None, returncode=0):
        self.stdout = AsyncMock()
        self.stderr = AsyncMock()
        self.stdin = MagicMock()  # stdin.write is synchronous
        self.returncode = returncode

        # Setup stdout streaming
        if stdout_lines:
            # readline side_effect needs to return bytes ending with newline, then empty bytes to signal EOF
            side_effects = [line.encode("utf-8") + b"\n" for line in stdout_lines] + [b""]
            self.stdout.readline.side_effect = side_effects
        else:
            self.stdout.readline.return_value = b""

        # Setup stderr streaming
        if stderr_lines:
            side_effects = [line.encode("utf-8") + b"\n" for line in stderr_lines] + [b""]
            self.stderr.readline.side_effect = side_effects
        else:
            self.stderr.readline.return_value = b""

    async def wait(self):
        return self.returncode


@pytest.mark.asyncio
async def test_agent_streaming_logs(mock_cli_client, mock_cli_role, mock_logger):
    """Test that BaseCLIAgent streams output to logs line-by-line."""

    with patch("clink.agents.base.get_parser", return_value=MagicMock(name="mock_parser")):
        agent = BaseCLIAgent(mock_cli_client)
        # Inject mock logger
        agent._logger = mock_logger

        stdout_content = ["Line 1", "Line 2", "Line 3"]
        mock_process = MockProcess(stdout_lines=stdout_content)

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_process) as mock_exec,
            patch("clink.agents.base.shutil.which", return_value="/bin/echo"),
        ):

            result = await agent.run(role=mock_cli_role, prompt="test prompt", files=[], images=[])

            # Verify process execution
            assert mock_exec.called

            # Verify result content
            assert result.stdout.strip() == "Line 1\nLine 2\nLine 3"

            # Verify real-time logging calls
            # We expect debug calls with [CLI OUTPUT] prefix for each line
            debug_calls = [
                call.args[0] for call in mock_logger.debug.call_args_list if "[CLI OUTPUT]" in str(call.args[0])
            ]

            assert len(debug_calls) == 3
            assert "[CLI OUTPUT] Line 1" in debug_calls[0]
            assert "[CLI OUTPUT] Line 2" in debug_calls[1]
            assert "[CLI OUTPUT] Line 3" in debug_calls[2]


@pytest.mark.asyncio
async def test_agent_output_callback(mock_cli_client, mock_cli_role):
    """Test that BaseCLIAgent invokes the output callback."""

    with patch("clink.agents.base.get_parser", return_value=MagicMock(name="mock_parser")):
        agent = BaseCLIAgent(mock_cli_client)
        stdout_content = ["Streamed Line 1", "Streamed Line 2"]
        mock_process = MockProcess(stdout_lines=stdout_content)

        callback_mock = AsyncMock()

        with (
            patch("asyncio.create_subprocess_exec", return_value=mock_process),
            patch("clink.agents.base.shutil.which", return_value="/bin/echo"),
        ):

            await agent.run(role=mock_cli_role, prompt="test", files=[], images=[], output_callback=callback_mock)

            # Verify callback was called for each line
            assert callback_mock.call_count == 2
            callback_mock.assert_any_call("Streamed Line 1\n")
            callback_mock.assert_any_call("Streamed Line 2\n")


@pytest.mark.asyncio
async def test_clink_tool_notifications(tmp_path):
    """Test that CLinkTool sends notifications to the session."""

    # Mock registry and client
    mock_registry = MagicMock()
    mock_role = ResolvedCLIRole(
        name="default",
        role_args=[],
        prompt_path=tmp_path / "prompt.txt",
    )
    (tmp_path / "prompt.txt").write_text("System Prompt")

    # Initialize client WITH the role
    mock_client = ResolvedCLIClient(
        name="test-cli",
        executable=["echo"],
        env={},
        working_dir=tmp_path,
        config_path=tmp_path,
        timeout_seconds=5.0,
        parser="gemini-json",
        roles={"default": mock_role},  # Add role to map
    )

    mock_registry.get_client.return_value = mock_client
    # mock_client.get_role is a real method now, no need to mock return_value
    mock_registry.list_clients.return_value = ["test-cli"]
    mock_registry.list_roles.return_value = ["default"]

    # Mock server session and context
    mock_session = AsyncMock()
    mock_request_context = MagicMock()
    mock_request_context.session = mock_session

    # Initialize tool with mocked registry
    with patch("tools.clink.get_registry", return_value=mock_registry):
        tool = CLinkTool()

        # Mock create_agent to return an agent that calls the callback
        async def mock_agent_run(*args, output_callback=None, **kwargs):
            if output_callback:
                if asyncio.iscoroutinefunction(output_callback):
                    await output_callback("Executing tool: test")
                else:
                    output_callback("Executing tool: test")

            return AgentOutput(
                parsed=MagicMock(content="Result", metadata={}),
                sanitized_command=["echo"],
                returncode=0,
                stdout="Result",
                stderr="",
                duration_seconds=0.1,
                parser_name="mock",
            )

        mock_agent = AsyncMock()
        mock_agent.run.side_effect = mock_agent_run

        with patch("tools.clink.create_agent", return_value=mock_agent):
            # Execute tool with request context
            arguments = {"prompt": "test", "cli_name": "test-cli", "_request_context": mock_request_context}

            await tool.execute(arguments)

            # Verify notification was sent
            mock_session.send_log_message.assert_called_once()
            call_kwargs = mock_session.send_log_message.call_args.kwargs
            # Expect "info" string literal instead of LoggingLevel.INFO
            assert call_kwargs["level"] == "info"
            assert "[test-cli] Executing tool: test" in call_kwargs["data"]
