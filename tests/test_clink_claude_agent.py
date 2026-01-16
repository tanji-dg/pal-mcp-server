import asyncio
import json
import shutil
from pathlib import Path

import pytest

from clink.agents.base import CLIAgentError
from clink.agents.claude import ClaudeAgent
from clink.models import ResolvedCLIClient, ResolvedCLIRole


class DummyStreamWriter:
    def __init__(self, parent):
        self.parent = parent

    def write(self, data):
        if self.parent.stdin_data is None:
            self.parent.stdin_data = b""
        self.parent.stdin_data += data

    async def drain(self):
        pass

    def close(self):
        pass


class DummyProcess:
    def __init__(self, *, stdout: bytes = b"", stderr: bytes = b"", returncode: int = 0):
        self.stdout = asyncio.StreamReader()
        self.stderr = asyncio.StreamReader()
        self.stdin = DummyStreamWriter(self)
        self.stdout.feed_data(stdout)
        self.stdout.feed_eof()
        self.stderr.feed_data(stderr)
        self.stderr.feed_eof()
        self.returncode = returncode
        self.stdin_data: bytes | None = None
        self._killed = False # Add killed flag

    async def communicate(self, input_data=None):
        if input_data:
            self.stdin_data = input_data
        # Read all remaining data from stdout/stderr to simulate full communication
        stdout_remaining = await self.stdout.read()
        stderr_remaining = await self.stderr.read()
        return stdout_remaining, stderr_remaining

    async def wait(self):
        # In these mock processes, output is pre-fed, so the process "completes" instantly.
        # Unless it was explicitly killed.
        if self._killed:
            return self.returncode
        # Simulate a quick process completion if not killed
        await asyncio.sleep(0.001) # Small delay to avoid busy loop in very fast test scenarios
        return self.returncode

    def kill(self):
        if not self._killed:
            self._killed = True
            # Set a non-zero return code for killed processes, if not already set by normal exit
            if self.returncode == 0:
                self.returncode = 137 # Standard code for SIGKILL/SIGTERM


@pytest.fixture()
def claude_agent():
    prompt_path = Path("systemprompts/clink/default.txt").resolve()
    role = ResolvedCLIRole(name="default", prompt_path=prompt_path, role_args=[])
    client = ResolvedCLIClient(
        name="claude",
        executable=["claude"],
        internal_args=["--print", "--output-format", "json"],
        config_args=["--permission-mode", "acceptEdits"],
        env={},
        default_total_timeout_seconds=0,
        default_idle_timeout_seconds=0,
        parser="claude_json",
        runner="claude",
        roles={"default": role},
        output_to_file=None,
        working_dir=None,
    )
    return ClaudeAgent(client), role


async def _run_agent_with_process(monkeypatch, agent, role, process, *, system_prompt="System prompt"):
    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return process

    def fake_which(executable_name):
        return f"/usr/bin/{executable_name}"

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(shutil, "which", fake_which)

    return await agent.run(
        role=role,
        prompt="Respond with 42",
        system_prompt=system_prompt,
        files=[],
        images=[],
    )


@pytest.mark.asyncio
async def test_claude_agent_injects_system_prompt(monkeypatch, claude_agent):
    agent, role = claude_agent
    stdout_payload = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "is_error": False,
            "result": "42",
        }
    ).encode()
    process = DummyProcess(stdout=stdout_payload)

    result = await _run_agent_with_process(monkeypatch, agent, role, process)

    assert "--append-system-prompt" in result.sanitized_command
    idx = result.sanitized_command.index("--append-system-prompt")
    assert result.sanitized_command[idx + 1] == "System prompt"
    assert process.stdin_data.decode().startswith("Respond with 42")


@pytest.mark.asyncio
async def test_claude_agent_recovers_error_payload(monkeypatch, claude_agent):
    agent, role = claude_agent
    stdout_payload = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "is_error": True,
            "result": "API Error",
        }
    ).encode()
    process = DummyProcess(stdout=stdout_payload, returncode=2)

    result = await _run_agent_with_process(monkeypatch, agent, role, process)

    assert result.returncode == 2
    assert result.parsed.content == "API Error"
    assert result.parsed.metadata["is_error"] is True


@pytest.mark.asyncio
async def test_claude_agent_propagates_unparseable_output(monkeypatch, claude_agent):
    agent, role = claude_agent
    process = DummyProcess(stdout=b"", returncode=1)

    with pytest.raises(CLIAgentError):
        await _run_agent_with_process(monkeypatch, agent, role, process)
