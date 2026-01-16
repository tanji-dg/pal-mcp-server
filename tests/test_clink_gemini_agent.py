import asyncio
import shutil
from pathlib import Path

import pytest

from clink.agents.base import CLIAgentError
from clink.agents.gemini import GeminiAgent
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
def gemini_agent():
    prompt_path = Path("systemprompts/clink/gemini_default.txt").resolve()
    role = ResolvedCLIRole(name="default", prompt_path=prompt_path, role_args=[])
    client = ResolvedCLIClient(
        name="gemini",
        executable=["gemini"],
        internal_args=[],
        config_args=[],
        env={},
        default_total_timeout_seconds=0,
        default_idle_timeout_seconds=0,
        parser="gemini_json",
        roles={"default": role},
        output_to_file=None,
        working_dir=None,
    )
    return GeminiAgent(client), role


async def _run_agent_with_process(monkeypatch, agent, role, process):
    async def fake_create_subprocess_exec(*_args, **_kwargs):
        return process

    def fake_which(executable_name):
        return f"/usr/bin/{executable_name}"

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(shutil, "which", fake_which)
    return await agent.run(role=role, prompt="do something", files=[], images=[])


@pytest.mark.asyncio
async def test_gemini_agent_recovers_tool_error(monkeypatch, gemini_agent):
    agent, role = gemini_agent
    error_json = """{
  "error": {
    "type": "FatalToolExecutionError",
    "message": "Error executing tool replace: Failed to edit",
    "code": "edit_expected_occurrence_mismatch"
  }
}"""
    stderr = ("Error: Failed to edit, expected 1 occurrence but found 2.\n" + error_json).encode()
    process = DummyProcess(stderr=stderr, returncode=54)

    result = await _run_agent_with_process(monkeypatch, agent, role, process)

    assert result.returncode == 54
    assert result.parsed.metadata["cli_error_recovered"] is True
    assert result.parsed.metadata["cli_error_code"] == "edit_expected_occurrence_mismatch"
    assert "Gemini CLI reported a tool failure" in result.parsed.content


@pytest.mark.asyncio
async def test_gemini_agent_propagates_unrecoverable_error(monkeypatch, gemini_agent):
    agent, role = gemini_agent
    stderr = b"Plain failure without structured payload"
    process = DummyProcess(stderr=stderr, returncode=54)

    with pytest.raises(CLIAgentError):
        await _run_agent_with_process(monkeypatch, agent, role, process)
