import asyncio
import shutil
import pytest
from pathlib import Path

from clink.agents.gemini import GeminiAgent
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from clink.agents.base import AgentOutput

# Dummy classes needed to mock the process execution
class DummyStreamWriter:
    def __init__(self, parent):
        self.parent = parent
        self.closed = False

    def write(self, data):
        if self.parent.stdin_data is None:
            self.parent.stdin_data = b""
        self.parent.stdin_data += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
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
        self.pid = 12345

    async def wait(self):
        return self.returncode

@pytest.fixture()
def gemini_agent():
    # Setup minimal role and client
    prompt_path = Path("systemprompts/clink/gemini_default.txt").resolve()
    # Create file if it doesn't exist to avoid path errors
    if not prompt_path.exists():
        prompt_path.parent.mkdir(parents=True, exist_ok=True)
        prompt_path.touch()

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

@pytest.mark.asyncio
async def test_gemini_agent_run_with_native_session_id_success(monkeypatch, gemini_agent):
    agent, role = gemini_agent
    
    # Mock subprocess creation
    async def fake_create_subprocess_exec(*args, **kwargs):
        # Return a successful dummy process so we can focus on the argument passing
        return DummyProcess(stdout=b'{"type":"result","response":"ok"}', returncode=0)

    def fake_which(executable_name):
        return f"/usr/bin/{executable_name}"

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_create_subprocess_exec)
    monkeypatch.setattr(shutil, "which", fake_which)

    # Calling run with native_session_id should NOT raise TypeError anymore
    result = await agent.run(
        role=role, 
        prompt="test prompt", 
        files=[], 
        images=[], 
        native_session_id="test-session-id"
    )
    assert result.returncode == 0
    assert result.parsed.content == "ok"
