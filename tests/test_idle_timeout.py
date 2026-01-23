
import asyncio
import pytest
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from clink.agents.base import BaseCLIAgent, CLIAgentError
from clink.models import ResolvedCLIClient, ResolvedCLIRole

@pytest.mark.asyncio
async def test_idle_timeout_trigger():
    """Verify that idle timeout triggers if process produces no output."""
    role = ResolvedCLIRole(
        name="default",
        prompt_path=Path("/tmp/test-prompt.txt"),
        total_timeout_seconds=5,
        idle_timeout_seconds=1
    )
    
    client = ResolvedCLIClient(
        name="test-cli",
        executable=["sleep", "100"],
        working_dir=None,
        default_total_timeout_seconds=5,
        default_idle_timeout_seconds=1,
        parser="gemini_json",
        roles={"default": role}
    )
    agent = BaseCLIAgent(client)
    
    with patch('shutil.which', return_value='/bin/sleep'), \
         patch('clink.agents.base.get_parser'), \
         patch('pathlib.Path.read_text', return_value="system prompt"):
        
        start_time = asyncio.get_event_loop().time()
        
        try:
            await agent.run(
                role=role,
                prompt="hello",
                files=[],
                images=[]
            )
        except CLIAgentError as e:
            duration = asyncio.get_event_loop().time() - start_time
            print(f"Caught expected error: {e}, duration: {duration}")
            # If idle timeout works, it should finish in ~1 second, not 5.
            assert duration < 3.0 
            assert "idle timed out" in str(e)

