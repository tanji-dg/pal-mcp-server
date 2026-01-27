import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from clink.agents.base import BaseCLIAgent, CLIAgentError
from clink.models import ResolvedCLIClient, ResolvedCLIRole

class MockStream:
    """A stream that hangs forever on readline."""
    async def readline(self):
        await asyncio.sleep(100)
        return b""

@pytest.mark.asyncio
async def test_clink_interrupt_during_hang():
    """
    Regression test: Verify that BaseCLIAgent can be interrupted even if
    the subprocess is hung and producing no output (blocking readline).
    """
    # 1. Setup mocks
    client = ResolvedCLIClient(
        name="test-cli",
        executable=["echo"],
        parser="gemini_json",
        working_dir=None,
        default_total_timeout_seconds=3600,
        default_idle_timeout_seconds=600,
        roles={},
    )
    from pathlib import Path
    role = ResolvedCLIRole(
        name="default",
        prompt_path=Path("dummy"),
        role_args=[],
    )
    
    agent = BaseCLIAgent(client)
    
    # Mock publisher to simulate interruption after a short delay
    mock_publisher = MagicMock()
    # Initially False, then becomes True
    mock_publisher.is_interrupted.side_effect = [False, False, True, True, True]
    mock_publisher.check_interruption = AsyncMock(return_value=True)

    # Mock subprocess
    mock_process = AsyncMock()
    mock_process.stdout = MockStream()
    mock_process.stderr = MockStream()
    mock_process.returncode = None
    mock_process.pid = 12345
    
    # Mock process group ID for killing
    with patch("os.getpgid", return_value=12345), \
         patch("os.killpg") as mock_killpg, \
         patch("shutil.which", return_value="/usr/bin/echo"), \
         patch("asyncio.create_subprocess_exec", return_value=mock_process), \
         patch("clink.agents.base.get_publisher", return_value=mock_publisher):
        
        # 2. Run the agent
        # We expect InterruptedError to be raised because the interruption 
        # is detected in the _read_stream loop despite readline() hanging.
        # Note: In BaseCLIAgent.run, the tasks are gathered and InterruptedError 
        # is propagated.
        
        try:
            # We use a short timeout for the test itself just in case it still hangs
            await asyncio.wait_for(
                agent.run(
                    role=role,
                    prompt="test prompt",
                    files=[],
                    images=[]
                ),
                timeout=5.0
            )
            pytest.fail("Should have raised CLIAgentError")
        except CLIAgentError as e:
            # This is what we want! 
            assert "interrupted" in str(e).lower()
        except asyncio.TimeoutError:
            pytest.fail("Test hung! The fix is likely not working or wait_for timeout is too long.")

        # 3. Verify kill was attempted
        # It should have called killpg with SIGTERM first
        assert mock_killpg.called
        assert mock_killpg.call_args_list[0][0][1] == 15 # SIGTERM
