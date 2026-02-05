import asyncio
import pytest
import time
from pathlib import Path
from unittest.mock import MagicMock, AsyncMock, patch
from clink.agents.base import BaseCLIAgent, CLIAgentError
from clink.models import ResolvedCLIClient, ResolvedCLIRole

async def mock_hang():
    """A coroutine that hangs until cancelled."""
    try:
        while True:
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        return b""
    return b""

class MockStream:
    """A stream that hangs but is responsive to cancellation."""
    def __init__(self):
        self.readline = AsyncMock(side_effect=mock_hang)

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
    mock_process.wait = AsyncMock(side_effect=mock_hang)
    mock_process.returncode = None
    mock_process.pid = 12345
    
    # Mock process group ID for killing
    with patch("os.getpgid", return_value=12345), \
         patch("os.killpg") as mock_killpg, \
         patch("shutil.which", return_value="/usr/bin/echo"), \
         patch("asyncio.create_subprocess_exec", return_value=mock_process), \
         patch("clink.agents.base.get_publisher", return_value=mock_publisher):
        
        try:
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
            pytest.fail("Test hung! Interrupt was not detected or did not break the hang.")

        assert mock_killpg.called

@pytest.mark.asyncio
async def test_clink_idle_timeout_during_hang():
    """
    Regression test: Verify that BaseCLIAgent aborts immediately on idle timeout
    even if the subprocess is hung and blocking readline().
    """
    # 1. Setup mocks
    client = ResolvedCLIClient(
        name="test-cli",
        executable=["echo"],
        parser="gemini_json",
        working_dir=None,
        default_total_timeout_seconds=3600,
        default_idle_timeout_seconds=1, # Very short idle timeout
        roles={},
    )
    role = ResolvedCLIRole(
        name="default",
        prompt_path=Path("dummy"),
        role_args=[],
    )
    
    agent = BaseCLIAgent(client)
    
    mock_publisher = MagicMock()
    mock_publisher.is_interrupted.return_value = False
    mock_publisher.check_interruption = AsyncMock(return_value=False)

    # Mock subprocess
    mock_process = AsyncMock()
    mock_process.stdout = MockStream()
    mock_process.stderr = MockStream()
    mock_process.wait = AsyncMock(side_effect=mock_hang)
    mock_process.returncode = None
    mock_process.pid = 12345
    
    with patch("os.getpgid", return_value=12345), \
         patch("os.killpg") as mock_killpg, \
         patch("shutil.which", return_value="/usr/bin/echo"), \
         patch("asyncio.create_subprocess_exec", return_value=mock_process), \
         patch("clink.agents.base.get_publisher", return_value=mock_publisher):
        
        try:
            await asyncio.wait_for(
                agent.run(
                    role=role,
                    prompt="test prompt",
                    files=[],
                    images=[]
                ),
                timeout=5.0
            )
            pytest.fail("Should have raised CLIAgentError due to idle timeout")
        except CLIAgentError as e:
            assert "idle timed out" in str(e).lower()
        except asyncio.TimeoutError:
            pytest.fail("Test hung! Idle timeout was not detected or did not break the hang.")

        assert mock_killpg.called
