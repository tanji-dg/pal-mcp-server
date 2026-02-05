import asyncio
import pytest
import os
import signal
import psutil
import time
from unittest.mock import MagicMock
from clink.agents.base import BaseCLIAgent
from clink.models import ResolvedCLIClient

@pytest.mark.asyncio
async def test_kill_process_tree_robustness():
    """
    Verify that _kill_process_tree recursively kills children.
    """
    client = ResolvedCLIClient(
        name="test",
        executable=["bash"],
        parser="gemini_json",
        working_dir=None,
        default_total_timeout_seconds=10,
        default_idle_timeout_seconds=10,
        roles={},
    )
    agent = BaseCLIAgent(client)
    
    # Spawn a process that spawns a child
    # bash -c 'sleep 100 & sleep 100'
    process = await asyncio.create_subprocess_exec(
        "bash", "-c", "sleep 100 & sleep 100",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    pid = process.pid
    # Give bash a moment to spawn children
    await asyncio.sleep(0.5)
    
    p = psutil.Process(pid)
    children = p.children(recursive=True)
    assert len(children) >= 1
    child_pids = [c.pid for c in children]
    
    # Kill the tree
    await agent._kill_process_tree(process)
    
    # Wait for things to settle
    await asyncio.sleep(0.5)
    
    # Verify everything is dead
    assert not psutil.pid_exists(pid)
    for cpid in child_pids:
        assert not psutil.pid_exists(cpid), f"Child process {cpid} still exists!"

@pytest.mark.asyncio
async def test_kill_process_tree_with_start_new_session_false():
    """
    Verify that killing works even WITHOUT start_new_session=True (which we just removed).
    """
    # This is the same as above but explicitly confirming our new configuration works
    await test_kill_process_tree_robustness()
