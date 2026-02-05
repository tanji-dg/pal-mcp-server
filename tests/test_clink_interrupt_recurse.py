
import json
import asyncio
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from tools.clink import CLinkTool

@pytest.mark.asyncio
async def test_interruption_during_massive_prompt_transfer():
    """
    Test that interruption works even during the 'dead zone' of 
    writing a massive prompt to stdin.
    """
    tool = CLinkTool()
    
    # Simulate a very large prompt that would normally block drain()
    massive_prompt = "REASONING " * 100000 # Several megabytes
    
    mock_process = AsyncMock()
    mock_process.returncode = None
    mock_process.pid = 1234
    
    # Mock stdin.drain to block for a while, simulating slow I/O
    async def slow_drain():
        await asyncio.sleep(2.0)
    mock_process.stdin.drain = slow_drain
    
    mock_registry = MagicMock()
    mock_client = MagicMock()
    mock_client.name = "gemini"
    mock_client.parser = "claude_json"
    mock_client.output_to_file = None
    mock_client.default_total_timeout_seconds = 0
    mock_client.default_idle_timeout_seconds = 0
    mock_registry.get_client.return_value = mock_client
    
    # Mock publisher to return 'interrupted' after 0.5s
    mock_publisher = AsyncMock()
    interrupt_flag = [False]
    async def check_interrupt():
        return interrupt_flag[0]
    mock_publisher.check_interruption = check_interrupt
    
    with patch("tools.clink.create_agent") as mock_create_agent, \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=mock_publisher), \
         patch("clink.agents.base.get_publisher", return_value=mock_publisher), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        # Setup real BaseCLIAgent but with mocked process
        from clink.agents.base import BaseCLIAgent
        agent = BaseCLIAgent(mock_registry.get_client())
        mock_create_agent.return_value = agent
        
        with patch("asyncio.create_subprocess_exec", return_value=mock_process), \
             patch("shutil.which", return_value="/usr/bin/mock-cli"), \
             patch("os.getpgid", return_value=5678), \
             patch("os.killpg") as mock_killpg:
            
            # Start execution in background
            task = asyncio.create_task(tool.execute({
                "prompt": massive_prompt,
                "cli_name": "gemini",
                "role": "default",
                "client_context_budget": None
            }))
            
            # Wait a bit and then trigger interruption
            await asyncio.sleep(0.5)
            interrupt_flag[0] = True
            
            # Wait for task to finish
            result_list = await task
            payload = json.loads(result_list[0].text)
            
            # VERIFICATION:
            # 1. It should return an error/success status with 'interrupted' info
            if not ("interrupted" in payload["metadata"] or payload["status"] == "success"):
                print(f"\nDEBUG: Payload = {json.dumps(payload, indent=2)}")
            assert "interrupted" in payload["metadata"] or payload["status"] == "success"
            
            # 2. killpg should have been called despite drain() being blocked
            # (Because interruption monitor now starts before drain)
            mock_killpg.assert_called()
            print("Successfully interrupted during massive prompt transfer!")

@pytest.mark.asyncio
async def test_prevent_recurse_after_interruption():
    """
    Test that CLinkTool.execute refuses to start if the session is already interrupted.
    """
    tool = CLinkTool()
    
    mock_publisher = AsyncMock()
    mock_publisher.check_interruption.return_value = True # Already stopped!
    
    with patch("tools.clink.get_publisher", return_value=mock_publisher):
        result = await tool.execute({
            "prompt": "next step",
            "cli_name": "codex",
            "_request_context": MagicMock()
        })
        
        payload = json.loads(result[0].text)
        assert payload["status"] == "error"
        assert "cancelled" in payload["content"] or "cancelled" in payload["metadata"].values()
        print("Successfully prevented recursive call after interruption!")
