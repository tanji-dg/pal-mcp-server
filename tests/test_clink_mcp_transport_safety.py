
import json
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from tools.clink import CLinkTool
from clink.agents.base import AgentOutput
from clink.parsers.base import ParsedCLIResponse

@pytest.mark.asyncio
async def test_mcp_transport_safety_regression():
    """
    Fundamental regression test: Ensures that CLinkTool always returns a payload
    that is safe for MCP transport, even when internal processing involves huge data.
    """
    tool = CLinkTool()
    
    # 1. 巨大な生データ（ハングの元）をシミュレート
    # 内部的には数MBのデータを持っている状態
    huge_thinking = "Thinking " * 10000 # 90KB
    huge_content = "Final Output " * 5000 # 65KB
    huge_logs = ["Log " + "x" * 1000 for _ in range(1000)] # 1MB
    huge_raw_events = [{"type": "event", "data": "y" * 1000} for _ in range(1000)] # 1MB
    
    mock_parsed = ParsedCLIResponse(
        content=huge_content,
        thinking=huge_thinking,
        metadata={
            "raw_events": huge_raw_events, # これが消える必要がある
            "raw": {"data": "z" * 100000}, # これが消える必要がある
            "usage": {"input_tokens": 100, "output_tokens": 50} # これは残る必要がある
        }
    )
    
    mock_result = AgentOutput(
        parsed=mock_parsed,
        sanitized_command=["test"],
        returncode=0,
        stdout="raw stdout",
        stderr="",
        duration_seconds=0.5,
        parser_name="claude_json"
    )
    
    mock_agent = AsyncMock()
    mock_agent.run.return_value = mock_result
    
    mock_registry = MagicMock()
    mock_registry.get_client.return_value.name = "claude"
    
    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
        
        # 2. ツールを実行
        # execute() は MCP server が直接受け取る [TextContent] を返す
        mcp_response_list = await tool.execute({
            "prompt": "test",
            "cli_name": "claude"
        })
        
        # 3. 根本的な検証: シリアライズ可能性とサイズ
        assert isinstance(mcp_response_list, list)
        assert len(mcp_response_list) == 1
        
        # 実際の MCP 送信時の挙動をシミュレート (json.dumps)
        raw_text_payload = mcp_response_list[0].text
        
        # シリアライズに失敗しないか？
        try:
            final_json = json.loads(raw_text_payload)
        except Exception as e:
            pytest.fail(f"Final MCP payload is not valid JSON: {e}")
            
        # 4. サイズの検証
        # 内部でMB単位のデータがあっても、クライアントに渡すJSONは十分に小さくなければならない
        payload_size = len(raw_text_payload)
        print(f"Final MCP Payload Size: {payload_size} bytes")
        
        # 安全閾値: 100KB (通常、MCPクライアントがハングしないサイズ)
        # 本文がオフロードされているため、これより遥かに小さくなるはず
        assert payload_size < 100_000, f"MCP Payload is too large ({payload_size} bytes), might cause client hangs."
        
        # 5. 内容の検証（クリーニングの徹底）
        metadata = final_json.get("metadata", {})
        assert "raw_events" not in metadata, "Huge 'raw_events' leaked into MCP metadata!"
        assert "raw" not in metadata, "Huge 'raw' data leaked into MCP metadata!"
        assert "logs" not in metadata, "Huge log history leaked into MCP metadata!"
        
        # 必要な情報は残っているか？
        assert "usage" in metadata
        assert "output_offloaded" in metadata
        assert metadata["output_offloaded"] is True
        
        # 6. 思考プロセスが（制限内で）含まれているか
        assert "<thinking>" in final_json["content"]
        assert "Thinking Preview" in final_json["content"]
        assert huge_thinking[:1000] in final_json["content"]

@pytest.mark.asyncio
async def test_mcp_transport_safety_small_response():
    """
    Ensures metadata is clean even for small responses.
    """
    tool = CLinkTool()
    mock_parsed = ParsedCLIResponse(
        content="Small success",
        metadata={"raw_events": [{"data": "secret"}]}
    )
    mock_result = AgentOutput(
        parsed=mock_parsed,
        sanitized_command=["test"],
        returncode=0,
        stdout="", stderr="", duration_seconds=0.1, parser_name="test"
    )
    
    mock_agent = AsyncMock()
    mock_agent.run.return_value = mock_result
    mock_registry = MagicMock()
    mock_registry.get_client.return_value.name = "test"

    with patch("tools.clink.create_agent", return_value=mock_agent), \
         patch("tools.clink.get_registry", return_value=mock_registry), \
         patch("tools.clink.get_publisher", return_value=None), \
         patch.object(tool, "_record_assistant_turn"), \
         patch.object(tool, "handle_prompt_file_with_fallback", return_value="prompt"):
         
        mcp_response = await tool.execute({"prompt": "test"})
        final_json = json.loads(mcp_response[0].text)
        
        assert "raw_events" not in final_json["metadata"]
        assert final_json["content"] == "Small success"
