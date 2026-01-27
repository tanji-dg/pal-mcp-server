
import json
import pytest
from unittest.mock import MagicMock, patch
from tools.clink import CLinkTool
from tools.models import ToolOutput

class TestClinkMetadata:
    """
    Tests for CLinkTool metadata handling, specifically ensuring that 
    large log histories are pruned to prevent client-side hangs.
    """

    @pytest.mark.asyncio
    async def test_metadata_logs_pruning_on_offload(self):
        """
        Verify that huge logs are removed from metadata when output is offloaded.
        """
        tool = CLinkTool()
        mock_client = MagicMock()
        mock_client.name = "test_cli"
        
        # Simulate HUGE logs (e.g. 5MB)
        huge_logs = ["Log line " + "x" * 1000 for _ in range(5000)]
        
        # Simulate HUGE content to trigger offloading (100KB+)
        huge_content = "Output " + "y" * 100000
        
        with patch("tools.clink.Path") as MockPath, \
             patch("tools.clink.datetime") as MockDateTime, \
             patch("tools.clink.uuid") as MockUUID:
            
            # Mock file writing
            mock_file = MagicMock()
            mock_file.absolute.return_value = "/tmp/fake_output.txt"
            MockPath.return_value.__truediv__.return_value.__truediv__.return_value = mock_file
            
            metadata = {
                "some_key": "some_value",
                "logs": huge_logs 
            }
            
            # Execute offloading logic
            content, cleaned_metadata, was_offloaded = tool._apply_output_limit(
                mock_client,
                huge_content,
                metadata,
                thinking="",
                logs=huge_logs
            )
            
            assert was_offloaded is True
            assert "logs" not in cleaned_metadata, "Logs MUST be removed from metadata on offload"
            assert cleaned_metadata.get("output_offloaded") is True
            
            # Verify serialization size
            output_obj = ToolOutput(
                status="success",
                content=content,
                content_type="text",
                metadata=cleaned_metadata
            )
            json_str = output_obj.model_dump_json()
            assert len(json_str) < 10000, f"Payload too large: {len(json_str)} bytes"

    @pytest.mark.asyncio
    async def test_metadata_logs_pruning_on_truncation(self):
        """
        Verify logs are pruned even when output is just truncated (fallback or summary).
        """
        tool = CLinkTool()
        mock_client = MagicMock()
        mock_client.name = "test_cli"
        
        huge_logs = ["Log " + "x"*1000] * 1000
        content = "Some content"
        
        # Force offload to fail (e.g. permission error) to trigger truncation fallback
        with patch("tools.clink.Path") as MockPath:
            mock_dir = MagicMock()
            mock_dir.mkdir.side_effect = Exception("Permission denied")
            MockPath.return_value.__truediv__.return_value.__truediv__.return_value = mock_dir
            
            metadata = {"logs": huge_logs}
            
            content, cleaned_metadata, was_offloaded = tool._apply_output_limit(
                mock_client,
                content,
                metadata,
                thinking="",
                logs=huge_logs
            )
            
            assert was_offloaded is False
            assert "logs" not in cleaned_metadata, "Logs MUST be removed on truncation"
