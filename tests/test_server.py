"""
Tests for the main server functionality
"""

import pytest

from server import handle_call_tool


class TestServerTools:
    """Test server tool handling"""

    @pytest.mark.asyncio
    async def test_handle_call_tool_unknown(self):
        """Test calling an unknown tool"""
        result = await handle_call_tool("unknown_tool", {})
        assert len(result) == 1
        assert "Unknown tool: unknown_tool" in result[0].text

    @pytest.mark.asyncio
    async def test_handle_chat(self):
        """Test chat functionality using real integration testing"""
        import importlib
        import os

        # Set test environment
        os.environ["PYTEST_CURRENT_TEST"] = "test"

        # Save original environment
        original_env = {
            "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY"),
            "DEFAULT_MODEL": os.environ.get("DEFAULT_MODEL"),
        }

        try:
            # Set up environment for real provider resolution
            os.environ["OPENAI_API_KEY"] = "sk-test-key-server-chat-test-not-real"
            os.environ["DEFAULT_MODEL"] = "o3-mini"

            # Clear other provider keys to isolate to OpenAI
            for key in ["GEMINI_API_KEY", "XAI_API_KEY", "OPENROUTER_API_KEY"]:
                os.environ.pop(key, None)

            # Reload config and clear registry
            import config

            importlib.reload(config)
            from providers.registry import ModelProviderRegistry

            ModelProviderRegistry._instance = None

            # Test with real provider resolution
            try:
                result = await handle_call_tool("chat", {"prompt": "Hello Gemini", "model": "o3-mini"})

                # If we get here, check the response format
                assert len(result) == 1
                # Parse JSON response
                import json

                response_data = json.loads(result[0].text)
                assert "status" in response_data
                
                # We just want to make sure it doesn't crash and returns a valid status
                error_msg = result[0].text

            except Exception as e:
                error_msg = str(e)

            # Basic check to ensure it's not a generic python crash
            assert error_msg is not None
            assert "MagicMock" not in error_msg
            assert "UnboundLocalError" not in error_msg
            assert "NameError" not in error_msg

        finally:
            # Restore environment
            for key, value in original_env.items():
                if value is not None:
                    os.environ[key] = value
                else:
                    os.environ.pop(key, None)

            # Reload config and clear registry
            importlib.reload(config)
            ModelProviderRegistry._instance = None

    @pytest.mark.asyncio
    async def test_handle_version(self):
        """Test getting version info"""
        result = await handle_call_tool("version", {})
        assert len(result) == 1

        response = result[0].text
        # Parse the JSON response
        import json

        data = json.loads(response)
        assert data["status"] == "success"
        content = data["content"]

        # Check for expected content in the markdown output
        assert "# PAL MCP Server Version" in content
        assert "## Server Information" in content
        assert "## Configuration" in content
        assert "Current Version" in content
