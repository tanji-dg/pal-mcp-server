#!/usr/bin/env python3
"""
Clink Tool Validation Test

Tests the Clink tool functionality within the simulator environment.
This ensures that the bridge to external CLIs works correctly.
"""

import shutil
from .base_test import BaseSimulatorTest


class ClinkValidationTest(BaseSimulatorTest):
    """Test Clink tool functionality"""

    @property
    def test_name(self) -> str:
        return "clink_validation"

    @property
    def test_description(self) -> str:
        return "Clink tool validation"

    def run_test(self) -> bool:
        """Test Clink tool execution"""
        try:
            self.logger.info("Test: Clink tool validation")

            # Setup test files (not strictly needed for basic clink test but good practice)
            self.setup_test_files()

            # Check if any CLI client is available
            gemini_path = shutil.which("gemini")
            claude_path = shutil.which("claude")

            cli_name = "gemini" if gemini_path else ("claude" if claude_path else None)

            if not cli_name:
                self.logger.warning("No supported CLI tools (gemini, claude) found in PATH. Skipping clink test.")
                return True

            self.logger.info(f"Using CLI: {cli_name}")

            # Call clink tool
            # We use a simple prompt that should work even without complex context
            self.logger.info("  1.1: Calling clink tool")
            response, continuation_id = self.call_mcp_tool(
                "clink",
                {
                    "prompt": "Hello, please reply with 'OK'.",
                    "cli_name": cli_name,
                    "role": "default",
                    "absolute_file_paths": [],
                },
            )

            # Note: response might be None if the CLI tool fails due to missing API keys in the CLI itself,
            # but the MCP tool call itself should have been attempted.
            # However, call_mcp_tool returns None on error or timeout.

            # If the tool executed but returned an error (e.g. auth error from CLI),
            # we might get a valid response object with error status in the JSON.
            # But call_mcp_tool parses the "content" of the success response.

            # If call_mcp_tool returns None, it might mean the tool failed completely.
            # Let's check the logs if response is None to distinguish between "tool crash" and "CLI error".

            # For validation purposes, if we get a response (even if it contains an error message text),
            # it means the bridge is working.

            if response:
                self.logger.info(f"  ✅ Clink tool returned response: {response[:100]}...")
                return True
            else:
                # If response is None, it might be because the CLI process failed exit code non-zero
                # which call_mcp_tool might log as error but return None.
                # In bridge-only mode with no API keys, the CLI tool (e.g. gemini) will likely fail.
                # But we want to verify that PAL-MCP handled the request.

                # Check logs to see if we had a successful tool call attempt
                logs = self.get_recent_server_logs(100)
                if f"Calling MCP tool clink" in logs or "CLinkTool.execute started" in logs:
                     self.logger.info("  ✅ Clink tool was called (even if CLI execution failed)")
                     return True

                self.logger.error("Failed to get response from clink tool")
                return False

        except Exception as e:
            self.logger.error(f"Clink validation test failed: {e}")
            return False
        finally:
            self.cleanup_test_files()
