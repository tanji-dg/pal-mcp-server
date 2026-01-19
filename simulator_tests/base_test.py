#!/usr/bin/env python3
"""
Base Test Class for Communication Simulator Tests

Provides common functionality and utilities for all simulator tests.
"""

import json
import logging
import os
import subprocess
import time
from typing import Optional

from .log_utils import LogUtils


class BaseSimulatorTest:
    """Base class for all communication simulator tests"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.test_files = {}
        self.test_dir = None

        # Configure logging first
        log_level = logging.DEBUG if verbose else logging.INFO
        logging.basicConfig(level=log_level, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(self.__class__.__name__)

        self.python_path = self._get_python_path()

    def _get_python_path(self) -> str:
        """Get the Python path for the virtual environment"""
        current_dir = os.getcwd()

        # Try .venv first (modern convention)
        venv_python = os.path.join(current_dir, ".venv", "bin", "python")
        if os.path.exists(venv_python):
            return venv_python

        # Try venv as fallback
        venv_python = os.path.join(current_dir, "venv", "bin", "python")
        if os.path.exists(venv_python):
            return venv_python

        # Try .pal_venv as fallback
        pal_venv_python = os.path.join(current_dir, ".pal_venv", "bin", "python")
        if os.path.exists(pal_venv_python):
            return pal_venv_python

        # Fallback to system python if venv doesn't exist
        self.logger.warning("Virtual environment not found, using system python")
        return "python"

    def setup_test_files(self):
        """Create test files for the simulation"""
        # Test Python file
        python_content = '''"""
Sample Python module for testing MCP conversation continuity
"""

def fibonacci(n):
    """Calculate fibonacci number recursively"""
    if n <= 1:
        return n
    return fibonacci(n-1) + fibonacci(n-2)

def factorial(n):
    """Calculate factorial iteratively"""
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result

class Calculator:
    """Simple calculator class"""

    def __init__(self):
        self.history = []

    def add(self, a, b):
        result = a + b
        self.history.append(f"{a} + {b} = {result}")
        return result

    def multiply(self, a, b):
        result = a * b
        self.history.append(f"{a} * {b} = {result}")
        return result
'''

        # Test configuration file
        config_content = """{
  "database": {
    "host": "localhost",
    "port": 5432,
    "name": "testdb",
    "ssl": true
  },
  "cache": {
    "redis_url": "redis://localhost:6379",
    "ttl": 3600
  },
  "logging": {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  }
}"""

        # Create files in the current project directory
        current_dir = os.getcwd()
        self.test_dir = os.path.join(current_dir, "test_simulation_files")
        os.makedirs(self.test_dir, exist_ok=True)

        test_py = os.path.join(self.test_dir, "test_module.py")
        test_config = os.path.join(self.test_dir, "config.json")

        with open(test_py, "w") as f:
            f.write(python_content)
        with open(test_config, "w") as f:
            f.write(config_content)

        # Ensure absolute paths for MCP server compatibility
        self.test_files = {"python": os.path.abspath(test_py), "config": os.path.abspath(test_config)}
        self.logger.debug(f"Created test files with absolute paths: {list(self.test_files.values())}")

    def call_mcp_tool(self, tool_name: str, params: dict) -> tuple[Optional[str], Optional[str]]:
        """Call an MCP tool via standalone server using Popen for better stream control"""
        try:
            # Prepare the MCP initialization and tool call sequence
            init_request = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "clientInfo": {"name": "communication-simulator", "version": "1.0.0"},
                },
            }

            # Send initialized notification
            initialized_notification = {"jsonrpc": "2.0", "method": "notifications/initialized"}

            # Prepare the tool call request
            tool_request = {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": params},
            }  # Combine all messages
            messages = [
                json.dumps(init_request, ensure_ascii=False),
                json.dumps(initialized_notification, ensure_ascii=False),
                json.dumps(tool_request, ensure_ascii=False),
            ]

            # Join with newlines as MCP expects
            input_data = "\n".join(messages) + "\n"

            # Call the standalone MCP server directly using Popen
            server_cmd = [self.python_path, "server.py"]

            self.logger.debug(f"Calling MCP tool {tool_name} with proper initialization")

            process = subprocess.Popen(
                server_cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,  # Line buffered
            )

            # Write input data
            try:
                process.stdin.write(input_data)
                process.stdin.flush()
                # Do NOT close stdin immediately, as this signals EOF to the server
                # and might cause it to shut down before processing the request.
            except Exception as e:
                self.logger.error(f"Failed to write to server stdin: {e}")
                process.kill()
                return None, None

            # Read stdout line by line until we find the response
            response_text = None
            start_time = time.time()
            timeout = 3600  # 1 hour timeout for long running tools

            while True:
                if time.time() - start_time > timeout:
                    self.logger.error(f"Timeout waiting for response from {tool_name}")
                    break

                # Check if process has exited unexpectedly
                if process.poll() is not None:
                    # Capture stderr for debugging
                    stderr_output = process.stderr.read()
                    self.logger.error(f"Server process exited unexpectedly with code {process.returncode}")
                    if stderr_output:
                        self.logger.error(f"Stderr: {stderr_output}")
                    break

                # Non-blocking read would be ideal, but for simplicity we assume the server outputs lines
                line = process.stdout.readline()
                if not line:
                    continue

                if line.strip() and line.startswith("{"):
                    try:
                        response = json.loads(line)
                        # Look for the tool call response with the expected ID
                        if response.get("id") == 2:
                            if "result" in response:
                                result = response["result"]
                                # Handle new response format with 'content' array
                                if isinstance(result, dict) and "content" in result:
                                    content_array = result["content"]
                                    if isinstance(content_array, list) and len(content_array) > 0:
                                        response_text = content_array[0].get("text", "")
                                # Handle legacy format
                                elif isinstance(result, list) and len(result) > 0:
                                    response_text = result[0].get("text", "")
                            elif "error" in response:
                                self.logger.error(f"MCP error: {response['error']}")

                            # We found the response (success or error), break the loop
                            break
                    except json.JSONDecodeError:
                        pass

            # Clean up process
            try:
                process.terminate()
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
            except Exception:
                pass

            if not response_text:
                return None, None

            # Extract continuation_id if present
            continuation_id = self._extract_continuation_id(response_text)

            return response_text, continuation_id

        except Exception as e:
            self.logger.error(f"MCP tool call failed: {e}")
            return None, None

    def _parse_mcp_response(self, stdout: str, expected_id: int = 2) -> Optional[str]:
        """Parse MCP JSON-RPC response from stdout"""
        try:
            lines = stdout.strip().split("\n")
            for line in lines:
                if line.strip() and line.startswith("{"):
                    response = json.loads(line)
                    # Look for the tool call response with the expected ID
                    if response.get("id") == expected_id and "result" in response:
                        # Extract the actual content from the response
                        result = response["result"]
                        # Handle new response format with 'content' array
                        if isinstance(result, dict) and "content" in result:
                            content_array = result["content"]
                            if isinstance(content_array, list) and len(content_array) > 0:
                                return content_array[0].get("text", "")
                        # Handle legacy format
                        elif isinstance(result, list) and len(result) > 0:
                            return result[0].get("text", "")
                    elif response.get("id") == expected_id and "error" in response:
                        self.logger.error(f"MCP error: {response['error']}")
                        return None

            # If we get here, log all responses for debugging
            self.logger.warning(f"No valid tool call response found for ID {expected_id}")
            self.logger.warning(f"Full stdout: {stdout}")
            self.logger.warning(f"Total stdout lines: {len(lines)}")
            for i, line in enumerate(lines[:10]):  # Log first 10 lines
                self.logger.warning(f"Line {i}: {line[:100]}...")
            return None

        except json.JSONDecodeError as e:
            self.logger.error(f"Failed to parse MCP response: {e}")
            self.logger.debug(f"Stdout that failed to parse: {stdout}")
            return None

    def _extract_continuation_id(self, response_text: str) -> Optional[str]:
        """Extract continuation_id from response metadata"""
        try:
            # Parse the response text as JSON to look for continuation metadata
            response_data = json.loads(response_text)

            # Look for continuation_id in various places
            if isinstance(response_data, dict):
                # Check for direct continuation_id field (new workflow tools)
                if "continuation_id" in response_data:
                    return response_data["continuation_id"]

                # Check metadata
                metadata = response_data.get("metadata", {})
                if "thread_id" in metadata:
                    return metadata["thread_id"]

                # Check follow_up_request
                follow_up = response_data.get("follow_up_request", {})
                if follow_up and "continuation_id" in follow_up:
                    return follow_up["continuation_id"]

                # Check continuation_offer
                continuation_offer = response_data.get("continuation_offer", {})
                if continuation_offer and "continuation_id" in continuation_offer:
                    return continuation_offer["continuation_id"]

            self.logger.debug(f"No continuation_id found in response: {response_data}")
            return None

        except json.JSONDecodeError as e:
            self.logger.debug(f"Failed to parse response for continuation_id: {e}")
            return None

    def run_command(self, cmd: list[str], check: bool = True, capture_output: bool = False, **kwargs):
        """Run a shell command with logging"""
        if self.verbose:
            self.logger.debug(f"Running: {' '.join(cmd)}")

        return subprocess.run(cmd, check=check, capture_output=capture_output, **kwargs)

    def create_additional_test_file(self, filename: str, content: str) -> str:
        """Create an additional test file for mixed scenario testing"""
        if not hasattr(self, "test_dir") or not self.test_dir:
            raise RuntimeError("Test directory not initialized. Call setup_test_files() first.")

        file_path = os.path.join(self.test_dir, filename)
        with open(file_path, "w") as f:
            f.write(content)
        # Return absolute path for MCP server compatibility
        return os.path.abspath(file_path)

    def cleanup_test_files(self):
        """Clean up test files"""
        if hasattr(self, "test_dir") and self.test_dir and os.path.exists(self.test_dir):
            import shutil

            shutil.rmtree(self.test_dir)
            self.logger.debug(f"Removed test files directory: {self.test_dir}")

    # ============================================================================
    # Log Utility Methods (delegate to LogUtils)
    # ============================================================================

    def get_server_logs_since(self, since_time: Optional[str] = None) -> str:
        """Get server logs from both main and activity log files."""
        return LogUtils.get_server_logs_since(since_time)

    def get_recent_server_logs(self, lines: int = 500) -> str:
        """Get recent server logs from the main log file."""
        return LogUtils.get_recent_server_logs(lines)

    def get_server_logs_subprocess(self, lines: int = 500) -> str:
        """Get server logs using subprocess (alternative method)."""
        return LogUtils.get_server_logs_subprocess(lines)

    def check_server_logs_for_errors(self, lines: int = 500) -> list[str]:
        """Check server logs for error messages."""
        return LogUtils.check_server_logs_for_errors(lines)

    def extract_conversation_usage_logs(self, logs: str) -> list[dict[str, int]]:
        """Extract token budget calculation information from logs."""
        return LogUtils.extract_conversation_usage_logs(logs)

    def extract_conversation_token_usage(self, logs: str) -> list[int]:
        """Extract conversation token usage values from logs."""
        return LogUtils.extract_conversation_token_usage(logs)

    def extract_thread_creation_logs(self, logs: str) -> list[dict[str, str]]:
        """Extract thread creation logs with parent relationships."""
        return LogUtils.extract_thread_creation_logs(logs)

    def extract_history_traversal_logs(self, logs: str) -> list[dict[str, any]]:
        """Extract conversation history traversal logs."""
        return LogUtils.extract_history_traversal_logs(logs)

    def validate_file_deduplication_in_logs(self, logs: str, tool_name: str, test_file: str) -> bool:
        """Validate that logs show file deduplication behavior."""
        return LogUtils.validate_file_deduplication_in_logs(logs, tool_name, test_file)

    def search_logs_for_pattern(
        self, pattern: str, logs: Optional[str] = None, case_sensitive: bool = False
    ) -> list[str]:
        """Search logs for a specific pattern."""
        return LogUtils.search_logs_for_pattern(pattern, logs, case_sensitive)

    def get_log_file_info(self) -> dict[str, dict[str, any]]:
        """Get information about log files."""
        return LogUtils.get_log_file_info()

    def run_test(self) -> bool:
        """Run the test - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement run_test()")

    @property
    def test_name(self) -> str:
        """Get the test name - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement test_name property")

    @property
    def test_description(self) -> str:
        """Get the test description - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement test_description property")
