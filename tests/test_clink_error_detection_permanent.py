"""
Permanent unit tests for clink fatal error detection logic.
Ensures that specific log patterns from Gemini/Claude CLIs trigger termination.
"""

import pytest
from clink.constants import FATAL_ERROR_KEYWORDS

def check_fatal_error(line: str) -> bool:
    """Simulation of the check logic in BaseCLIAgent._read_stream."""
    return any(keyword in line for keyword in FATAL_ERROR_KEYWORDS)

class TestClinkErrorDetectionPermanent:
    
    def test_gemini_resource_exhausted(self):
        # Actual log from production
        log = 'Attempt 2 failed with status 429. Retrying with backoff... ApiError: {"error":{"message":"{\n  \"error\": {\n    \"code\": 429,\n    \"message\": \"Resource has been exhausted (e.g. check quota).\",\n    \"status\": \"RESOURCE_EXHAUSTED\"\n  }\n}\n","code":429,"status":"Too Many Requests"}}'
        assert check_fatal_error(log) is True

    def test_claude_quota_exhausted(self):
        # Claude style error
        log = '{"error": {"message": "You have exhausted your capacity on this model. Your quota will reset after 1h15m13s.", "code": 429}}'
        assert check_fatal_error(log) is True

    def test_claude_quota_reset_metadata(self):
        # Metadata field often present in Claude errors
        log = '"quotaResetDelay": "1h13m59.891660529s"'
        assert check_fatal_error(log) is True

    def test_temporary_rate_limit_ignored(self):
        # Temporary limits (short wait) should NOT be fatal if not containing fatal keywords
        # Note: If "Rate limited" is NOT in FATAL_ERROR_KEYWORDS, this passes.
        # "Rate limited" implies wait, not necessarily fatal unless it says "quota reset"
        log = "[AccountManager] Rate limited: user@example.com. Available in 10s"
        assert check_fatal_error(log) is False

    def test_permission_denied(self):
        log = "Error: Permission denied (public key)."
        assert check_fatal_error(log) is True
