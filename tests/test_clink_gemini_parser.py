"""Tests for the Gemini CLI JSON parser."""

import pytest

from clink.parsers.gemini import GeminiJSONParser, ParserError


def _build_rate_limit_stdout() -> str:
    return (
        "{\n"
        '  "response": "",\n'
        '  "stats": {\n'
        '    "models": {\n'
        '      "gemini-2.5-pro": {\n'
        '        "api": {\n'
        '          "totalRequests": 5,\n'
        '          "totalErrors": 5,\n'
        '          "totalLatencyMs": 13319\n'
        "        },\n"
        '        "tokens": {"prompt": 0, "candidates": 0, "total": 0, "cached": 0, "thoughts": 0, "tool": 0}\n'
        "      }\n"
        "    },\n"
        '    "tools": {"totalCalls": 0},\n'
        '    "files": {"totalLinesAdded": 0, "totalLinesRemoved": 0}\n'
        "  }\n"
        "}"
    )


def test_gemini_parser_handles_rate_limit_empty_response():
    parser = GeminiJSONParser()
    stdout = _build_rate_limit_stdout()
    stderr = "Attempt 1 failed with status 429. Retrying with backoff... ApiError: quota exceeded"

    parsed = parser.parse(stdout, stderr)

    assert "429" in parsed.content
    assert parsed.metadata.get("rate_limit_status") == 429
    assert parsed.metadata.get("empty_response") is True
    assert "Attempt 1 failed" in parsed.metadata.get("stderr", "")


def test_gemini_parser_extracts_legacy_usage():
    """Verify that tokens are extracted from legacy models stats."""
    parser = GeminiJSONParser()
    stdout = (
        '{"response": "Old way", "stats": {'
        '"models": {"gemini-pro": {"tokens": {"prompt": 10, "candidates": 5, "total": 15}}}}}'
    )
    
    parsed = parser.parse(stdout, stderr="")
    
    assert parsed.content == "Old way"
    assert parsed.metadata["usage"]["input_tokens"] == 10
    assert parsed.metadata["usage"]["output_tokens"] == 5
    assert parsed.metadata["usage"]["total_tokens"] == 15


def test_gemini_parser_extracts_stream_usage():
    """Verify that input/output tokens are extracted from modern stream stats."""
    parser = GeminiJSONParser()
    stdout = (
        '{"type":"init","model":"gemini-2.0-flash"}\n'
        '{"type":"message","role":"assistant","content":"Hello","delta":true}\n'
        '{"type":"result","status":"success","response":"Hello","stats":{'
        '"input_tokens":123,"output_tokens":45,"total_tokens":168}}'
    )
    
    parsed = parser.parse(stdout, stderr="")
    
    assert parsed.content == "Hello"
    assert parsed.metadata["model_used"] == "gemini-2.0-flash"
    assert parsed.metadata["usage"]["input_tokens"] == 123
    assert parsed.metadata["usage"]["output_tokens"] == 45
    assert parsed.metadata["usage"]["total_tokens"] == 168
