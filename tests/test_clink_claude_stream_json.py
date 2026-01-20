"""Test Claude streaming JSON parsing."""

import json
import pytest
from clink.parsers.claude import ClaudeJSONParser
from clink.parsers.base import ParserError

STREAM_OUTPUT = """{"type":"system","subtype":"init","model":"claude-3-5-sonnet"}
{"type":"message","role":"assistant","content":"Thinking","delta":true}
{"type":"message","role":"assistant","content":" about life.","delta":true}
{"type":"assistant","message":{"content":[{"type":"text","text":"Thinking about life."}]}}
{"type":"result","status":"success","result":"Thinking about life."}
"""

def test_parse_stream_output():
    parser = ClaudeJSONParser()
    response = parser.parse(STREAM_OUTPUT, "")
    assert response.content == "Thinking about life."
    assert response.metadata["model_used"] == "claude-3-5-sonnet"

def test_parse_stream_output_partial():
    """Test parsing stream output without final result."""
    output = """{"type":"message","role":"assistant","content":"Thinking"}
{"type":"message","role":"assistant","content":"..."}"""
    parser = ClaudeJSONParser()
    response = parser.parse(output, "")
    assert response.content == "Thinking..."
