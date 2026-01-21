"""Unit tests for ClaudeJSONParser streaming support."""

import json
import pytest
from clink.parsers.base import ParserError
from clink.parsers.claude import ClaudeJSONParser

def test_claude_parser_handles_stream_deltas():
    """Verify that the parser correctly reconstructs content from stream_event deltas."""
    parser = ClaudeJSONParser()
    
    # Simulate a sequence of JSON objects from Claude CLI stream
    events = [
        {"type": "system", "subtype": "init", "model": "claude-3-5-sonnet"},
        {
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "Hello"}
            }
        },
        {
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": " world"}
            }
        },
        {
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "!"}
            }
        },
        {
            "type": "result",
            "status": "success",
            "usage": {"input_tokens": 100, "output_tokens": 50}
        }
    ]
    
    stdout = "\n".join(json.dumps(e) for e in events)
    
    parsed = parser.parse(stdout=stdout, stderr="")
    
    # Verify content reconstruction
    assert parsed.content == "Hello world!"
    
    # Verify metadata extraction from multiple events
    assert parsed.metadata["model_used"] == "claude-3-5-sonnet"
    assert parsed.metadata["usage"]["input_tokens"] == 100
    assert parsed.metadata["usage"]["output_tokens"] == 50

def test_claude_parser_handles_mixed_thinking_and_text_deltas():
    """Verify that text deltas are extracted even when mixed with thinking deltas."""
    parser = ClaudeJSONParser()
    
    events = [
        {
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "thinking_delta", "thinking": "I should say hello."}
            }
        },
        {
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "index": 1,
                "delta": {"type": "text_delta", "text": "Hello!"}
            }
        }
    ]
    
    stdout = "\n".join(json.dumps(e) for e in events)
    
    parsed = parser.parse(stdout=stdout, stderr="")
    
    # Text deltas should be in content
    assert parsed.content == "Hello!"
    # Thinking deltas should be in thinking field
    assert parsed.thinking == "I should say hello."

def test_claude_parser_raises_error_if_no_content():
    """Verify that the parser raises ParserError if no text content can be extracted."""
    parser = ClaudeJSONParser()
    
    # Case where there are events but none contain text content
    events = [
        {"type": "system", "subtype": "init", "model": "claude-3-5-sonnet"},
        {"type": "other_event", "data": "value"}
    ]
    
    stdout = "\n".join(json.dumps(e) for e in events)
    
    with pytest.raises(ParserError, match="Claude CLI response did not contain a textual result"):
        parser.parse(stdout=stdout, stderr="")
