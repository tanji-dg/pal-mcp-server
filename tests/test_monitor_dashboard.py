"""
Unit tests for Monitor Dashboard log parsing logic.
These tests simulate the JavaScript logic in dashboard.html to ensure correctness
and prevent regressions in log processing for Terminal View.
"""

import json
import pytest

def parse_log_content_simulated(raw):
    """
    Python implementation of the dashboard.html parseLogContent function.
    This MUST be kept in sync with the JavaScript implementation.
    """
    if not raw:
        return {'type': 'unknown', 'text': '', 'isDelta': False}
    
    try:
        if raw.strip().startswith('{'):
            data = json.loads(raw)
            was_stream_event = False
            
            # Unwrap Claude stream_event wrapper
            if data.get('type') == 'stream_event' and data.get('event'):
                data = data['event']
                was_stream_event = True
            
            # Ignore internal stream/control events
            if data.get('type') in ['stream', 'ping', 'user']:
                return None

            # Ignore Claude stream lifecycle events
            ignored_claude_events = [
                'message_start', 'content_block_start', 
                'message_delta', 'message_stop', 'content_block_stop'
            ]
            if data.get('type') in ignored_claude_events:
                return None
            
            # 1. Standard Message / Assistant (Claude/Gemini)
            if data.get('type') in ['message', 'assistant']:
                content = data.get('content') or data.get('thought')
                
                # Handle Claude message object (nested in 'message' field)
                if data.get('message') and data['message'].get('content'):
                     content = data['message']['content']
                
                # Handle Claude content list
                if isinstance(content, list):
                    # Check for tool_use in content list
                    tool_use = next((c for c in content if isinstance(c, dict) and c.get('type') == 'tool_use'), None)
                    if tool_use:
                        name = tool_use.get('name') or 'tool'
                        return {'type': 'tool-use', 'text': f'> Executing {name}...', 'isDelta': False}

                    # Filter text
                    text_parts = []
                    for c in content:
                        if isinstance(c, dict) and c.get('type') == 'text':
                            text_parts.append(c.get('text', ''))
                        elif isinstance(c, str):
                            text_parts.append(c)
                    content = ''.join(text_parts)
                
                # Check for delta flag or if it looks like a partial chunk
                is_delta = data.get('delta') is True or data.get('type') == 'message'
                
                if content and isinstance(content, str):
                    return {'type': 'thinking', 'text': content, 'isDelta': is_delta}

            # 2. Claude specific: content_block_delta
            if data.get('type') == 'content_block_delta' and data.get('delta'):
                if data['delta'].get('text'):
                    return {'type': 'thinking', 'text': data['delta']['text'], 'isDelta': True}
                if data['delta'].get('type') == 'input_json_delta':
                    return None
            
            # 3. Error (Top level)
            if data.get('type') == 'error':
                msg = data.get('message') or (data.get('error') or {}).get('message') or 'unknown error'
                return {'type': 'error', 'text': f'! Error: {msg}', 'isDelta': False}
            
            # 4. Tool Use
            if data.get('type') == 'tool_use':
                name = data.get('name') or data.get('tool_name') or 'tool'
                return {'type': 'tool-use', 'text': f'> Executing {name}...', 'isDelta': False}
            
            # 5. Tool Result
            if data.get('type') == 'tool_result':
                is_error = data.get('status') == 'error' or data.get('is_error')
                text = data.get('content') or data.get('output') or ''
                if isinstance(text, list):
                    text = ' '.join([str(t) for t in text])
                if not isinstance(text, str):
                    text = json.dumps(text)
                if len(text) > 300:
                    text = text[:300] + '...'
                return {'type': 'error' if is_error else 'tool-result', 'text': f'< {text}', 'isDelta': False}

            # 6. Final Result
            if data.get('type') == 'result':
                result_text = ''
                if data.get('result'):
                    if isinstance(data['result'], str):
                        result_text = data['result']
                    elif isinstance(data['result'], list):
                        result_text = '\n'.join(data['result'])
                
                if result_text and len(result_text) < 100:
                    return {'type': 'system', 'text': f'> Finished: {result_text}', 'isDelta': False}
                
                cost = data.get('total_cost_usd')
                cost_str = f' (Cost: ${cost:.4f})' if isinstance(cost, (int, float)) and cost > 0 else ''
                return {'type': 'system', 'text': f'> Finished{cost_str}', 'isDelta': False}

            # 7. Initialization
            if data.get('type') == 'init' or (data.get('type') == 'system' and data.get('subtype') == 'init'):
                model = data.get('model') or 'unknown model'
                return {'type': 'system', 'text': f'> Initialized (Model: {model})', 'isDelta': False}

            # 8. Codex Events
            if data.get('type') == 'turn.completed':
                return None

            if data.get('type') in ['item.started', 'item.completed']:
                item = data.get('item', {})
                item_type = item.get('type')
                if item_type == 'reasoning':
                    if data.get('type') == 'item.started' and item.get('text'):
                        return {'type': 'thinking', 'text': item.get('text'), 'isDelta': True}
                    return None
                if item_type == 'message':
                    return None
                if item_type == 'command_execution':
                    if data.get('type') == 'item.started':
                        return {'type': 'tool-use', 'text': f'> Executing {item.get('command')}...', 'isDelta': False}
                    if data.get('type') == 'item.completed':
                        is_error = item.get('status') in ['failed', 'error']
                        if is_error:
                            return {'type': 'error', 'text': f'! Command failed: {item.get('command')}', 'isDelta': False}
                        return {'type': 'tool-result', 'text': f'< Command finished: {item.get('command')}', 'isDelta': False}

            # Final safety for stream events
            if was_stream_event:
                return None

    except json.JSONDecodeError:
        pass
    
    return {'type': 'raw', 'text': raw, 'isDelta': False}


class TestMonitorDashboardLogic:
    """Comprehensive tests for dashboard parsing logic."""

    def test_thinking_delta(self):
        # Gemini style
        raw = '{"type":"message","role":"assistant","content":"Thinking","delta":true}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'thinking'
        assert res['text'] == 'Thinking'
        assert res['isDelta'] is True

    def test_claude_content_block_delta(self):
        # Claude stream chunk
        raw = '{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'thinking'
        assert res['text'] == 'Hello'
        assert res['isDelta'] is True

    def test_claude_tool_use_in_assistant(self):
        # Tool call wrapped in assistant message
        raw = json.dumps({
            "type": "assistant",
            "message": {
                "content": [{"type": "tool_use", "name": "Bash", "input": {}}]
            }
        })
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'tool-use'
        assert res['text'] == '> Executing Bash...'

    def test_ignored_events(self):
        # Ensure noise is filtered
        assert parse_log_content_simulated('{"type":"ping"}') is None
        assert parse_log_content_simulated('{"type":"user"}') is None
        assert parse_log_content_simulated('{"type":"stream_event","event":{"type":"message_start"}}') is None
        assert parse_log_content_simulated('{"type":"content_block_delta","delta":{"type":"input_json_delta"}}') is None

    def test_top_level_error(self):
        raw = '{"type":"error","message":"Loop detected"}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'error'
        assert "! Error: Loop detected" in res['text']

    def test_initialization(self):
        raw = '{"type":"init","model":"gemini-pro"}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'system'
        assert "gemini-pro" in res['text']

    def test_codex_events(self):
        # Reasoning
        raw = '{"type":"item.started","item":{"type":"reasoning","text":"Plan..."}}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'thinking'
        assert res['text'] == 'Plan...'
        
        # Command success
        raw = '{"type":"item.completed","item":{"type":"command_execution","command":"ls","status":"success"}}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'tool-result'
        assert "ls" in res['text']

    def test_raw_fallback(self):
        # Non-JSON or unknown top-level JSON
        assert parse_log_content_simulated("Plain text")['type'] == 'raw'
        assert parse_log_content_simulated('{"type":"completely_unknown"}')['type'] == 'raw'
