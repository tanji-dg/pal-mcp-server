
import json
import pytest
import re

def parse_log_content_simulated(raw):
    if not raw:
        return None
    
    display_data = {'type': 'raw', 'text': raw, 'isDelta': False}

    clean_raw = raw.strip()
    if not clean_raw.startswith('{'):
        # Fallback for non-JSON
        lower = raw.lower()
        if any(x in raw for x in ['❌', '⚠️']) or 'error' in lower or 'fail' in lower: display_data['type'] = 'error'
        elif '🧠' in raw: display_data['type'] = 'thinking'
        elif '🛠️' in raw: display_data['type'] = 'tool-use'
        elif '✅' in raw: display_data['type'] = 'tool-result'
        return display_data

    try:
        parts = clean_raw.replace('}{', '}\n{').split('\n')
        results = []
        final_is_delta = False
        primary_type = None
        any_handled = False

        type_to_icon = {
            'message': '🧠', 'assistant': '🧠', 'thinking': '🧠',
            'user': '👤', 'tool_use': '🛠️', 'tool_result': '✅',
            'init': '🚀', 'result': '🏁', 'turn.failed': '❌', 'error': '❌'
        }

        def extract(obj):
            if not obj: return ""
            if isinstance(obj, str): return obj
            if isinstance(obj, list): return "".join(extract(x) for x in obj)
            
            if isinstance(obj, dict):
                if obj.get("type") == "tool_result":
                    icon = '❌' if (obj.get("is_error") or obj.get("status") == "error") else '✅'
                    return f"{icon} Result: {extract(obj.get('content') or obj.get('output') or obj.get('result')) or 'done'}"
                
                if obj.get("type") == "thinking" and obj.get("thinking") is not None: return obj["thinking"] or "..."
                if obj.get("type") == "text" and obj.get("text") is not None: return obj["text"] or "..."
                if obj.get("type") == "thinking_delta" and obj.get("thinking") is not None: return obj["thinking"]
                if obj.get("type") == "text_delta" and obj.get("text") is not None: return obj["text"]
                
                if obj.get("type") == "init": return obj.get("model") or "..."
                if obj.get("type") == "result": return obj.get("status") or "..."
                if obj.get("type") == "tool_use": return obj.get("name") or obj.get("tool_name") or "..."
                
                # Claude stream lifecycle events
                if obj.get("type") in ['message_start', 'content_block_start', 'message_delta', 'message_stop', 'content_block_stop']:
                    return "✓" if "stop" in obj["type"] else ""

                for k in ['content', 'thought', 'thinking', 'text', 'message', 'event', 'delta', 'result', 'model']:
                    if obj.get(k):
                        val = extract(obj[k])
                        if val != "": return val
            return ""

        for part in parts:
            try:
                data = json.loads(part)
                if primary_type is None: primary_type = data.get("type")
                any_handled = True
                
                is_delta = data.get("delta") is True or data.get("type") == 'message' or \
                           (data.get("event") and data["event"].get("type") == 'content_block_delta')
                if is_delta: final_is_delta = True

                text = extract(data)
                if text is not None:
                    msg_type = data.get("type", "")
                    icon = type_to_icon.get(msg_type, "")
                    if data.get("event", {}).get("content_block", {}).get("type") == 'tool_use': icon = '🛠️'
                    if data.get("event", {}).get("content_block", {}).get("type") == 'thinking': icon = '🧠'
                    
                    icons = list(type_to_icon.values()) + ['🧠', '🛠️']
                    has_icon = any(text.startswith(i) for i in icons)
                    
                    if not is_delta and icon and not has_icon and text != "" and text != "✓":
                        formatted_type = msg_type[0].upper() + msg_type[1:].replace('_', ' ')
                        results.append(f"{icon} {formatted_type}: {text}")
                    else:
                        results.append(text)
            except json.JSONDecodeError:
                continue

        if any_handled:
            display_data['text'] = "".join(results)
            display_data['isDelta'] = final_is_delta
            
            txt = display_data['text']
            t = primary_type or 'raw'
            if '🧠' in txt or any(x in t for x in ['think', 'assistant', 'message', 'stream']):
                display_data['type'] = 'thinking'
            elif '🛠️' in txt or 'tool_use' in t:
                display_data['type'] = 'tool-use'
            elif any(x in txt for x in ['✅', '❌']) or 'tool_result' in t or t == 'user':
                display_data['type'] = 'error' if ('❌' in txt or 'error' in t) else 'tool-result'
            else:
                display_data['type'] = 'system'
            
            if display_data['text'] == "" and not final_is_delta: return None
            if display_data['text'] == "✓": return None
            return display_data

    except Exception:
        pass
    
    return display_data


class TestMonitorDashboardLogic:
    def test_result_finished(self):
        raw = json.dumps({"type": "result", "status": "ok"})
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'system'
        assert '🏁 Result: ok' in res['text']

    def test_concatenated_json(self):
        raw = '{"type":"message","content":"Think","delta":true}{"type":"message","content":"ing","delta":true}'
        res = parse_log_content_simulated(raw)
        assert res['text'] == 'Thinking'
        assert res['isDelta'] is True
        assert res['type'] == 'thinking'

    def test_thinking_delta(self):
        raw = '{"type":"message","role":"assistant","content":"Thinking","delta":true}'
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'thinking'
        assert res['text'] == 'Thinking'
        assert res['isDelta'] is True

    def test_claude_thinking_delta(self):
        raw = json.dumps({
            "type": "stream_event",
            "event": {
                "type": "content_block_delta",
                "delta": {"type": "thinking_delta", "thinking": "reasoning step"}
            }
        })
        res = parse_log_content_simulated(raw)
        assert res['type'] == 'thinking'
        assert res['text'] == 'reasoning step'
        assert res['isDelta'] is True

    def test_claude_thinking_start(self):
        raw = '{"type":"stream_event","event":{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}}'
        res = parse_log_content_simulated(raw)
        assert res is None

    def test_claude_message_stop(self):
        raw = '{"type":"stream_event","event":{"type":"message_stop"}}'
        res = parse_log_content_simulated(raw)
        # Should be fully silenced
        assert res is None

    def test_user_message_tool_result(self):
        raw = json.dumps({
            "type": "user",
            "message": {
                "role": "user",
                "content": [{
                    "type": "tool_result",
                    "tool_use_id": "toolu_1",
                    "content": "Success content"
                }]
            }
        })
        res = parse_log_content_simulated(raw)
        # JS logic: if msgType is user and extractor already added an icon (✅ Result:), 
        # it doesn't add '👤 User: ' prefix.
        assert "Result: Success content" in res['text']
        assert res['type'] == 'tool-result'

    def test_assistant_content_list(self):
        raw = json.dumps({
            "type": "assistant",
            "message": {
                "content": [
                    {"type": "text", "text": "Hello "},
                    {"type": "text", "text": "world"}
                ]
            }
        })
        res = parse_log_content_simulated(raw)
        assert "🧠 Assistant: Hello world" in res['text']
        assert res['type'] == 'thinking'

    def test_tool_use_beautification(self):
        raw = '{"type":"tool_use","name":"read_file"}'
        res = parse_log_content_simulated(raw)
        assert "🛠️ Tool use: read_file" in res['text']
        assert res['type'] == 'tool-use'

    def test_tool_result_beautification(self):
        raw = '{"type":"tool_result","tool_id":"toolu_1","content":"done"}'
        res = parse_log_content_simulated(raw)
        assert "✅ Result: done" in res['text']
        assert res['type'] == 'tool-result'

    def test_initialization(self):
        raw = '{"type":"init","model":"gemini-pro"}'
        res = parse_log_content_simulated(raw)
        assert "🚀 Init: gemini-pro" in res['text']
        assert res['type'] == 'system'

    def test_raw_fallback(self):
        res = parse_log_content_simulated("Plain text")
        assert res['type'] == 'raw'
        assert res['text'] == "Plain text"
