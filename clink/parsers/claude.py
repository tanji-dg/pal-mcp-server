"""Parser for Claude CLI JSON output."""

from __future__ import annotations

import json
from typing import Any

from .base import BaseParser, ParsedCLIResponse, ParserError


class ClaudeJSONParser(BaseParser):
    """Parse stdout produced by `claude --output-format json`."""

    name = "claude_json"

    def parse(self, stdout: str, stderr: str) -> ParsedCLIResponse:
        if not stdout.strip():
            raise ParserError("Claude CLI returned empty stdout while JSON output was expected")

        # Try to parse as single JSON or list first (legacy/simple behavior)
        try:
            loaded = json.loads(stdout)
            is_stream = False
        except json.JSONDecodeError:
            # Failed to parse as single JSON, likely stream-json format (multiple objects)
            loaded = None
            is_stream = True

        payload = None
        events: list[dict[str, Any]] = []
        accumulated_content = []
        model_from_system = None

        if not is_stream and loaded is not None:
            # ... (existing non-stream logic)
            if isinstance(loaded, dict):
                payload = loaded
            elif isinstance(loaded, list):
                events = [item for item in loaded if isinstance(item, dict)]
                result_entry = next(
                    (item for item in events if item.get("type") == "result" or "result" in item),
                    None,
                )
                assistant_entry = next(
                    (item for item in reversed(events) if item.get("type") == "assistant"),
                    None,
                )
                payload = result_entry or assistant_entry or (events[-1] if events else {})
                if not payload:
                    raise ParserError("Claude CLI JSON array did not contain any parsable objects")
            else:
                raise ParserError("Claude CLI returned unexpected JSON payload")
        else:
            # Stream parsing logic
            for line in stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                    events.append(data)
                    
                    msg_type = data.get("type")
                    if msg_type == "message":
                        # Accumulate partial content (if --include-partial-messages is used)
                        content = data.get("content")
                        if content and isinstance(content, str):
                            accumulated_content.append(content)
                    elif msg_type == "stream_event":
                        # Handle stream chunks (content_block_delta)
                        event_data = data.get("event", {})
                        if event_data.get("type") == "content_block_delta":
                            delta = event_data.get("delta", {})
                            if delta.get("type") == "text_delta":
                                text = delta.get("text")
                                if text:
                                    accumulated_content.append(text)
                            elif delta.get("type") == "thinking_delta":
                                thinking = delta.get("thinking")
                                if thinking:
                                    # Optionally capture thinking, or just treat as content for now
                                    # Depending on desired output format
                                    pass 
                    elif msg_type == "assistant":
                        # Final message in stream
                        message_obj = data.get("message")
                        if isinstance(message_obj, dict):
                            content_list = message_obj.get("content")
                            if isinstance(content_list, list):
                                for item in content_list:
                                    if isinstance(item, dict) and item.get("type") == "text":
                                        text = item.get("text")
                                        if text:
                                            accumulated_content.append(text)
                            elif isinstance(content_list, str):
                                accumulated_content.append(content_list)
                    elif msg_type == "result":
                        payload = data
                    elif msg_type == "system":
                         # Capture model info from init
                         if data.get("subtype") == "init":
                             model_from_system = data.get("model")
                             if not payload:
                                 payload = data
                    elif msg_type == "error":
                         # Capture error as payload if no result yet
                         if not payload:
                             payload = data
                except json.JSONDecodeError:
                    continue
            
            if not payload and events:
                # If no explicit result, look for last useful event
                payload = events[-1]

        if not payload and not accumulated_content:
             raise ParserError("Failed to extract valid JSON payload from Claude CLI output")

        metadata = self._build_metadata(payload or {}, stderr, model_name=model_from_system)
        if events:
            metadata["raw_events"] = events
        if loaded is not None and not is_stream:
            metadata["raw"] = loaded

        # 1. Try explicit result field
        result = payload.get("result") if payload else None
        content: str = ""
        if isinstance(result, str):
            content = result.strip()
        elif isinstance(result, list):
             joined = [part.strip() for part in result if isinstance(part, str) and part.strip()]
             content = "\n".join(joined)
        
        # 2. If no result content, use accumulated stream content
        if not content and accumulated_content:
            content = "".join(accumulated_content).strip()

        if content:
            return ParsedCLIResponse(content=content, metadata=metadata)

        # 3. Fallback to message extraction
        message = self._extract_message(payload or {})
        if message:
            return ParsedCLIResponse(content=message, metadata=metadata)

        stderr_text = stderr.strip()
        if stderr_text:
            metadata.setdefault("stderr", stderr_text)
            return ParsedCLIResponse(
                content="Claude CLI returned no textual result. Raw stderr was preserved for troubleshooting.",
                metadata=metadata,
            )

        raise ParserError("Claude CLI response did not contain a textual result")

    def _build_metadata(self, payload: dict[str, Any], stderr: str, model_name: str | None = None) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "raw": payload,
            "is_error": bool(payload.get("is_error")),
        }
        
        # Check for top-level error object (common in API errors)
        if "error" in payload and isinstance(payload["error"], dict):
            metadata["is_error"] = True

        if model_name:
            metadata["model_used"] = model_name

        type_field = payload.get("type")
        if isinstance(type_field, str):
            metadata["type"] = type_field
        subtype_field = payload.get("subtype")
        if isinstance(subtype_field, str):
            metadata["subtype"] = subtype_field

        duration_ms = payload.get("duration_ms")
        if isinstance(duration_ms, (int, float)):
            metadata["duration_ms"] = duration_ms
        api_duration = payload.get("duration_api_ms")
        if isinstance(api_duration, (int, float)):
            metadata["duration_api_ms"] = api_duration

        usage = payload.get("usage")
        if isinstance(usage, dict):
            metadata["usage"] = usage

        model_usage = payload.get("modelUsage")
        if isinstance(model_usage, dict) and model_usage:
            metadata["model_usage"] = model_usage
            if "model_used" not in metadata:
                first_model = next(iter(model_usage.keys()))
                metadata["model_used"] = first_model

        # Also check direct model field often found in 'assistant' or 'message' events
        if "model_used" not in metadata:
            direct_model = payload.get("model") or (payload.get("message") or {}).get("model")
            if isinstance(direct_model, str):
                metadata["model_used"] = direct_model

        permission_denials = payload.get("permission_denials")
        if isinstance(permission_denials, list) and permission_denials:
            metadata["permission_denials"] = permission_denials

        session_id = payload.get("session_id")
        if isinstance(session_id, str) and session_id:
            metadata["session_id"] = session_id
        uuid_field = payload.get("uuid")
        if isinstance(uuid_field, str) and uuid_field:
            metadata["uuid"] = uuid_field

        stderr_text = stderr.strip()
        if stderr_text:
            metadata.setdefault("stderr", stderr_text)

        return metadata

    def _extract_message(self, payload: dict[str, Any]) -> str | None:
        message = payload.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()

        error_field = payload.get("error")
        if isinstance(error_field, dict):
            error_message = error_field.get("message") or "Unknown API error"
            
            # Look for retry delay info in details
            details = error_field.get("details", [])
            if isinstance(details, list):
                for detail in details:
                    if not isinstance(detail, dict):
                        continue
                    # Check for quota reset info
                    metadata = detail.get("metadata", {})
                    if isinstance(metadata, dict) and "quotaResetDelay" in metadata:
                        error_message += f" (Quota resets in {metadata['quotaResetDelay']})"
                    elif "retryDelay" in detail:
                        error_message += f" (Retry delay: {detail['retryDelay']})"
            
            return error_message.strip()

        return None
