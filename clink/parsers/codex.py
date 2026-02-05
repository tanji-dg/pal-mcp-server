"""Parser for Codex CLI JSONL output."""

from __future__ import annotations

import json
from typing import Any

from .base import BaseParser, ParsedCLIResponse, ParserError


class CodexJSONLParser(BaseParser):
    """Parse stdout emitted by `codex exec --json`."""

    name = "codex_jsonl"

    def parse(self, stdout: str, stderr: str) -> ParsedCLIResponse:
        lines = [line.strip() for line in (stdout or "").splitlines() if line.strip()]
        events: list[dict[str, Any]] = []
        agent_messages: list[str] = []
        errors: list[str] = []
        usage: dict[str, Any] | None = None
        metadata: dict[str, Any] = {}

        for line in lines:
            if not line.startswith("{"):
                continue
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue

            events.append(event)
            event_type = event.get("type")
            if event_type == "init":
                native_session_id = event.get("session_id")
                if native_session_id:
                    metadata["native_session_id"] = native_session_id
            elif event_type == "item.completed":
                item = event.get("item") or {}
                item_type = item.get("type")
                if item_type == "agent_message":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        agent_messages.append(text.strip())
                elif item_type == "reasoning":
                    text = item.get("text")
                    if isinstance(text, str) and text.strip():
                        # Use a list to keep track of thinking for the response object
                        if "thinking_parts" not in locals():
                            thinking_parts = []
                        thinking_parts.append(text.strip())
            elif event_type == "error" or event_type == "turn.failed":
                err_obj = event.get("error") if event_type == "turn.failed" else event
                if isinstance(err_obj, dict):
                    message = err_obj.get("message")
                    if isinstance(message, str) and message.strip():
                        errors.append(message.strip())
            elif event_type == "turn.completed":
                turn_usage = event.get("usage")
                if isinstance(turn_usage, dict):
                    usage = turn_usage

        if not agent_messages and errors:
            agent_messages.extend(errors)

        if not agent_messages:
            raise ParserError("Codex CLI JSONL output did not include an agent_message item")

        content = "\n\n".join(agent_messages).strip()
        thinking = "\n\n".join(locals().get("thinking_parts", [])).strip() or None
        
        metadata["events"] = events
        if errors:
            metadata["errors"] = errors
        if usage:
            metadata["usage"] = usage
        if stderr and stderr.strip():
            metadata["stderr"] = stderr.strip()

        return ParsedCLIResponse(content=content, metadata=metadata, thinking=thinking)
