"""Parser for Gemini CLI JSON output."""

from __future__ import annotations

import json
from typing import Any

from .base import BaseParser, ParsedCLIResponse, ParserError


class GeminiJSONParser(BaseParser):
    """Parse stdout produced by `gemini -o json`."""

    name = "gemini_json"

    def parse(self, stdout: str, stderr: str) -> ParsedCLIResponse:
        if not stdout.strip():
            raise ParserError("Gemini CLI returned empty stdout while JSON output was expected")

        # Check for authentication requirement
        if "accounts.google.com" in stdout or "authorize the application" in stdout:
            raise ParserError(
                "Gemini CLI requires authentication. Please run 'gemini prompt \"test\"' "
                "directly in your terminal to complete the login process."
            )

        # Handle multiple JSON objects (stream-json format)
        payload = None
        accumulated_response = []
        accumulated_thinking = []
        model_from_init = None

        for raw_line in stdout.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            # Handle cases where multiple JSON objects are concatenated in one line (e.g., }{)
            json_parts = line.replace("}{", "}\n{").split("\n")

            for part in json_parts:
                part = part.strip()
                if not (part.startswith("{") and part.endswith("}")):
                    continue
                try:
                    data = json.loads(part)
                    msg_type = data.get("type")

                    # Capture model from init event
                    if msg_type == "init":
                        model_from_init = data.get("model")

                    # Accumulate content from message events (stream-json chunks)
                    elif msg_type == "message":
                        # Be lenient with role names: assistant (standard), gemini/model (variants)
                        role = data.get("role")
                        if role in ("assistant", "gemini", "model"):
                            # Check multiple possible content fields: 'content' (standard), 'text' (variant)
                            content = data.get("content") or data.get("text")
                            if content:
                                accumulated_response.append(content)
                            
                            # Check for thought content
                            thought = data.get("thought")
                            if thought:
                                accumulated_thinking.append(thought)

                    # Update payload based on priority
                    if msg_type == "result":
                        payload = data
                    elif msg_type == "error" or ("error" in data and not msg_type):
                        if payload is None or payload.get("type") not in ("result",):
                            payload = data
                    elif not msg_type and ("response" in data or "text" in data):
                        if payload is None or payload.get("type") not in ("result", "error"):
                            payload = data
                    elif payload is None:
                        # Last resort: any valid JSON object
                        payload = data
                except json.JSONDecodeError:
                    continue

        if payload is None and not accumulated_response:
            # Fallback to older robust extraction if split/parse failed
            brace_index = stdout.find("{")
            if brace_index != -1:
                try:
                    payload = json.loads(stdout[brace_index:])
                except json.JSONDecodeError:
                    pass

        if payload is None and not accumulated_response:
            raise ParserError("Failed to extract valid JSON payload from Gemini CLI output")

        # Resolve final response text
        response_text = ""
        if payload:
            response = payload.get("response") or payload.get("text")
            if isinstance(response, str) and response.strip():
                response_text = response.strip()

        if not response_text and accumulated_response:
            response_text = "".join(accumulated_response).strip()

        metadata: dict[str, Any] = {"raw": payload or {}}
        if model_from_init:
            metadata["model_used"] = model_from_init

        if payload:
            # Mark as error if payload contains an error object
            if "error" in payload and isinstance(payload["error"], dict):
                metadata["is_error"] = True

            stats = payload.get("stats")
            if isinstance(stats, dict):
                metadata["stats"] = stats
                # Legacy stats format (SessionMetrics)
                models = stats.get("models")
                if isinstance(models, dict) and models:
                    model_name = next(iter(models.keys()))
                    metadata["model_used"] = model_name

                # New stream stats format (StreamStats)
                # Ensure model_used is set from init even if stats don't have model info
                if "total_tokens" in stats and "model_used" not in metadata and model_from_init:
                    metadata["model_used"] = model_from_init

        if response_text:
            if stderr and stderr.strip():
                metadata["stderr"] = stderr.strip()
            
            thinking_content = "".join(accumulated_thinking).strip() if accumulated_thinking else None
            return ParsedCLIResponse(content=response_text, metadata=metadata, thinking=thinking_content)

        fallback_message, extra_metadata = self._build_fallback_message(payload or {}, stderr)
        if fallback_message:
            metadata.update(extra_metadata)
            if stderr and stderr.strip():
                metadata["stderr"] = stderr.strip()
            return ParsedCLIResponse(content=fallback_message, metadata=metadata)

        raise ParserError("Gemini CLI response is missing a textual 'response' field")

    def _build_fallback_message(self, payload: dict[str, Any], stderr: str) -> tuple[str | None, dict[str, Any]]:
        """Derive a human friendly message when Gemini returns empty content."""

        stderr_text = stderr.strip() if stderr else ""
        stderr_lower = stderr_text.lower()
        extra_metadata: dict[str, Any] = {"empty_response": True}

        # Check for structured error first
        error_field = payload.get("error")
        if isinstance(error_field, dict):
            msg = error_field.get("message") or "Unknown API error"
            
            # Extract delay info if present (matches logic in coordinator/dashboard)
            delay_info = ""
            details = error_field.get("details", [])
            if isinstance(details, list):
                for detail in details:
                    if not isinstance(detail, dict):
                        continue
                    metadata = detail.get("metadata", {})
                    if isinstance(metadata, dict) and "quotaResetDelay" in metadata:
                        delay_info = f" (Quota resets in {metadata['quotaResetDelay']})"
                    elif "retryDelay" in detail:
                        delay_info = f" (Retry delay: {detail['retryDelay']})"
            
            return f"{msg}{delay_info}", extra_metadata

        if "429" in stderr_lower or "rate limit" in stderr_lower:
            extra_metadata["rate_limit_status"] = 429
            message = (
                "Gemini request returned no content because the API reported a 429 rate limit. "
                "Retry after reducing the request size or waiting for quota to replenish."
            )
            return message, extra_metadata

        stats = payload.get("stats")
        if isinstance(stats, dict):
            models = stats.get("models")
            if isinstance(models, dict) and models:
                first_model = next(iter(models.values()))
                if isinstance(first_model, dict):
                    api_stats = first_model.get("api")
                    if isinstance(api_stats, dict):
                        total_errors = api_stats.get("totalErrors")
                        total_requests = api_stats.get("totalRequests")
                        if isinstance(total_errors, int) and total_errors > 0:
                            extra_metadata["api_total_errors"] = total_errors
                            if isinstance(total_requests, int):
                                extra_metadata["api_total_requests"] = total_requests
                            message = (
                                "Gemini CLI returned no textual output. The API reported "
                                f"{total_errors} error(s); see stderr for details."
                            )
                            return message, extra_metadata

        if stderr_text:
            message = "Gemini CLI returned no textual output. Raw stderr was preserved for troubleshooting."
            return message, extra_metadata

        return None, extra_metadata
