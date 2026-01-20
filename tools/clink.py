"""clink tool - bridge PAL MCP requests to external AI CLIs."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mcp.types import TextContent
from pydantic import BaseModel, Field

from clink import get_registry
from clink.agents import AgentOutput, CLIAgentError, create_agent
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from config import TEMPERATURE_BALANCED
from monitor.publisher import get_publisher
from tools.models import ToolModelCategory, ToolOutput
from tools.shared.base_models import COMMON_FIELD_DESCRIPTIONS
from tools.shared.exceptions import ToolExecutionError
from tools.simple.base import SchemaBuilder, SimpleTool

logger = logging.getLogger(__name__)

MAX_RESPONSE_CHARS = 20_000
SUMMARY_PATTERN = re.compile(r"<SUMMARY>(.*?)</SUMMARY>", re.IGNORECASE | re.DOTALL)


class CLinkRequest(BaseModel):
    """Request model for clink tool."""

    prompt: str = Field(..., description="Prompt forwarded to the target CLI.")
    cli_name: str | None = Field(
        default=None,
        description="Configured CLI client name to invoke. Defaults to the first configured CLI if omitted.",
    )
    role: str | None = Field(
        default=None,
        description="Optional role preset defined in the CLI configuration (defaults to 'default').",
    )
    absolute_file_paths: list[str] = Field(
        default_factory=list,
        description=COMMON_FIELD_DESCRIPTIONS["absolute_file_paths"],
    )
    images: list[str] = Field(
        default_factory=list,
        description=COMMON_FIELD_DESCRIPTIONS["images"],
    )
    continuation_id: str | None = Field(
        default=None,
        description=COMMON_FIELD_DESCRIPTIONS["continuation_id"],
    )


class CLinkTool(SimpleTool):
    """Bridge MCP requests to configured CLI agents.

    Schema metadata is cached at construction time and execution relies on the shared
    SimpleTool hooks for conversation memory. Prompt preparation is customised so we
    pass instructions and file references suitable for another CLI agent.
    """

    def __init__(self) -> None:
        # Cache registry metadata so the schema surfaces concrete enum values.
        self._registry = get_registry()
        self._cli_names = self._registry.list_clients()
        self._role_map: dict[str, list[str]] = {name: self._registry.list_roles(name) for name in self._cli_names}
        self._all_roles: list[str] = sorted({role for roles in self._role_map.values() for role in roles})
        if "gemini" in self._cli_names:
            self._default_cli_name = "gemini"
        else:
            self._default_cli_name = self._cli_names[0] if self._cli_names else None
        self._active_system_prompt: str = ""
        super().__init__()

    def get_name(self) -> str:
        return "clink"

    def get_description(self) -> str:
        return (
            "Link a request to an external AI CLI (Gemini CLI, Qwen CLI, etc.) through PAL MCP to reuse "
            "their capabilities inside existing workflows."
        )

    def get_annotations(self) -> dict[str, Any]:
        return {"readOnlyHint": True}

    def requires_model(self) -> bool:
        return False

    def get_model_category(self) -> ToolModelCategory:
        return ToolModelCategory.BALANCED

    def get_default_temperature(self) -> float:
        return TEMPERATURE_BALANCED

    def get_system_prompt(self) -> str:
        return self._active_system_prompt or ""

    def get_request_model(self):
        return CLinkRequest

    def get_input_schema(self) -> dict[str, Any]:
        # Surface configured CLI names and roles directly in the schema so MCP clients
        # (and downstream agents) can discover available options without consulting
        # a separate registry call.
        role_descriptions = []
        for name in self._cli_names:
            roles = ", ".join(sorted(self._role_map.get(name, ["default"]))) or "default"
            role_descriptions.append(f"{name}: {roles}")

        if role_descriptions:
            cli_available = ", ".join(self._cli_names) if self._cli_names else "(none configured)"
            default_text = (
                f" Default: {self._default_cli_name}." if self._default_cli_name and len(self._cli_names) <= 1 else ""
            )
            cli_description = (
                "Configured CLI client name (from conf/cli_clients). Available: " + cli_available + default_text
            )
            role_description = (
                "Optional role preset defined for the selected CLI (defaults to 'default'). Roles per CLI: "
                + "; ".join(role_descriptions)
            )
        else:
            cli_description = "Configured CLI client name (from conf/cli_clients)."
            role_description = "Optional role preset defined for the selected CLI (defaults to 'default')."

        properties = {
            "prompt": {
                "type": "string",
                "description": "User request forwarded to the CLI (conversation context is pre-applied).",
            },
            "cli_name": {
                "type": "string",
                "enum": self._cli_names,
                "description": cli_description,
            },
            "role": {
                "type": "string",
                "enum": self._all_roles or ["default"],
                "description": role_description,
            },
            "absolute_file_paths": SchemaBuilder.SIMPLE_FIELD_SCHEMAS["absolute_file_paths"],
            "images": SchemaBuilder.COMMON_FIELD_SCHEMAS["images"],
            "continuation_id": SchemaBuilder.COMMON_FIELD_SCHEMAS["continuation_id"],
        }

        schema = {
            "type": "object",
            "properties": properties,
            "required": ["prompt"],
            "additionalProperties": False,
        }

        if len(self._cli_names) > 1:
            schema["required"].append("cli_name")

        return schema

    def get_tool_fields(self) -> dict[str, dict[str, Any]]:
        """Unused by clink because we override the schema end-to-end."""
        return {}

    async def execute(self, arguments: dict[str, Any]) -> list[TextContent]:
        logger.debug(f"CLinkTool.execute started with keys: {list(arguments.keys())}")
        self._current_arguments = arguments
        request = self.get_request_model()(**arguments)

        path_error = self._validate_file_paths(request)
        if path_error:
            self._raise_tool_error(path_error)

        selected_cli = request.cli_name or self._default_cli_name
        if not selected_cli:
            self._raise_tool_error("No CLI clients are configured for clink.")

        try:
            client_config = self._registry.get_client(selected_cli)
        except KeyError as exc:
            self._raise_tool_error(str(exc))

        try:
            role_config = client_config.get_role(request.role)
        except KeyError as exc:
            self._raise_tool_error(str(exc))

        absolute_file_paths = self.get_request_files(request)
        images = self.get_request_images(request)
        continuation_id = self.get_request_continuation_id(request)

        self._model_context = arguments.get("_model_context")

        system_prompt_text = role_config.prompt_path.read_text(encoding="utf-8")
        include_system_prompt = not self._use_external_system_prompt(client_config)

        try:
            prompt_text = await self._prepare_prompt_for_role(
                request,
                role_config,
                system_prompt=system_prompt_text,
                include_system_prompt=include_system_prompt,
            )
        except Exception as exc:
            logger.exception("Failed to prepare clink prompt")
            self._raise_tool_error(f"Failed to prepare prompt: {exc}")

        # Prepare output callback for real-time notifications
        request_context = arguments.get("_request_context")

        # Track last notification to avoid spamming the UI
        state = {"last_msg": "", "last_time": 0.0}
        # Mapping from tool_id to tool_name for Gemini stream-json events
        tool_names: dict[str, str] = {}
        MIN_INTERVAL = 0.0  # Disable rate-limiting for maximum responsiveness

        # ANSI escape sequence pattern for stripping color codes
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

        # Determine effective session ID for monitor display
        effective_session_id = continuation_id or arguments.get("_instance_id") or "standalone"

        async def _notification_callback(line: str):
            logger.debug(f"CLINK NOTIFICATION RAW LINE: {line.strip()}")
            # Stream raw output to monitor if enabled
            try:
                publisher = get_publisher()
                if publisher:
                    await publisher.tool_log(client_config.name, line, session_id=effective_session_id)
            except Exception:
                pass

            if not request_context:
                logger.debug(f"CLI RAW (no context): [{client_config.name}] [Session: {effective_session_id[:8]}] {line.strip()}")
                return

            # Subprocesses might flush multiple lines at once in a single buffer chunk.
            # Split and process each non-empty line to ensure real-time responsiveness.
            lines = line.splitlines()
            for raw_msg in lines:
                msg = raw_msg.strip()
                if not msg:
                    continue

                # Always log raw output to server debug log for diagnostic purposes
                logger.debug(f"CLI RAW: [{client_config.name}] {msg}")

                try:
                    content = None
                    # 1. JSON format handling (gemini and codex)
                    if msg.startswith("{") and msg.endswith("}"):
                        try:
                            data = json.loads(msg)
                            msg_type = data.get("type")

                            # Gemini stream-json events
                            if msg_type == "message":
                                role = data.get("role")
                                # Support both standard content and thought parts
                                payload_content = data.get("content") or data.get("thought")
                                if role == "assistant" and payload_content:
                                    # Only notify about thinking/progress messages, not final answers
                                    # For Gemini: check for 'delta' flag
                                    # For Claude: all stream-json message chunks are deltas
                                    if data.get("delta") is True or client_config.name == "claude":
                                        content = f"🧠 Thinking: {payload_content}"
                            elif msg_type == "tool_use":
                                name = data.get("tool_name")
                                tool_id = data.get("tool_id")
                                if tool_id and name:
                                    tool_names[tool_id] = name
                                content = f"🛠️ Executing: {name or 'tool'}"
                            elif msg_type == "tool_result":
                                tool_id = data.get("tool_id")
                                status = data.get("status")
                                name = tool_names.get(tool_id) if tool_id else None
                                if status == "success":
                                    content = f"✅ Executed: {name or 'tool'}"
                                elif status == "error":
                                    content = f"❌ Error in: {name or 'tool'}"

                            # Legacy gemini events (backward compatibility during transition)
                            elif msg_type == "tool_call":
                                status = data.get("status")
                                name = data.get("name")
                                if status == "Executing":
                                    content = f"🛠️ Executing: {name}"
                                elif status == "Success":
                                    content = f"✅ Executed: {name}"
                                elif status == "Error":
                                    content = f"❌ Error in: {name}"

                            # Codex (Legacy/Other) format handling
                            elif msg_type in ["item.started", "item.completed"]:
                                item = data.get("item", {})
                                label = item.get("type")

                                if label == "command_execution":
                                    status_prefix = "🛠️ Executing" if msg_type == "item.started" else "✅ Executed"
                                    content = f"{status_prefix}: {item.get('command')}"
                                elif label == "reasoning":
                                    status_prefix = "🧠 Thinking" if msg_type == "item.started" else "🧠 Thought"
                                    content = f"{status_prefix}: {item.get('text')}"
                        except json.JSONDecodeError:
                            pass

                    # 2. gemini (Text) format handling - filter for important milestones
                    else:
                        important_keywords = [
                            "Loading extension:",
                            "Error executing tool",
                            "Error when talking to Gemini API",
                            "Executing tool",
                        ]
                        if any(k in msg for k in important_keywords):
                            content = msg

                    # Apply rate-limiting and deduplication
                    if content:
                        # Strip ANSI codes to prevent TUI corruption in the host
                        clean_content = ansi_escape.sub("", content)
                        # Add session info
                        session_label = f"[{effective_session_id[:8]}] " if effective_session_id != "standalone" else ""
                        display_content = f"{session_label}{clean_content}"

                        now = time.monotonic()
                        if display_content != state["last_msg"] or (now - state["last_time"]) > MIN_INTERVAL:
                            state["last_msg"] = display_content
                            state["last_time"] = now

                            # Log the notification we are about to send for server-side monitoring
                            logger.debug(f"MCP NOTIFICATION: [{client_config.name}] {display_content}")

                            await request_context.session.send_log_message(
                                level="info",
                                data=f"[{client_config.name}] {display_content}",
                            )
                        else:
                            logger.debug(f"MCP NOTIFICATION SKIPPED (rate-limit): {display_content}")
                except Exception as e:
                    logger.warning(f"Failed to send notification: {e}")

        agent = create_agent(client_config)
        try:
            logger.debug("Starting agent execution...")
            result = await agent.run(
                role=role_config,
                prompt=prompt_text,
                system_prompt=system_prompt_text if system_prompt_text.strip() else None,
                files=absolute_file_paths,
                images=images,
                output_callback=_notification_callback if request_context else None,
            )
            logger.debug("Agent execution completed.")
        except CLIAgentError as exc:
            metadata = self._build_error_metadata(client_config, exc)
            self._raise_tool_error(
                f"CLI '{client_config.name}' execution failed: {exc}",
                metadata=metadata,
            )

        metadata = self._build_success_metadata(client_config, role_config, result)
        metadata = self._prune_metadata(metadata, client_config, reason="normal")

        # If content is empty for Claude, try to construct it from metadata
        if client_config.name == "claude" and not result.parsed.content.strip():
            raw_metadata = result.parsed.metadata.get("raw")
            if raw_metadata and isinstance(raw_metadata, dict):
                # Attempt to parse usage/cost information for a meaningful message
                cost = raw_metadata.get("total_cost_usd")
                usage = raw_metadata.get("usage", {})
                model_usage = raw_metadata.get("modelUsage", {})
                num_turns = raw_metadata.get("num_turns")
                duration_ms = raw_metadata.get("duration_ms")

                content_parts = ["Claude CLI execution completed successfully, but returned no direct textual result."]
                if num_turns is not None:
                    content_parts.append(f"Turns: {num_turns}")
                if duration_ms is not None:
                    content_parts.append(f"Duration: {duration_ms}ms")
                if cost is not None:
                    content_parts.append(f"Total Cost: ${cost:.6f}")
                if usage:
                    content_parts.append("Usage Details:")
                    if usage.get("input_tokens") is not None:
                        content_parts.append(f"  Input Tokens: {usage['input_tokens']:,}")
                    if usage.get("output_tokens") is not None:
                        content_parts.append(f"  Output Tokens: {usage['output_tokens']:,}")
                if model_usage:
                    for model, stats in model_usage.items():
                        content_parts.append(f"Model ({model}):")
                        if stats.get("inputTokens") is not None:
                            content_parts.append(f"  Input Tokens: {stats['inputTokens']:,}")
                        if stats.get("outputTokens") is not None:
                            content_parts.append(f"  Output Tokens: {stats['outputTokens']:,}")
                        if stats.get("costUSD") is not None:
                            content_parts.append(f"  Cost: ${stats['costUSD']:.6f}")

                content = "\n".join(content_parts)
                logger.info(f"Generated content for empty Claude response: {content}")
            else:
                content = "Claude CLI execution completed, but no textual result was returned and metadata was unparseable."
        else:
            content = result.parsed.content

        model_info = {
            "provider": client_config.name,
            "model_name": result.parsed.metadata.get("model_used"),
        }

        if continuation_id:
            try:
                self._record_assistant_turn(continuation_id, content, request, model_info)
            except Exception:
                logger.debug(f"Failed to record assistant turn for continuation {continuation_id}", exc_info=True)

        continuation_offer = self._create_continuation_offer(request, model_info)
        
        if continuation_offer:
            tool_output = self._create_continuation_offer_response(
                content,
                continuation_offer,
                request,
                model_info,
            )
            tool_output.metadata = self._merge_metadata(tool_output.metadata, metadata)
        else:
            tool_output = ToolOutput(
                status="success",
                content=content,
                content_type="text",
                metadata=metadata,
            )

        return [TextContent(type="text", text=tool_output.model_dump_json())]

    async def prepare_prompt(self, request) -> str:
        client_config = self._registry.get_client(request.cli_name)
        role_config = client_config.get_role(request.role)
        system_prompt_text = role_config.prompt_path.read_text(encoding="utf-8")
        include_system_prompt = not self._use_external_system_prompt(client_config)
        return await self._prepare_prompt_for_role(
            request,
            role_config,
            system_prompt=system_prompt_text,
            include_system_prompt=include_system_prompt,
        )

    async def _prepare_prompt_for_role(
        self,
        request: CLinkRequest,
        role: ResolvedCLIRole,
        *,
        system_prompt: str,
        include_system_prompt: bool,
    ) -> str:
        """Load the role prompt and assemble the final user message."""
        self._active_system_prompt = system_prompt
        try:
            user_content = self.handle_prompt_file_with_fallback(request).strip()
            guidance = self._agent_capabilities_guidance()
            file_section = self._format_file_references(self.get_request_files(request))

            sections: list[str] = []
            active_prompt = self.get_system_prompt().strip()
            if include_system_prompt and active_prompt:
                sections.append(active_prompt)
            sections.append(guidance)
            sections.append("=== USER REQUEST ===\n" + user_content)
            if file_section:
                sections.append("=== FILE REFERENCES ===\n" + file_section)
            sections.append("Provide your response below using your own CLI tools as needed:")
            return "\n\n".join(sections)
        finally:
            self._active_system_prompt = ""

    def _use_external_system_prompt(self, client: ResolvedCLIClient) -> bool:
        runner_name = (client.runner or client.name).lower()
        return runner_name == "claude"

    def _build_success_metadata(
        self,
        client: ResolvedCLIClient,
        role: ResolvedCLIRole,
        result: AgentOutput,
    ) -> dict[str, Any]:
        """Capture execution metadata for successful CLI calls."""
        metadata: dict[str, Any] = {
            "cli_name": client.name,
            "role": role.name,
            "command": result.sanitized_command,
            "duration_seconds": round(result.duration_seconds, 3),
            "parser": result.parser_name,
            "return_code": result.returncode,
        }
        metadata.update(result.parsed.metadata)

        if result.stderr.strip():
            metadata.setdefault("stderr", result.stderr.strip())
        if result.output_file_content and "raw" not in metadata:
            metadata["raw_output_file"] = result.output_file_content
        return metadata

    def _merge_metadata(self, base: dict[str, Any] | None, extra: dict[str, Any]) -> dict[str, Any]:
        merged = dict(base or {})
        merged.update(extra)
        return merged

    def _apply_output_limit(
        self,
        client: ResolvedCLIClient,
        content: str,
        metadata: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        if len(content) <= MAX_RESPONSE_CHARS:
            return content, metadata

        summary = self._extract_summary(content)
        if summary:
            summary_text = summary
            if len(summary_text) > MAX_RESPONSE_CHARS:
                logger.debug(
                    "Clink summary from %s exceeded %d chars; truncating summary to fit.",
                    client.name,
                    MAX_RESPONSE_CHARS,
                )
                summary_text = summary_text[:MAX_RESPONSE_CHARS]
            summary_metadata = self._prune_metadata(metadata, client, reason="summary")
            summary_metadata.update(
                {
                    "output_summarized": True,
                    "output_original_length": len(content),
                    "output_summary_length": len(summary_text),
                    "output_limit": MAX_RESPONSE_CHARS,
                }
            )
            logger.info(
                "Clink compressed %s output via <SUMMARY>: original=%d chars, summary=%d chars",
                client.name,
                len(content),
                len(summary_text),
            )
            return summary_text, summary_metadata

        truncated_metadata = self._prune_metadata(metadata, client, reason="truncated")
        truncated_metadata.update(
            {
                "output_truncated": True,
                "output_original_length": len(content),
                "output_limit": MAX_RESPONSE_CHARS,
            }
        )

        excerpt_limit = min(4000, MAX_RESPONSE_CHARS // 2)
        excerpt = content[:excerpt_limit]
        truncated_metadata["output_excerpt_length"] = len(excerpt)

        logger.warning(
            "Clink truncated %s output: original=%d chars exceeds limit=%d; excerpt_length=%d",
            client.name,
            len(content),
            MAX_RESPONSE_CHARS,
            len(excerpt),
        )

        message = (
            f"CLI '{client.name}' produced {len(content)} characters, exceeding the configured clink limit "
            f"({MAX_RESPONSE_CHARS} characters). The full output was suppressed to stay within MCP response caps. "
            "Please narrow the request (review fewer files, summarize results) or run the CLI directly for the full log.\n\n"
            f"--- Begin excerpt ({len(excerpt)} of {len(content)} chars) ---\n{excerpt}\n--- End excerpt ---"
        )

        return message, truncated_metadata

    def _extract_summary(self, content: str) -> str | None:
        match = SUMMARY_PATTERN.search(content)
        if not match:
            return None
        summary = match.group(1).strip()
        return summary or None

    def _prune_metadata(
        self,
        metadata: dict[str, Any],
        client: ResolvedCLIClient,
        *,
        reason: str,
    ) -> dict[str, Any]:
        cleaned = dict(metadata)
        events = cleaned.pop("events", None)
        if events is not None:
            cleaned[f"events_removed_for_{reason}"] = True
            logger.debug(
                "Clink dropped %s events metadata for %s response (%s)",
                client.name,
                reason,
                type(events).__name__,
            )
        return cleaned

    def _build_error_metadata(self, client: ResolvedCLIClient, exc: CLIAgentError) -> dict[str, Any]:
        """Assemble metadata for failed CLI calls."""
        metadata: dict[str, Any] = {
            "cli_name": client.name,
            "return_code": exc.returncode,
        }

        # Limit the size of captured output in errors to avoid breaking the UI
        MAX_ERROR_OUTPUT = 4000

        if exc.stdout:
            stdout = exc.stdout.strip()
            if len(stdout) > MAX_ERROR_OUTPUT:
                stdout = stdout[:MAX_ERROR_OUTPUT] + f"\n... (truncated {len(stdout) - MAX_ERROR_OUTPUT} chars)"
            metadata["stdout"] = stdout

        if exc.stderr:
            stderr = exc.stderr.strip()
            if len(stderr) > MAX_ERROR_OUTPUT:
                stderr = stderr[:MAX_ERROR_OUTPUT] + f"\n... (truncated {len(stderr) - MAX_ERROR_OUTPUT} chars)"
            metadata["stderr"] = stderr

        return metadata

    def _raise_tool_error(self, message: str, metadata: dict[str, Any] | None = None) -> None:
        error_output = ToolOutput(status="error", content=message, content_type="text", metadata=metadata)
        raise ToolExecutionError(error_output.model_dump_json())

    def _agent_capabilities_guidance(self) -> str:
        return (
            "You are operating through the Gemini CLI agent. You have access to your full suite of "
            "CLI capabilities—including launching web searches, reading files, and using any other "
            "available tools. Gather current information yourself and deliver the final answer without "
            "asking the PAL MCP host to perform searches or file reads."
        )

    def _format_file_references(self, files: list[str]) -> str:
        if not files:
            return ""

        references: list[str] = []
        for file_path in files:
            try:
                path = Path(file_path)
                stat = path.stat()
                modified = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
                size = stat.st_size
                references.append(f"- {file_path} (last modified {modified}, {size} bytes)")
            except OSError:
                references.append(f"- {file_path} (unavailable)")
        return "\n".join(references)
