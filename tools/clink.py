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
        
        # Reset any stale interruption state before starting
        publisher = get_publisher()
        if publisher:
            publisher.reset_interruption()

        # --- ENSURE CONVERSATION PERSISTENCE START ---
        from utils.conversation_memory import create_thread, add_turn, get_thread
        
        # If no continuation_id, create a new thread immediately to record the user's intent
        is_new_thread = False
        if not continuation_id:
            initial_request_dict = self.get_request_as_dict(request)
            continuation_id = create_thread(tool_name=self.get_name(), initial_request=initial_request_dict)
            is_new_thread = True
            logger.debug(f"Created new thread {continuation_id}")
            # Ensure the caller and request object know about the new thread ID
            self._current_arguments["continuation_id"] = continuation_id
            request.continuation_id = continuation_id
            
        # Record user turn immediately for NEW threads to ensure persistence before execution
        # (Existing threads already have the turn added by server.py during reconstruction)
        if is_new_thread:
            user_prompt = self.get_request_prompt(request)
            user_files = self.get_request_files(request)
            user_images = self.get_request_images(request)
            add_turn(continuation_id, "user", user_prompt, files=user_files, images=user_images, tool_name=self.get_name())
            logger.debug(f"Recorded new user turn for thread {continuation_id}")
        # --- ENSURE CONVERSATION PERSISTENCE END ---

        # Track last notification to avoid spamming the UI
        state = {"last_msg": "", "last_time": 0.0}
        # Mapping from tool_id to tool_name for Gemini stream-json events
        tool_names: dict[str, str] = {}
        MIN_INTERVAL = 0.0  # Disable rate-limiting for maximum responsiveness

        # ANSI escape sequence pattern for stripping color codes
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

        # Determine effective session ID for monitor display
        effective_session_id = continuation_id or arguments.get("_instance_id") or "standalone"

        # --- PRE-CREATE ASSISTANT TURN FOR LIVE UPDATES ---
        # Initialize an empty assistant turn so we can update it with streaming content
        # This ensures the DB always has the latest state even before the tool finishes
        if continuation_id:
            from utils.conversation_memory import update_current_turn
            # Add placeholder turn
            model_info_placeholder = {"model_provider": client_config.name, "model_name": "loading..."}
            add_turn(continuation_id, "assistant", "⏳ *Processing...*", tool_name=self.get_name(), **model_info_placeholder)

        # Track state for UI notifications and DB updates
        state = {
            "last_msg": "", 
            "last_time": 0.0,
            "last_db_update": 0.0,
            "accumulated_thinking": [],
            "accumulated_logs": [],
            "summary_buffer": [],
            "in_summary": False
        }
        
        # Mapping from tool_id to tool_name for Gemini stream-json events
        tool_names: dict[str, str] = {}
        MIN_INTERVAL = 0.0  # Disable rate-limiting for maximum responsiveness
        DB_UPDATE_INTERVAL = 5.0 # Update DB every 5 seconds

        # ANSI escape sequence pattern for stripping color codes
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

        async def _notification_callback(line: str):
            logger.debug(f"CLINK NOTIFICATION RAW LINE: {line.strip()}")
            
            if not request_context:
                logger.debug(f"CLI RAW (no context): [{client_config.name}] [Session: {effective_session_id[:8]}] {line.strip()}")
                return

            publisher = get_publisher()
            
            def handle_summary_extraction(text: str) -> Optional[str]:
                """Stateful summary extraction across chunks."""
                nonlocal state
                result = None
                text_lower = text.lower()
                
                if "<summary>" in text_lower:
                    state["in_summary"] = True
                    state["summary_buffer"] = []
                    start_tag_idx = text_lower.find("<summary>")
                    content_after_start = text[start_tag_idx + 9:]
                    if content_after_start:
                        if "</summary>" in content_after_start.lower():
                            end_tag_idx = content_after_start.lower().find("</summary>")
                            state["summary_buffer"].append(content_after_start[:end_tag_idx])
                            result = "".join(state["summary_buffer"]).strip()
                            state["in_summary"] = False
                        else:
                            state["summary_buffer"].append(content_after_start)
                elif "</summary>" in text_lower and state["in_summary"]:
                    end_tag_idx = text_lower.find("</summary>")
                    state["summary_buffer"].append(text[:end_tag_idx])
                    result = "".join(state["summary_buffer"]).strip()
                    state["in_summary"] = False
                elif state["in_summary"]:
                    state["summary_buffer"].append(text)
                
                return f"📋 Summary: {result}" if result else None

            # Subprocesses might flush multiple lines at once in a single buffer chunk.
            lines = line.splitlines()
            for raw_msg in lines:
                msg = raw_msg.strip()
                if not msg: continue

                try:
                    content = None
                    db_updated_needed = False
                    
                    # 1. JSON format handling
                    json_start = msg.find('{')
                    json_end = msg.rfind('}')
                    
                    current_json_str = None
                    if json_start != -1 and json_end != -1 and json_end > json_start:
                        current_json_str = msg[json_start:json_end+1]
                        try:
                            data = json.loads(current_json_str)
                            msg_type = data.get("type")

                            # Always forward raw JSON to monitor for token/state tracking
                            if publisher:
                                await publisher.tool_log(self.get_name(), current_json_str, session_id=effective_session_id)

                            # Gemini stream-json events
                            if msg_type == "message":
                                role = data.get("role")
                                payload_content = data.get("content") or data.get("thought")
                                if role == "assistant" and payload_content:
                                    if data.get("delta") is True:
                                        summary_msg = handle_summary_extraction(payload_content)
                                        if summary_msg:
                                            content = summary_msg
                                        else:
                                            state["accumulated_thinking"].append(payload_content)
                                            if not state["in_summary"]:
                                                content = f"🧠 Thinking: {payload_content}"
                            
                            # Claude stream-json events
                            elif msg_type == "stream_event":
                                event_data = data.get("event", {})
                                etype = event_data.get("type")
                                if etype == "content_block_delta":
                                    delta = event_data.get("delta", {})
                                    dtype = delta.get("type")
                                    if dtype == "thinking_delta":
                                        thought = delta.get("thinking")
                                        if thought:
                                            summary_msg = handle_summary_extraction(thought)
                                            if summary_msg:
                                                content = summary_msg
                                            else:
                                                state["accumulated_thinking"].append(thought)
                                                if not state["in_summary"]:
                                                    content = f"🧠 Thinking: {thought}"
                                    elif dtype == "text_delta":
                                        text = delta.get("text")
                                        if text:
                                            summary_msg = handle_summary_extraction(text)
                                            if summary_msg: content = summary_msg
                                
                            elif msg_type == "tool_use":
                                name = data.get("tool_name") or data.get("name")
                                tool_id = data.get("tool_id")
                                if tool_id and name: tool_names[tool_id] = name
                                log_entry = f"🛠️ Executing: {name or 'tool'}"
                                content = log_entry
                                state["accumulated_logs"].append(log_entry)
                                db_updated_needed = True 
                                
                            elif msg_type == "tool_result":
                                tool_id = data.get("tool_id")
                                status = data.get("status")
                                name = tool_names.get(tool_id) if tool_id else None
                                log_entry = f"{'✅ Executed' if status == 'success' else '❌ Error in'}: {name or 'tool'}"
                                content = log_entry
                                state["accumulated_logs"].append(log_entry)
                                db_updated_needed = True

                            elif msg_type == "tool_call":
                                status = data.get("status")
                                name = data.get("name")
                                content = f"{'🛠️ Executing' if status == 'Executing' else '✅ Executed'}: {name}"
                                state["accumulated_logs"].append(content)
                                db_updated_needed = True

                            elif msg_type in ["item.started", "item.completed"]:
                                item = data.get("item", {})
                                label = item.get("type")
                                if label == "command_execution":
                                    content = f"{'🛠️ Executing' if msg_type == 'item.started' else '✅ Executed'}: {item.get('command')}"
                                elif label == "reasoning":
                                    content = f"{'🧠 Thinking' if msg_type == 'item.started' else '🧠 Thought'}: {item.get('text')}"
                                if content: state["accumulated_logs"].append(content)
                            
                            elif msg_type == "system":
                                logger.debug(f"CLI SYSTEM EVENT: {msg}")
                                continue
                        except json.JSONDecodeError: pass

                    # 2. Text format handling
                    if not content:
                        summary_msg = handle_summary_extraction(msg)
                        if summary_msg:
                            content = summary_msg
                        elif not state["in_summary"]:
                            important_keywords = ["Loading extension:", "Error executing tool", "Executing tool", "Executed tool", "Tool result:"]
                            if any(k in msg for k in important_keywords):
                                content = msg
                                state["accumulated_logs"].append(msg)
                        
                        # Forward raw text logs to monitor if no JSON was sent
                        if publisher and not current_json_str:
                            await publisher.tool_log(self.get_name(), msg, session_id=effective_session_id)

                    # Update DB periodically
                    now = time.monotonic()
                    if continuation_id and (db_updated_needed or (now - state["last_db_update"] > DB_UPDATE_INTERVAL)):
                        current_thinking = "".join(state["accumulated_thinking"])
                        current_logs = "\n".join(state["accumulated_logs"])
                        live_content = ""
                        if current_thinking: live_content += f"<thinking>{current_thinking}</thinking>\n\n"
                        if current_logs: live_content += f"### 🔄 Live Progress\n{current_logs}\n\n"
                        live_content += "⏳ *Processing...*"
                        update_current_turn(continuation_id, live_content, tool_name=self.get_name())
                        state["last_db_update"] = now

                    # UI Notification
                    if content:
                        clean_content = ansi_escape.sub("", content)
                        session_label = f"[{effective_session_id[:8]}] " if effective_session_id != "standalone" else ""
                        display_content = f"{session_label}{clean_content}"

                        if display_content != state["last_msg"] or (now - state["last_time"]) > MIN_INTERVAL:
                            state["last_msg"] = display_content
                            state["last_time"] = now
                            await request_context.session.send_log_message(level="info", data=f"[{client_config.name}] {display_content}")
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
        except Exception as exc:
            # Record error turn before raising to ensure persistence
            error_msg = f"Error during CLI execution: {exc}"
            
            # Special handling for Interrupted/Partial results OR general errors with output
            is_cli_error = isinstance(exc, CLIAgentError)
            is_interrupted = is_cli_error and ("interrupted" in str(exc).lower() or "interrupted" in (exc.stderr or "").lower())
            
            if is_cli_error and (is_interrupted or exc.stdout):
                try:
                    # Notify monitor about interruption/salvage
                    if publisher:
                        status_msg = "⚠️ Task interrupted by user. Salvaging progress..." if is_interrupted else "❌ Task failed. Capturing partial output..."
                        await publisher.tool_log(self.get_name(), status_msg, session_id=effective_session_id)

                    # Attempt to parse partial stdout to salvage progress
                    partial_parsed = agent._parser.parse(exc.stdout, exc.stderr)
                    partial_content = partial_parsed.content
                    if partial_parsed.thinking:
                        partial_content = f"<thinking>{partial_parsed.thinking}</thinking>\n\n{partial_content}"
                    
                    header = "⚠️ **Task Interrupted**" if is_interrupted else "❌ **Task Failed**"
                    salvaged_content = f"{header}\n\nProgress before stopping:\n{partial_content}"
                    
                    if not is_interrupted:
                        salvaged_content += f"\n\nError Details:\n{exc}"

                    # Record this salvaged turn so it's in the history for next time
                    model_info = {"provider": client_config.name, "model_name": partial_parsed.metadata.get("model_used") or "error"}
                    self._record_assistant_turn(continuation_id, salvaged_content, request, model_info)
                    
                    # If interrupted, return graceful success. If error, re-raise but with context saved.
                    if is_interrupted:
                        tool_output = ToolOutput(
                            status="success", 
                            content=salvaged_content,
                            content_type="text",
                            metadata={
                                "cli_name": client_config.name,
                                "status": "interrupted",
                                "interrupted_by": "user",
                                "partial": True
                            },
                        )
                        return [TextContent(type="text", text=tool_output.model_dump_json())]
                except Exception as parse_exc:
                    logger.debug(f"Failed to parse partial results: {parse_exc}")
                    if publisher:
                        await publisher.tool_log(self.get_name(), f"Failed to salvage output: {parse_exc}", session_id=effective_session_id)

            try:
                model_info = {"provider": client_config.name, "model_name": "error"}
                self._record_assistant_turn(continuation_id, error_msg, request, model_info)
            except Exception:
                logger.debug("Failed to record error turn", exc_info=True)

            if isinstance(exc, CLIAgentError):
                metadata = self._build_error_metadata(client_config, exc)
                self._raise_tool_error(
                    f"CLI '{client_config.name}' execution failed: {exc}",
                    metadata=metadata,
                )
            else:
                self._raise_tool_error(str(exc))

        metadata = self._build_success_metadata(client_config, role_config, result)
        metadata = self._prune_metadata(metadata, client_config, reason="normal")

        # Check for error status in parsed result (even if CLI return code was 0)
        if result.parsed.metadata.get("is_error"):
            error_content = result.parsed.content
            # Record error turn before raising to ensure persistence
            try:
                error_model_info = {
                    "provider": client_config.name,
                    "model_name": result.parsed.metadata.get("model_used") or "error"
                }
                self._record_assistant_turn(continuation_id, error_content, request, error_model_info)
            except Exception:
                logger.debug("Failed to record error turn for parsed error", exc_info=True)
                
            self._raise_tool_error(
                error_content,
                metadata=metadata
            )

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

        # Apply output size limits (truncation/summarization)
        content, metadata = self._apply_output_limit(client_config, content, metadata)

        # Prepend thinking content if available to preserve reasoning history
        if result.parsed.thinking:
            # Use standard thinking tags for better compatibility with models
            content = f"<thinking>{result.parsed.thinking}</thinking>\n\n{content}"

        model_info = {
            "provider": client_config.name,
            "model_name": result.parsed.metadata.get("model_used"),
        }

        # Always record assistant turn if continuation is active.
        # Since request.continuation_id is now updated, _create_continuation_offer_response
        # will skip its own recording logic, so we handle it here.
        if continuation_id:
            try:
                self._record_assistant_turn(continuation_id, content, request, model_info)
            except Exception:
                logger.debug(f"Failed to record assistant turn for continuation {continuation_id}", exc_info=True)

        # Continuation offer logic needs updated continuation_id
        # We need to make sure request.continuation_id is updated for _create_continuation_offer
        request.continuation_id = continuation_id
        
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

    def _record_assistant_turn(
        self, continuation_id: str, response_text: str, request, model_info: Optional[dict]
    ) -> None:
        """
        Persist an assistant response in conversation memory by updating the pre-created turn.

        CLinkTool always pre-creates an assistant turn for live updates in execute(),
        so we use update_current_turn instead of add_turn to avoid duplicate entries.
        """
        if not continuation_id:
            return

        from utils.conversation_memory import update_current_turn

        model_provider = None
        model_name = None
        model_metadata = None

        if model_info:
            provider = model_info.get("provider")
            if provider:
                if isinstance(provider, str):
                    model_provider = provider
                else:
                    try:
                        model_provider = provider.get_provider_type().value
                    except AttributeError:
                        model_provider = str(provider)
            model_name = model_info.get("model_name")
            model_response = model_info.get("model_response")
            if model_response:
                model_metadata = {"usage": model_response.usage, "metadata": model_response.metadata}

        update_current_turn(
            continuation_id,
            response_text,
            tool_name=self.get_name(),
            model_provider=model_provider,
            model_name=model_name,
            model_metadata=model_metadata,
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
