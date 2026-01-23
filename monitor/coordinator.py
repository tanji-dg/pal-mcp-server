"""
Monitor Coordinator for PAL MCP Server.

This module implements the central coordinator that:
- Receives status events from multiple MCP server instances
- Maintains aggregated state in memory
- Broadcasts state updates to connected WebSocket clients (dashboards)
- Handles instance registration, heartbeats, and timeout detection

Architecture:
    MCP Server instances connect via HTTP or Unix socket to publish events.
    Dashboards connect via WebSocket to receive real-time state updates.

Usage:
    # HTTP mode (default)
    python monitor/run_coordinator.py --transport http --port 9876

    # Unix socket mode
    python monitor/run_coordinator.py --transport unix --socket /tmp/pal-monitor.sock

    # Both (Unix for MCP, WebSocket for dashboards)
    python monitor/run_coordinator.py --transport unix --socket /tmp/pal-monitor.sock --ws-port 9876
"""

import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Union

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse

from monitor.models import (
    AggregatedState,
    EventResponse,
    InstanceStatus,
    ToolCall,
    ToolEvent,
    ToolEventType,
    utc_now,
    format_dt_iso,
)
from utils.storage_backend import get_storage_backend

logger = logging.getLogger(__name__)

# Configuration constants
HEARTBEAT_INTERVAL = 10  # seconds between heartbeats
INSTANCE_TIMEOUT = 30  # seconds before marking instance as offline
MAX_RECENT_CALLS = 20  # maximum number of recent calls to keep per instance
BROADCAST_INTERVAL = 1.0  # seconds between state broadcasts


class InstanceTracker:
    """Tracks the status and history of a single MCP server instance."""

    def __init__(self, instance_id: str, uptime_seconds: float = 0.0):
        self.instance_id = instance_id
        self.uptime_at_register = uptime_seconds
        self.start_time = time.time()
        self.state = "idle"
        self.last_heartbeat = utc_now()
        self.interrupted = False
        
        # Support for nested/parallel tool calls
        self.active_tools: dict[str, datetime] = {} # tool_name -> start_time
        self.active_tool_inputs: dict[str, str] = {} # tool_name -> input
        self._tool_start_times: dict[str, float] = {} # tool_id -> start_timestamp (float)
        self._tool_name_cache: dict[str, str] = {} # Map tool_id -> name
        
        self.active_tool: Optional[str] = None # Primary/most recent tool
        self.active_tool_input: Optional[str] = None
        self.session_id: Optional[str] = None
        self.model_name: Optional[str] = None
        self.active_role: Optional[str] = None
        self.tool_start_time: Optional[datetime] = None
        self.recent_calls: deque[ToolCall] = deque(maxlen=MAX_RECENT_CALLS)
        self.recent_logs: deque[ToolEvent] = deque(maxlen=100) # Buffer last 100 log lines
        
        # Current status for display
        self.last_status = None
        
        # Reliability: track last completion to prevent late logs from reviving 'busy' state
        self.last_completion_time: datetime = utc_now()
        
        # Lifetime stats (Instance Totals)
        self.total_calls: int = 0
        self.total_errors: int = 0
        self.input_tokens: int = 0
        self.output_tokens: int = 0
        self.cache_read_tokens: int = 0
        self.cache_creation_tokens: int = 0
        
        # Session breakdown metrics
        self.thinking_ms: int = 0
        self.execution_ms: int = 0
        self._last_thinking_start: Optional[float] = None
        self._last_activity_time: Optional[float] = None

        # Internal tracking for deltas (per tool execution)
        self._last_request_tokens = {
            "input": 0,
            "output": 0,
            "cache_read": 0,
            "cache_creation": 0
        }

        # Metrics for last window (1 hour)
        self._calls_1m: list[tuple[float, bool]] = []  # (timestamp, is_error)
        self._durations_1m: list[tuple[float, int]] = []  # (timestamp, duration_ms)

    def update_heartbeat(self, uptime_seconds: Optional[float] = None):
        """Update last heartbeat time."""
        self.last_heartbeat = utc_now()
        if uptime_seconds is not None:
            self.uptime_at_register = uptime_seconds
            self.start_time = time.time()

    def _update_tokens_incremental(self, usage: dict):
        """Update instance totals using deltas from reported cumulative request tokens."""
        # Mapping between usage keys and internal keys
        mapping = {
            "input": ["input_tokens", "inputTokens", "prompt_tokens", "promptTokens"],
            "output": ["output_tokens", "outputTokens", "candidates_tokens", "candidatesTokens"],
            "cache_read": ["cache_read_input_tokens", "cacheReadInputTokens", "cached_tokens", "cachedTokens", "cached"],
            "cache_creation": ["cache_creation_input_tokens", "cacheCreationInputTokens"]
        }
        
        updated = False
        for key, possible_keys in mapping.items():
            val = 0
            for pk in possible_keys:
                if pk in usage:
                    val = usage[pk]
                    break
            
            if val > 0:
                # If the new value is greater than what we last saw for THIS request,
                # add the difference to the LIFETIME total.
                last_val = self._last_request_tokens[key]
                if val > last_val:
                    delta = val - last_val
                    if key == "input": self.input_tokens += delta
                    elif key == "output": self.output_tokens += delta
                    elif key == "cache_read": self.cache_read_tokens += delta
                    elif key == "cache_creation": self.cache_creation_tokens += delta
                    
                    self._last_request_tokens[key] = val
                    updated = True
        
        if updated:
            logger.debug(f"Tokens updated for {self.instance_id}: input={self.input_tokens}, output={self.output_tokens}, cached={self.cache_read_tokens}")

    def start_tool(self, tool_name: str, tool_input: Optional[str] = None):
        """Record tool execution start."""
        self.interrupted = False # Reset interruption state
        now = utc_now()
        now_ts = time.time()
        self.state = "busy"
        self.active_tools[tool_name] = now
        self.active_tool_inputs[tool_name] = tool_input
        
        # Reset session metrics for new primary tool execution
        if tool_name == "clink":
            self.thinking_ms = 0
            self.execution_ms = 0
            self._last_thinking_start = now_ts
            self._last_activity_time = now_ts

        # Update primary display info
        self.active_tool = tool_name
        self.active_tool_input = tool_input
        self.tool_start_time = now
        self.last_heartbeat = now
        self.last_status = f"Starting {tool_name}..."

        # Reset request-local token counters
        for k in self._last_request_tokens:
            self._last_request_tokens[k] = 0

        # Try to extract model/role/session name from input arguments
        if tool_input:
            try:
                args = json.loads(tool_input)
                # For clink, cli_name is the most useful identifier for 'model'
                self.model_name = args.get("model") or args.get("cli_name") or self.model_name
                self.active_role = args.get("role") or self.active_role
                self.session_id = args.get("continuation_id") or self.session_id
            except Exception:
                pass

    def log_activity(self, tool_name: str, log_data: Optional[str] = None, original_event: Optional[ToolEvent] = None):
        """Update activity status based on log event."""
        now = utc_now()
        # Use wall-clock timestamp for all calculations to be compatible with log timestamps
        now_ts = time.time()
        
        # Store log in buffer if event provided
        if original_event:
            self.recent_logs.append(original_event)
            # Capture session_id from event if present
            if original_event.session_id:
                self.session_id = original_event.session_id

        # If we are already idle, don't let logs pull us back to busy
        # unless it's explicitly a tool start we might have missed,
        # OR we are a fresh tracker (monitor restart) and this is our first activity.
        if self.state == "idle":
            # Heuristic check for tool start patterns in logs
            is_start_event = log_data and ('"type": "tool_use"' in log_data or '"type": "item.started"' in log_data or '"type": "message_start"' in log_data)
            
            # If it's an explicit start event, or a fresh tracker seeing JSON, go to busy
            is_fresh = self.active_tool is None and self.total_calls == 0
            if is_start_event or (is_fresh and log_data and log_data.strip().startswith("{")):
                # If it's a start event but it's older than our last completion, ignore it (late log)
                if not is_fresh and original_event and original_event.timestamp < self.last_completion_time:
                    return
                self.state = "busy"
                # If fresh, pick clink as default tool name if none provided
                if not self.active_tool:
                    self.active_tool = tool_name or "clink"
                    self.tool_start_time = now
            else:
                # Still record heartbeat and update status if it's short, but stay idle
                self.last_heartbeat = now
                if log_data and not log_data.startswith("{") and len(log_data) < 30:
                    self.last_status = log_data.strip()
                return

        self.state = "busy"

        if tool_name not in self.active_tools:
            self.active_tools[tool_name] = now
            
        if not self.active_tool:
            self.active_tool = tool_name
            self.tool_start_time = now
            
        self.last_heartbeat = now
        
        if log_data:
            is_json_log = False
            try:
                # Handle potential multiple JSON objects in one log chunk, including concatenated ones like }{
                raw_lines = log_data.strip().split("\n")
                lines = []
                for rl in raw_lines:
                    # Handle }{ case
                    parts = rl.replace("}{", "}\n{").split("\n")
                    lines.extend(parts)

                for line in lines:
                    line = line.strip()
                    if not (line.startswith("{") and line.endswith("}")):
                        continue
                        
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    is_json_log = True
                    msg_type = data.get("type")
                    
                    # Determine high-precision event time
                    # Prioritize internal JSON timestamp if available
                    event_time_ts = now_ts
                    if "timestamp" in data:
                        try:
                            # Handle ISO format: 2026-01-20T11:26:13.055Z
                            ts_str = data["timestamp"].replace("Z", "+00:00")
                            event_time_ts = datetime.fromisoformat(ts_str).timestamp()
                        except Exception:
                            pass

                    # Accumulate thinking time if this event is model reasoning
                    is_reasoning = (
                        msg_type in ["message", "assistant"] or
                        "thought" in data or "thinking" in data or
                        (msg_type == "content_block_delta" and data.get("delta", {}).get("type") == "thinking_delta")
                    )
                    
                    if is_reasoning and self._last_activity_time:
                        delta_ms = int((event_time_ts - self._last_activity_time) * 1000)
                        if 0 < delta_ms < 30000: # Ignore gaps > 30s as potential idling
                            self.thinking_ms += delta_ms
                    
                    self._last_activity_time = event_time_ts

                    # Capture session_id from log if available - REMOVED: frequently overwrites with wrong internal IDs
                    # if "session_id" in data:
                    #     self.session_id = data["session_id"]

                    # Unwrap Claude stream_event wrapper
                    if msg_type == "stream_event" and "event" in data and isinstance(data["event"], dict):
                        data = data["event"]
                        msg_type = data.get("type")

                    # Extract Token Usage (Post-unwrap)
                    # Check multiple possible locations for usage/stats
                    # Note: We now only do this once here, or in specialized blocks below
                    u = data.get("usage") or data.get("stats")
                    if not u and isinstance(data.get("message"), dict):
                        u = data["message"].get("usage")
                    
                    # specialized msg_types will handle their own updates to avoid confusion
                    if isinstance(u, dict) and msg_type not in ["result", "modelUsage"]:
                        self._update_tokens_incremental(u)

                    # Update model name if found in log metadata
                    new_model = None
                    if "model" in data:
                        new_model = data["model"]
                    elif "message" in data and isinstance(data["message"], dict) and "model" in data["message"]:
                        new_model = data["message"]["model"]
                    elif "model_used" in data:
                        new_model = data["model_used"]
                    elif "metadata" in data and isinstance(data["metadata"], dict):
                        new_model = data["metadata"].get("model_used") or data["metadata"].get("model_name")
                    elif "modelUsage" in data and isinstance(data["modelUsage"], dict):
                        models = list(data["modelUsage"].keys())
                        if models:
                            new_model = models[0]

                    if new_model:
                        # Guard: Don't let low-level model names (like gemini-3-flash)
                        # overwrite high-level CLI identifiers (like claude) or complex names
                        is_low_level = any(m in new_model.lower() for m in ["gemini-3-flash", "gemini-2.0-flash-lite"])

                        # High level names include CLI names or already resolved model strings
                        high_level_identifiers = ["claude", "codex", "gemini", "sonnet", "haiku", "opus", "gpt-4", "o1", "o3"]
                        is_high_level = self.model_name and (
                            "(" in self.model_name or
                            any(h in self.model_name.lower() for h in high_level_identifiers)
                        )

                        if not (is_low_level and is_high_level):
                            self.model_name = new_model

                    elif msg_type == "modelUsage" or "modelUsage" in data:
                        # Aggregate across all models if present
                        mu = data.get("modelUsage") or data
                        if isinstance(mu, dict):
                            totals = {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}
                            for m_stats in mu.values():
                                if not isinstance(m_stats, dict): continue
                                totals["input_tokens"] += m_stats.get("input_tokens") or m_stats.get("inputTokens") or m_stats.get("prompt_tokens") or m_stats.get("promptTokens") or 0
                                totals["output_tokens"] += m_stats.get("output_tokens") or m_stats.get("outputTokens") or m_stats.get("candidates_tokens") or m_stats.get("candidatesTokens") or 0
                                totals["cache_read_input_tokens"] += m_stats.get("cache_read_input_tokens") or m_stats.get("cacheReadInputTokens") or m_stats.get("cached_tokens") or m_stats.get("cachedTokens") or m_stats.get("cached") or 0
                                totals["cache_creation_input_tokens"] += m_stats.get("cache_creation_input_tokens") or m_stats.get("cacheCreationInputTokens") or 0
                            self._update_tokens_incremental(totals)

                    if msg_type == "content_block_start":
                        # Handle streaming tool use start (Gemini/Claude)
                        content_block = data.get("content_block", {})
                        if content_block.get("type") == "tool_use":
                            name = content_block.get("name") or "tool"
                            tool_id = content_block.get("id")
                            if tool_id:
                                self._tool_name_cache[tool_id] = name
                                self._tool_start_times[tool_id] = event_time_ts
                            self.last_status = f"Calling {name}"
                            self.active_tool = name
                            self.tool_start_time = utc_now()

                    elif msg_type == "message" or msg_type == "assistant":
                        self.last_status = "Thinking"
                        # Scan content for tool_use to capture start times if sent as a whole block
                        content = data.get("content")
                        # Handle nested message structure
                        if not content and isinstance(data.get("message"), dict):
                            content = data["message"].get("content")
                            
                        if isinstance(content, list):
                            for item in content:
                                if isinstance(item, dict) and item.get("type") == "tool_use":
                                    name = item.get("name") or "tool"
                                    tool_id = item.get("id")
                                    if tool_id:
                                        self._tool_name_cache[tool_id] = name
                                        # Use event time as start time since we received the whole block
                                        if tool_id not in self._tool_start_times:
                                            self._tool_start_times[tool_id] = event_time_ts
                                    self.last_status = f"Calling {name}"
                                    self.active_tool = name
                                    self.tool_start_time = utc_now()

                    elif msg_type == "tool_use":
                        # Support both 'name' and 'tool_name' keys
                        name = data.get("tool_name") or data.get("name") or "tool"
                        tool_id = data.get("tool_id")
                        if tool_id:
                            self._tool_name_cache[tool_id] = name
                            # Store start time as high-precision float timestamp
                            self._tool_start_times[tool_id] = event_time_ts
                        self.last_status = f"Calling {name}"
                        self.active_tool = name
                        self.tool_start_time = utc_now()
                    elif msg_type == "tool_result":
                        # Sub-tool execution finished (detected via logs)
                        tool_id = data.get("tool_id")
                        status = data.get("status", "completed")
                        name = self._tool_name_cache.get(tool_id) if tool_id else None

                        # Check content for error keywords even if status is ok
                        content = data.get("content") or data.get("output") or ""
                        if isinstance(content, list):
                            # Handle content list from certain tool outputs
                            content = " ".join([str(c) for c in content])
                        elif isinstance(content, dict):
                            content = str(content)

                        is_content_error = False
                        if content:
                            content_lower = content.lower()
                            # Check for specific error indicators in the output
                            if "error:" in content_lower or "exception:" in content_lower or "failed:" in content_lower:
                                is_content_error = True

                        # Calculate duration
                        duration_ms = 0
                        if tool_id and tool_id in self._tool_start_times:
                            start_t_val = self._tool_start_times.pop(tool_id)
                            # Accurate difference calculation (in seconds -> ms)
                            # Support both float timestamps and datetime objects for test compatibility
                            if isinstance(start_t_val, datetime):
                                start_t_val = start_t_val.timestamp()
                            duration_ms = int((event_time_ts - start_t_val) * 1000)
                            # Accumulate into session execution time
                            if duration_ms > 0:
                                self.execution_ms += duration_ms

                        # Update metrics
                        self.total_calls += 1
                        if status == "error" or is_content_error:
                            self.total_errors += 1
                            self.last_status = f"Error in {name or 'tool'}"
                        else:
                            self.last_status = f"Result from {name or 'tool'}"

                        # Add to recent calls
                        call = ToolCall(
                            tool=name or "tool",
                            tool_input=None, # Input not cached for sub-tools yet
                            tool_output=content[:1000] if content else None,
                            duration_ms=duration_ms,
                            status="error" if (status == "error" or is_content_error) else "success",
                            model_name=self.model_name,
                            timestamp=utc_now(),
                        )
                        self.recent_calls.appendleft(call)
                        
                        # Track for window metrics
                        self._calls_1m.append((now_ts, status == "error" or is_content_error))
                        self._durations_1m.append((now_ts, duration_ms))
                    elif msg_type == "user":
                        # Handle Claude tool results embedded in user message
                        message = data.get("message", {})
                        content_list = message.get("content", [])
                        if isinstance(content_list, list):
                            for item in content_list:
                                if isinstance(item, dict) and item.get("type") == "tool_result":
                                    self.total_calls += 1
                                    
                                    tool_id = item.get("tool_use_id")
                                    name = self._tool_name_cache.get(tool_id) if tool_id else None
                                    is_error = item.get("is_error", False)
                                    
                                    # Check content for error keywords
                                    content_str = item.get("content", "")
                                    if isinstance(content_str, str):
                                        lower = content_str.lower()
                                        if "error:" in lower or "exception:" in lower or "failed:" in lower:
                                            is_error = True
                                    
                                    # Calculate duration
                                    duration_ms = 0
                                    if tool_id and tool_id in self._tool_start_times:
                                        start_t_ts = self._tool_start_times.pop(tool_id)
                                        duration_ms = int((event_time_ts - start_t_ts) * 1000)

                                    if is_error:
                                        self.total_errors += 1
                                        self.last_status = f"Error in {name or 'tool'}"
                                    else:
                                        self.last_status = f"Result from {name or 'tool'}"

                                    # Add to recent calls
                                    call = ToolCall(
                                        tool=name or "tool",
                                        tool_input=None,
                                        tool_output=content_str[:1000] if isinstance(content_str, str) else None,
                                        duration_ms=duration_ms,
                                        status="error" if is_error else "success",
                                        model_name=self.model_name,
                                        timestamp=utc_now(),
                                    )
                                    self.recent_calls.appendleft(call)
                                    
                                    # Track metrics
                                    self._calls_1m.append((now_ts, is_error))
                                    self._durations_1m.append((now_ts, duration_ms))

                    elif msg_type == "result": # Final result from Gemini CLI or Claude CLI
                        # Update tokens from final result
                        u = data.get("usage") or data.get("stats")
                        if isinstance(u, dict):
                            self._update_tokens_incremental(u)
                        
                        if "modelUsage" in data and isinstance(data["modelUsage"], dict):
                            # Aggregate across all models
                            totals = {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}
                            for m_stats in data["modelUsage"].values():
                                if not isinstance(m_stats, dict): continue
                                totals["input_tokens"] += m_stats.get("inputTokens") or m_stats.get("input_tokens") or 0
                                totals["output_tokens"] += m_stats.get("outputTokens") or m_stats.get("output_tokens") or 0
                                totals["cache_read_input_tokens"] += m_stats.get("cacheReadInputTokens") or m_stats.get("cache_read_input_tokens") or m_stats.get("cached") or 0
                                totals["cache_creation_input_tokens"] += m_stats.get("cacheCreationInputTokens") or m_stats.get("cache_creation_input_tokens") or 0
                            self._update_tokens_incremental(totals)

                        status = data.get("status", "ok")
                        if status == "error" or data.get("is_error"):
                            self.last_status = "Error"
                        else:
                            self.last_status = "Responding"

                    elif msg_type == "item.started": # Codex format
                        item = data.get("item", {})
                        item_id = item.get("id")
                        if item_id:
                            self._tool_start_times[item_id] = event_time_ts

                        if item.get("type") == "command_execution":
                            self.last_status = f"Executing {item.get('command', 'cmd')}"
                        elif item.get("type") == "reasoning":
                            self.last_status = "Thinking"

                    elif msg_type == "item.completed": # Codex format
                        item = data.get("item", {})
                        item_id = item.get("id")
                        status = item.get("status")
                        
                        duration_ms = 0
                        if item_id and item_id in self._tool_start_times:
                            start_t = self._tool_start_times.pop(item_id)
                            # Support both float and datetime
                            if isinstance(start_t, datetime):
                                start_t = start_t.timestamp()
                            duration_ms = int((event_time_ts - start_t) * 1000)

                        cmd = item.get("command")
                        if cmd or item.get("type") == "command_execution":
                            self.total_calls += 1
                            is_err = status == "failed"
                            if is_err: self.total_errors += 1
                            
                            # Standardize tool name to first word of command for metrics compatibility
                            tool_name_short = cmd.split()[0] if cmd else "cmd"
                            
                            call = ToolCall(
                                tool=tool_name_short,
                                status="error" if is_err else "success",
                                duration_ms=duration_ms,
                                timestamp=utc_now(),
                            )
                            self.recent_calls.appendleft(call)
                            self._calls_1m.append((event_time_ts, is_err))
                            self._durations_1m.append((event_time_ts, duration_ms))
                            self.last_status = f"{'Error in' if is_err else 'Result from'} {cmd or 'cmd'}"

                    elif msg_type == "error":
                        error_obj = data.get("error") or data
                        delay_info = ""
                        if isinstance(error_obj, dict):
                            details = error_obj.get("details", [])
                            if isinstance(details, list):
                                for detail in details:
                                    if not isinstance(detail, dict):
                                        continue
                                    metadata = detail.get("metadata", {})
                                    if isinstance(metadata, dict) and "quotaResetDelay" in metadata:
                                        delay_info = f" (Quota resets in {metadata['quotaResetDelay']})"
                                    if "retryDelay" in detail:
                                        delay_info = f" (Retry in {detail['retryDelay']})"
                        
                        self.last_status = f"Rate Limited{delay_info}"
            except Exception:
                pass

            if not is_json_log or self.last_status == f"Starting {tool_name}...":
                log_lower = log_data.lower()
                if "thinking" in log_lower:
                    self.last_status = "Thinking"
                elif "calling tool" in log_lower or "executing" in log_lower:
                    self.last_status = "Executing"
                elif "result" in log_lower:
                    self.last_status = "Finishing"
                elif "finished" in log_lower or "completed" in log_lower:
                    self.last_status = "Completed"
                elif len(log_data) < 100: # Heuristic for short status message
                    self.last_status = log_data.strip()

    def end_tool(self, tool_name: Optional[str] = None, duration_ms: int = 0, is_error: bool = False, tool_output: Optional[str] = None, model_name: Optional[str] = None):
        """Record tool execution completion."""
        # Support positional duration_ms for backward compatibility with some tests
        if isinstance(tool_name, int) and duration_ms == 0:
            duration_ms = tool_name
            tool_name = None

        now_ts = time.time()
        status = "error" if is_error else "success"
        self.last_completion_time = utc_now()
        
        # Determine which tool actually ended
        target_tool = tool_name or self.active_tool

        # Update lifetime stats
        self.total_calls += 1
        if is_error:
            self.total_errors += 1

        if target_tool:
            # Extract tokens and status from final tool output if available
            extracted_content = None
            if tool_output:
                try:
                    data = json.loads(tool_output)

                    def process_obj(obj):
                        nonlocal status, extracted_content
                        if not isinstance(obj, dict):
                            return
                        
                        # Check for interruption status
                        if obj.get("status") == "interrupted" or (obj.get("metadata") and obj.get("metadata").get("status") == "interrupted"):
                            status = "interrupted"

                        # Extract human-readable content for the history dump
                        if "content" in obj and isinstance(obj["content"], str):
                            extracted_content = obj["content"]
                        
                        # Check for multiple possible usage locations
                        u = obj.get("usage") or obj.get("stats") or (obj.get("metadata") and obj.get("metadata").get("usage"))
                        if isinstance(u, dict):
                            self._update_tokens_incremental(u)
                        
                        # Aggregate modelUsage if present
                        mu = obj.get("modelUsage")
                        if isinstance(mu, dict):
                            totals = {"input_tokens": 0, "output_tokens": 0, "cache_read_input_tokens": 0, "cache_creation_input_tokens": 0}
                            for m_stats in mu.values():
                                if not isinstance(m_stats, dict): continue
                                totals["input_tokens"] += m_stats.get("inputTokens") or m_stats.get("input_tokens") or 0
                                totals["output_tokens"] += m_stats.get("outputTokens") or m_stats.get("output_tokens") or 0
                                totals["cache_read_input_tokens"] += m_stats.get("cacheReadInputTokens") or m_stats.get("cache_read_input_tokens") or m_stats.get("cached") or 0
                                totals["cache_creation_input_tokens"] += m_stats.get("cacheCreationInputTokens") or m_stats.get("cache_creation_input_tokens") or 0
                            self._update_tokens_incremental(totals)
                        
                        # Recursively process "text" field if it contains JSON
                        if isinstance(obj.get("text"), str):
                            try:
                                inner = json.loads(obj["text"])
                                process_obj(inner)
                            except Exception:
                                pass
                        
                        # Process nested metadata
                        if isinstance(obj.get("metadata"), dict):
                            process_obj(obj["metadata"])

                    if isinstance(data, list):
                        for item in data:
                            process_obj(item)
                    else:
                        process_obj(data)
                except Exception:
                    pass

            effective_model = model_name or (self.model_name if target_tool == self.active_tool else None)

            call = ToolCall(
                tool=target_tool,
                tool_input=self.active_tool_inputs.get(target_tool),
                tool_output=extracted_content or tool_output,
                duration_ms=duration_ms,
                status=status,
                model_name=effective_model,
                timestamp=utc_now(),
            )
            self.recent_calls.appendleft(call)

            # Track for window metrics
            self._calls_1m.append((now_ts, is_error))
            self._durations_1m.append((now_ts, duration_ms))
            
            # Remove from active list
            if target_tool in self.active_tools:
                del self.active_tools[target_tool]
            if target_tool in self.active_tool_inputs:
                del self.active_tool_inputs[target_tool]

        # Update state based on remaining tools
        is_primary_completion = (target_tool and target_tool == self.active_tool)
        is_clink_completion = (target_tool == "clink")

        if not self.active_tools or is_primary_completion or is_clink_completion:
            self.state = "idle"
            if status == "interrupted":
                self.last_status = "Interrupted by user"
                self.interrupted = False # Reset flag after handling
            else:
                self.last_status = f"Completed {target_tool} ({status})" if target_tool else "Idle"
            
            self.active_tool = None
            self.active_tool_input = None
            self.model_name = None
            self.session_id = None
            self.tool_start_time = None
            self.active_tools.clear() 
            self.active_tool_inputs.clear()
        else:
            self.state = "busy"
            self.active_tool = list(self.active_tools.keys())[-1]
            self.tool_start_time = self.active_tools[self.active_tool]
            self.last_status = f"Finished {target_tool}, back to {self.active_tool}"
            
        self.last_heartbeat = utc_now()

    def get_uptime(self) -> float:
        """Calculate current uptime in seconds."""
        if self.is_timed_out() or self.state == "offline":
            return self.uptime_at_register + (self.last_heartbeat.timestamp() - self.start_time)
        return self.uptime_at_register + (time.time() - self.start_time)

    def get_error_rate_1m(self) -> float:
        """Calculate error rate over last window."""
        self._cleanup_old_metrics()
        if not self._calls_1m:
            return 0.0
        errors = sum(1 for _, is_error in self._calls_1m if is_error)
        return errors / len(self._calls_1m)

    def get_avg_execution_time_1m(self) -> float:
        """Calculate average execution time over last window."""
        self._cleanup_old_metrics()
        if not self._durations_1m:
            return 0.0
        return sum(d for _, d in self._durations_1m) / len(self._durations_1m)

    def _cleanup_old_metrics(self):
        cutoff = time.time() - 3600
        self._calls_1m = [(t, e) for t, e in self._calls_1m if t > cutoff]
        self._durations_1m = [(t, d) for t, d in self._durations_1m if t > cutoff]

    def is_timed_out(self) -> bool:
        now = utc_now()
        hb = self.last_heartbeat
        if hb.tzinfo is None:
            hb = hb.replace(tzinfo=timezone.utc)
        return now - hb > timedelta(seconds=INSTANCE_TIMEOUT)

    def to_status(self) -> InstanceStatus:
        """Convert to InstanceStatus model."""
        state = "offline" if self.is_timed_out() else self.state
        return InstanceStatus(
            instance_id=self.instance_id,
            uptime_seconds=self.get_uptime(),
            state=state,
            last_heartbeat=self.last_heartbeat,
            active_tool=self.active_tool if state == "busy" else None,
            session_id=self.session_id,
            model_name=self.model_name if state == "busy" else None,
            active_role=self.active_role if state == "busy" else None,
            tool_start_time=self.tool_start_time if state == "busy" else None,
            recent_calls=list(self.recent_calls),
            error_rate_1m=self.get_error_rate_1m(),
            avg_execution_time_1m=self.get_avg_execution_time_1m(),
            last_status=self.last_status,
            total_calls=self.total_calls,
            total_errors=self.total_errors,
            thinking_ms=self.thinking_ms,
            execution_ms=self.execution_ms,
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            cache_read_tokens=self.cache_read_tokens,
            cache_creation_tokens=self.cache_creation_tokens,
        )


class MonitorCoordinator:
    """Central coordinator for MCP server monitoring."""

    def __init__(self):
        self.instances: dict[str, InstanceTracker] = {}
        self.websocket_clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()
        self._broadcast_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Start the coordinator background tasks."""
        async with self._lock:
            if self._running:
                return

            self._running = True
            self._broadcast_task = asyncio.create_task(self._broadcast_loop())
            logger.info("Monitor coordinator started")

    async def stop(self):
        """Stop the coordinator and cleanup."""
        self._running = False
        if self._broadcast_task:
            self._broadcast_task.cancel()
            try:
                await self._broadcast_task
            except asyncio.CancelledError:
                pass
        logger.info("Monitor coordinator stopped")

    async def process_event(self, event: Union[ToolEvent, dict]) -> EventResponse:
        """Process an incoming event from an MCP server instance."""
        broadcast_log_event = None
        interrupted = False

        # Robust extraction regardless of object type
        def get_val(obj, key):
            if isinstance(obj, dict):
                return obj.get(key)
            val = getattr(obj, key, None)
            if val is None and hasattr(obj, "model_dump"):
                return obj.model_dump().get(key)
            return val

        instance_id = get_val(event, "instance_id")
        event_type = get_val(event, "event_type")

        if not instance_id:
            logger.error(f"Event missing instance_id. Type: {type(event)}")
            return EventResponse(status="error", message="Missing instance_id")

        async with self._lock:
            if event_type == ToolEventType.REGISTER:
                uptime = get_val(event, 'uptime_seconds') or 0.0
                self.instances[instance_id] = InstanceTracker(instance_id, uptime)
                logger.info(f"Instance registered: {instance_id}")
                return EventResponse(status="ok")

            elif event_type == ToolEventType.UNREGISTER:
                if instance_id in self.instances:
                    del self.instances[instance_id]
                    logger.info(f"Instance unregistered: {instance_id}")
                return EventResponse(status="ok")

            # Auto-register if not found
            if instance_id not in self.instances:
                uptime = get_val(event, 'uptime_seconds') or 0.0
                self.instances[instance_id] = InstanceTracker(instance_id, uptime)
                logger.info(f"Instance auto-registered: {instance_id}")

            tracker = self.instances[instance_id]
            
            # Update thinking metrics based on elapsed time since last event
            now_ts = time.time()
            if tracker.last_status == "Thinking" and tracker._last_activity_time:
                delta = int((now_ts - tracker._last_activity_time) * 1000)
                # Filter outliers (negative or > 5 mins without heartbeat)
                if 0 < delta < 300000:
                    tracker.thinking_ms += delta
            
            tracker._last_activity_time = now_ts
            
            interrupted = tracker.interrupted

            if event_type == ToolEventType.HEARTBEAT:
                uptime = get_val(event, 'uptime_seconds')
                tracker.update_heartbeat(uptime)

            elif event_type == ToolEventType.TOOL_START:
                tool_name = get_val(event, 'tool_name')
                tool_input = get_val(event, 'tool_input')
                if tool_name:
                    tracker.start_tool(tool_name, tool_input)
                    logger.debug(f"Tool started: {tool_name} on {instance_id}")

            elif event_type == ToolEventType.TOOL_END:
                tool_name = get_val(event, 'tool_name')
                duration_ms = get_val(event, 'duration_ms')
                tool_output = get_val(event, 'tool_output')
                model_name = get_val(event, 'model_name')
                tracker.end_tool(tool_name, duration_ms or 0, is_error=False, tool_output=tool_output, model_name=model_name)
                logger.debug(f"Tool completed: {tool_name} on {instance_id}")

            elif event_type == ToolEventType.TOOL_ERROR:
                tool_name = get_val(event, 'tool_name')
                duration_ms = get_val(event, 'duration_ms')
                model_name = get_val(event, 'model_name')
                tool_output = get_val(event, 'tool_output')
                error_message = get_val(event, 'error_message') or 'Unknown error'
                tracker.end_tool(tool_name, duration_ms or 0, is_error=True, tool_output=tool_output, model_name=model_name)
                logger.warning(f"Tool error: {tool_name} on {instance_id} - {error_message}")

            elif event_type == ToolEventType.TOOL_LOG:
                tool_name = get_val(event, 'tool_name')
                log_data = get_val(event, 'log_data')
                
                # Prepare event object for broadcasting and storage
                if isinstance(event, dict):
                    try:
                        # Best effort conversion for broadcast
                        broadcast_log_event = ToolEvent(**event)
                    except Exception as e:
                        logger.warning(f"Failed to convert log event dict to model: {e}")
                        broadcast_log_event = None
                else:
                    broadcast_log_event = event

                if tool_name:
                    # Log activity needs the event object for storage
                    tracker.log_activity(tool_name, log_data, original_event=broadcast_log_event)
                
                if broadcast_log_event:
                    if not broadcast_log_event.session_id and tracker.session_id:
                        broadcast_log_event.session_id = tracker.session_id


        if broadcast_log_event:
            await self.broadcast_log(broadcast_log_event)
            
        return EventResponse(status="ok", interrupted=interrupted)

    async def interrupt_instance(self, instance_id: str):
        """Request interruption for a specific instance."""
        async with self._lock:
            if instance_id in self.instances:
                logger.warning(f"Interruption requested for instance: {instance_id}")
                tracker = self.instances[instance_id]
                tracker.interrupted = True
                tracker.last_status = "Interrupting..."
                return True
        return False

    async def broadcast_log(self, event: ToolEvent):
        """Broadcast log event to all connected clients."""
        if not self.websocket_clients:
            return
        message = event.to_json()
        async with self._lock:
            disconnected = set()
            for client in self.websocket_clients:
                try:
                    await client.send_text(message)
                except Exception:
                    disconnected.add(client)
            for client in disconnected:
                self.websocket_clients.discard(client)

    async def add_websocket_client(self, websocket: WebSocket):
        """Add a new WebSocket client connection."""
        async with self._lock:
            self.websocket_clients.add(websocket)
            logger.info(f"Dashboard connected. Total clients: {len(self.websocket_clients)}")
        
        try:
            state = await self.get_aggregated_state()
            await websocket.send_text(state.to_json())
            
            async with self._lock:
                for instance in self.instances.values():
                    # Limit the amount of backlog sent to new clients
                    backlog = list(instance.recent_logs)[-50:]
                    for log_event in backlog:
                        try:
                            await websocket.send_text(log_event.to_json())
                        except Exception:
                            break
        except Exception as e:
            logger.error(f"Failed to send initial data to dashboard: {e}", exc_info=True)

    async def remove_websocket_client(self, websocket: WebSocket):
        """Remove a WebSocket client connection."""
        async with self._lock:
            self.websocket_clients.discard(websocket)
            logger.info(f"Dashboard disconnected. Total clients: {len(self.websocket_clients)}")

    async def get_aggregated_state(self) -> AggregatedState:
        """Get current aggregated state of all instances."""
        async with self._lock:
            instances = [tracker.to_status() for tracker in self.instances.values()]
            return AggregatedState.from_instances(instances)

    async def _broadcast_loop(self):
        """Periodically broadcast state to all connected WebSocket clients."""
        while self._running:
            try:
                await asyncio.sleep(BROADCAST_INTERVAL)
                if not self.websocket_clients:
                    continue
                
                try:
                    state = await self.get_aggregated_state()
                    message = state.to_json()
                    
                    if state.total_calls > 0 or state.total_thinking_ms > 0:
                        logger.debug(f"Broadcasting metrics: calls={state.total_calls}, thinking={state.total_thinking_ms}ms, execution={state.total_execution_ms}ms")
                except Exception as e:
                    logger.error(f"Failed to generate aggregated state for broadcast: {e}", exc_info=True)
                    continue

                async with self._lock:
                    disconnected = set()
                    for client in self.websocket_clients:
                        try:
                            await client.send_text(message)
                        except Exception:
                            disconnected.add(client)
                    for client in disconnected:
                        self.websocket_clients.discard(client)
                        logger.debug("Removed stale WebSocket client")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Unexpected error in broadcast loop: {e}", exc_info=True)
                await asyncio.sleep(1.0) # Safety backoff
                logger.error(f"Error in broadcast loop: {e}")


_coordinator: Optional[MonitorCoordinator] = None


def get_coordinator() -> MonitorCoordinator:
    global _coordinator
    if _coordinator is None:
        _coordinator = MonitorCoordinator()
    return _coordinator


@asynccontextmanager
async def lifespan(app: FastAPI):
    coordinator = get_coordinator()
    await coordinator.start()
    yield
    await coordinator.stop()


def create_app() -> FastAPI:
    app = FastAPI(
        title="PAL MCP Monitor Coordinator",
        description="Real-time monitoring coordinator for PAL MCP Server instances",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def log_transport_middleware(request: Request, call_next):
        client = request.scope.get("client")
        path = request.scope.get("path")
        if path != "/health":
            transport = "HTTP"
            if not client:
                transport = "Unix Socket"
            elif isinstance(client, (list, tuple)) and (len(client) == 0 or client[0] == "unix"):
                transport = "Unix Socket"
            logger.info(f"Request to {path} via {transport}")
        response = await call_next(request)
        return response

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(dashboard_path, encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except FileNotFoundError:
            return HTMLResponse(
                content="<h1>Dashboard not found</h1>",
                status_code=404,
            )

    @app.get("/history", response_class=HTMLResponse)
    async def history_page():
        history_path = os.path.join(os.path.dirname(__file__), "history.html")
        try:
            with open(history_path, encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except FileNotFoundError:
            return HTMLResponse(
                content="<h1>History view not found</h1>",
                status_code=404,
            )

    @app.get("/api/history")
    async def get_history():
        storage = get_storage_backend()
        conversations = storage.list_all(include_expired=True)
        formatted = {}
        for cid, (content, expires_at) in conversations.items():
            try:
                parsed_content = json.loads(content)
                formatted[cid] = {
                    "content": parsed_content,
                    "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
                }
            except json.JSONDecodeError:
                formatted[cid] = {"error": "Failed to parse content", "raw": content}
        return {"conversations": formatted}

    @app.post("/api/instances/{instance_id}/kill")
    async def kill_instance(instance_id: str):
        coordinator = get_coordinator()
        if instance_id not in coordinator.instances:
            raise HTTPException(status_code=404, detail="Instance not found")
        try:
            pid_str, host = instance_id.split("@", 1)
            pid = int(pid_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid instance ID format")
        try:
            logger.warning(f"Killing instance {instance_id} requested via API")
            os.kill(pid, signal.SIGTERM)
            async with coordinator._lock:
                if instance_id in coordinator.instances:
                    coordinator.instances[instance_id].state = "offline"
                    coordinator.instances[instance_id].last_status = "Terminated by user"
            return {"status": "ok", "message": f"Signal SIGTERM sent to PID {pid}"}
        except ProcessLookupError:
            async with coordinator._lock:
                if instance_id in coordinator.instances:
                    coordinator.instances[instance_id].state = "offline"
            return {"status": "ok", "message": "Process was already terminated"}
        except Exception as e:
            logger.error(f"Failed to kill instance {instance_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/instances/{instance_id}/interrupt")
    async def interrupt_instance_api(instance_id: str):
        """Signal a specific MCP server instance to interrupt its current task."""
        coordinator = get_coordinator()
        success = await coordinator.interrupt_instance(instance_id)
        if not success:
            raise HTTPException(status_code=404, detail="Instance not found")
        return {"status": "ok", "message": "Interruption signal queued"}

    @app.get("/health")
    async def health_check():
        coordinator = get_coordinator()
        return {"status": "healthy", "instances": len(coordinator.instances), "clients": len(coordinator.websocket_clients)}

    @app.get("/status")
    async def get_status():
        coordinator = get_coordinator()
        state = await coordinator.get_aggregated_state()
        return {"type": state.type, "timestamp": state.timestamp.isoformat(), "instances": [inst.to_dict() for inst in state.instances]}

    @app.post("/event")
    async def receive_event(event: dict):
        coordinator = get_coordinator()
        return await coordinator.process_event(event)

    @app.post("/events")
    async def receive_events(events: List[dict]):
        coordinator = get_coordinator()
        interrupted = False
        for event in events:
            resp = await coordinator.process_event(event)
            if resp.interrupted:
                interrupted = True
        return EventResponse(status="ok", interrupted=interrupted)

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        try:
            logger.info(f"WebSocket connection attempt from {websocket.client}")
            await websocket.accept()
            logger.info(f"WebSocket connection accepted from {websocket.client}")
            coordinator = get_coordinator()
            await coordinator.add_websocket_client(websocket)
            try:
                while True:
                    try:
                        data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                        if data == "ping":
                            await websocket.send_text("pong")
                    except asyncio.TimeoutError:
                        try:
                            await websocket.send_text('{"type": "ping"}')
                        except Exception:
                            break
            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected: {websocket.client}")
                pass
            except Exception as e:
                logger.error(f"WebSocket error in loop: {e}")
            finally:
                await coordinator.remove_websocket_client(websocket)
        except Exception as e:
            logger.error(f"WebSocket connection failed: {e}")

    return app


app = create_app()


def run_with_unix_socket(
    socket_path: str,
    ws_host: str = "0.0.0.0",
    ws_port: int = 9876,
    log_level: str = "info",
):
    import uvicorn
    if os.path.exists(socket_path):
        os.unlink(socket_path)
    logger.info(f"Starting coordinator on Unix socket: {socket_path}")
    uvicorn.run(app, uds=socket_path, log_level=log_level)


def run_with_http(
    host: str = "0.0.0.0",
    port: int = 9876,
    log_level: str = "info",
):
    import uvicorn
    logger.info(f"Starting coordinator on http://{host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    parser = argparse.ArgumentParser(description="PAL MCP Monitor Coordinator")
    parser.add_argument("--transport", choices=["http", "unix"], default="http")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9876)
    parser.add_argument("--socket", default="/tmp/pal-monitor.sock")
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()
    if args.transport == "unix":
        run_with_unix_socket(socket_path=args.socket, ws_host=args.host, ws_port=args.port, log_level=args.log_level)
    else:
        run_with_http(host=args.host, port=args.port, log_level=args.log_level)
