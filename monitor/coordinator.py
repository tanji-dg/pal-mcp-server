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
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse

from monitor.models import (
    AggregatedState,
    InstanceStatus,
    ToolCall,
    ToolEvent,
    ToolEventType,
    utc_now,
)
from utils.storage_backend import get_storage_backend

logger = logging.getLogger(__name__)

# Configuration constants
HEARTBEAT_INTERVAL = 10  # seconds between heartbeats
INSTANCE_TIMEOUT = 30  # seconds before marking instance as offline
MAX_RECENT_CALLS = 50  # maximum number of recent calls to keep per instance
BROADCAST_INTERVAL = 1.0  # seconds between state broadcasts


class InstanceTracker:
    """Tracks the status and history of a single MCP server instance."""

    def __init__(self, instance_id: str, uptime_seconds: float = 0.0):
        self.instance_id = instance_id
        self.uptime_at_register = uptime_seconds
        self.start_time = time.time()
        self.state = "idle"
        self.last_heartbeat = utc_now()
        
        # Support for nested/parallel tool calls
        self.active_tools: dict[str, datetime] = {} # tool_name -> start_time
        self.active_tool_inputs: dict[str, str] = {} # tool_name -> input
        self._tool_start_times: dict[str, datetime] = {} # tool_id -> start_time (for log-based duration tracking)
        
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
        self._tool_name_cache: dict[str, str] = {} # Map tool_id -> name
        
        # Reliability: track last completion to prevent late logs from reviving 'busy' state
        self.last_completion_time: datetime = utc_now()
        
        # Lifetime stats
        self.total_calls: int = 0
        self.total_errors: int = 0

        # Metrics for last window (1 hour)
        self._calls_1m: list[tuple[float, bool]] = []  # (timestamp, is_error)
        self._durations_1m: list[tuple[float, int]] = []  # (timestamp, duration_ms)

    def update_heartbeat(self, uptime_seconds: Optional[float] = None):
        """Update last heartbeat time."""
        self.last_heartbeat = utc_now()
        if uptime_seconds is not None:
            self.uptime_at_register = uptime_seconds
            self.start_time = time.time()

    def start_tool(self, tool_name: str, tool_input: Optional[str] = None):
        """Record tool execution start."""
        now = utc_now()
        self.state = "busy"
        self.active_tools[tool_name] = now
        self.active_tool_inputs[tool_name] = tool_input
        
        # Update primary display info
        self.active_tool = tool_name
        self.active_tool_input = tool_input
        self.tool_start_time = now
        self.last_heartbeat = now
        self.last_status = f"Starting {tool_name}..."

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
            is_fresh = self.active_tool is None and self.total_calls == 0
            is_start_event = log_data and ('"type": "tool_use"' in log_data or '"type": "item.started"' in log_data)
            
            if is_fresh or is_start_event:
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
                # Handle potential multiple JSON objects in one log chunk
                lines = log_data.strip().split("\n")
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
                    
                    # Capture session_id from log if available
                    if "session_id" in data:
                        self.session_id = data["session_id"]

                    # Unwrap Claude stream_event wrapper
                    if msg_type == "stream_event" and "event" in data and isinstance(data["event"], dict):
                        data = data["event"]
                        msg_type = data.get("type")

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

                    if msg_type == "message":
                        self.last_status = "Thinking"
                    elif msg_type == "tool_use":
                        # Support both 'name' and 'tool_name' keys
                        name = data.get("tool_name") or data.get("name") or "tool"
                        tool_id = data.get("tool_id")
                        if tool_id:
                            self._tool_name_cache[tool_id] = name
                            self._tool_start_times[tool_id] = now
                        self.last_status = f"Calling {name}"
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
                            start_t = self._tool_start_times.pop(tool_id)
                            duration_ms = int((now.timestamp() - start_t.timestamp()) * 1000)

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
                        now_ts = time.time()
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
                                        start_t = self._tool_start_times.pop(tool_id)
                                        duration_ms = int((now.timestamp() - start_t.timestamp()) * 1000)

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
                                    now_ts = time.time()
                                    self._calls_1m.append((now_ts, is_error))
                                    self._durations_1m.append((now_ts, duration_ms))

                    elif msg_type == "result": # Final result from Gemini CLI or Claude CLI
                        # Check if this is Claude CLI output (has 'subtype') or Gemini (has 'stats')
                        is_claude = "subtype" in data

                        status = data.get("status", "ok")

                        # Note: Final CLI result might be worth counting, but to be consistent with
                        # "only MCP tools count", we'll exclude it from stats and only update status.

                        if status == "error" or data.get("is_error"):
                            if is_claude:
                                # For Claude, check permission_denials
                                denials = data.get("permission_denials")
                                if denials:
                                    self.last_status = "Permission Denied"
                                else:
                                    self.last_status = "Error (Claude API)"
                            else:
                                self.last_status = "Error (Gemini API)"
                        else:
                            self.last_status = "Responding"

                        # When a final result is seen in logs, it's often followed immediately by process exit
                        # We keep it 'busy' until end_tool is called by MCP server

                    elif msg_type == "item.started": # Codex format
                        item = data.get("item", {})
                        item_id = item.get("id")
                        if item_id:
                            self._tool_start_times[item_id] = now

                        if item.get("type") == "command_execution":
                            self.last_status = f"Executing {item.get('command', 'cmd')}"
                        elif item.get("type") == "reasoning":
                            self.last_status = "Reasoning"
                    elif msg_type == "item.completed": # Codex completion/error
                        item = data.get("item", {})
                        status = item.get("status")
                        item_id = item.get("id")
                        item_type = item.get("type")

                        # Calculate duration
                        duration_ms = 0
                        if item_id and item_id in self._tool_start_times:
                            start_t = self._tool_start_times.pop(item_id)
                            duration_ms = int((now.timestamp() - start_t.timestamp()) * 1000)

                        if status in ["failed", "error"]:
                            cmd = item.get("command") or "operation"
                            self.last_status = f"Error in {cmd}"
                            is_error = True
                        else:
                            is_error = False
                            if item_type == "command_execution":
                                 self.last_status = f"Completed {item.get('command', 'cmd')}"

                        # Record metrics for command executions
                        if item_type == "command_execution":
                            self.total_calls += 1
                            if is_error:
                                self.total_errors += 1
                            
                            # Add to recent calls
                            cmd_name = item.get("command", "command").split(" ")[0]
                            call = ToolCall(
                                tool=cmd_name,
                                duration_ms=duration_ms,
                                status="error" if is_error else "success",
                                model_name=self.model_name,
                                timestamp=utc_now(),
                            )
                            self.recent_calls.appendleft(call)
                            
                            # Track window metrics
                            now_ts = time.time()
                            self._calls_1m.append((now_ts, is_error))
                            self._durations_1m.append((now_ts, duration_ms))
                    elif msg_type == "error":
                         # Capture error as payload if no result yet
                         if not payload:
                             payload = data
                    
                    # Handle raw API errors or objects with error field
                    error_obj = data.get("error")
                    if isinstance(error_obj, dict):
                        # Look for retry delay info
                        delay_info = ""
                        details = error_obj.get("details", [])
                        if isinstance(details, list):
                            for detail in details:
                                if not isinstance(detail, dict):
                                    continue
                                # Check metadata for quotaResetDelay
                                metadata = detail.get("metadata", {})
                                if isinstance(metadata, dict) and "quotaResetDelay" in metadata:
                                    delay_info = f" (Quota resets in {metadata['quotaResetDelay']})"
                                # Check retryDelay field
                                if "retryDelay" in detail:
                                    delay_info = f" (Retry in {detail['retryDelay']})"
                        
                        self.last_status = f"Rate Limited{delay_info}"
            except Exception:
                pass

            if not is_json_log:
                # Not JSON or parse failed, check for common patterns in text logs
                log_lower = log_data.lower()
                if "thinking" in log_lower:
                    self.last_status = "Thinking"
                elif "calling tool" in log_lower or "executing" in log_lower:
                    self.last_status = "Executing"
                elif "returning" in log_lower:
                    self.last_status = "Finishing"
                else:
                    if len(log_data) < 30:
                        self.last_status = log_data.strip()

    def end_tool(self, tool_name: Optional[str], duration_ms: int, is_error: bool = False, tool_output: Optional[str] = None, model_name: Optional[str] = None):
        """Record tool execution completion."""
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
            # Use provided model_name, or fall back to tracker's current model_name
            effective_model = model_name or (self.model_name if target_tool == self.active_tool else None)

            call = ToolCall(
                tool=target_tool,
                tool_input=self.active_tool_inputs.get(target_tool),
                tool_output=tool_output,
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
        # If the primary tool finished, force idle state even if sub-tools (from logs) seem active
        # Also specifically handle 'clink' which acts as a container
        is_primary_completion = (target_tool and target_tool == self.active_tool)
        is_clink_completion = (target_tool == "clink")

        if not self.active_tools or is_primary_completion or is_clink_completion:
            self.state = "idle"
            self.last_status = f"Completed {target_tool} ({status})" if target_tool else "Idle"
            self.active_tool = None
            self.active_tool_input = None
            self.session_id = None
            self.model_name = None
            self.tool_start_time = None
            self.active_tools.clear() # Ensure all are cleared
            self.active_tool_inputs.clear()
        else:
            # Still busy with other tools (e.g. parent clink)
            self.state = "busy"
            # Pick one of the remaining tools as primary for display
            self.active_tool = list(self.active_tools.keys())[-1]
            self.tool_start_time = self.active_tools[self.active_tool]
            self.last_status = f"Finished {target_tool}, back to {self.active_tool}"
            
        self.last_heartbeat = utc_now()

    def get_uptime(self) -> float:
        """Calculate current uptime in seconds."""
        if self.is_timed_out() or self.state == "offline":
            # If offline, freeze uptime at last heartbeat
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
        """Remove metrics older than 1 hour (3600s)."""
        cutoff = time.time() - 3600
        self._calls_1m = [(t, e) for t, e in self._calls_1m if t > cutoff]
        self._durations_1m = [(t, d) for t, d in self._durations_1m if t > cutoff]

    def is_timed_out(self) -> bool:
        """Check if instance has exceeded heartbeat timeout."""
        return utc_now() - self.last_heartbeat > timedelta(seconds=INSTANCE_TIMEOUT)

    def to_status(self) -> InstanceStatus:
        """Convert to InstanceStatus model."""
        state = "offline" if self.is_timed_out() else self.state
        return InstanceStatus(
            instance_id=self.instance_id,
            uptime_seconds=self.get_uptime(),
            state=state,
            last_heartbeat=self.last_heartbeat,
            active_tool=self.active_tool if state == "busy" else None,
            session_id=self.session_id if state == "busy" else None,
            model_name=self.model_name if state == "busy" else None,
            active_role=self.active_role if state == "busy" else None,
            tool_start_time=self.tool_start_time if state == "busy" else None,
            recent_calls=list(self.recent_calls),
            error_rate_1m=self.get_error_rate_1m(),
            avg_execution_time_1m=self.get_avg_execution_time_1m(),
            last_status=self.last_status,
            total_calls=self.total_calls,
            total_errors=self.total_errors,
        )


class MonitorCoordinator:
    """
    Central coordinator for MCP server monitoring.

    Manages:
    - Instance registration and tracking
    - Event processing from MCP servers
    - WebSocket connections from dashboards
    - Periodic state broadcasts
    """

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

    async def process_event(self, event: ToolEvent):
        """Process an incoming event from an MCP server instance."""
        broadcast_log_event = None

        async with self._lock:
            instance_id = event.instance_id

            if event.event_type == ToolEventType.REGISTER:
                uptime = event.uptime_seconds or 0.0
                self.instances[instance_id] = InstanceTracker(instance_id, uptime)
                logger.info(f"Instance registered: {instance_id}")

            elif event.event_type == ToolEventType.UNREGISTER:
                if instance_id in self.instances:
                    # Don't delete, just mark as offline to keep history in dashboard
                    self.instances[instance_id].state = "offline"
                    logger.info(f"Instance marked offline: {instance_id}")

            elif instance_id in self.instances:
                tracker = self.instances[instance_id]

                if event.event_type == ToolEventType.HEARTBEAT:
                    tracker.update_heartbeat(event.uptime_seconds)

                elif event.event_type == ToolEventType.TOOL_START:
                    if event.tool_name:
                        tracker.start_tool(event.tool_name, event.tool_input)
                        logger.debug(f"Tool started: {event.tool_name} on {instance_id}")

                elif event.event_type == ToolEventType.TOOL_END:
                    tracker.end_tool(event.tool_name, event.duration_ms or 0, is_error=False, tool_output=event.tool_output, model_name=event.model_name)
                    logger.debug(f"Tool completed: {event.tool_name} on {instance_id} " f"({event.duration_ms}ms)")

                elif event.event_type == ToolEventType.TOOL_ERROR:
                    tracker.end_tool(event.tool_name, event.duration_ms or 0, is_error=True, model_name=event.model_name)
                    logger.warning(f"Tool error: {event.tool_name} on {instance_id} - " f"{event.error_message}")

                elif event.event_type == ToolEventType.TOOL_LOG:
                    # Mark as busy since we are receiving logs
                    if event.tool_name:
                        tracker.log_activity(event.tool_name, event.log_data, original_event=event)
                    # Mark for broadcast after releasing lock
                    broadcast_log_event = event

            else:
                # Auto-register instance on first event
                self.instances[instance_id] = InstanceTracker(instance_id, event.uptime_seconds or 0.0)
                logger.info(f"Instance auto-registered: {instance_id}")
                # Process the event now that instance exists
                tracker = self.instances[instance_id]
                if event.event_type == ToolEventType.TOOL_START:
                    if event.tool_name:
                        tracker.start_tool(event.tool_name, event.tool_input)
                elif event.event_type == ToolEventType.TOOL_END:
                    tracker.end_tool(event.tool_name, event.duration_ms or 0, is_error=False, tool_output=event.tool_output, model_name=event.model_name)
                elif event.event_type == ToolEventType.TOOL_ERROR:
                    tracker.end_tool(event.tool_name, event.duration_ms or 0, is_error=True, model_name=event.model_name)
                elif event.event_type == ToolEventType.TOOL_LOG:
                    if event.tool_name:
                        tracker.log_activity(event.tool_name, event.log_data, original_event=event)
                    broadcast_log_event = event
                elif event.event_type == ToolEventType.HEARTBEAT:
                    tracker.update_heartbeat(event.uptime_seconds)

        # Broadcast log if needed (outside lock to prevent deadlock)
        if broadcast_log_event:
            await self.broadcast_log(broadcast_log_event)

    async def broadcast_log(self, event: ToolEvent):
        """Broadcast log event to all connected clients."""
        if not self.websocket_clients:
            return

        # Prepare message for dashboard
        # Add type field explicitly if not present in serialized output or different from event_type
        message = event.to_json()
        
        # Broadcast to all clients
        async with self._lock:
            disconnected = set()
            for client in self.websocket_clients:
                try:
                    await client.send_text(message)
                except Exception:
                    disconnected.add(client)

            # Remove disconnected clients
            for client in disconnected:
                self.websocket_clients.discard(client)

    async def add_websocket_client(self, websocket: WebSocket):
        """Add a new WebSocket client connection."""
        async with self._lock:
            self.websocket_clients.add(websocket)
            logger.info(f"Dashboard connected. Total clients: {len(self.websocket_clients)}")

        # Send initial state immediately
        state = await self.get_aggregated_state()
        try:
            await websocket.send_text(state.to_json())
            
            # Send buffered logs from all instances
            async with self._lock:
                for instance in self.instances.values():
                    # Send oldest first
                    for log_event in instance.recent_logs:
                        await websocket.send_text(log_event.to_json())
                        
        except Exception as e:
            logger.warning(f"Failed to send initial data: {e}")

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

                state = await self.get_aggregated_state()
                message = state.to_json()

                # Broadcast to all clients
                async with self._lock:
                    disconnected = set()
                    for client in self.websocket_clients:
                        try:
                            await client.send_text(message)
                        except Exception:
                            disconnected.add(client)

                    # Remove disconnected clients
                    for client in disconnected:
                        self.websocket_clients.discard(client)
                        logger.debug("Removed stale WebSocket client")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in broadcast loop: {e}")


# Global coordinator instance
_coordinator: Optional[MonitorCoordinator] = None


def get_coordinator() -> MonitorCoordinator:
    """Get or create the global coordinator instance."""
    global _coordinator
    if _coordinator is None:
        _coordinator = MonitorCoordinator()
    return _coordinator


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage coordinator lifecycle with the FastAPI app."""
    coordinator = get_coordinator()
    await coordinator.start()
    yield
    await coordinator.stop()


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    app = FastAPI(
        title="PAL MCP Monitor Coordinator",
        description="Real-time monitoring coordinator for PAL MCP Server instances",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def log_transport_middleware(request: Request, call_next):
        """Log the transport type (HTTP or Unix socket)."""
        # Determine transport based on client address or scope
        # Uvicorn sets scope['client'] to None or ['unix'] for Unix sockets depending on version/config
        # For TCP/HTTP, it's usually (host, port)

        client = request.scope.get("client")
        path = request.scope.get("path")

        # Skip health checks to reduce noise
        if path != "/health":
            transport = "HTTP"
            if not client:
                # Often None for Unix sockets in some ASGI implementations
                transport = "Unix Socket"
            elif isinstance(client, (list, tuple)) and (len(client) == 0 or client[0] == "unix"):
                transport = "Unix Socket"

            logger.info(f"Request to {path} via {transport}")

        response = await call_next(request)
        return response

    # Serve dashboard HTML
    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        """Serve the monitoring dashboard."""
        dashboard_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
        try:
            with open(dashboard_path, encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except FileNotFoundError:
            return HTMLResponse(
                content="<h1>Dashboard not found</h1><p>dashboard.html is missing</p>",
                status_code=404,
            )

    @app.get("/history", response_class=HTMLResponse)
    async def history_page():
        """Serve the conversation history viewer."""
        history_path = os.path.join(os.path.dirname(__file__), "history.html")
        try:
            with open(history_path, encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        except FileNotFoundError:
            return HTMLResponse(
                content="<h1>History view not found</h1><p>history.html is missing</p>",
                status_code=404,
            )

    @app.get("/api/history")
    async def get_history():
        """Get conversation history from storage."""
        storage = get_storage_backend()
        conversations = storage.list_all(include_expired=True)
        
        # Format for frontend
        formatted = {}
        for cid, (content, expires_at) in conversations.items():
            try:
                # Content is stored as JSON string
                parsed_content = json.loads(content)
                formatted[cid] = {
                    "content": parsed_content,
                    "expires_at": datetime.fromtimestamp(expires_at, tz=timezone.utc).isoformat()
                }
            except json.JSONDecodeError:
                formatted[cid] = {
                    "error": "Failed to parse content",
                    "raw": content
                }
        return {"conversations": formatted}

    @app.post("/api/instances/{instance_id}/kill")
    async def kill_instance(instance_id: str):
        """Terminate a specific MCP server instance."""
        coordinator = get_coordinator()
        
        # 1. Check if instance is tracked
        if instance_id not in coordinator.instances:
            raise HTTPException(status_code=404, detail="Instance not found")
            
        # 2. Extract PID
        try:
            pid_str, host = instance_id.split("@", 1)
            pid = int(pid_str)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid instance ID format (expected PID@HOSTNAME)")

        # 3. Terminate process
        try:
            logger.warning(f"Killing instance {instance_id} (PID {pid}) requested via API")
            os.kill(pid, signal.SIGTERM)
            
            # Mark as offline immediately
            async with coordinator._lock:
                if instance_id in coordinator.instances:
                    coordinator.instances[instance_id].state = "offline"
                    coordinator.instances[instance_id].last_status = "Terminated by user"
            
            return {"status": "ok", "message": f"Signal SIGTERM sent to PID {pid}"}
        except ProcessLookupError:
            # Process already gone
            async with coordinator._lock:
                if instance_id in coordinator.instances:
                    coordinator.instances[instance_id].state = "offline"
            return {"status": "ok", "message": "Process was already terminated"}
        except PermissionError:
            raise HTTPException(status_code=403, detail="Permission denied to kill process")
        except Exception as e:
            logger.error(f"Failed to kill instance {instance_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/health")
    async def health_check():
        """Health check endpoint."""
        coordinator = get_coordinator()
        return {
            "status": "healthy",
            "instances": len(coordinator.instances),
            "clients": len(coordinator.websocket_clients),
        }

    @app.get("/status")
    async def get_status():
        """Get current aggregated status as JSON."""
        coordinator = get_coordinator()
        state = await coordinator.get_aggregated_state()
        return {
            "type": state.type,
            "timestamp": state.timestamp.isoformat(),
            "instances": [inst.to_dict() for inst in state.instances],
        }

    @app.post("/event")
    async def receive_event(event: ToolEvent):
        """Receive an event from an MCP server instance."""
        coordinator = get_coordinator()
        await coordinator.process_event(event)
        return {"status": "ok"}

    @app.post("/events")
    async def receive_events(events: list[ToolEvent]):
        """Receive multiple events from an MCP server instance."""
        coordinator = get_coordinator()
        for event in events:
            await coordinator.process_event(event)
        return {"status": "ok", "processed": len(events)}

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket):
        """WebSocket endpoint for real-time dashboard updates."""
        await websocket.accept()
        coordinator = get_coordinator()
        await coordinator.add_websocket_client(websocket)

        try:
            while True:
                # Keep connection alive, handle any incoming messages
                try:
                    data = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                    # Handle ping/pong or other client messages if needed
                    if data == "ping":
                        await websocket.send_text("pong")
                except asyncio.TimeoutError:
                    # Send keepalive ping
                    try:
                        await websocket.send_text('{"type": "ping"}')
                    except Exception:
                        break
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.debug(f"WebSocket error: {e}")
        finally:
            await coordinator.remove_websocket_client(websocket)

    return app


# Create the app instance for uvicorn
app = create_app()


def run_with_unix_socket(
    socket_path: str,
    ws_host: str = "0.0.0.0",
    ws_port: int = 9876,
    log_level: str = "info",
):
    """
    Run the coordinator with Unix socket for MCP communication
    and WebSocket server for dashboard connections.

    Args:
        socket_path: Path to Unix socket for MCP server events
        ws_host: Host for WebSocket server (dashboards)
        ws_port: Port for WebSocket server
        log_level: Logging level
    """
    import uvicorn

    # Remove existing socket file if present
    if os.path.exists(socket_path):
        os.unlink(socket_path)

    logger.info(f"Starting coordinator on Unix socket: {socket_path}")
    logger.info(f"WebSocket server will be available at ws://{ws_host}:{ws_port}/ws")

    # Run with Unix socket
    uvicorn.run(
        app,
        uds=socket_path,
        log_level=log_level,
    )


def run_with_http(
    host: str = "0.0.0.0",
    port: int = 9876,
    log_level: str = "info",
):
    """
    Run the coordinator with HTTP for both MCP and dashboard connections.

    Args:
        host: Host to bind to
        port: Port to listen on
        log_level: Logging level
    """
    import uvicorn

    logger.info(f"Starting coordinator on http://{host}:{port}")
    logger.info(f"WebSocket endpoint: ws://{host}:{port}/ws")

    uvicorn.run(
        app,
        host=host,
        port=port,
        log_level=log_level,
    )


if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    parser = argparse.ArgumentParser(description="PAL MCP Monitor Coordinator")
    parser.add_argument(
        "--transport",
        choices=["http", "unix"],
        default="http",
        help="Transport type (default: http)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="HTTP host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=9876,
        help="HTTP port (default: 9876)",
    )
    parser.add_argument(
        "--socket",
        default="/tmp/pal-monitor.sock",
        help="Unix socket path (default: /tmp/pal-monitor.sock)",
    )
    parser.add_argument(
        "--log-level",
        default="info",
        help="Log level (default: info)",
    )

    args = parser.parse_args()

    if args.transport == "unix":
        run_with_unix_socket(
            socket_path=args.socket,
            ws_host=args.host,
            ws_port=args.port,
            log_level=args.log_level,
        )
    else:
        run_with_http(
            host=args.host,
            port=args.port,
            log_level=args.log_level,
        )