import unittest
import asyncio
import time
import json
from monitor.coordinator import MonitorCoordinator
from monitor.models import ToolEvent, ToolEventType

class TestMonitorStatistics(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.coordinator = MonitorCoordinator()
        # Mock broadcast_loop to prevent hanging
        async def mock_loop(): return
        self.coordinator._broadcast_loop = mock_loop
        await self.coordinator.start()
        
        self.instance_id = "test_instance"
        # Register instance
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "register"
        })

    async def asyncTearDown(self):
        await self.coordinator.stop()

    async def test_full_metrics_aggregation(self):
        """Verify that all major metrics (calls, errors, tokens, time) are correctly aggregated."""
        
        # 1. Start a tool (clink)
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_start",
            "tool_name": "clink"
        })

        # 2. Report initial tokens (Should be treated as baseline -> 0 display)
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_log",
            "tool_name": "clink",
            "log_data": json.dumps({"type": "usage", "usage": {"input_tokens": 1000, "output_tokens": 100}})
        })
        
        state = await self.coordinator.get_aggregated_state()
        self.assertEqual(state.total_input_tokens, 0, "First token report should be baseline")

        # 3. Report more tokens (+500 input, +50 output)
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_log",
            "tool_name": "clink",
            "log_data": json.dumps({"type": "usage", "usage": {"input_tokens": 1500, "output_tokens": 150}})
        })

        # 4. Finish a sub-tool (Add 1 call and execution time)
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_log",
            "tool_name": "clink",
            "log_data": json.dumps({"type": "tool_result", "tool_id": "t1", "status": "success", "content": "ok"}),
            "timestamp": "2026-01-20T12:00:01.000Z" 
        })
        # Note: durations for log-based sub-tools depend on seeing a tool_use first to record start time.
        # Here we just check if it increments calls.

        # 5. Finish primary tool with error
        await self.coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_end",
            "tool_name": "clink",
            "duration_ms": 500,
            "is_error": True
        })

        state = await self.coordinator.get_aggregated_state()
        
        # Assertions
        self.assertEqual(state.total_calls, 2, "Should count 1 sub-tool result + 1 primary tool end")
        self.assertEqual(state.total_errors, 1, "Should count the primary tool error")
        self.assertEqual(state.total_input_tokens, 500, "Should count the delta (1500 - 1000)")
        self.assertEqual(state.total_output_tokens, 50, "Should count the delta (150 - 100)")
        self.assertEqual(state.total_execution_ms, 500, "Should aggregate execution time from tool_end")

    async def test_restart_behavior(self):
        """Verify that monitor restart (new coordinator) starts from 0 effectively."""
        # Scenario: Agent was already at 5000 tokens
        # New Monitor starts
        new_coordinator = MonitorCoordinator()
        await new_coordinator.process_event({"instance_id": self.instance_id, "event_type": "register"})
        
        # First report from 'alive' agent
        await new_coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_log",
            "tool_name": "clink",
            "log_data": json.dumps({"type": "usage", "usage": {"input_tokens": 5000}})
        })
        
        state = await new_coordinator.get_aggregated_state()
        self.assertEqual(state.total_input_tokens, 0, "Restarted monitor should baseline existing agent tokens to 0")
        
        # Next increment
        await new_coordinator.process_event({
            "instance_id": self.instance_id,
            "event_type": "tool_log",
            "tool_name": "clink",
            "log_data": json.dumps({"type": "usage", "usage": {"input_tokens": 5100}})
        })
        
        state = await new_coordinator.get_aggregated_state()
        self.assertEqual(state.total_input_tokens, 100, "Should only count delta after restart baseline")

if __name__ == "__main__":
    unittest.main()