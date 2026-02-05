"""Codex-specific CLI agent hooks."""

from __future__ import annotations

import asyncio
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from clink.parsers.base import ParserError

from .base import AgentOutput, BaseCLIAgent


class CodexAgent(BaseCLIAgent):
    """Codex CLI agent with JSONL recovery support."""

    def __init__(self, client: ResolvedCLIClient):
        super().__init__(client)

    async def on_stream_line(
        self, line: str, stream_name: str, process: asyncio.subprocess.Process
    ) -> None:
        """Detect logical completion in Codex JSONL stream."""
        if '"type":"turn.completed"' in line:
            self._logger.info("Codex turn.completed detected in stream. Signaling completion.")
            self._completion_event.set()

    def _build_command(
        self, *, role: ResolvedCLIRole, system_prompt: str | None, native_session_id: str | None = None
    ) -> list[str]:
        base = list(self.client.executable)
        
        # If we have a native session ID, use 'resume' subcommand
        if native_session_id:
            # We assume internal_args contains 'exec', we replace it with 'resume'
            # or just append 'resume <id>'.
            # Based on 'codex --help', it's 'codex resume <id>'
            if "exec" in self.client.internal_args:
                new_internal = [arg if arg != "exec" else "resume" for arg in self.client.internal_args]
                base.extend(new_internal)
            else:
                base.append("resume")
            
            base.append(native_session_id)
            base.append("-")
        else:
            base.extend(self.client.internal_args)

        base.extend(self.client.config_args)
        base.extend(role.role_args)

        return base

    def _recover_from_error(
        self,
        *,
        returncode: int,
        stdout: str,
        stderr: str,
        sanitized_command: list[str],
        duration_seconds: float,
        output_file_content: str | None,
    ) -> AgentOutput | None:
        try:
            parsed = self._parser.parse(stdout, stderr)
        except ParserError:
            return None

        return AgentOutput(
            parsed=parsed,
            sanitized_command=sanitized_command,
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            duration_seconds=duration_seconds,
            parser_name=self._parser.name,
            output_file_content=output_file_content,
        )
