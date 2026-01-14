"""Gemini-specific CLI agent hooks."""

from __future__ import annotations

import json
from typing import Any

from clink.models import ResolvedCLIClient
from clink.parsers.base import ParsedCLIResponse

from .base import AgentOutput, BaseCLIAgent


class GeminiAgent(BaseCLIAgent):
    """Gemini-specific behaviour."""

    def __init__(self, client: ResolvedCLIClient):
        # Prefer the local built version if it exists in the source tree
        from config import PROJECT_ROOT

        local_built_script = PROJECT_ROOT / "gemini-cli" / "gemini-built.sh"
        if local_built_script.exists() and client.executable == ["gemini"]:
            # Create a modified client with the local path
            # We use model_copy to keep the original client object immutable where possible
            modified_client = client.model_copy(update={"executable": [str(local_built_script)]})
            super().__init__(modified_client)
        else:
            super().__init__(client)

    def _build_command(self, *, role: ResolvedCLIRole, system_prompt: str | None) -> list[str]:
        """Prioritize certain flags like --model for Gemini CLI."""
        base = super()._build_command(role=role, system_prompt=system_prompt)

        # Reorder to put --model or -m first if present
        model_flag = None
        model_value = None
        other_args = []

        # Start from index 1 to skip the executable
        it = iter(range(1, len(base)))
        for i in it:
            arg = base[i]
            if arg in ("--model", "-m") and i + 1 < len(base):
                model_flag = arg
                model_value = base[i + 1]
                next(it)  # Skip the value
            else:
                other_args.append(arg)

        final = [base[0]]
        if model_flag:
            final.extend([model_flag, model_value])
        final.extend(other_args)

        return final

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
        combined = "\n".join(part for part in (stderr, stdout) if part)
        if not combined:
            return None

        brace_index = combined.find("{")
        if brace_index == -1:
            return None

        json_candidate = combined[brace_index:]
        try:
            payload: dict[str, Any] = json.loads(json_candidate)
        except json.JSONDecodeError:
            return None

        error_block = payload.get("error")
        if not isinstance(error_block, dict):
            return None

        code = error_block.get("code")
        err_type = error_block.get("type")
        detail_message = error_block.get("message")

        prologue = combined[:brace_index].strip()
        lines: list[str] = []
        if prologue and (not detail_message or prologue not in detail_message):
            lines.append(prologue)
        if detail_message:
            lines.append(detail_message)

        header = "Gemini CLI reported a tool failure"
        if code:
            header = f"{header} ({code})"
        elif err_type:
            header = f"{header} ({err_type})"

        content_lines = [header.rstrip(".") + "."]
        content_lines.extend(lines)
        message = "\n".join(content_lines).strip()

        metadata = {
            "cli_error_recovered": True,
            "cli_error_code": code,
            "cli_error_type": err_type,
            "cli_error_payload": payload,
        }

        parsed = ParsedCLIResponse(content=message or header, metadata=metadata)
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
