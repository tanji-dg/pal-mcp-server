"""Execute configured CLI agents for the clink tool and parse output."""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from clink.constants import DEFAULT_STREAM_LIMIT, FATAL_ERROR_KEYWORDS
from clink.models import ResolvedCLIClient, ResolvedCLIRole
from clink.parsers import BaseParser, ParsedCLIResponse, ParserError, get_parser

logger = logging.getLogger("clink.agent")

def get_publisher():
    from monitor.publisher import get_publisher as _get_publisher
    return _get_publisher()


@dataclass
class AgentOutput:
    """Container returned by CLI agents after successful execution."""

    parsed: ParsedCLIResponse
    sanitized_command: list[str]
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    parser_name: str
    output_file_content: str | None = None


class CLIAgentError(RuntimeError):
    """Raised when a CLI agent fails (non-zero exit, timeout, parse errors)."""

    def __init__(self, message: str, *, returncode: int | None = None, stdout: str = "", stderr: str = "") -> None:
        super().__init__(message)
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class BaseCLIAgent:
    """Execute a configured CLI command and parse its output."""

    def __init__(self, client: ResolvedCLIClient):
        self.client = client
        self._parser: BaseParser = get_parser(client.parser)
        self._logger = logging.getLogger(f"clink.runner.{client.name}")

    async def run(
        self,
        *,
        role: ResolvedCLIRole,
        prompt: str,
        system_prompt: str | None = None,
        files: Sequence[str],
        images: Sequence[str],
        output_callback: callable[[str], None] | None = None,
    ) -> AgentOutput:
        # Files and images are already embedded into the prompt by the tool; they are
        # accepted here only to keep parity with SimpleTool callers.
        _ = (files, images)
        # The runner simply executes the configured CLI command for the selected role.
        command = self._build_command(role=role, system_prompt=system_prompt)
        env = self._build_environment()

        # Resolve executable path for cross-platform compatibility (especially Windows)
        executable_name = command[0]
        resolved_executable = shutil.which(executable_name)
        if resolved_executable is None:
            raise CLIAgentError(
                f"Executable '{executable_name}' not found in PATH for CLI '{self.client.name}'. "
                f"Ensure the command is installed and accessible."
            )
        command[0] = resolved_executable

        sanitized_command = list(command)

        cwd = str(self.client.working_dir) if self.client.working_dir else None
        limit = DEFAULT_STREAM_LIMIT

        output_file_content: str | None = None
        start_time = time.monotonic()

        output_file_path: Path | None = None
        command_with_output_flag = list(command)

        if self.client.output_to_file:
            fd, tmp_path = tempfile.mkstemp(prefix="clink-", suffix=".json")
            os.close(fd)
            output_file_path = Path(tmp_path)
            flag_template = self.client.output_to_file.flag_template
            try:
                rendered_flag = flag_template.format(path=str(output_file_path))
            except KeyError as exc:  # pragma: no cover - defensive
                raise CLIAgentError(f"Invalid output flag template '{flag_template}': missing placeholder {exc}")
            command_with_output_flag.extend(shlex.split(rendered_flag))
            sanitized_command = list(command_with_output_flag)

        self._logger.debug("Executing CLI command: %s", " ".join(sanitized_command))
        if cwd:
            self._logger.debug("Working directory: %s", cwd)

        try:
            process = await asyncio.create_subprocess_exec(
                *command_with_output_flag,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=cwd,
                limit=limit,
                env=env,
            )
        except FileNotFoundError as exc:
            raise CLIAgentError(f"Executable not found for CLI '{self.client.name}': {exc}") from exc

        # Write prompt to stdin
        if process.stdin:
            try:
                process.stdin.write(prompt.encode("utf-8"))
                await process.stdin.drain()
                process.stdin.close()
                await process.stdin.wait_closed()
            except Exception as exc:
                self._logger.warning(f"Failed to write to stdin: {exc}")

        # Resolve effective timeouts
        # Resolve effective timeouts, allowing role-level 0 to override defaults
        total_timeout = self.client.default_total_timeout_seconds
        if role.total_timeout_seconds is not None:
            total_timeout = role.total_timeout_seconds

        idle_timeout = self.client.default_idle_timeout_seconds
        if role.idle_timeout_seconds is not None:
            idle_timeout = role.idle_timeout_seconds

        self._logger.debug(f"Using total_timeout: {total_timeout}s, idle_timeout: {idle_timeout}s")

        # Stream output while buffering for final result
        stdout_buffer = []
        stderr_buffer = []
        activity_event = asyncio.Event()  # Event to signal activity on streams

        async def _read_stream(
            stream,
            buffer,
            log_level,
            activity_event: asyncio.Event,
            output_callback: callable[[str], None] | None,
            stream_name: str,
        ):
            publisher = get_publisher()
            
            self._logger.debug(f"[_read_stream] Starting for stream: {stream_name}")
            while True:
                # Check for interruption from dashboard/monitor
                if publisher.is_interrupted():
                    self._logger.warning(f"Interruption requested. Killing process {process.pid}")
                    try:
                        process.kill()
                    except ProcessLookupError:
                        pass
                    raise InterruptedError("Task interrupted by user via monitor dashboard")

                line = await stream.readline()
                self._logger.debug(f"[_read_stream] Read line from {stream_name}: {line!r}")
                if not line:
                    self._logger.debug(f"[_read_stream] EOF for stream: {stream_name}")
                    break
                decoded_line = line.decode("utf-8", errors="replace")
                buffer.append(decoded_line)
                
                # Check for fatal errors in stderr to abort early (e.g. quota exhausted)
                if stream_name == "stderr":
                    for keyword in FATAL_ERROR_KEYWORDS:
                        if keyword in decoded_line:
                            self._logger.error(f"Detected fatal error keyword '{keyword}' in stderr. Aborting.")
                            raise CLIAgentError(
                                f"Fatal CLI error detected: {decoded_line.strip()}",
                                stdout="".join(stdout_buffer),
                                stderr="".join(stderr_buffer) + decoded_line,
                            )

                # Emit real-time log entry for tests and callers (format eagerly so mocks capture text)
                self._logger.debug("[CLI OUTPUT] %s", decoded_line.rstrip("\n"))
                # Invoke output callback if provided (supports sync and async)
                if output_callback:
                    try:
                        maybe_coro = output_callback(decoded_line)
                        if asyncio.iscoroutine(maybe_coro):
                            await maybe_coro
                    except Exception as exc:  # pragma: no cover - defensive guard
                        self._logger.warning("Output callback failed: %s", exc)
                activity_event.set()  # Signal activity
                # activity_event.clear() # Clear after processing for next wait - removed, handled by monitor

        async def _idle_timeout_monitor(
            process: asyncio.subprocess.Process, activity_event: asyncio.Event, idle_timeout: int
        ):

            while True:
                try:
                    # Wait for activity, or idle timeout if no activity
                    await asyncio.wait_for(
                        activity_event.wait(),
                        timeout=idle_timeout if idle_timeout is not None and idle_timeout > 0 else None,
                    )
                    activity_event.clear()  # Reset for next cycle
                except asyncio.TimeoutError:
                    # Idle timeout occurred, no activity for 'idle_timeout' seconds
                    self._logger.warning(
                        f"CLI '{self.client.name}' idle timed out after {idle_timeout} seconds. Terminating process."
                    )
                    if process.returncode is None:  # Guard against already terminated process
                        try:
                            process.kill()
                        except ProcessLookupError:
                            pass
                    # Raise an exception to break the gather early
                    raise CLIAgentError(f"CLI '{self.client.name}' idle timed out after {idle_timeout} seconds")
                except asyncio.CancelledError:
                    # Monitor task was cancelled, meaning main process completed or total timeout hit
                    break
                # Small delay to prevent busy-waiting if event is set/cleared very rapidly
                try:  # Add try-except around sleep
                    await asyncio.sleep(
                        0.01
                    )  # Small delay to prevent busy-waiting if event is set/cleared very rapidly
                except asyncio.CancelledError:
                    break  # Break if cancelled during sleep

        async def _interruption_monitor(process: asyncio.subprocess.Process):
            """Dedicated task to monitor interruption requests from the dashboard."""
            publisher = get_publisher()
            while process.returncode is None: # Exit loop if process finished
                try:
                    if await publisher.check_interruption():
                        self._logger.warning(f"Interruption signal received via monitor. Killing process {process.pid}")
                        try:
                            if process.returncode is None:
                                process.kill()
                        except Exception as e:
                            self._logger.debug(f"Failed to kill process: {e}")
                        raise InterruptedError("Task interrupted by user via monitor dashboard")
                    
                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    break
                except InterruptedError:
                    raise # Propagate to gather
                except Exception as e:
                    self._logger.debug(f"Interruption monitor encountered error: {e}")
                    await asyncio.sleep(1.0)

        # Setup tasks for stream monitoring
        stream_tasks = [
            asyncio.create_task(
                _read_stream(
                    process.stdout,
                    stdout_buffer,
                    logging.DEBUG,
                    activity_event,
                    output_callback,
                    "stdout",
                )
            ),
            asyncio.create_task(
                _read_stream(
                    process.stderr,
                    stderr_buffer,
                    logging.DEBUG,
                    activity_event,
                    output_callback,
                    "stderr",
                )
            ),
        ]

        # Start process.wait() as a separate task
        process_wait_task = asyncio.create_task(process.wait())
        tasks_to_gather = stream_tasks + [process_wait_task]

        # Add interruption monitor to active tasks
        interrupt_monitor_task = asyncio.create_task(_interruption_monitor(process))
        tasks_to_gather.append(interrupt_monitor_task)

        idle_monitor_task = None
        if idle_timeout is not None and idle_timeout > 0:
            idle_monitor_task = asyncio.create_task(_idle_timeout_monitor(process, activity_event, idle_timeout))
            tasks_to_gather.append(idle_monitor_task)

            # Add a done callback to process_wait_task to cancel idle_monitor_task
            def _cancel_idle_monitor(fut):
                if idle_monitor_task and not idle_monitor_task.done():
                    idle_monitor_task.cancel()

            process_wait_task.add_done_callback(_cancel_idle_monitor)

        try:
            # Use total_timeout for the entire gather operation
            await asyncio.wait_for(
                asyncio.gather(*tasks_to_gather),
                timeout=total_timeout if total_timeout is not None and total_timeout > 0 else None,
            )
        except InterruptedError:
            # Process was already killed in _read_stream, escalate immediately
            # Try a quick cleanup but don't block
            if process.returncode is None:
                try:
                    # Non-blocking check/cleanup
                    await asyncio.wait_for(process.wait(), timeout=0.1)
                except Exception:
                    pass
            
            # Extract partial output for restoration
            stdout_text = "".join(stdout_buffer)
            stderr_text = "".join(stderr_buffer)
            duration = time.monotonic() - start_time
            
            # Re-raise with partial results attached so the tool can use them
            raise CLIAgentError(
                "Task interrupted by user",
                returncode=process.returncode,
                stdout=stdout_text,
                stderr=stderr_text
            )
        except asyncio.TimeoutError as exc:
            # Total timeout occurred for the entire operation
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            
            # Cancel all subtasks
            for t in tasks_to_gather:
                if not t.done():
                    t.cancel()
            
            # Wait for tasks to handle cancellation
            try:
                await asyncio.gather(*tasks_to_gather, return_exceptions=True)
            except Exception:
                pass

            raise CLIAgentError(
                f"CLI '{self.client.name}' total timed out after {total_timeout} seconds",
                returncode=None,
            ) from exc
        except asyncio.CancelledError:
            # External cancellation
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            raise
        except Exception:
            # Any other error (including our new idle timeout CLIAgentError)
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
            raise

        finally:
            # Ensure all tasks are cancelled and cleaned up properly
            for t in tasks_to_gather:
                if not t.done():
                    t.cancel()
            
            # Wait for tasks to settle without crashing
            try:
                await asyncio.wait(tasks_to_gather, timeout=0.2)
            except Exception:
                pass

            # Final check to ensure process is truly dead
            if process.returncode is None:
                try:
                    process.kill()
                    # Final non-blocking wait
                    await asyncio.wait_for(process.wait(), timeout=0.1)
                except Exception:
                    pass

        stdout_text = "".join(stdout_buffer)
        stderr_text = "".join(stderr_buffer)
        duration = time.monotonic() - start_time
        return_code = process.returncode  # Use the final return code

        if output_file_path and output_file_path.exists():
            output_file_content = output_file_path.read_text(encoding="utf-8", errors="replace")
            if self.client.output_to_file and self.client.output_to_file.cleanup:
                try:
                    output_file_path.unlink()
                except OSError:  # pragma: no cover - best effort cleanup
                    pass

            if output_file_content and not stdout_text.strip():
                stdout_text = output_file_content

        if return_code != 0:
            recovered = self._recover_from_error(
                returncode=return_code,
                stdout=stdout_text,
                stderr=stderr_text,
                sanitized_command=sanitized_command,
                duration_seconds=duration,
                output_file_content=output_file_content,
            )
            if recovered is not None:
                return recovered

        if return_code != 0:
            raise CLIAgentError(
                f"CLI '{self.client.name}' exited with status {return_code}",
                returncode=return_code,
                stdout=stdout_text,
                stderr=stderr_text,
            )

        try:
            parsed = self._parser.parse(stdout_text, stderr_text)
        except ParserError as exc:
            raise CLIAgentError(
                f"Failed to parse output from CLI '{self.client.name}': {exc}",
                returncode=return_code,
                stdout=stdout_text,
                stderr=stderr_text,
            ) from exc

        return AgentOutput(
            parsed=parsed,
            sanitized_command=sanitized_command,
            returncode=return_code,
            stdout=stdout_text,
            stderr=stderr_text,
            duration_seconds=duration,
            parser_name=self._parser.name,
            output_file_content=output_file_content,
        )

    def _build_command(self, *, role: ResolvedCLIRole, system_prompt: str | None) -> list[str]:
        base = list(self.client.executable)
        base.extend(self.client.internal_args)
        base.extend(self.client.config_args)
        base.extend(role.role_args)

        return base

    def _build_environment(self) -> dict[str, str]:
        env = os.environ.copy()

        # Ensure critical variables for CLI credentials/config are preserved
        for key in ["HOME", "USER", "PATH", "SHELL", "LANG"]:
            if key not in env and key in os.environ:
                env[key] = os.environ[key]

        env.update(self.client.env)
        return env

    # ------------------------------------------------------------------
    # Error recovery hooks
    # ------------------------------------------------------------------

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
        """Hook for subclasses to convert CLI errors into successful outputs.

        Return an AgentOutput to treat the failure as success, or None to signal
        that normal error handling should proceed.
        """

        return None
