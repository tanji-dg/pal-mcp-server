"""Execute configured CLI agents for the clink tool and parse output."""

from __future__ import annotations

import asyncio
import logging
import os
import shlex
import shutil
import signal
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
        native_session_id: str | None = None,
    ) -> AgentOutput:
        # Files and images are already embedded into the prompt by the tool; they are
        # accepted here only to keep parity with SimpleTool callers.
        _ = (files, images)
        # The runner simply executes the configured CLI command for the selected role.
        command = self._build_command(role=role, system_prompt=system_prompt, native_session_id=native_session_id)
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
                # start_new_session=True is removed to prevent hanging on completion.
                # We now use psutil to clean up the process tree instead.
            )
        except FileNotFoundError as exc:
            raise CLIAgentError(f"Executable not found for CLI '{self.client.name}': {exc}") from exc

        # Resolve effective timeouts
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
        activity_event = asyncio.Event()
        self._completion_event = asyncio.Event()

        # Setup ALL tasks concurrently
        stdin_task = asyncio.create_task(self._write_stdin(process, prompt))
        stdout_task = asyncio.create_task(self._read_stream(process, process.stdout, stdout_buffer, activity_event, output_callback, "stdout"))
        stderr_task = asyncio.create_task(self._read_stream(process, process.stderr, stderr_buffer, activity_event, output_callback, "stderr"))
        wait_task = asyncio.create_task(process.wait())
        interrupt_task = asyncio.create_task(self._interruption_monitor(process))
        completion_task = asyncio.create_task(self._completion_event.wait())
        
        tasks_to_gather = [stdin_task, stdout_task, stderr_task, wait_task, interrupt_task, completion_task]
        
        if idle_timeout > 0:
            idle_task = asyncio.create_task(self._idle_timeout_monitor(process, activity_event, idle_timeout))
            tasks_to_gather.append(idle_task)

        try:
            start_wait = time.monotonic()
            while True:
                remaining = None
                if total_timeout > 0:
                    remaining = total_timeout - (time.monotonic() - start_wait)
                    if remaining <= 0:
                        raise asyncio.TimeoutError()

                done, pending = await asyncio.wait(
                    tasks_to_gather,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                if not done:
                    raise asyncio.TimeoutError()

                # Check for exceptions first
                for t in done:
                    if not t.cancelled() and t.exception():
                        # Check for InterruptedError immediately
                        if isinstance(t.exception(), InterruptedError):
                            raise t.exception()
                        # Propagate other exceptions
                        raise t.exception()

                # Check if we should stop
                if wait_task.done() or completion_task.done():
                    if completion_task.done():
                        self._logger.info("Logical completion detected. Proceeding to cleanup.")
                    break
                
                # If we are here, some other task (like stdin_task) finished.
                # Continue waiting for the remaining tasks.
                tasks_to_gather = list(pending)
                if not tasks_to_gather:
                    break

            # CRITICAL: Cancel remaining tasks immediately to avoid hangs
            for t in pending:
                if not t.done():
                    t.cancel()

        except InterruptedError:
            self._logger.warning(f"[run] Caught InterruptedError, killing process {process.pid}")
            # Process was already killed in _read_stream or _interruption_monitor, escalate immediately
            # Try a quick cleanup but don't block
            if process.returncode is None:
                try:
                    await self._kill_process_tree(process)
                    await asyncio.wait_for(process.wait(), timeout=0.1)
                except Exception:
                    pass
            
            # Cancel all subtasks
            for t in tasks_to_gather:
                if not t.done():
                    t.cancel()

            # Extract partial output for restoration
            stdout_text = "".join(stdout_buffer)
            stderr_text = "".join(stderr_buffer)
            
            # Re-raise with partial results attached so the tool can use them
            raise CLIAgentError(
                "Task interrupted by user",
                returncode=process.returncode,
                stdout=stdout_text,
                stderr=stderr_text
            )
        except asyncio.TimeoutError as exc:
            self._logger.warning("[run] Caught asyncio.TimeoutError")
            # Total timeout occurred for the entire operation
            if process.returncode is None:
                try:
                    await self._kill_process_tree(process)
                except Exception:
                    pass
            
            # Cancel all subtasks
            for t in tasks_to_gather:
                if not t.done():
                    t.cancel()
            
            # Wait for tasks to handle cancellation
            try:
                await asyncio.wait(tasks_to_gather, timeout=0.2)
            except Exception:
                pass

            raise CLIAgentError(
                f"CLI '{self.client.name}' total timed out after {total_timeout} seconds",
                returncode=None,
            ) from exc
        except Exception as e:
            self._logger.debug(f"[run] Caught unexpected exception: {type(e).__name__}: {e}")
            # Any other error (including our new idle timeout CLIAgentError or CancelledError)
            if process.returncode is None:
                try:
                    await self._kill_process_tree(process)
                except Exception:
                    pass
            
            # CRITICAL: Cancel all tasks immediately to stop blocking readline()
            self._logger.debug(f"[run] Cancelling remaining {len(tasks_to_gather)} tasks")
            for t in tasks_to_gather:
                if not t.done():
                    t.cancel()
            
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
                    await self._kill_process_tree(process)
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

        if return_code != 0 and not self._completion_event.is_set():
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

        if return_code != 0 and not self._completion_event.is_set():
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

    async def _kill_process_tree(self, proc: asyncio.subprocess.Process):
        """Kill the entire process tree using psutil to ensure no orphaned children are left behind."""
        if proc.returncode is not None:
            return
            
        pid = proc.pid
        self._logger.debug(f"Terminating process tree for PID {pid}")
        
        try:
            import psutil
            parent = psutil.Process(pid)
            children = parent.children(recursive=True)
            
            # Terminate children first
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    pass
            
            # Terminate parent
            parent.terminate()
            
            # Give a short window for graceful exit
            _, alive = psutil.wait_procs(children + [parent], timeout=0.2)
            
            # Force kill any remaining processes
            for p in alive:
                try:
                    self._logger.debug(f"Force killing remaining process {p.pid}")
                    p.kill()
                except psutil.NoSuchProcess:
                    pass
        except ImportError:
            self._logger.warning("psutil not available, falling back to basic kill")
            proc.kill()
        except Exception as e:
            self._logger.warning(f"Failed to kill process tree for {pid}: {e}")
            try:
                proc.kill()
            except Exception:
                pass

    async def _kill_process_group(self, proc: asyncio.subprocess.Process):
        """Kill the entire process group. Replaced by _kill_process_tree for more robustness."""
        await self._kill_process_tree(proc)

    async def _write_stdin(self, proc: asyncio.subprocess.Process, data: str):
        """Cancellable task to write prompt to stdin."""
        if not proc.stdin:
            return
        try:
            proc.stdin.write(data.encode("utf-8"))
            await proc.stdin.drain()
            proc.stdin.close()
            await proc.stdin.wait_closed()
        except asyncio.CancelledError:
            self._logger.debug("Stdin write cancelled")
            raise
        except Exception as exc:
            self._logger.warning(f"Failed to write to stdin: {exc}")

    async def on_stream_line(
        self, line: str, stream_name: str, process: asyncio.subprocess.Process
    ) -> None:
        """Hook for subclasses to inspect stream output and react (e.g. early exit)."""
        pass

    async def _read_stream(
        self,
        process: asyncio.subprocess.Process,
        stream,
        buffer,
        activity_event: asyncio.Event,
        output_callback: callable[[str], None] | None,
        stream_name: str,
    ):
        publisher = get_publisher()
        self._logger.debug(f"[_read_stream] Starting for stream: {stream_name}")
        while True:
            # Immediate check for interruption
            if publisher.is_interrupted():
                raise InterruptedError("Task interrupted by user")

            try:
                # Use timeout to avoid blocking during hang
                line = await asyncio.wait_for(stream.readline(), timeout=0.5)
            except asyncio.TimeoutError:
                continue 

            if not line:
                break
            decoded_line = line.decode("utf-8", errors="replace")
            buffer.append(decoded_line)
            
            # Allow subclasses to react to output (e.g. detect turn completion)
            await self.on_stream_line(decoded_line, stream_name, process)
            
            if stream_name == "stderr":
                for keyword in FATAL_ERROR_KEYWORDS:
                    if keyword in decoded_line:
                        raise CLIAgentError(f"Fatal CLI error: {decoded_line.strip()}")

            if output_callback:
                try:
                    maybe_coro = output_callback(decoded_line)
                    if asyncio.iscoroutine(maybe_coro):
                        await maybe_coro
                except Exception:
                    pass
            activity_event.set()

    async def _interruption_monitor(self, process: asyncio.subprocess.Process):
        """Dedicated task to monitor interruption requests from the dashboard."""
        publisher = get_publisher()
        while process.returncode is None:
            try:
                if await publisher.check_interruption():
                    self._logger.warning(f"Interruption signal received. Escalating.")
                    # Termination will be handled by the InterruptedError handler in run()
                    raise InterruptedError("Task interrupted by user via monitor dashboard")
                await asyncio.sleep(0.1) # Faster polling
            except asyncio.CancelledError:
                break

    async def _idle_timeout_monitor(
        self, process: asyncio.subprocess.Process, activity_event: asyncio.Event, idle_timeout: int
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
                        # Use killpg to ensure whole group is killed on idle timeout
                        pgid = os.getpgid(process.pid)
                        os.killpg(pgid, signal.SIGKILL)
                    except Exception:
                        process.kill()
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

    def _build_command(
        self, *, role: ResolvedCLIRole, system_prompt: str | None, native_session_id: str | None = None
    ) -> list[str]:
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
