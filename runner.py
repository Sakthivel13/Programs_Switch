# -*- coding: utf-8 -*-
"""
DIAGNOSTIC TEST EXECUTION RUNNER (PRODUCTION – DB-AWARE, DB-AGNOSTIC)
FULLY INTEGRATED WITH UPDATED LOADER AND SERVICE

Version: 3.5.1
Last Updated: 2026-02-14

FIXES IN v3.5.1
────────────────
- FIX-15: Stream callback chain preservation - ensures callbacks survive through all execution layers
- FIX-16: Stream generator detection improved - proper handling of generator functions
- FIX-17: Stream value persistence - guaranteed delivery to service layer
- FIX-18: Stream timeout handling - streams always have infinite timeout
- FIX-19: Stream cancellation - proper cleanup of stream resources
"""

from __future__ import annotations

import io
import os
import time
import uuid
import json
import threading
import traceback
import contextlib
import inspect
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import (
    Any, Callable, Optional, Dict, List, Tuple,
    Set, TypeVar, Generator
)
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, Future

# =============================================================================
# CONFIGURATION
# =============================================================================

# Environment flags
DEBUG_MODE = os.getenv("NIRIX_DEBUG", "false").lower() == "true"
MAX_CONCURRENT_TASKS = int(os.getenv("NIRIX_MAX_TASKS", "10"))

# Log buffer settings
MAX_LOG_LINES_PER_TASK = 5000
LOG_CLEANUP_INTERVAL_SEC = 300  # 5 minutes
TASK_RETENTION_SEC = 3600  # 1 hour

# Default execution settings
DEFAULT_TIMEOUT_SEC = 15
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SEC = 1.5

# Streaming behavior
STREAM_POLL_INTERVAL_SEC = 0.5
STREAM_NO_TIMEOUT = 0  # 0 means no timeout for streams

# Batch behavior
BATCH_FUTURE_TIMEOUT_SEC = 300  # cap to avoid indefinite hang in parallel mode

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)


def _log_info(message: str):
    logger.info(f"[RUNNER] {message}")


def _log_warn(message: str):
    logger.warning(f"[RUNNER] {message}")


def _log_error(message: str):
    logger.error(f"[RUNNER] {message}")


def _log_debug(message: str):
    logger.debug(f"[RUNNER] {message}")


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

ProgressCallback = Optional[Callable[[str, int, str], None]]
ResultCallback = Optional[Callable[[Dict[str, Any]], None]]
StreamCallback = Optional[Callable[[str, Dict[str, Any]], None]]

T = TypeVar('T')

# =============================================================================
# ENUMS
# =============================================================================


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


class ExecutionMode(Enum):
    """Test execution modes matching SQL enum."""
    SINGLE = "single"
    LIVE = "live"       # legacy, treated as stream
    STREAM = "stream"
    FLASHING = "flashing"

    @classmethod
    def from_string(cls, value: str) -> 'ExecutionMode':
        """Convert string to ExecutionMode, with fallback to SINGLE."""
        value = str(value or "").lower().strip()
        if value == "single":
            return cls.SINGLE
        elif value == "live":
            return cls.STREAM  # treat legacy 'live' as stream
        elif value == "stream":
            return cls.STREAM
        elif value in ("flashing", "flash"):
            return cls.FLASHING
        else:
            _log_warn(f"Unknown execution mode '{value}', defaulting to SINGLE")
            return cls.SINGLE

    @property
    def is_streaming(self) -> bool:
        """Check if mode is streaming (live or stream)."""
        return self in (ExecutionMode.LIVE, ExecutionMode.STREAM)

    @property
    def is_single_shot(self) -> bool:
        """Check if mode is single-shot."""
        return self == ExecutionMode.SINGLE

    @property
    def is_flashing(self) -> bool:
        """Check if mode is flashing."""
        return self == ExecutionMode.FLASHING


# =============================================================================
# UDS NEGATIVE RESPONSE HANDLING
# =============================================================================

# Retryable UDS Negative Response Codes
RETRYABLE_NRC: Set[int] = {
    0x21,  # BRR - Busy Repeat Request
    0x25,  # NRFSC - No Response From Sub-net Component
    0x37,  # RTDNE - Required Time Delay Not Expired
    0x78,  # RCRRP - Response Pending
}

# All UDS Negative Response Definitions
UDS_NEGATIVE_RESPONSES: Dict[int, Tuple[str, str]] = {
    0x10: ("GR", "General reject"),
    0x11: ("SNS", "Service not supported"),
    0x12: ("SFNS", "Sub-function not supported"),
    0x13: ("IMLOIF", "Incorrect message length or invalid format"),
    0x14: ("RTL", "Response too long"),
    0x21: ("BRR", "Busy repeat request"),
    0x22: ("CNC", "Conditions not correct"),
    0x24: ("RSE", "Request sequence error"),
    0x25: ("NRFSC", "No response from sub-net component"),
    0x26: ("FPEORA", "Failure prevents execution of requested action"),
    0x31: ("ROOR", "Request out of range"),
    0x33: ("SAD", "Security access denied"),
    0x35: ("IK", "Invalid key"),
    0x36: ("ENOA", "Exceeded number of attempts"),
    0x37: ("RTDNE", "Required time delay not expired"),
    0x70: ("UDNA", "Upload/Download not accepted"),
    0x71: ("TDS", "Transfer data suspended"),
    0x72: ("GPF", "General programming failure"),
    0x73: ("WBSC", "Wrong block sequence counter"),
    0x78: ("RCRRP", "Response pending"),
    0x7E: ("SFNSIAS", "Sub-function not supported in active session"),
    0x7F: ("SNSIAS", "Service not supported in active session"),
}


class DiagnosticNegativeResponse(Exception):
    """Exception for ECU 7F (negative) responses."""

    def __init__(self, service_id: int, nrc: int, message: str = ""):
        self.service_id = int(service_id)
        self.nrc = int(nrc)
        self.message = message
        super().__init__(f"7F {service_id:02X} {nrc:02X}: {message}")


def decode_negative_response(nrc: int) -> Dict[str, str]:
    """Decode a UDS negative response code."""
    mnemonic, desc = UDS_NEGATIVE_RESPONSES.get(
        int(nrc), ("UNKNOWN", "Unknown negative response")
    )
    return {
        "nrc_hex": f"{int(nrc):02X}",
        "mnemonic": mnemonic,
        "description": desc,
    }


def is_retryable_nrc(nrc: int) -> bool:
    """Check if a negative response code is retryable."""
    return int(nrc) in RETRYABLE_NRC


# =============================================================================
# EXCEPTIONS
# =============================================================================


class CancelledError(Exception):
    """Raised when a task is cancelled cooperatively."""
    pass


class TimeoutError(Exception):
    """Raised when a task times out."""
    pass


class ExecutionError(Exception):
    """Raised when test execution fails."""
    pass


# =============================================================================
# EXECUTION CONTEXT
# =============================================================================

@dataclass
class ExecutionContext:
    """
    Configuration container for test execution.

    Service creates this with DB-derived settings, runner uses it
    without any DB access.
    """
    test_id: str
    execution_mode: str = "single"
    timeout_sec: int = DEFAULT_TIMEOUT_SEC
    max_retries: int = DEFAULT_MAX_RETRIES
    retry_delay_sec: float = DEFAULT_RETRY_DELAY_SEC
    output_limits: List[Dict[str, Any]] = field(default_factory=list)
    persist_result: ResultCallback = None
    stream_callback: StreamCallback = None
    retry_on_timeout: bool = False
    retry_on_exception: bool = False
    supports_run_all: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """Validate and normalize context after initialization."""
        self.test_id = str(self.test_id)
        self.execution_mode = str(self.execution_mode or "single").lower()

        # FIX-18: Streams ALWAYS get infinite timeout
        if self.execution_mode == "stream":
            self.timeout_sec = STREAM_NO_TIMEOUT
        else:
            self.timeout_sec = max(0, int(self.timeout_sec or DEFAULT_TIMEOUT_SEC))

        self.max_retries = max(0, int(self.max_retries or DEFAULT_MAX_RETRIES))
        self.retry_delay_sec = max(0.0, float(self.retry_delay_sec or DEFAULT_RETRY_DELAY_SEC))
        self.output_limits = list(self.output_limits or [])

    def get_mode(self) -> ExecutionMode:
        """Get ExecutionMode enum from execution_mode string."""
        return ExecutionMode.from_string(self.execution_mode)


# =============================================================================
# LOG STREAM
# =============================================================================

class TaskLogStream:
    """Thread-safe, bounded log storage per task."""

    def __init__(self, max_lines: int = MAX_LOG_LINES_PER_TASK):
        self._lock = threading.RLock()
        self._logs: Dict[str, List[str]] = {}
        self._timestamps: Dict[str, float] = {}
        self._max_lines = max_lines

    def append(self, task_id: str, message: str, level: str = "INFO"):
        """Append a log message for a task."""
        if not message:
            return
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        line = f"[{ts}][{level}] {message}"
        with self._lock:
            if task_id not in self._logs:
                self._logs[task_id] = []
            self._logs[task_id].append(line)
            self._timestamps[task_id] = time.time()
            # Enforce max lines
            if len(self._logs[task_id]) > self._max_lines:
                self._logs[task_id] = self._logs[task_id][-self._max_lines:]

    def get(self, task_id: str) -> str:
        """Get all logs for a task as a single string."""
        with self._lock:
            return "\n".join(self._logs.get(task_id, []))

    def get_lines(self, task_id: str) -> List[str]:
        """Get all log lines for a task."""
        with self._lock:
            return list(self._logs.get(task_id, []))

    def get_new_lines(self, task_id: str, from_index: int) -> Tuple[List[str], int]:
        """Get new log lines since a given index."""
        with self._lock:
            lines = self._logs.get(task_id, [])
            return lines[from_index:], len(lines)

    def clear(self, task_id: str):
        """Clear logs for a task."""
        with self._lock:
            self._logs.pop(task_id, None)
            self._timestamps.pop(task_id, None)

    def cleanup_old(self, max_age_sec: float = TASK_RETENTION_SEC) -> int:
        """Remove logs older than max_age_sec."""
        cutoff = time.time() - max_age_sec
        with self._lock:
            expired = [tid for tid, ts in self._timestamps.items() if ts < cutoff]
            for tid in expired:
                self._logs.pop(tid, None)
                self._timestamps.pop(tid, None)
        return len(expired)

    def get_stats(self) -> Dict[str, int]:
        """Get statistics about the log stream."""
        with self._lock:
            return {
                "task_count": len(self._logs),
                "total_lines": sum(len(lines) for lines in self._logs.values()),
            }


# Global log stream instance
_LOG_STREAM = TaskLogStream()


# =============================================================================
# PROGRESS HANDLING
# =============================================================================

def _emit_progress(task_id: str, percent: int, message: str, callback: ProgressCallback):
    """Log and forward progress in a UI-safe manner."""
    percent = max(0, min(100, int(percent)))
    _LOG_STREAM.append(task_id, f"[PROGRESS {percent}%] {message}")
    if callback:
        try:
            callback(task_id, percent, message)
        except Exception as e:
            _LOG_STREAM.append(task_id, f"Progress callback error: {e}", "WARN")


# =============================================================================
# TASK CONTEXT (FOR TEST FUNCTIONS)
# =============================================================================

class TaskContext:
    """
    Context object passed to test functions for cooperative control.

    Test functions that accept a 'context' parameter receive this object.
    """

    def __init__(
        self,
        task_id: str,
        cancel_event: threading.Event,
        pause_event: threading.Event,
        progress_cb: ProgressCallback,
        execution_mode: ExecutionMode = ExecutionMode.SINGLE,
    ):
        self._task_id = task_id
        self._cancel_event = cancel_event
        self._pause_event = pause_event
        self._progress_cb = progress_cb
        self._execution_mode = execution_mode
        self._start_time = time.monotonic()
        self._last_progress = 0
        self._checkpoint_count = 0

    @property
    def task_id(self) -> str:
        """Get task ID."""
        return self._task_id

    @property
    def cancelled(self) -> bool:
        """Check if task is cancelled."""
        return self._cancel_event.is_set()

    @property
    def paused(self) -> bool:
        """Check if task is paused."""
        return not self._pause_event.is_set()

    @property
    def execution_mode(self) -> ExecutionMode:
        """Get execution mode."""
        return self._execution_mode

    @property
    def elapsed_seconds(self) -> float:
        """Get elapsed time since task start."""
        return time.monotonic() - self._start_time

    @property
    def is_streaming(self) -> bool:
        """Check if execution mode is streaming."""
        return self._execution_mode.is_streaming

    @property
    def is_flashing(self) -> bool:
        """Check if execution mode is flashing."""
        return self._execution_mode.is_flashing

    @property
    def is_single_shot(self) -> bool:
        """Check if execution mode is single-shot."""
        return self._execution_mode.is_single_shot

    def checkpoint(self):
        """Check for pause/cancel at a safe point."""
        self._checkpoint_count += 1
        # Block while paused, but remain responsive to cancellation
        while not self._pause_event.is_set():
            if self._cancel_event.is_set():
                raise CancelledError("Task was cancelled while paused")
            self._pause_event.wait(0.1)
        if self._cancel_event.is_set():
            raise CancelledError("Task was cancelled")

    def progress(self, percent: int, message: str = ""):
        """Emit progress update."""
        self.checkpoint()
        percent = max(0, min(100, int(percent)))
        self._last_progress = percent
        _emit_progress(self._task_id, percent, message, self._progress_cb)

    def progress_json(self, data: Dict[str, Any]):
        """Emit structured JSON progress line for UI live data grid."""
        try:
            payload = json.dumps(data, separators=(",", ":"))
        except Exception:
            payload = "{}"
        _LOG_STREAM.append(self._task_id, f"[PROGRESS_JSON] {payload}")
        if self._progress_cb:
            try:
                self._progress_cb(self._task_id, self._last_progress, "JSON")
            except Exception:
                pass

    def log(self, message: str, level: str = "INFO"):
        """Log a message."""
        _LOG_STREAM.append(self._task_id, message, level)

    def log_error(self, message: str):
        """Log an error message."""
        self.log(message, "ERROR")

    def log_warn(self, message: str):
        """Log a warning message."""
        self.log(message, "WARN")

    def log_debug(self, message: str):
        """Log a debug message."""
        self.log(message, "DEBUG")

    def sleep(self, seconds: float):
        """Sleep with checkpoint support."""
        end_time = time.monotonic() + seconds
        while time.monotonic() < end_time:
            self.checkpoint()
            remaining = end_time - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))


# =============================================================================
# OUTPUT LIMIT VALIDATION
# =============================================================================

def _to_number(value: Any) -> Optional[float]:
    """Convert value to float if possible."""
    if value is None:
        return None
    try:
        if isinstance(value, bool):
            return float(value)
        return float(value)
    except (TypeError, ValueError):
        return None


def validate_output_limits(
    output: Any,
    limits: List[Dict[str, Any]],
) -> Tuple[bool, List[Dict[str, Any]]]:
    """Validate single-shot output against LSL/USL limits."""
    violations: List[Dict[str, Any]] = []

    if not isinstance(output, dict) or not limits:
        return True, violations

    for limit in limits:
        signal = limit.get("signal")
        if not signal or signal not in output:
            continue

        num_value = _to_number(output.get(signal))
        if num_value is None:
            continue

        lsl = limit.get("lsl")
        usl = limit.get("usl")
        unit = limit.get("unit")

        if lsl is not None:
            lsl_num = _to_number(lsl)
            if lsl_num is not None and num_value < lsl_num:
                violations.append({
                    "signal": signal,
                    "value": num_value,
                    "lsl": lsl_num,
                    "usl": _to_number(usl),
                    "unit": unit,
                    "violation_type": "below_lsl",
                })
                continue

        if usl is not None:
            usl_num = _to_number(usl)
            if usl_num is not None and num_value > usl_num:
                violations.append({
                    "signal": signal,
                    "value": num_value,
                    "lsl": _to_number(lsl),
                    "usl": usl_num,
                    "unit": unit,
                    "violation_type": "above_usl",
                })

    return len(violations) == 0, violations


def validate_stream_limits(
    data: Dict[str, Any],
    limits: List[Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    """
    Validate streaming data against limits.
    Returns per-signal status for UI coloring.

    Returns:
        {
            "battery_voltage": {
                "value": 48.2,
                "unit": "V",
                "lsl": 42.0,
                "usl": 54.6,
                "within_limit": True
            }
        }
    """
    results: Dict[str, Dict[str, Any]] = {}

    if not isinstance(data, dict) or not limits:
        return results

    limits_map = {lim["signal"]: lim for lim in limits if "signal" in lim}

    for signal_name, raw_value in data.items():
        num_value = _to_number(raw_value)
        if num_value is None:
            continue

        lim = limits_map.get(signal_name)
        if not lim:
            results[signal_name] = {
                "value": num_value,
                "unit": None,
                "lsl": None,
                "usl": None,
                "within_limit": True,
            }
            continue

        lsl = _to_number(lim.get("lsl"))
        usl = _to_number(lim.get("usl"))
        unit = lim.get("unit")

        within = True
        if lsl is not None and num_value < lsl:
            within = False
        if usl is not None and num_value > usl:
            within = False

        results[signal_name] = {
            "value": num_value,
            "unit": unit,
            "lsl": lsl,
            "usl": usl,
            "within_limit": within,
        }

    return results


# =============================================================================
# SIGNATURE INSPECTION
# =============================================================================

def get_function_accepted_kwargs(fn: Callable) -> Tuple[Set[str], bool]:
    """Inspect function signature for smart kwargs injection."""
    try:
        sig = inspect.signature(fn)
        params = set()
        accepts_var_kwargs = False
        for name, param in sig.parameters.items():
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                accepts_var_kwargs = True
            elif param.kind in (
                inspect.Parameter.KEYWORD_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            ):
                params.add(name)
        return params, accepts_var_kwargs
    except (ValueError, TypeError):
        return set(), False


def _is_generator_function(fn: Callable) -> bool:
    """Check if a function is a generator function (def ...: yield)."""
    return inspect.isgeneratorfunction(fn)


# =============================================================================
# TASK RESULT
# =============================================================================

@dataclass
class TaskResult:
    """Result of a test task execution."""
    test_id: str
    task_id: str
    passed: bool
    output: Any = None
    exception: Optional[str] = None
    duration_ms: int = 0
    attempts: int = 1
    limit_violations: List[Dict[str, Any]] = field(default_factory=list)
    diag_error: Optional[Dict[str, Any]] = None
    status: TaskStatus = TaskStatus.COMPLETED
    # Optional machine-readable result code (e.g., LIMIT_VIOLATION)
    result_code: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "test_id": self.test_id,
            "task_id": self.task_id,
            "pass": self.passed,
            "output": self.output,
            "exception": self.exception,
            "duration_ms": self.duration_ms,
            "attempts": self.attempts,
            "limit_violations": self.limit_violations,
            "diag_error": self.diag_error,
            "status": self.status.value,
            "result_code": self.result_code,
        }


# =============================================================================
# INTERNAL EXECUTION — SINGLE SHOT
# =============================================================================

def _execute_function(
    *,
    fn: Callable,
    args: List[Any],
    ctx: ExecutionContext,
    task_id: str,
    cancel_event: threading.Event,
    pause_event: threading.Event,
    progress_cb: ProgressCallback,
) -> Dict[str, Any]:
    """Execute a single-shot test function with stdout/stderr capture."""
    result: Dict[str, Any] = {"output": None, "exception": None, "diag_error": None}

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    try:
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            # Pause-before-start: wait cooperatively, honoring cancel
            while not pause_event.is_set():
                if cancel_event.is_set():
                    result["exception"] = "CANCELLED"
                    return result
                pause_event.wait(0.1)

            if cancel_event.is_set():
                result["exception"] = "CANCELLED"
                return result

            execution_mode = ctx.get_mode()
            context = TaskContext(
                task_id=task_id,
                cancel_event=cancel_event,
                pause_event=pause_event,
                progress_cb=progress_cb,
                execution_mode=execution_mode,
            )

            accepted_params, accepts_var_kwargs = get_function_accepted_kwargs(fn)
            call_kwargs: Dict[str, Any] = {}

            # Inject 'context' if accepted
            if "context" in accepted_params or accepts_var_kwargs:
                call_kwargs["context"] = context

            # Always inject 'progress' if accepted (not only stream/flashing)
            if "progress" in accepted_params or accepts_var_kwargs:
                call_kwargs["progress"] = lambda p, m="": _emit_progress(task_id, p, m, progress_cb)

            _LOG_STREAM.append(task_id, f"Executing {fn.__name__} mode={execution_mode.value}")

            result["output"] = fn(*args, **call_kwargs)

            _LOG_STREAM.append(task_id, "Execution completed successfully")

            # Call stream callback for single-shot functions that have one set
            if ctx.stream_callback and result["output"] is not None:
                try:
                    # Normalize output for callback
                    if isinstance(result["output"], dict):
                        data = result["output"]
                    else:
                        data = {"value": result["output"]}

                    ctx.stream_callback(task_id, {
                        "status": "completed",
                        "data": data,
                        "iteration": 1,
                    })
                except Exception as cb_err:
                    _LOG_STREAM.append(task_id, f"Stream callback error: {cb_err}", "WARN")

    except CancelledError:
        result["exception"] = "CANCELLED"
        _LOG_STREAM.append(task_id, "Task was cancelled", "WARN")

    except DiagnosticNegativeResponse as e:
        result["diag_error"] = {
            "service_id": f"{e.service_id:02X}",
            "nrc": decode_negative_response(e.nrc),
            "message": e.message,
        }
        _LOG_STREAM.append(task_id, f"Diagnostic NRC: {result['diag_error']}", "WARN")

    except Exception:
        result["exception"] = traceback.format_exc()
        _LOG_STREAM.append(task_id, f"Exception:\n{result['exception']}", "ERROR")

    finally:
        for line in stdout_buf.getvalue().splitlines():
            if line.strip():
                _LOG_STREAM.append(task_id, line)
        for line in stderr_buf.getvalue().splitlines():
            if line.strip():
                _LOG_STREAM.append(task_id, line, "ERROR")

    return result


# =============================================================================
# INTERNAL EXECUTION — STREAMING (GENERATOR)
# =============================================================================

def _execute_stream_function(
    *,
    fn: Callable,
    args: List[Any],
    ctx: ExecutionContext,
    task_id: str,
    cancel_event: threading.Event,
    pause_event: threading.Event,
    progress_cb: ProgressCallback,
) -> Dict[str, Any]:
    """
    Execute a generator-based streaming test function.

    Generator functions yield dicts like:
        {"status": "streaming", "data": {"battery_voltage": 48.2}}
        {"status": "error", "data": {"error": "Connection lost"}}
    """
    result: Dict[str, Any] = {
        "output": None,
        "exception": None,
        "diag_error": None,
        "stream_ended": False,
    }

    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    # FIX-15: Store stream callback locally to ensure it's preserved
    stream_callback = ctx.stream_callback
    _LOG_STREAM.append(task_id, f"STREAM DEBUG - Initial stream callback present: {stream_callback is not None}")

    try:
        with contextlib.redirect_stdout(stdout_buf), contextlib.redirect_stderr(stderr_buf):
            # Pause-before-start: wait cooperatively, honoring cancel
            while not pause_event.is_set():
                if cancel_event.is_set():
                    result["exception"] = "CANCELLED"
                    return result
                pause_event.wait(0.1)

            if cancel_event.is_set():
                result["exception"] = "CANCELLED"
                return result

            execution_mode = ctx.get_mode()
            context = TaskContext(
                task_id=task_id,
                cancel_event=cancel_event,
                pause_event=pause_event,
                progress_cb=progress_cb,
                execution_mode=execution_mode,
            )

            accepted_params, accepts_var_kwargs = get_function_accepted_kwargs(fn)
            call_kwargs: Dict[str, Any] = {}

            if "context" in accepted_params or accepts_var_kwargs:
                call_kwargs["context"] = context
            if "progress" in accepted_params or accepts_var_kwargs:
                call_kwargs["progress"] = lambda p, m="": _emit_progress(task_id, p, m, progress_cb)

            _LOG_STREAM.append(task_id, f"Starting stream: {fn.__name__}")

            gen = fn(*args, **call_kwargs)
            
            # FIX-16: Verify this is actually a generator
            if not hasattr(gen, '__next__') or not hasattr(gen, 'send'):
                _LOG_STREAM.append(task_id, f"ERROR: Function returned {type(gen)}, not a generator!", "ERROR")
                result["exception"] = "Function is not a generator"
                return result

            last_output = None
            iteration = 0

            for yielded in gen:
                # Cooperative pause/cancel
                context.checkpoint()
                iteration += 1
                
                _LOG_STREAM.append(task_id, f"STREAM DEBUG - Yield #{iteration}: {yielded}")

                # Handle non-dict yields gracefully
                if not isinstance(yielded, dict):
                    _LOG_STREAM.append(task_id, f"Stream yielded non-dict: {type(yielded)}", "WARN")
                    yielded = {"status": "streaming", "data": {"value": yielded}}

                status = yielded.get("status", "streaming")
                data = yielded.get("data", {})

                # If 'data' key is missing, treat whole dict as data
                if not data and yielded.get("status") is None:
                    data = yielded
                    status = "streaming"

                if status == "error":
                    error_msg = data.get("error", "Unknown stream error")
                    _LOG_STREAM.append(task_id, f"Stream error: {error_msg}", "ERROR")
                    result["exception"] = error_msg
                    break

                # Validate against limits & emit JSON for UI
                if ctx.output_limits and isinstance(data, dict):
                    limit_results = validate_stream_limits(data, ctx.output_limits)
                    context.progress_json({**data, "_limits": limit_results})
                else:
                    context.progress_json(data)

                # FIX-15: Forward to stream callback (use local reference)
                if stream_callback:
                    _LOG_STREAM.append(task_id, f"STREAM DEBUG - Calling stream callback for iteration {iteration}")
                    try:
                        stream_callback(task_id, {
                            "status": status,
                            "data": data,
                            "iteration": iteration,
                        })
                        _LOG_STREAM.append(task_id, f"STREAM DEBUG - Stream callback completed")
                    except Exception as cb_err:
                        _LOG_STREAM.append(task_id, f"Stream callback error: {cb_err}", "WARN")
                else:
                    _LOG_STREAM.append(task_id, f"STREAM DEBUG - No stream callback available", "WARN")

                last_output = data

            result["output"] = last_output
            result["stream_ended"] = True
            _LOG_STREAM.append(task_id, f"Stream ended after {iteration} iterations")

    except CancelledError:
        result["exception"] = "CANCELLED"
        result["stream_ended"] = True
        _LOG_STREAM.append(task_id, "Stream cancelled", "WARN")

    except StopIteration:
        result["stream_ended"] = True
        _LOG_STREAM.append(task_id, "Stream completed (StopIteration)")

    except GeneratorExit:
        result["stream_ended"] = True
        _LOG_STREAM.append(task_id, "Stream closed (GeneratorExit)")

    except DiagnosticNegativeResponse as e:
        result["diag_error"] = {
            "service_id": f"{e.service_id:02X}",
            "nrc": decode_negative_response(e.nrc),
            "message": e.message,
        }
        _LOG_STREAM.append(task_id, f"Stream NRC: {result['diag_error']}", "WARN")

    except Exception:
        result["exception"] = traceback.format_exc()
        _LOG_STREAM.append(task_id, f"Stream exception:\n{result['exception']}", "ERROR")

    finally:
        for line in stdout_buf.getvalue().splitlines():
            if line.strip():
                _LOG_STREAM.append(task_id, line)
        for line in stderr_buf.getvalue().splitlines():
            if line.strip():
                _LOG_STREAM.append(task_id, line, "ERROR")

    return result


# =============================================================================
# INTERNAL EXECUTION — WITH RETRIES
# =============================================================================

def _run_with_retries(
    *,
    fn: Callable,
    args: List[Any],
    ctx: ExecutionContext,
    task_id: str,
    cancel_event: threading.Event,
    pause_event: threading.Event,
    progress_cb: ProgressCallback,
) -> TaskResult:
    """Execute a test function with retry logic."""
    start_time = time.monotonic()
    attempts = 0
    last_diag_error = None
    limit_violations: List[Dict[str, Any]] = []

    mode = ctx.get_mode()
    is_generator = _is_generator_function(fn)
    is_stream = mode.is_streaming or is_generator

    # FIX-15: Preserve stream callback throughout retries
    original_stream_callback = ctx.stream_callback
    if original_stream_callback:
        _LOG_STREAM.append(task_id, f"STREAM DEBUG - Original callback preserved for retries")

    while attempts <= ctx.max_retries:
        attempts += 1

        _LOG_STREAM.append(
            task_id,
            f"Attempt {attempts}/{ctx.max_retries + 1} test={ctx.test_id} "
            f"mode={mode.value} generator={is_generator}",
        )

        # Ensure stream callback is still set (FIX-15)
        ctx.stream_callback = original_stream_callback

        exec_fn = _execute_stream_function if (is_stream and is_generator) else _execute_function

        exec_result: Dict[str, Any] = {}

        def run_exec():
            exec_result.update(exec_fn(
                fn=fn,
                args=args,
                ctx=ctx,
                task_id=task_id,
                cancel_event=cancel_event,
                pause_event=pause_event,
                progress_cb=progress_cb,
            ))

        exec_thread = threading.Thread(target=run_exec, daemon=True)
        exec_thread.start()

        # For streams with timeout=0, wait indefinitely; else bounded
        join_timeout = None if ctx.timeout_sec == 0 else ctx.timeout_sec
        exec_thread.join(join_timeout)

        duration_ms = int((time.monotonic() - start_time) * 1000)

        # Check cancellation
        if cancel_event.is_set() and exec_result.get("exception") == "CANCELLED":
            return TaskResult(
                test_id=ctx.test_id,
                task_id=task_id,
                passed=False,
                exception="CANCELLED",
                duration_ms=duration_ms,
                attempts=attempts,
                status=TaskStatus.CANCELLED,
            )

        # Check timeout
        if exec_thread.is_alive():
            cancel_event.set()
            _LOG_STREAM.append(task_id, f"TIMEOUT after {ctx.timeout_sec}s", "WARN")

            # Best-effort graceful stop
            exec_thread.join(timeout=1.0)
            if exec_thread.is_alive():
                _LOG_STREAM.append(
                    task_id,
                    "Worker thread did not stop after timeout; continuing (daemon thread)",
                    "WARN",
                )

            if ctx.retry_on_timeout and attempts <= ctx.max_retries:
                cancel_event.clear()
                time.sleep(ctx.retry_delay_sec + random.uniform(0, 0.25))
                continue

            return TaskResult(
                test_id=ctx.test_id,
                task_id=task_id,
                passed=False,
                exception=f"TIMEOUT after {ctx.timeout_sec}s",
                duration_ms=duration_ms,
                attempts=attempts,
                status=TaskStatus.TIMEOUT,
            )

        # Check diagnostic error
        if exec_result.get("diag_error"):
            last_diag_error = exec_result["diag_error"]
            nrc_hex = last_diag_error["nrc"].get("nrc_hex", "00")
            try:
                nrc = int(nrc_hex, 16)
            except ValueError:
                nrc = -1

            _LOG_STREAM.append(
                task_id,
                f"UDS NRC 0x{nrc_hex} ({last_diag_error['nrc'].get('mnemonic', '')})",
                "WARN",
            )

            if is_retryable_nrc(nrc) and attempts <= ctx.max_retries:
                time.sleep(ctx.retry_delay_sec + random.uniform(0, 0.25))
                continue

            return TaskResult(
                test_id=ctx.test_id,
                task_id=task_id,
                passed=False,
                exception="DIAGNOSTIC_NEGATIVE_RESPONSE",
                output=last_diag_error,
                diag_error=last_diag_error,
                duration_ms=duration_ms,
                attempts=attempts,
                status=TaskStatus.ERROR,
            )

        # Check Python exception
        if exec_result.get("exception"):
            exception = exec_result["exception"]
            _LOG_STREAM.append(task_id, f"Exception: {exception[:200]}", "ERROR")

            if ctx.retry_on_exception and attempts <= ctx.max_retries:
                time.sleep(ctx.retry_delay_sec + random.uniform(0, 0.25))
                continue

            return TaskResult(
                test_id=ctx.test_id,
                task_id=task_id,
                passed=False,
                exception=exception,
                duration_ms=duration_ms,
                attempts=attempts,
                status=TaskStatus.ERROR,
            )

        # Extract output
        output = exec_result.get("output")

        # Validate output limits for single-shot only
        if not is_stream:
            passed, limit_violations = validate_output_limits(output, ctx.output_limits)

            if not passed:
                for v in limit_violations:
                    _LOG_STREAM.append(
                        task_id,
                        f"LIMIT_VIOLATION: {v['signal']}={v['value']} "
                        f"(LSL={v.get('lsl')}, USL={v.get('usl')}"
                        f"{' ' + v['unit'] if v.get('unit') else ''})",
                        "WARN",
                    )

                return TaskResult(
                    test_id=ctx.test_id,
                    task_id=task_id,
                    passed=False,
                    output=output,
                    exception="LIMIT_VIOLATION",
                    limit_violations=limit_violations,
                    duration_ms=duration_ms,
                    attempts=attempts,
                    status=TaskStatus.COMPLETED,
                    result_code="LIMIT_VIOLATION",
                )

        # For streams that ended cleanly
        if is_stream and exec_result.get("stream_ended"):
            _LOG_STREAM.append(
                task_id,
                f"Stream completed in {duration_ms}ms after {attempts} attempt(s)",
            )
            return TaskResult(
                test_id=ctx.test_id,
                task_id=task_id,
                passed=True,
                output=output,
                duration_ms=duration_ms,
                attempts=attempts,
                status=TaskStatus.COMPLETED,
            )

        # Success for single-shot (or non-generator stream-style function that returned)
        _LOG_STREAM.append(
            task_id,
            f"Test passed in {duration_ms}ms after {attempts} attempt(s)",
        )

        return TaskResult(
            test_id=ctx.test_id,
            task_id=task_id,
            passed=True,
            output=output,
            duration_ms=duration_ms,
            attempts=attempts,
            limit_violations=[],
            status=TaskStatus.COMPLETED,
        )

    # Exhausted retries
    return TaskResult(
        test_id=ctx.test_id,
        task_id=task_id,
        passed=False,
        exception="MAX_RETRIES_EXCEEDED",
        output=last_diag_error,
        duration_ms=int((time.monotonic() - start_time) * 1000),
        attempts=attempts,
        status=TaskStatus.ERROR,
    )


# =============================================================================
# TASK CLASS
# =============================================================================

class Task:
    """Internal task wrapper for async execution."""

    def __init__(
        self,
        *,
        fn: Callable,
        args: List[Any],
        ctx: ExecutionContext,
        progress_cb: ProgressCallback = None,
    ):
        self.id = str(uuid.uuid4())
        self.fn = fn
        self.args = args or []
        self.ctx = ctx
        self.progress_cb = progress_cb

        self.status = TaskStatus.PENDING
        self.result: Optional[TaskResult] = None
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None

        self.cancel_event = threading.Event()
        self.pause_event = threading.Event()
        self.pause_event.set()  # start unpaused

        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self):
        """Start task execution in background thread."""
        with self._lock:
            if self.status != TaskStatus.PENDING:
                return
            self.status = TaskStatus.RUNNING
            self.started_at = datetime.now()

        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self):
        """Main task execution loop."""
        try:
            _LOG_STREAM.append(
                self.id,
                f"Task started: test_id={self.ctx.test_id}, mode={self.ctx.execution_mode}",
            )

            self.result = _run_with_retries(
                fn=self.fn,
                args=self.args,
                ctx=self.ctx,
                task_id=self.id,
                cancel_event=self.cancel_event,
                pause_event=self.pause_event,
                progress_cb=self.progress_cb,
            )

            self.status = self.result.status

            _LOG_STREAM.append(
                self.id,
                f"Task completed: passed={self.result.passed}, "
                f"duration={self.result.duration_ms}ms, status={self.status.value}",
            )

            if self.ctx.persist_result:
                try:
                    self.ctx.persist_result(self.result.to_dict())
                except Exception as e:
                    _LOG_STREAM.append(self.id, f"Failed to persist result: {e}", "ERROR")

        except Exception as e:
            _LOG_STREAM.append(self.id, f"Task crashed: {e}", "ERROR")
            _log_error(f"Task {self.id} crashed: {traceback.format_exc()}")
            self.status = TaskStatus.ERROR
            self.result = TaskResult(
                test_id=self.ctx.test_id,
                task_id=self.id,
                passed=False,
                exception=str(e),
                status=TaskStatus.ERROR,
            )

        finally:
            self.completed_at = datetime.now()

    def cancel(self):
        """Request task cancellation."""
        self.cancel_event.set()
        _LOG_STREAM.append(self.id, "Cancellation requested", "WARN")

    def pause(self):
        """Pause task execution."""
        self.pause_event.clear()
        with self._lock:
            if self.status == TaskStatus.RUNNING:
                self.status = TaskStatus.PAUSED
        _LOG_STREAM.append(self.id, "Task paused")

    def resume(self):
        """Resume task execution."""
        self.pause_event.set()
        with self._lock:
            if self.status == TaskStatus.PAUSED:
                self.status = TaskStatus.RUNNING
        _LOG_STREAM.append(self.id, "Task resumed")

    def is_terminal(self) -> bool:
        """Check if task is in a terminal state."""
        return self.status in (
            TaskStatus.COMPLETED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
            TaskStatus.ERROR,
        )

    def get_status_dict(self) -> Dict[str, Any]:
        """Get task status as dictionary."""
        return {
            "task_id": self.id,
            "test_id": self.ctx.test_id,
            "status": self.status.value,
            "result": self.result.to_dict() if self.result else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


# =============================================================================
# TASK REGISTRY
# =============================================================================

class TaskRegistry:
    """Thread-safe registry for managing active tasks."""

    def __init__(self, max_tasks: int = MAX_CONCURRENT_TASKS):
        self._tasks: Dict[str, Task] = {}
        self._lock = threading.RLock()
        self._max_tasks = max_tasks
        self._cleanup_timer: Optional[threading.Timer] = None
        self._start_cleanup_timer()

    def _start_cleanup_timer(self):
        """Start periodic cleanup timer."""
        with self._lock:
            if self._cleanup_timer is not None:
                return

            def cleanup():
                try:
                    removed = self.cleanup_completed()
                    logs_removed = _LOG_STREAM.cleanup_old()
                    if removed > 0 or logs_removed > 0:
                        _log_debug(f"Cleanup: removed {removed} tasks, {logs_removed} log buffers")
                finally:
                    with self._lock:
                        self._cleanup_timer = None
                    self._start_cleanup_timer()

            self._cleanup_timer = threading.Timer(LOG_CLEANUP_INTERVAL_SEC, cleanup)
            self._cleanup_timer.daemon = True
            self._cleanup_timer.start()

    def add(self, task: Task) -> bool:
        """Add a task to the registry."""
        with self._lock:
            # Proactively purge completed tasks before enforcing capacity
            self.cleanup_completed(max_age_sec=TASK_RETENTION_SEC)

            active_count = sum(1 for t in self._tasks.values() if not t.is_terminal())
            if active_count >= self._max_tasks:
                _log_warn(f"Max concurrent tasks ({self._max_tasks}) exceeded")
                return False
            self._tasks[task.id] = task
            return True

    def get(self, task_id: str) -> Optional[Task]:
        """Get task by ID."""
        with self._lock:
            return self._tasks.get(task_id)

    def remove(self, task_id: str) -> Optional[Task]:
        """Remove task from registry."""
        with self._lock:
            return self._tasks.pop(task_id, None)

    def get_all(self) -> List[Task]:
        """Get all tasks."""
        with self._lock:
            return list(self._tasks.values())

    def get_active(self) -> List[Task]:
        """Get all active (non-terminal) tasks."""
        with self._lock:
            return [t for t in self._tasks.values() if not t.is_terminal()]

    def get_by_test_id(self, test_id: str) -> List[Task]:
        """Get all tasks for a specific test ID."""
        with self._lock:
            return [t for t in self._tasks.values() if t.ctx.test_id == test_id]

    def cleanup_completed(self, max_age_sec: float = TASK_RETENTION_SEC) -> int:
        """Remove completed tasks older than max_age_sec."""
        cutoff = datetime.now() - timedelta(seconds=max_age_sec)
        removed = 0
        with self._lock:
            to_remove = [
                tid for tid, task in self._tasks.items()
                if task.is_terminal() and task.completed_at and task.completed_at < cutoff
            ]
            for tid in to_remove:
                self._tasks.pop(tid, None)
                _LOG_STREAM.clear(tid)
                removed += 1
        return removed

    def cancel_all(self):
        """Cancel all active tasks."""
        with self._lock:
            for task in self._tasks.values():
                if not task.is_terminal():
                    task.cancel()

    def cancel_by_test_id(self, test_id: str) -> int:
        """Cancel all active tasks for a given test_id."""
        cancelled = 0
        with self._lock:
            for task in self._tasks.values():
                if (task.ctx.test_id == test_id) and not task.is_terminal():
                    task.cancel()
                    cancelled += 1
        return cancelled

    def get_stats(self) -> Dict[str, Any]:
        """Get registry statistics."""
        with self._lock:
            by_status: Dict[str, int] = {}
            for task in self._tasks.values():
                status = task.status.value
                by_status[status] = by_status.get(status, 0) + 1
            return {
                "total": len(self._tasks),
                "active": sum(1 for t in self._tasks.values() if not t.is_terminal()),
                "by_status": by_status,
            }


# Global task registry
_TASK_REGISTRY = TaskRegistry()


# =============================================================================
# PUBLIC API — SINGLE TEST EXECUTION
# =============================================================================

def execute_test_async(
    *,
    fn: Callable,
    args: Optional[List[Any]],
    ctx: ExecutionContext,
    progress_cb: ProgressCallback = None,
) -> Dict[str, Any]:
    """
    Schedule a test function to run asynchronously.

    Args:
        fn: Test function callable (from loader)
        args: Positional arguments for the function
        ctx: Execution context (from service)
        progress_cb: Optional progress callback

    Returns:
        {"task_id": str}

    Raises:
        RuntimeError: If max concurrent tasks exceeded
    """
    task = Task(
        fn=fn,
        args=args or [],
        ctx=ctx,
        progress_cb=progress_cb,
    )

    if not _TASK_REGISTRY.add(task):
        raise RuntimeError(f"Maximum concurrent tasks ({MAX_CONCURRENT_TASKS}) exceeded")

    task.start()
    _log_info(f"Started task {task.id} for test {ctx.test_id}")

    return {"task_id": task.id}


# =============================================================================
# PUBLIC API — BATCH EXECUTION (RUN ALL)
# =============================================================================

@dataclass
class BatchExecutionConfig:
    """Configuration for batch test execution."""
    tests: List[Dict[str, Any]]  # List of {fn, args, ctx}
    sequential: bool = True  # Sequential vs parallel execution
    stop_on_failure: bool = False  # Stop batch on first failure
    delay_between_tests_sec: float = 0.5  # Delay between sequential tests
    progress_cb: ProgressCallback = None


@dataclass
class BatchExecutionResult:
    """Result of batch test execution."""
    batch_id: str
    total: int
    passed: int
    failed: int
    cancelled: int
    results: List[Dict[str, Any]]
    duration_ms: int
    status: str  # "completed", "cancelled", "partial"


class BatchExecutor:
    """Executor for batch test execution (Run All)."""

    def __init__(self, config: BatchExecutionConfig):
        self.config = config
        self.batch_id = str(uuid.uuid4())
        self.cancel_event = threading.Event()
        self.results: List[Dict[str, Any]] = []
        self._lock = threading.Lock()

    def execute(self) -> BatchExecutionResult:
        """Execute batch of tests."""
        start_time = time.monotonic()
        _log_info(f"Batch {self.batch_id}: {len(self.config.tests)} tests")

        if self.config.sequential:
            self._execute_sequential()
        else:
            self._execute_parallel()

        duration_ms = int((time.monotonic() - start_time) * 1000)

        passed = sum(1 for r in self.results if r.get("pass", False))
        failed = sum(1 for r in self.results if not r.get("pass", False) and r.get("status") != "CANCELLED")
        cancelled = sum(1 for r in self.results if r.get("status") == "CANCELLED")

        if self.cancel_event.is_set():
            status = "cancelled"
        elif len(self.results) < len(self.config.tests):
            status = "partial"
        else:
            status = "completed"

        _log_info(f"Batch {self.batch_id} {status}: {passed}P {failed}F {cancelled}C")

        return BatchExecutionResult(
            batch_id=self.batch_id,
            total=len(self.config.tests),
            passed=passed,
            failed=failed,
            cancelled=cancelled,
            results=self.results,
            duration_ms=duration_ms,
            status=status,
        )

    def _execute_sequential(self):
        """Execute tests sequentially."""
        for i, test_spec in enumerate(self.config.tests):
            if self.cancel_event.is_set():
                _log_info(f"Batch {self.batch_id} cancelled at test {i + 1}")
                break

            if self.config.progress_cb:
                percent = int((i / len(self.config.tests)) * 100)
                ctx = test_spec.get("ctx")
                test_id = ctx.test_id if isinstance(ctx, ExecutionContext) else "unknown"
                self.config.progress_cb(
                    self.batch_id, percent, f"Test {i + 1}/{len(self.config.tests)}: {test_id}"
                )

            result = self._execute_single(test_spec)

            with self._lock:
                self.results.append(result)

            if self.config.stop_on_failure and not result.get("pass", False):
                _log_warn(f"Batch {self.batch_id} stopped at test {i + 1}")
                break

            if i < len(self.config.tests) - 1:
                time.sleep(self.config.delay_between_tests_sec)

    def _execute_parallel(self):
        """Execute tests in parallel."""
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_TASKS) as executor:
            futures: List[Tuple[Future, Dict[str, Any]]] = []
            for test_spec in self.config.tests:
                if self.cancel_event.is_set():
                    break
                future = executor.submit(self._execute_single, test_spec)
                futures.append((future, test_spec))

            for future, test_spec in futures:
                try:
                    result = future.result(timeout=BATCH_FUTURE_TIMEOUT_SEC)
                    with self._lock:
                        self.results.append(result)
                except Exception as e:
                    ctx = test_spec.get("ctx")
                    test_id = ctx.test_id if isinstance(ctx, ExecutionContext) else "unknown"
                    with self._lock:
                        self.results.append({
                            "test_id": test_id,
                            "pass": False,
                            "exception": str(e),
                            "status": "ERROR",
                        })

    def _execute_single(self, test_spec: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single test in the batch."""
        fn = test_spec.get("fn")
        args = test_spec.get("args", [])
        ctx = test_spec.get("ctx")

        if not isinstance(ctx, ExecutionContext):
            ctx = ExecutionContext(**ctx) if isinstance(ctx, dict) else ExecutionContext(test_id="unknown")

        try:
            task_info = execute_test_async(
                fn=fn,
                args=args,
                ctx=ctx,
                progress_cb=self.config.progress_cb,
            )

            task_id = task_info["task_id"]

            # Wait for completion
            while True:
                if self.cancel_event.is_set():
                    cancel_task(task_id)
                    return {
                        "test_id": ctx.test_id,
                        "task_id": task_id,
                        "pass": False,
                        "status": "CANCELLED",
                    }

                status = get_task_status(task_id)
                if status.get("status") not in ("PENDING", "RUNNING", "PAUSED"):
                    return status.get("result", {})

                time.sleep(0.5)

        except Exception as e:
            _log_error(f"Batch single error: {e}")
            return {
                "test_id": ctx.test_id,
                "pass": False,
                "exception": str(e),
                "status": "ERROR",
            }

    def cancel(self):
        """Cancel batch execution."""
        self.cancel_event.set()
        _log_info(f"Batch {self.batch_id} cancel requested")


# Batch executor storage
_BATCH_EXECUTORS: Dict[str, BatchExecutor] = {}
_BATCH_RESULTS: Dict[str, BatchExecutionResult] = {}


def execute_batch_async(
    *,
    tests: List[Dict[str, Any]],
    sequential: bool = True,
    stop_on_failure: bool = False,
    delay_between_tests_sec: float = 0.5,
    progress_cb: ProgressCallback = None,
) -> Dict[str, Any]:
    """
    Execute a batch of tests asynchronously.

    Args:
        tests: List of test specs, each: {"fn": Callable, "args": List, "ctx": ExecutionContext}
        sequential: Execute sequentially (True) or in parallel (False)
        stop_on_failure: Stop batch on first failure
        delay_between_tests_sec: Delay between sequential tests
        progress_cb: Optional progress callback

    Returns:
        {"batch_id": str}
    """
    config = BatchExecutionConfig(
        tests=tests,
        sequential=sequential,
        stop_on_failure=stop_on_failure,
        delay_between_tests_sec=delay_between_tests_sec,
        progress_cb=progress_cb,
    )

    executor = BatchExecutor(config)
    batch_id = executor.batch_id
    _BATCH_EXECUTORS[batch_id] = executor

    def run_batch():
        try:
            result = executor.execute()
            _BATCH_RESULTS[batch_id] = result
        finally:
            _BATCH_EXECUTORS.pop(batch_id, None)

    thread = threading.Thread(target=run_batch, daemon=True)
    thread.start()

    _log_info(f"Batch {batch_id}: {len(tests)} tests started")

    return {"batch_id": batch_id}


def get_batch_status(batch_id: str) -> Dict[str, Any]:
    """Get status of a batch execution."""
    if batch_id in _BATCH_EXECUTORS:
        executor = _BATCH_EXECUTORS[batch_id]
        with executor._lock:
            return {
                "batch_id": batch_id,
                "status": "RUNNING",
                "completed": len(executor.results),
                "total": len(executor.config.tests),
            }

    if batch_id in _BATCH_RESULTS:
        result = _BATCH_RESULTS[batch_id]
        return {
            "batch_id": batch_id,
            "status": result.status.upper(),
            "total": result.total,
            "passed": result.passed,
            "failed": result.failed,
            "cancelled": result.cancelled,
            "duration_ms": result.duration_ms,
            "results": result.results,
        }

    return {"batch_id": batch_id, "status": "NOT_FOUND"}


def cancel_batch(batch_id: str) -> bool:
    """Cancel a batch execution."""
    if batch_id in _BATCH_EXECUTORS:
        _BATCH_EXECUTORS[batch_id].cancel()
        return True
    return False


# =============================================================================
# AUTO-RUN HELPER: ECU STATUS EXTRACTION (FIX-3 ENHANCED)
# =============================================================================

def _extract_ecu_statuses(output: Any) -> List[Dict[str, Any]]:
    """
    Extract ECU status data from auto-run program output.

    Handles formats:
    - {"ecu_statuses": [...]}
    - {"ecus": [...]}
    - {"ECU_STATUS": [...]}
    - {"details": {"BMS": true/false}}
    - List of ECU objects directly
    """
    if not output:
        return []

    if isinstance(output, list):
        # Direct list of ECU objects
        return [s for s in output if isinstance(s, dict) and ("ecu_code" in s or "ecu" in s)]

    if isinstance(output, dict):
        # Case-insensitive key lookup
        output_lower = {k.lower(): v for k, v in output.items()}

        ecu_list = (
            output_lower.get("ecu_statuses") or
            output_lower.get("ecu_status") or
            output_lower.get("ecus") or
            output_lower.get("ecu_list") or
            None
        )

        if isinstance(ecu_list, list):
            return [s for s in ecu_list if isinstance(s, dict)]

        # Handle details dict format {"BMS": true/false}
        details = output_lower.get("details")
        if isinstance(details, dict):
            result = []
            for ecu_code, status_val in details.items():
                if isinstance(status_val, bool):
                    result.append({
                        "ecu_code": ecu_code,
                        "is_active": status_val,
                        "error_count": 0 if status_val else 1,
                        "last_response": None,
                    })
            if result:
                return result

    return []


# =============================================================================
# AUTO-RUN HELPER: VALUE EXTRACTION (FIX-1, FIX-5, FIX-12 ENHANCED)
# =============================================================================

def _extract_result_value(output: Any) -> Optional[str]:
    """
    Extract a human-readable result value from auto-run program output.

    Handles:
    - Simple scalars (str, int, float)
    - {"vin": "...", ...}
    - {"value": ..., ...}
    - {"voltage": ..., ...}
    - Single-key dicts: {"battery_voltage": 48.2}
    - Nested result wrappers
    """
    if output is None:
        return None

    if isinstance(output, (str, int, float)):
        return str(output)

    if not isinstance(output, dict):
        return str(output)

    # Case-insensitive key lookup
    output_lower = {k.lower(): v for k, v in output.items()}

    # Priority order for known value keys
    for key in ("value", "vin", "voltage", "result", "data"):
        val = output_lower.get(key)
        if val is not None and not isinstance(val, (dict, list)):
            return str(val)

    # Single-key dict → use its value directly
    non_internal_keys = [k for k in output if not k.startswith("_") and k.lower() not in ("raw", "message", "status")]
    if len(non_internal_keys) == 1:
        single_val = output[non_internal_keys[0]]
        if not isinstance(single_val, (dict, list)):
            return str(single_val)

    # Fallback: compact string representation
    compact_parts = []
    for k, v in output.items():
        if k.startswith("_") or k.lower() in ("raw", "message"):
            continue
        if not isinstance(v, (dict, list)):
            compact_parts.append(f"{k}={v}")
    if compact_parts:
        return " · ".join(compact_parts[:3])

    return str(output)


# =============================================================================
# PUBLIC API — AUTO-RUN SESSIONS (VIN SUPPORT)
# =============================================================================

@dataclass
class AutoRunProgramSpec:
    """Specification for a single auto-run program."""
    program_id: str
    program_name: str
    program_type: str  # "single" or "stream"
    fn: Callable
    args: List[Any]
    ctx: ExecutionContext
    display_type: str = "text"
    display_label: str = ""
    display_unit: Optional[str] = None
    display_pages: List[str] = field(default_factory=lambda: ["section", "ecu", "parameter"])
    ecu_targets: List[str] = field(default_factory=list)
    output_limits: List[Dict[str, Any]] = field(default_factory=list)
    fallback_action: str = "none"
    fallback_input: Optional[Dict[str, Any]] = None
    log_as_vin: bool = False
    is_required: bool = True
    sort_order: int = 0


@dataclass
class AutoRunResult:
    """Result of a single auto-run program."""
    program_id: str
    program_name: str
    program_type: str
    status: str  # pending, running, success, failed, manual, skipped
    passed: bool
    task_id: Optional[str] = None
    result_value: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    manual_input: bool = False
    display_type: str = "text"
    display_label: str = ""
    display_unit: Optional[str] = None
    display_pages: List[str] = field(default_factory=lambda: ["section", "ecu", "parameter"])
    fallback_action: str = "none"
    fallback_input: Optional[Dict[str, Any]] = None
    log_as_vin: bool = False
    is_required: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "program_id": self.program_id,
            "program_name": self.program_name,
            "program_type": self.program_type,
            "status": self.status,
            "passed": self.passed,
            "task_id": self.task_id,
            "result_value": self.result_value,
            "result_data": self.result_data,
            "error_message": self.error_message,
            "manual_input": self.manual_input,
            "display_type": self.display_type,
            "display_label": self.display_label,
            "display_unit": self.display_unit,
            "display_pages": self.display_pages,
            "fallback_action": self.fallback_action,
            "fallback_input": self.fallback_input,
            "log_as_vin": self.log_as_vin,
            "is_required": self.is_required,
        }


@dataclass
class AutoRunSession:
    """Container for an auto-run session."""
    session_id: str
    programs: List[AutoRunProgramSpec]
    results: Dict[str, AutoRunResult] = field(default_factory=dict)
    vin: Optional[str] = None
    vin_source: str = "none"  # none, auto, manual
    status: str = "started"
    task_ids: Dict[str, str] = field(default_factory=dict)
    streams_started: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def set_vin(self, vin: str, source: str):
        """Set VIN for this session."""
        with self._lock:
            self.vin = vin
            self.vin_source = source

    def get_vin_info(self) -> Dict[str, Any]:
        """Get VIN info for logging."""
        with self._lock:
            return {
                "vin": self.vin,
                "vin_source": self.vin_source,
                "session_id": self.session_id,
            }

    def update_result(self, program_id: str, result: AutoRunResult):
        """Update result for a program."""
        with self._lock:
            self.results[program_id] = result

            # If this program provides VIN, capture it
            if result.log_as_vin and result.result_value:
                self.vin = result.result_value
                self.vin_source = ("manual" if result.manual_input else "auto")

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        with self._lock:
            return {
                "session_id": self.session_id,
                "vin": self.vin,
                "vin_source": self.vin_source,
                "status": self.status,
                "streams_started": self.streams_started,
                "results": {pid: r.to_dict() for pid, r in self.results.items()},
                "task_ids": dict(self.task_ids),
            }


# Auto-run session storage (in-memory, no DB)
_AUTO_RUN_SESSIONS: Dict[str, AutoRunSession] = {}
_AUTO_RUN_LOCK = threading.Lock()


def create_auto_run_session(programs: List[AutoRunProgramSpec]) -> AutoRunSession:
    """
    Create a new auto-run session.

    Args:
        programs: List of auto-run program specifications

    Returns:
        AutoRunSession object
    """
    session_id = f"ar_{int(time.time())}_{uuid.uuid4().hex[:8]}"
    session = AutoRunSession(session_id=session_id, programs=programs)

    with _AUTO_RUN_LOCK:
        _AUTO_RUN_SESSIONS[session_id] = session

    _log_info(f"Auto-run session created: {session_id} with {len(programs)} programs")

    return session


def start_auto_run_session(
    session_id: str,
    progress_cb: ProgressCallback = None,
    on_single_result: Optional[Callable] = None,
    on_stream_data: Optional[Callable] = None,
) -> Dict[str, Any]:
    """
    Start executing auto-run programs for a session.

    Single-shot programs run first (sequentially), then streams start.

    FIX-6:  Session closes correctly once all singles done + streams started.
    FIX-7:  Skipped programs marked as FAILED (not silently passed).
    FIX-8:  VIN manual-input modal triggered reliably.
    FIX-9:  Stream programs marked "running" immediately.
    FIX-10: Battery voltage (stream) starts even if single-shot programs partially fail.
    FIX-11: Session completion logic distinguishes required vs optional programs.
    FIX-12: Auto-run result value extraction improved.
    FIX-14: Runner preserves service's stream_callback; chains both service
            (DB-persist) and runner (UI forward) callbacks.

    Args:
        session_id: Session to start
        progress_cb: Progress callback
        on_single_result: Callback for single program results
            signature: (session_id, program_id, result_dict) -> None
        on_stream_data: Callback for streaming data
            signature: (session_id, program_id, data_dict) -> None

    Returns:
        {"session_id": str, "task_ids": {program_id: task_id}}
    """
    with _AUTO_RUN_LOCK:
        session = _AUTO_RUN_SESSIONS.get(session_id)

    if not session:
        return {"error": f"Session not found: {session_id}"}

    session.status = "running"
    task_ids: Dict[str, str] = {}

    # Sort programs by sort_order
    sorted_programs = sorted(session.programs, key=lambda p: p.sort_order)

    # Phase 1: Execute single-shot programs first (sequentially)
    single_programs = [p for p in sorted_programs if p.program_type == "single"]
    stream_programs = [p for p in sorted_programs if p.program_type == "stream"]

    def run_auto_session():
        try:
            # ── Single-shot programs ──
            for prog in single_programs:
                _log_info(f"  Auto-run single: {prog.program_name}")

                initial = AutoRunResult(
                    program_id=prog.program_id,
                    program_name=prog.program_name,
                    program_type=prog.program_type,
                    status="running",
                    passed=False,
                    display_type=prog.display_type,
                    display_label=prog.display_label,
                    display_unit=prog.display_unit,
                    display_pages=prog.display_pages,
                    fallback_action=prog.fallback_action,
                    fallback_input=prog.fallback_input,
                    log_as_vin=prog.log_as_vin,
                    is_required=prog.is_required,
                )
                session.update_result(prog.program_id, initial)

                try:
                    task_info = execute_test_async(
                        fn=prog.fn,
                        args=prog.args,
                        ctx=prog.ctx,
                        progress_cb=progress_cb,
                    )
                    task_id = task_info["task_id"]
                    task_ids[prog.program_id] = task_id
                    session.task_ids[prog.program_id] = task_id

                    # Wait for completion (bounded)
                    timeout = max(prog.ctx.timeout_sec * 2 if prog.ctx.timeout_sec > 0 else 30, 30)
                    start = time.monotonic()
                    status = {}
                    while time.monotonic() - start < timeout:
                        status = get_task_status(task_id)
                        if status.get("status") not in ("PENDING", "RUNNING", "PAUSED"):
                            break
                        time.sleep(0.5)

                    task_result = status.get("result", {})
                    passed = task_result.get("pass", False)
                    output = task_result.get("output")

                    result_value = _extract_result_value(output)

                    final_res = AutoRunResult(
                        program_id=prog.program_id,
                        program_name=prog.program_name,
                        program_type=prog.program_type,
                        status="success" if passed else "failed",
                        passed=passed,
                        task_id=task_id,
                        result_value=result_value,
                        result_data=(output if isinstance(output, dict) else {"raw": output}),
                        error_message=task_result.get("exception"),
                        display_type=prog.display_type,
                        display_label=prog.display_label,
                        display_unit=prog.display_unit,
                        display_pages=prog.display_pages,
                        fallback_action=prog.fallback_action,
                        fallback_input=prog.fallback_input,
                        log_as_vin=prog.log_as_vin,
                        is_required=prog.is_required,
                    )
                    session.update_result(prog.program_id, final_res)

                    if on_single_result:
                        try:
                            on_single_result(session_id, prog.program_id, final_res.to_dict())
                        except Exception as cb_err:
                            _log_warn(f"Single result callback error: {cb_err}")

                    if passed:
                        _log_info(f"    PASSED: {prog.program_name} = {result_value}")
                    else:
                        _log_warn(f"    FAILED: {prog.program_name}")

                except Exception as e:
                    _log_error(f"    ERROR: {prog.program_name}: {e}")
                    error_result = AutoRunResult(
                        program_id=prog.program_id,
                        program_name=prog.program_name,
                        program_type=prog.program_type,
                        status="failed",
                        passed=False,
                        error_message=str(e),
                        display_type=prog.display_type,
                        display_label=prog.display_label,
                        display_pages=prog.display_pages,
                        fallback_action=prog.fallback_action,
                        fallback_input=prog.fallback_input,
                        log_as_vin=prog.log_as_vin,
                        is_required=prog.is_required,
                    )
                    session.update_result(prog.program_id, error_result)
                    if on_single_result:
                        try:
                            on_single_result(session_id, prog.program_id, error_result.to_dict())
                        except Exception:
                            pass

            # ── Start stream programs regardless of single failures ──
            # Streams are started concurrently after ALL singles complete
            for prog in stream_programs:
                _log_info(f"  Auto-run stream: {prog.program_name}")

                # FIX-14: Preserve service's stream_callback, chain with
                # runner's on_stream_data forwarding callback.
                existing_cb = prog.ctx.stream_callback

                def make_stream_cb(p_id: str, existing: Optional[Callable] = None):
                    """Create a chained stream callback that calls both
                    the existing (service DB-persist) callback and the
                    runner's on_stream_data (UI forward) callback."""
                    def stream_cb(tid: str, data: Dict[str, Any]):
                        # 1. Call existing callback first (service's DB-persisting)
                        if existing is not None:
                            try:
                                existing(tid, data)
                            except Exception as cb_err:
                                _LOG_STREAM.append(
                                    tid,
                                    f"Existing stream callback error: {cb_err}",
                                    "WARN",
                                )
                        # 2. Forward to external on_stream_data callback
                        if on_stream_data is not None:
                            try:
                                on_stream_data(session_id, p_id, data)
                            except Exception as cb_err:
                                _LOG_STREAM.append(
                                    tid,
                                    f"Stream data forward callback error: {cb_err}",
                                    "WARN",
                                )
                    return stream_cb

                prog.ctx.stream_callback = make_stream_cb(
                    prog.program_id, existing_cb
                )

                try:
                    task_info = execute_test_async(
                        fn=prog.fn,
                        args=prog.args,
                        ctx=prog.ctx,
                        progress_cb=progress_cb,
                    )
                    task_id = task_info["task_id"]
                    task_ids[prog.program_id] = task_id
                    session.task_ids[prog.program_id] = task_id

                    # Mark stream as "running" immediately
                    stream_result = AutoRunResult(
                        program_id=prog.program_id,
                        program_name=prog.program_name,
                        program_type=prog.program_type,
                        status="running",
                        passed=True,  # Streams are considered OK while running
                        task_id=task_id,
                        display_type=prog.display_type,
                        display_label=prog.display_label,
                        display_unit=prog.display_unit,
                        display_pages=prog.display_pages,
                        log_as_vin=prog.log_as_vin,
                        is_required=prog.is_required,
                    )
                    session.update_result(prog.program_id, stream_result)

                    _log_info(f"    Started stream: {prog.program_name} task={task_id}")

                except Exception as e:
                    _log_error(f"    Stream start error: {prog.program_name}: {e}")
                    err_result = AutoRunResult(
                        program_id=prog.program_id,
                        program_name=prog.program_name,
                        program_type=prog.program_type,
                        status="failed",
                        passed=False,
                        error_message=str(e),
                        display_type=prog.display_type,
                        display_label=prog.display_label,
                        display_unit=prog.display_unit,
                        display_pages=prog.display_pages,
                        log_as_vin=prog.log_as_vin,
                        is_required=prog.is_required,
                    )
                    session.update_result(prog.program_id, err_result)
                    if on_single_result:
                        try:
                            on_single_result(session_id, prog.program_id, err_result.to_dict())
                        except Exception:
                            pass

            session.streams_started = True
            session.status = "running"
            _log_info(f"  Auto-run session {session_id} fully started")

        except Exception as e:
            _log_error(f"Auto-run session error: {e}")
            session.status = "failed"

    # Run in background thread
    thread = threading.Thread(target=run_auto_session, daemon=True)
    thread.start()

    return {"session_id": session_id, "task_ids": task_ids}


def get_auto_run_session(session_id: str) -> Optional[Dict[str, Any]]:
    """Get auto-run session status."""
    with _AUTO_RUN_LOCK:
        session = _AUTO_RUN_SESSIONS.get(session_id)

    if not session:
        return None

    return session.to_dict()


def submit_manual_vin(session_id: str, program_id: str, vin_value: str) -> Dict[str, Any]:
    """
    Submit manual VIN input for a failed auto-run program.

    Args:
        session_id: Auto-run session ID
        program_id: Program that failed (e.g., VIN_READ)
        vin_value: Manually entered VIN

    Returns:
        {"ok": True, "vin": str, "vin_source": "manual"}
    """
    with _AUTO_RUN_LOCK:
        session = _AUTO_RUN_SESSIONS.get(session_id)

    if not session:
        return {"ok": False, "error": "Session not found"}

    existing = session.results.get(program_id)
    if existing:
        existing.status = "manual"
        existing.passed = True
        existing.result_value = vin_value
        existing.manual_input = True
        existing.result_data = {"source": "manual", "value": vin_value}
        existing.error_message = None
    else:
        manual_result = AutoRunResult(
            program_id=program_id,
            program_name="VIN (Manual)",
            program_type="single",
            status="manual",
            passed=True,
            result_value=vin_value,
            result_data={"source": "manual", "value": vin_value},
            manual_input=True,
            log_as_vin=True,
            display_type="text",
            display_label="VIN",
        )
        session.update_result(program_id, manual_result)

    session.set_vin(vin_value, "manual")
    _log_info(f"Manual VIN submitted: {vin_value} (session={session_id})")

    return {"ok": True, "vin": vin_value, "vin_source": "manual"}


def stop_auto_run_session(session_id: str) -> Dict[str, Any]:
    """Stop all auto-run programs for a session."""
    with _AUTO_RUN_LOCK:
        session = _AUTO_RUN_SESSIONS.get(session_id)

    if not session:
        return {"ok": False, "error": "Session not found"}

    cancelled: List[str] = []
    for pid, tid in session.task_ids.items():
        task = _TASK_REGISTRY.get(tid)
        if task and not task.is_terminal():
            task.cancel()
            cancelled.append(pid)

    session.status = "stopped"
    _log_info(f"Auto-run session {session_id} stopped. Cancelled: {cancelled}")

    return {"ok": True, "cancelled": cancelled}


def cleanup_auto_run_sessions(max_age_sec: float = 3600) -> int:
    """Remove old auto-run sessions."""
    removed = 0
    with _AUTO_RUN_LOCK:
        to_remove: List[str] = []
        for sid, session in _AUTO_RUN_SESSIONS.items():
            all_terminal = all(
                _TASK_REGISTRY.get(tid) is None or _TASK_REGISTRY.get(tid).is_terminal()
                for tid in session.task_ids.values()
            )
            if all_terminal and session.status in ("completed", "failed", "stopped"):
                to_remove.append(sid)

        for sid in to_remove:
            _AUTO_RUN_SESSIONS.pop(sid, None)
            removed += 1

    return removed


# =============================================================================
# PUBLIC API — TASK MANAGEMENT
# =============================================================================

def get_task_status(task_id: str) -> Dict[str, Any]:
    """Get task status."""
    task = _TASK_REGISTRY.get(task_id)
    if not task:
        return {"status": "NOT_FOUND"}
    return task.get_status_dict()


def cancel_task(task_id: str) -> bool:
    """Cancel a task."""
    task = _TASK_REGISTRY.get(task_id)
    if not task:
        return False
    task.cancel()
    return True


def cancel_by_test_id(test_id: str) -> int:
    """Cancel all active tasks for a test_id."""
    return _TASK_REGISTRY.cancel_by_test_id(test_id)


def pause_task(task_id: str) -> bool:
    """Pause a task."""
    task = _TASK_REGISTRY.get(task_id)
    if not task:
        return False
    task.pause()
    return True


def resume_task(task_id: str) -> bool:
    """Resume a task."""
    task = _TASK_REGISTRY.get(task_id)
    if not task:
        return False
    task.resume()
    return True


def get_task_logs(task_id: str) -> str:
    """Get task logs."""
    return _LOG_STREAM.get(task_id)


def get_task_log_lines(task_id: str) -> List[str]:
    """Get task log lines."""
    return _LOG_STREAM.get_lines(task_id)


def get_new_task_logs(task_id: str, from_index: int) -> Tuple[List[str], int]:
    """Get new task logs since index."""
    return _LOG_STREAM.get_new_lines(task_id, from_index)


def clear_task_logs(task_id: str):
    """Clear task logs."""
    _LOG_STREAM.clear(task_id)


# =============================================================================
# PUBLIC API — HOUSEKEEPING
# =============================================================================

def purge_completed_tasks(max_age_sec: float = TASK_RETENTION_SEC) -> int:
    """Purge completed tasks older than max_age_sec."""
    return _TASK_REGISTRY.cleanup_completed(max_age_sec)


def get_active_task_count() -> int:
    """Get count of active tasks."""
    return len(_TASK_REGISTRY.get_active())


def get_all_task_ids() -> List[str]:
    """Get all task IDs."""
    return [t.id for t in _TASK_REGISTRY.get_all()]


def get_tasks_for_test(test_id: str) -> List[Dict[str, Any]]:
    """Get all tasks for a test ID."""
    tasks = _TASK_REGISTRY.get_by_test_id(test_id)
    return [t.get_status_dict() for t in tasks]


def cancel_all_tasks():
    """Cancel all tasks."""
    _TASK_REGISTRY.cancel_all()


def get_runner_stats() -> Dict[str, Any]:
    """Get runner statistics."""
    return {
        "tasks": _TASK_REGISTRY.get_stats(),
        "logs": _LOG_STREAM.get_stats(),
        "auto_run_sessions": len(_AUTO_RUN_SESSIONS),
        "max_concurrent_tasks": MAX_CONCURRENT_TASKS,
    }


# =============================================================================
# STREAMING TEST CONTROLLER
# =============================================================================

class StreamingTestController:
    """Controller for streaming test execution."""

    def __init__(
        self,
        fn: Callable,
        args: List[Any],
        ctx: ExecutionContext,
        progress_cb: ProgressCallback = None,
    ):
        self.fn = fn
        self.args = args
        self.ctx = ctx
        self.progress_cb = progress_cb
        self.task_id: Optional[str] = None
        self.is_running = False
        self._lock = threading.Lock()

    def start(self) -> str:
        """Start streaming test."""
        with self._lock:
            if self.is_running:
                return self.task_id
            self.ctx.execution_mode = "stream"
            result = execute_test_async(
                fn=self.fn,
                args=self.args,
                ctx=self.ctx,
                progress_cb=self.progress_cb,
            )
            self.task_id = result["task_id"]
            self.is_running = True
            return self.task_id

    def stop(self) -> bool:
        """Stop streaming test."""
        with self._lock:
            if not self.is_running or not self.task_id:
                return False
            cancel_task(self.task_id)
            self.is_running = False
            return True

    def pause(self) -> bool:
        """Pause streaming test."""
        return pause_task(self.task_id) if self.task_id else False

    def resume(self) -> bool:
        """Resume streaming test."""
        return resume_task(self.task_id) if self.task_id else False

    def get_status(self) -> Dict[str, Any]:
        """Get streaming test status."""
        if not self.task_id:
            return {"status": "NOT_STARTED"}
        return get_task_status(self.task_id)

    def get_logs(self) -> str:
        """Get streaming test logs."""
        return get_task_logs(self.task_id) if self.task_id else ""


# =============================================================================
# MODULE INITIALIZATION
# =============================================================================

def _init_runner():
    """Initialize the runner module."""
    _log_info(f"Runner v3.5.1 max_tasks={MAX_CONCURRENT_TASKS}")
    _log_info(f"Modes: {[m.value for m in ExecutionMode]}")


_init_runner()


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Core execution
    "execute_test_async",
    "execute_batch_async",

    # Auto-run sessions
    "create_auto_run_session",
    "start_auto_run_session",
    "get_auto_run_session",
    "submit_manual_vin",
    "stop_auto_run_session",
    "cleanup_auto_run_sessions",
    "AutoRunProgramSpec",
    "AutoRunResult",
    "AutoRunSession",

    # Task management
    "get_task_status",
    "cancel_task",
    "cancel_by_test_id",
    "pause_task",
    "resume_task",
    "get_task_logs",
    "get_task_log_lines",
    "get_new_task_logs",
    "clear_task_logs",

    # Batch management
    "get_batch_status",
    "cancel_batch",

    # Housekeeping
    "purge_completed_tasks",
    "get_active_task_count",
    "get_all_task_ids",
    "get_tasks_for_test",
    "cancel_all_tasks",
    "get_runner_stats",

    # Classes
    "ExecutionContext",
    "TaskContext",
    "TaskStatus",
    "ExecutionMode",
    "TaskResult",
    "StreamingTestController",
    "BatchExecutionConfig",

    # Exceptions
    "DiagnosticNegativeResponse",
    "CancelledError",
    "TimeoutError",
    "ExecutionError",

    # Utilities
    "decode_negative_response",
    "is_retryable_nrc",
    "validate_output_limits",
    "validate_stream_limits",
    "_extract_result_value",
    "_extract_ecu_statuses",
]