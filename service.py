# -*- coding: utf-8 -*-
"""
DIAGNOSTIC EXECUTION SERVICE (PRODUCTION – DB AUTHORITATIVE)
FULLY INTEGRATED WITH UPDATED LOADER AND RUNNER

Version: 4.3.0
Last Updated: 2026-02-14

FIXES IN v4.3.0
────────────────
- FIX-20: Stream callback chain preservation - ensures stream values reach database
- FIX-21: Stream value persistence - guaranteed delivery to auto_run_stream_values
- FIX-22: Stream limits pre-computation - optimize per-program limit lookups
- FIX-23: Stream debug logging - comprehensive logging for troubleshooting
- FIX-24: ECU status persistence with stream programs - proper handling
"""

from __future__ import annotations

import os
import re
import json
import time
import traceback
import inspect
from datetime import datetime
from typing import Dict, Any, Optional, List, Callable, Iterable, Tuple

# =============================================================================
# DATABASE IMPORTS
# =============================================================================

try:
    from database import query_one, query_all, execute
    DATABASE_AVAILABLE = True
except ImportError:
    query_one = query_all = execute = None
    DATABASE_AVAILABLE = False

# =============================================================================
# LOADER IMPORTS
# =============================================================================

from diagnostics.loader import (
    load_test_function,
    load_function_from_path,
    get_vehicle_root,
    discover_vehicle_sections,
    discover_ecus,
    discover_parameters,
    discover_health_tabs,
    get_sections_from_json,
    get_auto_run_config,
    VehicleNotFoundError,
    ModuleLoadError,
    FunctionNotFoundError,
    safe_name,
)

# =============================================================================
# RUNNER IMPORTS
# =============================================================================

from diagnostics.runner import (
    execute_test_async,
    execute_batch_async,
    get_task_status,
    cancel_task,
    cancel_by_test_id,
    pause_task,
    resume_task,
    get_task_logs,
    get_task_log_lines,
    get_new_task_logs,
    clear_task_logs,
    get_runner_stats,
    purge_completed_tasks,
    cancel_all_tasks,
    get_tasks_for_test,
    get_batch_status,
    cancel_batch,
    # Auto-run session support
    create_auto_run_session,
    start_auto_run_session,
    get_auto_run_session,
    submit_manual_vin,
    stop_auto_run_session,
    cleanup_auto_run_sessions,
    AutoRunProgramSpec,
    AutoRunResult,
    AutoRunSession,
    # Streaming
    StreamingTestController,
    # Core types
    ExecutionContext,
    TaskStatus,
    ExecutionMode,
    BatchExecutionConfig,
)

# =============================================================================
# CAN UTILS IMPORTS
# =============================================================================

try:
    from diagnostics.can_utils import get_config_value, set_config_value
    CAN_UTILS_AVAILABLE = True
except ImportError:
    get_config_value = set_config_value = None
    CAN_UTILS_AVAILABLE = False

# =============================================================================
# CONSTANTS
# =============================================================================

# Role constants
ROLE_SUPER_ADMIN = "super_admin"
ROLE_ADMIN = "admin"
ROLE_TECHNICIAN = "technician"

# Execution modes (match SQL enum and runner.ExecutionMode)
EXECUTION_MODE_SINGLE = "single"
EXECUTION_MODE_STREAM = "stream"
EXECUTION_MODE_FLASHING = "flashing"

# Test page types
PAGE_TYPE_LIVE_PARAMETER = "LIVE_PARAMETER"
PAGE_TYPE_WRITE_DATA_IDENTIFIER = "WRITE_DATA_IDENTIFIER"
PAGE_TYPE_INPUT_OUTPUT_CONTROL = "INPUT_OUTPUT_CONTROL"
PAGE_TYPE_ROUTINE_CONTROL = "ROUTINE_CONTROL"
PAGE_TYPE_DTC = "DTC"
PAGE_TYPE_ECU_FLASHING = "ECU_FLASHING"
PAGE_TYPE_IUPR = "IUPR"
PAGE_TYPE_VEHICLE_HEALTH = "VEHICLE_HEALTH"
PAGE_TYPE_VEHICLE_HEALTH_IO = "VEHICLE_HEALTH_IO"
PAGE_TYPE_VEHICLE_HEALTH_PHYSICAL = "VEHICLE_HEALTH_PHYSICAL"
PAGE_TYPE_DEALER_DETAILS = "DEALER_DETAILS"

# Default execution settings
DEFAULT_TIMEOUT_SEC = 15
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_DELAY_SEC = 1.5

# Auto-run fallback actions
FALLBACK_MANUAL_INPUT = "manual_input"
FALLBACK_WARN_AND_CONTINUE = "warn_and_continue"
FALLBACK_BLOCK = "block"
FALLBACK_NONE = "none"

# =============================================================================
# LOGGING
# =============================================================================

import logging

logger = logging.getLogger(__name__)


def _log_info(message: str):
    logger.info(f"[SERVICE] {message}")


def _log_warn(message: str):
    logger.warning(f"[SERVICE] {message}")


def _log_error(message: str):
    logger.error(f"[SERVICE] {message}")


def _log_debug(message: str):
    logger.debug(f"[SERVICE] {message}")


# =============================================================================
# PROGRESS CALLBACK REGISTRATION
# =============================================================================

_progress_callback: Optional[Callable[[str, int, str], None]] = None


def register_progress_callback(func: Callable[[str, int, str], None]):
    """
    Register a UI-safe progress callback.

    Signature: (task_id: str, percent: int, message: str) -> None
    """
    global _progress_callback
    _progress_callback = func
    _log_info("Progress callback registered")


def unregister_progress_callback():
    """Unregister the progress callback."""
    global _progress_callback
    _progress_callback = None
    _log_info("Progress callback unregistered")


# =============================================================================
# CUSTOM EXCEPTIONS
# =============================================================================

class PermissionDeniedError(Exception):
    """Raised when a user lacks permission for an operation."""
    pass


class TestNotFoundError(Exception):
    """Raised when a test is not found."""
    pass


class ValidationError(Exception):
    """Raised when input validation fails."""

    def __init__(self, message: str, errors: List[str] = None):
        super().__init__(message)
        self.errors = errors or []


class ServiceExecutionError(Exception):
    """Raised when test execution fails at service layer."""
    pass


class AutoRunError(Exception):
    """Raised when auto-run session fails."""

    def __init__(self, message: str, program_id: str = None, fallback_action: str = None):
        super().__init__(message)
        self.program_id = program_id
        self.fallback_action = fallback_action


# =============================================================================
# SMALL HELPERS (AUTO-RUN PERSISTENCE HARDENING)
# =============================================================================

_NUM_RE = re.compile(r"-?\d+(\.\d+)?")

def _coerce_float(v: Any) -> Optional[float]:
    """
    Convert a value to float for numeric DB columns.
    Returns None if conversion is impossible.
    """
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # Allow "48.2 V" style strings
        m = _NUM_RE.search(s)
        if not m:
            return None
        try:
            return float(m.group(0))
        except Exception:
            return None
    # Don't attempt to coerce dict/list etc.
    return None


def _parse_ts(v: Any) -> Optional[datetime]:
    """
    Parse timestamps safely for ecu_active_status.last_response (timestamp column).
    Accepts datetime or ISO strings; returns None otherwise.
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            # tolerate Z suffix
            return datetime.fromisoformat(s.replace("Z", ""))
        except Exception:
            return None
    return None


def _as_dict(obj: Any) -> Optional[Dict[str, Any]]:
    return obj if isinstance(obj, dict) else None


def _as_list(obj: Any) -> Optional[List[Any]]:
    return obj if isinstance(obj, list) else None


def _extract_ecu_statuses_anywhere(result_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract ECU status from a variety of shapes. Supports:
    - result["ecu_statuses"] / result["ecus"]
    - result["result_data"]["ecu_statuses"] / ["ecus"]
    - result["result_data"]["data"]["ecu_statuses"] / ["ecus"]
    - result["result_data"]["details"] as dict {"BMS": true/false}
    - a raw list in result["result_data"]
    """
    if not isinstance(result_dict, dict):
        return []

    candidates: List[Any] = []

    # top-level keys (rare, but allow)
    candidates.append(result_dict.get("ecu_statuses"))
    candidates.append(result_dict.get("ecus"))
    candidates.append(result_dict.get("details"))

    rd = result_dict.get("result_data")
    candidates.append(rd)

    if isinstance(rd, dict):
        candidates.append(rd.get("ecu_statuses"))
        candidates.append(rd.get("ecus"))
        candidates.append(rd.get("details"))

        data = rd.get("data")
        if isinstance(data, dict):
            candidates.append(data.get("ecu_statuses"))
            candidates.append(data.get("ecus"))
            candidates.append(data.get("details"))

    # Normalize
    for c in candidates:
        if isinstance(c, list):
            out: List[Dict[str, Any]] = []
            for item in c:
                if isinstance(item, dict):
                    out.append(item)
            if out:
                return out

        if isinstance(c, dict):
            # details dict
            # shape: {"BMS": true/false, ...}
            # Convert to ecu_statuses list
            if all(isinstance(k, str) for k in c.keys()):
                ok_values = [v for v in c.values() if isinstance(v, (bool, int))]
                if ok_values and len(ok_values) == len(c):
                    out2: List[Dict[str, Any]] = []
                    for ecu_code, ok in c.items():
                        out2.append({
                            "ecu_code": ecu_code,
                            "is_active": bool(ok),
                            "error_count": 0 if bool(ok) else 1,
                            "last_response": None,  # timestamp column; set only if real timestamp
                        })
                    return out2

    return []


# =============================================================================
# PERMISSION ENFORCEMENT
# =============================================================================

def check_vehicle_permission(user_id: int, vehicle_id: int, user_role: str) -> bool:
    """Check if a user has permission to access a vehicle."""
    if user_role == ROLE_SUPER_ADMIN:
        return True

    if not DATABASE_AVAILABLE or query_one is None:
        _log_warn("Database not available for permission check")
        return False

    try:
        row = query_one("""
            SELECT 1 FROM app.user_vehicle_permissions
            WHERE user_id = :uid
              AND vehicle_id = :vid
              AND is_active = TRUE
        """, {"uid": user_id, "vid": vehicle_id})

        return row is not None

    except Exception as e:
        _log_error(f"Error checking vehicle permission: {e}")
        return False


def check_test_permission(user_id: int, test_id: str, user_role: str) -> bool:
    """Check if a user has permission to execute a specific test."""
    if user_role == ROLE_SUPER_ADMIN:
        return True

    if not DATABASE_AVAILABLE or query_one is None:
        _log_warn("Database not available for permission check")
        return False

    try:
        row = query_one("""
            SELECT 1 FROM app.user_test_permissions
            WHERE user_id = :uid
              AND test_action_id = :tid
              AND permission_type = 'execute'
              AND is_active = TRUE
              AND (expires_at IS NULL OR expires_at > CURRENT_TIMESTAMP)
        """, {"uid": user_id, "tid": test_id})

        return row is not None

    except Exception as e:
        _log_error(f"Error checking test permission: {e}")
        # backward compat: allow if table missing
        return True


def enforce_vehicle_permission(user_id: int, vehicle_id: int, user_role: str):
    """Enforce vehicle permission, raising exception if denied."""
    if not check_vehicle_permission(user_id, vehicle_id, user_role):
        raise PermissionDeniedError(
            f"User {user_id} lacks permission for vehicle {vehicle_id}"
        )


def enforce_test_permission(user_id: int, test_id: str, user_role: str):
    """Enforce test permission, raising exception if denied."""
    if not check_test_permission(user_id, test_id, user_role):
        raise PermissionDeniedError(
            f"User {user_id} lacks permission for test {test_id}"
        )


# =============================================================================
# INPUT VALIDATION
# =============================================================================

def validate_input_value(value: Any, spec: Dict[str, Any]) -> Any:
    """Validate and coerce an input value according to its specification."""
    input_type = spec.get("input_type", "string")
    name = spec.get("name", "unknown")

    if value is None or value == "":
        if spec.get("is_required", False):
            raise ValidationError(f"Required input missing: {name}")

        default = spec.get("default_value")
        if default is not None:
            return _coerce_value(default, input_type)
        return None

    try:
        coerced = _coerce_value(value, input_type)

        length = spec.get("length")
        if length is not None and input_type in ("string", "hex"):
            if len(str(coerced)) != int(length):
                raise ValidationError(
                    f"Input '{name}' length must be {length}, got {len(str(coerced))}"
                )

        if input_type in ("int", "float"):
            min_val = spec.get("min_value")
            max_val = spec.get("max_value")

            if min_val is not None and float(coerced) < float(min_val):
                raise ValidationError(
                    f"Input '{name}' must be >= {min_val}, got {coerced}"
                )

            if max_val is not None and float(coerced) > float(max_val):
                raise ValidationError(
                    f"Input '{name}' must be <= {max_val}, got {coerced}"
                )

        enum_values = spec.get("enum_values")
        if enum_values:
            if isinstance(enum_values, str):
                try:
                    enum_values = json.loads(enum_values)
                except Exception:
                    pass

            if isinstance(enum_values, list) and coerced not in enum_values:
                raise ValidationError(
                    f"Input '{name}' must be one of {enum_values}, got {coerced}"
                )

        return coerced

    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(f"Invalid {input_type} value for '{name}': {e}")


def _coerce_value(value: Any, input_type: str) -> Any:
    """Coerce a value to the specified type."""
    if input_type == "int":
        return int(value)
    elif input_type == "float":
        return float(value)
    elif input_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes", "on")
        return bool(value)
    elif input_type == "hex":
        hex_str = str(value).upper().replace("0X", "").replace("0x", "")
        int(hex_str, 16)  # validate
        return hex_str
    elif input_type == "datetime":
        return str(value)
    else:
        return str(value)


def resolve_test_inputs(test_id: str, user_inputs: Dict[str, Any]) -> List[Any]:
    """
    Resolve and validate test inputs from DB specs.

    Returns ordered argument list.
    """
    if not DATABASE_AVAILABLE or query_all is None:
        return list(user_inputs.values()) if user_inputs else []

    try:
        input_specs = query_all("""
            SELECT
                name, label, input_type, length,
                min_value, max_value, enum_values,
                default_value, config_key, format_hint,
                is_required, sort_order
            FROM app.test_inputs
            WHERE test_id = :tid
            ORDER BY sort_order, name
        """, {"tid": test_id})

    except Exception as e:
        _log_error(f"Error loading input specs for test {test_id}: {e}")
        return list(user_inputs.values()) if user_inputs else []

    if not input_specs:
        return list(user_inputs.values()) if user_inputs else []

    args: List[Any] = []
    errors: List[str] = []

    for spec in input_specs:
        name = spec["name"]

        if name in user_inputs:
            value = user_inputs[name]
        elif spec.get("default_value") is not None:
            value = spec["default_value"]
        elif spec.get("is_required", False):
            errors.append(f"Required input missing: {name}")
            continue
        else:
            continue

        try:
            validated = validate_input_value(value, spec)
            args.append(validated)
        except ValidationError as e:
            errors.append(str(e))

    if errors:
        raise ValidationError("Input validation failed", errors=errors)

    return args


# =============================================================================
# FLASHING SAFETY
# =============================================================================

def validate_flashing_requirements(
    test_id: str, user_inputs: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate flashing-specific requirements."""
    if not DATABASE_AVAILABLE or query_one is None:
        return {}

    try:
        flash_config = query_one("""
            SELECT file_name, file_type, method, required_inputs
            FROM app.test_flashing_config
            WHERE test_id = :tid
        """, {"tid": test_id})

    except Exception as e:
        _log_error(f"Error loading flashing config for test {test_id}: {e}")
        return {}

    if not flash_config:
        return {}

    required = flash_config.get("required_inputs")
    if required:
        if isinstance(required, str):
            try:
                required = json.loads(required)
            except Exception:
                pass

        if isinstance(required, list):
            # Note: this uses truthiness; if you need to allow 0/False, adjust.
            missing = [k for k in required if not user_inputs.get(k)]
            if missing:
                raise ValidationError(
                    f"Flashing requires missing inputs: {', '.join(missing)}",
                    errors=[f"Missing: {k}" for k in missing],
                )

    return {
        "file_name": flash_config.get("file_name"),
        "file_type": flash_config.get("file_type"),
        "method": flash_config.get("method"),
    }


# =============================================================================
# EXECUTION CONFIG LOADING
# =============================================================================

def load_execution_config(test_id: str) -> Dict[str, Any]:
    """Load execution configuration for a test from DB."""
    default_config = {
        "execution_mode": EXECUTION_MODE_SINGLE,
        "supports_run_all": True,
        "timeout_sec": DEFAULT_TIMEOUT_SEC,
        "max_retries": DEFAULT_MAX_RETRIES,
        "retry_delay_sec": DEFAULT_RETRY_DELAY_SEC,
        "retry_on_timeout": False,
        "retry_on_exception": False,
    }

    if not DATABASE_AVAILABLE or query_one is None:
        return default_config

    try:
        config = query_one("""
            SELECT
                execution_mode, supports_run_all,
                timeout_sec, max_retries, retry_delay_sec,
                retry_on_timeout, retry_on_exception
            FROM app.test_execution_config
            WHERE test_id = :tid
        """, {"tid": test_id})

        if config:
            return {
                "execution_mode": config.get("execution_mode")
                or default_config["execution_mode"],
                "supports_run_all": config.get("supports_run_all", True),
                "timeout_sec": config.get("timeout_sec")
                or default_config["timeout_sec"],
                "max_retries": config.get("max_retries")
                or default_config["max_retries"],
                "retry_delay_sec": config.get("retry_delay_sec")
                or default_config["retry_delay_sec"],
                "retry_on_timeout": config.get("retry_on_timeout", False),
                "retry_on_exception": config.get("retry_on_exception", False),
            }

    except Exception as e:
        _log_error(f"Error loading execution config for test {test_id}: {e}")

    return default_config


def load_output_limits(test_id: str) -> List[Dict[str, Any]]:
    """Load output limits (LSL/USL) for a test."""
    if not DATABASE_AVAILABLE or query_all is None:
        return []

    try:
        limits = query_all("""
            SELECT signal, lsl, usl, unit
            FROM app.test_output_limits
            WHERE test_id = :tid
        """, {"tid": test_id})

        return [dict(lim) for lim in (limits or [])]

    except Exception as e:
        _log_error(f"Error loading output limits for test {test_id}: {e}")
        return []


# =============================================================================
# TEST METADATA LOADING
# =============================================================================

def load_test_metadata(test_id: str) -> Optional[Dict[str, Any]]:
    """Load complete test metadata from database."""
    if not DATABASE_AVAILABLE or query_one is None:
        return None

    try:
        test = query_one("""
            SELECT
                t.id,
                t.label,
                t.description,
                t.module_name,
                t.function_name,
                t.button_name,
                t.parameter_page_type,
                t.function_role,
                t.section,
                t.ecu,
                t.parameter,
                t.version,
                t.is_active,
                t.sort_order,
                t.vehicle_id,
                v.name AS vehicle_name,
                v.category AS vehicle_category
            FROM app.tests t
            JOIN app.vehicles v ON v.id = t.vehicle_id
            WHERE t.id = :tid
              AND t.is_active = TRUE
              AND v.is_active = TRUE
        """, {"tid": test_id})

        return dict(test) if test else None

    except Exception as e:
        _log_error(f"Error loading test metadata for {test_id}: {e}")
        return None


def load_test_by_vehicle_and_id(
    vehicle_name: str, test_id: str
) -> Optional[Dict[str, Any]]:
    """Load test metadata with vehicle name verification."""
    if not DATABASE_AVAILABLE or query_one is None:
        return None

    try:
        test = query_one("""
            SELECT
                t.id,
                t.label,
                t.description,
                t.module_name,
                t.function_name,
                t.button_name,
                t.parameter_page_type,
                t.function_role,
                t.section,
                t.ecu,
                t.parameter,
                t.version,
                t.is_active,
                t.sort_order,
                t.vehicle_id,
                v.name AS vehicle_name
            FROM app.tests t
            JOIN app.vehicles v ON v.id = t.vehicle_id
            WHERE t.id = :tid
              AND v.name = :vname
              AND t.is_active = TRUE
              AND v.is_active = TRUE
        """, {"tid": test_id, "vname": vehicle_name})

        return dict(test) if test else None

    except Exception as e:
        _log_error(f"Error loading test {test_id} for vehicle {vehicle_name}: {e}")
        return None


# =============================================================================
# CAN CONFIGURATION
# =============================================================================

def get_can_configuration() -> Dict[str, Any]:
    """Get CAN interface configuration."""
    default_config = {
        "can_interface": "PCAN_USBBUS1",
        "bitrate": 500000,
    }

    if not CAN_UTILS_AVAILABLE or get_config_value is None:
        return default_config

    try:
        interface = get_config_value("can_interface")
        bitrate = get_config_value("can_bitrate")

        return {
            "can_interface": interface or default_config["can_interface"],
            "bitrate": int(bitrate) if bitrate else default_config["bitrate"],
        }

    except Exception as e:
        _log_error(f"Error getting CAN configuration: {e}")
        return default_config


# =============================================================================
# RESULT PERSISTENCE
# =============================================================================

def create_result_persister(
    test_id: str, user_id: int, vehicle_id: int
) -> Callable[[Dict[str, Any]], None]:
    """
    Create a result persistence callback for a test execution.

    Uses CASE to safely handle NULL json values.
    """

    def persist_result(result: Dict[str, Any]):
        if not DATABASE_AVAILABLE or execute is None:
            return

        try:
            output = result.get("output")
            if output is not None:
                if isinstance(output, (dict, list)):
                    output_json = json.dumps(output)
                else:
                    output_json = json.dumps({"value": output})
            else:
                output_json = None

            violations = result.get("limit_violations", [])
            violations_json = json.dumps(violations) if violations else None

            execute("""
                INSERT INTO app.test_execution_results
                (test_id, task_id, vehicle_id, user_id, pass, exception,
                 output_json, limit_violations, duration_ms, attempts,
                 created_at)
                VALUES
                (:test_id, :task_id, :vehicle_id, :user_id, :pass,
                 :exception,
                 CASE WHEN :output_json IS NULL THEN NULL ELSE CAST(:output_json AS jsonb) END,
                 CASE WHEN :violations  IS NULL THEN NULL ELSE CAST(:violations  AS jsonb) END,
                 :duration_ms, :attempts, CURRENT_TIMESTAMP)
            """, {
                "test_id": test_id,
                "task_id": result.get("task_id"),
                "vehicle_id": vehicle_id,
                "user_id": user_id,
                "pass": result.get("pass", False),
                "exception": (
                    str(result.get("exception", ""))[:1000]
                    if result.get("exception")
                    else None
                ),
                "output_json": output_json,
                "violations": violations_json,
                "duration_ms": result.get("duration_ms", 0),
                "attempts": result.get("attempts", 1),
            })

            _log_debug(
                f"Persisted result for test {test_id}, "
                f"task {result.get('task_id')}"
            )

        except Exception as e:
            _log_error(f"Failed to persist result for test {test_id}: {e}")
            _log_error(traceback.format_exc())

    return persist_result


# =============================================================================
# AUTO-RUN — DB HELPERS
# =============================================================================

def _table_exists(schema: str, table: str) -> bool:
    """Return True if table exists (protect optional logging tables)."""
    if not DATABASE_AVAILABLE or query_one is None:
        return False
    try:
        row = query_one("""
            SELECT 1
              FROM information_schema.tables
             WHERE table_schema = :schema
               AND table_name   = :table
        """, {"schema": schema, "table": table})
        return row is not None
    except Exception:
        return False


def _ensure_auto_run_session_row(
    session_id: str,
    vehicle_id: int,
    vehicle_name: str,
    user_id: int,
    section_type: str,
    status: str = "running",
) -> None:
    """
    Ensure a row exists in app.auto_run_sessions for the given session_id.
    Includes vehicle_name to satisfy NOT NULL constraint.
    Idempotent: uses ON CONFLICT DO NOTHING.
    """
    if not DATABASE_AVAILABLE or execute is None:
        return
    try:
        execute("""
            INSERT INTO app.auto_run_sessions
                (session_id, vehicle_id, user_id, vehicle_name, section_type, status, started_at)
            VALUES
                (:sid, :vid, :uid, :vname, :section, :status, CURRENT_TIMESTAMP)
            ON CONFLICT (session_id) DO NOTHING
        """, {
            "sid": session_id,
            "vid": vehicle_id,
            "uid": user_id,
            "vname": vehicle_name,
            "section": section_type,
            "status": status,
        })
    except Exception as e:
        _log_error(f"Failed to upsert auto_run_session {session_id}: {e}")


def _mark_auto_run_session_status(session_id: str, status: str) -> None:
    """Update status and end time for an auto-run session."""
    if not DATABASE_AVAILABLE or execute is None:
        return
    try:
        execute("""
            UPDATE app.auto_run_sessions
               SET status = :status,
                   ended_at = CASE
                                WHEN :status IN ('completed', 'stopped', 'failed') THEN CURRENT_TIMESTAMP
                                ELSE ended_at
                              END
             WHERE session_id = :sid
        """, {"sid": session_id, "status": status})
    except Exception as e:
        _log_warn(f"Failed to update auto_run_session status for {session_id}: {e}")


def _update_session_programs_config(session_id: str, programs_config: List[Dict[str, Any]]) -> None:
    """Persist the snapshot of auto-run programs used for this session."""
    if not DATABASE_AVAILABLE or execute is None:
        return
    try:
        execute("""
            UPDATE app.auto_run_sessions
               SET programs_config = CAST(:cfg AS jsonb)
             WHERE session_id = :sid
        """, {"sid": session_id, "cfg": json.dumps(programs_config)})
    except Exception as e:
        _log_warn(f"Failed to persist programs_config for session {session_id}: {e}")


def _update_session_vin(session_id: str, vin_value: str, source: str) -> None:
    """Store VIN and its source ('auto'|'manual') into auto_run_sessions."""
    if not DATABASE_AVAILABLE or execute is None:
        return
    vin_value = (vin_value or "").strip().upper()
    if len(vin_value) != 17:
        return
    try:
        execute("""
            UPDATE app.auto_run_sessions
               SET vin = :vin, vin_source = :src, vin_input_needed = TRUE
             WHERE session_id = :sid
        """, {"sid": session_id, "vin": vin_value, "src": source})
    except Exception as e:
        _log_warn(f"Failed to update VIN in session {session_id}: {e}")


def _set_vin_input_needed(session_id: str, needed: bool = True) -> None:
    """Set vin_input_needed flag in auto_run_sessions."""
    if not DATABASE_AVAILABLE or execute is None:
        return
    try:
        execute("""
            UPDATE app.auto_run_sessions
               SET vin_input_needed = :needed
             WHERE session_id = :sid
        """, {"sid": session_id, "needed": needed})
    except Exception as e:
        _log_warn(f"Failed to set vin_input_needed for session {session_id}: {e}")


# FIX-21: Enhanced stream value persistence with better error handling
def _persist_stream_value(
    *,
    session_id: str,
    vehicle_id: int,
    program_id: str,
    signal_name: str,
    signal_value: Optional[float],
    signal_unit: Optional[str],
    lsl: Optional[float],
    usl: Optional[float],
) -> None:
    """Upsert latest stream value into app.auto_run_stream_values."""
    
    _log_debug(f"STREAM PERSIST - Called for {session_id}/{program_id}/{signal_name}")
    _log_debug(f"  - Value: {signal_value}, Unit: {signal_unit}")
    _log_debug(f"  - Limits: LSL={lsl}, USL={usl}")
    
    if not DATABASE_AVAILABLE or execute is None:
        _log_error("STREAM PERSIST - Database not available!")
        return

    is_within = None
    try:
        if signal_value is not None and (lsl is not None or usl is not None):
            v = float(signal_value)
            ok_l = (lsl is None or v >= float(lsl))
            ok_u = (usl is None or v <= float(usl))
            is_within = ok_l and ok_u
            _log_debug(f"STREAM PERSIST - Limit check: v={v}, lsl={lsl}, usl={usl}, within={is_within}")
    except Exception as e:
        _log_warn(f"STREAM PERSIST - Limit calculation error: {e}")
        is_within = None

    try:
        _log_debug(f"STREAM PERSIST - Executing INSERT/UPDATE")
        execute("""
            INSERT INTO app.auto_run_stream_values
              (session_id, vehicle_id, program_id,
               signal_name, signal_value, signal_unit,
               lsl, usl, is_within_limit, updated_at)
            VALUES
              (:sid, :vid, :pid,
               :sname, :sval, :sunit,
               :lsl, :usl, :within, CURRENT_TIMESTAMP)
            ON CONFLICT (session_id, signal_name) DO UPDATE SET
               signal_value    = EXCLUDED.signal_value,
               signal_unit     = EXCLUDED.signal_unit,
               lsl             = EXCLUDED.lsl,
               usl             = EXCLUDED.usl,
               is_within_limit = EXCLUDED.is_within_limit,
               updated_at      = CURRENT_TIMESTAMP
        """, {
            "sid": session_id,
            "vid": vehicle_id,
            "pid": program_id,
            "sname": signal_name,
            "sval": signal_value,
            "sunit": signal_unit,
            "lsl": lsl,
            "usl": usl,
            "within": is_within,
        })
        _log_debug(f"STREAM PERSIST - Success for {signal_name}")
    except Exception as e:
        _log_error(f"STREAM PERSIST - Failed: {e}")
        _log_error(traceback.format_exc())


def _persist_ecu_status(
    *,
    session_id: str,
    vehicle_id: int,
    ecu_code: str,
    is_active: bool,
    last_response: Any = None,
    error_count: Any = 0,
) -> None:
    """
    Upsert per-ECU active status into app.ecu_active_status.
    """
    if not DATABASE_AVAILABLE or execute is None:
        return

    ecu_code = (ecu_code or "").strip()
    if not ecu_code:
        return

    last_resp_ts = _parse_ts(last_response)

    try:
        execute("""
            INSERT INTO app.ecu_active_status
              (session_id, vehicle_id, ecu_code, is_active, last_response, error_count, updated_at)
            VALUES
              (:sid, :vid, :ecu, :active, :last_resp, :err, CURRENT_TIMESTAMP)
            ON CONFLICT (session_id, ecu_code) DO UPDATE SET
              is_active     = EXCLUDED.is_active,
              last_response = EXCLUDED.last_response,
              error_count   = EXCLUDED.error_count,
              updated_at    = CURRENT_TIMESTAMP
        """, {
            "sid": session_id,
            "vid": vehicle_id,
            "ecu": ecu_code,
            "active": bool(is_active),
            "last_resp": last_resp_ts,
            "err": int(error_count or 0),
        })
        _log_debug(f"Persisted ECU status: {ecu_code}={is_active}")
    except Exception as e:
        _log_warn(f"Failed to upsert ECU status [{ecu_code}]: {e}")


# =============================================================================
# AUTO-RUN RESULT PERSISTENCE
# =============================================================================

def create_auto_run_result_persister(
    vehicle_id: int,
    user_id: int,
    session_id: str,
    section_type: Optional[str] = None,
    vehicle_name: Optional[str] = None,
) -> Callable[[str, str, Dict[str, Any]], None]:
    """
    Create a callback to persist individual auto-run program results.
    """

    def persist_auto_run_result(
        sid: str, program_id: str, result: Dict[str, Any]
    ):
        if not DATABASE_AVAILABLE or execute is None:
            return

        try:
            _ensure_auto_run_session_row(
                session_id=session_id,
                vehicle_id=vehicle_id,
                vehicle_name=vehicle_name or result.get("vehicle_name") or "",
                user_id=user_id,
                section_type=section_type or result.get("section_type") or "diagnostics",
                status="running",
            )

            execute("""
                INSERT INTO app.auto_run_results
                (session_id, vehicle_id, user_id, program_id,
                 program_name, program_type, status, passed,
                 result_value, result_data, error_message,
                 manual_input, log_as_vin, created_at, updated_at)
                VALUES
                (:sid, :vid, :uid, :pid,
                 :pname, :ptype, :status, :passed,
                 :rvalue,
                 CASE WHEN :rdata IS NULL THEN NULL ELSE CAST(:rdata AS jsonb) END,
                 :error,
                 :manual, :vin_flag, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ON CONFLICT (session_id, program_id) DO UPDATE SET
                    status        = EXCLUDED.status,
                    passed        = EXCLUDED.passed,
                    result_value  = EXCLUDED.result_value,
                    result_data   = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    manual_input  = EXCLUDED.manual_input,
                    updated_at    = CURRENT_TIMESTAMP
            """, {
                "sid": sid,
                "vid": vehicle_id,
                "uid": user_id,
                "pid": program_id,
                "pname": result.get("program_name", program_id),
                "ptype": result.get("program_type", "single"),
                "status": result.get("status", "unknown"),
                "passed": result.get("passed", False),
                "rvalue": result.get("result_value"),
                "rdata": json.dumps(result.get("result_data"))
                         if result.get("result_data") is not None else None,
                "error": result.get("error_message"),
                "manual": result.get("manual_input", False),
                "vin_flag": result.get("log_as_vin", False),
            })

            _log_debug(f"Persisted auto-run result: {program_id} session={sid}")

        except Exception as e:
            _log_error(f"Failed to persist auto-run result {program_id}: {e}")

    return persist_auto_run_result


# =============================================================================
# AUTO-RUN FUNCTION RESOLUTION
# =============================================================================

def _resolve_auto_run_function(
    vehicle_name: str,
    program: Dict[str, Any],
) -> Optional[Callable]:
    """
    Resolve the callable for an auto-run program.
    """
    module_name = program.get("module_name", "")
    function_name = program.get("function_name", "")
    program_id = program.get("program_id", "unknown")
    program_type = program.get("program_type", "single")

    if not module_name or not function_name:
        _log_error(
            f"Auto-run program {program_id} missing module_name or function_name. "
            f"Check section_tests.json 'auto_run_programs' config."
        )
        return None

    try:
        root = get_vehicle_root(vehicle_name)
    except FileNotFoundError:
        _log_error(f"Vehicle root not found: {vehicle_name}")
        return None

    safe_module = safe_name(module_name)

    candidates = [
        os.path.join(root, "Auto_Run", f"{safe_module}.py"),
        os.path.join(root, "Diagnostics", "Auto_Run", f"{safe_module}.py"),
    ]

    ecu_targets = program.get("ecu_targets", [])
    for ecu in ecu_targets:
        candidates.append(
            os.path.join(
                root, "Diagnostics", safe_name(ecu),
                "Auto_Run", f"{safe_module}.py"
            )
        )

    _log_debug(f"Resolving {program_id}: Searching {len(candidates)} paths for {module_name}.py")

    for module_path in candidates:
        if os.path.isfile(module_path):
            try:
                fn = load_function_from_path(module_path, function_name)
                
                # FIX-23: Check if function is generator for stream programs
                if program_type == "stream" and not inspect.isgeneratorfunction(fn):
                    _log_error(f"Program {program_id} is type 'stream' but function {function_name} is not a generator!")
                    # Don't return None, let it fail at runtime with clear error
                
                _log_info(f"Resolved auto-run function: {function_name} from {module_path}")
                return fn
            except (ModuleLoadError, FunctionNotFoundError) as e:
                _log_warn(f"Found file {module_path} but function load failed: {e}")
                continue
            except Exception as e:
                _log_error(f"Unexpected error loading function from {module_path}: {e}")
                continue
        else:
            _log_debug(f"Path not found: {module_path}")

    _log_error(
        f"Auto-run function NOT FOUND: {module_name}.{function_name} "
        f"for vehicle {vehicle_name}. Searched paths: {candidates}"
    )
    return None


# =============================================================================
# AUTO-RUN SESSION MANAGEMENT
# =============================================================================

def start_auto_run(
    *,
    user_id: int,
    user_role: str,
    vehicle_name: str,
    section_type: str,
    on_progress: Optional[Callable[[str, int, str], None]] = None,
    on_single_result: Optional[Callable] = None,
    on_stream_data: Optional[Callable] = None,
    on_vin_needed: Optional[Callable[[str, str, Dict], None]] = None,
) -> Dict[str, Any]:
    """
    Start an auto-run session for a vehicle section.
    """
    _log_info(
        f"Starting auto-run: vehicle={vehicle_name}, "
        f"section={section_type}, user={user_id}"
    )

    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return {"ok": False, "error": "VEHICLE_NOT_FOUND"}

    vehicle_id = vehicle["id"]

    try:
        enforce_vehicle_permission(user_id, vehicle_id, user_role)
    except PermissionDeniedError:
        return {"ok": False, "error": "VEHICLE_PERMISSION_DENIED"}

    try:
        program_configs = get_auto_run_config(vehicle_name, section_type)
    except VehicleNotFoundError:
        return {"ok": False, "error": "VEHICLE_NOT_FOUND"}
    except Exception as e:
        _log_error(f"Error loading auto-run config: {e}")
        return {"ok": False, "error": f"CONFIG_LOAD_FAILED: {e}"}

    if not program_configs:
        _log_info(f"No auto-run programs for {vehicle_name}/{section_type}")
        return {
            "ok": True,
            "session_id": None,
            "programs": [],
            "task_ids": {},
            "message": "No auto-run programs configured",
        }

    can_config = get_can_configuration()

    specs: List[AutoRunProgramSpec] = []
    skipped: List[Dict[str, Any]] = []

    # FIX-22: Pre-compute limits map for stream programs
    limits_by_program: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for prog_config in sorted(program_configs, key=lambda p: p.get("sort_order", 0)):
        program_id = prog_config.get("program_id", "")
        program_name = prog_config.get("program_name", program_id)
        program_type = prog_config.get("program_type", "single")

        if not program_id:
            _log_warn("Skipping auto-run program with empty program_id")
            continue

        fn = _resolve_auto_run_function(vehicle_name, prog_config)
        if fn is None:
            skipped.append({
                "program_id": program_id,
                "program_name": program_name,
                "reason": "FUNCTION_NOT_FOUND",
            })
            continue

        execution_mode = prog_config.get("execution_mode", "single")
        timeout_sec = prog_config.get("timeout_sec", DEFAULT_TIMEOUT_SEC)
        output_limits = prog_config.get("output_limits", [])

        # FIX-22: Store limits for stream programs
        if program_type == "stream":
            prog_limits: Dict[str, Dict[str, Any]] = {}
            for lim in output_limits:
                sig = lim.get("signal")
                if sig:
                    prog_limits[sig] = lim
            limits_by_program[program_id] = prog_limits
            _log_debug(f"Stored limits for stream program {program_id}: {prog_limits}")

        persist_fn = create_result_persister(
            test_id=program_id,
            user_id=user_id,
            vehicle_id=vehicle_id,
        )

        ctx = ExecutionContext(
            test_id=program_id,
            execution_mode=execution_mode,
            timeout_sec=timeout_sec,
            max_retries=1,
            retry_delay_sec=0.5,
            output_limits=output_limits,
            persist_result=persist_fn,
            stream_callback=None,  # Will be set later
            retry_on_timeout=False,
            retry_on_exception=False,
            supports_run_all=False,
            metadata={
                "vehicle_name": vehicle_name,
                "section_type": section_type,
                "program_type": program_type,
                "is_auto_run": True,
            },
        )

        args = [can_config["can_interface"], can_config["bitrate"]]

        spec = AutoRunProgramSpec(
            program_id=program_id,
            program_name=program_name,
            program_type=program_type,
            fn=fn,
            args=args,
            ctx=ctx,
            display_type=prog_config.get("display_type", "text"),
            display_label=prog_config.get("display_label", program_name),
            display_unit=prog_config.get("display_unit"),
            display_pages=prog_config.get("display_pages", ["section", "ecu", "parameter"]),
            ecu_targets=prog_config.get("ecu_targets", []),
            output_limits=output_limits,
            fallback_action=prog_config.get("fallback_action", "none"),
            fallback_input=prog_config.get("fallback_input"),
            log_as_vin=prog_config.get("log_as_vin", False),
            is_required=prog_config.get("is_required", True),
            sort_order=prog_config.get("sort_order", 0),
        )

        specs.append(spec)
        _log_debug(f"Prepared auto-run spec: {program_id} ({spec.program_type}, {spec.display_type})")

    if not specs:
        return {"ok": False, "error": "NO_RESOLVABLE_PROGRAMS", "skipped": skipped}

    session = create_auto_run_session(specs)
    session_id = session.session_id

    _log_info(f"Auto-run session created: {session_id} with {len(specs)} programs")

    _ensure_auto_run_session_row(
        session_id=session_id,
        vehicle_id=vehicle_id,
        vehicle_name=vehicle_name,
        user_id=user_id,
        section_type=section_type,
        status="running",
    )

    try:
        _update_session_programs_config(session_id, program_configs)
    except Exception as e:
        _log_warn(f"Failed to persist programs_config: {e}")

    db_persister = create_auto_run_result_persister(
        vehicle_id, user_id, session_id, section_type, vehicle_name
    )

    vin_manual_triggered = set()

    # Map for callback lookup
    specs_map = {s.program_id: s for s in specs}

    def on_single_result_wrapper(sid: str, pid: str, result: Dict[str, Any]):
        """Persist single program result + VIN + ECU statuses."""
        db_persister(sid, pid, result)

        # VIN persistence (auto read)
        try:
            if result.get("log_as_vin") and result.get("passed"):
                vin_value = result.get("result_value")
                if isinstance(vin_value, str) and len(vin_value.strip()) == 17:
                    _update_session_vin(sid, vin_value, source="auto")
        except Exception as e:
            _log_warn(f"Failed to update session VIN: {e}")

        # VIN fallback trigger
        if (
            result.get("log_as_vin", False)
            and not result.get("passed", False)
            and result.get("fallback_action") == FALLBACK_MANUAL_INPUT
            and pid not in vin_manual_triggered
        ):
            vin_manual_triggered.add(pid)
            _log_info(f"VIN auto-read failed for {pid}, marking vin_input_needed")
            
            _set_vin_input_needed(sid, needed=True)

            if on_vin_needed:
                try:
                    on_vin_needed(sid, pid, result.get("fallback_input", {}))
                except Exception as e:
                    _log_error(f"on_vin_needed callback error: {e}")

        # Optional VIN log table
        try:
            if (result.get("log_as_vin") and result.get("passed")
                    and _table_exists("app", "auto_run_vin_log")):
                vin_value = result.get("result_value")
                if (DATABASE_AVAILABLE and execute is not None
                        and isinstance(vin_value, str)
                        and len(vin_value.strip()) == 17):
                    execute("""
                        INSERT INTO app.auto_run_vin_log
                            (session_id, program_id, vin, source, created_at)
                        VALUES
                            (:sid, :pid, :vin, 'auto', CURRENT_TIMESTAMP)
                        ON CONFLICT DO NOTHING
                    """, {"sid": sid, "pid": pid, "vin": vin_value.strip().upper()})
        except Exception as e:
            _log_warn(f"Failed to log auto VIN: {e}")

        # ECU active statuses handling (FIXED)
        try:
            # 1. Try to extract from payload
            ecu_statuses = _extract_ecu_statuses_anywhere(result)
            
            if ecu_statuses:
                # Payload had explicit statuses
                for s in ecu_statuses:
                    if not isinstance(s, dict):
                        continue
                    _persist_ecu_status(
                        session_id=sid,
                        vehicle_id=vehicle_id,
                        ecu_code=s.get("ecu_code") or s.get("ecu") or "",
                        is_active=bool(s.get("is_active", False)),
                        last_response=s.get("last_response"),
                        error_count=s.get("error_count") or 0,
                    )
            else:
                # 2. Payload did NOT have statuses. Check if we have targets.
                spec = specs_map.get(pid)
                if spec and spec.ecu_targets:
                    # If the program failed, we assume targets are inactive
                    is_ok = result.get("passed", False)
                    if not is_ok:
                        _log_warn(f"Program {pid} failed. Marking targets {spec.ecu_targets} as inactive.")
                        for ecu_code in spec.ecu_targets:
                            _persist_ecu_status(
                                session_id=sid,
                                vehicle_id=vehicle_id,
                                ecu_code=ecu_code,
                                is_active=False,
                                error_count=1
                            )
        except Exception as e:
            _log_warn(f"Failed to persist ECU statuses: {e}")

        if on_single_result:
            try:
                on_single_result(sid, pid, result)
            except Exception as e:
                _log_error(f"on_single_result callback error: {e}")

    # FIX-20 & FIX-21: Enhanced stream data wrapper with comprehensive logging
    def on_stream_data_wrapper(sid: str, pid: str, stream_payload: Any):
        """
        Persist streaming data to DB and forward to external callback.
        """
        _log_info(f"STREAM WRAPPER - Called for {pid}, session={sid}")
        _log_info(f"STREAM WRAPPER - Payload: {stream_payload}")
        
        # FIX-22: Use pre-computed limits
        lims = limits_by_program.get(pid, {})
        _log_info(f"STREAM WRAPPER - Limits for {pid}: {lims}")

        def handle_data_dict(data: Dict[str, Any]):
            for signal_name, raw_value in data.items():
                if not isinstance(signal_name, str) or signal_name.startswith("_"):
                    continue

                _log_info(f"STREAM WRAPPER - Processing {signal_name}={raw_value}")
                num_value = _coerce_float(raw_value)
                
                if num_value is None:
                    _log_info(f"STREAM WRAPPER - Skipping {signal_name}: not numeric")
                    continue

                lim = lims.get(signal_name, {})
                lsl = _coerce_float(lim.get("lsl"))
                usl = _coerce_float(lim.get("usl"))
                unit = lim.get("unit")

                _log_info(f"STREAM WRAPPER - Calling _persist_stream_value for {signal_name}={num_value}")
                _persist_stream_value(
                    session_id=sid,
                    vehicle_id=vehicle_id,
                    program_id=pid,
                    signal_name=signal_name,
                    signal_value=num_value,
                    signal_unit=unit,
                    lsl=lsl,
                    usl=usl,
                )

        samples = stream_payload if isinstance(stream_payload, list) else [stream_payload]

        for sample in samples:
            if not isinstance(sample, dict):
                _log_info(f"STREAM WRAPPER - Skipping non-dict sample: {sample}")
                continue
            
            # Extract data from runner's standard format
            raw_data = sample.get("data") if isinstance(sample.get("data"), dict) else sample
            _log_info(f"STREAM WRAPPER - Raw data: {raw_data}")
            
            if isinstance(raw_data, dict):
                handle_data_dict(raw_data)

        if on_stream_data:
            try:
                on_stream_data(sid, pid, stream_payload)
            except Exception as e:
                _log_error(f"on_stream_data callback error: {e}")

    # FIX-20: Set stream callbacks on ExecutionContext with proper closure
    _log_info(f"Setting stream callbacks for {len(specs)} programs")
    stream_count = 0
    for spec in specs:
        if spec.program_type == "stream":
            stream_count += 1
            _log_info(f"  Setting stream callback for {spec.program_id}")
            
            # Create closure with captured variables
            def make_stream_cb(program_id: str):
                def stream_cb(task_id: str, data: Dict[str, Any]):
                    _log_info(f"STREAM CB CALLED - {program_id} with data: {data}")
                    on_stream_data_wrapper(session_id, program_id, data)
                return stream_cb
            
            spec.ctx.stream_callback = make_stream_cb(spec.program_id)
            _log_info(f"  Stream callback set for {spec.program_id}: {spec.ctx.stream_callback is not None}")
            
            # Verify function is generator
            if not inspect.isgeneratorfunction(spec.fn):
                _log_error(f"  WARNING: {spec.program_id} is type 'stream' but function is not a generator!")

    _log_info(f"Stream callbacks set for {stream_count} programs")

    start_result = start_auto_run_session(
        session_id=session_id,
        progress_cb=on_progress or _progress_callback,
        on_single_result=on_single_result_wrapper,
        on_stream_data=on_stream_data_wrapper,
    )

    if start_result.get("error"):
        _mark_auto_run_session_status(session_id, "failed")
        return {"ok": False, "session_id": session_id, "error": start_result["error"]}

    program_summary = []
    for spec in specs:
        program_summary.append({
            "program_id": spec.program_id,
            "program_name": spec.program_name,
            "program_type": spec.program_type,
            "display_type": spec.display_type,
            "display_label": spec.display_label,
            "display_unit": spec.display_unit,
            "display_pages": spec.display_pages,
            "fallback_action": spec.fallback_action,
            "fallback_input": spec.fallback_input,
            "log_as_vin": spec.log_as_vin,
            "is_required": spec.is_required,
        })

    return {
        "ok": True,
        "session_id": session_id,
        "programs": program_summary,
        "skipped": skipped,
        "task_ids": start_result.get("task_ids", {}),
    }


def get_auto_run_status(session_id: str) -> Dict[str, Any]:
    """
    Get the current status of an auto-run session.
    """
    session_data = get_auto_run_session(session_id)
    if not session_data:
        return {"ok": False, "error": "SESSION_NOT_FOUND"}

    task_ids = session_data.get("task_ids", {})
    live_statuses = {}
    for pid, tid in task_ids.items():
        live_statuses[pid] = get_task_status(tid)
    session_data["live_task_statuses"] = live_statuses

    results = session_data.get("results", {})
    all_required_passed = True
    any_vin_needed = False
    blocking_programs: List[str] = []

    vin = session_data.get("vin")
    vin_source = session_data.get("vin_source", "none")

    for pid, result in results.items():
        is_required = result.get("is_required", True)
        passed = result.get("passed", False)
        status = result.get("status", "")
        fallback = result.get("fallback_action", "none")
        log_as_vin = result.get("log_as_vin", False)
        
        # If this is a VIN program and VIN was provided (auto or manual), consider it passed
        if log_as_vin and vin and vin_source in ("auto", "manual"):
            passed = True
            status = "manual" if vin_source == "manual" else "success"
            # Update result in session
            result["passed"] = True
            result["status"] = status
        
        if is_required and not passed:
            if fallback == FALLBACK_MANUAL_INPUT and status != "manual":
                any_vin_needed = True
                blocking_programs.append(pid)
            elif fallback == FALLBACK_BLOCK:
                all_required_passed = False
                blocking_programs.append(pid)
            elif status == "failed":
                if fallback != FALLBACK_WARN_AND_CONTINUE:
                    all_required_passed = False
                    blocking_programs.append(pid)

    # If VIN was provided, remove VIN programs from blocking and recalculate
    if vin and vin_source in ("auto", "manual"):
        any_vin_needed = False
        blocking_programs = [p for p in blocking_programs if not results.get(p, {}).get("log_as_vin")]
        
        all_required_passed = True
        for pid, result in results.items():
            if result.get("log_as_vin"):
                continue  # Skip VIN program
            
            is_required = result.get("is_required", True)
            passed = result.get("passed", False)
            fallback = result.get("fallback_action", "none")
            
            if is_required and not passed:
                if fallback == FALLBACK_BLOCK:
                    all_required_passed = False
                    break
                elif fallback != FALLBACK_WARN_AND_CONTINUE:
                    all_required_passed = False
                    break

    session_data["all_required_passed"] = all_required_passed
    session_data["vin_input_needed"] = any_vin_needed
    session_data["blocking_programs"] = blocking_programs
    session_data["ok"] = True
    return session_data


def submit_auto_run_vin(session_id: str, program_id: str, vin_value: str) -> Dict[str, Any]:
    """
    Submit manual VIN for a failed auto-run VIN read.
    """
    if not vin_value or not isinstance(vin_value, str):
        return {"ok": False, "error": "VIN value required"}

    vin_value = vin_value.strip().upper()

    if len(vin_value) != 17:
        return {"ok": False, "error": f"VIN must be 17 characters, got {len(vin_value)}"}

    invalid_chars = set("IOQ") & set(vin_value)
    if invalid_chars:
        return {"ok": False, "error": f"VIN contains invalid characters: {invalid_chars}"}

    result = submit_manual_vin(session_id, program_id, vin_value)

    if result.get("ok"):
        _log_info(f"Manual VIN accepted: {vin_value} session={session_id}")

        if DATABASE_AVAILABLE and execute is not None and _table_exists("app", "auto_run_vin_log"):
            try:
                execute("""
                    INSERT INTO app.auto_run_vin_log
                    (session_id, program_id, vin, source, created_at)
                    VALUES (:sid, :pid, :vin, 'manual', CURRENT_TIMESTAMP)
                """, {"sid": session_id, "pid": program_id, "vin": vin_value})
            except Exception as e:
                _log_warn(f"Failed to log manual VIN: {e}")

        try:
            _update_session_vin(session_id, vin_value, source="manual")
        except Exception as e:
            _log_warn(f"Failed to update session VIN (manual): {e}")

    return result


def stop_auto_run(session_id: str) -> Dict[str, Any]:
    """Stop all auto-run programs for a session."""
    result = stop_auto_run_session(session_id)
    if result.get("ok"):
        _log_info(f"Auto-run session stopped: {session_id}")
        _mark_auto_run_session_status(session_id, "stopped")
    else:
        _mark_auto_run_session_status(session_id, "failed")
    return result


# =============================================================================
# LEGACY AUTO-RUN API (BACKWARD COMPATIBILITY)
# =============================================================================

def run_auto_programs(
    *,
    user_id: int,
    user_role: str,
    vehicle_name: str,
    section: str,
) -> Dict[str, Any]:
    """
    Legacy auto-run entry point.

    Delegates to start_auto_run().
    """
    result = start_auto_run(
        user_id=user_id,
        user_role=user_role,
        vehicle_name=vehicle_name,
        section_type=section,
    )

    if not result.get("ok"):
        return {
            "ok": False,
            "programs": [],
            "all_passed": False,
            "error": result.get("error"),
        }

    session_id = result.get("session_id")
    if not session_id:
        return {
            "ok": True,
            "programs": [],
            "all_passed": True,
            "message": "No auto-run programs configured",
        }

    max_wait = 30
    start = time.monotonic()

    while time.monotonic() - start < max_wait:
        status = get_auto_run_status(session_id)
        results = status.get("results", {})
        programs = result.get("programs", [])

        single_programs = [p for p in programs if p.get("program_type") == "single"]
        completed_singles = sum(
            1
            for p in single_programs
            if p["program_id"] in results
            and results[p["program_id"]].get("status") in ("success", "failed", "manual")
        )

        if completed_singles >= len(single_programs):
            break

        time.sleep(0.5)

    final_status = get_auto_run_status(session_id)
    legacy_programs: List[Dict[str, Any]] = []

    for prog in result.get("programs", []):
        pid = prog["program_id"]
        prog_result = final_status.get("results", {}).get(pid, {})

        legacy_programs.append({
            "program_id": pid,
            "program_name": prog["program_name"],
            "required": prog.get("is_required", True),
            "ok": prog_result.get("passed", False),
            "task_id": final_status.get("task_ids", {}).get(pid),
            "error": prog_result.get("error_message"),
            "result_value": prog_result.get("result_value"),
            "display_type": prog.get("display_type", "text"),
            "display_label": prog.get("display_label", ""),
            "display_unit": prog.get("display_unit"),
            "log_as_vin": prog.get("log_as_vin", False),
            "fallback_action": prog.get("fallback_action", "none"),
        })

    return {
        "ok": True,
        "session_id": session_id,
        "programs": legacy_programs,
        "all_passed": final_status.get("all_required_passed", False),
        "vin": final_status.get("vin"),
        "vin_source": final_status.get("vin_source", "none"),
    }


# =============================================================================
# MAIN EXECUTION API
# =============================================================================

def run_test(
    *,
    user_id: int,
    user_role: str,
    vehicle_name: str,
    test_id: str,
    user_inputs: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Execute a diagnostic test asynchronously."""
    user_inputs = user_inputs or {}

    _log_info(f"run_test called: test_id={test_id}, vehicle={vehicle_name}, user={user_id}")

    test = load_test_by_vehicle_and_id(vehicle_name, test_id)
    if not test:
        return {"ok": False, "error": "TEST_NOT_FOUND"}

    vehicle_id = test["vehicle_id"]
    section = test.get("section", "").lower()
    ecu = test.get("ecu")
    parameter = test.get("parameter", "")
    page_type = test.get("parameter_page_type", "")

    try:
        enforce_vehicle_permission(user_id, vehicle_id, user_role)
    except PermissionDeniedError:
        return {"ok": False, "error": "VEHICLE_PERMISSION_DENIED"}

    try:
        enforce_test_permission(user_id, test_id, user_role)
    except PermissionDeniedError:
        return {"ok": False, "error": "TEST_PERMISSION_DENIED"}

    if section == "diagnostics" and not ecu:
        return {"ok": False, "error": "ECU_REQUIRED_FOR_DIAGNOSTICS"}

    if page_type == PAGE_TYPE_ECU_FLASHING:
        try:
            flash_config = validate_flashing_requirements(test_id, user_inputs)
            _log_debug(f"Flashing config validated: {flash_config}")
        except ValidationError as e:
            return {"ok": False, "error": str(e), "details": e.errors}

    try:
        resolved_inputs = resolve_test_inputs(test_id, user_inputs)
    except ValidationError as e:
        return {"ok": False, "error": str(e), "details": e.errors}

    exec_config = load_execution_config(test_id)
    output_limits = load_output_limits(test_id)
    execution_mode = exec_config.get("execution_mode", EXECUTION_MODE_SINGLE)

    try:
        test_fn = load_test_function(
            vehicle_name,
            section=section,
            ecu=ecu,
            parameter=parameter,
            module_name=test["module_name"],
            function_name=test["function_name"],
        )
    except VehicleNotFoundError as e:
        _log_error(f"Vehicle not found: {e}")
        return {"ok": False, "error": "VEHICLE_NOT_FOUND"}
    except ModuleLoadError as e:
        _log_error(f"Module load error: {e}")
        return {"ok": False, "error": "MODULE_LOAD_FAILED"}
    except FunctionNotFoundError as e:
        _log_error(f"Function not found: {e}")
        return {"ok": False, "error": "FUNCTION_NOT_FOUND"}
    except Exception as e:
        _log_error(f"Unexpected error loading test function: {e}")
        _log_error(traceback.format_exc())
        return {"ok": False, "error": f"FUNCTION_LOAD_FAILED: {e}"}

    can_config = get_can_configuration()
    args = [can_config["can_interface"], can_config["bitrate"]]
    args.extend(resolved_inputs)

    persist_fn = create_result_persister(test_id, user_id, vehicle_id)

    ctx = ExecutionContext(
        test_id=str(test_id),
        execution_mode=execution_mode,
        timeout_sec=int(exec_config.get("timeout_sec", DEFAULT_TIMEOUT_SEC)),
        max_retries=int(exec_config.get("max_retries", DEFAULT_MAX_RETRIES)),
        retry_delay_sec=float(exec_config.get("retry_delay_sec", DEFAULT_RETRY_DELAY_SEC)),
        output_limits=output_limits,
        persist_result=persist_fn,
        retry_on_timeout=bool(exec_config.get("retry_on_timeout", False)),
        retry_on_exception=bool(exec_config.get("retry_on_exception", False)),
        supports_run_all=bool(exec_config.get("supports_run_all", True)),
        metadata={
            "vehicle_name": vehicle_name,
            "section": section,
            "ecu": ecu,
            "parameter": parameter,
            "page_type": page_type,
        },
    )

    try:
        task = execute_test_async(
            fn=test_fn,
            args=args,
            ctx=ctx,
            progress_cb=_progress_callback,
        )
    except Exception as e:
        _log_error(f"Failed to execute test async: {e}")
        _log_error(traceback.format_exc())
        return {"ok": False, "error": f"EXECUTION_FAILED: {e}"}

    task_id = task.get("task_id")
    _log_info(f"Test {test_id} started with task_id={task_id}")

    return {"ok": True, "task_id": task_id, "test_label": test.get("label"), "execution_mode": execution_mode}


# =============================================================================
# UI LISTING API — VEHICLE
# =============================================================================

def get_vehicle_by_name(vehicle_name: str) -> Optional[Dict[str, Any]]:
    """Get vehicle record by name."""
    if not DATABASE_AVAILABLE or query_one is None:
        return None

    try:
        vehicle = query_one("""
            SELECT id, name, description, category, vin_pattern,
                   image_filename, is_active
            FROM app.vehicles
            WHERE name = :name AND is_active = TRUE
        """, {"name": vehicle_name})

        return dict(vehicle) if vehicle else None

    except Exception as e:
        _log_error(f"Error getting vehicle {vehicle_name}: {e}")
        return None


def get_vehicle_by_id(vehicle_id: int) -> Optional[Dict[str, Any]]:
    """Get vehicle record by ID."""
    if not DATABASE_AVAILABLE or query_one is None:
        return None

    try:
        vehicle = query_one("""
            SELECT id, name, description, category, vin_pattern,
                   image_filename, is_active
            FROM app.vehicles
            WHERE id = :id AND is_active = TRUE
        """, {"id": vehicle_id})

        return dict(vehicle) if vehicle else None

    except Exception as e:
        _log_error(f"Error getting vehicle {vehicle_id}: {e}")
        return None


# =============================================================================
# UI LISTING API — SECTIONS
# =============================================================================

def list_sections_for_vehicle(vehicle_name: str, user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """
    List available sections for a vehicle.
    DB → JSON fallback → directory fallback.
    """
    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return []

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return []

    sections: List[Dict[str, Any]] = []

    if DATABASE_AVAILABLE and query_all is not None:
        try:
            db_sections = query_all("""
                SELECT
                    vs.id, vs.name, vs.slug, vs.section_type,
                    vs.description, vs.icon, vs.sort_order,
                    vs.is_active,
                    vsm.auto_run_programs
                FROM app.vehicle_sections vs
                LEFT JOIN app.vehicle_section_map vsm
                    ON vsm.vehicle_id = vs.vehicle_id
                   AND vsm.section_id = vs.id
                WHERE vs.vehicle_id = :vid AND vs.is_active = TRUE
                ORDER BY vs.sort_order, vs.name
            """, {"vid": vehicle["id"]})

            for s in db_sections or []:
                auto_run = s.get("auto_run_programs")
                if auto_run and isinstance(auto_run, str):
                    try:
                        auto_run = json.loads(auto_run)
                    except Exception:
                        auto_run = []

                sections.append({
                    "id": s["id"],
                    "name": s["name"],
                    "slug": s["slug"],
                    "section_type": s["section_type"],
                    "description": s.get("description", ""),
                    "icon": s.get("icon", ""),
                    "sort_order": s.get("sort_order", 0),
                    "auto_run_programs": auto_run or [],
                })

            if sections:
                return sections

        except Exception as e:
            _log_error(f"Error loading sections from DB: {e}")

    try:
        json_sections = get_sections_from_json(vehicle_name)
        for s in json_sections:
            if s.get("is_active", True):
                sections.append({
                    "name": s["name"],
                    "slug": s["slug"],
                    "section_type": s["section_type"],
                    "description": s.get("description", ""),
                    "icon": s.get("icon", ""),
                    "sort_order": s.get("sort_order", 0),
                    "auto_run_programs": s.get("auto_run_programs", []),
                })
    except Exception as e:
        _log_warn(f"Error loading sections from JSON: {e}")

    if not sections:
        try:
            available = discover_vehicle_sections(vehicle_name)
            if available.get("diagnostics"):
                sections.append({
                    "name": "Diagnostics",
                    "slug": "diagnostics",
                    "section_type": "diagnostics",
                    "description": "ECU diagnostics",
                    "sort_order": 1,
                    "icon": "",
                    "auto_run_programs": [],
                })
            if available.get("vehicle_health"):
                sections.append({
                    "name": "Vehicle Health Report",
                    "slug": "vehicle_health",
                    "section_type": "vehicle_health",
                    "description": "Health assessment",
                    "sort_order": 2,
                    "icon": "",
                    "auto_run_programs": [],
                })
        except Exception as e:
            _log_warn(f"Error in directory discovery: {e}")

    return sections


# =============================================================================
# UI LISTING API — ECUS
# =============================================================================

def list_ecus_for_vehicle(vehicle_name: str, user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """List ECUs for a vehicle with icon support."""
    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return []

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return []

    ecus: List[Dict[str, Any]] = []

    if DATABASE_AVAILABLE and query_all is not None:
        try:
            db_ecus = query_all("""
                SELECT
                    vda.ecu_code, vda.ecu_name, vda.description,
                    vda.protocol, vda.emission, vda.sort_order,
                    df.icon
                FROM app.vehicle_diagnostic_actions vda
                LEFT JOIN app.diagnostic_folders df
                       ON df.id = vda.folder_id
                WHERE vda.vehicle_id = :vid AND vda.is_active = TRUE
                ORDER BY vda.sort_order, vda.ecu_name
            """, {"vid": vehicle["id"]})

            for e in db_ecus or []:
                ecus.append({
                    "ecu_code": e["ecu_code"],
                    "ecu_name": e["ecu_name"],
                    "description": e.get("description", ""),
                    "protocol": e.get("protocol", ""),
                    "emission": e.get("emission", ""),
                    "icon": e.get("icon", ""),
                    "sort_order": e.get("sort_order", 0),
                })

            if ecus:
                return ecus

        except Exception as e:
            _log_error(f"Error loading ECUs from DB: {e}")

    try:
        discovered = discover_ecus(vehicle_name)
        for e in discovered:
            ecus.append({
                "ecu_code": e.get("ecu_code", ""),
                "ecu_name": e.get("ecu_name", ""),
                "description": e.get("description", ""),
                "protocol": e.get("protocol", ""),
                "emission": e.get("emission", ""),
                "icon": e.get("icon", ""),
                "sort_order": e.get("sort_order", 0),
            })
    except Exception as e:
        _log_warn(f"Error in ECU discovery: {e}")

    return ecus


# =============================================================================
# UI LISTING API — PARAMETERS
# =============================================================================

def list_parameters_for_ecu(vehicle_name: str, ecu_code: str, user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """List parameters for an ECU."""
    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return []

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return []

    params: List[Dict[str, Any]] = []

    if DATABASE_AVAILABLE and query_all is not None:
        try:
            folder = query_one("""
                SELECT df.id
                FROM app.diagnostic_folders df
                WHERE df.ecu_code = :ecu
            """, {"ecu": ecu_code})

            if folder:
                db_params = query_all("""
                    SELECT
                        da.parameter_code, da.label,
                        da.description, da.execution_class,
                        da.icon, da.sort_order
                    FROM app.diagnostic_actions da
                    WHERE da.folder_id = :fid
                      AND da.is_active = TRUE
                    ORDER BY da.sort_order, da.label
                """, {"fid": folder["id"]})

                for p in db_params or []:
                    params.append({
                        "parameter_code": p["parameter_code"],
                        "label": p["label"],
                        "description": p.get("description", ""),
                        "execution_class": p.get("execution_class", "SINGLE"),
                        "icon": p.get("icon", ""),
                        "sort_order": p.get("sort_order", 0),
                    })

                if params:
                    return params

        except Exception as e:
            _log_error(f"Error loading parameters from DB: {e}")

    try:
        discovered = discover_parameters(vehicle_name, ecu_code)
        for p in discovered:
            params.append({
                "parameter_code": p.get("parameter_code", ""),
                "label": p.get("label", ""),
                "description": p.get("description", ""),
                "execution_class": p.get("execution_class", "SINGLE"),
                "icon": p.get("icon", ""),
                "sort_order": p.get("sort_order", 0),
            })
    except Exception as e:
        _log_warn(f"Error in parameter discovery: {e}")

    return params


# =============================================================================
# UI LISTING API — HEALTH TABS
# =============================================================================

def list_health_tabs_for_vehicle(vehicle_name: str, user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """List health tabs for a vehicle with icon support."""
    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return []

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return []

    tabs: List[Dict[str, Any]] = []

    if DATABASE_AVAILABLE and query_all is not None:
        try:
            health_section = query_one("""
                SELECT id FROM app.vehicle_health_sections
                WHERE vehicle_id = :vid AND is_active = TRUE
            """, {"vid": vehicle["id"]})

            if health_section:
                db_tabs = query_all("""
                    SELECT
                        vhf.folder_code, vhf.folder_name,
                        vhf.description, vhf.execution_class,
                        vhf.icon, vhf.sort_order
                    FROM app.vehicle_health_folders vhf
                    WHERE vhf.section_id = :sid
                      AND vhf.is_active = TRUE
                    ORDER BY vhf.sort_order, vhf.folder_name
                """, {"sid": health_section["id"]})

                for t in db_tabs or []:
                    tabs.append({
                        "folder_code": t["folder_code"],
                        "folder_name": t["folder_name"],
                        "description": t.get("description", ""),
                        "execution_class": t.get("execution_class", "SINGLE"),
                        "icon": t.get("icon", ""),
                        "sort_order": t.get("sort_order", 0),
                    })

                if tabs:
                    return tabs

        except Exception as e:
            _log_error(f"Error loading health tabs from DB: {e}")

    try:
        discovered = discover_health_tabs(vehicle_name)
        for t in discovered:
            tabs.append({
                "folder_code": t.get("folder_code", ""),
                "folder_name": t.get("folder_name", ""),
                "description": t.get("description", ""),
                "execution_class": t.get("execution_class", "SINGLE"),
                "icon": t.get("icon", ""),
                "sort_order": t.get("sort_order", 0),
            })
    except Exception as e:
        _log_warn(f"Error in health tab discovery: {e}")

    return tabs


# =============================================================================
# UI LISTING API — TESTS
# =============================================================================

def list_tests_for_parameter(
    *,
    vehicle_name: str,
    section: str,
    parameter: str,
    ecu: Optional[str] = None,
    user_id: int,
    user_role: str,
) -> List[Dict[str, Any]]:
    """List tests for a specific parameter."""
    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return []

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return []

    tests: List[Dict[str, Any]] = []

    if DATABASE_AVAILABLE and query_all is not None:
        try:
            query = """
                SELECT
                    t.id, t.label, t.description,
                    t.module_name, t.function_name,
                    t.button_name, t.parameter_page_type,
                    t.parameter, t.function_role,
                    t.version, t.is_active, t.sort_order
                FROM app.tests t
                WHERE t.vehicle_id = :vid
                  AND t.section = :section
                  AND t.parameter = :param
                  AND t.is_active = TRUE
            """

            params_dict = {
                "vid": vehicle["id"],
                "section": section.lower(),
                "param": parameter,
            }

            if section.lower() == "diagnostics" and ecu:
                query += " AND t.ecu = :ecu"
                params_dict["ecu"] = ecu

            query += " ORDER BY t.sort_order, t.label"

            db_tests = query_all(query, params_dict)

            for t in db_tests or []:
                test_id = t["id"]
                inputs = _load_test_inputs_for_ui(test_id)
                limits = load_output_limits(test_id)
                exec_config = load_execution_config(test_id)

                tests.append({
                    "id": test_id,
                    "label": t["label"],
                    "description": t.get("description", ""),
                    "button_name": t.get("button_name", "Run"),
                    "parameter_page_type": t["parameter_page_type"],
                    "parameter": t["parameter"],
                    "function_role": t.get("function_role"),
                    "version": t.get("version", "1.0"),
                    "execution_mode": exec_config.get("execution_mode", "single"),
                    "supports_run_all": exec_config.get("supports_run_all", True),
                    "timeout_sec": exec_config.get("timeout_sec", DEFAULT_TIMEOUT_SEC),
                    "inputs": inputs,
                    "output_limits": limits,
                    "module_name": t.get("module_name"),
                    "function_name": t.get("function_name"),
                })

        except Exception as e:
            _log_error(f"Error loading tests from DB: {e}")
            _log_error(traceback.format_exc())

    return tests


def _load_test_inputs_for_ui(test_id: str) -> List[Dict[str, Any]]:
    """Load test inputs formatted for UI consumption."""
    if not DATABASE_AVAILABLE or query_all is None:
        return []

    try:
        inputs = query_all("""
            SELECT
                name, label, input_type,
                length, min_value, max_value,
                enum_values, default_value,
                format_hint, is_required, sort_order
            FROM app.test_inputs
            WHERE test_id = :tid
            ORDER BY sort_order, name
        """, {"tid": test_id})

        result: List[Dict[str, Any]] = []
        for inp in inputs or []:
            item = dict(inp)

            if item.get("enum_values") and isinstance(item["enum_values"], str):
                try:
                    item["enum_values"] = json.loads(item["enum_values"])
                except Exception:
                    pass

            result.append({
                "name": item["name"],
                "label": item.get("label"),
                "type": item["input_type"],
                "length": item.get("length"),
                "min": item.get("min_value"),
                "max": item.get("max_value"),
                "enum": item.get("enum_values"),
                "default": item.get("default_value"),
                "hint": item.get("format_hint"),
                "required": item.get("is_required", False),
            })

        return result

    except Exception as e:
        _log_error(f"Error loading inputs for test {test_id}: {e}")
        return []


# =============================================================================
# CONVENIENCE API FOR TESTS PAGE
# =============================================================================

def get_tests_page_context(
    *,
    vehicle_name: str,
    section: Optional[str] = None,
    ecu: Optional[str] = None,
    parameter: Optional[str] = None,
    user_id: int,
    user_role: str,
) -> Dict[str, Any]:
    """Build complete context for the tests page UI."""
    context: Dict[str, Any] = {
        "vehicle": None,
        "vehicle_sections": {"diagnostics": False, "vehicle_health": False},
        "sections": [],
        "ecus": [],
        "parameters": [],
        "health_tabs": [],
        "tests": [],
        "current": {"section": section, "ecu": ecu, "parameter": parameter},
        "auto_run": None,
    }

    vehicle = get_vehicle_by_name(vehicle_name)
    if not vehicle:
        return context

    if not check_vehicle_permission(user_id, vehicle["id"], user_role):
        return context

    context["vehicle"] = vehicle

    try:
        context["vehicle_sections"] = discover_vehicle_sections(vehicle_name)
    except Exception:
        pass

    context["sections"] = list_sections_for_vehicle(vehicle_name, user_id, user_role)

    if not section:
        return context

    try:
        auto_run_configs = get_auto_run_config(vehicle_name, section)
        if auto_run_configs:
            context["auto_run"] = {
                "programs": [
                    {
                        "program_id": p.get("program_id"),
                        "program_name": p.get("program_name"),
                        "program_type": p.get("program_type"),
                        "display_type": p.get("display_type"),
                        "display_label": p.get("display_label"),
                        "display_unit": p.get("display_unit"),
                        "display_pages": p.get("display_pages", []),
                        "fallback_action": p.get("fallback_action"),
                        "log_as_vin": p.get("log_as_vin", False),
                        "is_required": p.get("is_required", True),
                    }
                    for p in auto_run_configs
                ],
                "has_programs": True,
            }
    except Exception as e:
        _log_warn(f"Error loading auto-run config for context: {e}")

    if section.lower() == "diagnostics":
        context["ecus"] = list_ecus_for_vehicle(vehicle_name, user_id, user_role)

        if ecu:
            context["parameters"] = list_parameters_for_ecu(vehicle_name, ecu, user_id, user_role)

            if parameter:
                context["tests"] = list_tests_for_parameter(
                    vehicle_name=vehicle_name,
                    section=section,
                    parameter=parameter,
                    ecu=ecu,
                    user_id=user_id,
                    user_role=user_role,
                )

    elif section.lower() == "vehicle_health":
        context["health_tabs"] = list_health_tabs_for_vehicle(vehicle_name, user_id, user_role)

        if parameter:
            context["tests"] = list_tests_for_parameter(
                vehicle_name=vehicle_name,
                section=section,
                parameter=parameter,
                user_id=user_id,
                user_role=user_role,
            )

    return context


# =============================================================================
# DTC-SPECIFIC HELPERS
# =============================================================================

def get_dtc_tests(vehicle_name: str, ecu: str, user_id: int, user_role: str) -> Dict[str, Optional[Dict[str, Any]]]:
    """Get DTC READ/CLEAR test pair for an ECU."""
    result = {"read": None, "clear": None}

    tests = list_tests_for_parameter(
        vehicle_name=vehicle_name,
        section="diagnostics",
        parameter="DTC",
        ecu=ecu,
        user_id=user_id,
        user_role=user_role,
    )

    for test in tests:
        role = test.get("function_role")
        if role == "READ":
            result["read"] = test
        elif role == "CLEAR":
            result["clear"] = test

    return result


# =============================================================================
# IUPR-SPECIFIC HELPERS
# =============================================================================

def get_iupr_tests(vehicle_name: str, ecu: str, mode: str, user_id: int, user_role: str) -> List[Dict[str, Any]]:
    """Get IUPR tests for an ECU."""
    parameter = f"IUPR_{mode.upper()}"
    return list_tests_for_parameter(
        vehicle_name=vehicle_name,
        section="diagnostics",
        parameter=parameter,
        ecu=ecu,
        user_id=user_id,
        user_role=user_role,
    )


# =============================================================================
# EXECUTION HISTORY
# =============================================================================

def get_test_execution_history(test_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Get execution history for a test."""
    if not DATABASE_AVAILABLE or query_all is None:
        return []

    try:
        results = query_all("""
            SELECT
                ter.id, ter.task_id, ter.pass, ter.exception,
                ter.output_json, ter.limit_violations,
                ter.duration_ms, ter.attempts, ter.created_at,
                u.name AS user_name
            FROM app.test_execution_results ter
            LEFT JOIN app.users u ON u.id = ter.user_id
            WHERE ter.test_id = :tid
            ORDER BY ter.created_at DESC
            LIMIT :limit
        """, {"tid": test_id, "limit": limit})

        history: List[Dict[str, Any]] = []
        for r in results or []:
            item = dict(r)

            if item.get("output_json"):
                if isinstance(item["output_json"], str):
                    try:
                        item["output"] = json.loads(item["output_json"])
                    except Exception:
                        item["output"] = item["output_json"]
                else:
                    item["output"] = item["output_json"]

            if item.get("limit_violations"):
                if isinstance(item["limit_violations"], str):
                    try:
                        item["limit_violations"] = json.loads(item["limit_violations"])
                    except Exception:
                        pass

            history.append(item)

        return history

    except Exception as e:
        _log_error(f"Error loading execution history for {test_id}: {e}")
        return []


def get_vehicle_execution_summary(vehicle_id: int, days: int = 7) -> Dict[str, Any]:
    """Get execution summary for a vehicle."""
    if not DATABASE_AVAILABLE or query_one is None:
        return {}

    days = max(1, min(365, int(days)))

    try:
        summary = query_one("""
            SELECT
                COUNT(*) AS total_executions,
                COUNT(*) FILTER (WHERE pass = TRUE) AS passed,
                COUNT(*) FILTER (WHERE pass = FALSE) AS failed,
                AVG(duration_ms) AS avg_duration_ms
            FROM app.test_execution_results
            WHERE vehicle_id = :vid
              AND created_at >= CURRENT_TIMESTAMP - CAST(:days || ' days' AS INTERVAL)
        """, {"vid": vehicle_id, "days": days})

        return dict(summary) if summary else {}

    except Exception as e:
        _log_error(f"Error loading execution summary: {e}")
        return {}


# =============================================================================
# RUN ALL TESTS
# =============================================================================

def run_all_tests_for_parameter(
    *,
    vehicle_name: str,
    section: str,
    parameter: str,
    ecu: Optional[str] = None,
    user_id: int,
    user_role: str,
    sequential: bool = True,
    stop_on_failure: bool = False,
) -> Dict[str, Any]:
    """
    Run all tests for a parameter using runner's batch execution.

    Filters out tests where supports_run_all is False.
    """
    tests = list_tests_for_parameter(
        vehicle_name=vehicle_name,
        section=section,
        parameter=parameter,
        ecu=ecu,
        user_id=user_id,
        user_role=user_role,
    )

    runnable = [t for t in tests if t.get("supports_run_all", True)]
    if not runnable:
        return {"ok": True, "tests": [], "message": "No runnable tests"}

    batch_specs = []
    veh = get_vehicle_by_name(vehicle_name)
    veh_id = veh["id"] if veh else 0

    for test in runnable:
        try:
            test_fn = load_test_function(
                vehicle_name,
                section=section,
                ecu=ecu,
                parameter=parameter,
                module_name=test.get("module_name", ""),
                function_name=test.get("function_name", ""),
            )

            can_config = get_can_configuration()
            args = [can_config["can_interface"], can_config["bitrate"]]

            exec_config = load_execution_config(test["id"])
            output_limits = load_output_limits(test["id"])

            persist_fn = create_result_persister(test["id"], user_id, veh_id)

            ctx = ExecutionContext(
                test_id=test["id"],
                execution_mode=exec_config.get("execution_mode", "single"),
                timeout_sec=exec_config.get("timeout_sec", DEFAULT_TIMEOUT_SEC),
                max_retries=exec_config.get("max_retries", DEFAULT_MAX_RETRIES),
                retry_delay_sec=exec_config.get("retry_delay_sec", DEFAULT_RETRY_DELAY_SEC),
                output_limits=output_limits,
                persist_result=persist_fn,
                retry_on_timeout=exec_config.get("retry_on_timeout", False),
                retry_on_exception=exec_config.get("retry_on_exception", False),
                supports_run_all=True,
                metadata={
                    "vehicle_name": vehicle_name,
                    "section": section,
                    "ecu": ecu,
                    "parameter": parameter,
                },
            )

            batch_specs.append({"fn": test_fn, "args": args, "ctx": ctx})

        except Exception as e:
            _log_error(f"Error preparing test {test.get('id', 'unknown')}: {e}")

    if not batch_specs:
        return {"ok": False, "error": "No tests could be prepared"}

    try:
        batch_result = execute_batch_async(
            tests=batch_specs,
            sequential=sequential,
            stop_on_failure=stop_on_failure,
            delay_between_tests_sec=0.5,
            progress_cb=_progress_callback,
        )

        return {"ok": True, "batch_id": batch_result.get("batch_id"), "total": len(batch_specs)}

    except Exception as e:
        _log_error(f"Batch execution failed: {e}")
        return {"ok": False, "error": str(e)}


# =============================================================================
# HOUSEKEEPING
# =============================================================================

def service_cleanup() -> Dict[str, int]:
    """Run periodic cleanup tasks."""
    tasks_removed = purge_completed_tasks()
    sessions_removed = cleanup_auto_run_sessions()

    _log_info(f"Cleanup: {tasks_removed} tasks, {sessions_removed} sessions removed")
    return {"tasks_removed": tasks_removed, "sessions_removed": sessions_removed}


def get_service_stats() -> Dict[str, Any]:
    """Get service-level statistics."""
    stats = get_runner_stats()
    stats["database_available"] = DATABASE_AVAILABLE
    stats["can_utils_available"] = CAN_UTILS_AVAILABLE
    return stats


# =============================================================================
# INITIALIZATION
# =============================================================================

def init_service():
    _log_info("Diagnostic service v4.3.0 initialized")

    if not DATABASE_AVAILABLE:
        _log_warn("Database not available — limited functionality")

    if not CAN_UTILS_AVAILABLE:
        _log_warn("CAN utils not available — using default configuration")


init_service()


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Exceptions
    "PermissionDeniedError",
    "TestNotFoundError",
    "ValidationError",
    "ServiceExecutionError",
    "AutoRunError",

    # Main execution
    "run_test",
    "run_all_tests_for_parameter",

    # Auto-run (new full API)
    "start_auto_run",
    "get_auto_run_status",
    "submit_auto_run_vin",
    "stop_auto_run",

    # Auto-run (legacy compat)
    "run_auto_programs",

    # Listing API
    "get_vehicle_by_name",
    "get_vehicle_by_id",
    "list_sections_for_vehicle",
    "list_ecus_for_vehicle",
    "list_parameters_for_ecu",
    "list_health_tabs_for_vehicle",
    "list_tests_for_parameter",
    "get_tests_page_context",

    # Specific helpers
    "get_dtc_tests",
    "get_iupr_tests",

    # History
    "get_test_execution_history",
    "get_vehicle_execution_summary",

    # Task management
    "get_task_status",
    "cancel_task",
    "pause_task",
    "resume_task",
    "get_task_logs",
    "get_task_log_lines",
    "get_new_task_logs",
    "clear_task_logs",

    # Progress callback
    "register_progress_callback",
    "unregister_progress_callback",

    # Permissions
    "check_vehicle_permission",
    "check_test_permission",

    # Metadata helpers
    "load_test_metadata",
    "load_execution_config",
    "load_output_limits",
    "get_can_configuration",

    # Housekeeping
    "service_cleanup",
    "get_service_stats",
]