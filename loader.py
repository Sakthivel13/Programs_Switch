# -*- coding: utf-8 -*-
"""
NIRIX Diagnostic System - Test Definition Loader
Supports corrected JSON schemas with database synchronization
and auto-run program configuration.

Searches for section_tests.json and ecu_tests.json at:
  1. <Vehicle>/                     (canonical)
  2. <Vehicle>/Diagnostics/         (fallback)
  3. <Vehicle>/ECUs/                (fallback)

Version: 2.1.0
Last Updated: 2026-02-15

FIXES IN v2.1.0
────────────────
- FIX-40: Corrected table references in sync functions
- FIX-41: Added missing fields for vehicle_health_folders
- FIX-42: Added index creation for performance
- FIX-43: Fixed vehicle_health_sections table references
- FIX-44: Removed references to non-existent tables
"""

from __future__ import annotations

import os
import sys
import json
import hashlib
import importlib.util
from functools import lru_cache
from typing import Dict, List, Callable, Optional, Any, Tuple


# =============================================================================
# EXCEPTIONS (EXPORTED FOR WEB APP COMPATIBILITY)
# =============================================================================

class VehicleNotFoundError(FileNotFoundError):
    """Raised when a vehicle's Test_Programs root or its JSON files are not found."""
    pass


class ModuleLoadError(ImportError):
    """Raised when a module file cannot be imported."""
    pass


class FunctionNotFoundError(AttributeError):
    """Raised when a function is not found or not callable in the imported module."""
    pass


# =============================================================================
# OPTIONAL DEPENDENCIES
# =============================================================================

try:
    from jsonschema import Draft202012Validator, ValidationError  # type: ignore
except ImportError:
    Draft202012Validator = None
    ValidationError = Exception

try:
    from database import query_one, query_all, execute
except ImportError:
    query_one = query_all = execute = None


# =============================================================================
# GLOBAL FLAGS & PATHS
# =============================================================================

CI_MODE = os.getenv("NIRIX_CI", "false").lower() == "true"
DEBUG_MODE = os.getenv("NIRIX_DEBUG", "false").lower() == "true"

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
TEST_PROGRAMS_DIR = os.path.join(BASE_DIR, "Test_Programs")
SCHEMA_DIR = os.path.join(BASE_DIR, "schema")

SECTION_TESTS_SCHEMA_PATH = os.path.join(SCHEMA_DIR, "section.schema.json")
ECU_TESTS_SCHEMA_PATH     = os.path.join(SCHEMA_DIR, "ecu.schema.json")
TESTS_SCHEMA_PATH         = os.path.join(SCHEMA_DIR, "tests.schema.json")

SECTION_TESTS_FILENAME = "section_tests.json"
ECU_TESTS_FILENAME     = "ecu_tests.json"
TESTS_FILENAME         = "tests.json"

if TEST_PROGRAMS_DIR not in sys.path:
    sys.path.insert(0, TEST_PROGRAMS_DIR)


# =============================================================================
# LOGGING HELPER
# =============================================================================

def _log(message: str, level: str = "INFO"):
    """Simple logging helper."""
    if DEBUG_MODE or level in ("ERROR", "WARN"):
        print(f"[LOADER][{level}] {message}")


# =============================================================================
# NAME NORMALIZATION & SAFETY
# =============================================================================

def normalize(name: str) -> str:
    """Normalize a name for comparison."""
    return "".join(
        c for c in (name or "").strip()
        if c.isalnum() or c in (" ", "_", "-")
    ).replace(" ", "_").lower()


def safe_name(name: str) -> str:
    """Validate name is safe for filesystem/path usage."""
    if (
        not name
        or "/" in name
        or "\\" in name
        or ".." in name
        or str(name).startswith(".")
        or ":" in str(name)
    ):
        raise ValueError(f"Illegal name: {name}")
    return name


def sanitize_for_db(value: Any, max_length: int = 1000) -> Optional[str]:
    """Sanitize a value for database insertion."""
    if value is None:
        return None
    s = str(value)
    return s[:max_length] if len(s) > max_length else s


# =============================================================================
# FILE FINGERPRINTING
# =============================================================================

def _file_fingerprint(path: str) -> str:
    """Generate a fingerprint for a file based on mtime and size."""
    st = os.stat(path)
    sig = f"{st.st_mtime_ns}:{st.st_size}".encode()
    return hashlib.sha1(sig).hexdigest()


# =============================================================================
# VEHICLE ROOT DISCOVERY
# =============================================================================

def get_vehicle_root(vehicle: str) -> str:
    """Get the filesystem root directory for a vehicle."""
    target = normalize(vehicle)

    if not os.path.isdir(TEST_PROGRAMS_DIR):
        raise FileNotFoundError(
            f"Test programs directory not found: {TEST_PROGRAMS_DIR}"
        )

    for d in os.listdir(TEST_PROGRAMS_DIR):
        full = os.path.join(TEST_PROGRAMS_DIR, d)
        if os.path.isdir(full) and normalize(d) == target:
            return full

    raise FileNotFoundError(f"Vehicle not found: {vehicle}")


def list_all_vehicles() -> List[str]:
    """List all vehicle directories in Test_Programs."""
    if not os.path.isdir(TEST_PROGRAMS_DIR):
        return []

    return [
        d for d in os.listdir(TEST_PROGRAMS_DIR)
        if os.path.isdir(os.path.join(TEST_PROGRAMS_DIR, d))
        and not d.startswith(".")
        and not d.startswith("_")
    ]


# =============================================================================
# SCHEMA LOADING & VALIDATION
# =============================================================================

_SCHEMA_CACHE: Dict[str, Dict] = {}
_VALIDATOR_CACHE: Dict[str, Any] = {}


def _load_schema(path: str) -> Dict:
    """Load a JSON schema from file with caching."""
    if path not in _SCHEMA_CACHE:
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Schema file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            _SCHEMA_CACHE[path] = json.load(f)
    return _SCHEMA_CACHE[path]


def _get_validator(schema_path: str):
    """Get a cached validator for a schema."""
    if Draft202012Validator is None:
        return None
    if schema_path not in _VALIDATOR_CACHE:
        schema = _load_schema(schema_path)
        _VALIDATOR_CACHE[schema_path] = Draft202012Validator(schema)
    return _VALIDATOR_CACHE[schema_path]


def validate_json(
    payload: Dict,
    schema_path: str,
    strict: bool = False,
) -> Tuple[bool, List[str]]:
    """Validate a JSON payload against a schema."""
    if Draft202012Validator is None:
        _log("jsonschema not installed, skipping validation", "WARN")
        return True, []

    if not os.path.isfile(schema_path):
        _log(f"Schema file not found: {schema_path}", "WARN")
        return True, []

    validator = _get_validator(schema_path)
    if validator is None:
        return True, []

    errors = list(validator.iter_errors(payload))
    if not errors:
        return True, []

    error_messages = []
    for e in errors[:10]:
        path = "/".join(map(str, e.path)) if e.path else "root"
        error_messages.append(f"{path}: {e.message}")

    if strict or CI_MODE:
        error_str = "; ".join(error_messages)
        raise ValueError(f"Schema validation failed: {error_str}")

    return False, error_messages


# =============================================================================
# JSON FILE LOADERS
# =============================================================================

def load_json_file(
    path: str,
    schema_path: Optional[str] = None,
    strict: bool = False,
) -> Dict:
    """Load and optionally validate a JSON file."""
    if not os.path.isfile(path):
        raise FileNotFoundError(f"JSON file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if schema_path:
        is_valid, errors = validate_json(data, schema_path, strict)
        if not is_valid:
            data["_schema_warnings"] = errors
            _log(f"Schema warnings for {path}: {errors}", "WARN")

    return data


def load_json_file_safe(
    path: str,
    schema_path: Optional[str] = None,
) -> Optional[Dict]:
    """Load JSON file, returning None if not found or invalid."""
    try:
        return load_json_file(path, schema_path, strict=False)
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as e:
        _log(f"Invalid JSON in {path}: {e}", "ERROR")
        return None
    except Exception as e:
        _log(f"Error loading {path}: {e}", "ERROR")
        return None


# =============================================================================
# FILE DISCOVERY HELPERS (FALLBACK LOOKUP)
# =============================================================================

def _find_first_existing(paths: List[str]) -> Optional[str]:
    """Return the first existing file path from candidates."""
    for p in paths:
        if os.path.isfile(p):
            return p
    return None


def _get_fallback_candidates(root: str, filename: str) -> List[str]:
    """
    Return canonical + fallback candidate paths for a config file.

    Search order:
      1. <root>/<filename>               (canonical)
      2. <root>/Diagnostics/<filename>    (fallback)
      3. <root>/ECUs/<filename>           (fallback)
    """
    return [
        os.path.join(root, filename),
        os.path.join(root, "Diagnostics", filename),
        os.path.join(root, "ECUs", filename),
    ]


# =============================================================================
# VEHICLE JSON FILE DISCOVERY
# =============================================================================

def discover_vehicle_json_files(vehicle: str) -> Dict[str, Any]:
    """
    Discover all JSON configuration files for a vehicle.

    Scans the vehicle root directory for:
    - section_tests.json (canonical + fallback paths)
    - ecu_tests.json (canonical + fallback paths)
    - All tests.json files (in Diagnostics/*/ and Vehicle_Health_Report/*/)

    Returns:
        Dict with file paths and existence/validity status
    """
    try:
        root = get_vehicle_root(vehicle)
    except FileNotFoundError:
        return {"error": f"Vehicle not found: {vehicle}", "files": {}}

    result: Dict[str, Any] = {
        "vehicle": vehicle,
        "root": root,
        "files": {
            "section_tests": {
                "path": None,
                "exists": False,
                "valid": None,
            },
            "ecu_tests": {
                "path": None,
                "exists": False,
                "valid": None,
            },
            "tests": [],
        },
    }

    # Check section_tests.json (canonical + fallbacks)
    section_candidates = _get_fallback_candidates(root, SECTION_TESTS_FILENAME)
    section_path = _find_first_existing(section_candidates)
    if section_path:
        result["files"]["section_tests"]["path"] = section_path
        result["files"]["section_tests"]["exists"] = True
        canonical = os.path.join(root, SECTION_TESTS_FILENAME)
        if section_path != canonical:
            result["files"]["section_tests"]["fallback"] = True
        try:
            load_json_file(section_path, SECTION_TESTS_SCHEMA_PATH, strict=True)
            result["files"]["section_tests"]["valid"] = True
        except Exception as e:
            result["files"]["section_tests"]["valid"] = False
            result["files"]["section_tests"]["error"] = str(e)

    # Check ecu_tests.json (canonical + fallbacks)
    ecu_candidates = _get_fallback_candidates(root, ECU_TESTS_FILENAME)
    ecu_path = _find_first_existing(ecu_candidates)
    if ecu_path:
        result["files"]["ecu_tests"]["path"] = ecu_path
        result["files"]["ecu_tests"]["exists"] = True
        canonical = os.path.join(root, ECU_TESTS_FILENAME)
        if ecu_path != canonical:
            result["files"]["ecu_tests"]["fallback"] = True
        try:
            load_json_file(ecu_path, ECU_TESTS_SCHEMA_PATH, strict=True)
            result["files"]["ecu_tests"]["valid"] = True
        except Exception as e:
            result["files"]["ecu_tests"]["valid"] = False
            result["files"]["ecu_tests"]["error"] = str(e)

    # Find all tests.json files
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if filename == TESTS_FILENAME:
                full_path = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(full_path, root)
                entry: Dict[str, Any] = {
                    "path": full_path,
                    "relative": rel_path,
                    "exists": True,
                    "valid": None,
                }
                try:
                    load_json_file(full_path, TESTS_SCHEMA_PATH, strict=True)
                    entry["valid"] = True
                except Exception as e:
                    entry["valid"] = False
                    entry["error"] = str(e)
                result["files"]["tests"].append(entry)

    return result


# =============================================================================
# SECTION TESTS JSON LOADER
# =============================================================================

def load_section_tests(vehicle: str) -> Optional[Dict]:
    """Load section_tests.json for a vehicle with fallback paths."""
    root = get_vehicle_root(vehicle)

    candidates = _get_fallback_candidates(root, SECTION_TESTS_FILENAME)
    path = _find_first_existing(candidates)
    if not path:
        return None

    canonical = os.path.join(root, SECTION_TESTS_FILENAME)
    if path != canonical:
        _log(f"Using non-root section_tests.json at: {path}", "WARN")

    return load_json_file_safe(path, SECTION_TESTS_SCHEMA_PATH)


def get_section_details(vehicle: str) -> List[Dict[str, Any]]:
    """Get full section details from section_tests.json."""
    data = load_section_tests(vehicle)
    if not data or "sections" not in data:
        return []

    return [
        {
            "name": s.get("name", ""),
            "slug": s.get("slug", ""),
            "section_type": s.get("section_type", ""),
            "description": s.get("description", ""),
            "icon": s.get("icon", ""),
            "sort_order": s.get("sort_order", 0),
            "auto_run_programs": s.get("auto_run_programs", []),
            "is_active": s.get("is_active", True),
            "ecus": s.get("ecus", []),
            "health_tabs": s.get("health_tabs", []),
        }
        for s in data["sections"]
    ]


# =============================================================================
# ECU TESTS JSON LOADER
# =============================================================================

def load_ecu_tests(vehicle: str) -> Optional[Dict]:
    """Load ecu_tests.json for a vehicle with fallback paths."""
    root = get_vehicle_root(vehicle)

    candidates = _get_fallback_candidates(root, ECU_TESTS_FILENAME)
    path = _find_first_existing(candidates)
    if not path:
        return None

    canonical = os.path.join(root, ECU_TESTS_FILENAME)
    if path != canonical:
        _log(f"Using non-root ecu_tests.json at: {path}", "WARN")

    return load_json_file_safe(path, ECU_TESTS_SCHEMA_PATH)


def get_ecu_details(
    vehicle: str,
    ecu_code: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get ECU details from ecu_tests.json."""
    data = load_ecu_tests(vehicle)
    if not data or "ecus" not in data:
        return []

    ecus = []
    for ecu in data["ecus"]:
        if ecu_code and ecu.get("ecu_code") != ecu_code:
            continue
        ecus.append({
            "ecu_code": ecu.get("ecu_code", ""),
            "ecu_name": ecu.get("ecu_name", ""),
            "description": ecu.get("description", ""),
            "protocol": ecu.get("protocol", ""),
            "emission": ecu.get("emission", ""),
            "icon": ecu.get("icon", ""),
            "is_active": ecu.get("is_active", True),
            "sort_order": ecu.get("sort_order", 0),
            "parameters": ecu.get("parameters", []),
        })
    return ecus


def get_parameter_details(vehicle: str, ecu_code: str) -> List[Dict[str, Any]]:
    """Get parameter details for an ECU from ecu_tests.json."""
    ecus = get_ecu_details(vehicle, ecu_code)
    if not ecus:
        return []

    ecu = ecus[0]
    return [
        {
            "parameter_code": p.get("parameter_code", ""),
            "label": p.get("label", ""),
            "description": p.get("description", ""),
            "icon": p.get("icon", ""),
            "execution_class": p.get("execution_class", "SINGLE"),
            "is_active": p.get("is_active", True),
            "sort_order": p.get("sort_order", 0),
        }
        for p in ecu.get("parameters", [])
    ]


# =============================================================================
# TESTS JSON LOADER
# =============================================================================

def load_tests_json(
    vehicle: str,
    section: str,
    parameter: str,
    ecu: Optional[str] = None,
) -> Optional[Dict]:
    """Load tests.json for a specific parameter."""
    root = get_vehicle_root(vehicle)

    if section == "diagnostics":
        if not ecu:
            raise ValueError("ecu required for diagnostics section")
        base_dir = os.path.join(
            root, "Diagnostics", safe_name(ecu), safe_name(parameter)
        )
    elif section == "vehicle_health":
        base_dir = os.path.join(
            root, "Vehicle_Health_Report", safe_name(parameter)
        )
    else:
        raise ValueError(f"Invalid section: {section}")

    tests_file = os.path.join(base_dir, TESTS_FILENAME)
    return load_json_file_safe(tests_file, TESTS_SCHEMA_PATH)


def get_tests_for_parameter(
    vehicle: str,
    section: str,
    parameter: str,
    ecu: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Get test definitions for a parameter."""
    data = load_tests_json(vehicle, section, parameter, ecu)
    if not data or "tests" not in data:
        return []
    return data["tests"]


# =============================================================================
# DISCOVERY FUNCTIONS (FOR UI/API)
# =============================================================================

def discover_vehicle_sections(vehicle: str) -> Dict[str, bool]:
    """Discover available sections for a vehicle."""
    result = {"diagnostics": False, "vehicle_health": False}

    try:
        data = load_section_tests(vehicle)
        if data and "sections" in data:
            for section in data["sections"]:
                if section.get("is_active", True):
                    st = (section.get("section_type", "") or "").lower()
                    if st in result:
                        result[st] = True
            if any(result.values()):
                return result

        root = get_vehicle_root(vehicle)
        result["diagnostics"] = os.path.isdir(
            os.path.join(root, "Diagnostics")
        )
        result["vehicle_health"] = os.path.isdir(
            os.path.join(root, "Vehicle_Health_Report")
        )
    except FileNotFoundError:
        pass

    return result


def discover_ecus(vehicle: str) -> List[Dict[str, Any]]:
    """Discover ECUs for a vehicle."""
    try:
        ecus = get_ecu_details(vehicle)
        if ecus:
            return [e for e in ecus if e.get("is_active", True)]

        root = get_vehicle_root(vehicle)
        diag_dir = os.path.join(root, "Diagnostics")
        if not os.path.isdir(diag_dir):
            return []

        return [
            {
                "ecu_code": d,
                "ecu_name": d.replace("_", " "),
                "description": "",
                "protocol": "",
                "emission": "",
                "is_active": True,
                "sort_order": 0,
            }
            for d in sorted(os.listdir(diag_dir))
            if os.path.isdir(os.path.join(diag_dir, d))
            and not d.startswith(".")
        ]
    except FileNotFoundError:
        return []


def discover_parameters(vehicle: str, ecu_code: str) -> List[Dict[str, Any]]:
    """Discover parameters for an ECU."""
    try:
        params = get_parameter_details(vehicle, ecu_code)
        if params:
            return [p for p in params if p.get("is_active", True)]

        root = get_vehicle_root(vehicle)
        ecu_dir = os.path.join(root, "Diagnostics", safe_name(ecu_code))
        if not os.path.isdir(ecu_dir):
            return []

        return [
            {
                "parameter_code": d,
                "label": d.replace("_", " "),
                "description": "",
                "execution_class": "SINGLE",
                "is_active": True,
                "sort_order": 0,
            }
            for d in sorted(os.listdir(ecu_dir))
            if os.path.isdir(os.path.join(ecu_dir, d))
            and not d.startswith(".")
        ]
    except FileNotFoundError:
        return []


def discover_health_tabs(vehicle: str) -> List[Dict[str, Any]]:
    """Discover health tabs for a vehicle."""
    try:
        data = load_section_tests(vehicle)
        if data and "sections" in data:
            for section in data["sections"]:
                st = (section.get("section_type") or "").lower()
                if st == "vehicle_health" and section.get("is_active", True):
                    tabs = section.get("health_tabs", [])
                    return [
                        {
                            "folder_code": tab.get("folder_code", ""),
                            "folder_name": tab.get("folder_name", ""),
                            "description": tab.get("description", ""),
                            "execution_class": tab.get(
                                "execution_class", "SINGLE"
                            ),
                            "icon": tab.get("icon", ""),
                            "is_active": tab.get("is_active", True),
                            "sort_order": tab.get("sort_order", 0),
                        }
                        for tab in tabs
                        if tab.get("is_active", True)
                    ]

        root = get_vehicle_root(vehicle)
        vh_dir = os.path.join(root, "Vehicle_Health_Report")
        if not os.path.isdir(vh_dir):
            return []

        return [
            {
                "folder_code": d,
                "folder_name": d.replace("_", " "),
                "description": "",
                "execution_class": _infer_execution_class(d),
                "is_active": True,
                "sort_order": 0,
            }
            for d in sorted(os.listdir(vh_dir))
            if os.path.isdir(os.path.join(vh_dir, d))
            and not d.startswith(".")
        ]
    except FileNotFoundError:
        return []


def _infer_execution_class(folder_name: str) -> str:
    """Infer execution class from folder name."""
    if "summary" in folder_name.lower():
        return "STREAM"
    return "SINGLE"


# =============================================================================
# MODULE IMPORT (SAFE, CACHED)
# =============================================================================

def _import_module(path: str, unique_name: str):
    """Import a Python module from a file path."""
    spec = importlib.util.spec_from_file_location(unique_name, path)
    if spec is None or spec.loader is None:
        raise ModuleLoadError(f"Cannot load module from: {path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[unique_name] = mod
    try:
        spec.loader.exec_module(mod)
    except Exception as e:
        # Clean up sys.modules on failure to avoid partial entries
        sys.modules.pop(unique_name, None)
        raise ModuleLoadError(f"Failed to execute module {path}: {e}") from e
    return mod


@lru_cache(maxsize=512)
def _load_cached_function(
    module_path: str,
    fn_name: str,
    fingerprint: str,
) -> Callable:
    """Load and cache a function from a module."""
    unique_name = (
        f"diag_{hashlib.md5(module_path.encode()).hexdigest()}"
        f"_{fingerprint[:8]}"
    )
    mod = _import_module(module_path, unique_name)

    fn = getattr(mod, fn_name, None)
    if not callable(fn):
        # Case-insensitive fallback lookup
        fn_lower = fn_name.lower()
        for attr_name in dir(mod):
            if attr_name.lower() == fn_lower:
                candidate = getattr(mod, attr_name)
                if callable(candidate):
                    fn = candidate
                    break

    if not callable(fn):
        raise FunctionNotFoundError(
            f"Function '{fn_name}' not found or not callable in {module_path}"
        )
    return fn


def clear_function_cache():
    """Clear the function cache."""
    _load_cached_function.cache_clear()


# =============================================================================
# PUBLIC API — ARBITRARY MODULE LOADING
# =============================================================================

def load_function_from_path(
    module_path: str,
    function_name: str,
) -> Callable:
    """
    Load a Python function from an arbitrary module path.

    This is a public API wrapper around the cached function loader,
    useful for loading auto-run programs or custom test functions
    from non-standard locations.

    Args:
        module_path: Full filesystem path to the .py module file
        function_name: Name of the function to load from the module

    Returns:
        Callable function object

    Raises:
        ModuleLoadError: If the module cannot be imported
        FunctionNotFoundError: If the function doesn't exist or isn't callable
        FileNotFoundError: If the module file doesn't exist

    Examples:
        >>> fn = load_function_from_path(
        ...     "/path/to/Test_Programs/MY_VEHICLE/Auto_Run/vin_read.py",
        ...     "read_vin"
        ... )
        >>> result = fn("PCAN_USBBUS1", 500000)
    """
    if not os.path.isfile(module_path):
        raise FileNotFoundError(f"Module file not found: {module_path}")

    fingerprint = _file_fingerprint(module_path)
    return _load_cached_function(module_path, function_name, fingerprint)


# =============================================================================
# RUNTIME API (USED BY service.py)
# =============================================================================

def load_test_function(
    vehicle: str,
    *,
    section: str,
    parameter: str,
    module_name: str,
    function_name: str,
    ecu: Optional[str] = None,
) -> Callable:
    """Load a test function from the filesystem."""
    try:
        root = get_vehicle_root(vehicle)
    except FileNotFoundError as e:
        raise VehicleNotFoundError(str(e)) from e

    sec = (section or "").lower().strip()

    if sec == "diagnostics":
        if not ecu:
            raise ValueError("ECU required for diagnostics section")
        base = os.path.join(
            root, "Diagnostics", safe_name(ecu), safe_name(parameter)
        )
    elif sec == "vehicle_health":
        base = os.path.join(
            root, "Vehicle_Health_Report", safe_name(parameter)
        )
    else:
        raise ValueError(f"Invalid section: {section}")

    module_path = os.path.join(base, f"{safe_name(module_name)}.py")
    if not os.path.isfile(module_path):
        raise ModuleLoadError(f"Module not found: {module_path}")

    fingerprint = _file_fingerprint(module_path)
    return _load_cached_function(module_path, function_name, fingerprint)


# =============================================================================
# VERSIONING & HASHING
# =============================================================================

def compute_content_hash(payload: Dict) -> str:
    """Compute SHA-256 hash of JSON payload."""
    blob = json.dumps(
        payload, sort_keys=True, separators=(",", ":")
    ).encode()
    return hashlib.sha256(blob).hexdigest()


def register_definition_version(
    vehicle_id: int,
    payload: Dict,
    source: str,
) -> bool:
    """Register a test definition version in the database."""
    if execute is None:
        _log("Database not available, skipping version registration", "WARN")
        return False

    try:
        tests_hash = compute_content_hash(payload)
        schema_version = payload.get("schema_version", "1.0")

        execute("""
            INSERT INTO app.test_definition_versions
            (vehicle_id, schema_version, tests_hash, source,
             is_active, created_at)
            VALUES (:vid, :sv, :hash, :src, TRUE, CURRENT_TIMESTAMP)
            ON CONFLICT (vehicle_id, tests_hash) DO UPDATE SET
                is_active  = TRUE,
                created_at = CURRENT_TIMESTAMP
        """, {
            "vid": vehicle_id,
            "sv": schema_version,
            "hash": tests_hash,
            "src": source,
        })
        return True
    except Exception as e:
        _log(f"Failed to register definition version: {e}", "ERROR")
        return False


# =============================================================================
# TYPE MAPPINGS
# =============================================================================

def _map_execution_mode(mode: str) -> str:
    """Map execution mode from JSON to SQL enum values."""
    mode_lower = (mode or "single").lower().strip()
    mapping = {
        "stream": "stream",
        "single": "single",
        "flash": "flashing",
        "flashing": "flashing",
        "live": "live",
    }
    return mapping.get(mode_lower, "single")


def _map_execution_class_to_mode(exec_class: str) -> str:
    """Map execution_class to execution_mode."""
    mapping = {
        "STREAM": "stream",
        "SINGLE": "single",
        "FLASH": "flashing",
        "DUAL": "stream",
    }
    return mapping.get(exec_class, "single")


# =============================================================================
# JSONB HELPERS
# =============================================================================

def _to_jsonb(value: Any) -> Optional[str]:
    """
    Convert a value to a JSON string suitable for PostgreSQL jsonb casting.

    - None               -> None  (SQL NULL)
    - []                 -> None  (treat empty list as NULL)
    - [items...]         -> '["item1","item2"]'
    - {}                 -> None  (treat empty dict as NULL)
    - {key:val}          -> '{"key":"value"}'
    - str (valid JSON)   -> kept as-is after validation
    - str (plain scalar) -> '"value"' (valid JSON scalar)
    - other scalars      -> json.dumps(value)
    """
    if value is None:
        return None

    if isinstance(value, (list, dict)):
        if not value:
            return None
        return json.dumps(value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        # Check if it's already valid JSON
        if stripped.startswith("[") or stripped.startswith("{"):
            try:
                json.loads(stripped)
                return stripped
            except json.JSONDecodeError:
                return json.dumps(stripped)
        # Plain string scalar — wrap as JSON string
        return json.dumps(stripped)

    # Numeric, bool, etc.
    return json.dumps(value)


def _to_jsonb_or_empty_array(value: Any) -> str:
    """
    Convert value to jsonb string, using '[]' as default instead of NULL.
    Used for fields that should always be a valid JSON array.
    """
    result = _to_jsonb(value)
    return result if result else "[]"


# =============================================================================
# FIX-42: DATABASE INDEX CREATION
# =============================================================================

def _ensure_indexes():
    """Create necessary indexes for performance if they don't exist."""
    if execute is None:
        return
    
    try:
        # Indexes for test tables
        execute("""
            CREATE INDEX IF NOT EXISTS idx_tests_vehicle_id 
            ON app.tests(vehicle_id)
        """)
        execute("""
            CREATE INDEX IF NOT EXISTS idx_tests_section_parameter 
            ON app.tests(section, parameter)
        """)
        
        # Indexes for test_inputs
        execute("""
            CREATE INDEX IF NOT EXISTS idx_test_inputs_test_id 
            ON app.test_inputs(test_id)
        """)
        
        # Indexes for test_execution_config
        execute("""
            CREATE INDEX IF NOT EXISTS idx_test_execution_config_test_id 
            ON app.test_execution_config(test_id)
        """)
        
        # Indexes for vehicle sections
        execute("""
            CREATE INDEX IF NOT EXISTS idx_vehicle_sections_vehicle_id 
            ON app.vehicle_sections(vehicle_id)
        """)
        
        # Indexes for diagnostic folders
        execute("""
            CREATE INDEX IF NOT EXISTS idx_diagnostic_folders_ecu_code 
            ON app.diagnostic_folders(ecu_code)
        """)
        
        _log("Database indexes ensured", "INFO")
    except Exception as e:
        _log(f"Failed to create indexes: {e}", "WARN")


# =============================================================================
# DATABASE SYNC — MAIN ENTRY POINT
# =============================================================================

def sync_tests_from_filesystem(strict: bool = False) -> Dict[str, int]:
    """
    Full ingestion from filesystem JSON files to database.

    For each active vehicle in the database:
    1. Finds vehicle root directory in Test_Programs/
    2. Loads section_tests.json → syncs sections + auto-run programs
    3. Loads ecu_tests.json → syncs ECUs + parameters
    4. Walks all tests.json files → syncs test definitions
    """
    if query_all is None or execute is None:
        raise RuntimeError("Database helpers not available")

    # FIX-42: Ensure indexes exist before syncing
    _ensure_indexes()

    stats: Dict[str, int] = {
        "vehicles_processed": 0,
        "vehicles_skipped": 0,
        "sections_created": 0,
        "sections_updated": 0,
        "ecus_created": 0,
        "ecus_updated": 0,
        "parameters_created": 0,
        "parameters_updated": 0,
        "health_tabs_created": 0,
        "health_tabs_updated": 0,
        "tests_created": 0,
        "tests_updated": 0,
        "errors": 0,
    }

    _ensure_global_diagnostic_sections()

    db_vehicles = query_all("""
        SELECT id, name FROM app.vehicles WHERE is_active = TRUE
    """)

    vehicles_map = {normalize(v["name"]): v for v in db_vehicles}

    for vehicle_dir in list_all_vehicles():
        vehicle = vehicles_map.get(normalize(vehicle_dir))

        if not vehicle:
            _log(
                f"Vehicle '{vehicle_dir}' not found in database, skipping",
                "WARN",
            )
            stats["vehicles_skipped"] += 1
            continue

        vehicle_id = vehicle["id"]
        vehicle_name = vehicle["name"]

        _log(f"Processing vehicle: {vehicle_name} (ID: {vehicle_id})")
        stats["vehicles_processed"] += 1

        try:
            # ── section_tests.json ──
            _log(f"  Loading {SECTION_TESTS_FILENAME}")
            try:
                section_data = load_section_tests(vehicle_name)
                if section_data:
                    _sync_sections_to_db(vehicle_id, section_data, stats)
                    register_definition_version(
                        vehicle_id, section_data, "filesystem"
                    )
                    _log(f"  {SECTION_TESTS_FILENAME} synced OK")
                else:
                    _log(
                        f"  {SECTION_TESTS_FILENAME} not found in any "
                        f"supported path",
                        "WARN",
                    )
            except Exception as e:
                _log(
                    f"  Error syncing {SECTION_TESTS_FILENAME}: {e}",
                    "ERROR",
                )
                stats["errors"] += 1

            # ── ecu_tests.json ──
            _log(f"  Loading {ECU_TESTS_FILENAME}")
            try:
                ecu_data = load_ecu_tests(vehicle_name)
                if ecu_data:
                    _sync_ecus_to_db(vehicle_id, ecu_data, stats)
                    register_definition_version(
                        vehicle_id, ecu_data, "filesystem"
                    )
                    _log(f"  {ECU_TESTS_FILENAME} synced OK")
                else:
                    _log(
                        f"  {ECU_TESTS_FILENAME} not found in any "
                        f"supported path",
                        "WARN",
                    )
            except Exception as e:
                _log(
                    f"  Error syncing {ECU_TESTS_FILENAME}: {e}",
                    "ERROR",
                )
                stats["errors"] += 1

            # ── All tests.json files ──
            root = get_vehicle_root(vehicle_name)
            _sync_all_tests_to_db(vehicle_id, root, strict, stats)

        except Exception as e:
            _log(f"Error processing vehicle {vehicle_dir}: {e}", "ERROR")
            stats["errors"] += 1
            if strict:
                raise

    _log(f"Sync complete: {stats}")
    return stats


def _ensure_global_diagnostic_sections():
    """Ensure global diagnostic section definitions exist."""
    if execute is None:
        return

    sections = [
        (
            "diagnostics",
            "Diagnostics",
            "ECU diagnostics and parameter monitoring",
        ),
        (
            "vehicle_health",
            "Vehicle Health Report",
            "Comprehensive vehicle health assessment",
        ),
    ]

    for code, name, desc in sections:
        execute("""
            INSERT INTO app.diagnostic_sections
            (section_code, section_name, description, is_active)
            VALUES (:code, :name, :desc, TRUE)
            ON CONFLICT (section_code) DO UPDATE SET
                section_name = EXCLUDED.section_name,
                description  = EXCLUDED.description,
                is_active    = TRUE
        """, {"code": code, "name": name, "desc": desc})


# =============================================================================
# DATABASE SYNC — SECTIONS + AUTO-RUN PROGRAMS
# =============================================================================

def _sync_sections_to_db(vehicle_id: int, data: Dict, stats: Dict):
    """Sync sections from section_tests.json to database."""
    for section in data.get("sections", []):
        section_type = (
            section.get("section_type", "") or ""
        ).lower().strip()
        section_slug = section.get("slug", "")

        if not section_type or not section_slug:
            _log(
                f"  Section missing section_type or slug: "
                f"{section.get('name', '?')}",
                "WARN",
            )
            continue

        # Upsert vehicle_sections
        existing = query_one("""
            SELECT id FROM app.vehicle_sections
            WHERE vehicle_id = :vid AND slug = :slug
        """, {"vid": vehicle_id, "slug": section_slug})

        if existing:
            execute("""
                UPDATE app.vehicle_sections SET
                    name         = :name,
                    section_type = :type,
                    description  = :desc,
                    icon         = :icon,
                    sort_order   = :sort,
                    is_active    = :active,
                    updated_at   = CURRENT_TIMESTAMP
                WHERE id = :id
            """, {
                "id": existing["id"],
                "name": section.get("name", ""),
                "type": section_type,
                "desc": section.get("description"),
                "icon": section.get("icon"),
                "sort": section.get("sort_order", 0),
                "active": section.get("is_active", True),
            })
            section_id = existing["id"]
            stats["sections_updated"] += 1
        else:
            execute("""
                INSERT INTO app.vehicle_sections
                (vehicle_id, name, slug, section_type, description,
                 icon, sort_order, is_active)
                VALUES
                (:vid, :name, :slug, :type, :desc,
                 :icon, :sort, :active)
            """, {
                "vid": vehicle_id,
                "name": section.get("name", ""),
                "slug": section_slug,
                "type": section_type,
                "desc": section.get("description"),
                "icon": section.get("icon"),
                "sort": section.get("sort_order", 0),
                "active": section.get("is_active", True),
            })

            row = query_one("""
                SELECT id FROM app.vehicle_sections
                WHERE vehicle_id = :vid AND slug = :slug
            """, {"vid": vehicle_id, "slug": section_slug})
            section_id = row["id"]
            stats["sections_created"] += 1

        # ── Sync auto-run programs ──
        auto_run = section.get("auto_run_programs", [])
        _sync_auto_run_programs_to_db(
            vehicle_id, section_id, auto_run, stats
        )

        # ── Sync health tabs ──
        if section_type == "vehicle_health":
            health_tabs = section.get("health_tabs", [])
            if health_tabs:
                _sync_health_tabs_to_db(vehicle_id, health_tabs, stats)


# =============================================================================
# DATABASE SYNC — SECTIONS + AUTO-RUN PROGRAMS
# =============================================================================

def _sync_auto_run_programs_to_db(
    vehicle_id: int,
    section_id: int,
    programs: List[Dict],
    stats: Dict,
):
    """
    Sync auto-run program config to vehicle_section_map as jsonb.
    """
    normalized = []

    for prog in programs:
        pid = prog.get("program_id", "")
        if not pid:
            _log("    Auto-run program missing program_id, skipping", "WARN")
            continue

        # FIX: Ensure module_name and function_name are present for service.py resolution
        norm = {
            "program_id": pid,
            "program_name": prog.get("program_name", pid),
            "program_type": prog.get("program_type", "single"),
            
            # CRITICAL FIX: Carry these through for _resolve_auto_run_function
            "module_name": prog.get("module_name"),
            "function_name": prog.get("function_name"),
            
            "execution_mode": _map_execution_mode(
                prog.get("execution_mode", "single")
            ),
            "display_type": prog.get("display_type", "text"),
            "display_label": prog.get("display_label", pid),
            "display_unit": prog.get("display_unit"),
            "display_pages": prog.get(
                "display_pages", ["section", "ecu", "parameter"]
            ),
            "ecu_targets": prog.get("ecu_targets", []),
            "output_limits": prog.get("output_limits", []),
            "fallback_action": prog.get("fallback_action", "none"),
            "fallback_input": prog.get("fallback_input"),
            "log_as_vin": prog.get("log_as_vin", False),
            "is_required": prog.get("is_required", True),
            "timeout_sec": prog.get("timeout_sec", 15),
            "sort_order": prog.get("sort_order", 0),
        }

        normalized.append(norm)
        _log(
            f"    Auto-run: {pid} "
            f"({norm['program_type']}, {norm['display_type']})"
        )

    auto_json = json.dumps(normalized) if normalized else "[]"

    execute("""
        INSERT INTO app.vehicle_section_map
        (vehicle_id, section_id, auto_run_programs, is_active)
        VALUES (:vid, :sid, CAST(:auto AS jsonb), TRUE)
        ON CONFLICT (vehicle_id, section_id) DO UPDATE SET
            auto_run_programs = CAST(:auto AS jsonb),
            is_active         = TRUE
    """, {
        "vid": vehicle_id,
        "sid": section_id,
        "auto": auto_json,
    })


# =============================================================================
# DATABASE SYNC — HEALTH TABS
# =============================================================================

def _sync_health_tabs_to_db(
    vehicle_id: int,
    tabs: List[Dict],
    stats: Dict,
):
    """
    Sync health tabs to database using vehicle_health_sections and vehicle_health_folders.
    FIX-41: Added missing fields for vehicle_health_folders.
    """
    # Get or create vehicle health section
    vh_section = query_one("""
        SELECT id FROM app.vehicle_health_sections
        WHERE vehicle_id = :vid AND slug = 'vehicle_health'
    """, {"vid": vehicle_id})

    if not vh_section:
        execute("""
            INSERT INTO app.vehicle_health_sections
            (vehicle_id, section_name, slug, description,
             sort_order, is_active)
            VALUES
            (:vid, 'Vehicle Health Report', 'vehicle_health',
             'Comprehensive vehicle health assessment', 0, TRUE)
        """, {"vid": vehicle_id})

        vh_section = query_one("""
            SELECT id FROM app.vehicle_health_sections
            WHERE vehicle_id = :vid AND slug = 'vehicle_health'
        """, {"vid": vehicle_id})

    vh_section_id = vh_section["id"]

    for tab in tabs:
        folder_code = tab.get("folder_code", "")
        folder_name = tab.get("folder_name", "")
        if not folder_code:
            continue

        # FIX-41: Check for existing folder
        existing = query_one("""
            SELECT id FROM app.vehicle_health_folders
            WHERE section_id = :sid AND folder_code = :code
        """, {"sid": vh_section_id, "code": folder_code})

        exec_class = tab.get("execution_class", "SINGLE")

        if existing:
            execute("""
                UPDATE app.vehicle_health_folders SET
                    folder_name     = :name,
                    description     = :desc,
                    execution_class = :exec,
                    icon            = :icon,
                    sort_order      = :sort,
                    is_active       = :active
                WHERE id = :id
            """, {
                "id": existing["id"],
                "name": folder_name,
                "desc": tab.get("description"),
                "exec": exec_class,
                "icon": tab.get("icon"),
                "sort": tab.get("sort_order", 0),
                "active": tab.get("is_active", True),
            })
            folder_id = existing["id"]
            stats["health_tabs_updated"] += 1
        else:
            execute("""
                INSERT INTO app.vehicle_health_folders
                (section_id, folder_code, folder_name, description,
                 execution_class, icon, sort_order, is_active)
                VALUES
                (:sid, :code, :name, :desc,
                 :exec, :icon, :sort, :active)
            """, {
                "sid": vh_section_id,
                "code": folder_code,
                "name": folder_name,
                "desc": tab.get("description"),
                "exec": exec_class,
                "icon": tab.get("icon"),
                "sort": tab.get("sort_order", 0),
                "active": tab.get("is_active", True),
            })

            row = query_one("""
                SELECT id FROM app.vehicle_health_folders
                WHERE section_id = :sid AND folder_code = :code
            """, {"sid": vh_section_id, "code": folder_code})
            folder_id = row["id"]
            stats["health_tabs_created"] += 1

        # FIX-43: Use vehicle_health_map (not vehicle_health_actions)
        execute("""
            INSERT INTO app.vehicle_health_map
            (vehicle_id, health_section_id, folder_id, is_active)
            VALUES (:vid, :hsid, :fid, TRUE)
            ON CONFLICT (health_section_id, folder_id) DO UPDATE SET
                is_active = TRUE
        """, {
            "vid": vehicle_id,
            "hsid": vh_section_id,
            "fid": folder_id,
        })


# =============================================================================
# DATABASE SYNC — ECUS AND PARAMETERS
# =============================================================================

def _sync_ecus_to_db(vehicle_id: int, data: Dict, stats: Dict):
    """Sync ECUs and parameters from ecu_tests.json to database."""
    ecus_list = data.get("ecus", [])
    if not ecus_list:
        _log("  No ECUs found in ecu_tests.json", "WARN")
        return

    # Get or create diagnostic section
    diag_section = query_one("""
        SELECT id FROM app.vehicle_diagnostic_sections
        WHERE vehicle_id = :vid AND slug = 'diagnostics'
    """, {"vid": vehicle_id})

    if not diag_section:
        global_section = query_one("""
            SELECT id FROM app.diagnostic_sections
            WHERE section_code = 'diagnostics'
        """)
        global_section_id = (
            global_section["id"] if global_section else None
        )

        execute("""
            INSERT INTO app.vehicle_diagnostic_sections
            (vehicle_id, section_id, section_name, slug,
             description, sort_order, is_active)
            VALUES
            (:vid, :sid, 'Diagnostics', 'diagnostics',
             'ECU Diagnostics', 0, TRUE)
        """, {"vid": vehicle_id, "sid": global_section_id})

        diag_section = query_one("""
            SELECT id FROM app.vehicle_diagnostic_sections
            WHERE vehicle_id = :vid AND slug = 'diagnostics'
        """, {"vid": vehicle_id})

    diag_section_id = diag_section["id"]

    for ecu in ecus_list:
        ecu_code = ecu.get("ecu_code", "")
        if not ecu_code:
            _log("  ECU missing ecu_code, skipping", "WARN")
            continue

        _log(f"  Processing ECU: {ecu_code}")

        # Upsert diagnostic_folders (global ECU definition)
        global_folder = query_one("""
            SELECT id FROM app.diagnostic_folders
            WHERE ecu_code = :code
        """, {"code": ecu_code})

        if global_folder:
            execute("""
                UPDATE app.diagnostic_folders SET
                    ecu_name    = :name,
                    description = :desc,
                    protocol    = :proto,
                    emission    = :emission,
                    icon        = :icon,
                    is_active   = :active,
                    sort_order  = :sort,
                    updated_at  = CURRENT_TIMESTAMP
                WHERE id = :id
            """, {
                "id": global_folder["id"],
                "name": ecu.get("ecu_name", ""),
                "desc": ecu.get("description"),
                "proto": ecu.get("protocol"),
                "emission": ecu.get("emission"),
                "icon": ecu.get("icon"),
                "active": ecu.get("is_active", True),
                "sort": ecu.get("sort_order", 0),
            })
            global_folder_id = global_folder["id"]
            stats["ecus_updated"] += 1
        else:
            execute("""
                INSERT INTO app.diagnostic_folders
                (ecu_code, ecu_name, description, protocol,
                 emission, icon, is_active, sort_order)
                VALUES
                (:code, :name, :desc, :proto,
                 :emission, :icon, :active, :sort)
            """, {
                "code": ecu_code,
                "name": ecu.get("ecu_name", ""),
                "desc": ecu.get("description"),
                "proto": ecu.get("protocol"),
                "emission": ecu.get("emission"),
                "icon": ecu.get("icon"),
                "active": ecu.get("is_active", True),
                "sort": ecu.get("sort_order", 0),
            })

            row = query_one("""
                SELECT id FROM app.diagnostic_folders
                WHERE ecu_code = :code
            """, {"code": ecu_code})
            global_folder_id = row["id"]
            stats["ecus_created"] += 1

        # Upsert vehicle_diagnostic_actions (vehicle-specific ECU entry)
        vehicle_ecu = query_one("""
            SELECT id FROM app.vehicle_diagnostic_actions
            WHERE vehicle_id = :vid AND ecu_code = :code
        """, {"vid": vehicle_id, "code": ecu_code})

        if vehicle_ecu:
            execute("""
                UPDATE app.vehicle_diagnostic_actions SET
                    ecu_name    = :name,
                    description = :desc,
                    protocol    = :proto,
                    emission    = :emission,
                    is_active   = :active,
                    sort_order  = :sort
                WHERE id = :id
            """, {
                "id": vehicle_ecu["id"],
                "name": ecu.get("ecu_name", ""),
                "desc": ecu.get("description"),
                "proto": ecu.get("protocol"),
                "emission": ecu.get("emission"),
                "active": ecu.get("is_active", True),
                "sort": ecu.get("sort_order", 0),
            })
        else:
            execute("""
                INSERT INTO app.vehicle_diagnostic_actions
                (vehicle_id, diagnostic_section_id, folder_id,
                 ecu_code, ecu_name, description, protocol,
                 emission, is_active, sort_order)
                VALUES
                (:vid, :dsid, :fid,
                 :code, :name, :desc, :proto,
                 :emission, :active, :sort)
            """, {
                "vid": vehicle_id,
                "dsid": diag_section_id,
                "fid": global_folder_id,
                "code": ecu_code,
                "name": ecu.get("ecu_name", ""),
                "desc": ecu.get("description"),
                "proto": ecu.get("protocol"),
                "emission": ecu.get("emission"),
                "active": ecu.get("is_active", True),
                "sort": ecu.get("sort_order", 0),
            })

        # Sync parameters
        params = ecu.get("parameters", [])
        _log(f"    Parameters to sync: {len(params)}")
        for param in params:
            _sync_parameter_to_db(global_folder_id, param, stats)


def _sync_parameter_to_db(folder_id: int, param: Dict, stats: Dict):
    """Sync a parameter to database."""
    param_code = param.get("parameter_code", "")
    if not param_code:
        return

    existing = query_one("""
        SELECT id FROM app.diagnostic_actions
        WHERE folder_id = :fid AND parameter_code = :code
    """, {"fid": folder_id, "code": param_code})

    exec_class = param.get("execution_class", "SINGLE")
    label = param.get("label", "")

    if existing:
        execute("""
            UPDATE app.diagnostic_actions SET
                label           = :label,
                description     = :desc,
                execution_class = :exec,
                icon            = :icon,
                is_active       = :active,
                sort_order      = :sort
            WHERE id = :id
        """, {
            "id": existing["id"],
            "label": label,
            "desc": param.get("description"),
            "exec": exec_class,
            "icon": param.get("icon"),
            "active": param.get("is_active", True),
            "sort": param.get("sort_order", 0),
        })
        stats["parameters_updated"] += 1
    else:
        execute("""
            INSERT INTO app.diagnostic_actions
            (folder_id, parameter_code, label, description,
             execution_class, icon, is_active, sort_order)
            VALUES
            (:fid, :code, :label, :desc,
             :exec, :icon, :active, :sort)
        """, {
            "fid": folder_id,
            "code": param_code,
            "label": label,
            "desc": param.get("description"),
            "exec": exec_class,
            "icon": param.get("icon"),
            "active": param.get("is_active", True),
            "sort": param.get("sort_order", 0),
        })
        stats["parameters_created"] += 1

    _log(f"    Synced parameter: {param_code}")


# =============================================================================
# DATABASE SYNC — TESTS
# =============================================================================

def _sync_all_tests_to_db(
    vehicle_id: int,
    root: str,
    strict: bool,
    stats: Dict,
):
    """Find and sync all tests.json files in a vehicle directory."""
    diag_dir = os.path.join(root, "Diagnostics")
    if os.path.isdir(diag_dir):
        for ecu in os.listdir(diag_dir):
            ecu_dir = os.path.join(diag_dir, ecu)
            if not os.path.isdir(ecu_dir) or ecu.startswith("."):
                continue
            for param in os.listdir(ecu_dir):
                param_dir = os.path.join(ecu_dir, param)
                if not os.path.isdir(param_dir) or param.startswith("."):
                    continue
                tests_file = os.path.join(param_dir, TESTS_FILENAME)
                if os.path.isfile(tests_file):
                    _load_and_sync_tests_file(
                        vehicle_id, tests_file, strict, stats
                    )

    vh_dir = os.path.join(root, "Vehicle_Health_Report")
    if os.path.isdir(vh_dir):
        for tab in os.listdir(vh_dir):
            tab_dir = os.path.join(vh_dir, tab)
            if not os.path.isdir(tab_dir) or tab.startswith("."):
                continue
            tests_file = os.path.join(tab_dir, TESTS_FILENAME)
            if os.path.isfile(tests_file):
                _load_and_sync_tests_file(
                    vehicle_id, tests_file, strict, stats
                )


def _load_and_sync_tests_file(
    vehicle_id: int,
    tests_file: str,
    strict: bool,
    stats: Dict,
):
    """Load a tests.json file and sync to database."""
    try:
        data = load_json_file(tests_file, TESTS_SCHEMA_PATH, strict)
        _sync_tests_data_to_db(vehicle_id, data, stats)
        register_definition_version(vehicle_id, data, "filesystem")
        _log(f"    Synced: {tests_file}")
    except Exception as e:
        _log(f"Error syncing {tests_file}: {e}", "ERROR")
        stats["errors"] += 1
        if strict:
            raise


def _sync_tests_data_to_db(vehicle_id: int, data: Dict, stats: Dict):
    """Sync test data from a tests.json payload to database."""
    section = (data.get("section", "diagnostics") or "").lower().strip()
    ecu = data.get("ecu")
    parameter = data.get("parameter", "")

    for test in data.get("tests", []):
        _upsert_test_to_db(
            vehicle_id=vehicle_id,
            test=test,
            section=section,
            ecu=ecu,
            parameter=parameter,
            stats=stats,
        )


def _upsert_test_to_db(
    vehicle_id: int,
    test: Dict,
    section: str,
    ecu: Optional[str],
    parameter: str,
    stats: Dict,
):
    """Insert or update a single test."""
    test_id = test.get("id", "")
    if not test_id:
        _log("Test missing 'id' field, skipping", "WARN")
        return

    page_type = test.get("parameter_page_type", "LIVE_PARAMETER")
    execution = test.get("execution", {}) or {}
    execution_mode = _map_execution_mode(execution.get("mode", "single"))

    existing = query_one("""
        SELECT id FROM app.tests WHERE id = :id
    """, {"id": test_id})

    test_data = {
        "id": test_id,
        "vehicle_id": vehicle_id,
        "label": test.get("label", ""),
        "description": sanitize_for_db(test.get("description"), 1000),
        "module_name": test.get("module_name", ""),
        "function_name": test.get("function_name", ""),
        "button_name": test.get("button_name", "Run"),
        "parameter_page_type": page_type,
        "function_role": test.get("function_role"),
        "section": section,
        "ecu": ecu,
        "parameter": parameter,
        "version": test.get("version", "1.0"),
        "is_active": test.get("is_active", True),
        "sort_order": test.get("sort_order", 0),
    }

    if existing:
        execute("""
            UPDATE app.tests SET
                vehicle_id          = :vehicle_id,
                label               = :label,
                description         = :description,
                module_name         = :module_name,
                function_name       = :function_name,
                button_name         = :button_name,
                parameter_page_type = :parameter_page_type,
                function_role       = :function_role,
                section             = :section,
                ecu                 = :ecu,
                parameter           = :parameter,
                version             = :version,
                is_active           = :is_active,
                sort_order          = :sort_order,
                updated_at          = CURRENT_TIMESTAMP
            WHERE id = :id
        """, test_data)
        stats["tests_updated"] += 1
    else:
        execute("""
            INSERT INTO app.tests
            (id, vehicle_id, label, description, module_name,
             function_name, button_name, parameter_page_type,
             function_role, section, ecu, parameter,
             version, is_active, sort_order)
            VALUES
            (:id, :vehicle_id, :label, :description, :module_name,
             :function_name, :button_name, :parameter_page_type,
             :function_role, :section, :ecu, :parameter,
             :version, :is_active, :sort_order)
        """, test_data)
        stats["tests_created"] += 1

    # Sync child tables
    _sync_test_inputs(test_id, test.get("inputs", []))
    _sync_test_output_limits(test_id, test.get("output_limits", []))
    _sync_test_execution_config(test_id, execution, execution_mode)
    _sync_test_flashing_config(test_id, test.get("flashing"))


# =============================================================================
# DATABASE SYNC — TEST CHILD TABLES
# =============================================================================

def _sync_test_inputs(test_id: str, inputs: List[Dict]):
    """Sync test inputs to database (delete-and-reinsert)."""
    execute(
        "DELETE FROM app.test_inputs WHERE test_id = :id",
        {"id": test_id},
    )

    for i, inp in enumerate(inputs):
        name = inp.get("name", "")
        if not name:
            continue

        raw_enum = inp.get("enum_values")
        enum_values = _to_jsonb(raw_enum)

        default_raw = inp.get("default_value")
        default_value = (
            str(default_raw) if default_raw is not None else None
        )

        execute("""
            INSERT INTO app.test_inputs
            (test_id, name, label, input_type, length,
             min_value, max_value, enum_values, default_value,
             config_key, format_hint, is_required, sort_order)
            VALUES
            (:test_id, :name, :label, :type, :length,
             :min, :max, CAST(:enum AS jsonb), :default,
             :config, :hint, :required, :sort)
        """, {
            "test_id": test_id,
            "name": name,
            "label": inp.get("label"),
            "type": inp.get("input_type", "string"),
            "length": inp.get("length"),
            "min": inp.get("min_value"),
            "max": inp.get("max_value"),
            "enum": enum_values,
            "default": default_value,
            "config": inp.get("config_key"),
            "hint": inp.get("format_hint"),
            "required": inp.get("is_required", False),
            "sort": inp.get("sort_order", i),
        })


def _sync_test_output_limits(test_id: str, limits: List[Dict]):
    """Sync test output limits to database (delete-and-reinsert)."""
    execute(
        "DELETE FROM app.test_output_limits WHERE test_id = :id",
        {"id": test_id},
    )

    for lim in limits:
        signal = lim.get("signal", "")
        if not signal:
            continue

        execute("""
            INSERT INTO app.test_output_limits
            (test_id, signal, lsl, usl, unit)
            VALUES (:test_id, :signal, :lsl, :usl, :unit)
        """, {
            "test_id": test_id,
            "signal": signal,
            "lsl": lim.get("lsl"),
            "usl": lim.get("usl"),
            "unit": lim.get("unit"),
        })


def _sync_test_execution_config(
    test_id: str,
    execution: Dict,
    execution_mode: str,
):
    """Sync test execution config to database (delete-and-reinsert)."""
    execute(
        "DELETE FROM app.test_execution_config WHERE test_id = :id",
        {"id": test_id},
    )

    execute("""
        INSERT INTO app.test_execution_config
        (test_id, execution_mode, supports_run_all, timeout_sec,
         max_retries, retry_delay_sec, retry_on_timeout,
         retry_on_exception)
        VALUES
        (:test_id, :mode, :run_all, :timeout,
         :retries, :delay, :retry_timeout, :retry_exception)
    """, {
        "test_id": test_id,
        "mode": execution_mode,
        "run_all": execution.get("supports_run_all", True),
        "timeout": execution.get("timeout_sec", 15),
        "retries": execution.get("max_retries", 3),
        "delay": execution.get("retry_delay_sec", 1.5),
        "retry_timeout": execution.get("retry_on_timeout", False),
        "retry_exception": execution.get("retry_on_exception", False),
    })


def _sync_test_flashing_config(
    test_id: str,
    flashing: Optional[Dict],
):
    """Sync test flashing config to database (delete-and-reinsert)."""
    execute(
        "DELETE FROM app.test_flashing_config WHERE test_id = :id",
        {"id": test_id},
    )

    if not flashing:
        return

    required_inputs = _to_jsonb(flashing.get("required_inputs", []))

    execute("""
        INSERT INTO app.test_flashing_config
        (test_id, file_name, file_type, method, required_inputs)
        VALUES
        (:test_id, :file_name, :file_type, :method,
         CAST(:required_inputs AS jsonb))
    """, {
        "test_id": test_id,
        "file_name": flashing.get("file_name", ""),
        "file_type": flashing.get("file_type", "HEX"),
        "method": flashing.get("method", "UDS"),
        "required_inputs": required_inputs,
    })


# =============================================================================
# AUTO-RUN PROGRAM RETRIEVAL
# =============================================================================

def get_auto_run_config(
    vehicle: str,
    section_type: str,
) -> List[Dict[str, Any]]:
    """
    Get auto-run program configuration for a vehicle section.

    Tries database first (vehicle_section_map.auto_run_programs),
    falls back to JSON file.

    Returns list of auto-run program objects with full metadata.
    """
    # Try database first
    if query_one is not None:
        try:
            db_vehicle = query_one("""
                SELECT v.id
                FROM app.vehicles v
                WHERE v.name = :name AND v.is_active = TRUE
            """, {"name": vehicle})

            if db_vehicle:
                vehicle_id = db_vehicle["id"]

                section = query_one("""
                    SELECT vs.id
                    FROM app.vehicle_sections vs
                    WHERE vs.vehicle_id = :vid
                      AND vs.section_type = :type
                      AND vs.is_active = TRUE
                """, {"vid": vehicle_id, "type": section_type})

                if section:
                    mapping = query_one("""
                        SELECT auto_run_programs
                        FROM app.vehicle_section_map
                        WHERE vehicle_id = :vid
                          AND section_id = :sid
                          AND is_active = TRUE
                    """, {"vid": vehicle_id, "sid": section["id"]})

                    if mapping and mapping.get("auto_run_programs"):
                        programs = mapping["auto_run_programs"]
                        if isinstance(programs, str):
                            programs = json.loads(programs)
                        if programs:
                            return programs

        except Exception as e:
            _log(f"Error fetching auto-run from DB: {e}", "ERROR")

    # Fallback to JSON
    return get_auto_run_programs(vehicle, section_type)


# =============================================================================
# CI VALIDATION
# =============================================================================

def validate_all_tests_fs(strict: bool = False) -> Dict[str, Any]:
    """Validate all JSON files in the Test_Programs directory."""
    results: Dict[str, Any] = {
        "vehicles_checked": 0,
        "section_files_valid": 0,
        "section_files_invalid": 0,
        "ecu_files_valid": 0,
        "ecu_files_invalid": 0,
        "test_files_valid": 0,
        "test_files_invalid": 0,
        "errors": [],
    }

    for vehicle_dir in list_all_vehicles():
        root = os.path.join(TEST_PROGRAMS_DIR, vehicle_dir)
        results["vehicles_checked"] += 1

        # Validate section_tests.json (canonical + fallback paths)
        section_candidates = _get_fallback_candidates(
            root, SECTION_TESTS_FILENAME
        )
        section_path = _find_first_existing(section_candidates)
        if section_path:
            try:
                load_json_file(
                    section_path, SECTION_TESTS_SCHEMA_PATH, strict=True
                )
                results["section_files_valid"] += 1
            except Exception as e:
                results["section_files_invalid"] += 1
                rel = os.path.relpath(section_path, TEST_PROGRAMS_DIR)
                results["errors"].append(f"{rel}: {e}")

        # Validate ecu_tests.json (canonical + fallback paths)
        ecu_candidates = _get_fallback_candidates(
            root, ECU_TESTS_FILENAME
        )
        ecu_path = _find_first_existing(ecu_candidates)
        if ecu_path:
            try:
                load_json_file(
                    ecu_path, ECU_TESTS_SCHEMA_PATH, strict=True
                )
                results["ecu_files_valid"] += 1
            except Exception as e:
                results["ecu_files_invalid"] += 1
                rel = os.path.relpath(ecu_path, TEST_PROGRAMS_DIR)
                results["errors"].append(f"{rel}: {e}")

        _validate_tests_in_directory(root, results, strict)

    return results


def _validate_tests_in_directory(
    root: str,
    results: Dict,
    strict: bool,
):
    """Recursively validate all tests.json files."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for filename in filenames:
            if filename == TESTS_FILENAME:
                filepath = os.path.join(dirpath, filename)
                rel_path = os.path.relpath(filepath, TEST_PROGRAMS_DIR)
                try:
                    load_json_file(
                        filepath, TESTS_SCHEMA_PATH, strict=True
                    )
                    results["test_files_valid"] += 1
                except Exception as e:
                    results["test_files_invalid"] += 1
                    results["errors"].append(f"{rel_path}: {e}")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def get_vehicle_info(vehicle: str) -> Dict[str, Any]:
    """Get comprehensive info about a vehicle's test configuration."""
    try:
        root = get_vehicle_root(vehicle)
    except FileNotFoundError as e:
        return {"error": f"Vehicle not found: {vehicle}", "details": str(e)}

    info: Dict[str, Any] = {
        "vehicle": vehicle,
        "root_path": root,
        "has_section_tests": os.path.isfile(
            os.path.join(root, SECTION_TESTS_FILENAME)
        ),
        "has_ecu_tests": os.path.isfile(
            os.path.join(root, ECU_TESTS_FILENAME)
        ),
        "sections": discover_vehicle_sections(vehicle),
        "ecus": [],
        "health_tabs": [],
    }

    for ecu in discover_ecus(vehicle):
        ecu_code = ecu.get("ecu_code", "")
        info["ecus"].append({
            "ecu_code": ecu_code,
            "ecu_name": ecu.get("ecu_name", ""),
            "parameters": (
                len(discover_parameters(vehicle, ecu_code))
                if ecu_code else 0
            ),
        })

    info["health_tabs"] = discover_health_tabs(vehicle)
    return info


def reload_vehicle_tests(
    vehicle: str,
    strict: bool = False,
) -> Dict[str, int]:
    """Reload tests for a specific vehicle."""
    if query_one is None or execute is None:
        raise RuntimeError("Database helpers not available")

    db_vehicle = query_one("""
        SELECT id, name FROM app.vehicles
        WHERE name = :name AND is_active = TRUE
    """, {"name": vehicle})

    if not db_vehicle:
        raise ValueError(f"Vehicle not found in database: {vehicle}")

    vehicle_id = db_vehicle["id"]

    stats: Dict[str, int] = {
        "sections_created": 0,
        "sections_updated": 0,
        "ecus_created": 0,
        "ecus_updated": 0,
        "parameters_created": 0,
        "parameters_updated": 0,
        "health_tabs_created": 0,
        "health_tabs_updated": 0,
        "tests_created": 0,
        "tests_updated": 0,
        "errors": 0,
    }

    try:
        root = get_vehicle_root(vehicle)

        # ── section_tests.json ──
        try:
            section_data = load_section_tests(vehicle)
            if section_data:
                _sync_sections_to_db(vehicle_id, section_data, stats)
                register_definition_version(
                    vehicle_id, section_data, "filesystem"
                )
        except Exception as e:
            _log(f"Error syncing {SECTION_TESTS_FILENAME}: {e}", "ERROR")
            stats["errors"] += 1

        # ── ecu_tests.json ──
        try:
            ecu_data = load_ecu_tests(vehicle)
            if ecu_data:
                _sync_ecus_to_db(vehicle_id, ecu_data, stats)
                register_definition_version(
                    vehicle_id, ecu_data, "filesystem"
                )
        except Exception as e:
            _log(f"Error syncing {ECU_TESTS_FILENAME}: {e}", "ERROR")
            stats["errors"] += 1

        # ── All tests.json files ──
        _sync_all_tests_to_db(vehicle_id, root, strict, stats)

    except Exception as e:
        _log(f"Error reloading vehicle {vehicle}: {e}", "ERROR")
        stats["errors"] += 1
        if strict:
            raise

    return stats


# =============================================================================
# COMPATIBILITY API (EXPORTS EXPECTED BY WEBSITE APP)
# =============================================================================

def get_sections_from_json(vehicle: str) -> List[Dict[str, Any]]:
    """Compatibility wrapper for section details."""
    try:
        return get_section_details(vehicle)
    except FileNotFoundError as e:
        raise VehicleNotFoundError(str(e))


def get_ecus_from_json(vehicle: str) -> List[Dict[str, Any]]:
    """Compatibility wrapper for ECU details."""
    try:
        return get_ecu_details(vehicle)
    except FileNotFoundError as e:
        raise VehicleNotFoundError(str(e))


def get_parameters_from_json(
    vehicle: str,
    ecu_code: str,
) -> List[Dict[str, Any]]:
    """Compatibility wrapper for parameter details."""
    try:
        return get_parameter_details(vehicle, ecu_code)
    except FileNotFoundError as e:
        raise VehicleNotFoundError(str(e))


def get_auto_run_programs(
    vehicle: str,
    section_type: str,
) -> List[Dict[str, Any]]:
    """
    Extract auto-run program specs from section_tests.json.
    Fallback used by get_auto_run_config when DB unavailable.
    """
    try:
        data = load_section_tests(vehicle)
    except FileNotFoundError as e:
        raise VehicleNotFoundError(str(e))

    if not data or "sections" not in data:
        return []

    wanted = (section_type or "").lower().strip()
    for section in data["sections"]:
        if not section.get("is_active", True):
            continue
        st = (section.get("section_type") or "").lower().strip()
        if st != wanted:
            continue

        programs = section.get("auto_run_programs", []) or []

        normalized: List[Dict[str, Any]] = []
        for p in programs:
            pid = p.get("program_id", "")
            if not pid:
                continue

            normalized.append({
                "program_id": pid,
                "program_name": p.get("program_name", pid),
                "program_type": p.get("program_type", "single"),
                # NEW: carried through from JSON definition
                "module_name": p.get("module_name"),
                "function_name": p.get("function_name"),
                "execution_mode": _map_execution_mode(
                    p.get("execution_mode", "single")
                ),
                "display_type": p.get("display_type", "text"),
                "display_label": p.get("display_label", pid),
                "display_unit": p.get("display_unit"),
                "display_pages": p.get(
                    "display_pages", ["section", "ecu", "parameter"]
                ),
                "ecu_targets": p.get("ecu_targets", []),
                "output_limits": p.get("output_limits", []),
                "fallback_action": p.get("fallback_action", "none"),
                "fallback_input": p.get("fallback_input"),
                "log_as_vin": p.get("log_as_vin", False),
                "is_required": p.get("is_required", True),
                "timeout_sec": p.get("timeout_sec", 15),
                "sort_order": p.get("sort_order", 0),
            })

        return normalized

    return []


# =============================================================================
# MODULE INITIALIZATION
# =============================================================================

def _init():
    """Initialize the loader module."""
    if not os.path.isdir(SCHEMA_DIR):
        _log(f"Schema directory not found: {SCHEMA_DIR}", "WARN")
    if not os.path.isdir(TEST_PROGRAMS_DIR):
        _log(
            f"Test programs directory not found: {TEST_PROGRAMS_DIR}",
            "WARN",
        )
    
    # FIX-42: Ensure indexes exist on startup
    try:
        _ensure_indexes()
    except Exception as e:
        _log(f"Failed to create indexes during init: {e}", "WARN")


_init()


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Exceptions
    "VehicleNotFoundError",
    "ModuleLoadError",
    "FunctionNotFoundError",

    # Core loaders
    "load_test_function",
    "load_function_from_path",
    "get_sections_from_json",
    "get_ecus_from_json",
    "get_parameters_from_json",
    "get_auto_run_programs",
    "get_auto_run_config",
    "get_tests_for_parameter",

    # Discovery
    "discover_vehicle_sections",
    "discover_vehicle_json_files",
    "discover_ecus",
    "discover_parameters",
    "discover_health_tabs",
    "list_all_vehicles",

    # Database sync
    "sync_tests_from_filesystem",
    "reload_vehicle_tests",

    # Validation
    "validate_all_tests_fs",

    # Utilities
    "get_vehicle_root",
    "get_vehicle_info",
    "clear_function_cache",
    "safe_name",
    "normalize",
]