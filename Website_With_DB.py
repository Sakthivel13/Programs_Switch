# -*- coding: utf-8 -*-
"""
NIRIX DIAGNOSTICS – MAIN WEB APPLICATION (PostgreSQL Production)
FULLY INTEGRATED WITH UPDATED LOADER, SERVICE, RUNNER, AND SCANNER (OpenCV)

Version: 3.1.0
Last Updated: 2026-02-14

FIXES IN v3.1.0
────────────────
- FIX-30: Stream values API endpoint - ensures auto_run_stream_values are queryable
- FIX-31: Auto-run session VIN persistence - VIN displays correctly in UI
- FIX-32: Enhanced error handling for stream value polling
- FIX-33: Added stream values cleanup on session end
- FIX-34: Improved debug logging for troubleshooting
"""

# =============================================================================
# STANDARD LIBRARIES
# =============================================================================
import os
import sys
import re
import json
import socket
import random
import datetime
import threading
import platform
import uuid
import traceback
from typing import Optional, Dict, List, Any

# =============================================================================
# FLASK
# =============================================================================
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, jsonify, send_from_directory, abort, Response
)

# =============================================================================
# SECURITY
# =============================================================================
from werkzeug.security import generate_password_hash, check_password_hash

# =============================================================================
# INTERNAL MODULES (POSTGRESQL SAFE)
# =============================================================================
from database import query_one, query_all, execute

from auth import (
    require_login,
    ROLE_SUPER_ADMIN,
    ROLE_ADMIN,
    ROLE_TECHNICIAN,
)

from diagnostics.service import get_auto_run_status as svc_get_auto_run_status

# =============================================================================
# DIAGNOSTICS MODULES - UPDATED IMPORTS
# =============================================================================
from diagnostics.loader import (
    # Sync & validation
    sync_tests_from_filesystem,
    validate_all_tests_fs,

    # Runtime function loading (filesystem only)
    load_test_function,

    # Discovery (JSON + fallback)
    discover_vehicle_sections,
    discover_ecus,
    discover_parameters,
    discover_health_tabs,

    # JSON helpers
    get_sections_from_json,
    get_ecus_from_json,
    get_parameters_from_json,
    get_auto_run_programs,

    # Exceptions
    VehicleNotFoundError,
    ModuleLoadError,
    FunctionNotFoundError,
)

from diagnostics.runner import (
    # Task management APIs (used by website endpoints)
    get_task_status,
    cancel_task,
    pause_task,
    resume_task,
    get_task_logs,
    get_new_task_logs,
    clear_task_logs,

    # Housekeeping
    purge_completed_tasks,
    get_active_task_count,
    get_runner_stats,

    # Batch (optional endpoint)
    execute_batch_async,
    get_batch_status,
    cancel_batch,

    # Runner types (optional)
    ExecutionContext,
)

from diagnostics.service import (
    # Main DB-authoritative execution API
    run_test,
    start_auto_run,              # new-style auto-run sessions
    run_auto_programs,           # legacy auto-run (kept for compat)

    run_all_tests_for_parameter,

    # Permission checking
    check_vehicle_permission,
    check_test_permission,

    # UI listing APIs
    list_sections_for_vehicle,
    list_ecus_for_vehicle,
    list_parameters_for_ecu,
    list_health_tabs_for_vehicle,
    list_tests_for_parameter,

    # Metadata helpers
    get_vehicle_by_name,
    load_test_metadata,
    load_execution_config,
    load_output_limits,
    get_can_configuration,

    # History
    get_test_execution_history,

    # Progress callback registration
    register_progress_callback,

    # Auto-run VIN submission (needed by tests.html manual VIN popup)
    submit_auto_run_vin,
)

# =============================================================================
# SCANNER (OPTIONAL) - OpenCV station camera QR/Barcode scan
# =============================================================================
SCANNER_AVAILABLE = False
try:
    # New scanner.py supports optional preview
    from diagnostics.scanner import (
        start_scan, get_scan, cancel_scan,
        get_scan_frame_jpeg, cleanup_scans
    )
    SCANNER_AVAILABLE = True
except Exception as e:
    print(f"[STARTUP] Scanner not available: {e}")
    start_scan = get_scan = cancel_scan = None
    get_scan_frame_jpeg = None
    cleanup_scans = None
    SCANNER_AVAILABLE = False

# =============================================================================
# CAN UTILS (OPTIONAL)
# =============================================================================
try:
    from diagnostics.can_utils import (
        get_config_value,
        set_config_value,
    )
    CAN_UTILS_AVAILABLE = True
except ImportError:
    CAN_UTILS_AVAILABLE = False

    def get_config_value(key, default=None):
        try:
            row = query_one(
                "SELECT value_text FROM app.config WHERE key_name = :k",
                {"k": key}
            )
            return row["value_text"] if row else default
        except Exception:
            return default

    def set_config_value(key, value):
        try:
            execute("""
                INSERT INTO app.config (key_name, value_text)
                VALUES (:k, :v)
                ON CONFLICT (key_name) DO UPDATE SET value_text = EXCLUDED.value_text
            """, {"k": key, "v": str(value)})
        except Exception:
            pass

# =============================================================================
# STARTUP: SYNC + VALIDATE (SAFE TO RUN ON STARTUP)
# =============================================================================
print("\n" + "=" * 60)
print("NIRIX DIAGNOSTICS - Starting...")
print("=" * 60 + "\n")

# Optional: allow disabling sync at startup in production multi-worker deployments
SYNC_ON_START = os.getenv("NIRIX_SYNC_ON_START", "true").lower() in ("1", "true", "yes")
VALIDATE_ON_START = os.getenv("NIRIX_VALIDATE_ON_START", "true").lower() in ("1", "true", "yes")

if SYNC_ON_START:
    try:
        print("[STARTUP] Syncing tests from filesystem...")
        sync_result = sync_tests_from_filesystem()
        print(f"[STARTUP] Test sync complete: {sync_result}")
    except Exception as e:
        print(f"[STARTUP] Warning: Test sync failed: {e}")
        if os.getenv("NIRIX_DEBUG", "false").lower() == "true":
            traceback.print_exc()
else:
    print("[STARTUP] Sync disabled (NIRIX_SYNC_ON_START=false)")

if VALIDATE_ON_START:
    try:
        print("[STARTUP] Validating test definitions (schemas)...")
        validate_result = validate_all_tests_fs()
        print(f"[STARTUP] Test validation: {validate_result}")
    except Exception as e:
        print(f"[STARTUP] Warning: Test validation failed: {e}")
else:
    print("[STARTUP] Validation disabled (NIRIX_VALIDATE_ON_START=false)")

# =============================================================================
# PATHS & DIRECTORIES
# =============================================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

VEHICLE_IMAGE_FOLDER = os.path.join(BASE_DIR, "Vehicle_Images")
TEST_PROGRAMS_DIR = os.path.join(BASE_DIR, "Test_Programs")
TEST_LOGS_DIR = os.path.join(BASE_DIR, "Test_Logs")
SCHEMA_DIR = os.path.join(BASE_DIR, "schema")

os.makedirs(VEHICLE_IMAGE_FOLDER, exist_ok=True)
os.makedirs(TEST_PROGRAMS_DIR, exist_ok=True)
os.makedirs(TEST_LOGS_DIR, exist_ok=True)

# =============================================================================
# APP INITIALIZATION
# =============================================================================
app = Flask(__name__, template_folder="templates")

# IMPORTANT: set FLASK_SECRET in env in production
app.secret_key = os.environ.get("FLASK_SECRET", "CHANGE_ME_IMMEDIATELY")

app.config["JSON_SORT_KEYS"] = False
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False

# =============================================================================
# VALIDATION CONSTANTS
# =============================================================================
VIN_RE = re.compile(r"^[0-9A-Z]{17}$")
TEST_ID_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")

# =============================================================================
# LOG BUFFER (OPTIONAL GLOBAL LIVE LOG)
# =============================================================================
LOG_BUFFER: List[str] = []
LOG_BUFFER_LOCK = threading.Lock()
MAX_LOG_LINES = 5000

# =============================================================================
# STATION ID (HOSTNAME)
# =============================================================================
STATION_ID = socket.gethostname()

# =============================================================================
# GLOBAL LOGGER
# =============================================================================
def append_global_log(text: str, level: str = "INFO") -> None:
    if not text:
        return
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}][{level}] {text}"

    with LOG_BUFFER_LOCK:
        LOG_BUFFER.append(line)
        if len(LOG_BUFFER) > MAX_LOG_LINES:
            del LOG_BUFFER[: len(LOG_BUFFER) - MAX_LOG_LINES]

    app.logger.info(line)

# =============================================================================
# PROGRESS CALLBACK FOR RUNNER (VIA SERVICE)
# =============================================================================
def _ui_progress_callback(task_id: str, percent: int, message: str):
    append_global_log(f"[TASK:{task_id[:8]}] Progress {percent}%: {message}", "DEBUG")

register_progress_callback(_ui_progress_callback)

# =============================================================================
# SMALL UTILITIES
# =============================================================================
def _sanitize_filename_part(part: str) -> str:
    part = (part or "").strip()
    part = re.sub(r"[^A-Za-z0-9_\-]", "_", part)
    return part or "X"

def hash_pin(pin: str) -> str:
    return generate_password_hash(pin)

def verify_pin(hashed: str, pin: str) -> bool:
    if not hashed:
        return False
    return check_password_hash(hashed, pin)

def get_user_session() -> Optional[Dict]:
    return session.get("user")

def require_auth():
    user = get_user_session()
    if not user:
        abort(401)
    return user

# =============================================================================
# EMAIL (OPTIONAL)
# =============================================================================
from email.message import EmailMessage
import smtplib

SMTP_HOST = os.environ.get("SMTP_HOST", "localhost")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "25"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "false").lower() in ("1", "true", "yes")

WIN32_AVAILABLE = False
if sys.platform.startswith("win"):
    try:
        import win32com.client  # type: ignore
        WIN32_AVAILABLE = True
    except Exception:
        WIN32_AVAILABLE = False

def send_email_outlook(to_address: str, subject: str, body: str) -> bool:
    if not WIN32_AVAILABLE:
        return False
    try:
        ol = win32com.client.Dispatch("Outlook.Application")  # type: ignore
        mail = ol.CreateItem(0)
        mail.To = to_address
        mail.Subject = subject
        mail.Body = body
        mail.Send()
        return True
    except Exception as e:
        app.logger.exception("Outlook email failed: %s", e)
        return False

def send_email_smtp(
    to_address: str,
    subject: str,
    body: str,
    from_address: Optional[str] = None,
) -> bool:
    try:
        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = from_address or (SMTP_USER or f"noreply@{socket.gethostname()}")
        msg["To"] = to_address
        msg.set_content(body)

        if SMTP_HOST in ("", None, "localhost") and not SMTP_USER:
            server = smtplib.SMTP("localhost")
        else:
            server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10)
            if SMTP_USE_TLS:
                server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)

        server.send_message(msg)
        server.quit()
        return True
    except Exception as e:
        app.logger.exception("SMTP email failed: %s", e)
        return False

def send_email(to_address: str, subject: str, body: str) -> bool:
    if WIN32_AVAILABLE and send_email_outlook(to_address, subject, body):
        append_global_log(f"Email sent via Outlook → {to_address}")
        return True
    if send_email_smtp(to_address, subject, body):
        append_global_log(f"Email sent via SMTP → {to_address}")
        return True
    append_global_log(f"FAILED EMAIL → {to_address} | {subject}", "WARN")
    return False

# =============================================================================
# USER FETCH HELPERS
# =============================================================================
def get_user_by_email(email: str) -> Optional[Dict]:
    return query_one("""
        SELECT
            u.id, u.name, u.employee_id, u.email,
            u.pin, u.is_approved, u.is_disabled,
            u.theme,
            r.name AS role,
            u.role_id,
            u.reset_code
        FROM app.users u
        LEFT JOIN app.roles r ON r.id = u.role_id
        WHERE u.email = :e
        LIMIT 1
    """, {"e": email})

def get_user_by_id(user_id: int) -> Optional[Dict]:
    return query_one("""
        SELECT
            u.id, u.name, u.employee_id, u.email,
            u.pin, u.is_approved, u.is_disabled,
            u.theme,
            r.name AS role,
            u.role_id,
            u.reset_code
        FROM app.users u
        LEFT JOIN app.roles r ON r.id = u.role_id
        WHERE u.id = :id
        LIMIT 1
    """, {"id": user_id})

def login_user_into_session(user_row: Dict) -> Dict:
    session_user = {
        "id": user_row["id"],
        "name": user_row["name"],
        "employee_id": user_row["employee_id"],
        "email": user_row["email"],
        "role": user_row.get("role") or ROLE_TECHNICIAN,
        "theme": user_row.get("theme") or None,
    }
    session["user"] = session_user
    return session_user

def email_exists(email: str) -> bool:
    row = query_one(
        "SELECT id FROM app.users WHERE email = :e LIMIT 1",
        {"e": email},
    )
    return bool(row)

# =============================================================================
# LIVE SESSION ID (PER BROWSER)
# =============================================================================
def get_live_session_id() -> str:
    if "_live_session_id" not in session:
        session["_live_session_id"] = uuid.uuid4().hex
    return session["_live_session_id"]

# =============================================================================
# LIVE SYSTEM TRACKING (UPSERT)
# =============================================================================
def update_live_system() -> None:
    user = session.get("user")
    if not user:
        return

    session_id = get_live_session_id()
    ip_address = (
        request.headers.get("X-Forwarded-For", "")
        .split(",")[0].strip()
        or request.remote_addr
        or "unknown"
    )

    server_base_url = request.host_url.rstrip("/")
    host_name = socket.gethostname()
    os_name = f"{platform.system()} {platform.release()}"

    try:
        execute("""
            INSERT INTO app.live_system_track (
                session_id,
                server_base_url,
                user_id,
                host_name,
                ip_address,
                os_name,
                first_seen,
                last_active_at,
                last_seen,
                is_active
            )
            VALUES (
                :sid, :server, :uid,
                :host, :ip, :os,
                NOW(), NOW(), NOW(), TRUE
            )
            ON CONFLICT (session_id, server_base_url)
            DO UPDATE SET
                user_id        = EXCLUDED.user_id,
                host_name      = EXCLUDED.host_name,
                ip_address     = EXCLUDED.ip_address,
                os_name        = EXCLUDED.os_name,
                last_active_at = NOW(),
                last_seen      = NOW(),
                is_active      = TRUE
        """, {
            "sid": session_id,
            "server": server_base_url,
            "uid": user["id"],
            "host": host_name,
            "ip": ip_address,
            "os": os_name,
        })
    except Exception as e:
        app.logger.warning(f"Live system tracking failed: {e}")

@app.before_request
def _before_request():
    if session.get("user"):
        update_live_system()

# =============================================================================
# PERIODIC CLEANUP
# =============================================================================
def _start_cleanup_thread():
    def cleanup_loop():
        import time as _time
        while True:
            try:
                _time.sleep(300)
                removed = purge_completed_tasks()
                if removed > 0:
                    append_global_log(f"Cleaned up {removed} completed tasks", "DEBUG")

                # Optional scanner cleanup
                if SCANNER_AVAILABLE and cleanup_scans is not None:
                    cleanup_scans(max_age_sec=300)

                # FIX-33: Clean up old stream values (older than 1 hour)
                try:
                    execute("""
                        DELETE FROM app.auto_run_stream_values
                        WHERE updated_at < NOW() - INTERVAL '1 hour'
                    """)
                except Exception as e:
                    app.logger.warning(f"Stream values cleanup failed: {e}")

            except Exception as e:
                app.logger.error(f"Cleanup error: {e}")

    threading.Thread(target=cleanup_loop, daemon=True).start()

_start_cleanup_thread()

# =============================================================================
# ADMIN BLUEPRINT (OPTIONAL)
# =============================================================================
try:
    from admin import admin_bp
    app.register_blueprint(admin_bp)
except ImportError as e:
    print(f"[STARTUP] Warning: Admin blueprint not loaded: {e}")

# =============================================================================
# ROUTE AUDIT (OPTIONAL)
# =============================================================================
def audit_routes(app_instance: Flask):
    print("\n" + "=" * 60)
    print("ROUTE AUDIT")
    print("=" * 60 + "\n")

    endpoints = {}
    for rule in app_instance.url_map.iter_rules():
        endpoints.setdefault(rule.endpoint, []).append(str(rule))

    for endpoint, rules in sorted(endpoints.items()):
        for r in rules:
            print(f"  {endpoint:40s} -> {r}")

    REQUIRED_ENDPOINTS = [
        "dashboard",
        "tests_root",
        "tests_page",
        "logs_page",
        "downloads_page",
        "login",
        "logout",
        "api_run_test",
        "api_run_all_tests",
        "api_run_auto_programs",
        "api_task_status",
        "api_task_cancel",
        "api_task_pause",
        "api_task_resume",
        "api_task_logs",
        "api_batch_status",
        "api_batch_cancel",
        "forgot_pin",
        "reset_pin_verify",
        "reset_pin_new",
    ]

    missing = [ep for ep in REQUIRED_ENDPOINTS if ep not in endpoints]
    if missing:
        print(f"\nROUTE AUDIT: Missing {len(missing)} endpoints")
        for ep in missing:
            print(f"  - {ep}")
    else:
        print(f"\nROUTE AUDIT PASSED: All {len(REQUIRED_ENDPOINTS)} required endpoints present")
    print()

# =============================================================================
# CONTEXT PROCESSORS
# =============================================================================
@app.context_processor
def inject_user():
    return {"user": session.get("user")}

@app.context_processor
def inject_theme():
    theme = "light"
    if "user" in session and session["user"].get("theme"):
        theme = session["user"]["theme"]
    else:
        try:
            row = query_one(
                "SELECT value_text FROM app.config WHERE key_name = 'default_theme'"
            )
            if row and row["value_text"] in ("light", "dark"):
                theme = row["value_text"]
        except Exception:
            pass
    return {"default_theme": theme}

# =============================================================================
# THEME API
# =============================================================================
@app.route("/api/set_theme", methods=["POST"])
def api_set_theme():
    user = session.get("user")
    if not user:
        return jsonify({"error": "unauthorized"}), 401

    data = request.get_json(silent=True) or {}
    theme = (data.get("theme") or "").lower().strip()
    if theme not in ("light", "dark"):
        return jsonify({"error": "invalid_theme"}), 400

    execute(
        "UPDATE app.users SET theme = :theme WHERE id = :uid",
        {"theme": theme, "uid": user["id"]},
    )
    user["theme"] = theme
    session["user"] = user
    return jsonify({"status": "ok", "theme": theme})

# =============================================================================
# ROOT
# =============================================================================
@app.route("/")
def root():
    if "user" in session:
        return redirect(url_for("dashboard"))

    row = query_one("SELECT value_text FROM app.config WHERE key_name = 'vci_mode'")
    default_vci = "pcan"
    if row and row.get("value_text") == "socketcan":
        default_vci = "socketcan"

    return render_template(
        "login.html",
        pc_name=STATION_ID,
        default_vci=default_vci,
        user=None
    )

# =============================================================================
# LOGIN
# =============================================================================
@app.route("/login", methods=["POST"])
def login():
    email = (request.form.get("email") or "").strip().lower()
    pin = (request.form.get("pin") or "").strip()
    vci_mode = (request.form.get("vci_mode") or "pcan").lower()

    user = get_user_by_email(email)
    if not user or not verify_pin(user.get("pin"), pin):
        return render_template("login.html", error="Invalid credentials", user=None)

    if not user.get("is_approved"):
        return render_template("login.html", error="Account not approved", user=None)

    if user.get("is_disabled"):
        return render_template("login.html", error="Account disabled", user=None)

    set_config_value("vci_mode", vci_mode)
    can_interface = "PCAN_USBBUS1" if vci_mode == "pcan" else "can0"
    set_config_value("can_interface", can_interface)

    login_user_into_session(user)
    append_global_log(f"User logged in: {email}")
    return redirect(url_for("dashboard"))

# =============================================================================
# LOGOUT
# =============================================================================
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("root"))

# =============================================================================
# FORGOT PIN – STEP 1
# =============================================================================
@app.route("/forgot-pin", methods=["GET", "POST"])
def forgot_pin():
    if request.method == "GET":
        return render_template("reset_pin.html", step="email", user=None)

    email = (request.form.get("email") or "").strip().lower()
    user = query_one("SELECT id FROM app.users WHERE email = :e", {"e": email})
    if not user:
        return render_template("reset_pin.html", step="email", error="Email not found", user=None)

    code = f"{random.randint(0, 999999):06d}"
    execute("UPDATE app.users SET reset_code = :code WHERE id = :id", {"code": code, "id": user["id"]})

    subject = "NIRIX PIN Reset Code"
    body = f"PIN reset requested for: {email}\n\nReset Code: {code}"
    approver = get_config_value("approver_email") or email
    send_email(approver, subject, body)

    return render_template("reset_pin.html", step="verify", email=email, message="Reset code sent to approver.", user=None)

# =============================================================================
# FORGOT PIN – STEP 2
# =============================================================================
@app.route("/reset-pin-verify", methods=["POST"])
def reset_pin_verify():
    email = (request.form.get("email") or "").strip().lower()
    code = (request.form.get("code") or "").strip()

    row = query_one("SELECT reset_code FROM app.users WHERE email = :e", {"e": email})
    if not row or row.get("reset_code") != code:
        return render_template("reset_pin.html", step="verify", email=email, error="Invalid or expired reset code", user=None)

    return render_template("reset_pin.html", step="new_pin", email=email, user=None)

# =============================================================================
# FORGOT PIN – STEP 3
# =============================================================================
@app.route("/reset-pin-new", methods=["POST"])
def reset_pin_new():
    email = (request.form.get("email") or "").strip().lower()
    pin = (request.form.get("pin") or "").strip()
    pin2 = (request.form.get("pin2") or "").strip()

    if pin != pin2:
        return render_template("reset_pin.html", step="new_pin", email=email, error="PINs do not match", user=None)

    hashed = hash_pin(pin)
    execute("UPDATE app.users SET pin = :pin, reset_code = NULL WHERE email = :email", {"pin": hashed, "email": email})
    append_global_log(f"PIN reset completed for {email}")

    return render_template("reset_pin.html", step="email", message="PIN reset successful. Please login.", user=None)

# =============================================================================
# REGISTRATION
# =============================================================================
@app.route("/register", methods=["GET", "POST"])
def register():
    def get_pending_user_id(email: str) -> Optional[int]:
        row = query_one("SELECT id FROM app.users WHERE email = :e", {"e": email})
        return row["id"] if row else None

    if request.method == "GET":
        if request.args.get("reset") == "1":
            session.pop("pending_registration", None)
        return render_template("register.html", pc_name=STATION_ID, user=None, pending=session.get("pending_registration"))

    action = request.form.get("action", "start")
    pending = session.get("pending_registration")

    if action == "start":
        name = (request.form.get("name") or "").strip()
        emp_id = (request.form.get("employee_id") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        pin = (request.form.get("pin") or "").strip()
        pin_confirm = (request.form.get("pin_confirm") or "").strip()

        if not all([name, emp_id, email, pin, pin_confirm]):
            return render_template("register.html", error="All fields are required", pending=None, user=None)

        if not re.fullmatch(r"\d{4}", pin):
            return render_template("register.html", error="PIN must be exactly 4 digits", pending=None, user=None)

        if pin != pin_confirm:
            return render_template("register.html", error="PIN mismatch", pending=None, user=None)

        if email_exists(email):
            return render_template("register.html", error="Email already registered", pending=None, user=None)

        approval_code = f"{random.randint(0, 999999):06d}"
        hashed = hash_pin(pin)

        role_row = query_one("SELECT id FROM app.roles WHERE name = 'technician' LIMIT 1")

        execute("""
            INSERT INTO app.users (
                name, employee_id, email, pin, role_id,
                is_approved, is_disabled, reset_code
            )
            VALUES (
                :name, :emp, :email, :pin, :role,
                FALSE, FALSE, :code
            )
        """, {
            "name": name,
            "emp": emp_id,
            "email": email,
            "pin": hashed,
            "role": role_row["id"] if role_row else None,
            "code": approval_code
        })

        user_id = get_pending_user_id(email)
        pending = {
            "id": user_id,
            "name": name,
            "employee_id": emp_id,
            "email": email,
            "approval_code": approval_code
        }
        session["pending_registration"] = pending

        approver = get_config_value("approver_email") or email
        subject = "Approval Required: NIRIX Registration"
        body = f"""New NIRIX user registration:

Name: {name}
Employee ID: {emp_id}
Email: {email}

Approval Code: {approval_code}
"""
        send_email(approver, subject, body)
        return render_template("register.html", pending=pending, message="Approval request sent.", user=None)

    if action == "verify":
        if not pending:
            return render_template("register.html", error="No pending registration", pending=None, user=None)

        entered = (request.form.get("approval_code") or "").strip()
        row = query_one("SELECT reset_code FROM app.users WHERE email = :e", {"e": pending["email"]})
        if not row or row.get("reset_code") != entered:
            return render_template("register.html", error="Invalid approval code", pending=pending, user=None)

        execute("UPDATE app.users SET is_approved = TRUE, reset_code = NULL WHERE email = :e", {"e": pending["email"]})
        session.pop("pending_registration", None)
        return render_template("login.html", message="Registration approved. You may login.", user=None)

    if action == "resend":
        if not pending:
            return render_template("register.html", error="No pending registration", pending=None, user=None)

        new_code = f"{random.randint(0, 999999):06d}"
        execute("UPDATE app.users SET reset_code = :code WHERE email = :e", {"code": new_code, "e": pending["email"]})

        pending["approval_code"] = new_code
        session["pending_registration"] = pending

        approver = get_config_value("approver_email") or pending["email"]
        send_email(approver, "NIRIX Approval Code Resent", f"New approval code: {new_code}")

        return render_template("register.html", pending=pending, message="Approval code resent.", user=None)

    return render_template("register.html", error="Invalid action", pending=pending, user=None)

# =============================================================================
# DASHBOARD
# =============================================================================
@app.route("/dashboard")
def dashboard():
    require_login()

    user = session["user"]
    role = user["role"]
    user_id = user["id"]

    vin = (request.args.get("vin") or "").strip()
    search = (request.args.get("search") or "").strip()
    category = (request.args.get("category") or "").strip()

    where = ["v.is_active = TRUE"]
    params: Dict[str, Any] = {}

    if vin:
        where.append("v.vin_pattern ILIKE :vin")
        params["vin"] = f"%{vin}%"

    if search:
        where.append("v.name ILIKE :search")
        params["search"] = f"%{search}%"

    if category:
        where.append("v.category = :cat")
        params["cat"] = category

    where_sql = " AND ".join(where)

    if role == ROLE_SUPER_ADMIN:
        vehicles = query_all(f"""
            SELECT id, name, description, vin_pattern, category, image_filename
            FROM app.vehicles v
            WHERE {where_sql}
            ORDER BY v.name
        """, params)
    else:
        vehicles = query_all(f"""
            SELECT v.id, v.name, v.description, v.vin_pattern, v.category, v.image_filename
            FROM app.vehicles v
            JOIN app.user_vehicle_permissions uvp ON uvp.vehicle_id = v.id
            WHERE uvp.user_id = :uid AND {where_sql}
            ORDER BY v.name
        """, dict(params, uid=user_id))

    categories = sorted({v["category"] for v in vehicles if v.get("category")})

    return render_template(
        "dashboard.html",
        user=user,
        vehicles=vehicles,
        categories=categories,
        vin=vin,
        search=search,
        selected_category=category,
    )

# =============================================================================
# TESTS PAGE - LEGACY ROUTE
# =============================================================================
@app.route("/tests/<path:model_name>")
def tests_page(model_name):
    return redirect(url_for("tests_root", model=model_name))

# =============================================================================
# TESTS PAGE (HIERARCHICAL NAVIGATION - MAIN ROUTE)
# =============================================================================
@app.route("/tests")
def tests_root():
    if "user" not in session:
        return redirect(url_for("root"))

    user = session["user"]
    role = user["role"]
    user_id = user["id"]

    model_name = (request.args.get("model") or "").strip()
    section = (request.args.get("section") or "").strip().lower()
    ecu = (request.args.get("ecu") or "").strip()
    parameter = (request.args.get("parameter") or "").strip()

    # FIX-31: VIN display support (passed from auto-run UI)
    auto_run_session_id = (request.args.get("auto_run_session_id") or "").strip()
    current_vin: Optional[str] = None
    current_vin_source: Optional[str] = None

    if auto_run_session_id:
        try:
            row = query_one("""
                SELECT vin, vin_source
                FROM app.auto_run_sessions
                WHERE session_id = :sid
                LIMIT 1
            """, {"sid": auto_run_session_id})
            if row:
                current_vin = row.get("vin")
                current_vin_source = row.get("vin_source")
                app.logger.info(f"Loaded VIN for session {auto_run_session_id}: {current_vin}")
        except Exception as e:
            app.logger.warning(f"Failed to load VIN for session {auto_run_session_id}: {e}")

    # PHASE 0: Vehicle selection
    if not model_name:
        if role == ROLE_SUPER_ADMIN:
            vehicles = query_all("""
                SELECT id, name, description, category, vin_pattern, image_filename
                FROM app.vehicles
                WHERE is_active = TRUE
                ORDER BY name
            """)
        else:
            vehicles = query_all("""
                SELECT v.id, v.name, v.description, v.category, v.vin_pattern, v.image_filename
                FROM app.vehicles v
                JOIN app.user_vehicle_permissions uvp ON uvp.vehicle_id = v.id
                WHERE uvp.user_id = :uid AND v.is_active = TRUE
                ORDER BY v.name
            """, {"uid": user_id})

        return render_template(
            "tests.html",
            user=user,
            model_name=None,
            vehicles=vehicles,
            selected_vehicle=None,
            sections=[],
            section=None,
            ecus=[],
            ecu=None,
            parameters=[],
            health_tabs=[],
            parameter=None,
            tests=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    # Vehicle lookup + RBAC
    vehicle = query_one("""
        SELECT id, name, description, category, vin_pattern, image_filename
        FROM app.vehicles
        WHERE name = :name AND is_active = TRUE
        LIMIT 1
    """, {"name": model_name})

    if not vehicle:
        abort(404, "Vehicle not found")

    if role != ROLE_SUPER_ADMIN:
        if not check_vehicle_permission(user_id, vehicle["id"], role):
            abort(403, "Vehicle access denied")

    # PHASE 1: Section selection
    if not section:
        sections = list_sections_for_vehicle(model_name, user_id, role)
        try:
            vehicle_sections = discover_vehicle_sections(model_name)
        except Exception:
            vehicle_sections = {"diagnostics": False, "vehicle_health": False}

        return render_template(
            "tests.html",
            user=user,
            model_name=model_name,
            selected_vehicle=vehicle,
            vehicle_sections=vehicle_sections,
            sections=sections,
            section=None,
            ecus=[],
            ecu=None,
            parameters=[],
            health_tabs=[],
            parameter=None,
            tests=[],
            vehicles=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    # PHASE 2: ECU selection (Diagnostics)
    if section == "diagnostics" and not ecu:
        ecus = list_ecus_for_vehicle(model_name, user_id, role)
        return render_template(
            "tests.html",
            user=user,
            model_name=model_name,
            selected_vehicle=vehicle,
            section=section,
            ecus=ecus,
            ecu=None,
            parameters=[],
            health_tabs=[],
            parameter=None,
            tests=[],
            vehicles=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    # PHASE 3: Parameter selection (Diagnostics)
    if section == "diagnostics" and ecu and not parameter:
        parameters = list_parameters_for_ecu(model_name, ecu, user_id, role)
        return render_template(
            "tests.html",
            user=user,
            model_name=model_name,
            selected_vehicle=vehicle,
            section=section,
            ecus=[],
            ecu=ecu,
            parameters=parameters,
            health_tabs=[],
            parameter=None,
            tests=[],
            vehicles=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    # PHASE 3b: Health tab selection (Vehicle Health)
    if section == "vehicle_health" and not parameter:
        health_tabs = list_health_tabs_for_vehicle(model_name, user_id, role)
        return render_template(
            "tests.html",
            user=user,
            model_name=model_name,
            selected_vehicle=vehicle,
            section=section,
            ecus=[],
            ecu=None,
            parameters=[],
            health_tabs=health_tabs,
            parameter=None,
            tests=[],
            vehicles=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    # PHASE 4: Test sequence page
    if parameter:
        # IUPR convention: parameter codes are IUPR_PRIMARY / IUPR_SECONDARY
        if section == "diagnostics" and parameter.upper() == "IUPR":
            return redirect(url_for(
                "tests_root",
                model=model_name,
                section=section,
                ecu=ecu,
                parameter="IUPR_PRIMARY",
                auto_run_session_id=auto_run_session_id or None,
            ))

        tests = list_tests_for_parameter(
            vehicle_name=model_name,
            section=section,
            parameter=parameter,
            ecu=ecu if section == "diagnostics" else None,
            user_id=user_id,
            user_role=role,
        )

        return render_template(
            "tests.html",
            user=user,
            model_name=model_name,
            selected_vehicle=vehicle,
            section=section,
            ecus=[],
            ecu=ecu if section == "diagnostics" else None,
            parameters=[],
            health_tabs=[],
            parameter=parameter,
            tests=tests,
            vehicles=[],
            # VIN context
            current_vin=current_vin,
            auto_run_session_id=auto_run_session_id,
        )

    return redirect(url_for("tests_root"))

# =============================================================================
# API: RUN SINGLE TEST (DB AUTHORITATIVE)
# =============================================================================
@app.route("/api/run_test", methods=["POST"])
def api_run_test():
    """
    DB-authoritative execution:
    Payload:
      { "vehicle_name": "...", "test_id": "...", "user_inputs": {...} }

    Backward compatible: ignores extra fields like section/ecu/parameter if provided.
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    user = session["user"]
    payload = request.get_json(silent=True) or {}

    vehicle_name = (payload.get("vehicle_name") or "").strip()
    test_id = str(payload.get("test_id") or "").strip()
    user_inputs = payload.get("user_inputs") or {}

    if not vehicle_name:
        return jsonify({"ok": False, "error": "vehicle_name_required"}), 400

    if not test_id or not TEST_ID_RE.fullmatch(test_id):
        return jsonify({"ok": False, "error": "invalid_test_id"}), 400

    result = run_test(
        user_id=int(user["id"]),
        user_role=str(user["role"]),
        vehicle_name=vehicle_name,
        test_id=test_id,
        user_inputs=user_inputs,
    )

    if result.get("ok"):
        append_global_log(f"Test started: {test_id} task_id={result.get('task_id')}")
        return jsonify(result)

    return jsonify(result), 400

# =============================================================================
# API: RUN ALL TESTS (BATCH EXECUTION) - OPTIONAL / BACKWARD COMPAT
# =============================================================================
@app.route("/api/run_all_tests", methods=["POST"])
def api_run_all_tests():
    """
    Batch execution endpoint (optional; not required by new tests.html).
    Payload:
    {
        "vehicle_name": "...",
        "test_ids": ["...","..."],
        "sequential": true,
        "stop_on_failure": false
    }
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    user = session["user"]
    payload = request.get_json(silent=True) or {}

    vehicle_name = (payload.get("vehicle_name") or "").strip()
    test_ids = payload.get("test_ids") or []
    sequential = bool(payload.get("sequential", True))
    stop_on_failure = bool(payload.get("stop_on_failure", False))

    if not vehicle_name:
        return jsonify({"ok": False, "error": "vehicle_name_required"}), 400
    if not isinstance(test_ids, list) or not test_ids:
        return jsonify({"ok": False, "error": "no_tests_specified"}), 400

    tests_specs = []
    can_cfg = get_can_configuration()

    for tid in test_ids:
        tid = str(tid or "").strip()
        if not tid or not TEST_ID_RE.fullmatch(tid):
            continue

        if user["role"] != ROLE_SUPER_ADMIN:
            if not check_test_permission(int(user["id"]), tid, str(user["role"])):
                continue

        meta = load_test_metadata(tid)
        if not meta:
            continue

        exec_cfg = load_execution_config(tid)
        if not exec_cfg.get("supports_run_all", True):
            continue

        try:
            fn = load_test_function(
                vehicle_name,
                section=meta.get("section"),
                ecu=meta.get("ecu"),
                parameter=meta.get("parameter"),
                module_name=meta.get("module_name"),
                function_name=meta.get("function_name"),
            )
        except Exception as e:
            app.logger.warning(f"Batch: failed to load function for {tid}: {e}")
            continue

        limits = load_output_limits(tid)

        ctx = ExecutionContext(
            test_id=tid,
            execution_mode=exec_cfg.get("execution_mode", "single"),
            timeout_sec=exec_cfg.get("timeout_sec", 15),
            max_retries=exec_cfg.get("max_retries", 3),
            retry_delay_sec=exec_cfg.get("retry_delay_sec", 1.5),
            output_limits=limits,
            supports_run_all=True,
        )

        args = [can_cfg["can_interface"], can_cfg["bitrate"]]
        tests_specs.append({"fn": fn, "args": args, "ctx": ctx})

    if not tests_specs:
        return jsonify({"ok": False, "error": "no_valid_tests_to_execute"}), 400

    try:
        result = execute_batch_async(
            tests=tests_specs,
            sequential=sequential,
            stop_on_failure=stop_on_failure,
            progress_cb=_ui_progress_callback,
        )
        append_global_log(f"Batch started: count={len(tests_specs)} batch_id={result['batch_id']}")
        return jsonify({"ok": True, "batch_id": result["batch_id"], "test_count": len(tests_specs)})
    except Exception as e:
        app.logger.exception("Failed to start batch execution")
        return jsonify({"ok": False, "error": str(e)}), 500

# =============================================================================
# API: RUN AUTO-RUN PROGRAMS (SECTION ENTRY) - NEW AUTO-RUN SESSIONS
# =============================================================================
@app.route("/api/run_auto_programs", methods=["POST"])
def api_run_auto_programs():
    """
    Start auto-run programs for a section using the new auto-run session system.

    Payload:
      { "vehicle_name": "...", "section": "diagnostics"|"vehicle_health" }

    Returns (adapted for tests.html auto-run UI):
      {
        "ok": true,
        "session_id": "...",
        "programs": [
          {
            "program_id": "...",
            "program_name": "...",
            "program_type": "single"|"stream",
            ...
          }, ...
        ],
        "skipped": [...]
      }
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    user = session["user"]
    payload = request.get_json(silent=True) or {}

    vehicle_name = (payload.get("vehicle_name") or "").strip()
    section = (payload.get("section") or "").strip().lower()

    if not vehicle_name or section not in ("diagnostics", "vehicle_health"):
        return jsonify({"ok": False, "error": "invalid_parameters"}), 400

    result = start_auto_run(
        user_id=int(user["id"]),
        user_role=str(user["role"]),
        vehicle_name=vehicle_name,
        section_type=section,
    )

    if not result.get("ok"):
        # Always return ok:false with error string for fail-closed UI logic
        return jsonify({"ok": False, "error": result.get("error", "AUTO_RUN_FAILED"), "details": result}), 400

    programs = result.get("programs") or []
    task_ids = result.get("task_ids") or {}

    adapted = []
    for p in programs:
        pid = p.get("program_id")
        p2 = dict(p)
        p2["task_id"] = task_ids.get(pid)
        if "required" not in p2:
            p2["required"] = bool(p2.get("is_required", True))
        adapted.append(p2)

    return jsonify({
        "ok": True,
        "session_id": result.get("session_id"),
        "programs": adapted,
        "skipped": result.get("skipped", []),
    })

# =============================================================================
# API: AUTO-RUN MANUAL VIN SUBMISSION (tests.html expects this)
# =============================================================================
@app.route("/api/auto_run/vin", methods=["POST"])
def api_auto_run_vin():
    """
    Payload:
      { "session_id": "...", "program_id": "...", "vin": "17CHARS" }

    Returns:
      { "ok": true, "vin": "...", "vin_source": "manual" }  OR  { "ok": false, "error": ... }
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    payload = request.get_json(silent=True) or {}
    session_id = (payload.get("session_id") or "").strip()
    program_id = (payload.get("program_id") or "").strip()
    vin = (payload.get("vin") or payload.get("vin_value") or "").strip().upper()

    if not session_id or not program_id:
        return jsonify({"ok": False, "error": "session_id_and_program_id_required"}), 400

    if not VIN_RE.fullmatch(vin):
        return jsonify({"ok": False, "error": "invalid_vin"}), 400

    if any(c in vin for c in ("I", "O", "Q")):
        return jsonify({"ok": False, "error": "vin_contains_invalid_chars"}), 400

    try:
        return jsonify(submit_auto_run_vin(session_id, program_id, vin))
    except Exception as e:
        app.logger.exception("Manual VIN submission failed")
        return jsonify({"ok": False, "error": str(e)}), 500

# =============================================================================
# API: AUTO-RUN SESSION STATUS (OPTIONAL)
# =============================================================================
@app.get("/api/auto_run/<session_id>/status")
def api_auto_run_status(session_id: str):
    """
    Return auto-run session status in the shape expected by tests.html.

    Service is authoritative for live state (task_ids, results, blocking_programs,
    live_task_statuses). We optionally include DB snapshot under `db_session`.
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    # 1) Live status from service (what the UI expects)
    try:
        live = svc_get_auto_run_status(session_id)
        if not live or live.get("ok") is not True:
            return jsonify(live or {"ok": False, "error": "not_found"}), 404
    except Exception as e:
        app.logger.exception("Failed to get auto-run live status")
        return jsonify({"ok": False, "error": str(e)}), 500

    # 2) Optional DB snapshot (VIN persistence, timestamps, etc.)
    try:
        row = query_one("""
            SELECT session_id, vehicle_id, vehicle_name, section_type,
                   status, vin, vin_source, started_at, ended_at
            FROM app.auto_run_sessions
            WHERE session_id = :sid
            LIMIT 1
        """, {"sid": session_id})
        if row:
            live["db_session"] = dict(row)
    except Exception as e:
        # Don't fail the endpoint if DB snapshot fails
        app.logger.warning(f"Failed to load DB auto_run_session snapshot: {e}")

    return jsonify(live)

# =============================================================================
# FIX-30: API: AUTO-RUN STREAM VALUES (FOR LIVE DISPLAY)
# =============================================================================
@app.get("/api/auto_run/<session_id>/stream_values")
def api_get_stream_values(session_id: str):
    """
    Return the latest stream values for a session.
    Used by UI to show live battery voltage, etc.
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401
    
    try:
        # FIX-32: Enhanced error handling and logging
        app.logger.debug(f"Fetching stream values for session {session_id}")
        
        # Fetch latest values for this session
        rows = query_all("""
            SELECT program_id, signal_name, signal_value, signal_unit, 
                   lsl, usl, is_within_limit, updated_at
            FROM app.auto_run_stream_values
            WHERE session_id = :sid
            ORDER BY signal_name
        """, {"sid": session_id})
        
        values = [dict(r) for r in rows] if rows else []
        app.logger.debug(f"Found {len(values)} stream values for session {session_id}")
        
        return jsonify({
            "ok": True, 
            "values": values,
            "session_id": session_id,
            "count": len(values)
        })
    except Exception as e:
        app.logger.error(f"Failed to fetch stream values for session {session_id}: {e}")
        app.logger.error(traceback.format_exc())
        return jsonify({
            "ok": False, 
            "error": str(e),
            "session_id": session_id
        }), 500

# =============================================================================
# API: ECU ACTIVE STATUS
# =============================================================================
@app.get("/api/auto_run/<session_id>/ecu_status")
def api_get_ecu_status(session_id: str):
    """
    Return ECU active status for a session.
    Used by UI to show green/red dots for ECU connectivity.
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401
        
    try:
        rows = query_all("""
            SELECT ecu_code, is_active, error_count, last_response
            FROM app.ecu_active_status
            WHERE session_id = :sid
        """, {"sid": session_id})
        
        return jsonify({
            "ok": True,
            "status": [dict(r) for r in rows] if rows else []
        })
    except Exception as e:
        app.logger.error(f"Failed to fetch ECU status for session {session_id}: {e}")
        return jsonify({"ok": False, "error": str(e)}), 500

# =============================================================================
# API: SCAN (OpenCV station camera) - start/status/cancel + optional frame
# =============================================================================
@app.post("/api/scan/start")
def api_scan_start():
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    if not SCANNER_AVAILABLE or start_scan is None:
        return jsonify({"ok": False, "error": "scanner_not_available"}), 503

    payload = request.get_json(silent=True) or {}
    kind = (payload.get("kind") or "text").strip().lower()
    timeout_sec = payload.get("timeout_sec")

    if kind not in ("text", "hex", "vin"):
        kind = "text"

    try:
        sess = start_scan(kind=kind, timeout_sec=timeout_sec)
        return jsonify({
            "ok": True,
            "scan_id": sess.scan_id,
            "preview_supported": bool(get_scan_frame_jpeg is not None),
        })
    except Exception as e:
        app.logger.exception("Scan start failed")
        return jsonify({"ok": False, "error": str(e)}), 500

@app.get("/api/scan/<scan_id>/status")
def api_scan_status(scan_id: str):
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    if not SCANNER_AVAILABLE or get_scan is None:
        return jsonify({"ok": False, "error": "scanner_not_available"}), 503

    s = get_scan(scan_id)
    if not s:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({
        "ok": True,
        "status": s.status,
        "value": s.value,
        "error": s.error,
        "has_preview": bool(getattr(s, "last_frame_jpeg", None)),
    })

@app.post("/api/scan/<scan_id>/cancel")
def api_scan_cancel(scan_id: str):
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    if not SCANNER_AVAILABLE or cancel_scan is None:
        return jsonify({"ok": False, "error": "scanner_not_available"}), 503

    try:
        ok = cancel_scan(scan_id)
        return jsonify({"ok": bool(ok)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

# OPTIONAL: backend preview frame (scanner.py must support get_scan_frame_jpeg)
@app.get("/api/scan/<scan_id>/frame")
def api_scan_frame(scan_id: str):
    if "user" not in session:
        return ("", 401)

    if not SCANNER_AVAILABLE or get_scan_frame_jpeg is None:
        return ("", 404)

    try:
        jpg = get_scan_frame_jpeg(scan_id)
        if not jpg:
            return ("", 204)
        return Response(jpg, mimetype="image/jpeg", headers={"Cache-Control": "no-store"})
    except Exception:
        return ("", 204)

# =============================================================================
# API: RUN ALL FOR PARAMETER (SERVICE CONVENIENCE)
# =============================================================================
@app.route("/api/run_all_for_parameter", methods=["POST"])
def api_run_all_for_parameter():
    """
    Payload:
      { vehicle_name, section, parameter, ecu? }
    """
    if "user" not in session:
        return jsonify({"ok": False, "error": "not_authenticated"}), 401

    user = session["user"]
    payload = request.get_json(silent=True) or {}

    vehicle_name = (payload.get("vehicle_name") or "").strip()
    section = (payload.get("section") or "").strip().lower()
    parameter = (payload.get("parameter") or "").strip()
    ecu = (payload.get("ecu") or "").strip() if section == "diagnostics" else None

    if not vehicle_name or section not in ("diagnostics", "vehicle_health") or not parameter:
        return jsonify({"ok": False, "error": "invalid_parameters"}), 400

    result = run_all_tests_for_parameter(
        vehicle_name=vehicle_name,
        section=section,
        parameter=parameter,
        ecu=ecu,
        user_id=int(user["id"]),
        user_role=str(user["role"]),
    )
    return jsonify(result)

# =============================================================================
# API: TASK STATUS / CANCEL / PAUSE / RESUME / LOGS
# =============================================================================
@app.route("/api/task/<task_id>/status")
def api_task_status(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    return jsonify(get_task_status(task_id))

@app.route("/api/task/<task_id>/cancel", methods=["POST"])
def api_task_cancel(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    ok = cancel_task(task_id)
    if ok:
        append_global_log(f"Task cancelled: {task_id}", "WARN")
    return jsonify({"ok": bool(ok)})

@app.route("/api/task/<task_id>/pause", methods=["POST"])
def api_task_pause(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    ok = pause_task(task_id)
    return jsonify({"ok": bool(ok)})

@app.route("/api/task/<task_id>/resume", methods=["POST"])
def api_task_resume(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    ok = resume_task(task_id)
    return jsonify({"ok": bool(ok)})

@app.route("/api/task/<task_id>/logs")
def api_task_logs(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401

    from_index = request.args.get("from_index", type=int)
    if from_index is not None:
        new_lines, total = get_new_task_logs(task_id, from_index)
        return jsonify({
            "log_lines": new_lines,
            "total_lines": total,
            "from_index": from_index,
        })

    return jsonify({"log_text": get_task_logs(task_id) or ""})

@app.route("/api/task/<task_id>/logs/clear", methods=["POST"])
def api_task_logs_clear(task_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    clear_task_logs(task_id)
    return jsonify({"ok": True})

# =============================================================================
# API: BATCH STATUS / CANCEL
# =============================================================================
@app.route("/api/batch/<batch_id>/status")
def api_batch_status(batch_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    return jsonify(get_batch_status(batch_id))

@app.route("/api/batch/<batch_id>/cancel", methods=["POST"])
def api_batch_cancel(batch_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    ok = cancel_batch(batch_id)
    if ok:
        append_global_log(f"Batch cancelled: {batch_id}", "WARN")
    return jsonify({"ok": bool(ok)})

# =============================================================================
# API: GET TESTS FOR PARAMETER (LISTING)
# =============================================================================
@app.route("/api/tests")
def api_get_tests():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401

    user = session["user"]

    vehicle_name = (request.args.get("vehicle_name") or "").strip()
    section = (request.args.get("section") or "").strip().lower()
    parameter = (request.args.get("parameter") or "").strip()
    ecu = (request.args.get("ecu") or "").strip() if section == "diagnostics" else None

    if not vehicle_name or section not in ("diagnostics", "vehicle_health") or not parameter:
        return jsonify({"error": "invalid_parameters"}), 400

    tests = list_tests_for_parameter(
        vehicle_name=vehicle_name,
        section=section,
        parameter=parameter,
        ecu=ecu,
        user_id=int(user["id"]),
        user_role=str(user["role"]),
    )
    return jsonify({"tests": tests})

# =============================================================================
# API: GET SECTIONS / ECUS / PARAMETERS / HEALTH TABS
# =============================================================================
@app.route("/api/sections/<vehicle_name>")
def api_get_sections(vehicle_name):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    user = session["user"]
    sections = list_sections_for_vehicle(vehicle_name, int(user["id"]), str(user["role"]))
    return jsonify({"sections": sections})

@app.route("/api/ecus/<vehicle_name>")
def api_get_ecus(vehicle_name):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    user = session["user"]
    ecus = list_ecus_for_vehicle(vehicle_name, int(user["id"]), str(user["role"]))
    return jsonify({"ecus": ecus})

@app.route("/api/parameters/<vehicle_name>/<ecu_code>")
def api_get_parameters(vehicle_name, ecu_code):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    user = session["user"]
    parameters = list_parameters_for_ecu(vehicle_name, ecu_code, int(user["id"]), str(user["role"]))
    return jsonify({"parameters": parameters})

@app.route("/api/health_tabs/<vehicle_name>")
def api_get_health_tabs(vehicle_name):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    user = session["user"]
    tabs = list_health_tabs_for_vehicle(vehicle_name, int(user["id"]), str(user["role"]))
    return jsonify({"health_tabs": tabs})

# =============================================================================
# API: TEST EXECUTION HISTORY
# =============================================================================
@app.route("/api/test/<test_id>/history")
def api_get_test_history(test_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    limit = request.args.get("limit", 50, type=int)
    history = get_test_execution_history(test_id, limit)
    return jsonify({"history": history})

# =============================================================================
# API: RUNNER STATS
# =============================================================================
@app.route("/api/runner/stats")
def api_runner_stats():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    return jsonify(get_runner_stats())

# =============================================================================
# SAVE TEST LOG (FILESYSTEM + DB)
# =============================================================================
@app.route("/api/log/save_test", methods=["POST"])
def api_save_test_log():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401

    data = request.get_json(silent=True) or {}

    vehicle_name = (data.get("vehicle_name") or "").strip()
    vin = (data.get("vin") or "").strip().upper()
    log_text = data.get("log_text") or ""
    test_id = data.get("test_id")
    task_id = data.get("task_id")

    if not vehicle_name or not vin or not log_text:
        return jsonify({"error": "missing_fields"}), 400
    if not VIN_RE.fullmatch(vin):
        return jsonify({"error": "invalid_vin"}), 400

    vehicle_dir = os.path.join(TEST_LOGS_DIR, _sanitize_filename_part(vehicle_name))
    os.makedirs(vehicle_dir, exist_ok=True)

    timestamp = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{vin}_{timestamp}.txt"
    filepath = os.path.join(vehicle_dir, filename)

    header = f"""# NIRIX Test Log
# Vehicle: {vehicle_name}
# VIN: {vin}
# Test ID: {test_id or 'N/A'}
# Task ID: {task_id or 'N/A'}
# Timestamp: {datetime.datetime.utcnow().isoformat()}
# User: {session['user']['email']}
# Station: {STATION_ID}
# ----------------------------------------

"""

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(header + log_text)

    execute("""
        INSERT INTO app.logs (filename, vehicle_name, vin, user_id, created_at)
        VALUES (:fn, :veh, :vin, :uid, NOW())
    """, {
        "fn": filename,
        "veh": vehicle_name,
        "vin": vin,
        "uid": session["user"]["id"],
    })

    append_global_log(f"Test log saved: {filename}")
    return jsonify({"status": "ok", "filename": filename})

# =============================================================================
# LOGS PAGE
# =============================================================================
@app.route("/logs")
def logs_page():
    if "user" not in session:
        return redirect(url_for("root"))

    user = session["user"]
    role = user["role"]
    user_id = user["id"]

    q = (request.args.get("q") or "").lower()
    vin = (request.args.get("vin") or "").strip()
    vehicle_name = (request.args.get("vehicle_name") or "").strip()

    page = max(1, int(request.args.get("page", 1)))
    per_page = 25
    offset = (page - 1) * per_page

    where: List[str] = []
    params: Dict[str, Any] = {}

    if role != ROLE_SUPER_ADMIN:
        where.append("user_id = :uid")
        params["uid"] = user_id

    if q:
        where.append("(LOWER(filename) LIKE :q OR LOWER(vehicle_name) LIKE :q OR LOWER(vin) LIKE :q)")
        params["q"] = f"%{q}%"

    if vin:
        where.append("vin = :vin")
        params["vin"] = vin

    if vehicle_name:
        where.append("vehicle_name = :veh")
        params["veh"] = vehicle_name

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    total_row = query_one(f"SELECT COUNT(*) AS cnt FROM app.logs {where_sql}", params)
    total = total_row["cnt"] if total_row else 0
    total_pages = max(1, (total + per_page - 1) // per_page)

    logs = query_all(f"""
        SELECT id, filename, vehicle_name, vin, user_id, created_at
        FROM app.logs {where_sql}
        ORDER BY created_at DESC
        LIMIT :limit OFFSET :offset
    """, {**params, "limit": per_page, "offset": offset})

    vins = query_all("SELECT DISTINCT vin FROM app.logs WHERE vin IS NOT NULL ORDER BY vin")
    vehicle_names = query_all("SELECT DISTINCT vehicle_name FROM app.logs WHERE vehicle_name IS NOT NULL ORDER BY vehicle_name")

    return render_template(
        "logs.html",
        user=user,
        logs=logs,
        vins=[v["vin"] for v in vins],
        vehicle_names=[m["vehicle_name"] for m in vehicle_names],
        q=q,
        vin_filter=vin,
        vehicle_name_filter=vehicle_name,
        page=page,
        total=total,
        total_pages=total_pages,
    )

# =============================================================================
# LOG VIEW
# =============================================================================
@app.route("/logs/view/<int:log_id>")
def logs_view(log_id):
    if "user" not in session:
        return jsonify({"error": "auth"}), 401

    user = session["user"]
    role = user["role"]

    if role == ROLE_SUPER_ADMIN:
        row = query_one("SELECT filename, vehicle_name FROM app.logs WHERE id = :id", {"id": log_id})
    else:
        row = query_one("""
            SELECT filename, vehicle_name FROM app.logs
            WHERE id = :id AND user_id = :uid
        """, {"id": log_id, "uid": user["id"]})

    if not row:
        return jsonify({"error": "not_found"}), 404

    log_path_1 = os.path.join(TEST_LOGS_DIR, _sanitize_filename_part(row["vehicle_name"]), row["filename"])
    log_path_2 = os.path.join(TEST_LOGS_DIR, row["vehicle_name"], row["filename"])

    log_path = log_path_1 if os.path.isfile(log_path_1) else log_path_2
    if not os.path.isfile(log_path):
        return jsonify({"error": "file_missing"}), 404

    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    return jsonify({"filename": row["filename"], "content": content})

# =============================================================================
# LOG DOWNLOAD
# =============================================================================
@app.route("/logs/download/<int:log_id>")
def logs_download(log_id):
    if "user" not in session:
        abort(401)

    user = session["user"]
    role = user["role"]

    if role == ROLE_SUPER_ADMIN:
        row = query_one("SELECT filename, vehicle_name FROM app.logs WHERE id = :id", {"id": log_id})
    else:
        row = query_one("""
            SELECT filename, vehicle_name FROM app.logs
            WHERE id = :id AND user_id = :uid
        """, {"id": log_id, "uid": user["id"]})

    if not row:
        abort(404)

    log_dir_1 = os.path.join(TEST_LOGS_DIR, _sanitize_filename_part(row["vehicle_name"]))
    log_dir_2 = os.path.join(TEST_LOGS_DIR, row["vehicle_name"])
    log_dir = log_dir_1 if os.path.isdir(log_dir_1) else log_dir_2

    return send_from_directory(log_dir, row["filename"], as_attachment=True)

# =============================================================================
# DOWNLOADS PAGE
# =============================================================================
@app.route("/downloads")
def downloads_page():
    if "user" not in session:
        return redirect(url_for("root"))

    driver_links = {
        "pcan": {
            "name": "PCAN",
            "vendor": "Peak Systems",
            "description": "PCAN USB driver for Windows/Linux",
            "url": "https://www.peak-system.com/Drivers.523.0.html?&L=1",
        },
        "kvaser": {
            "name": "Kvaser",
            "vendor": "Kvaser",
            "description": "Kvaser CAN driver",
            "url": "https://www.kvaser.com/downloads",
        },
        "vector": {
            "name": "Vector",
            "vendor": "Vector",
            "description": "Vector CAN driver (XL Driver Library)",
            "url": "https://vector.com/downloads",
        },
    }

    return render_template(
        "downloads.html",
        user=session["user"],
        driver_links=driver_links,
    )

# =============================================================================
# VEHICLE IMAGE SERVE
# =============================================================================
@app.route("/vehicle_image/<path:filename>")
def vehicle_image(filename):
    if not filename:
        abort(404)

    file_path = os.path.join(VEHICLE_IMAGE_FOLDER, filename)
    if not os.path.isfile(file_path):
        abort(404)

    return send_from_directory(VEHICLE_IMAGE_FOLDER, filename)

# =============================================================================
# GLOBAL LIVE LOG BUFFER APIs
# =============================================================================
@app.route("/api/live_logs")
def api_live_logs():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    with LOG_BUFFER_LOCK:
        text = "\n".join(LOG_BUFFER)
    return jsonify({"log_text": text})

@app.route("/api/append_log", methods=["POST"])
def api_append_log():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    data = request.get_json(silent=True) or {}
    append_global_log(data.get("text", ""), data.get("level", "INFO"))
    return jsonify({"status": "ok"})

@app.route("/api/clear_logs", methods=["POST"])
def api_clear_logs():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401
    with LOG_BUFFER_LOCK:
        LOG_BUFFER.clear()
    return jsonify({"status": "ok"})

# =============================================================================
# HEALTH CHECK ENDPOINT
# =============================================================================
@app.route("/health")
def health_check():
    stats = get_runner_stats()
    return jsonify({
        "status": "ok",
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "station": STATION_ID,
        "active_tasks": stats.get("tasks", {}).get("active", 0),
        "scanner_available": SCANNER_AVAILABLE,
        "version": "3.1.0",
    })

# =============================================================================
# ADMIN: SYNC TESTS (SUPER ADMIN ONLY)
# =============================================================================
@app.route("/api/admin/sync_tests", methods=["POST"])
def api_sync_tests():
    if "user" not in session:
        return jsonify({"error": "auth"}), 401

    user = session["user"]
    if user["role"] != ROLE_SUPER_ADMIN:
        return jsonify({"error": "admin_required"}), 403

    try:
        result = sync_tests_from_filesystem()
        append_global_log(f"Manual test sync completed: {result}")
        return jsonify({"ok": True, "result": result})
    except Exception as e:
        app.logger.exception("Test sync failed")
        return jsonify({"ok": False, "error": str(e)}), 500

# =============================================================================
# ERROR HANDLERS
# =============================================================================
@app.route("/favicon.ico")
def favicon():
    return "", 204

@app.errorhandler(401)
def error_401(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "unauthorized"}), 401
    return redirect(url_for("root"))

@app.errorhandler(403)
def error_403(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "forbidden"}), 403
    return render_template("error.html", error="Access Denied", code=403), 403

@app.errorhandler(404)
def error_404(e):
    if request.path.startswith("/api/"):
        return jsonify({"error": "not_found"}), 404
    return render_template("error.html", error="Page Not Found", code=404), 404

@app.errorhandler(500)
def error_500(e):
    app.logger.exception("Internal server error")
    if request.path.startswith("/api/"):
        return jsonify({"error": "internal_server_error"}), 500
    return render_template("error.html", error="Internal Server Error", code=500), 500

# =============================================================================
# APPLICATION ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    audit_routes(app)

    # Open browser (optional)
    try:
        import webbrowser
        webbrowser.open("http://127.0.0.1:8000")
    except Exception:
        pass

    print("\n" + "=" * 60)
    print("NIRIX Diagnostics v3.1.0")
    print("URL: http://0.0.0.0:8000")
    print(f"Station ID: {STATION_ID}")
    print(f"Active Tasks: {get_active_task_count()}")
    print(f"Scanner Available: {SCANNER_AVAILABLE}")
    print("=" * 60 + "\n")

    app.run(
        host="0.0.0.0",
        port=8000,
        debug=False,
        use_reloader=False
    )