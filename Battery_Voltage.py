# -*- coding: utf-8 -*-
"""
Battery Voltage – PRODUCTION (Self-Sufficient)

Auto-run streaming entry point:
  module_name: battery_voltage
  function_name: read_battery_voltage_stream
  program_type: "stream"
  execution_mode: "stream"

Single-shot entry point:
  module_name: battery_voltage
  function_name: read_battery_voltage
  program_type: "single"
  execution_mode: "single"

UDS DID: 22 E1 42  (Battery Voltage)
CAN IDs: 0x7F0 (req), 0x7F1 (resp)

Version: 2.0.0
Last Updated: 2026-02-14

FIXES:
- Added proper BusABC import
- Enhanced error handling for CAN bus initialization
- Added GeneratorExit handling
- Improved logging for debugging
"""

from __future__ import annotations

import time
import logging
from typing import Dict, Any, Optional, Generator

import can
from can import BusABC  # FIXED: Added missing import

# FIX: Do not import from can_utils. This prevents ImportError crashes on load.
# from diagnostics.can_utils import open_can_bus, close_can_bus

logging.getLogger("can").setLevel(logging.ERROR)


def _open_bus(can_interface: str, bitrate: int) -> Optional[can.Bus]:
    """
    Local helper to open CAN bus (matches pattern in ecu_active_check/vin_read).
    Returns None if bus cannot be opened.
    """
    iface = (can_interface or "").strip()
    try:
        if iface.upper().startswith("PCAN"):
            return can.Bus(interface="pcan", channel=iface, bitrate=int(bitrate), fd=False)
        if iface.lower().startswith("can"):
            return can.Bus(interface="socketcan", channel=iface, bitrate=int(bitrate))
        raise ValueError(f"Unsupported CAN interface: {iface}")
    except Exception as e:
        print(f"[BATTERY] Failed to open CAN bus: {e}")
        return None


def _serialize_can_message(msg: can.Message) -> Dict[str, Any]:
    return {
        "arbitration_id": f"{msg.arbitration_id:03X}",
        "is_extended_id": bool(msg.is_extended_id),
        "dlc": int(msg.dlc),
        "data": [f"{b:02X}" for b in msg.data],
        "timestamp": float(getattr(msg, "timestamp", 0.0) or 0.0),
    }


def _read_voltage_once(bus: BusABC, *, context=None, progress=None) -> Dict[str, Any]:
    """
    Send UDS ReadDataByIdentifier (22 E1 42) once and parse response.
    Returns dict with battery_voltage, message, raw.
    Raises TimeoutError on no response.
    """
    def log(msg: str, level: str = "INFO"):
        if context:
            context.log(msg, level)
        else:
            print(f"[{level}] {msg}")

    if context:
        context.checkpoint()
        context.progress(10, "Sending UDS request (22 E1 42)")
    if progress:
        progress(10, "Sending UDS request (22 E1 42)")

    req = can.Message(
        arbitration_id=0x7F0,
        is_extended_id=False,
        data=[0x03, 0x22, 0xE1, 0x42, 0x00, 0x00, 0x00, 0x00],
    )
    log(f"Tx {req.arbitration_id:03X} " + " ".join(f"{b:02X}" for b in req.data))
    
    try:
        bus.send(req)
    except Exception as e:
        log(f"Failed to send CAN message: {e}", "ERROR")
        raise

    if context:
        context.progress(35, "Waiting for ECU response")

    deadline = time.time() + 2.0
    resp = None
    while time.time() < deadline:
        if context:
            context.checkpoint()

        try:
            msg = bus.recv(timeout=0.3)
        except Exception as e:
            log(f"Error receiving CAN message: {e}", "ERROR")
            continue
            
        if not msg:
            continue
        if msg.arbitration_id != 0x7F1:
            continue

        log(f"Rx {msg.arbitration_id:03X} " + " ".join(f"{b:02X}" for b in msg.data))
        resp = msg
        break

    if not resp:
        raise TimeoutError("No response from ECU for DID E1 42")

    # Positive response format: 62 E1 42 XX ...
    if not (len(resp.data) >= 5 and resp.data[1] == 0x62 and resp.data[2] == 0xE1 and resp.data[3] == 0x42):
        return {
            "battery_voltage": None,
            "message": "Invalid response",
            "raw": {"request": _serialize_can_message(req), "response": _serialize_can_message(resp)},
        }

    voltage = resp.data[4] * 0.1  # 0.1 V resolution
    return {
        "battery_voltage": float(voltage),
        "message": f"{voltage:.1f} V",
        "raw": {"request": _serialize_can_message(req), "response": _serialize_can_message(resp)},
    }


def read_battery_voltage(
    can_interface: str,
    bitrate: int,
    context=None,
    progress=None,
) -> Dict[str, Any]:
    """
    SINGLE-SHOT entry point.
    Returns a dict: {"battery_voltage": float|None, "message": str, "raw": {...}}
    """
    bus = None
    try:
        if context:
            context.checkpoint()
            context.progress(5, "Opening CAN bus")
        if progress:
            progress(5, "Opening CAN bus")

        bus = _open_bus(can_interface, int(bitrate))
        if bus is None:
            error_msg = f"Failed to open CAN bus: {can_interface}"
            if context:
                context.log(error_msg, "ERROR")
            return {
                "battery_voltage": None,
                "message": error_msg,
                "raw": None,
            }

        result = _read_voltage_once(bus, context=context, progress=progress)

        if context:
            if result.get("battery_voltage") is not None:
                context.progress(100, f"Battery Voltage: {result['battery_voltage']:.1f} V")
                context.progress_json({"battery_voltage": result["battery_voltage"]})
            else:
                context.progress(100, "Battery Voltage read: invalid response")

        return result

    except Exception as e:
        error_msg = f"Battery voltage read failed: {e}"
        if context:
            context.log(error_msg, "ERROR")
        return {
            "battery_voltage": None,
            "message": error_msg,
            "raw": None,
        }
    finally:
        if bus:
            try:
                bus.shutdown()
            except Exception:
                pass


def read_battery_voltage_stream(
    can_interface: str,
    bitrate: int,
    context=None,
    progress=None,
) -> Generator[Dict[str, Any], None, None]:
    """
    STREAMING entry point (GENERATOR) for auto-run.

    Yields runner-compatible dicts:
      {"status": "streaming", "data": {"battery_voltage": <float>}}

    runner.py will:
      - validate limits & emit progress_json,
      - call service.on_stream_data(...) with this dict,
      - persist per-signal values.

    Cancellation/pause are honored via context.checkpoint().
    """
    bus = None
    iteration = 0
    
    try:
        if context:
            context.checkpoint()
            context.progress(5, "Opening CAN bus")
            context.log(f"BATTERY STREAM: Starting with {can_interface} @ {bitrate} bps", "INFO")
        if progress:
            progress(5, "Opening CAN bus")

        bus = _open_bus(can_interface, int(bitrate))
        if bus is None:
            error_msg = f"Failed to open CAN bus: {can_interface}"
            if context:
                context.log(error_msg, "ERROR")
            yield {
                "status": "error", 
                "data": {"error": error_msg}
            }
            return

        if context:
            context.log("BATTERY STREAM: CAN bus opened successfully", "INFO")

        # Loop forever (runner will cancel or service will stop session)
        while True:
            iteration += 1
            if context:
                context.checkpoint()
                if iteration % 10 == 0:  # Log every 10 iterations
                    context.log(f"BATTERY STREAM: Iteration {iteration}", "DEBUG")

            try:
                single = _read_voltage_once(bus, context=context, progress=progress)
            except TimeoutError:
                # On timeout in stream: just indicate "no data" / skip this tick
                if context:
                    context.progress(60, "No response (retrying)")
                if progress:
                    progress(60, "No response (retrying)")
                time.sleep(0.3)
                continue
            except Exception as e:
                # Unexpected error
                if context:
                    context.log(f"BATTERY STREAM: Unexpected error: {e}", "ERROR")
                yield {
                    "status": "error",
                    "data": {"error": str(e)}
                }
                time.sleep(1.0)  # Back off on error
                continue

            value = single.get("battery_voltage")
            if value is not None:
                if context:
                    context.progress(100, f"Battery Voltage: {value:.1f} V")
                    if iteration % 5 == 0:  # Log every 5 successful reads
                        context.log(f"BATTERY STREAM: Read {value:.1f}V (iteration {iteration})", "INFO")
                if progress:
                    progress(100, f"Battery Voltage: {value:.1f} V")

                # YIELD for runner → service.on_stream_data → DB persist
                yield {
                    "status": "streaming", 
                    "data": {"battery_voltage": value}
                }
            else:
                # Invalid frame
                if context:
                    context.log("BATTERY STREAM: Invalid response received", "WARN")
                yield {
                    "status": "streaming", 
                    "data": {}
                }

            # Pace (runner timeout for streams defaults to 0 = no timeout)
            if context:
                context.sleep(0.4)
            else:
                time.sleep(0.4)

    except GeneratorExit:
        # Handle generator cleanup gracefully
        if context:
            context.log("BATTERY STREAM: Generator closed", "INFO")
    except Exception as e:
        if context:
            context.log(f"BATTERY STREAM: Fatal error: {e}", "ERROR")
        yield {
            "status": "error", 
            "data": {"error": str(e)}
        }
    finally:
        if context:
            context.log("BATTERY STREAM: Shutting down", "INFO")
        if bus:
            try:
                bus.shutdown()
                if context:
                    context.log("BATTERY STREAM: CAN bus closed", "INFO")
            except Exception as e:
                if context:
                    context.log(f"BATTERY STREAM: Error closing bus: {e}", "ERROR")