#!/usr/bin/env python3
"""
Minimal GNSS/RTK WebSocket server that streams $GxGGA positions.

- Reads NMEA sentences from a USB/COM serial port.
- Filters and parses $GxGGA (e.g., $GPGGA, $GNGGA, ...).
- Broadcasts decoded position updates to all connected WebSocket clients.
- JSON payload example:
  {
    "type": "gga",
    "lat": 48.856614,
    "lon": 2.3522219,
    "alt_m": 35.1,
    "fix_quality": 4,
    "num_sats": 18,
    "hdop": 0.7,
    "timestamp_utc": "12:34:56"
  }

Dependencies:
    pip install websockets pyserial

Run:
    python gnss_ws_server.py --port auto --baud 115200 --host 0.0.0.0 --ws-port 8765
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import signal
import sys
import threading
from typing import Any, Dict, Optional, Set

import serial
from serial import Serial, SerialException
from serial.tools import list_ports
from websockets.server import WebSocketServerProtocol, serve


GGA_REGEX = re.compile(r"^\$(?:GP|GN|GL|GA|GB)GGA,")
NMEA_LINE = re.compile(r"^\$.*\*[0-9A-Fa-f]{2}\s*$")


def find_default_serial_port() -> Optional[str]:
    """Try to guess the serial port of the GNSS device."""
    candidates: list[str] = []
    for p in list_ports.comports():
        desc = (p.description or "").lower()
        hwid = (p.hwid or "").lower()
        if any(k in desc for k in ("gnss", "gps", "rtk", "ublox", "receiver")):
            return p.device
        if any(k in desc for k in ("usb", "uart", "serial")) or any(
            k in hwid for k in ("usb", "uart", "acm", "cp210", "ch340", "ftdi")
        ):
            candidates.append(p.device)
    for preferred in ("/dev/ttyACM0", "/dev/ttyUSB0"):
        if preferred in candidates:
            return preferred
    return candidates[0] if candidates else None


def nmea_checksum_ok(sentence: str) -> bool:
    """Validate NMEA checksum."""
    sentence = sentence.strip()
    if not sentence.startswith("$") or "*" not in sentence:
        return False
    data, _, checksum_str = sentence[1:].partition("*")
    try:
        expected = int(checksum_str[:2], 16)
    except ValueError:
        return False
    calc = 0
    for ch in data:
        calc ^= ord(ch)
    return calc == expected


def ddmm_to_decimal(value: str, hemi: str, is_lat: bool) -> Optional[float]:
    """Convert ddmm.mmmm or dddmm.mmmm to decimal degrees."""
    if not value or "." not in value:
        return None
    try:
        if is_lat:
            deg = int(value[:2])
            minutes = float(value[2:])
        else:
            deg = int(value[:3])
            minutes = float(value[3:])
    except ValueError:
        return None
    decimal = deg + minutes / 60.0
    if (is_lat and hemi == "S") or (not is_lat and hemi == "W"):
        decimal = -decimal
    return decimal


def parse_gga(sentence: str) -> Optional[Dict[str, Any]]:
    """Parse a GGA sentence and return structured data."""
    if not GGA_REGEX.match(sentence):
        return None
    if not nmea_checksum_ok(sentence):
        return None
    core = sentence.strip()[1:]
    data, _, _ = core.partition("*")
    parts = data.split(",")
    try:
        utc = parts[1] or ""
        lat = ddmm_to_decimal(parts[2], parts[3], is_lat=True)
        lon = ddmm_to_decimal(parts[4], parts[5], is_lat=False)
        fix_quality = int(parts[6] or 0)
        num_sats = int(parts[7] or 0)
        hdop = float(parts[8]) if parts[8] else None
        alt_m = float(parts[9]) if parts[9] else None
    except (IndexError, ValueError):
        return None
    if lat is None or lon is None:
        return None
    return {
        "type": "gga",
        "timestamp_utc": utc,
        "lat": lat,
        "lon": lon,
        "alt_m": alt_m,
        "fix_quality": fix_quality,
        "num_sats": num_sats,
        "hdop": hdop,
    }


class SerialReader(threading.Thread):
    """
    Thread reading NMEA lines from a serial port and pushing them to the
    asyncio loop thread-safely.
    """

    def __init__(
        self,
        port: str,
        baud: int,
        queue: "asyncio.Queue[str]",
        stop_event: threading.Event,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(daemon=True)
        self.port = port
        self.baud = baud
        self.queue = queue
        self.stop_event = stop_event
        self.loop = loop  # main thread's event loop

    def run(self) -> None:
        try:
            with Serial(self.port, self.baud, timeout=1) as ser:
                while not self.stop_event.is_set():
                    try:
                        raw = ser.readline()
                        if not raw:
                            continue
                        line = raw.decode("ascii", errors="ignore").strip()
                        if NMEA_LINE.match(line):
                            # Schedule a thread-safe put into the asyncio queue.
                            self.loop.call_soon_threadsafe(
                                self.queue.put_nowait, line
                            )
                    except SerialException:
                        break
        except SerialException as exc:
            # Surface a synthetic message into the asyncio queue.
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                f"$ERR,SerialException,{str(exc)}*00",
            )


async def broadcaster(
    queue: "asyncio.Queue[str]", clients: "Set[WebSocketServerProtocol]"
) -> None:
    """Read GGA sentences from queue and broadcast to clients."""
    while True:
        line = await queue.get()
        if GGA_REGEX.match(line):
            data = parse_gga(line)
            if data is None:
                continue
            payload = json.dumps(data)
            dead: Set[WebSocketServerProtocol] = set()
            for ws in list(clients):
                try:
                    await ws.send(payload)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                clients.discard(ws)


async def ws_handler(
    websocket: WebSocketServerProtocol,
    path: str,  # required by websockets signature
    clients: "Set[WebSocketServerProtocol]",
) -> None:
    """Handle WebSocket client lifecycle."""
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "hello", "msg": "connected"}))
        async for _ in websocket:
            pass
    finally:
        clients.discard(websocket)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="GNSS/RTK NMEA ($GxGGA) WebSocket server"
    )
    parser.add_argument("--port", default="auto", help="Serial port (COMx or /dev/ttyACM0).")
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate (default 115200).")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address (default 0.0.0.0).")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket port (default 8765).")
    return parser.parse_args()


async def amain() -> None:
    args = parse_args()

    # Obtain the running loop (this is the loop we must use from the serial thread).
    loop = asyncio.get_running_loop()

    serial_port = find_default_serial_port() if args.port.lower() == "auto" else args.port
    if not serial_port:
        print("ERROR: Could not find serial device, specify --port", file=sys.stderr)
        sys.exit(2)

    print(f"[INFO] Serial: {serial_port} @ {args.baud} baud")
    print(f"[INFO] WebSocket: ws://{args.host}:{args.ws_port}")

    queue: asyncio.Queue[str] = asyncio.Queue()
    clients: Set[WebSocketServerProtocol] = set()
    stop_event = threading.Event()

    # Pass the main loop into the serial thread.
    reader = SerialReader(serial_port, args.baud, queue, stop_event, loop)
    reader.start()

    broadcaster_task = asyncio.create_task(broadcaster(queue, clients))
    async with serve(
        lambda ws, p: ws_handler(ws, p, clients),
        args.host,
        args.ws_port,
        ping_interval=20,
        ping_timeout=20,
    ):
        # Graceful shutdown
        loop = asyncio.get_running_loop()
        stop_future = loop.create_future()

        def _stop() -> None:
            if not stop_future.done():
                stop_future.set_result(None)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                # Signals may not be available on some platforms (e.g., Windows).
                pass

        await stop_future

    # Teardown.
    stop_event.set()
    broadcaster_task.cancel()
    try:
        await broadcaster_task
    except asyncio.CancelledError:
        pass


def main() -> None:
    try:
        asyncio.run(amain())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
