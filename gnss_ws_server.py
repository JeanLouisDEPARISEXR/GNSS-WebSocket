#!/usr/bin/env python3
"""
GNSS/RTK WebSocket server streaming $GxGGA from one or more serial receivers.

- Supports multiple serial ports (e.g., --port COM27 --port COM8) or
  a comma-separated list (--port COM27,COM8). "auto" will try to guess one port.
- Each outgoing JSON payload includes a "source" field to identify the receiver.

Example run:
    python gnss_ws_server.py --port COM27 --port COM8 --baud 115200 --host 127.0.0.1 --ws-port 8765

Payload example:
  {
    "type": "gga",
    "source": "COM27",
    "timestamp_utc": "12:34:56",
    "lat": 48.8566,
    "lon": 2.3522,
    "alt_m": 35.1,
    "fix_quality": 4,
    "num_sats": 18,
    "hdop": 0.7
  }
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import signal
import sys
import threading
from typing import Any, Dict, Optional, Set, List, Tuple

import serial
from serial import Serial, SerialException
from serial.tools import list_ports
from websockets.server import WebSocketServerProtocol, serve

GGA_REGEX = re.compile(r"^\$(?:GP|GN|GL|GA|GB)GGA,")
NMEA_LINE = re.compile(r"^\$.*\*[0-9A-Fa-f]{2}\s*$")


def find_default_serial_port() -> Optional[str]:
    """Try to guess a single serial port of a GNSS device."""
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
    """Parse a GGA sentence and return structured data (without 'source')."""
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
    asyncio loop thread-safely, along with a 'source' tag.
    """

    def __init__(
        self,
        source_name: str,
        port: str,
        baud: int,
        queue: "asyncio.Queue[Tuple[str, str]]",
        stop_event: threading.Event,
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        super().__init__(daemon=True)
        self.source_name = source_name
        self.port = port
        self.baud = baud
        self.queue = queue
        self.stop_event = stop_event
        self.loop = loop

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
                            self.loop.call_soon_threadsafe(
                                self.queue.put_nowait, (self.source_name, line)
                            )
                    except SerialException:
                        break
        except SerialException as exc:
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                (self.source_name, f"$ERR,SerialException,{str(exc)}*00"),
            )


async def broadcaster(
    queue: "asyncio.Queue[Tuple[str, str]]",
    clients: "Set[WebSocketServerProtocol]",
) -> None:
    """Read (source, line) from queue; if GGA, broadcast JSON with 'source'."""
    while True:
        source, line = await queue.get()
        if GGA_REGEX.match(line):
            data = parse_gga(line)
            if data is None:
                continue
            data["source"] = source
            payload = json.dumps(data)
            dead = set()
            for ws in list(clients):
                try:
                    await ws.send(payload)
                except Exception:
                    dead.add(ws)
            for ws in dead:
                clients.discard(ws)


async def ws_handler(
    websocket: WebSocketServerProtocol,
    path: str,
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
        description="GNSS/RTK NMEA ($GxGGA) WebSocket server (multi-receiver)"
    )
    parser.add_argument(
        "--port",
        action="append",
        default=[],
        help=(
            "Serial port(s). Repeat --port or use comma list (e.g., COM27,COM8). "
            "Use 'auto' to guess one port."
        ),
    )
    parser.add_argument("--baud", type=int, default=115200, help="Baud rate.")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address.")
    parser.add_argument("--ws-port", type=int, default=8765, help="WebSocket port.")
    return parser.parse_args()


def normalize_ports(arg_ports: List[str]) -> List[str]:
    """Expand comma-separated and resolve 'auto' if present."""
    ports: List[str] = []
    for item in arg_ports:
        if not item:
            continue
        for p in item.split(","):
            p = p.strip()
            if p:
                ports.append(p)
    if not ports:
        # default to auto if nothing provided
        ports = ["auto"]
    # Resolve 'auto' into a single guessed port (keep order)
    resolved: List[str] = []
    for p in ports:
        if p.lower() == "auto":
            guessed = find_default_serial_port()
            if guessed:
                resolved.append(guessed)
        else:
            resolved.append(p)
    # Deduplicate while preserving order
    seen = set()
    unique = []
    for p in resolved:
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


async def amain() -> None:
    args = parse_args()
    loop = asyncio.get_running_loop()

    ports = normalize_ports(args.port)
    if not ports:
        print("ERROR: No serial ports found/resolved.", file=sys.stderr)
        sys.exit(2)

    print(f"[INFO] WebSocket: ws://{args.host}:{args.ws_port}")
    for p in ports:
        print(f"[INFO] Serial: {p} @ {args.baud} baud")

    queue: asyncio.Queue[Tuple[str, str]] = asyncio.Queue()
    clients: Set[WebSocketServerProtocol] = set()
    stop_event = threading.Event()

    # Launch one reader per port
    readers = [
        SerialReader(source_name=p, port=p, baud=args.baud, queue=queue, stop_event=stop_event, loop=loop)
        for p in ports
    ]
    for r in readers:
        r.start()

    broadcaster_task = asyncio.create_task(broadcaster(queue, clients))
    async with serve(
        lambda ws, p: ws_handler(ws, p, clients),
        args.host,
        args.ws_port,
        ping_interval=20,
        ping_timeout=20,
    ):
        stop_future = loop.create_future()

        def _stop() -> None:
            if not stop_future.done():
                stop_future.set_result(None)

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _stop)
            except NotImplementedError:
                pass

        await stop_future

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
