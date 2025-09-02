import argparse
from serial.tools import list_ports


def find_default_serial_port():
    """Try to guess a GNSS device serial port."""
    candidates = []
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="GNSS WebSocket server")

    # NEW: allow JSON config file
    parser.add_argument(
        "--config",
        type=str,
        help="Path to JSON configuration file (e.g. config/gnss.json)"
    )

    # existing flags (examples; keep yours if already defined)
    parser.add_argument("--port", action="append", default=[], help="Serial port (repeatable)")
    parser.add_argument("--label", action="append", default=[], help="Alias mapping PORT:NAME (repeatable)")
    parser.add_argument("--baud", type=int, default=115200, help="Serial baud rate")
    parser.add_argument("--host", default="127.0.0.1", help="WebSocket host")
    parser.add_argument("--ws-port", dest="ws_port", type=int, default=8765, help="WebSocket port")

    return parser.parse_args(argv)


def normalize_ports(arg_ports):
    """Expand comma-separated values and resolve 'auto' to a guessed port."""
    ports = []
    for item in arg_ports:
        for p in item.split(","):
            p = p.strip()
            if p:
                ports.append(p)
    if not ports:
        ports = ["auto"]

    resolved = []
    for p in ports:
        if p.lower() == "auto":
            guessed = find_default_serial_port()
            if guessed:
                resolved.append(guessed)
        else:
            resolved.append(p)
    return list(dict.fromkeys(resolved))  # dedupe while preserving order


def parse_labels(label_args):
    """
    Parse --label entries like 'COM27:ROVER' into a dict { 'COM27': 'ROVER' }.
    Ignores malformed entries gracefully.
    """
    mapping = {}
    for item in label_args:
        if ":" not in item:
            continue
        port, alias = item.split(":", 1)
        port = port.strip()
        alias = alias.strip()
        if port and alias:
            mapping[port] = alias
    return mapping
