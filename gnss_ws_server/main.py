# main.py
"""
GNSS WebSocket Server with per-receiver config (Solution 2) and backward compatibility.

New JSON schema (preferred):
{
  "host": "127.0.0.1",
  "ws_port": 8765,
  "defaults": { "baud": 115200, "gga_period_sec": 10 },
  "receivers": [
    { "port": "COM27", "label": "BASE",  "baud": 115200 },
    { "port": "COM9",  "label": "ROVER", "baud": 57600,
      "correction": {
        "type": "ntrip",
        "url": "ntrip://user:pass@caster:2101/MOUNT",
        "send_gga": true,
        "gga_source": "ROVER",
        "gga_period_sec": 10
      }
    }
  ],
  "corrections": { "PROFILE1": { ... } },   # optional profiles
  "log_level": "INFO"
}

If "correction" is omitted or null for a receiver -> no correction.
If "correction" is a string, it references a profile under "corrections".
If "receivers" is absent, we fall back to the old schema.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, asdict
from typing import Callable, Dict, List, Optional, Tuple

# ---- Robust imports for local CLI helpers -----------------------------------
try:
    from gnss_config import parse_args, normalize_ports, parse_labels  # type: ignore
except Exception:  # fallback to config.py
    from .config import parse_args, normalize_ports, parse_labels  # type: ignore

from .serial_reader import SerialReader
from .websocket_server import broadcaster, start_server
from .ntrip_client import NtripClient  # async version provided in your project


# --------------------------- Status tracking ---------------------------------
@dataclass
class ReceiverStatus:
    name: str
    port: str
    baud: int
    nmea_count: int = 0
    last_gga_ts: Optional[float] = None
    rtcm_bytes: int = 0
    last_rtcm_ts: Optional[float] = None

    def as_dict(self) -> Dict:
        d = asdict(self)

        def _fmt(ts: Optional[float]) -> Optional[str]:
            if ts is None:
                return None
            return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

        d["last_gga"] = _fmt(self.last_gga_ts)
        d["last_rtcm"] = _fmt(self.last_rtcm_ts)
        return d


class StatsManager:
    def __init__(self) -> None:
        self._by_name: Dict[str, ReceiverStatus] = {}
        self._lock = threading.Lock()

    def ensure(self, name: str, port: str, baud: int) -> None:
        with self._lock:
            self._by_name.setdefault(name, ReceiverStatus(name=name, port=port, baud=baud))

    def add_gga(self, name: str) -> None:
        now = time.time()
        with self._lock:
            st = self._by_name.get(name)
            if st is None:
                st = ReceiverStatus(name=name, port="?", baud=0)
                self._by_name[name] = st
            st.nmea_count += 1
            st.last_gga_ts = now

    def add_rtcm(self, name: str, nbytes: int) -> None:
        now = time.time()
        with self._lock:
            st = self._by_name.get(name)
            if st is None:
                st = ReceiverStatus(name=name, port="?", baud=0)
                self._by_name[name] = st
            st.rtcm_bytes += int(nbytes)
            st.last_rtcm_ts = now

    def snapshot(self) -> Dict[str, Dict]:
        with self._lock:
            return {k: v.as_dict() for k, v in self._by_name.items()}


_STATS = StatsManager()


def get_receiver_status(name: Optional[str] = None) -> Dict[str, Dict]:
    snap = _STATS.snapshot()
    if name is not None:
        return {name: snap[name]} if name in snap else {}
    return snap


# ------------------------------ Helpers --------------------------------------
def _load_json_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _unique_preserve(seq):
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _build_ntrip_url(cfg: dict) -> str:
    """
    Build URL from separate fields for backward compat:
    caster_host, caster_port, mountpoint, username, password, use_tls (optional)
    """
    host = cfg.get("caster_host")
    port = int(cfg.get("caster_port", 2101))
    mp = (cfg.get("mountpoint") or "").lstrip("/")
    user = cfg.get("username") or ""
    pwd = cfg.get("password") or ""
    use_tls = bool(cfg.get("use_tls", False))
    scheme = "ntrips" if use_tls else "ntrip"
    auth = f"{user}:{pwd}@" if (user or pwd) else ""
    return f"{scheme}://{auth}{host}:{port}/{mp}"


def _resolve_correction_object(receiver_entry: dict, profiles: dict, defaults: dict) -> Optional[dict]:
    """
    Return a normalized correction object for a receiver, or None if no correction.
    - If receiver_entry["correction"] is None or missing: return None.
    - If it's a string: look it up in profiles.
    - If it's an object: return it as-is.
    Also ensure gga_period_sec defaulting from defaults if not provided.
    """
    corr = receiver_entry.get("correction", None)
    if corr is None:
        return None
    if isinstance(corr, str):
        prof = profiles.get(corr)
        if not isinstance(prof, dict):
            print(f"[WARN] Correction profile '{corr}' not found or invalid.")
            return None
        corr_obj = dict(prof)  # shallow copy
    elif isinstance(corr, dict):
        corr_obj = dict(corr)
    else:
        return None

    # Backward compat: allow separated fields instead of 'url'
    if "url" not in corr_obj:
        corr_obj["url"] = _build_ntrip_url(corr_obj)

    # Defaults for GGA period
    if "gga_period_sec" not in corr_obj:
        corr_obj["gga_period_sec"] = int(defaults.get("gga_period_sec", 10))

    return corr_obj


def _normalize_new_schema(file_cfg: dict, cli_baud: Optional[int]) -> Tuple[List[dict], Dict]:
    """
    Normalize the new 'receivers' schema into a list of:
      { "name": <label>, "port": <os_port>, "baud": <int>, "correction": <dict|None> }
    Returns (receivers, meta) where meta contains global values like host/ws_port.
    """
    defaults = file_cfg.get("defaults", {}) or {}
    receivers_cfg = file_cfg.get("receivers", []) or []
    profiles = file_cfg.get("corrections", {}) or {}

    # If no receivers in new schema, signal caller to use old schema
    if not receivers_cfg:
        return [], {}

    host = file_cfg.get("host", "127.0.0.1")
    ws_port = int(file_cfg.get("ws_port", 8765))

    norm_list = []
    for entry in receivers_cfg:
        if not isinstance(entry, dict):
            continue
        port = entry.get("port")
        if not port:
            continue
        name = entry.get("label", port)
        baud = int(entry.get("baud") or defaults.get("baud") or cli_baud or 115200)

        corr_obj = _resolve_correction_object(entry, profiles, defaults)

        norm_list.append({
            "name": name,
            "port": port,
            "baud": baud,
            "correction": corr_obj,  # None or dict with at least 'url'
        })

    meta = {"host": host, "ws_port": ws_port, "defaults": defaults}
    return norm_list, meta


def _normalize_old_schema(file_cfg: dict, args) -> Tuple[List[dict], Dict]:
    """
    Backward-compat path: build the same normalized list from old keys.
    Old keys: ports[], labels{}, baud, host, ws_port
    """
    host = file_cfg.get("host", "127.0.0.1")
    ws_port = int(file_cfg.get("ws_port", 8765))
    baud_global = int(getattr(args, "baud", None) or file_cfg.get("baud", 115200))

    # Merge CLI-added ports too
    ports = []
    if isinstance(file_cfg.get("ports"), list):
        ports.extend(file_cfg["ports"])
    if getattr(args, "port", None):
        ports.extend(args.port)
    ports = _unique_preserve(ports)
    ports = normalize_ports(ports)  # reuse your helper

    labels_map = {}
    if isinstance(file_cfg.get("labels"), dict):
        labels_map.update(file_cfg["labels"])
    if getattr(args, "label", None):
        labels_map.update(parse_labels(args.label))

    norm_list = []
    for p in ports:
        name = labels_map.get(p, p)
        norm_list.append({
            "name": name,
            "port": p,
            "baud": baud_global,
            "correction": None,  # old schema had only global corrections; ignored here
        })

    meta = {"host": host, "ws_port": ws_port, "defaults": {"baud": baud_global}}
    return norm_list, meta


async def periodic_status_printer(
    interval_sec: int, get_status: Callable[[], Dict[str, Dict]]
) -> None:
    if interval_sec <= 0:
        return
    while True:
        await asyncio.sleep(interval_sec)
        snap = get_status()
        if not snap:
            print("[STATUS] No receivers registered yet.")
            continue
        print("[STATUS] ---- Receivers ----")
        for name, st in snap.items():
            nmea = st.get("nmea_count", 0)
            last_gga = st.get("last_gga")
            rtcm = st.get("rtcm_bytes", 0)
            last_rtcm = st.get("last_rtcm")
            port = st.get("port")
            baud = st.get("baud")
            print(
                f"[STATUS] {name:>10} | port={port} @{baud} | "
                f"NMEA={nmea} (last GGA: {last_gga}) | "
                f"RTCM={rtcm} bytes (last: {last_rtcm})"
            )


# --------------------------------- Main --------------------------------------
async def main() -> None:
    args = parse_args()

    # Optional CLI flag for status interval (if not already in your config.py)
    status_interval = getattr(args, "status_interval", None)
    if status_interval is None:
        status_interval = 10

    # Load JSON config if provided
    file_cfg = {}
    if getattr(args, "config", None):
        cfg_path = args.config
        if not os.path.isabs(cfg_path):
            cfg_path = os.path.join(os.getcwd(), cfg_path)
        if not os.path.exists(cfg_path):
            print(f"ERROR: Config file not found: {cfg_path}", file=sys.stderr)
            sys.exit(2)
        try:
            file_cfg = _load_json_config(cfg_path)
        except Exception as e:
            print(f"ERROR: Failed to read JSON config: {e}", file=sys.stderr)
            sys.exit(2)

    # Try new schema first
    new_list, meta = _normalize_new_schema(file_cfg, getattr(args, "baud", None))
    if new_list:
        host = meta["host"]
        ws_port = meta["ws_port"]
        receivers = new_list
        defaults = meta.get("defaults", {})
    else:
        # Fallback to old schema
        receivers, meta = _normalize_old_schema(file_cfg, args)
        host = meta["host"]
        ws_port = meta["ws_port"]
        defaults = meta.get("defaults", {})

    # Sanity check
    if not receivers:
        print("ERROR: No receivers configured.", file=sys.stderr)
        sys.exit(2)

    # Display summary
    print(f"[INFO] WebSocket: ws://{host}:{ws_port}")
    for rcv in receivers:
        print(f"[INFO] Serial: {rcv['port']} @ {rcv['baud']} baud -> alias: {rcv['name']}")
        _STATS.ensure(rcv["name"], rcv["port"], rcv["baud"])

    # Shared resources
    loop = asyncio.get_running_loop()
    queue: asyncio.Queue = asyncio.Queue()
    clients = set()
    stop_event = threading.Event()  # used by SerialReader threads

    # Start SerialReader threads (per receiver, with its own baud)
    readers: List[SerialReader] = []
    for rcv in receivers:
        reader = SerialReader(
            source_name=rcv["name"],
            port=rcv["port"],
            baud=rcv["baud"],
            queue=queue,
            stop_event=stop_event,
            loop=loop,
        )
        reader.start()
        readers.append(reader)

    # Indexes for quick lookups
    name_to_reader = {r.source_name: r for r in readers}
    port_to_reader = {r.port: r for r in readers}

    # Cache of latest raw GGA (for NTRIP GGA uplink)
    latest_gga_raw: Dict[str, bytes] = {}

    def on_gga(data: Dict) -> None:
        raw = (data.get("raw") or "").strip()
        src = data.get("source")
        if src:
            _STATS.add_gga(src)
        if raw and src:
            latest_gga_raw[src] = (raw + "\r\n").encode("ascii", errors="ignore")

    # Start broadcaster + WS server
    broadcaster_task = asyncio.create_task(broadcaster(queue, clients, gga_sink=on_gga))
    ws_server = await start_server(host, ws_port, clients)
    print(f"[INFO] WebSocket server listening on ws://{host}:{ws_port}")

    # Periodic status printer
    status_task = asyncio.create_task(periodic_status_printer(int(status_interval), get_receiver_status))

    # Start NTRIP clients for receivers that have a correction
    def make_writer_cb(receiver_name: str, reader: SerialReader) -> Callable[[bytes], None]:
        def _write(chunk: bytes) -> None:
            if chunk:
                _STATS.add_rtcm(receiver_name, len(chunk))
                reader.inject(chunk)
        return _write

    def make_gga_provider(src_key: str) -> Callable[[], Optional[bytes]]:
        def _prov() -> Optional[bytes]:
            return latest_gga_raw.get(src_key)
        return _prov

    ntrip_clients: List[NtripClient] = []

    for rcv in receivers:
        corr = rcv.get("correction")
        if not corr:  # None or missing -> no correction for this receiver
            continue

        # Prefer explicit 'url', else build from separated fields (back-compat)
        url = corr.get("url") or _build_ntrip_url(corr)
        gga_src_key = corr.get("gga_source") or rcv["name"]
        gga_enabled = bool(corr.get("send_gga", False))
        gga_period = float(corr.get("gga_period_sec", defaults.get("gga_period_sec", 10)))

        client = NtripClient(
            name=f"{rcv['name']}",
            url=url,
            writer_cb=make_writer_cb(rcv["name"], name_to_reader[rcv["name"]]),
            loop=loop,
            gga_provider=make_gga_provider(gga_src_key) if gga_enabled else None,
            gga_interval_sec=gga_period,
        )
        client.start()
        ntrip_clients.append(client)
        print(f"[INFO] NTRIP started for {rcv['name']} -> {rcv['name']} ({url})")

    # Graceful shutdown
    stop_future = loop.create_future()

    def stop() -> None:
        if not stop_future.done():
            stop_future.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    await stop_future

    # Shutdown sequence
    for cli in ntrip_clients:
        try:
            cli.stop()
        except Exception:
            pass

    stop_event.set()
    ws_server.close()
    await ws_server.wait_closed()

    broadcaster_task.cancel()
    status_task.cancel()
    try:
        await broadcaster_task
    except asyncio.CancelledError:
        pass
    try:
        await status_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        import traceback
        print("[FATAL] Unhandled exception:", e, file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
