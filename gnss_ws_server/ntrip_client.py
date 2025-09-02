"""
ntrip_client.py
---------------
Minimal NTRIP v1/v2 client for RTCM over TCP/TLS.

Features:
- Supports ntrip://, ntrips://, http://, https:// URLs
- Basic Authorization (user:pass in URL)
- Reconnect with exponential backoff
- Optional periodic upstream $GGA (via gga_provider callable)
- Prints SOURCETABLE if the caster replies with an unexpected status (e.g., 404)
- Thread-safe friendly (driven by asyncio)
"""

from __future__ import annotations

import asyncio
import base64
import ssl as ssl_lib
from typing import Callable, Optional
from urllib.parse import urlparse


class NtripClient:
    """
    NtripClient connects to an NTRIP caster and forwards incoming RTCM bytes
    to a provided writer callback (e.g., a serial writer).

    Parameters
    ----------
    name : str
        Alias for logs (e.g., 'ROVER').
    url : str
        NTRIP URL: ntrip[s]://user:pass@host:port/MOUNT
    writer_cb : Callable[[bytes], None]
        Function invoked with raw RTCM bytes to forward to the receiver.
    loop : asyncio.AbstractEventLoop
        Event loop (for scheduling).
    gga_provider : Optional[Callable[[], Optional[bytes]]]
        Optional callable returning the latest $GGA bytes (CRLF-terminated).
        If provided, $GGA is sent periodically upstream.

    Other options can be tuned via properties below.
    """

    def __init__(
        self,
        name: str,
        url: str,
        writer_cb: Callable[[bytes], None],
        loop: asyncio.AbstractEventLoop,
        gga_provider: Optional[Callable[[], Optional[bytes]]] = None,
        gga_interval_sec: float = 10.0,
        reconnect_initial: float = 2.0,
        reconnect_max: float = 30.0,
    ) -> None:
        self.name = name
        self.url = url
        self.writer_cb = writer_cb
        self.loop = loop
        self.gga_provider = gga_provider
        self.gga_interval_sec = gga_interval_sec
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max

        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

    # Public API -------------------------------------------------------------

    def start(self) -> None:
        """Start the client task."""
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = self.loop.create_task(self._run())

    def stop(self) -> None:
        """Signal the client to stop and cancel its task."""
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()

    def set_gga_provider(self, provider: Optional[Callable[[], Optional[bytes]]]) -> None:
        """Install/replace the GGA provider at runtime."""
        self.gga_provider = provider

    # Internal ---------------------------------------------------------------

    async def _run(self) -> None:
        backoff = self.reconnect_initial
        while not self._stop.is_set():
            try:
                await self._run_once()
                backoff = self.reconnect_initial  # reset on clean exit
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001
                print(f"[NTRIP:{self.name}] error: {exc}")
                await asyncio.sleep(backoff)
                backoff = min(self.reconnect_max, backoff * 2)

    async def _run_once(self) -> None:
        pr = urlparse(self.url)
        scheme = (pr.scheme or "").lower()

        if scheme in ("ntrip", "http"):
            use_ssl = False
        elif scheme in ("ntrips", "https"):
            use_ssl = True
        else:
            raise ValueError(f"Unsupported NTRIP scheme: {scheme!r}")

        host = pr.hostname
        if not host:
            raise ValueError("NTRIP URL missing host")
        port = pr.port or (443 if use_ssl else 2101)

        mount = pr.path or "/"
        if not mount.startswith("/"):
            mount = "/" + mount

        user = pr.username or ""
        password = pr.password or ""
        auth_b64 = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")

        ssl_ctx = None
        if use_ssl:
            ssl_ctx = ssl_lib.create_default_context()

        reader, writer = await asyncio.open_connection(host=host, port=port, ssl=ssl_ctx)
        try:
            # NTRIP request
            req = (
                f"GET {mount} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "User-Agent: NTRIP pyclient/1.0\r\n"
                "Ntrip-Version: Ntrip/2.0\r\n"
                "Accept: */*\r\n"
                "Connection: keep-alive\r\n"
                f"Authorization: Basic {auth_b64}\r\n"
                "\r\n"
            ).encode("ascii")
            writer.write(req)
            await writer.drain()

            # Read response headers
            headers = await reader.readuntil(b"\r\n\r\n")
            if b"200 OK" not in headers and b"ICY 200 OK" not in headers:
                # Try to fetch SOURCETABLE to help troubleshooting
                table = await self._fetch_sourcetable(host, port, ssl_ctx, auth_b64)
                try:
                    txt = table.decode("utf-8", errors="ignore")
                except Exception:
                    txt = repr(table)
                print(f"[NTRIP:{self.name}] unexpected response.\n"
                      f"=== SOURCETABLE ===\n{txt}\n=== end ===")
                raise RuntimeError(f"Unexpected NTRIP response: {headers!r}")

            print(f"[NTRIP:{self.name}] connected to {host}:{port}{mount}")

            # Start periodic GGA sender (optional)
            gga_task: Optional[asyncio.Task] = None
            if self.gga_provider:
                gga_task = asyncio.create_task(self._send_gga_periodically(writer))

            # Forward RTCM chunks to writer_cb
            while not self._stop.is_set():
                chunk = await reader.read(8192)
                if not chunk:
                    break
                try:
                    self.writer_cb(chunk)
                except Exception as exc:  # noqa: BLE001
                    print(f"[NTRIP:{self.name}] writer_cb error: {exc}")
                    # Keep the connection alive; continue reading
                    continue

            if gga_task:
                gga_task.cancel()
                try:
                    await gga_task
                except asyncio.CancelledError:
                    pass

        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _fetch_sourcetable(
        self,
        host: str,
        port: int,
        ssl_ctx: Optional[ssl_lib.SSLContext],
        auth_b64: str,
    ) -> bytes:
        """Fetch SOURCETABLE from '/' to help the user choose a valid mountpoint."""
        reader, writer = await asyncio.open_connection(host=host, port=port, ssl=ssl_ctx)
        try:
            req = (
                "GET / HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                "User-Agent: NTRIP pyclient/1.0\r\n"
                "Ntrip-Version: Ntrip/2.0\r\n"
                "Accept: */*\r\n"
                "Connection: close\r\n"
                f"Authorization: Basic {auth_b64}\r\n"
                "\r\n"
            ).encode("ascii")
            writer.write(req)
            await writer.drain()
            data = await reader.read(-1)
            return data
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _send_gga_periodically(self, writer) -> None:
        """Send latest GGA upstream every gga_interval_sec if a provider is set."""
        assert self.gga_provider is not None
        interval = max(1.0, float(self.gga_interval_sec))
        while not self._stop.is_set():
            gga = None
            try:
                gga = self.gga_provider()
            except Exception:
                gga = None
            if gga:
                try:
                    writer.write(gga)  # should already be CRLF-terminated
                    await writer.drain()
                except Exception:
                    return
            await asyncio.sleep(interval)


__all__ = ["NtripClient"]
