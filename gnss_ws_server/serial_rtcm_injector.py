# serial_rtcm_injector.py
from __future__ import annotations
import threading
import time
import queue
from dataclasses import dataclass
from typing import Optional, Tuple, List

import serial  # pyserial

@dataclass
class InjectorStats:
    drops: int = 0
    write_errors: int = 0
    reopen_count: int = 0
    bytes_written: int = 0

class SerialRtcmInjector:
    """
    Writer RTCM thread-safe, à buffer borné, avec reconnexion.
    - enqueue(data: bytes): thread-safe, non bloquant
    - start()/stop()
    """
    def __init__(
        self,
        port: str,
        baudrate: int = 115200,
        queue_size: int = 256,
        write_timeout: float = 1.0,
        reopen_backoff: Tuple[float, float] = (0.5, 5.0),  # min/max sec
        coalesce_max_bytes: int = 4096,  # max bytes à écrire d'un coup
        log: Optional[callable] = None,  # log(level:str, msg:str)
    ):
        self.port = port
        self.baudrate = baudrate
        self.queue: "queue.Queue[bytes]" = queue.Queue(maxsize=queue_size)
        self.write_timeout = write_timeout
        self.reopen_backoff = reopen_backoff
        self.coalesce_max_bytes = coalesce_max_bytes

        self._ser: Optional[serial.Serial] = None
        self._t: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.stats = InjectorStats()
        self._log = log or (lambda level, msg: None)

    # ---------- Public API ----------
    def start(self) -> None:
        if self._t and self._t.is_alive():
            return
        self._stop.clear()
        self._t = threading.Thread(target=self._run, name="rtcm-writer", daemon=True)
        self._t.start()

    def stop(self) -> None:
        self._stop.set()
        if self._t:
            self._t.join(timeout=3)
        self._close_serial()

    def enqueue(self, data: bytes) -> None:
        """Non bloquant. Si plein : drop oldest pour garder le plus récent (latest-wins)."""
        if not data:
            return
        try:
            self.queue.put_nowait(data)
        except queue.Full:
            # Drop oldest
            try:
                _ = self.queue.get_nowait()
            except queue.Empty:
                pass
            self.stats.drops += 1
            try:
                self.queue.put_nowait(data)
            except queue.Full:
                # si re-plein (forte pression), on abandonne ce paquet
                self.stats.drops += 1

    # ---------- Implementation ----------
    def _open_serial(self) -> None:
        self._close_serial()
        self._ser = serial.Serial(
            port=self.port,
            baudrate=self.baudrate,
            timeout=0,               # non-blocking read (on ne lit pas)
            write_timeout=self.write_timeout
        )
        self._log("info", f"RTCM injector: opened {self.port}@{self.baudrate}")

    def _close_serial(self) -> None:
        if self._ser:
            try:
                self._ser.close()
            except Exception:
                pass
        self._ser = None

    def _reopen_loop(self) -> None:
        """Essaie d’ouvrir le port avec backoff exponentiel borné."""
        delay = self.reopen_backoff[0]
        while not self._stop.is_set():
            try:
                self._open_serial()
                return
            except serial.SerialException as e:
                self.stats.reopen_count += 1
                self._log("error", f"RTCM injector: open failed: {e}. Retrying in {delay:.1f}s")
                time.sleep(delay)
                delay = min(delay * 2, self.reopen_backoff[1])

    def _coalesce(self, first_chunk: bytes) -> bytes:
        """Regroupe plusieurs paquets du buffer jusqu’à coalesce_max_bytes."""
        chunks: List[bytes] = [first_chunk]
        size = len(first_chunk)
        while size < self.coalesce_max_bytes:
            try:
                nxt = self.queue.get_nowait()
            except queue.Empty:
                break
            chunks.append(nxt)
            size += len(nxt)
        return b"".join(chunks)

    def _run(self) -> None:
        self._reopen_loop()
        while not self._stop.is_set():
            if not self._ser:
                self._reopen_loop()
                continue
            try:
                # On attend un premier paquet (bloquant court)
                try:
                    first = self.queue.get(timeout=0.2)
                except queue.Empty:
                    continue

                payload = self._coalesce(first)

                # Ecriture (peut lever SerialTimeoutException/SerialException)
                written = self._ser.write(payload)
                # Selon plateforme, flush() peut être utile pour forcer l'envoi
                try:
                    self._ser.flush()  # peut être no-op selon driver
                except Exception:
                    pass

                self.stats.bytes_written += int(written or 0)

            except serial.SerialTimeoutException as e:
                self.stats.write_errors += 1
                self._log("warning", f"RTCM injector: write timeout ({e}). Dropping current batch.")
                # on n’essaie pas de réécrire ce batch : la queue continue (fresh > stale)
                continue
            except serial.SerialException as e:
                self.stats.write_errors += 1
                self._log("error", f"RTCM injector: serial error ({e}). Reopening port.")
                self._close_serial()
                self._reopen_loop()
            except Exception as e:
                self.stats.write_errors += 1
                self._log("error", f"RTCM injector: unexpected error: {e!r}")
                # en cas d’erreur inconnue, on continue la boucle

