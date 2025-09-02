import threading
from serial import Serial, SerialException
from .nmea_parser import NMEAParser

class SerialGNSSReader(threading.Thread):
    """Thread class for reading GNSS data from a serial port."""

    def __init__(self, name: str, port: str, baud: int, queue, stop_event, loop):
        super().__init__(daemon=True)
        self.name = name
        self.port = port
        self.baud = baud
        self.queue = queue
        self.stop_event = stop_event
        self.loop = loop

    def run(self):
        try:
            with Serial(self.port, self.baud, timeout=1) as ser:
                while not self.stop_event.is_set():
                    try:
                        raw = ser.readline()
                        if not raw:
                            continue
                        line = raw.decode("ascii", errors="ignore").strip()
                        if NMEAParser.NMEA_LINE.match(line):
                            self.loop.call_soon_threadsafe(
                                self.queue.put_nowait, (self.name, line)
                            )
                    except SerialException:
                        break
        except SerialException as exc:
            self.loop.call_soon_threadsafe(
                self.queue.put_nowait,
                (self.name, f"$ERR,SerialException,{str(exc)}*00"),
            )
