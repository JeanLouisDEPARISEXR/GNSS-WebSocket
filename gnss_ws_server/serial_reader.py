import threading
from serial import Serial, SerialException
from .nmea_parser import NMEA_LINE

class SerialReader(threading.Thread):
    """Thread to read GNSS NMEA lines from a serial port and enqueue them."""

    def __init__(self, source_name, port, baud, queue, stop_event, loop):
        super().__init__(daemon=True)
        self.source_name = source_name
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
