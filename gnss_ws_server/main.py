import asyncio
import signal
import threading
from .config import parse_args, normalize_ports, parse_labels
from .serial_reader import SerialGNSSReader
from .websocket_server import GNSSWebSocketServer

class GNSSManager:
    def __init__(self):
        self.args = parse_args()
        self.loop = asyncio.get_event_loop()
        self.queue = asyncio.Queue()
        self.stop_event = threading.Event()
        self.server = GNSSWebSocketServer(self.args.host, self.args.ws_port, self.queue)
        self.readers = []

    def setup_readers(self):
        ports = normalize_ports(self.args.port)
        labels = parse_labels(self.args.label)
        for p in ports:
            name = labels.get(p, p)
            print(f"[INFO] Serial: {p} @ {self.args.baud} baud -> alias: {name}")
            r = SerialGNSSReader(name, p, self.args.baud, self.queue, self.stop_event, self.loop)
            self.readers.append(r)

    async def start(self):
        await self.server.start()
        for r in self.readers:
            r.start()

    async def stop(self):
        self.stop_event.set()
        await self.server.stop()

async def main():
    manager = GNSSManager()
    manager.setup_readers()
    await manager.start()

    stop_future = asyncio.get_running_loop().create_future()
    def stop():
        if not stop_future.done():
            stop_future.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    await stop_future
    await manager.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
