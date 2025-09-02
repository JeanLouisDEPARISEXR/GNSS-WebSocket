import asyncio
import json
from websockets.server import serve
from .nmea_parser import NMEAParser

class GNSSWebSocketServer:
    def __init__(self, host: str, port: int, queue: asyncio.Queue):
        self.host = host
        self.port = port
        self.queue = queue
        self.clients = set()
        self._server = None
        self._task = None

    async def start(self):
        self._server = await serve(self._handler, self.host, self.port)
        self._task = asyncio.create_task(self._broadcaster())
        print(f"[INFO] WebSocket server listening on ws://{self.host}:{self.port}")

    async def _handler(self, websocket, path):
        self.clients.add(websocket)
        try:
            await websocket.send(json.dumps({"type": "hello", "msg": "connected"}))
            async for _ in websocket:
                pass
        finally:
            self.clients.discard(websocket)

    async def _broadcaster(self):
        while True:
            source, line = await self.queue.get()
            if NMEAParser.GGA_REGEX.match(line):
                data = NMEAParser.parse_gga(line)
                if not data:
                    continue
                data["source"] = source
                payload = json.dumps(data)
                dead = []
                for ws in list(self.clients):
                    try:
                        await ws.send(payload)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    self.clients.discard(ws)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
