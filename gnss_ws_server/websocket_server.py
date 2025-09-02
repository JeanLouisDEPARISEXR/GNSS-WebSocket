import asyncio
import json
from websockets.server import serve
from .nmea_parser import GGA_REGEX, parse_gga

async def broadcaster(queue, clients):
    """Read queue and broadcast GGA messages to all connected clients."""
    while True:
        source, line = await queue.get()
        if GGA_REGEX.match(line):
            data = parse_gga(line)
            if not data:
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

async def ws_handler(websocket, path, clients):
    """Handle WebSocket connection lifecycle."""
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "hello", "msg": "connected"}))
        async for _ in websocket:
            pass
    finally:
        clients.discard(websocket)

async def start_server(host, ws_port, clients):
    """Start and return a WebSocket server."""
    server = await serve(lambda ws, p: ws_handler(ws, p, clients),
                         host, ws_port, ping_interval=20, ping_timeout=20)
    return server
