# websocket_server.py
import asyncio
import json
from typing import Callable, Optional, Set, Tuple

from websockets.server import serve, WebSocketServerProtocol
from .nmea_parser import GGA_REGEX, parse_gga


async def broadcaster(
    queue: "asyncio.Queue[Tuple[str, str]]",
    clients: Set[WebSocketServerProtocol],
    gga_sink: Optional[Callable[[dict], None]] = None,
) -> None:
    """
    Continuously read (source, nmea_line) tuples from the queue
    and broadcast valid GGA sentences to all connected WebSocket clients.

    Args:
        queue: An asyncio.Queue containing (source, NMEA sentence) tuples.
        clients: A set of active WebSocket connections.
        gga_sink: Optional callback that receives the parsed GGA data
                  (including raw sentence) for external use (e.g., NTRIP).
    """
    while True:
        source, line = await queue.get()

        # Only handle GGA sentences
        if not GGA_REGEX.match(line):
            continue

        data = parse_gga(line)
        if not data:
            continue

        # Attach metadata
        data["source"] = source
        data["raw"] = line

        # Invoke external callback (e.g., for caching latest GGA)
        if gga_sink:
            try:
                gga_sink(data)
            except Exception:
                # Prevent callback errors from stopping the broadcaster
                pass

        # Broadcast the data as JSON
        payload = json.dumps(data)
        dead_clients = set()
        for ws in list(clients):
            try:
                await ws.send(payload)
            except Exception:
                dead_clients.add(ws)

        # Remove disconnected clients
        for ws in dead_clients:
            clients.discard(ws)


async def ws_handler(
    websocket: WebSocketServerProtocol,
    path: str,
    clients: Set[WebSocketServerProtocol],
) -> None:
    """
    Handle the lifecycle of a WebSocket connection.

    Sends a greeting message upon connection and listens for
    incoming messages (ignored in this server since it's
    a broadcast-only system).

    Args:
        websocket: The WebSocket connection object.
        path: The request path (unused).
        clients: A set of active WebSocket connections.
    """
    clients.add(websocket)
    try:
        await websocket.send(json.dumps({"type": "hello", "msg": "connected"}))
        async for _ in websocket:
            # No incoming messages are processed
            pass
    finally:
        clients.discard(websocket)


async def start_server(
    host: str,
    ws_port: int,
    clients: Set[WebSocketServerProtocol],
):
    """
    Start and return a WebSocket server.

    Args:
        host: The hostname or IP address to bind to.
        ws_port: The WebSocket port to listen on.
        clients: A set to track active WebSocket connections.

    Returns:
        The WebSocket server instance created by `websockets.serve`.
    """
    server = await serve(
        lambda ws, p: ws_handler(ws, p, clients),
        host,
        ws_port,
        ping_interval=20,
        ping_timeout=20,
        max_queue=None,  # Avoid blocking if client is slow
    )
    return server
