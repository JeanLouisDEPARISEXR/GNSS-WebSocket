import asyncio
import signal
import sys
import threading

from .config import parse_args, normalize_ports, parse_labels
from .serial_reader import SerialReader
from .websocket_server import broadcaster, start_server


async def main():
    args = parse_args()
    loop = asyncio.get_running_loop()

    ports = normalize_ports(args.port)
    if not ports:
        print("ERROR: No serial ports found.", file=sys.stderr)
        sys.exit(2)

    # Build alias mapping: default to port name when no label provided
    labels = parse_labels(args.label)
    port_to_name = {p: labels.get(p, p) for p in ports}

    print(f"[INFO] WebSocket: ws://{args.host}:{args.ws_port}")
    for p in ports:
        print(f"[INFO] Serial: {p} @ {args.baud} baud  -> alias: {port_to_name[p]}")

    queue = asyncio.Queue()
    clients = set()
    stop_event = threading.Event()

    # Start one reader per port, with source_name = alias (or port if none)
    readers = [
        SerialReader(source_name=port_to_name[p], port=p, baud=args.baud,
                     queue=queue, stop_event=stop_event, loop=loop)
        for p in ports
    ]
    for r in readers:
        r.start()

    # Start broadcaster + WS server
    broadcaster_task = asyncio.create_task(broadcaster(queue, clients))
    ws_server = await start_server(args.host, args.ws_port, clients)
    print(f"[INFO] WebSocket server listening on ws://{args.host}:{args.ws_port}")

    # Handle shutdown signals
    stop_future = loop.create_future()

    def stop():
        if not stop_future.done():
            stop_future.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop)
        except NotImplementedError:
            pass

    # Wait for stop
    await stop_future

    # Graceful shutdown
    stop_event.set()
    ws_server.close()
    await ws_server.wait_closed()

    broadcaster_task.cancel()
    try:
        await broadcaster_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
