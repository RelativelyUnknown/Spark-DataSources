"""Deterministic paginated JSON API: GET /records?offset=&limit= -> [{id,value,name,ts}, ...]."""
import json
import threading
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit


@lru_cache(maxsize=512)
def page_body(offset: int, limit: int) -> bytes:
    return json.dumps(
        [
            {"id": i, "value": (i % 1000) / 8, "name": f"user-{i}", "ts": 1_700_000_000 + i}
            for i in range(offset, offset + limit)
        ],
        separators=(",", ":"),
    ).encode()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        u = urlsplit(self.path)
        if u.path != "/records":
            self.send_error(404)
            return
        q = parse_qs(u.query)
        body = page_body(int(q.get("offset", ["0"])[0]), int(q.get("limit", ["100"])[0]))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def start(port: int = 0) -> tuple[ThreadingHTTPServer, str]:
    """Start in a background thread; returns (server, base_url)."""
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


if __name__ == "__main__" and "--async" not in __import__("sys").argv:
    srv, url = start(8765)
    print("serving", url)
    threading.Event().wait()


# ------------------------------------------------------------------ async server (own process)
# Latency-injecting variant for the fan-out benchmark: `delay_ms` per request, HTTP/1.1 keep-alive, and it
# runs in a separate process so neither the Spark driver's GIL nor the server's thread count can be the limit.
import asyncio  # noqa: E402
import os  # noqa: E402
import socket  # noqa: E402


async def _serve_conn(reader, writer):
    try:
        while True:
            head = await reader.readuntil(b"\r\n\r\n")
            target = head.split(b" ", 2)[1].decode()
            u = urlsplit(target)
            q = parse_qs(u.query)
            if u.path != "/records":
                writer.write(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n")
                await writer.drain()
                continue
            delay = float(q.get("delay_ms", ["0"])[0]) / 1000
            body = page_body(int(q.get("offset", ["0"])[0]), int(q.get("limit", ["100"])[0]))
            if delay:
                await asyncio.sleep(delay)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                         + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError):
        pass
    finally:
        writer.close()


def _serve_forever(announce=True):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(4096)
    if announce:
        print(f"http://127.0.0.1:{sock.getsockname()[1]}", flush=True)

    async def main():
        srv = await asyncio.start_server(_serve_conn, sock=sock, backlog=4096)
        async with srv:
            await srv.serve_forever()

    asyncio.run(main())


def start_async_process():
    """Run the asyncio mock as a plain subprocess (no fork/spawn: the parent may host a JVM and be
    driven from stdin). Returns (process, base_url); call .terminate() when done."""
    import subprocess
    import sys

    p = subprocess.Popen([sys.executable, os.path.abspath(__file__), "--async"], stdout=subprocess.PIPE, text=True)
    return p, p.stdout.readline().strip()


if __name__ == "__main__" and "--async" in __import__("sys").argv:
    _serve_forever()
