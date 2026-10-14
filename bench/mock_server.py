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


if __name__ == "__main__":
    srv, url = start(8765)
    print("serving", url)
    threading.Event().wait()
