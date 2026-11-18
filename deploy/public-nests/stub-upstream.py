"""A stand-in nest for test-proxy.sh: answers 200 on every path it is asked for.

That is the point. Behind this, any path the proxy lets through is a 200, so a 403 or 404 in
check-proxy.sh can only have come from the proxy. `?q=slow` hangs for 25 s, longer than the proxy's
15 s read timeout, to prove the backstop.
"""
import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Stub(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _answer(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n).decode("utf-8", "replace") if n else ""
        if "q=slow" in self.path or "slow" in body:
            time.sleep(25)
        status = 200
        if "COPY" in body or "DELETE" in body:
            status = 400
            out = {"error": "only SELECT/WITH queries are allowed on the read-only SQL surface"}
        elif self.path.startswith("/mcp"):
            out = {"jsonrpc": "2.0", "id": 1, "result": {"tools": []}}
        else:
            out = {"stub": True, "path": self.path, "host": self.headers.get("host")}
        data = json.dumps(out).encode()
        self.send_response(status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = _answer

    def log_message(self, *a):
        pass

    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError):
            pass  # the proxy hung up on a request it timed out


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1])), Stub).serve_forever()
