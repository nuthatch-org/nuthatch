#!/usr/bin/env python3
"""Local preview of the showcase. Serves this directory and forwards /nests/* to a real host, so the page is
same-origin with the nest's /sql, /ready and /tables, as it is when deployed. The nest sends no CORS headers.

    python3 showcase/serve.py [--port 8099] [--upstream https://nuthatch-indexer.com]
"""
import argparse, http.server, os, urllib.error, urllib.request

ap = argparse.ArgumentParser()
ap.add_argument("--port", type=int, default=8099)
ap.add_argument("--upstream", default="https://nuthatch-indexer.com")
args = ap.parse_args()
ROOT = os.path.dirname(os.path.abspath(__file__))


class H(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=ROOT, **k)

    def _proxy(self):
        n = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(n) if n else None
        req = urllib.request.Request(args.upstream + self.path, data=body, method=self.command)
        for h in ("content-type", "accept"):
            if self.headers.get(h):
                req.add_header(h, self.headers[h])
        req.add_header("user-agent", "nuthatch-showcase-preview")
        try:
            r = urllib.request.urlopen(req, timeout=30)
        except urllib.error.HTTPError as e:
            r = e
        data = r.read()
        self.send_response(r.status if hasattr(r, "status") else r.code)
        self.send_header("content-type", r.headers.get("content-type", "application/json"))
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self._proxy() if self.path.startswith("/nests/") else super().do_GET()

    def do_POST(self):
        self._proxy() if self.path.startswith("/nests/") else self.send_error(405)

    def end_headers(self):
        self.send_header("cache-control", "no-store")
        super().end_headers()


http.server.ThreadingHTTPServer(("127.0.0.1", args.port), H).serve_forever()
