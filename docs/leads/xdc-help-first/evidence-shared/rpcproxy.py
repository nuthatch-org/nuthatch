# Counting pass-through JSON-RPC proxy: usage rpcproxy.py PORT UPSTREAM COUNTFILE
import sys, json, threading, urllib.request, collections, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port, up, out = int(sys.argv[1]), sys.argv[2], sys.argv[3]
cnt = collections.Counter(); lock = threading.Lock(); t0 = time.time()
def dump():
    with lock: json.dump({"http_requests": cnt["_http"], "rpc_calls": sum(v for k,v in cnt.items() if not k.startswith("_")), "by_method": {k:v for k,v in cnt.items() if not k.startswith("_")}, "errors": cnt["_err"]}, open(out,"w"))
class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length",0)))
        try:
            j = json.loads(body); items = j if isinstance(j,list) else [j]
            with lock:
                cnt["_http"] += 1
                for i in items: cnt[i.get("method","?")] += 1
        except Exception: pass
        try:
            r = urllib.request.urlopen(urllib.request.Request(up, body, {"content-type":"application/json","user-agent":"curl/8"}), timeout=60).read(); code=200
        except Exception as e:
            r = json.dumps({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":str(e)}}).encode(); code=200
            with lock: cnt["_err"] += 1
        self.send_response(code); self.send_header("content-type","application/json"); self.send_header("content-length",str(len(r))); self.end_headers(); self.wfile.write(r); dump()
ThreadingHTTPServer(("127.0.0.1",port),H).serve_forever()
