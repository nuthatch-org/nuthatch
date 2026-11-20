import sys, os, json, threading, urllib.request, urllib.error, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
port=int(sys.argv[1]); upstream=os.environ['UPSTREAM']; stats_path=sys.argv[2]
CU={'eth_getLogs':75,'eth_blockNumber':10,'eth_getBlockByNumber':16,'eth_call':26,'eth_getBlockByHash':16,'eth_chainId':0,'eth_getCode':19,'eth_getTransactionReceipt':20}
lock=threading.Lock(); st={'methods':{},'cu':0,'http':0,'errors':0,'rate_limited':0}
def save():
    with open(stats_path,'w') as f: json.dump(st,f)
class H(BaseHTTPRequestHandler):
    def log_message(self,*a): pass
    def do_POST(self):
        body=self.rfile.read(int(self.headers.get('content-length',0)))
        try: j=json.loads(body); items=j if isinstance(j,list) else [j]
        except Exception: items=[]
        req=urllib.request.Request(upstream,data=body,headers={'content-type':'application/json','user-agent':'curl/8'})
        try:
            r=urllib.request.urlopen(req,timeout=120); data=r.read(); code=r.status
        except urllib.error.HTTPError as e: data=e.read(); code=e.code
        except Exception: data=b'{}'; code=502
        with lock:
            st['http']+=1
            if code==429: st['rate_limited']+=1
            if code>=400: st['errors']+=1
            for it in items:
                m=it.get('method','?'); st['methods'][m]=st['methods'].get(m,0)+1
                st['cu']+=CU.get(m,26)
            save()
        self.send_response(code); self.send_header('content-type','application/json'); self.send_header('content-length',str(len(data))); self.end_headers(); self.wfile.write(data)
ThreadingHTTPServer(('127.0.0.1',port),H).serve_forever()
