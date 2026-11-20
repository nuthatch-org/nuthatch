# independent count of all logs for one address over [from,to] via plain eth_getLogs, 100k-block windows
import sys, json, urllib.request, time
rpc, addr, lo, hi = sys.argv[1], sys.argv[2], int(sys.argv[3]), int(sys.argv[4])
def call(body):
    for a in range(6):
        try:
            r = urllib.request.Request(rpc, json.dumps(body).encode(), {"content-type":"application/json","user-agent":"curl/8"})
            j = json.load(urllib.request.urlopen(r, timeout=60))
            if "error" not in j: return j
        except Exception: pass
        time.sleep(2*(a+1))
    raise SystemExit("rpc failed")
n=0; calls=0; w=100000
b=lo
while b<=hi:
    e=min(b+w-1,hi)
    j=call({"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":hex(b),"toBlock":hex(e),"address":addr}]}); calls+=1
    n+=len(j["result"]); b=e+1
print(json.dumps({"address":addr,"from":lo,"to":hi,"logs":n,"getlogs_calls":calls}))
