#!/usr/bin/env python3
"""Reconcile a Nuthatch UBIScheme nest against the contract's own daily ledger.

For every protocol day fully covered by the nest, compares
  count(UBIClaimed)  vs getClaimerCount(day)
  sum(amount)        vs getClaimAmount(day)
Both reads are eth_call at one pinned block (R1: never "latest").
usage: reconcile_nest.py NEST_URL RPC_URL UBISCHEME_ADDR [--json out.json]
"""
import sys, json, urllib.request

def keccak256(data: bytes) -> bytes:
    RC=[0x0000000000000001,0x0000000000008082,0x800000000000808A,0x8000000080008000,0x000000000000808B,0x0000000080000001,0x8000000080008081,0x8000000000008009,0x000000000000008A,0x0000000000000088,0x0000000080008009,0x000000008000000A,0x000000008000808B,0x800000000000008B,0x8000000000008089,0x8000000000008003,0x8000000000008002,0x8000000000000080,0x000000000000800A,0x800000008000000A,0x8000000080008081,0x8000000000008080,0x0000000080000001,0x8000000080008008]
    R=[[0,36,3,41,18],[1,44,10,45,2],[62,6,43,15,61],[28,55,25,21,56],[27,20,39,8,14]]
    M=(1<<64)-1
    rol=lambda x,n:((x<<n)|(x>>(64-n)))&M if n else x
    def f(A):
        for rc in RC:
            C=[A[x][0]^A[x][1]^A[x][2]^A[x][3]^A[x][4] for x in range(5)]
            D=[C[(x-1)%5]^rol(C[(x+1)%5],1) for x in range(5)]
            A=[[A[x][y]^D[x] for y in range(5)] for x in range(5)]
            B=[[0]*5 for _ in range(5)]
            for x in range(5):
                for y in range(5): B[y][(2*x+3*y)%5]=rol(A[x][y],R[x][y])
            A=[[B[x][y]^((~B[(x+1)%5][y])&B[(x+2)%5][y]) for y in range(5)] for x in range(5)]
            A[0][0]^=rc
        return A
    rate=136; p=bytearray(data); p.append(0x01)
    while len(p)%rate: p.append(0)
    p[-1]|=0x80
    A=[[0]*5 for _ in range(5)]
    for off in range(0,len(p),rate):
        for i in range(rate//8):
            A[i%5][i//5]^=int.from_bytes(p[off+8*i:off+8*i+8],"little")
        A=f(A)
    out=b"".join(A[i%5][i//5].to_bytes(8,"little") for i in range(4))
    return out
assert keccak256(b"").hex()=="c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"
sel=lambda s:"0x"+keccak256(s.encode())[:4].hex()

def post(url, body):
    req=urllib.request.Request(url,json.dumps(body).encode(),{"content-type":"application/json","user-agent":"curl/8"})
    return json.load(urllib.request.urlopen(req,timeout=120))
def sql(nest,q): 
    r=post(nest.rstrip("/")+"/sql",{"q":q,"max_rows":100000})
    if "error" in r: raise SystemExit(r["error"])
    return r
def call(rpc,to,data,block):
    r=post(rpc,{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"to":to,"data":data},hex(block)]})
    if "error" in r: raise RuntimeError(r["error"])
    return int(r["result"],16)

nest,rpc,addr=sys.argv[1:4]
out=sys.argv[sys.argv.index("--json")+1] if "--json" in sys.argv else None
pad=lambda n:format(n,"064x")

def rpcj(m,params): 
    r=post(rpc,{"jsonrpc":"2.0","id":1,"method":m,"params":params}); 
    if "error" in r: raise RuntimeError(r["error"])
    return r["result"]
pin=int(rpcj("eth_getBlockByNumber",["finalized",False])["number"],16)   # pinned, numeric from here on
period_start=call(rpc,addr,sel("periodStart()")+"",pin)
cov=sql(nest,"select 1")  # liveness
prov=sql(nest,"select max(block_number) hi from ubi_scheme__u_b_i_claimed")["provenance"]
sealed=prov["sealed_through"]
sealed_ts=int(rpcj("eth_getBlockByNumber",[hex(sealed-1),False])["timestamp"],16)
first=sql(nest,"select min(block_timestamp) t0 from ubi_scheme__u_b_i_claimed")["rows"][0]["t0"]
# days fully inside [first covered second, sealed_ts]; nest starts at the contract's deployment block, so nothing precedes it
d_lo=max(0,-(-(first-period_start)//86400)) if first>period_start else 0
d_lo=(first-period_start)//86400  # first claim's own day: complete, the nest began before any claim
d_hi=(sealed_ts-period_start)//86400-1
q=(f"select cast(floor((block_timestamp-{period_start})/86400) as bigint) d, count(*) n, "
   f"cast(sum(cast(amount as hugeint)) as varchar) s from ubi_scheme__u_b_i_claimed "
   f"where block_timestamp>={period_start+d_lo*86400} and block_timestamp<{period_start+(d_hi+1)*86400} group by 1 order by 1")
got={r["d"]:(int(r["n"]),int(r["s"])) for r in sql(nest,q)["rows"]}
res=[];bad=[];missing=0
for d in range(d_lo,d_hi+1):
    n,s_=got.get(d,(0,0))
    cn=call(rpc,addr,sel("getClaimerCount(uint256)")+pad(d),pin)
    ca=call(rpc,addr,sel("getClaimAmount(uint256)")+pad(d),pin)
    ok=(n==cn and s_==ca)
    res.append({"day":d,"nest_claims":n,"chain_claims":cn,"nest_amount":s_,"chain_amount":ca,"match":ok})
    if not ok: bad.append(res[-1])
summary={"pinned_block":pin,"period_start":period_start,"days_checked":len(res),"days_matching":len(res)-len(bad),"sealed_through":sealed,"first_day":d_lo,"last_day":d_hi,"mismatches":bad[:20]}
print(json.dumps(summary,indent=1))
if out: json.dump({"summary":summary,"days":res},open(out,"w"),indent=1)
sys.exit(1 if bad else 0)
