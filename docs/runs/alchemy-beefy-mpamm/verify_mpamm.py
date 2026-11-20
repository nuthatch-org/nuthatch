import json,urllib.request,urllib.parse,random
API='http://127.0.0.1:18203'; RPC='http://127.0.0.1:18103'
def sql(q): return json.load(urllib.request.urlopen(API+'/sql?'+urllib.parse.urlencode({'q':q}),timeout=300))
def rpc(m,p):
    r=urllib.request.Request(RPC,json.dumps({'jsonrpc':'2.0','id':1,'method':m,'params':p}).encode(),{'content-type':'application/json'})
    return json.load(urllib.request.urlopen(r,timeout=120))
sch=json.load(open('/home/grokbot/abm-run/mpamm-dense/schema.json'))
T0=[t['topic0'] for t in sch['tables'] if t['table']=='capricorn_pool__swap'][0]
POOLS={'0x63093325c05cd32b18034d3ea29199fb7098e4df':(False,6,18),'0x91c483fbe8feef4dad525781e6d65d2e668b3a47':(False,6,18)} # baseIsToken0, stableDec, baseDec (token1=WMON)
def s256(h):
    v=int(h,16); return v-(1<<256) if v>>255 else v
def fill(a0,a1,bi0,sd,bd):  # capricorn.ts decodeCapricornSwap
    bd_,sd_=(a0,a1) if bi0 else (a1,a0)
    if bd_==0 or sd_==0 or (bd_>0)==(sd_>0): return None
    return ('sell' if bd_>0 else 'buy', abs(sd_)/10**sd, abs(bd_)/10**bd)
head=int(rpc('eth_blockNumber',[])['result'],16)
# nest rows up to sealed
r=sql('select address,block_number,log_index,tx_hash,amount0_dec,amount1_dec,reserve0_dec,reserve1_dec from capricorn_pool__swap')
sealed=r['provenance']['sealed_through']
nest={(x['tx_hash'],x['log_index']):x for x in r['rows'] if x['block_number']<=sealed}
# raw logs independently
raw={}
def fetch(lo,hi):
    out=rpc('eth_getLogs',[{'fromBlock':hex(lo),'toBlock':hex(hi),'address':list(POOLS),'topics':[T0]}])
    if 'error' in out:
        if hi==lo: raise Exception(out)
        m=(lo+hi)//2; fetch(lo,m); fetch(m+1,hi); return
    for l in out['result']:
        d=l['data'][2:]; w=[d[i*64:(i+1)*64] for i in range(4)]
        raw[(l['transactionHash'],int(l['logIndex'],16))]=dict(address=l['address'].lower(),block=int(l['blockNumber'],16),a0=s256(w[0]),a1=s256(w[1]),r0=int(w[2],16),r1=int(w[3],16))
lo=65199911
while lo<=sealed:
    fetch(lo,min(lo+2_000_000,sealed)); lo+=2_000_001
print('nest rows',len(nest),'raw logs',len(raw),'sealed_through',sealed)
missing=[k for k in raw if k not in nest]; extra=[k for k in nest if k not in raw]
print('raw not in nest',len(missing),'nest not in raw',len(extra))
mism=0
for k,x in nest.items():
    y=raw.get(k)
    if not y: continue
    if not(int(x['amount0_dec'])==y['a0'] and int(x['amount1_dec'])==y['a1'] and int(x['reserve0_dec'])==y['r0'] and int(x['reserve1_dec'])==y['r1'] and x['address']==y['address'] and x['block_number']==y['block']): mism+=1
print('field mismatches (amounts, reserves, address, block)',mism)
# capricorn.ts decode: priceable fills & totals
tn=tr=0; vn=vr=0.0; fm=0
for k,x in nest.items():
    f=fill(int(x['amount0_dec']),int(x['amount1_dec']),*[POOLS[x['address']][i] for i in (0,)],POOLS[x['address']][1],POOLS[x['address']][2])
    if f: tn+=1; vn+=f[1]
for k,y in raw.items():
    f=fill(y['a0'],y['a1'],False,6,18)
    if f: tr+=1; vr+=f[1]
print('priceable fills nest',tn,'raw',tr,'usd volume nest %.2f raw %.2f'%(vn,vr))
# on-chain reserves at block for 40 sampled swaps (last swap in its block per pool only, to be safe)
ks=list(nest); random.seed(1); samp=random.sample(ks,min(60,len(ks)))
ok=bad=skip=0
for k in samp:
    x=nest[k]
    same=[j for j in nest if nest[j]['block_number']==x['block_number'] and nest[j]['address']==x['address']]
    if max(nest[j]['log_index'] for j in same)!=x['log_index']: skip+=1; continue
    o=rpc('eth_call',[{'to':x['address'],'data':'0x0902f1ac'},hex(x['block_number'])])
    if 'result' not in o: bad+=1; print('callerr',o); continue
    d=o['result'][2:]; r0,r1=int(d[:64],16),int(d[64:128],16)
    if r0==int(x['reserve0_dec']) and r1==int(x['reserve1_dec']): ok+=1
    else: bad+=1; print('RESERVE MISMATCH',k,r0,r1,x['reserve0_dec'],x['reserve1_dec'])
print('getReserves at block vs nest reserve fields: pass',ok,'fail',bad,'skipped(not last log in block)',skip)
