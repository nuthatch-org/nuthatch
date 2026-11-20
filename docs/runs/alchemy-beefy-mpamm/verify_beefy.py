import json,urllib.request,urllib.parse,collections,sys
API='http://127.0.0.1:18204'; RPC='http://127.0.0.1:18104'
def sql(q):
    return json.load(urllib.request.urlopen(API+'/sql?'+urllib.parse.urlencode({'q':q}),timeout=120))
def rpc(m,p):
    r=urllib.request.Request(RPC,json.dumps({'jsonrpc':'2.0','id':1,'method':m,'params':p}).encode(),{'content-type':'application/json'})
    return json.load(urllib.request.urlopen(r,timeout=60))
Z='0x'+'0'*40
tot=0;bad=0;res=[]
bn=None
for t in ['classic_vault','reward_pool','clm_manager']:
    r=sql(f'select address,"from","to",value_dec,block_number from {t}__transfer')
    bn=r['provenance']['as_of']
    bn=min(bn,r['provenance']['sealed_through'])  # blocks both sides have
    sup=collections.Counter(); bal=collections.defaultdict(collections.Counter)
    for x in r['rows']:
        if x['block_number']>bn: continue
        v=int(x['value_dec']); a=x['address']
        if x['from']==Z: sup[a]+=v
        else: bal[a][x['from']]-=v
        if x['to']==Z: sup[a]-=v
        else: bal[a][x['to']]+=v
        if x['from']==Z: bal[a][Z]+=0
    for a in sorted(set(sup)|set(bal)):
        out=rpc('eth_call',[{'to':a,'data':'0x18160ddd'},hex(bn)])
        ok=('result' in out and int(out['result'],16)==sup[a]); tot+=1; bad+= (not ok)
        res.append((t,a,'supply',sup[a],out.get('result',out.get('error')),ok))
        # top 2 holders balanceOf
        top=sorted(((h,v) for h,v in bal[a].items() if h!=Z),key=lambda x:-x[1])[:2]
        for h,v in top:
            o=rpc('eth_call',[{'to':a,'data':'0x70a08231'+h[2:].rjust(64,'0')},hex(bn)])
            ok=('result' in o and int(o['result'],16)==v); tot+=1; bad+=(not ok)
            res.append((t,a,'bal '+h[:10],v,o.get('result',o.get('error')),ok))
print('checked_at_block',bn,'checks',tot,'pass',tot-bad,'fail',bad)
for r in res:
    if not r[5]: print('FAIL',r)
json.dump(res,open('/home/grokbot/abm-run/beefy-verify.json','w'))
