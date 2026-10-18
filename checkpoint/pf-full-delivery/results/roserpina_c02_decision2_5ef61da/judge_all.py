import json,glob,sys,collections
sys.path.insert(0,'.')
from oracle import judge
out=sys.argv[1]; res=[]
for d in sorted(glob.glob(out+'/[SDXF]*')):
    try: res.append(judge(d))
    except Exception as e: res.append({'id':d.split('/')[-1],'verdict':'BLOCKED','checks':{'ORACLE_ERROR':{'status':'BLOCKED','detail':repr(e)}}})
json.dump(res,open(out+'/verdicts.json','w'))
v=collections.Counter(r['verdict'] for r in res); print(dict(v))
cc=collections.defaultdict(collections.Counter)
for r in res:
    for k,c in r['checks'].items():
        if not k.startswith('_'): cc[k][c['status']]+=1
for k in sorted(cc): print(f"  {k:45s} {dict(cc[k])}")
for r in res:
    for k,c in r['checks'].items():
        if c['status'] in ('VIOLATION','BLOCKED'): print(r['id'],r.get('sequence'),r.get('capital'),k,c['detail'][:250])
