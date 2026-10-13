import json,hashlib,sys
from eval_pred_restart import verdetto
raw={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
SENT={'<assente>','NON_DEFINITO','?',''}
def check(R):
    res={}; seen=set()
    for r in R:
        e=[]
        for s in r['sources']:
            h=hashlib.sha256(raw[s['ref'].split('/')[-1]].encode()).hexdigest()
            if s['sha256']!=h or s['sha256_registrato']!=h: e.append('SHA')
        cids={c['cid'] for c in r['clauses']}
        for c in r['clauses']:
            if c['natura']=='PRIMARIA_VERBATIM' and c['text'] not in raw['6085510337']: e.append('S5 origine '+c['cid'])
            ks={x['kind'] for x in r['cases'] if c['cid'] in x['covers']}
            if not {'positive','negative','boundary'}<=ks: e.append('S6 P/N/B '+c['cid'])
        for x in r['cases']:
            if x['cid'] in seen: e.append('cid duplicato')
            seen.add(x['cid'])
            if not set(x['covers'])<=cids: e.append('S6 covers '+x['cid'])
            if verdetto(x)!=x['verdetto_osservato_vs_oracolo']: e.append('S7 '+x['cid'])
            if isinstance(x['atteso'],str) and x['atteso'] in SENT: e.append('S8 '+x['cid'])
            if x['predicato']['op'] in('riavvio_eq',) and str(x['atteso'])!=str(x['fixtures']['fixture']) and not (isinstance(x['atteso'],float) and x['atteso']==float(x['fixtures']['fixture'])) and not (isinstance(x['atteso'],bool) and str(x['atteso'])==x['fixtures']['fixture']): e.append('S1 atteso non da fixture '+x['cid'])
            o=x['osservato_028cee9']
            if 'sigkill' in o and o['sigkill'] is not True: e.append('S3 '+x['cid'])
        if not any(x['predicato']['op']=='metodo_db_reale' and x['verdetto_osservato_vs_oracolo']=='CONFORME' for x in r['cases']): e.append('S3 nessuna prova SIGKILL+nuovo processo')
        if 'O1' in cids and not any('O1' in x['covers'] and x['stato_oracolo']=='OWNER_IN_VIGORE' for x in r['cases']): e.append('O1 senza oracolo owner')
        res[r['difference_id']]=e
    return res
if __name__=='__main__':
    res=check(json.load(open(sys.argv[1])))
    print(sum(not v for v in res.values()),'/',len(res),'OK'); [print(k,v[:4]) for k,v in res.items() if v]
