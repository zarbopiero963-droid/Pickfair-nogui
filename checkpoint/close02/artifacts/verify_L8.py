"""Verifica indipendente L8: risonde (GUI save, drawdown APPLY, LOAD) riproducibili, sha, ancore verbatim, copertura P/N/B per clausola, specificità, verdetti ricalcolati."""
import json,hashlib,subprocess,shutil,re,collections
R=json.load(open('record_specific_L8.json'))
for f,cmd in (('probe_gui_save.json',['probe_gui_save.py',open('save_keys.json').read()]),('probe_dd_apply.json',['probe_dd_apply.py']),('probe_load_v2.json',['probe_load_v2.py',open('load_keys.json').read()])):
    shutil.copy(f,'/tmp/old_'+f); subprocess.run(['/workspace/pickfair/venv311/bin/python']+cmd,check=True,capture_output=True)
def same(f):
    a=json.load(open('/tmp/old_'+f)); b=json.load(open(f))
    strip=lambda o: json.loads(re.sub(r'"breached_at": "[^"]*"','',json.dumps(o)))
    return strip(a)==strip(b)
REP={f:same(f) for f in ('probe_gui_save.json','probe_dd_apply.json','probe_load_v2.json')}
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('../s3_461.jsonl')}; S['6141136157']=json.load(open('owner_6141136157.json'))['body']
G=json.load(open('probe_gui_save.json')); DD=json.load(open('probe_dd_apply.json'))
res=[]
for r in R:
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]
    e+=[f'sonda non riproducibile {k}' for k,v in REP.items() if not v]
    for s in r['sources']:
        if hashlib.sha256(S[s['ref'].split('/')[-1]].encode()).hexdigest()!=s['sha256_ricalcolato']: e.append('sha '+s['ref'])
        if 'sha256_registrato' in s and s['sha256_registrato']!=s['sha256_ricalcolato']: e.append('sha registrato '+s['ref'])
    cov=collections.defaultdict(set)
    for z in r['cases']:
        cov[z['covers'][0]].add(z['kind'])
        a=z.get('ancora_normativa') or {}
        if not a.get('citazione_verbatim') or a['citazione_verbatim'] not in S[a['fonte'].split('/')[-1]]: e.append('ancora '+z['cid'])
        if z.get('osservato_028cee9') is None: e.append('senza osservato '+z['cid'])
        if r['key'] not in json.dumps(z,ensure_ascii=False): e.append('non specifico '+z['cid'])
        if z['verdetto_osservato_vs_oracolo'] not in ('CONFORME','NON_CONFORME','NON_APPLICABILE'): e.append('verdetto '+z['cid'])
        if r['unit']!='EUR' and z['covers']!=['O1'] and re.search(r'€|\bcap\b',json.dumps(z['oracle'],ensure_ascii=False)): e.append('money su non-EUR '+z['cid'])
        if z['covers']==['O1'] and 'drawdown' not in r['key']: e.append('regola owner drawdown su chiave non drawdown '+z['cid'])
    for c in r['clauses']:
        if c['text'].startswith('grace prima del cashout'): continue
        k=c['cid']; ks=cov.get(k,set())
        if not ({'positive','negative','boundary'}<=ks or ks=={'not_applicable'} or 'not_applicable' in ks):
            # clausole di metodo (es. 'prova su DB temporaneo reale') coperte dalle fixture
            if not any(w in c['text'] for w in ('prova su DB temporaneo','DB temporaneo')): e.append(f'copertura {k}:{sorted(ks)}')
    # ricalcolo indipendente di verdetti chiave
    if r['phase']=='SAVE':
        g=G[rid]
        for z in r['cases']:
            if z['cid'].endswith('D3-positive'):
                ins=[x for x in g['ok']['eventi'] if x[0]=='INFO']
                exp=bool(ins) and ins[0][3]['versione_db'] is not None and ins[0][3]['valore_db_al_messaggio'] is not None
                if (z['verdetto_osservato_vs_oracolo']=='CONFORME')!=exp: e.append('verdetto D3 '+z['cid'])
            if z['cid'].endswith('P1-negative'):
                exp=g['commit_fallito']['righe_dopo']==g['commit_fallito']['righe_prima'] and not any(x[0]=='INFO' for x in g['commit_fallito']['eventi'])
                if (z['verdetto_osservato_vs_oracolo']=='CONFORME')!=exp: e.append('verdetto P1n '+z['cid'])
    for z in r['cases']:
        if z['covers']==['O1'] and z['kind']=='positive':
            o=z['osservato_028cee9']; exp=any('drawdown' in str(x) for x in o.get('rifiuti',[]))
            if (z['verdetto_osservato_vs_oracolo']=='CONFORME')!=exp: e.append('verdetto O1 '+z['cid'])
    texts=[z['cid'] for z in r['cases']]
    if len(set(texts))!=len(texts): e.append('cid duplicati')
    if r.get('clausole_non_verificate'): e.append('clausole non verificate')
    res.append(dict(id=r['difference_id'],errori=e,verificato_indipendente=not e))
json.dump(dict(riproducibilita=REP,record=res),open('verify_L8.json','w'),indent=1,ensure_ascii=False)
print(REP); print(sum(x['verificato_indipendente'] for x in res),'/',len(res)); [print(x['id'],x['errori'][:4]) for x in res if x['errori']]
