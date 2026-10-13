"""Verifica indipendente di completezza L7: risonda in processo separato, ricalcola sha, copertura clausole, specificità, no €/cap su chiavi non monetarie."""
import json,hashlib,subprocess,os,shutil,re,collections
R=json.load(open('record_specific_L7.json'))
shutil.copy('probe_load_v2.json','/tmp/probe_prev.json')
subprocess.run(['/workspace/pickfair/venv311/bin/python','probe_load_v2.py',open('load_keys.json').read()],check=True,capture_output=True)
NEW=json.load(open('probe_load_v2.json')); OLD=json.load(open('/tmp/probe_prev.json'))
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('../s3_461.jsonl')}
res=[]
for r in R:
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]
    if NEW[rid]!=OLD[rid]: e.append('sonda non riproducibile')
    for s in r['sources']:
        if hashlib.sha256(S[s['ref'].split('/')[-1]].encode()).hexdigest()!=s['sha256_ricalcolato']: e.append('sha '+s['ref'])
    for c in r['clauses']:
        ks={z['kind'] for z in r['cases'] if c['cid'] in z['covers'] or z['cid'].split('-LOAD-')[1].split('-')[0] in (c['cid'],)}
        ks2={z['kind'] for z in r['cases'] if z['oracle'] and z['oracle'][0]['text'] and c['text'][:15] in json.dumps(r['clauses'])}
        if not ({'positive','negative','boundary'}<=ks or 'not_applicable' in ks): pass
    cov=collections.defaultdict(set)
    for z in r['cases']: cov[z['cid'].split('-LOAD-')[1].rsplit('-',1)[0]].add(z['kind'])
    if len(cov)<len(r['clauses'])-len(r['clausole_non_verificate']): e.append(f'clausole coperte {len(cov)}/{len(r["clauses"])}')
    for k,v in cov.items():
        if not({'positive','negative','boundary'}<=v or v=={'not_applicable'}): e.append('copertura '+k+str(v))
    for z in r['cases']:
        if True:
            if r['key'] not in json.dumps(z['fixtures'],ensure_ascii=False)+z['oracle'][0]['text'] and r['key'].split('.')[1] not in z['oracle'][0]['text'] and r['key'] not in json.dumps(z['fixtures']) : e.append('oracolo non specifico '+z['cid'])
            if z['osservato_028cee9'] is None: e.append('senza osservato '+z['cid'])
        if r['unit']!='EUR' and re.search(r'€|cap\b|importo',json.dumps(z,ensure_ascii=False).replace('valore monetario','').replace('regola €/cap','')): e.append('money su non-EUR '+z['cid'])
    # ricalcolo indipendente di un sottoinsieme di verdetti
    o=NEW[rid]; NA='<attributo assente in RoserpinaConfig>'
    for z in r['cases']:
        t=z['cid']; v=z['verdetto_osservato_vs_oracolo']
        if t.endswith('-boundary') and '-D1-' in t and o['riavvio']['valore'] in (NA,) and v=='CONFORME': e.append('verdetto riavvio errato '+t)
        if any(x in t for x in ('-P1-negative','-D2-negative')) and v=='CONFORME' and not (o['raw_abc']['log_warn'] or any(q not in ('()','{}','') for q in o['raw_abc']['flags'].values())): e.append('verdetto corrotto errato '+t)
        if '-P2-negative' in t and v=='CONFORME' and not o['raw_nan']['log_warn']: e.append('verdetto nan errato '+t)
    for z in r['cases']:
        a=z.get('ancora_normativa') or {}
        if not a.get('citazione_verbatim') or a['citazione_verbatim'] not in S[a['fonte'].split('/')[-1]]: e.append('ancora non verbatim '+z['cid'])
        if z['oracle'][0]['provenance']!='REQUIREMENT': e.append('provenienza '+z['cid'])
    texts=[z['oracle'][0]['text'] for z in r['cases']]
    if len(set(texts))!=len(texts): e.append('oracoli duplicati')
    if r['clausole_non_verificate']: e.append('clausole non verificate')
    res.append(dict(id=r['difference_id'],errori=e,verificato_indipendente=not e))
json.dump(res,open('verify_L7.json','w'),indent=1,ensure_ascii=False)
print(sum(x['verificato_indipendente'] for x in res),'/',len(res)); [print(x['id'],x['errori'][:4]) for x in res if x['errori']]
