"""Controllo di completezza INDIPENDENTE per SAVE: non importa builder/preflight/valutatori.
Estrae la riga SAVE e le sezioni di controllo dalla fonte grezza #461; verifica chiave, sha, copertura, guasti, drawdown."""
import json,re,hashlib,sys
raw={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
rows=[(cid,m.group(1)) for cid,b in raw.items() for m in re.finditer(r'^\| SAVE \| [^|]+ \| (.+?) \[[A-Z]+\] \|',b,re.M)]
assert len(rows)==1; row_cid,row=rows[0]; fonte=[t.strip() for t in row.split(';')]
def section(rid):
    for cid,b in raw.items():
        m=re.search(r'^#+ '+re.escape(rid)+r' .*?(?=^#+ RSR-|\Z)',b,re.M|re.S)
        if m: return cid,m.group(0)
    return None,''
R=json.load(open(sys.argv[1])); out={}
for r in R:
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]; texts=[c['text'] for c in r['clauses']]
    for f in fonte:
        if f not in texts: e.append('clausola di fonte mancante: '+f)
    cid,sec=section(rid)
    k=re.search(r'chiave `([^`]+)`',sec or '')
    if not k or k.group(1)!=r['key']: e.append('chiave diversa dalla fonte')
    refs={s['ref'] for s in r['sources']}
    for need in (f'#461/{row_cid}',f'#461/{cid}'):
        if need not in refs: e.append('fonte non citata '+need)
    for s in r['sources']:
        sid=s['ref'].split('/')[-1]
        if sid in raw:
            h=hashlib.sha256(raw[sid].encode()).hexdigest()
            if s.get('sha256',h)!=h or s.get('sha256_registrato',h)!=h: e.append('sha '+s['ref'])
    sc={x['fixtures']['scenario'].get('nome') for x in r['cases']}
    for need in ('ok','commit_fallito','commit_fallito_con_precedente','versione_fallita','invariato'):
        if need not in sc: e.append('scenario mancante '+need)
    for c in r['clauses']:
        ks={x['kind'] for x in r['cases'] if c['cid'] in x['covers']}
        if not {'positive','negative','boundary'}<=ks: e.append('clausola senza P/N/B '+c['cid'])
    for x in r['cases']:
        if x.get('osservato_028cee9') is None or x.get('verdetto_osservato_vs_oracolo') not in ('CONFORME','NON_CONFORME','NON_APPLICABILE','NON_ESERCITABILE'): e.append('caso incompleto '+x['cid'])
    if 'drawdown' in r['key']:
        m={(x['fixtures']['scenario'].get('modo'),x['fixtures']['scenario'].get('nome','')) for x in r['cases'] if 'O1' in x['covers']}
        for need in (('SIM','10.00'),('LIVE','10.00'),('SIM','9.99'),('LIVE','11.00'),('LIVE','50%')):
            if not any(a==need[0] and need[1] in b for a,b in m): e.append('drawdown manca '+str(need))
    out[r['difference_id']]=e
json.dump(dict(fonte_riga=row_cid,clausole_fonte=fonte,esiti=out),open('indep_L8b.json','w'),indent=1,ensure_ascii=False)
print('clausole di fonte:',fonte); print(sum(not v for v in out.values()),'/',len(out),'completi'); [print(k,v[:3]) for k,v in out.items() if v]
