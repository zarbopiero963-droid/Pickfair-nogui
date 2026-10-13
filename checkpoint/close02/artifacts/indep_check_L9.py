"""Controllo INDIPENDENTE RESTART: non importa builder/preflight/valutatore. Estrae dalla fonte grezza la riga RESTART e le sezioni RSR."""
import json,re,hashlib,sys
raw={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
rows=[(cid,m.group(1)) for cid,b in raw.items() for m in re.finditer(r'^\| RESTART \| [^|]+ \| (.+?) \[[A-Z]+\] \|',b,re.M)]
assert len(rows)==1; row_cid,row=rows[0]; fonte=[t.strip() for t in row.split(';')]
def section(rid):
    for cid,b in raw.items():
        m=re.search(r'^#+ '+re.escape(rid)+r' .*?(?=^#+ RSR-|\Z)',b,re.M|re.S)
        if m: return cid,m.group(0)
    return None,''
ids=sorted({m for b in raw.values() for m in re.findall(r'^#+ (RSR-\d\d) ',b,re.M)})
R={r['difference_id']:r for r in json.load(open(sys.argv[1]))}; out={}
attesi=[i for i in ids if int(i[4:])>=4]
for rid in attesi:
    d='DIFF-PF-%s-RESTART'%rid; e=[]
    if d not in R: out[d]=['record mancante']; continue
    r=R[d]; texts=[c['text'] for c in r['clauses']]
    for f in fonte:
        if f not in texts: e.append('clausola mancante: '+f)
    cid,sec=section(rid); k=re.search(r'chiave `([^`]+)`',sec)
    if not k or k.group(1)!=r['key']: e.append('chiave')
    for s in r['sources']:
        if s['sha256']!=hashlib.sha256(raw[s['ref'].split('/')[-1]].encode()).hexdigest(): e.append('sha')
    if '#461/'+row_cid not in {s['ref'] for s in r['sources']}: e.append('riga RESTART non citata')
    sc=' '.join(x['fixtures']['scenario'] for x in r['cases'])
    for need in ('commit_poi_kill','kill_in_transazione','esito_incerto','fantasma'):
        if need not in sc: e.append('scenario '+need)
    if not any(x['osservato_028cee9'].get('returncode_A')==-9 for x in r['cases']): e.append('nessun SIGKILL osservato')
    if any(x['verdetto_osservato_vs_oracolo'] not in('CONFORME','NON_CONFORME','NON_ESERCITABILE') for x in r['cases']): e.append('verdetto')
    if re.search(r'drawdown',r['key']) and 'pct' not in r['key'] or rid in('RSR-05','RSR-06','RSR-07','RSR-20'):
        if not any('O1' in x['covers'] and '11,00' in x['fixtures']['scenario'] for x in r['cases']) or not any('9,99' in x['fixtures']['scenario'] for x in r['cases']) or not any('perdita 10,00' in x['fixtures']['scenario'] for x in r['cases']): e.append('drawdown: mancano perdite 11,00/9,99/10,00 dopo riavvio')
    out[d]=e
extra=set(R)-{'DIFF-PF-%s-RESTART'%i for i in attesi}
json.dump(dict(riga=row_cid,clausole=fonte,attesi=len(attesi),extra=sorted(extra),esiti=out),open('indep_L9.json','w'),indent=1,ensure_ascii=False)
print('fonte',fonte,'attesi',len(attesi),'extra',extra); print(sum(not v for v in out.values()),'/',len(out),'completi'); [print(k,v) for k,v in out.items() if v]
