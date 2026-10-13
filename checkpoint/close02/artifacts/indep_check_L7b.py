"""Controllo di completezza INDIPENDENTE (non usa builder/preflight/valutatore):
estrae le clausole direttamente dalla fonte grezza #461 e confronta con i record."""
import json,re,hashlib,sys
raw={}
for l in open('/workspace/pickfair/bf2/s3_461.jsonl'):
    d=json.loads(l); raw[str(d['id'])]=d['body']
rows=[(cid,m.group(1)) for cid,b in raw.items() for m in re.finditer(r'^\| LOAD \| [^|]+ \| (.+?) \[[A-Z]+\] \|',b,re.M)]
assert rows, 'riga LOAD non trovata'
row_cid,row=rows[0]; fonte_clausole=[t.strip() for t in row.split(';')]
def section(rid):
    for cid,b in raw.items():
        m=re.search(r'^#+ '+re.escape(rid)+r' .*?(?=^#+ RSR-|\Z)',b,re.M|re.S)
        if m: return cid,m.group(0)
    return None,''
R=json.load(open(sys.argv[1])); out={}
for r in R:
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]
    texts=[c['text'] for c in r['clauses']]
    for fc in fonte_clausole:
        if fc not in texts: e.append('clausola di fonte mancante: '+fc)
    cid,sec=section(rid)
    if not sec: e.append('sezione controllo non trovata')
    else:
        k=re.search(r'chiave `([^`]+)`',sec); t=re.search(r'tipo `([^`]+)`, unità `([^`]+)`',sec)
        if not k or k.group(1)!=r['key']: e.append('chiave diversa dalla fonte')
        if t and r.get('tipo_fonte')!=t.group(1): e.append('tipo diverso dalla fonte')
        if 'vuoto=non impostato' in sec and not any(c['fixtures']['scenario'].get('raw')=='' and c['atteso_predicato'].get('atteso')=='None' for c in r['cases']): e.append('vuoto=non impostato senza caso atteso None')
        refs={s['ref'] for s in r['sources']}
        if f'#461/{cid}' not in refs: e.append('fonte sezione controllo non citata')
    if f'#461/{row_cid}' not in {s['ref'] for s in r['sources']}: e.append('fonte riga di fase non citata')
    for s in r['sources']:
        sid=s['ref'].split('/')[-1]
        if sid in raw:
            h=hashlib.sha256(raw[sid].encode()).hexdigest()
            if s.get('sha256',h)!=h or s.get('sha256_registrato',h)!=h: e.append('sha non corrispondente '+s['ref'])
    for c in r['clauses']:
        ks={x['kind'] for x in r['cases'] if c['cid'] in x['covers']}
        if not ({'positive','negative','boundary'}<=ks or ks=={'not_applicable'}): e.append('clausola senza P/N/B: '+c['cid'])
    for x in r['cases']:
        if x.get('osservato_028cee9') is None: e.append('caso senza osservato '+x['cid'])
        if x.get('verdetto_osservato_vs_oracolo') not in ('CONFORME','NON_CONFORME','NON_APPLICABILE'): e.append('caso senza verdetto '+x['cid'])
    if 'drawdown' in r['key']:
        modes={(x['fixtures']['scenario'].get('modo'),x['fixtures']['scenario'].get('nome','')) for x in r['cases'] if 'O1' in x['covers']}
        for need in (('SIM','10.00'),('LIVE','10.00'),('SIM','9.99'),('LIVE','11.00'),('LIVE','50%')):
            if not any(m==need[0] and need[1] in n for m,n in modes): e.append('drawdown: manca scenario '+str(need))
    out[r['difference_id']]=e
json.dump(dict(fonte_riga=row_cid,clausole_fonte=fonte_clausole,esiti=out),open('indep_'+__import__('os').path.basename(sys.argv[1]).split('_')[-1],'w'),indent=1,ensure_ascii=False)
print('clausole di fonte:',fonte_clausole); print(sum(not v for v in out.values()),'/',len(out),'completi'); [print(k,v[:3]) for k,v in out.items() if v]

# t80u (indipendente): regole owner estratte dal corpo grezzo #426/6143637861
_ob=json.load(open('/workspace/pickfair/bf2/close02/owner/6143637861.json'))['body']
_rules={m.group(1):m.group(0) for m in re.finditer(r'\((\d)\) [^\n]+',_ob)}
_map={'max_single_bet_pct':'1','max_total_exposure_pct':'1','max_event_exposure_pct':'1','max_stake_abs':'1','max_recovery_tables':'2','allow_recovery':'2','commission_pct':'3'}
_bad={}
for r in json.load(open(sys.argv[1])):
    k=r['key'].split('.',1)[1]
    if k in _map:
        e=[]; txt=_rules[_map[k]].lstrip('> ').strip()
        if not any(c['text']==txt for c in r['clauses']): e.append('regola owner verbatim assente')
        if 'PROVVISORIO' in json.dumps(r): e.append('provvisorio residuo')
        for kind in ('positive','negative','boundary'):
            if not any(kind==c['kind'] and any(a['fonte']=='#426/6143637861' for a in c['ancora_normativa']) for c in r['cases']): e.append('owner senza '+kind)
        if e: _bad[r['difference_id']]=e
print('t80u owner:',7-len(_bad),'/ 7 completi',_bad)
