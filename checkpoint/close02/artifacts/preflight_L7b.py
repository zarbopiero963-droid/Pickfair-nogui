"""Preflight rafforzato (t78u): un controllo per ogni rilievo Sol su R-L7-LOAD20."""
import json,re,hashlib
from eval_pred import evaluate
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
S['6141136157']=json.load(open('/workspace/pickfair/bf2/close02/owner_6141136157.json'))['body']
for c in ('6141902809','6141450045','6143637861'): S[c]=json.load(open(f'/workspace/pickfair/bf2/close02/owner/{c}.json'))['body']
def body(ref): return S[ref.split('/')[-1]]
def check(r,P,DD):
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]; p=P[rid]; cl={c['cid'] for c in r['clauses']}
    dc=str(p.get('default_codice'))
    for c in r['cases']:
        pr=c['atteso_predicato']; txt=c['oracle'][0]['text']; owner=any(a['fonte'].startswith('#426/') for a in c['ancora_normativa'])
        # S1 default dal codice come atteso (Sol #1)
        if c['fixtures']['scenario'].get('nome') in ('assente','default iniziale') and not owner:
            if pr.get('op')=='valore_eq' or re.search(r'default di classe|default\s+%s'%re.escape(dc),txt): e.append('S1 default dal codice come atteso '+c['cid'])
        if pr.get('op')=='valore_eq' and str(pr.get('atteso'))==dc and c['fixtures']['scenario'].get('raw') in (None,'') and not any('vuoto=non impostato'==a['citazione_verbatim'] for a in c['ancora_normativa']): e.append('S1 atteso = default codice '+c['cid'])
        # S2 inf/nan/finitezza senza fonte (Sol #2)
        if re.search(r'\binf\b|\bnan\b|finit',txt+json.dumps(c['fixtures'])) and not any(re.search(r'finit|\binf\b|\bnan\b',a['citazione_verbatim'] or '') for a in c['ancora_normativa']): e.append('S2 inf/nan non ancorato '+c['cid'])
        # S3 riavvio: processo nuovo, stesso DB, tipo (Sol #3)
        if 'riavvio' in json.dumps(c['fixtures']['scenario']) and not (pr.get('op')=='riavvio_eq' and pr.get('atteso_tipo')): e.append('S3 riavvio senza tipo/processo nuovo '+c['cid'])
        # S6 mapping clausole (Sol #6)
        for k in c['covers']:
            if k not in cl: e.append(f'S6 covers {k} inesistente '+c['cid'])
        # S7/S8 coerenza verdetto-predicato e niente sentinel come atteso
        try: v,_=evaluate(pr,p,DD)
        except Exception as ex: v=None; e.append('S7 predicato non valutabile '+c['cid'])
        if c.get('verdetto_osservato_vs_oracolo')!=v: e.append('S7 verdetto incoerente con il predicato '+c['cid'])
        if '<attributo assente' in json.dumps(pr)+txt: e.append('S8 sentinel come atteso '+c['cid'])
        # ancore verbatim
        for a in c['ancora_normativa']:
            if not a['citazione_verbatim'] or a['citazione_verbatim'] not in body(a['fonte']): e.append('ancora non verbatim '+c['cid'])
        # money su non-EUR
        if r['unit']!='EUR' and 'O1' not in c['covers'] and not (owner and 'drawdown' in r['key']) and re.search(r'€|\bcap\b',txt): e.append('money su non-EUR '+c['cid'])
    # S4 caso override == default codice (Sol #4)
    if p.get('override_uguale_default') and not any(c['fixtures']['scenario'].get('nome')=='override_uguale_default' for c in r['cases']): e.append('S4 manca override==default')
    # S5 origine = commento dove la citazione è verbatim (Sol #5)
    for c in r['clauses']:
        m=re.search(r'verbatim #461/(\d+)',c['origin'])
        if m and c['cid'].startswith('P') and c['text'] not in S[m.group(1)]: e.append(f'S5 origine errata {c["cid"]}')
    # S6 ogni clausola ha positivo/negativo/limite (o NA)
    for k in cl:
        ks={c['kind'] for c in r['cases'] if k in c['covers']}
        if not ({'positive','negative','boundary'}<=ks or ks=={'not_applicable'}): e.append(f'S6 copertura {k}:{sorted(ks)}')
    # fonti sha
    for s in r['sources']:
        if 'sha256' in s and hashlib.sha256(body(s['ref']).encode()).hexdigest()!=s['sha256']: e.append('sha '+s['ref'])
        if 'sha256_registrato' in s and hashlib.sha256(body(s['ref']).encode()).hexdigest()!=s['sha256_registrato']: e.append('sha registrato '+s['ref'])
    if len({c['cid'] for c in r['cases']})!=len(r['cases']): e.append('cid duplicati')
    if rid in ('RSR-02','RSR-03','RSR-04','RSR-12','RSR-13','RSR-15','RSR-31'):
        if 'PROVVISORIO' in json.dumps(r): e.append('T80 stato provvisorio residuo')
        od=r.get('owner_decision') or {}
        if od.get('ref')!='#426/6143637861' or hashlib.sha256(S['6143637861'].encode()).hexdigest()!=od.get('sha256'): e.append('T80 owner_decision mancante/sha')
        for c in r['cases']:
            if c['fixtures']['scenario'].get('nome') in ('assente','default iniziale') and c['oracle'][0].get('provenance')!='OWNER_DECISION': e.append('T80 provenienza non owner '+c['cid'])
        if 'O2' not in cl: e.append('T80 clausola O2 mancante')
    return e
if __name__=='__main__':
    import sys
    R=json.load(open(sys.argv[1])); P=json.load(open('probe_load_v3.json')); DD=json.load(open('probe_dd_apply.json'))
    res={r['difference_id']:check(r,P,DD) for r in R}
    json.dump(res,open('preflight_'+sys.argv[1].split('_')[-1],'w'),indent=1,ensure_ascii=False)
    print(sum(not v for v in res.values()),'/',len(res),'PASS'); [print(k,v[:4]) for k,v in res.items() if v]
