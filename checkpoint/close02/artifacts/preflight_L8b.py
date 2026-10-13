"""Preflight rafforzato per SAVE (regole L7b, S1-S8 + sha/money/cid)."""
import json,re,hashlib
from eval_pred_save import evaluate
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
S['6141136157']=json.load(open('/workspace/pickfair/bf2/close02/owner_6141136157.json'))['body']
for c in ('6141902809','6141450045'): S[c]=json.load(open(f'/workspace/pickfair/bf2/close02/owner/{c}.json'))['body']
CODE_DEF=json.load(open('/workspace/pickfair/bf2/close02/probe_load_v3.json'))
def body(ref): return S[ref.split('/')[-1]]
def check(r,P,DD):
    e=[]; rid='RSR-'+r['difference_id'].split('-')[3]; cl={c['cid'] for c in r['clauses']}
    dc=str((CODE_DEF.get(rid) or {}).get('default_codice'))
    for c in r['cases']:
        pr=c['atteso_predicato']; txt=c['oracle'][0]['text']; owner=any(a['fonte'].startswith('#426/') for a in c['ancora_normativa'])
        if str(pr.get('atteso'))==dc and c['fixtures']['scenario'].get('raw') in (None,'') and not owner: e.append('S1 atteso = default codice '+c['cid'])
        if re.search(r'default di classe|default del codice',txt) and not owner: e.append('S1 default codice nel testo '+c['cid'])
        if re.search(r'\binf\b|\bnan\b|finit',txt+json.dumps(c['fixtures'])) and not any(re.search(r'finit|\binf\b|\bnan\b',a['citazione_verbatim'] or '') for a in c['ancora_normativa']): e.append('S2 inf/nan non ancorato '+c['cid'])
        if 'riavvio' in json.dumps(c['fixtures']['scenario']) and pr.get('op')!='riavvio_eq': e.append('S3 riavvio senza processo nuovo '+c['cid'])
        for k in c['covers']:
            if k not in cl: e.append(f'S6 covers {k} inesistente '+c['cid'])
        try: v,_=evaluate(pr,P,DD)
        except Exception: v=None; e.append('S7 predicato non valutabile '+c['cid'])
        if c.get('verdetto_osservato_vs_oracolo')!=v: e.append('S7 verdetto incoerente '+c['cid'])
        if '<attributo assente' in json.dumps(pr)+txt: e.append('S8 sentinel come atteso '+c['cid'])
        for a in c['ancora_normativa']:
            if not a['citazione_verbatim'] or a['citazione_verbatim'] not in body(a['fonte']): e.append('ancora non verbatim '+c['cid'])
        if r['unit']!='EUR' and not (owner and 'drawdown' in r['key']) and re.search(r'€|\bcap\b',txt): e.append('money su non-EUR '+c['cid'])
        if not r.get('campo_gui_presente') and c['fixtures']['scenario'].get('nome')=='ok' and 'nessun campo GUI' not in txt: e.append('S9 fixture non inseribile non dichiarata '+c['cid'])
    if not any(c['fixtures']['scenario'].get('nome')=='invariato' for c in r['cases']): e.append('S4 manca salvataggio invariato')
    for c in r['clauses']:
        m=re.search(r'verbatim #461/(\d+)',c['origin'])
        if m and c['cid'].startswith('P') and c['text'] not in S[m.group(1)]: e.append(f'S5 origine errata {c["cid"]}')
    for k in cl:
        ks={c['kind'] for c in r['cases'] if k in c['covers']}
        if not {'positive','negative','boundary'}<=ks: e.append(f'S6 copertura {k}:{sorted(ks)}')
    for s in r['sources']:
        h=hashlib.sha256(body(s['ref']).encode()).hexdigest()
        if s.get('sha256',h)!=h or s.get('sha256_registrato',h)!=h: e.append('sha '+s['ref'])
    if len({c['cid'] for c in r['cases']})!=len(r['cases']): e.append('cid duplicati')
    return e
def probes():
    G=json.load(open('probe_gui_save.json')); SS=json.load(open('probe_save_same_v2.json')); DD=json.load(open('probe_dd_apply.json'))
    return (lambda rid:{'gui':G[rid],'same':SS[rid]}),DD
if __name__=='__main__':
    import sys
    R=json.load(open(sys.argv[1])); PP,DD=probes()
    res={r['difference_id']:check(r,PP('RSR-'+r['difference_id'].split('-')[3]),DD) for r in R}
    json.dump(res,open('preflight_L8b.json','w'),indent=1,ensure_ascii=False)
    print(sum(not v for v in res.values()),'/',len(res),'PASS'); [print(k,v[:4]) for k,v in res.items() if v]
