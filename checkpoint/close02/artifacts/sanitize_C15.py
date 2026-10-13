"""close02 — sanitizzazione C15 dei pacchetti L4 derivati da clausola (NON li rende L1):
rimuove regole owner Q3/Q4/Q5/D1/D2 senza soggetto e fixture/calcoli monetari su record
non monetari; il livello resta ORACOLO_DERIVATO_DA_CLAUSOLA (NOT_READY)."""
import json,re,sys,collections
B='/workspace/pickfair/bf2/'; sys.path.insert(0,B+'repair01'); import semantic_preflight as SP
L=json.load(open(B+'close02/record_specific_L4.json')); st=collections.Counter()
RX={'Q3':SP.CAP_RX,'D2':SP.CAP_RX,'Q4':SP.LAY_RX,'D1':SP.DD_RX,'Q5':SP.DD_RX}
for y in L:
    cl=[q['text'] for q in y['R1']['primary_clauses']+y['R1'].get('derived_clauses',[])]
    subj=(" ".join(cl)+" "+json.dumps(y.get('control') or {},ensure_ascii=False)+" "+y['requirement_id']).lower()
    for c in y['cases']:
        keep=[]
        for f in c.get('oracle') or []:
            rules=re.findall(r"\b(Q3|Q4|Q5|D1|D2)\b",f['source']) if f['provenance']=='OWNER_DECISION' else []
            if rules and not all(RX[r].search(subj) for r in rules): st['frammenti_owner_rimossi']+=1; continue
            keep.append(f)
        if keep!=c.get('oracle'):
            c['oracle']=keep; c['expected']=re.split(r' — ORACOLO',c['expected'])[0]+' — ORACOLO: '+'; '.join(f['text'] for f in keep)
        fx=c.get('fixtures') or {}
        if not SP.MONEY_RX.search(subj) and (fx.get('amounts') or str(fx.get('calc','n/a'))!='n/a'):
            fx.update(amounts=[],calc='n/a',limit='n/a'); st['fixture_rimosse']+=1
    y['record_specific']['c15_sanitized']=True
    y['record_specific']['readiness']='NOT_READY: oracolo derivato da clausola, fixture generiche, nessuna sonda'
json.dump(L,open(B+'close02/record_specific_L4.json','w'),ensure_ascii=False,indent=1); print(dict(st))
