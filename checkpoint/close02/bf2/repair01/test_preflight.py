import json,copy,re
from semantic_preflight import check, ind_clauses
R=json.load(open("rebuilt17v2.json")); res=[]
def T(name,ok): res.append((name,ok)); 
# 1 originali respinti
orig={r['difference_id']:r for r in json.load(open("../prep02/batch_rsr.json"))['records']}
sel=json.load(open("../pilot/sel.json"))
T("originali 17 tutti FAIL",all(check(orig[k]) for k in sel))
T("ricostruiti 17 tutti PASS",all(not check(r) for r in R))
# mutazioni: ciascuna deve far fallire OGNI record a cui è applicabile
def mut(name,fn,applies=lambda r:True):
    bad=[]
    for r in R:
        if not applies(r): continue
        m=copy.deepcopy(r); fn(m)
        if not check(m): bad.append(r['difference_id'])
    T(f"mutazione '{name}' → FAIL su tutti i record applicabili"+(f" (sopravvissuti {bad})" if bad else ""),not bad)
ph=lambda r:r['requirement_id'].split('-')[-1]
mut("rimozione caso che copre la prima clausola primaria",lambda m:m.update(cases=[c for c in m['cases'] if 'P1' not in c['covers']]))
mut("rimozione del 50% dei casi",lambda m:m.update(cases=m['cases'][::2]))
mut("rimozione di tutti i casi boundary",lambda m:m.update(cases=[c for c in m['cases'] if c['kind']!='boundary']))
mut("rimozione di tutti i casi positivi",lambda m:m.update(cases=[c for c in m['cases'] if c['kind']!='positive']))
mut("rimozione del mapping clausola→casi",lambda m:m.pop('clause_map'))
mut("mapping svuotato (case_ids vuoti)",lambda m:[e.update(case_ids=[]) for e in m['clause_map']])
mut("mapping ridiretto a casi che non coprono la clausola",lambda m:[e.update(case_ids=[m['cases'][0]['case_id']]) for e in m['clause_map']])
mut("case_id duplicato",lambda m:m['cases'].append(dict(m['cases'][0])))
def drop_one_mapped(m):
    e=max(m['clause_map'],key=lambda e:len(e['case_ids'])); ids=set(e['case_ids'])
    k={c['kind'] for c in m['cases'] if c['case_id'] in ids}
    # rimuove TUTTI i casi di un tipo dalla clausola più coperta: deve mancare quella molteplicità
    kind="boundary" if "boundary" in k else "negative"
    m['cases']=[c for c in m['cases'] if not (c['case_id'] in ids and c['kind']==kind)]
mut("rimozione dei casi di un tipo per una clausola",drop_one_mapped)
mut("rimozione numeri (amounts vuoti)",lambda m:[c['fixtures'].update(amounts=[]) for c in m['cases']],lambda r:ph(r) in {"PARITY","RACE","RESTART","FAILSAFE","AUDIT"})
mut("fonte importo ignota",lambda m:[a.update(source="INVENTATO") for c in m['cases'] for a in c['fixtures'].get('amounts',[])],lambda r:any(c['fixtures'].get('amounts') for c in r['cases']))
mut("limite senza fonte",lambda m:[c['fixtures'].update(limit=dict(source="X",value="9.00",unit="EUR")) for c in m['cases']],lambda r:any(isinstance(c['fixtures'].get('limit'),dict) for c in r['cases']))
mut("precondizioni rimosse (pre vuoto)",lambda m:[c.update(pre="") for c in m['cases']])
mut("stati di rischio rimossi",lambda m:[c.update(pre=re.sub(r"(?i)matched|unmatched|prenotazion\w*|ambiguous|incert\w*|ordin\w*|aperti","",c['pre'])) for c in m['cases']],lambda r:ph(r) in {"RESTART","FAILSAFE","RACE","AUDIT"})
mut("norm_vs_impl rimosso",lambda m:[c.pop('norm_vs_impl') for c in m['cases']])
mut("inserimento dominio 150% non comprovato",lambda m:m['cases'][0].update(expected=m['cases'][0]['expected']+" ; valore 150 rifiutato"))
mut("tolleranza 0,50 € inserita",lambda m:m['cases'][0].update(expected=m['cases'][0]['expected']+" con 0.50 € di tolleranza"))
mut("drawdown in %",lambda m:m['cases'][0].update(expected=m['cases'][0]['expected']+"; drawdown 10% raggiunto"))
mut("rimozione casi negativi",lambda m:m.update(cases=[c for c in m['cases'] if c['kind']!='negative']))
# controllo incrociato indipendente: ogni clausola del generatore è taggata, e il numero di clausole del preflight ≥ quelle del generatore
xr=[]
for r in R:
    gen={c['cid'] for c in r['R1']['primary_clauses']+r['R1']['derived_clauses']}; tagged={t for c in r['cases'] for t in c['covers']}
    xr.append(dict(id=r['difference_id'],gen_clauses=len(gen),tag_coperte=len(gen&tagged),gen_non_taggate=sorted(gen-tagged),preflight_clauses=len(ind_clauses(r['requirement_id']))))
T("controllo incrociato: ogni clausola del generatore taggata da ≥1 caso",all(not x['gen_non_taggate'] for x in xr))
json.dump(dict(results=[dict(test=n,pass_=ok) for n,ok in res],cross_check=xr),open("preflight_tests.json","w"),ensure_ascii=False,indent=1)
for n,ok in res: print("PASS" if ok else "FAIL",n)
# limite noto (informativo, non conteggiato): casi che si limitano a citare la clausola senza contenuto di fase
lim=0
for r in R:
    m=copy.deepcopy(r)
    for c in m['cases']:
        if '-K' in c['case_id']: c['expected']=re.sub(r":.*$","",c['expected'])+" "+" ".join(e['text'] for e in m['clause_map'] if c['case_id'] in e['case_ids'])
    lim+= not check(m)
print(f"INFO limite noto: {lim}/17 record passano anche con casi ancorati ridotti a citazione della clausola (C10 non misura il merito)")
o={r['difference_id']:r for r in json.load(open("../prep02/batch_rsr.json"))['records']+json.load(open("../prep02/batch324.json"))['records']}
nf=sum(bool(check(r)) for r in o.values()); print(("PASS" if nf==len(o) else "FAIL"),f"originali 570 tutti FAIL ({nf}/{len(o)})")

D=[r for r in json.load(open("rebuilt570.json")) if r.get('q5_drawdown')]
def mutD(name,fn):
    s=0
    for r in D:
        m=copy.deepcopy(r); fn(m); s+=bool(check(m))
    print(("PASS" if s==len(D) else "FAIL"),f"Q5 mutazione '{name}' → FAIL {s}/{len(D)}")
print(("PASS" if all(not check(r) for r in D) else "FAIL"),f"Q5 36 record drawdown ricostruiti PASS ({sum(not check(r) for r in D)}/{len(D)})")
mutD("rimozione caso SIM (DD3)",lambda m:m.update(cases=[c for c in m['cases'] if not c['case_id'].endswith('-DD3')]))
mutD("limite drawdown sostituito da % ",lambda m:[c['fixtures'].update(limit=dict(source="OBS_DEFAULT_RSR02",value="18.0",unit="%")) for c in m['cases'] if c['case_id'].split('-')[-1].startswith('DD')])
mutD("rimozione regola owner Q5 dal mapping",lambda m:m.update(clause_map=[e for e in m['clause_map'] if not e['origin'].startswith('owner')]))
mutD("rimozione caso al confine 10.00 (DD6)",lambda m:m.update(cases=[c for c in m['cases'] if not c['case_id'].endswith('-DD6')]))
mutD("caso 10.01 cambiato in 9.99",lambda m:[c.update(expected=c['expected'].replace('10.01','9.99'),pre=c['pre'].replace('89.99','90.01'),fixtures=dict(c['fixtures'],calc=c['fixtures']['calc'].replace('10.01','9.99'))) for c in m['cases']])

mutD("DD6 riportato a PENDING",lambda m:[c.update(expected="drawdown esattamente 10.00 €: operatore al confine PENDING_OWNER") for c in m['cases'] if c['case_id'].endswith('-DD6')])
A=[r for r in json.load(open("rebuilt570.json")) if r['phase']=="APPLY" and not check(r)]
s=0
for r in A:
    m=copy.deepcopy(r)
    for c in m['cases']:
        if c['kind']=='boundary':
            c['expected']=c['expected'].replace("AMMESSO","RIFIUTATO").replace("ammesso","rifiutato").replace("ammessa","rifiutata")
            c['fixtures']=dict(c['fixtures'],calc=str(c['fixtures'].get('calc','')).replace("ammesso","rifiutato"))
    s+=bool(check(m))
print(("PASS" if s==len(A) else "FAIL"),f"Q3 mutazione 'uguaglianza al cap rifiutata' → FAIL {s}/{len(A)}")

# C14 — provenienza oracolo (Opzione B owner): comportamento osservato non e' requisito
B="/workspace/pickfair/bf2/"
RS=json.load(open(B+"close02/record_specific_L1.json"))+json.load(open(B+"close02/record_specific_L2.json"))+json.load(open(B+"close02/record_specific_L3.json"))+json.load(open(B+"close02/record_specific_L4.json"))+json.load(open(B+"close02/record_specific_L5.json"))+json.load(open(B+"close02/record_specific_L6.json"))+json.load(open(B+"close02/record_specific_CASH.json"))
_c14=lambda r:[x for x in check(r) if x.startswith("C14")]
print(("PASS" if all(not _c14(r) for r in RS) else "FAIL"),f"C14 record-specific L1–L5 senza errori C14 ({sum(not _c14(r) for r in RS)}/{len(RS)}); preflight completo PASS {sum(not check(r) for r in RS)}/{len(RS)}")
_RSOK=[r for r in RS if not check(r)]
def mutC(name,fn):
    s=0
    for r in _RSOK:
        m=copy.deepcopy(r); fn(m); s+=bool(check(m))
    print(("PASS" if s==len(_RSOK) else "FAIL"),f"C14 mutazione '{name}' su record PASS → FAIL {s}/{len(_RSOK)}")
def _first(m): return [c for c in m['cases'] if c.get('oracle')][0]
mutC("provenienza OBSERVED_ONLY",lambda m:_first(m)['oracle'][0].update(provenance="OBSERVED_ONLY"))
mutC("oracolo che riporta l'osservato",lambda m:_first(m)['oracle'].append(dict(text="ritorna [] come a 028cee9",provenance="REQUIREMENT",source=_first(m)['oracle'][0]['source'])))
mutC("clausola citata non verbatim",lambda m:_first(m)['oracle'][0].update(source="P1 «testo inventato»"))
mutC("osservato fuori da impl_attuale",lambda m:_first(m).update(observed_028cee9={"x":"[]"}))
mutC("OWNER_DECISION senza regola registrata",lambda m:_first(m)['oracle'].append(dict(text="x",provenance="OWNER_DECISION",source="#426/1")))

# C15 — regole owner monetarie/fixture solo su record con il soggetto (owner 13/10 11:37)
import semantic_preflight as _sp
_ALL=RS
def _subj(r):
    allc=[c['text'] for c in r['R1']['primary_clauses']+r['R1'].get('derived_clauses',[])]
    return (" ".join(allc)+" "+json.dumps(r.get("control") or {},ensure_ascii=False)+" "+str(r.get("requirement_id",""))).lower()
_nocap=[r for r in _ALL if not check(r) and not _sp.CAP_RX.search(_subj(r))]
_nomoney=[r for r in _ALL if not check(r) and not _sp.MONEY_RX.search(_subj(r))]
_cap=[r for r in _ALL if not check(r) and _sp.CAP_RX.search(_subj(r))]
def _mut(pool,fn):
    s=0
    for r in pool:
        m=copy.deepcopy(r); fn(m); s+=bool(check(m))
    return s
def _addQ3(m): _first(m)['oracle'].append(dict(text="esposizione = cap ammessa",provenance="OWNER_DECISION",source="#426/6141450045 Q3"))
def _addfx(m): _first(m).setdefault('fixtures',{}).update(calc="9.00+1.00=10.00")
n=_mut(_nocap,_addQ3); print(("PASS" if _nocap and n==len(_nocap) else "FAIL"),f"C15 mutazione 'Q3 su record senza soggetto cap' → FAIL {n}/{len(_nocap)}")
n=_mut(_nomoney,_addfx); print(("PASS" if n==len(_nomoney) else "FAIL"),f"C15 mutazione 'fixture € su record non monetario' → FAIL {n}/{len(_nomoney)}")
n=_mut(_cap,_addQ3); print(("PASS" if n==0 else "FAIL"),f"C15 controllo: Q3 su record con soggetto cap resta PASS (FAIL {n}/{len(_cap)})")
