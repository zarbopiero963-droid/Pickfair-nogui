"""Preflight semantico PF-GA-REPAIR-01. Estrazione clausole INDIPENDENTE dal generatore (rilegge fonte e D02)."""
import json,re,unicodedata
B="/workspace/pickfair/bf2/"
REG=json.load(open(B+"repair01/values_registry.json"))
SRC=open(B+"opt01/sources/6085510337.md").read()
ROWS={}
for l in SRC.splitlines():
    m=re.match(r"\|\s*([A-Z]+)\s*\|\s*PR\d+\s*\|(.*)\|\s*$",l)
    if m: ROWS[m.group(1)]=m.group(2)
D2={x['requirement_id']:x['behavior'] for x in json.load(open(B+"d03v2/data/ALIGN-D02_ATOMIC_WORK_REGISTER.json"))['records']}
STOP=set("come anche solodella delle dello degli questo quella sono essere prima dopo senza ogni nella nelle tramite valore stato dove dovono devono".split())
norm=lambda s:unicodedata.normalize("NFKD",s.lower()).encode("ascii","ignore").decode()
def ind_clauses(rid,rec=None):
    ph=rid.split("-")[-1]; base=(rec or {}).get("child_verbatim") or D2[rid].split(" — ")[-1]
    txt=re.sub(r"\[[A-Z]+\]","",ROWS.get(ph,""))+" . "+base  # figli Q6: testo verbatim del contratto originale verificato
    txt=re.sub(r"(?i)\[(fonte|source)[^\]]*\]\([^)]*\)","",txt); txt=re.sub(r"\[([^\]]*)\]\([^)]*\)",r"\1",txt)  # citazioni [Fonte ..](url) non sono clausole  # link markdown: conta solo il testo, non l'URL
    out=[]
    for seg in re.split(r"[;.,]",txt):
        w=[x[:5] for x in re.findall(r"[a-z]{4,}",norm(seg).replace("/"," ")) if x not in STOP]+[x for x in re.findall(r"\b(gui|mcp|sim|lay|db)\b",norm(seg).replace("/"," "))]
        if len(w)>=2: out.append((seg.strip(),sorted(set(w))))
    return out
def casetext(c): return norm(" ".join(str(c.get(f,"")) for f in ("pre","initial_state","input","action","event","expected","post","failure")))
PH_KW={"FAILSAFE":[["ambigu"],["eccezion"],["falso successo"],["corrott"],["book"]],"LOAD":[["override"],["assente"],["vuoto"],["corrott"],["default"]],
 "PARITY":[["mcp"],["sim"],["live"],["back"],["lay"],["headless"]],"RACE":[["reload"],["barriera","serializz"],["concorrent"]],"SAVE":[["commit"],["atomic","transazion"]],
 "RESTART":[["riconcili"],["rischio"],["kill"],["readiness","ammission"]],"AUDIT":[["correlation"],["incert","ambigu"],["€"," eur","importo"],["segret"]],"VALID":[["nan"],["inf"],["bool"],["malformat"]]}
MONEY={"PARITY","RACE","RESTART","FAILSAFE","AUDIT","APPLY"}
COMPLEX={"RESTART","FAILSAFE","RACE","AUDIT"}
FORBID=[(r"operatore[^.;]{0,40}pending|cap_operator[^.;]{0,10}pending","operatore al confine marcato PENDING ma Q3 è decisa (#426/6141450045)"),(r"lay_cap_base[^.;]{0,10}pending|base per lay = pending","base LAY marcata PENDING ma Q4 è decisa (liability)"),(r"0[.,]50\s*€?.{0,20}toller","tolleranza 0,50 € non approvata"),(r"\b150\b","dominio 150% non comprovato"),(r"multipl\w* di 50","step stake Italia non approvato"),(r"toller\w*\s*0[.,]01","tolleranza 0,01 implicita"),(r"drawdown[^.]{0,40}%","drawdown in % (owner: 10 € assoluti)")]
MONEY_RX=re.compile(r"(€|\beur\b|stake|puntat|import|esposizion|exposure|liabilit|drawdown|perdit|loss|profit|cap\b|saldo|balance|fund|rischio|risk|quot|odds|prezz|price|\blay\b|\bback\b|scommess|\bbet|ordin|order|commission|bankroll|max_win|vincit|cashout|green|stop|p/l|pnl|settle|fill|cleared|matched|abbinat|liquid|payout|win\b|monet|cash|multiplier|moltiplic|\bbook)")
CAP_RX=re.compile(r"(esposizion|exposure|\bcap\b|max_open|limite|limit|stake|puntat|importo|place|piazz|ordine nuov|nuovo rischio|new risk|max_win|bankroll)")
LAY_RX=re.compile(r"(\blay\b|liabilit|rischio|risk|esposizion|exposure|stake|puntat|_bet|\bbet\b|dutching)")
DD_RX=re.compile(r"(drawdown|perdit|\bloss|stop)")
def check(rec):
    rid=rec['requirement_id']; ph=rid.split("-")[-1]; F=[]
    cases=rec.get('cases') or (rec.get('R4_oracle') or {}).get('cases') or []
    T=[casetext(c) for c in cases]; ALL=" ".join(T)
    # C1 copertura clausole (indipendente dai tag)
    for seg,kw in ind_clauses(rid,rec):
        if not any(sum(k in t for k in kw)>=max(2,int(0.6*len(kw)+0.999)) for t in T): F.append("C1 clausola non coperta: "+seg[:80])
    # C2 numeri monetari
    _subj0=(" ".join(c['text'] for c in (rec.get('R1') or {}).get('primary_clauses',[])+(rec.get('R1') or {}).get('derived_clauses',[]))+" "+json.dumps(rec.get('control') or {},ensure_ascii=False)+" "+str(rec.get('requirement_id',''))).lower()
    if ph in MONEY and MONEY_RX.search(_subj0):  # C15: C2 solo con soggetto monetario
        fx=[c.get('fixtures') for c in cases]
        if not all(isinstance(f,dict) and all(k in f for k in ("amounts","precision","commission","calc","limit")) for f in fx): F.append("C2 fixture monetarie incomplete (importi/precisione/commissione/calcolo/limite)")
        elif not any(f['amounts'] and all(re.fullmatch(r"-?\d+(\.\d+)?",str(a.get('value'))) and a.get('unit') for a in f['amounts']) for f in fx): F.append("C2 nessun importo numerico con unità")
    # C3 fonti dei valori
    for c in cases:
        f=c.get('fixtures') or {}
        for a in f.get('amounts',[]) or []:
            if a.get('source') not in REG: F.append(f"C3 importo senza fonte nel registro: {a}")
        l=f.get('limit')
        if isinstance(l,dict):
            s=REG.get(l.get('source'))
            if not s: F.append("C3 limite senza fonte vigente")
            elif s['kind']=="PENDING_OWNER" and "PENDING" not in str(c.get('expected','')): F.append("C3 limite PENDING_OWNER usato come definito")
            elif str(s.get('value'))!=str(l.get('value')) and s['value'] is not None: F.append("C3 valore limite diverso dal registro")
    # C4 normativo / impl / tecnica delegata
    if not cases or not all(isinstance(c.get('norm_vs_impl'),dict) and all(c['norm_vs_impl'].get(k) for k in ("normativo","impl_attuale","tecnica_delegata")) for c in cases): F.append("C4 distinzione normativo/impl/TECNICA_DELEGATA mancante")
    # C5 stati complessi
    if ph in COMPLEX:
        tok=[["matched"],["unmatched"],["prenotazion"],["ambiguous","incert"],["aperti","ordine"]]
        if not any(sum(any(t in norm(c.get('pre','') or c.get('initial_state','')) for t in g) for g in tok)>=4 for c in cases): F.append("C5 manca stato iniziale con rischio reale (ordini/matched/unmatched/prenotazioni/esiti incerti)")
    # C6 copertura specifica di fase
    for g in PH_KW.get(ph,[]):
        if "€" in g and not MONEY_RX.search(_subj0): continue  # C15: niente € su record non monetario
        if not any(t in ALL for t in g): F.append("C6 copertura di fase mancante: "+"/".join(g))
    if ph=="SAVE" and not any((c.get('norm_vs_impl') or {}).get('tecnica_delegata','—') not in ("","—") for c in cases): F.append("C6 SAVE senza marcatura TECNICA_DELEGATA")
    # C7 regole owner
    has_dd=any(isinstance((c.get('fixtures') or {}).get('limit'),dict) and c['fixtures']['limit'].get('source')=="DRAWDOWN_EUR_LIMIT" and c['fixtures']['limit'].get('value')=="10.00" for c in cases)
    for p,why in FORBID:
        if "drawdown" in p and has_dd: continue  # % citata solo come soglia, con oracle vincolante 10 € assoluti presente (Q5)
        if re.search(p,ALL): F.append("C7 "+why)
    if re.search(r"drawdown",ALL) and re.search(r"drawdown[^.]{0,40}%",ALL) and not has_dd: pass
    if "RSR-03" in rid and ph in MONEY and "10.00" not in ALL: F.append("C7 esposizione senza limite € owner (10.00)")
    # C8 tipi di caso
    kinds={c.get('kind') for c in cases}
    if not {"positive","negative"}<=kinds and not ({"negative","boundary"}<=kinds): F.append("C8 mancano casi positivi/negativi")
    if ph=="PARITY" and "boundary" not in kinds: F.append("C8 PARITY senza caso al confine")
    for c in cases:
        for f in ("pre","input","action","expected","post"):
            if not c.get(f): F.append(f"C9 caso {c.get('case_id','?')} senza campo {f}"); break
    # C10 mapping clausola->casi verificato sull'estrazione indipendente + molteplicità pos/neg/boundary per clausola
    byid={}
    for c in cases:
        if c.get('case_id') in byid: F.append("C10 case_id duplicato "+str(c.get('case_id')))
        byid[c.get('case_id')]=c
    cm=rec.get('clause_map')
    if not cm: F.append("C10 mapping clausola→casi assente")
    else:
        for seg,kw in ind_clauses(rid,rec):
            need=max(2,int(0.6*len(kw)+0.999))
            ents=[e for e in cm if sum(k in norm(e.get('text','')).replace("/"," ") for k in kw)>=need]
            if not ents: F.append("C10 clausola indipendente non mappata: "+seg[:70]); continue
            ids={i for e in ents for i in e.get('case_ids',[])}
            miss=sorted(i for i in ids if i not in byid)
            if miss: F.append(f"C10 mapping verso casi inesistenti {miss[:3]}: {seg[:50]}")
            good=[byid[i] for i in ids if i in byid and sum(k in casetext(byid[i]) for k in kw)>=need]
            kinds={c.get('kind') for c in good}
            if not {"positive","negative","boundary"}<=kinds: F.append(f"C10 molteplicità insufficiente {sorted(k for k in kinds if k)} per: {seg[:60]}")
    # C11 regole owner nel mapping (es. Q5): casi esistenti, pos/neg/boundary, e per il drawdown SIM+LIVE, 10.01 e 10.00 con limite DRAWDOWN_EUR_LIMIT
    for e in (cm or []):
        if not str(e.get('origin','')).startswith('owner'): continue
        byid2={c.get('case_id'):c for c in cases}; got=[byid2[i] for i in e.get('case_ids',[]) if i in byid2]
        if len(got)!=len(e.get('case_ids',[])) or not got: F.append("C11 regola owner mappata a casi mancanti: "+e['text'][:50])
        if not {"positive","negative","boundary"}<={c.get('kind') for c in got}: F.append("C11 regola owner senza pos/neg/boundary")
        if "drawdown" in norm(e['text']):
            T2=" ".join(casetext(c) for c in got)
            for tok in ("sim","live","10.01","10.00"):
                if tok not in T2: F.append("C11 drawdown Q5 senza "+tok)
            if not all((c.get('fixtures') or {}).get('limit',{}).get('source')=="DRAWDOWN_EUR_LIMIT" for c in got if isinstance((c.get('fixtures') or {}).get('limit'),dict)) or not any(isinstance((c.get('fixtures') or {}).get('limit'),dict) for c in got): F.append("C11 drawdown Q5 senza limite DRAWDOWN_EUR_LIMIT")
    if any(r in rid for r in ("RSR-05-","RSR-06-","RSR-07-","RSR-20-")) and not any(str(e.get('origin','')).startswith('owner') and 'drawdown' in norm(e['text']) for e in (cm or [])): F.append("C11 record drawdown % senza regola owner Q5")
    if rec.get("child_verbatim"):
        import glob
        allsrc=" ".join(open(f).read() for f in glob.glob(B+"opt01/sources/608*.md")+[B+"repair02/src_6089077286.md"])
        if rec["child_verbatim"] not in allsrc: F.append("C12 testo del figlio non verbatim in nessuna fonte originale")
    for c in cases:
        if str(c.get('case_id','')).endswith('-DD6') and 'stop' not in norm(c.get('expected','')): F.append("C13 DD6: drawdown 10.00 deve fermare (Q3)")
        calc=norm(str((c.get('fixtures') or {}).get('calc','')))
        if c.get('kind')=='boundary' and re.search(r"=\s*10[.,]00\s*=\s*limite",calc) and not ('ammess' in calc and 'ammess' in norm(c.get('expected',''))): F.append("C13 esposizione = cap deve essere ammessa (Q3)")
    # C14 provenienza dell'oracolo (owner Opzione B: il comportamento attuale non e' requisito)
    allc={c['cid']:c['text'] for c in (rec.get('R1') or {}).get('primary_clauses',[])+(rec.get('R1') or {}).get('derived_clauses',[])}
    ownsrc=" ".join(str(v.get('source','')) for v in REG.values() if isinstance(v,dict) and v.get('kind')=='OWNER_RULE')
    for c in cases:
        if 'observed_028cee9' in c: F.append(f"C14 {c.get('case_id')}: comportamento osservato fuori da impl_attuale")
        for fr in c.get('oracle') or []:
            pv=fr.get('provenance'); src=str(fr.get('source',''))
            if pv not in ('REQUIREMENT','OWNER_DECISION','TECNICA_DELEGATA'): F.append(f"C14 {c.get('case_id')}: oracolo con provenienza {pv} (OBSERVED_ONLY/assente vietato)"); continue
            if pv=='REQUIREMENT':
                ok=[m for m in re.findall(r"(\w+) «([^»]*)»",src) if allc.get(m[0])==m[1]]
                if not ok: F.append(f"C14 {c.get('case_id')}: REQUIREMENT senza clausola verbatim del record")
            if pv=='OWNER_DECISION' and not any(t in ownsrc for t in re.findall(r"#426/\d+",src)): F.append(f"C14 {c.get('case_id')}: OWNER_DECISION senza regola owner registrata")
            if pv=='TECNICA_DELEGATA' and not src.strip(): F.append(f"C14 {c.get('case_id')}: TECNICA_DELEGATA senza riferimento")
            if re.search(r"\b(ritorna|solleva|osservat)",norm(fr.get('text',''))): F.append(f"C14 {c.get('case_id')}: oracolo che riporta comportamento osservato")
    # C15 (owner 13/10 11:37): ogni regola owner monetaria solo se le clausole
    # del record (o la chiave di controllo) ne trattano il soggetto; fixture in €
    # solo su record con soggetto monetario/rischio.
    _subj=(" ".join(allc.values())+" "+json.dumps(rec.get("control") or {},ensure_ascii=False)+" "+str(rec.get("requirement_id",""))).lower()
    RULE_RX={'Q3':CAP_RX,'D2':CAP_RX,'Q4':LAY_RX,'D1':DD_RX,'Q5':DD_RX}
    for c in cases:
        for fr in c.get('oracle') or []:
            if fr.get('provenance')!='OWNER_DECISION': continue
            for rule in re.findall(r"\b(Q3|Q4|Q5|D1|D2)\b",str(fr.get('source',''))):
                if not RULE_RX[rule].search(_subj): F.append(f"C15 {c.get('case_id')}: regola owner {rule} su record senza il suo soggetto"); break
        fx=c.get('fixtures') or {}
        if (fx.get('amounts') or str(fx.get('calc','n/a')) not in ('n/a','')) and not MONEY_RX.search(_subj):
            F.append(f"C15 {c.get('case_id')}: fixture monetaria su record senza soggetto monetario/rischio")
    return F
if __name__=="__main__":
    import sys
    recs=json.load(open(sys.argv[1])); recs=recs['records'] if isinstance(recs,dict) else recs
    R={r.get('difference_id'):check(r) for r in recs}
    print(sum(not v for v in R.values()),"PASS /",len(R))
    json.dump(R,open(sys.argv[2],"w"),ensure_ascii=False,indent=1)
