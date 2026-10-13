"""close02 L4 — oracoli con provenienza (Opzione B) per i record rimanenti
(P0 non RSR-L1/L2/L3, poi P1/P2, compresi i figli Q6 dei parent split).
Ogni frammento REQUIREMENT cita verbatim la clausola coperta dal caso; la
formulazione per tipo di caso (positivo/negativo/limite) e' la clausola stessa
o la sua violazione: nessun comportamento osservato. OWNER_DECISION solo dove il
caso tocca Q3/Q4/D1/D2. Livello: oracolo DERIVATO DA CLAUSOLA (non record-specific
con fixture concrete come L1): dichiarato nel campo record_specific.level."""
import json,copy,re,collections,sys
B='/workspace/pickfair/bf2/'
M={x['id']:x for x in json.load(open(B+'repair01/master1035.json'))}
DONE=set(json.load(open(B+'close02/record_specific_status.json')))
R=json.load(open(B+'repair01/rebuilt570.json'))
Q=json.load(open(B+'repair02/packs_q6.json'))
def frag(t,p,s): return {'text':t,'provenance':p,'source':s}
KIND={'positive':"caso conforme: la clausola {cid} è soddisfatta e l'esito è riportato come successo",
      'negative':"violazione della clausola {cid}: rilevata, nessun effetto contrario alla clausola, esito non riportato come successo",
      'boundary':"al limite della clausola {cid}: la clausola vale anche al bordo; dati incompleti o ambigui non producono effetti non autorizzati"}
def build(x,prio,src_kind):
    y=json.loads(json.dumps(x,ensure_ascii=False).replace('limite owner','cap configurato (campagna #351, D2)')); cl={c['cid']:c['text'] for c in y['R1']['primary_clauses']+y['R1'].get('derived_clauses',[])}
    nofr=0
    for c in y['cases']:
        c.pop('observed_028cee9',None)
        fr=[frag(KIND.get(c.get('kind'),KIND['positive']).format(cid=cid),'REQUIREMENT',f'{cid} «{cl[cid]}»') for cid in c.get('covers',[]) if cid in cl]
        if 'O1' in c.get('covers',[]):
            e=re.split(r' — ORACOLO',c['expected'])[0]
            t="regola owner drawdown (D1, che ratifica Q5 per la soglia): soglia effettiva = min(configurata validata, 10.00 €) in SIM e LIVE; stop a perdita ≥ soglia"
            if not re.search(r'\b(ritorna|solleva|osservat)',e.lower()): t+=" — caso: "+e
            fr.append(frag(t,'OWNER_DECISION','#426/6141902809 D1; #426/6141136157 Q5'))
        if not fr: nofr+=1; continue
        txt=" ".join(str(c.get(f,'')) for f in ('input','pre','expected'))
        if re.search(r'\bLAY\b',txt): fr.append(frag("LAY: rischio = liability stake×(quota−1)",'OWNER_DECISION','#426/6141450045 Q4'))
        if '10.00' in txt or '10.01' in txt:
            fr.append(frag("esposizione = cap ammessa, oltre il cap bloccata, nessuna tolleranza, Decimal",'OWNER_DECISION','#426/6141450045 Q3'))
            fr.append(frag("10.00 € è il cap CONFIGURATO della campagna #351, non un default; cap assente/non valido ⇒ blocco nuovo rischio",'OWNER_DECISION','#426/6141902809 D2'))
        if y.get('q5_drawdown'): fr.append(frag("drawdown: soglia effettiva = min(configurata validata, 10.00 €) in SIM e LIVE; stop a perdita ≥ soglia",'OWNER_DECISION','#426/6141902809 D1'))
        c['oracle']=fr; c['expected']=c['expected']+' — ORACOLO: '+'; '.join(f['text'] for f in fr)
    y['record_specific']={'batch':'close02-L4','priority':prio,'origin':src_kind,'provenance_rule':'Opzione B','level':'ORACOLO_DERIVATO_DA_CLAUSOLA (fixture generiche; nessuna sonda)','cases_without_clause':nofr}
    return y
OUT=[]
for x in R:
    m=M[x['difference_id']]
    if x['difference_id'] in DONE or not m['status'].startswith(('PREFLIGHT','ATOMIC')): continue
    OUT.append(build(x,m['priority'],'rebuilt570'))
for q in Q:
    par=M.get(q.get('parent')) or {}
    if not par: continue
    OUT.append(build(q,par['priority'],'packs_q6'))
OUT.sort(key=lambda y:({'P0':0,'P1':1,'P2':2}[y['record_specific']['priority']],y['difference_id']))
json.dump(OUT,open(B+'close02/record_specific_L4.json','w'),ensure_ascii=False,indent=1)
print(len(OUT),collections.Counter((y['record_specific']['priority'],y['record_specific']['origin']) for y in OUT))
