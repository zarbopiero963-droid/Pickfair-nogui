"""close02 L3 — RSR P0 fasi APPLY/RACE/AUDIT/FAILSAFE/PARITY. Oracoli con
provenienza (Opzione B): REQUIREMENT = clausola verbatim del record resa specifica
per la chiave; OWNER_DECISION solo dove il caso tocca la regola (Q3/Q4/D1/D2).
Nessun comportamento osservato usato (nessuna sonda in L3)."""
import json,copy,re
B='/workspace/pickfair/bf2/'
M={x['id']:x for x in json.load(open(B+'repair01/master1035.json'))}
R=json.load(open(B+'repair01/rebuilt570.json'))
def frag(t,p,s): return {'text':t,'provenance':p,'source':s}
SPEC={'APPLY':{'P1':"{key}: valore effettivo letto al punto decisionale e di nuovo subito prima dell'effetto operativo",'P2':"{key}: superamento ⇒ blocco o avviso come da contratto del campo, senza effetto non autorizzato"},
 'RACE':{'P1':"{key}: con modifiche concorrenti o più segnali/tavoli nessun limite, prenotazione o identità di configurazione (versione) va persa"},
 'AUDIT':{'P1':"{key}: audit ricostruisce valore, decisione, origine, versione, correlation_id, esito e recovery, senza segreti"},
 'FAILSAFE':{'P1':"{key}: con dati corrotti, book/provider assente o ordine ambiguo nessun nuovo effetto monetario non autorizzato"},
 'PARITY':{'P1':"{key}: stessa regola per BACK/LAY e GUI/headless/MCP, SIM separato da LIVE",'P2':"{key}: rischio LAY misurato come liability, distinto dallo stake"}}
OUT=[]
for x in R:
    m=M[x['difference_id']]
    if m['priority']!='P0' or x['hand_built'] or not m['status'].startswith(('PREFLIGHT','RECORD_SPECIFIC')): continue
    if not x['requirement_id'].startswith('PF-RSR') or x['phase'] not in SPEC: continue
    y=copy.deepcopy(x); key=y['control']['key']; cl={c['cid']:c['text'] for c in y['R1']['primary_clauses']}
    for c in y['cases']:
        fr=[]
        for cid in c.get('covers',[]):
            if cid in cl and cid in SPEC[y['phase']]:
                fr.append(frag(SPEC[y['phase']][cid].format(key=key),'REQUIREMENT',f'{cid} «{cl[cid]}»'))
        if not fr: continue
        txt=" ".join(str(c.get(f,'')) for f in ('input','pre','expected'))
        if re.search(r'\bLAY\b',txt): fr.append(frag("LAY: rischio = liability stake×(quota−1)",'OWNER_DECISION','#426/6141450045 Q4'))
        if '10.00' in txt or '10.01' in txt:
            fr.append(frag("esposizione = cap ammessa, oltre il cap bloccata, nessuna tolleranza, Decimal",'OWNER_DECISION','#426/6141450045 Q3'))
            fr.append(frag("10.00 € è il cap di esposizione CONFIGURATO per la campagna #351, non un default; cap assente/non valido ⇒ blocco nuovo rischio",'OWNER_DECISION','#426/6141902809 D2'))
        if y.get('q5_drawdown'): fr.append(frag("drawdown: soglia effettiva = min(configurata validata, 10.00 €) in SIM e LIVE; stop a perdita ≥ soglia; distinta dal cap di esposizione",'OWNER_DECISION','#426/6141902809 D1'))
        c['oracle']=fr; c['expected']+=' — ORACOLO: '+'; '.join(f['text'] for f in fr)
    y['record_specific']={'batch':'close02-L3','key':key,'provenance_rule':'Opzione B','observed':'nessuna sonda (impl_attuale invariato)'}
    OUT.append(y)
json.dump(OUT,open(B+'close02/record_specific_L3.json','w'),ensure_ascii=False,indent=1)
import collections; print(len(OUT),collections.Counter(y['phase'] for y in OUT))
