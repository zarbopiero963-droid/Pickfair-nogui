"""close02 L6 — oracoli di L5 riscritti come enunciati specifici per chiave e fase.
- Clausola S0 = riga verbatim "Semantica/rischio" della sezione di controllo (#461), aggiunta
  ai derived_clauses solo quando normativa (non osservazioni FAIL/parser/loader).
- Ogni caso: frammento REQUIREMENT sulla clausola coperta, con enunciato specifico
  (chiave, valore di test, fase, tipo di caso) + frammento sulla decisione della chiave
  (REQUIREMENT S0 se normativa, altrimenti TECNICA_DELEGATA sul titolo del controllo).
- 'valore A' nel pre sostituito dal valore di test.
- impl_attuale: sonde LOAD (probe_rsr) e APPLY/RACE (probe_apply_race) a 028cee9."""
import json,re,sys,collections
B='/workspace/pickfair/bf2/'; sys.path.insert(0,B+'close02'); sys.path.insert(0,B+'repair01')
from rsr_decisions import DEC
import semantic_preflight as SP
L=json.load(open(B+'close02/record_specific_L5.json'))
PRB={v['key']:v for v in json.load(open(B+'close02/probe_rsr.json')).values()}
PAR=json.load(open(B+'close02/probe_apply_race.json'))
PRS=json.load(open(B+'close02/probe_restart.json'))
def frag(t,p,s): return {'text':t,'provenance':p,'source':s}
ST={ # (fase,tipo) -> enunciato; {n} nome, {k} chiave, {v1},{v2}, {d1} decisione con v1, {d2} con v2
 ('APPLY','positive'):"{n}={v1}: al punto decisionale e nella rilettura subito prima dell'invio vale {v1}, quindi {d1}",
 ('APPLY','negative'):"{n} cambiato da {v1} a {v2} (versione N+1) tra decisione ed effetto: l'invio applica {v2} riletto ({d2}) oppure non avviene; mai {v1} non più valido",
 ('APPLY','boundary'):"{n} assente o corrotto al momento dell'effetto: nessun ordine nuovo e motivo che nomina {k}",
 ('RACE','positive'):"salvataggi concorrenti di {n} ({v1} e {v2}): entrambe le versioni registrate (N+1, N+2), ogni segnale usa la versione letta alla sua decisione",
 ('RACE','negative'):"salvataggio di {n}={v2} con prenotazione aperta: la prenotazione resta conteggiata e nessun limite o prenotazione va perso",
 ('RACE','boundary'):"due salvataggi di {n} con la stessa versione attesa N: uno solo vince, l'altro è rifiutato come conflitto, nessuna modifica persa in silenzio",
 ('AUDIT','positive'):"l'audit della decisione registra {k}={v1}, versione N, correlation_id c-1, decisione ({d1}) ed esito",
 ('AUDIT','negative'):"l'audit della decisione con {k} non contiene password, app_key né session token",
 ('AUDIT','boundary'):"cambio di {n} da {v1} a {v2} durante la decisione: l'audit riporta entrambe le versioni e la recovery",
 ('FAILSAFE','positive'):"{n}={v1} valido e book disponibile: {d1}",
 ('FAILSAFE','negative'):"{k} corrotto ('abc') oppure book/provider assente: nessun nuovo ordine e motivo esplicito",
 ('FAILSAFE','boundary'):"ordine precedente ambiguo (timeout dopo invio) con {n}={v1}: nessun nuovo ordine sullo stesso mercato finché non è riconciliato",
 ('PARITY','positive'):"con {n}={v1} la stessa regola ({d1}) vale per BACK e LAY e per GUI, headless e MCP",
 ('PARITY','negative'):"con {n}={v1} SIM e LIVE applicano la stessa regola senza condividere stato o ordini",
 ('PARITY','boundary'):"LAY a quota 3.00 con {n}={v1}: la grandezza confrontata è quella indicata dalla regola ({d1}); la liability (stake×2.00) non è confusa con lo stake",
 ('OBSERVABLE','positive'):"dopo il salvataggio di {n}={v1}, GUI e headless mostrano {v1} con versione N",
 ('OBSERVABLE','negative'):"{k} corrotto ('abc'): GUI e headless mostrano un errore di configurazione, non un valore di default",
 ('OBSERVABLE','boundary'):"{k} assente: mostrato come non impostato, distinto da un valore salvato",
 ('S1','positive'):"con {n}={v1}: {d1}",
 ('S1','negative'):"cambiando solo {n} da {v1} a {v2} la decisione cambia ({d2}) senza disattivare gate più restrittivi",
 ('S1','boundary'):"bordo del dominio di {n}: dominio PENDING_OWNER, nessun bordo assunto; il caso resta PENDING",
 ('LOAD','positive'):"{k}={v1} persistito viene ricaricato come {v1}",
 ('LOAD','negative'):"{k}='abc' persistito è segnalato come corrotto, non sostituito in silenzio",
 ('LOAD','boundary'):"{k} assente è distinto da corrotto e da un valore salvato",
 ('RESTART','positive'):"dopo riavvio {k}={v1} vale {v1} con la stessa versione",
 ('RESTART','negative'):"dopo riavvio {k}='abc' è segnalato come corrotto e blocca nuovo rischio",
 ('RESTART','boundary'):"crash durante il salvataggio di {v2}: dopo riavvio vale {v1} oppure {v2} per intero, mai uno stato misto",
}
def impl(i,phase,k):
    p=PRB.get(k) or {}; a=PAR.get(i) or {}
    if phase in ('APPLY','PARITY','S1'): return "sonda statica 028cee9 (NON requisito): "+a.get('apply','n/a')
    if phase=='RESTART' and 'restart_valore' in PRS.get(i,{}): return "sonda 028cee9 (NON requisito): "+json.dumps(PRS[i],ensure_ascii=False)
    if phase=='RACE': return "sonda 028cee9 (NON requisito): "+json.dumps(a.get('race'),ensure_ascii=False)
    if phase=='AUDIT': return "sonda statica 028cee9 (NON requisito): "+a.get('apply','n/a')+"; contenuto audit non sondato"
    return "osservato 028cee9 (sonda, NON requisito): "+json.dumps({x:p.get(x) for x in ('stored_raw','load_same','load_restart','corrupt_abc','corrupt_nan','corrupt_inf','corrupt_empty')},ensure_ascii=False)
st=collections.Counter()
for y in L:
    i=y['control']['id']; k=y['control']['key']; n=y['control']['name']; v1,v2=y['record_specific']['test_values']
    dec,sem_ok=DEC[i]
    m=re.search(r'Semantica/rischio: (.*)',y['R1'].get('control_section_verbatim') or '')
    title=f"### {i} — {n}"
    if sem_ok and m:
        if not any(c['cid']=='S0' for c in y['R1']['derived_clauses']):
            y['R1']['derived_clauses'].append({'cid':'S0','text':m.group(1).strip(),'origin':f"verbatim {y['R1']['source']} sezione {i} (Semantica/rischio)"})
        dsrc=('REQUIREMENT',f"S0 «{m.group(1).strip()}»")
    else:
        dsrc=('TECNICA_DELEGATA',f"titolo del controllo {y['R1']['source']} «{title}»; riga Semantica/rischio non normativa")
    cl={c['cid']:c['text'] for c in y['R1']['primary_clauses']+y['R1']['derived_clauses']}
    F=dict(n=n,k=k,v1=v1,v2=v2,d1=dec.format(v=v1),d2=dec.format(v=v2))
    for c in y['cases']:
        if 'valore A' in c.get('pre',''): c['pre']=c['pre'].replace('valore A',v1); st['valore_A_corretti']+=1
        if 'O1' in c.get('covers',[]):
            c['oracle']=[f for f in c.get('oracle',[]) if 'Q4' not in f['source']]
            c['expected']=re.split(r' — ORACOLO',c['expected'])[0]+' — ORACOLO: '+'; '.join(f['text'] for f in c['oracle'])
            c['norm_vs_impl']['impl_attuale']=impl(i,y['phase'],k); continue
        txt=ST[(y['phase'],c['kind'])].format(**F)
        new=[frag(txt,'REQUIREMENT',f"{cid} «{cl[cid]}»") for cid in c.get('covers',[]) if cid in cl]
        tpl=ST[(y['phase'],c['kind'])]
        if '{d2}' in tpl: new.append(frag(f"decisione governata da {k} con il nuovo valore {v2}: "+F['d2'],dsrc[0],dsrc[1]))
        elif '{d1}' in tpl: new.append(frag(f"decisione governata da {k} con {v1}: "+F['d1'],dsrc[0],dsrc[1]))
        if y['phase']=='S1' and c['kind']=='negative': c['pre']=re.sub(r"=\S+ non coerente con il contesto",f" cambiato da {v1} a {v2} (solo questo campo; gli altri invariati)",c['pre'])
        own=[f for f in c.get('oracle',[]) if f['provenance']=='OWNER_DECISION']
        c['oracle']=new+own
        c['expected']=re.split(r' — ORACOLO',c['expected'])[0]+' — ORACOLO: '+'; '.join(f['text'] for f in c['oracle'])
        c['norm_vs_impl']['impl_attuale']=impl(i,y['phase'],k)
        st['casi_riscritti']+=1
    y['record_specific'].update(batch='close02-L6',level='RECORD_SPECIFIC_PER_CHIAVE_E_FASE',decision_source=dsrc[0])
json.dump(L,open(B+'close02/record_specific_L6.json','w'),ensure_ascii=False,indent=1)
print(len(L),dict(st),collections.Counter(y['record_specific']['decision_source'] for y in L))
