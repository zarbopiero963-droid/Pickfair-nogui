"""close02 — 6 P0 CASH resi specifici (fixture dalla scheda #461/6085516609, oracoli da
clausole verbatim; riga 43 della fonte aggiunta come clausole derivate verbatim)."""
import json,copy,re
B='/workspace/pickfair/bf2/'
SRC=open(B+'opt01/sources/6085516609.md').read()
L4={y['difference_id']:y for y in json.load(open(B+'close02/record_specific_L4.json'))}
R43=re.search(r"Dopo richiesta cashout: (.*?)\. Test del percorso",SRC,re.S).group(1)
V={ # clausole verbatim estratte dalla riga 43
 'V1':"fill completo→riconciliare payoff netto",
 'V2':"parziale→contabilizzare solo eseguito e mantenere residuo",
 'V3':"unmatched/ambiguous→non chiudere tavolo né azzerare rischio",
 'V4':"settlement identificato→un solo aggiornamento Roserpina, anche dopo retry/restart",
 'V5':"Il tempo residuo non crea eccezioni ai limiti"}
for t in V.values(): assert t in SRC, t
FX={"posizione":"BACK 10.00@2.00 matched su Under 1.5 HT (mercato 1.230000777, selezione 1222347)","uscita":"LAY eseguibile@7.00 (book fresco, profondità 50.00 €)","hedge":"20/7 ≈ 2.857142 (prima di tick/arrotondamento)","payoff_atteso":"−50/7 ≈ −7.142857 su entrambi gli esiti, prima di tick/arrotondamenti e commissione"}
def frag(t,p,s): return {'text':t,'provenance':p,'source':s}
# per record: [(kind, pre, input, [(cid, testo oracolo)])]
SPEC={
'DIFF-PF-CASH-04':[
 ('positive',"posizione {posizione}; residuo BACK 2.00@2.10 unmatched sullo stesso mercato","richiesta cashout sullo scope del tavolo",[('P1',"la richiesta considera sia l'ordine matched 10.00 sia il residuo unmatched 2.00: il residuo non è trattato come posizione chiusa"),('V3',"il residuo unmatched mantiene il tavolo aperto e il rischio non viene azzerato")]),
 ('negative',"posizione {posizione}; listCurrentOrders non disponibile (errore)","richiesta cashout",[('P1',"senza l'elenco aggiornato degli ordini correnti e dei residui il cashout non viene pubblicato"),('V3',"tavolo non chiuso, rischio non azzerato")]),
 ('boundary',"posizione {posizione}; ordine ambiguo BACK 1.00@2.20 (timeout dopo invio)","richiesta cashout",[('P1',"l'ordine ambiguo è conteggiato tra gli ordini correnti dello scope"),('V3',"ambiguous: tavolo non chiuso né rischio azzerato")])],
'DIFF-PF-CASH-19':[
 ('positive',"posizione {posizione}; hedge LAY {hedge}@7.00 interamente abbinato; current orders vuoti, cleared con entrambi i bet","riconciliazione dopo cashout",[('P1',"la chiusura è dichiarata solo se current e cleared concordano sui due bet"),('V1',"fill completo: riconciliato il payoff netto {payoff_atteso}")]),
 ('negative',"hedge risultante matched nei cleared ma ancora presente come EXECUTABLE nei current","riconciliazione",[('P1',"current e cleared discordanti: nessuna chiusura dichiarata, stato da riconciliare"),('V3',"rischio non azzerato finché la discordanza resta")]),
 ('boundary',"cleared orders con moreAvailable=true (pagina incompleta)","riconciliazione",[('P1',"una lettura incompleta di current o cleared non basta a riconciliare"),('V3',"tavolo non chiuso")])],
'DIFF-PF-CASH-21':[
 ('positive',"posizione {posizione}; hedge {hedge}@7.00 abbinato per intero; settlement del mercato","calcolo P/L realizzato",[('P1',"il P/L realizzato è calcolato dai fill effettivi e dal settlement, non dall'importo richiesto"),('V1',"payoff netto riconciliato = {payoff_atteso} corretto per tick e commissione effettivi")]),
 ('negative',"hedge abbinato per 1.50 su {hedge} richiesti","calcolo P/L realizzato",[('P1',"il P/L usa solo 1.50 eseguito"),('V2',"si contabilizza solo l'eseguito e il residuo resta esposto")]),
 ('boundary',"settlement ricevuto due volte (retry) per lo stesso mercato","aggiornamento P/L",[('P1',"il P/L realizzato è registrato una sola volta"),('V4',"un solo aggiornamento Roserpina anche dopo retry")])],
'DIFF-PF-CASH-23':[
 ('positive',"cashout pubblicato, hedge ancora unmatched; il processo riparte","nuovo segnale di ingresso dopo il riavvio",[('P1',"restart e recovery del cashout si completano prima di accettare nuovi ingressi"),('V3',"l'hedge unmatched mantiene il tavolo aperto dopo il riavvio")]),
 ('negative',"cashout con esito ambiguo prima del crash","nuovo ingresso subito dopo il riavvio",[('P1',"nessun nuovo ingresso finché la recovery non ha risolto l'esito ambiguo"),('V3',"rischio non azzerato")]),
 ('boundary',"settlement identificato prima del crash; Roserpina già aggiornata","riavvio",[('P1',"la recovery non ripete l'aggiornamento"),('V4',"un solo aggiornamento Roserpina anche dopo restart")])],
'DIFF-PF-CASH-25':[
 ('positive',"cashout in corso sul tavolo T1 (scope: mercato 1.230000777)","nuovo ingresso sullo stesso mercato arrivato durante il cashout",[('P1',"l'ingresso in volo è bloccato per tutto lo scope del cashout")]),
 ('negative',"cashout in corso sullo scope; ingresso già accodato prima della richiesta","esecuzione dell'ingresso accodato",[('P1',"un ingresso accodato prima del cashout non viene inviato dentro lo scope")]),
 ('boundary',"cashout concluso con hedge parziale (residuo aperto)","nuovo ingresso nello scope",[('P1',"il fencing resta finché lo scope ha residuo aperto"),('V2',"si mantiene il residuo"),('V5',"il tempo residuo non crea eccezioni ai limiti")])],
}
IMPL={ # sonde statiche (riferimenti di codice a 028cee9), NON requisiti
 'DIFF-PF-CASH-04':"sonda statica: core/runtime_controller.py:2609 _route_cashout_signal; :2582 _cashout_chain_wired",
 'DIFF-PF-CASH-19':"sonda statica: core/reconciliation_engine.py:1565 reconcile_batch",
 'DIFF-PF-CASH-21':"sonda statica: nessuna funzione P/L realizzato specifica individuata",
 'DIFF-PF-CASH-23':"sonda statica: core/state_recovery.py:74 _reconcile_startup_ghost_orders",
 'DIFF-PF-CASH-25':"sonda statica: core/reconciliation_engine.py:358-404 fencing token (batch), core/runtime_controller.py:2877 _cashout_gates_still_open"}
OUT=[]
for rid,cases in SPEC.items():
    y=copy.deepcopy(L4[rid]); r=y['R1']
    r['derived_clauses']=[c for c in r['derived_clauses'] if not c['cid'].startswith('V')]+[{'cid':k,'text':t,'origin':'verbatim #461/6085516609 (paragrafo "Dopo richiesta cashout")'} for k,t in V.items()]
    cl={c['cid']:c['text'] for c in r['primary_clauses']+r['derived_clauses']}
    tmpl=y['cases'][0]; new=[]; cmap={}
    for n,(kind,pre,inp,ors) in enumerate(cases,1):
        c=copy.deepcopy(tmpl); c['case_id']=f"{rid[8:]}-R{n}"; c['kind']=kind; c['covers']=sorted({cid for cid,_ in ors})
        c['pre']=pre.format(**FX); c['input']=inp; c['action']='esecuzione cashout su GUI/headless (SIM)'
        c['oracle']=[frag(t.format(**FX),'REQUIREMENT',f"{cid} «{cl[cid]}»") for cid,t in ors]
        c['expected']='; '.join(f"requisito «{cl[cid]}»" for cid in c['covers'])+' — ORACOLO: '+'; '.join(f['text'] for f in c['oracle'])
        c['fixtures']={'amounts':[],'precision':'importi € a 2 decimali; confronto esatto in Decimal','commission':'commissione market-net (PF-CASH-09) fuori da questo caso','calc':'n/a','limit':'n/a','scenario':FX}
        c['norm_vs_impl']={'normativo':f"{','.join(c['covers'])} verbatim {r['source'][:40]}",'impl_attuale':IMPL[rid],'tecnica_delegata':'meccanismo di fencing/recovery: delegato'}
        new.append(c)
        for cid in c['covers']: cmap.setdefault(cid,[]).append(c['case_id'])
    y['cases']=new
    y['clause_map']=[{'cid':cid,'text':cl[cid],'origin':'verbatim','clean':True,'case_ids':ids} for cid,ids in cmap.items()]
    y['record_specific']={'batch':'close02-CASH','priority':'P0','level':'RECORD_SPECIFIC_HAND_BUILT','provenance_rule':'Opzione B + C15','sources':{'R1_source':r['source'],'source_quote_verbatim':str(r.get('source_quote'))+' | '+R43}}
    OUT.append(y)
json.dump(OUT,open(B+'close02/record_specific_CASH.json','w'),ensure_ascii=False,indent=1); print(len(OUT))
