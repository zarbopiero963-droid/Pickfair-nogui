"""close02 — L1 v2: expected/oracoli specifici per record, da sonde eseguite
(probe.py/probe2.py, stub di _post_jsonrpc, nessuna rete) su 028cee9."""
import json,re,hashlib
B='/workspace/pickfair/bf2/close02/'
P=json.load(open(B+'probe_all.json')); L=json.load(open(B+'record_specific_L1.json'))
OPMAP={'listEventTypes':'list_event_types','listCompetitions':'list_competitions','listEvents':'list_events','listMarketCatalogue':'list_market_catalogue','listMarketBook':'get_market_book','listCurrentOrders':'get_current_orders','listClearedOrders':'list_cleared_orders','placeOrders':'place_bet','cancelOrders':'cancel_orders','replaceOrders':'replace_orders','getAccountFunds':'get_account_funds'}
def obs(mod,fn,tag):
    o=P.get(f'{mod}.{fn}',{}).get(tag)
    if not o: return None
    return ('ritorna '+o['ret']) if 'ret' in o else ('solleva '+o['raise'])
gaps=[]
for x in L:
    op=re.match(r'BF-\d+\s+(\w+)',x['R1']['primary_clauses'][0]['text']).group(1); fn=OPMAP[op]
    mod='betfair_client' if fn in ('place_bet','cancel_orders','replace_orders') else 'betfair_market_api'
    v,e,ex=obs(mod,fn,'valid'),obs(mod,fn,'error_payload'),obs(mod,fn,'exception')
    em=obs(mod,fn,'empty') or obs(mod,fn,'failure_report')
    simo={k:(('ritorna '+o['ret']) if 'ret' in o else ('solleva '+o['raise'])) for k,o in P.get('simulation_broker.'+fn,{}).items()}
    rec_gaps=[]
    if e and ('ritorna []' in e or 'ritorna None' in e or "'available': 0.0" in e or ("'ok': True" in e)):
        rec_gaps.append(f'{mod}.{fn}: payload di errore ⇒ {e[:90]} (indistinguibile da esito valido/vuoto)')
    if fn=='replace_orders' and e and 'faultcode' in e: rec_gaps.append(f'{mod}.{fn}: payload di errore restituito grezzo, nessun ok/False normalizzato')
    if fn=='place_bet' and 'unknown_market' in simo and 'SUCCESS' in simo['unknown_market']: rec_gaps.append('simulation_broker.place_bet: mercato sconosciuto 1.999 ⇒ SUCCESS')
    for c in x['cases']:
        if 'P1' not in c.get('covers',[]): continue
        base=c['expected']
        if c['kind']=='positive':
            c['fixtures']['stub']={'_post_jsonrpc_returns':'fixture "valid" di probe.py/probe2.py per '+fn}
            c['expected']=base+f" — ORACOLO {fn}: una sola chiamata JSON-RPC {op}; risultato = fixture valida senza perdita di campi (id, marketId/selectionId/betId conservati)."
            c['observed_028cee9']={'real':v,'sim':simo.get('valid') or simo.get('fresh')}
        elif c['kind']=='negative':
            c['fixtures']['stub']={'_post_jsonrpc_returns':'APINGException INVALID_SESSION_INFORMATION (payload) e RuntimeError (eccezione)'}
            c['expected']=base+f" — ORACOLO {fn}: errore osservabile e distinguibile dal successo (eccezione o ok=False con codice); VIETATO valore sostitutivo ([] / None / saldo 0.00 / ok=True) a fronte di errore; zero mutazioni."
            c['observed_028cee9']={'error_payload':e,'exception':ex}
        elif c['kind']=='boundary':
            c['fixtures']['stub']={'_post_jsonrpc_returns':'insieme vuoto / report FAILURE'}
            c['expected']=base+f" — ORACOLO {fn}: risultato vuoto LEGITTIMO ⇒ esito OK con insieme vuoto, distinguibile dall'errore; report FAILURE ⇒ esito non-successo con errorCode."
            c['observed_028cee9']={'empty_or_failure':em,'sim':{k:s for k,s in simo.items() if k!='valid'}}
    x['record_specific']['batch']='close02-L1v2'
    x['record_specific']['expected_upgrade']='oracoli specifici + fixture concrete + comportamento osservato (stub, non HTTP reale)'
    x['record_specific']['gap_candidates']=rec_gaps
    gaps+= [(x['difference_id'],g) for g in rec_gaps]
json.dump(L,open(B+'record_specific_L1.json','w'),ensure_ascii=False,indent=1)
json.dump(gaps,open(B+'L1_gap_candidates.json','w'),ensure_ascii=False,indent=1)
print(len(L),'record; gap candidati',len(gaps),'su', len({g for _,g in gaps}),'distinti')
for g in sorted({g for _,g in gaps}): print(' -',g)
