"""close02 v3 — ricostruzione L1+L2 con provenienza dell'oracolo (Opzione B owner:
il comportamento attuale NON e' requisito). Oracolo = frammenti con provenienza
REQUIREMENT(cid) / OWNER_DECISION(fonte) / TECNICA_DELEGATA(chiave). Il
comportamento osservato a 028cee9 va SOLO in norm_vs_impl.impl_attuale; le
divergenze diventano finding."""
import json,copy,re,sys
B='/workspace/pickfair/bf2/'
M={x['id']:x for x in json.load(open(B+'repair01/master1035.json'))}
R=json.load(open(B+'repair01/rebuilt570.json'))
PB=json.load(open(B+'close02/probe_all.json')); PR=json.load(open(B+'close02/probe_rsr.json'))
OPMAP={'listEventTypes':'list_event_types','listCompetitions':'list_competitions','listEvents':'list_events','listMarketCatalogue':'list_market_catalogue','listMarketBook':'get_market_book','listCurrentOrders':'get_current_orders','listClearedOrders':'list_cleared_orders','placeOrders':'place_bet','cancelOrders':'cancel_orders','replaceOrders':'replace_orders','getAccountFunds':'get_account_funds'}
def cid_of(x,pat):
    for c in x['R1']['primary_clauses']:
        if re.search(pat,c['text'],re.I): return c['cid'],c['text']
    return None,None
def frag(text,prov,src): return {'text':text,'provenance':prov,'source':src}
def obs_txt(o): return None if not o else (('ritorna '+o['ret']) if 'ret' in o else ('solleva '+o['raise']))
def finish(c,frags,impl,inp=None):
    if inp: c['input']=inp+' — caso: '+c['input']
    c['oracle']=frags
    c['expected']=c['expected']+' — ORACOLO: '+'; '.join(f['text'] for f in frags)
    c['norm_vs_impl']['impl_attuale']=c['norm_vs_impl']['impl_attuale']+' | osservato 028cee9 (sonda, NON requisito): '+json.dumps(impl,ensure_ascii=False)[:600]
P0=lambda x: M[x['difference_id']]['priority']=='P0' and not x['hand_built'] and M[x['difference_id']]['status'].startswith(('PREFLIGHT','RECORD_SPECIFIC'))
VALID={'list_event_types':[{"eventType":{"id":"1","name":"Soccer"},"marketCount":120}],
 'list_competitions':[{"competition":{"id":"81","name":"Serie A"},"marketCount":40}],
 'list_events':[{"event":{"id":"34000001","name":"Inter v Milan"},"marketCount":60}],
 'list_market_catalogue':[{"marketId":"1.230000001","marketName":"Match Odds","runners":[{"selectionId":47999,"runnerName":"Inter"}]}],
 'get_market_book':[{"marketId":"1.230000001","status":"OPEN","runners":[{"selectionId":47999,"status":"ACTIVE"}]}],
 'get_current_orders':{"currentOrders":[{"betId":"3001","marketId":"1.230000001","selectionId":47999,"side":"BACK","priceSize":{"price":2.0,"size":1.0},"status":"EXECUTABLE"}],"moreAvailable":False},
 'list_cleared_orders':{"clearedOrders":[{"betId":"3001","profit":1.0,"betOutcome":"WON"}],"moreAvailable":False},
 'get_account_funds':{"availableToBetBalance":100.0,"exposure":0.0},
 'place_bet':{"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","betId":"3001","sizeMatched":2.0,"averagePriceMatched":2.0}]},
 'cancel_orders':{"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","sizeCancelled":2.0,"instruction":{"betId":"3001"}}]},
 'replace_orders':{"status":"SUCCESS","instructionReports":[{"status":"SUCCESS","placeInstructionReport":{"status":"SUCCESS","betId":"3002"},"cancelInstructionReport":{"status":"SUCCESS"}}]}}
NEGFX={'session':'RuntimeError("SESSION_EXPIRED") da _post_jsonrpc (INVALID_SESSION_INFORMATION/NO_SESSION)','malformed':{"faultcode":"Client","detail":{"APINGException":{"errorCode":"INVALID_SESSION_INFORMATION"}}},'failure_report':{"status":"FAILURE","errorCode":"INSUFFICIENT_FUNDS","instructionReports":[{"status":"FAILURE","errorCode":"INSUFFICIENT_FUNDS"}]},'timeout':'RuntimeError("REQUEST_FAILED: TIMEOUT") dopo invio (single_shot)','empty':[]}
L1=[];L2=[];blocked=[]
for x in R:
    if not P0(x): continue
    sq=x['R1'].get('source_quote') or ''
    if x['requirement_id'].startswith('PF-BF') and re.search(r'\[[\w/]+\.py:\d+\]',sq):
        y=copy.deepcopy(x); op=re.match(r'BF-\d+\s+(\w+)',y['R1']['primary_clauses'][0]['text']).group(1); fn=OPMAP[op]
        mod='betfair_client' if fn in ('place_bet','cancel_orders','replace_orders') else 'betfair_market_api'
        pr=PB.get(f'{mod}.{fn}',{}); sim=PB.get('simulation_broker.'+fn,{})
        ph=y['phase']
        if ph=='AUTH':
            cid,t=cid_of(y,r'^AUTH:')
            F={'positive':[frag(f"{op} con sessione e permessi validi: operazione eseguita",'REQUIREMENT',f'{cid} «{t}»')],
               'negative':[frag(f"{op} con sessione non valida o permesso negato: esito 'errore non autorizzato' riportato come errore, non come successo",'REQUIREMENT',f'{cid} «{t}»')],
               'boundary':[frag(f"sessione che scade durante {op}: esito 'errore non autorizzato'",'REQUIREMENT',f'{cid} «{t}»')]}
        elif ph=='PROOF':
            cid,t=cid_of(y,r'^PROOF:')
            F={'positive':[frag(f"prova positiva di {op}",'REQUIREMENT',f'{cid} «{t}»')],
               'negative':[frag(f"prova negativa e risposta malformed di {op}: esito di errore classificato, mai esito positivo",'REQUIREMENT',f'{cid} «{t}»')],
               'boundary':[frag(f"risposta stale, disconnect e replay di {op}: esito non scambiato per dato fresco",'REQUIREMENT',f'{cid} «{t}»')]}
            if fn in ('place_bet','cancel_orders','replace_orders'):
                c2,t2=cid_of(y,r'ordine reale non eseguito')
                if c2: F['positive'].append(frag('nessun ordine reale eseguito nella prova','REQUIREMENT',f'{c2} «{t2}»'))
        elif ph=='ERROR':
            cid,t=cid_of(y,r'^ERROR:'); c2,t2=cid_of(y,r'retry monetario')
            F={'positive':[frag(f"errore di {op} classificato transiente/permanente/ambiguo",'REQUIREMENT',f'{cid} «{t}»')],
               'negative':[frag(f"rate limit e timeout di {op} classificati, senza retry monetario prima della riconciliazione",'REQUIREMENT',f'{cid} «{t}»; {c2} «{t2}»')],
               'boundary':[frag(f"timeout dopo l'invio di {op}: esito ambiguo; nessun retry monetario prima della riconciliazione",'REQUIREMENT',f'{c2} «{t2}»')]}
        else:
            blocked.append(y['difference_id']); continue
        for c in y['cases']:
            if 'P1' not in c.get('covers',[]): continue
            impl={'reale':{k:obs_txt(v) for k,v in pr.items()},'sim':{k:obs_txt(v) for k,v in sim.items()}}
            INP={'positive':f'chiamata {op} con sessione valida (stub _post_jsonrpc: fixture valida di probe.py)','negative':f'chiamata {op}: RuntimeError INVALID_SESSION_INFORMATION / risposta malformed (stub)','boundary':f'chiamata {op}: insieme vuoto, sessione che scade, timeout post-invio (stub)'}
            c['fixtures']['stub_payload']={'positive':{'valid':VALID[fn]},'negative':{k:NEGFX[k] for k in ('session','malformed','failure_report')},'boundary':{k:NEGFX[k] for k in ('empty','timeout')}}[c['kind']]
            finish(c,F[c['kind']],impl,INP[c['kind']])
        y['record_specific']={'batch':'close02-L1v3','provenance_rule':'Opzione B','sources':{'R1_source':y['R1']['source'],'source_quote_verbatim':y['R1']['source_quote'],'code_refs_618884e':re.findall(r'\[([\w/]+\.py:\d+)\]',sq)}}
        L1.append(y)
    elif x['requirement_id'].startswith('PF-RSR') and x['phase'] in ('SAVE','LOAD','VALID','RESTART'):
        y=copy.deepcopy(x); rid=y['control']['id']; o=PR[rid]; key=o['key']; attr=o['attr']; ph=y['phase']
        c1,t1=y['R1']['primary_clauses'][0]['cid'],y['R1']['primary_clauses'][0]['text']
        c2,t2=(y['R1']['primary_clauses'][1]['cid'],y['R1']['primary_clauses'][1]['text']) if len(y['R1']['primary_clauses'])>1 else (c1,t1)
        if not o.get('in_config'):
            for c in y['cases']:
                c['oracle']=[frag(f"{key}: da verificare quando il campo esiste; nessun valore inventato",'REQUIREMENT',f'{c1} «{t1}»')]
                c['norm_vs_impl']['impl_attuale']+=' | osservato 028cee9: campo assente da RoserpinaConfig'
            y['record_specific']={'batch':'close02-L2v3','blocked':'BLOCKED_FIELD_ABSENT_028cee9'}; L2.append(y); blocked.append(y['difference_id']); continue
        fx=o['fixture']
        corr={k[8:]:v for k,v in o.items() if k.startswith('corrupt_')}
        for c in y['cases']:
            k=c['kind']
            if ph=='SAVE':
                F={'positive':[frag(f"dopo il salvataggio di {attr}={fx!r} (fixture) la chiave {key} contiene il valore e la versione di configurazione è salvata nella stessa transazione",'REQUIREMENT',f'{c1} «{t1}»')],
                   'negative':[frag(f"errore iniettato al commit: né {key} né la versione cambiano (atomicità)",'REQUIREMENT',f'{c1} «{t1}»')],
                   'boundary':[frag(f"salvataggio identico: {key} invariato",'REQUIREMENT',f'{c1} «{t1}»'),frag("regola della versione su salvataggio identico",'TECNICA_DELEGATA','F6 close02: da fissare')]}[k]
            elif ph=='LOAD':
                F={'positive':[frag(f"{key} persistito = {fx!r} ⇒ ricaricato {attr} == {fx!r}",'REQUIREMENT',f'{c1} «{t1}»')],
                   'negative':[frag(f"{key} corrotto ('abc','nan','inf','') riconosciuto come CORROTTO, distinto da assente; nessun fallback silenzioso",'REQUIREMENT',f'{c1} «{t1}»; {c2} «{t2}»')],
                   'boundary':[frag(f"{key} assente su DB nuovo riconosciuto come ASSENTE; nessun valore fantasma",'REQUIREMENT',f'{c1} «{t1}»; {c2} «{t2}»')]}[k]
            elif ph=='VALID':
                F={'positive':[frag(f"valore finito del tipo di {attr} non rifiutato per tipo; l'appartenenza al dominio segue Q1/Q2 (PENDING_OWNER dove non definito)",'REQUIREMENT',f'{c1} «{t1}»')],
                   'negative':[frag(f"{attr} ∈ {{bool, NaN, +Inf, testo}} rifiutato prima di salvataggio/uso; ultimo stato valido invariato",'REQUIREMENT',f'{c1} «{t1}»; {c2} «{t2}»')],
                   'boundary':[frag(f"bordo del dominio di {key}: definito solo da fonte (Q1/Q2); altrimenti PENDING_OWNER",'REQUIREMENT',f'{c1} «{t1}»')]}[k]
            else:
                F=[frag(f"nuovo processo sullo stesso DB: {attr} == {fx!r} conservato",'REQUIREMENT',f'{c1} «{t1}»')]
            impl={'stored_raw':o.get('stored_raw'),'load':o.get('load_same'),'restart':o.get('load_restart'),'corrotti':corr,'default':o.get('default_class')}
            finish(c,F,impl,f"{attr}={fx!r} (fixture di test, non valore owner); DB sqlite temporaneo reale")
        y['record_specific']={'batch':'close02-L2v3','key':key,'fixture':fx,'provenance_rule':'Opzione B'}
        L2.append(y)
json.dump(L1,open(B+'close02/record_specific_L1.json','w'),ensure_ascii=False,indent=1)
json.dump(L2,open(B+'close02/record_specific_L2.json','w'),ensure_ascii=False,indent=1)
print('L1',len(L1),'L2',len(L2),'blocked',len(blocked))
