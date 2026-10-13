"""Applica la ratifica owner t80u (#426/6143637861) ai 7 record L7b: niente più PROVVISORIO_PENDING_OWNER; provenienza OWNER_DECISION; clausola O2 con casi APPLY dinamici."""
import json,hashlib
B=json.load(open('owner/6143637861.json'))['body']; SHA=hashlib.sha256(B.encode()).hexdigest(); REF='#426/6143637861'
R1='(1) max_single_bet_pct, max_total_exposure_pct, max_event_exposure_pct, max_stake_abs absent/invalid/unverifiable => block all new risk; no code default (18/35/18/10000) approved.'
R2='(2) max_recovery_tables, allow_recovery absent/invalid => no new recovery; absence never True, no invented max.'
R3='(3) commission_pct: only verified applicable rate; else explicitly estimated vs verified P/L; no risk-increasing decision on invented/estimated commission; risk-reducing ops only under conservative verifiable conditions.'
NA='Not an approval of 18%,35%,18%,10000€,2,True,4.5%.'
for q in (R1,R2,R3,NA): assert q in B,q
RULE={'RSR-02':R1,'RSR-03':R1,'RSR-04':R1,'RSR-15':R1,'RSR-12':R2,'RSR-31':R2,'RSR-13':R3}
DC={'RSR-02':'18.0','RSR-03':'35.0','RSR-04':'18.0','RSR-15':'10000.0','RSR-12':'2','RSR-31':'True','RSR-13':'4.5'}
R=json.load(open('record_specific_L7b.json'))
for r in R:
    rid='RSR-'+r['difference_id'].split('-')[3]
    if rid not in RULE: continue
    rule=RULE[rid]; anc=[{'fonte':REF,'citazione_verbatim':rule},{'fonte':REF,'citazione_verbatim':NA}]
    for c in r['cases']:
        n=c['fixtures']['scenario'].get('nome')
        if c.get('stato_oracolo')=='PROVVISORIO_PENDING_OWNER' or n in('assente','default iniziale','corrotto_abc') or 'corrotto_abc' in json.dumps(c['atteso_predicato']):
            c.pop('stato_oracolo',None)
            if n in('assente','default iniziale'):
                c['oracle']=[{'provenance':'OWNER_DECISION','fonte':REF,'text':f"{r['key']} assente ⇒ nessun default normativo (owner {REF}: {rule}); il valore del codice {DC[rid]} NON è approvato; atteso a LOAD: assenza segnalata o None, mai {DC[rid]} in silenzio"}]
                c['stato_oracolo']='DEFINITIVO_OWNER'
            else:
                c['oracle'].append({'provenance':'OWNER_DECISION','fonte':REF,'text':f'valore non valido ⇒ regola owner {REF}: {rule}'})
            c['ancora_normativa']=[a for a in c['ancora_normativa'] if a['fonte']!=REF]+anc
    r['pending_owner']=None
    r['owner_decision']={'ref':REF,'sha256':SHA,'data':'2026-10-13 12:33 Europe/Rome (t80u)','regola':rule,'non_approvati':NA,'default_codice_non_ratificato':DC[rid]}
    r['clauses']=[c for c in r['clauses'] if c['cid']!='O2']+[dict(cid='O2',text=rule,origin=f'owner {REF} verbatim (ratifica t80u)')]
    r['cases']=[c for c in r['cases'] if 'O2' not in c['covers']]
    def add(kind,sonda,op,txt,**kw):
        c=dict(cid=f'{rid}-LOAD-O2-{kind}-{sonda.split("|")[1]}',covers=['O2'],kind=kind,ancora_normativa=anc,
               fixtures={'config':{r['key']:None if 'assente' in sonda else ('abc' if 'non_valida' in sonda else 'valido')},'db':'sqlite temporaneo reale su file, processo nuovo','scenario':{'nome':sonda.split('|')[1],'raw':None},'altre_chiavi':'valori validi espliciti (probe_owner_t80u.py)'},
               oracle=[{'provenance':'OWNER_DECISION','fonte':REF,'text':txt}],atteso_predicato=dict(op=op,sonda=sonda,**kw),stato_oracolo='DEFINITIVO_OWNER')
        r['cases'].append(c)
    if rule is R1:
        add('positive',f'{rid}|controllo_tutte_valide','owner_controllo_ammesso','tutte le chiavi valide ⇒ il nuovo rischio può arrivare al sizing (controllo discriminante)')
        add('negative',f'{rid}|assente','owner_blocca_nuovo_rischio','chiave assente ⇒ block all new risk')
        add('boundary',f'{rid}|non_valida','owner_blocca_nuovo_rischio','chiave non valida ("abc") ⇒ block all new risk')
    elif rule is R2:
        v=DC[rid] if rid=='RSR-12' else 'True'
        add('positive',f'{rid}|controllo_tutte_valide','owner_no_recovery','valore valido esplicito non abilitante ⇒ conservato, nessuna recovery implicita',vietato=v)
        add('negative',f'{rid}|assente','owner_no_recovery',f'assente ⇒ no new recovery; absence never True, no invented max (vietato {v})',vietato=v)
        add('boundary',f'{rid}|non_valida','owner_no_recovery',f'non valido ⇒ no new recovery (vietato {v})',vietato=v)
    else:
        add('positive',f'{rid}|controllo_tutte_valide','owner_controllo_ammesso','commissione esplicita valida ⇒ ammissione consentita (controllo discriminante)')
        add('negative',f'{rid}|assente','owner_commissione_non_inventata','assente ⇒ niente 4.5 inventato e nessuna decisione che aumenta il rischio',vietato='4.5')
        add('boundary',f'{rid}|non_valida','owner_commissione_non_inventata','non valida ⇒ niente 4.5 inventato e nessuna decisione che aumenta il rischio',vietato='4.5')
    srcs={s['ref'] for s in r['sources']}
    if REF not in srcs: r['sources'].append({'ref':REF,'sha256':SHA})
json.dump(R,open('record_specific_L7b.json','w'),indent=1,ensure_ascii=False)
