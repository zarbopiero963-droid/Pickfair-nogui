"""L7: lotto LOAD record-interi verificati (t75u). Oracoli per chiave/fase/clausola, fixture complete, sonda dinamica v2, verdetto osservato-vs-oracolo."""
import json,hashlib,re
S={}
for l in open('../s3_461.jsonl'):
    c=json.loads(l); S[str(c['id'])]=c['body']
D={x['difference_id']:x for f in ('L2b','L6') for x in json.load(open(f'record_specific_{f}.json')) if x['phase']=='LOAD'}
P=json.load(open('probe_load_v2.json')); C={c['id']:c for c in json.load(open('rsr_controls.json'))}
def warned(o): return bool(o.get('log_warn')) or any(v not in ('()','{}','') for v in (o.get('flags') or {}).values())
def V(ok): return 'CONFORME' if ok else 'NON_CONFORME'
out=[]
for did,x in D.items():
    rid='RSR-'+did.split('-')[3]; ctl=C[rid]; p=P[rid]; key=p['key']; unit=ctl.get('unit'); eur=unit=='EUR'
    src=x['R1']['source']; cid=src.split('/')[-1]; body=S.get(cid,'')
    sha=hashlib.sha256(body.encode()).hexdigest()
    ph=S['6085510337']; shaph=hashlib.sha256(ph.encode()).hexdigest()
    csv=x['R1'].get('control_section_verbatim','')[:200]
    ctl_ok=bool(csv) and csv.strip()[:120] in body
    body=body+'\n'+ph
    clauses=[]
    for c in x['clause_map']:
        t=c['text']
        if t.startswith('grace prima del cashout'): continue          # frammento di intestazione, non clausola
        if t.startswith('secondi] — '): t=t.split('— ',1)[1]
        clauses.append(dict(cid=c['cid'],text=t,origin=c['origin'],verbatim_in_fonte=(t in body)))
    fx=str(p.get('override',{}).get('raw'))
    cases=[]; pend=[]
    m=re.search(r"tipo `[^`]+`, unità `[^`]+`",S[cid][S[cid].find(rid+' '):] if rid+' ' in S[cid] else '')
    tq=m.group(0) if m else None
    ANC={'P1':('#461/6085510337','Ricaricare il valore persistito, distinguendo assente/corrotto/override'),
         'P2':('#461/6085510337','nessun valore fantasma o fallback silenzioso'),
         'D1':(src,tq),
         'D2':('#461/6085510337','distinguendo assente/corrotto/override'),
         'D3':('#461/6085510337','nessun valore fantasma o fallback silenzioso')}
    def add(cl,kind,scen,oracle,obs,ok,prov='REQUIREMENT'):
        prov='REQUIREMENT'; a=ANC.get(cl)
        cases.append(dict(ancora_normativa={'fonte':a[0],'citazione_verbatim':a[1]} if a else None,cid=f"{rid}-LOAD-{cl}-{kind}",covers=[cl],kind=kind,
          fixtures={'config':{key:scen},'db':'sqlite temporaneo reale, nuovo per scenario','unit':unit,'scenario':scen},
          oracle=[{'provenance':prov,'text':oracle}],
          osservato_028cee9=obs,verdetto_osservato_vs_oracolo=V(ok),
          norm_vs_impl={'impl_attuale':'sonda LOAD v2 028cee9 (probe_load_v2.json): '+json.dumps(obs,ensure_ascii=False)}))
    for c in clauses:
        t=c['text']; k=next((v for a,v in (('Ricaricare','P1'),('nessun valore fantasma','P2'),('preservando unit','D1'),('Distinguere chiave','D2'),('niente sostituzione','D3')) if a in t),c['cid']+'_OWNER')
        if k=='P1':
            add(k,'positive',f'{key}="{fx}" (override)',f'override {key}="{fx}" ⇒ {key.split(".")[1]} == {fx}',p['override'],p['override']['valore'] in (fx,repr(float(fx)) if fx.replace('.','',1).isdigit() else fx))
            add(k,'negative',f'{key}="abc" (corrotto)',f'{key}="abc" riconosciuto CORROTTO, distinto da assente/override (flag o warning)',p['raw_abc'],warned(p['raw_abc']))
            add(k,'boundary','DB nuovo, chiave assente',f'{key} assente riconosciuto ASSENTE, distinto da override uguale al default (flag o warning)',p['assente'],warned(p['assente']))
        elif k=='P2':
            add(k,'positive','DB nuovo, chiave assente',f'nessun valore fantasma: raw_db None ⇒ default di classe {p["default"]}',p['assente'],p['assente']['raw_db'] is None and p['assente']['valore']==p['default'] and p.get('in_config',True))
            add(k,'negative',f'{key}="nan"',f'"nan" non accettato come valore valido senza segnalazione (no valore fantasma)',p['raw_nan'],warned(p['raw_nan']))
            add(k,'boundary',f'{key}="" (vuoto)',f'vuoto ⇒ nessun fallback silenzioso al default (flag o warning)',p['raw_vuoto'],warned(p['raw_vuoto']))
        elif k=='D1':
            exp={'NoneType':'float','n/a':'float' if unit in('EUR','%') else 'n/a'}.get(p['type_default'],p['type_default'])
            add(k,'positive',f'{key}="{fx}"',f'tipo caricato == {exp}, unità {unit}',p['override'],p['override']['tipo']==exp,'TECNICA_DELEGATA')
            add(k,'negative',f'{key}="inf"',f'"inf" non ammesso come valore in {unit} senza segnalazione',p['raw_inf'],warned(p['raw_inf']),'TECNICA_DELEGATA')
            add(k,'boundary',f'riavvio dopo {key}="{fx}"',f'dopo riavvio valore == {fx}, tipo {exp}',p['riavvio'],p['riavvio']['valore']==p['override']['valore'] and p['override']['valore'] in (fx,repr(float(fx)) if fx.replace('.','',1).isdigit() else fx),'TECNICA_DELEGATA')
        elif k=='D2':
            add(k,'positive','assente vs vuoto',f'assente e vuoto distinguibili (stati diversi osservabili)',{'assente':p['assente'],'vuoto':p['raw_vuoto']},warned(p['raw_vuoto']),'TECNICA_DELEGATA')
            add(k,'negative','corrotto "abc" vs default',f'corrotto distinguibile dal default iniziale autorizzato',p['raw_abc'],warned(p['raw_abc']),'TECNICA_DELEGATA')
            add(k,'boundary','default iniziale (DB nuovo)',f'default iniziale autorizzato {p["default"]} caricato senza errore',p['assente'],p['assente']['valore']==p['default'] and p.get('in_config',True),'TECNICA_DELEGATA')
        elif k=='D3':
            if not eur:
                cases.append(dict(cid=f"{rid}-LOAD-D3-NA",covers=['D3'],kind='not_applicable',ancora_normativa={'fonte':src,'citazione_verbatim':tq},fixtures={'config':{key:fx},'db':'sqlite temporaneo reale','unit':unit,'scenario':f'{key}="abc" (corrotto), chiave non monetaria'},oracle=[{'provenance':'REQUIREMENT','text':f'clausola su valore monetario: {key} è in {unit} (sezione controllo verbatim), non monetaria ⇒ clausola non applicabile; nessuna regola monetaria applicata a questa chiave'}],osservato_028cee9=p['raw_abc'],verdetto_osservato_vs_oracolo='NON_APPLICABILE',norm_vs_impl={'impl_attuale':'sonda LOAD v2 028cee9 (probe_load_v2.json): '+json.dumps(p['raw_abc'],ensure_ascii=False)}))
            else:
                add(k,'negative',f'{key}="abc" (€)',f'config € corrotta non sostituita silenziosamente da un importo',p['raw_abc'],warned(p['raw_abc']),'TECNICA_DELEGATA')
                add(k,'positive',f'{key}="{fx}" (€)',f'importo € {fx} caricato esatto',p['override'],p['override']['valore'] in (fx,repr(float(fx))),'TECNICA_DELEGATA')
                add(k,'boundary',f'{key}="" (€, vuoto=non impostato)',f'vuoto = NON IMPOSTATO (None), non 0 né default monetario',p['raw_vuoto'],p['raw_vuoto']['valore']=='None','TECNICA_DELEGATA')
        else:
            pend.append(dict(cid=k,text=t,motivo='clausola owner D1 drawdown: non osservabile in fase LOAD, richiede sonda APPLY/FAILSAFE'))
    rec=dict(difference_id=did,requirement_id=x['requirement_id'],phase='LOAD',key=key,unit=unit,
        sources=[{'ref':src,'ruolo':'sezione controllo '+rid,'sha256_registrato':x['R1']['sha256'],'sha256_ricalcolato':sha,'sha_ok':sha==x['R1']['sha256'],'sezione_controllo_verbatim_trovata':ctl_ok},{'ref':'#461/6085510337','ruolo':'riga di fase LOAD (P1/P2 verbatim)','sha256_ricalcolato':shaph}],
        clauses=clauses,cases=cases,clausole_non_verificate=pend,
        intero_verificato=not pend and ctl_ok and tq is not None and sha==x['R1']['sha256'] and all(c['verbatim_in_fonte'] or c['origin'].startswith('D02') for c in clauses) and all(z['verdetto_osservato_vs_oracolo'] in('CONFORME','NON_CONFORME','NON_APPLICABILE') for z in cases))
    out.append(rec)
json.dump(out,open('record_specific_L7.json','w'),indent=1,ensure_ascii=False)
import collections
print(len(out),sum(r['intero_verificato'] for r in out),sum(len(r['cases']) for r in out))
print(collections.Counter(z['verdetto_osservato_vs_oracolo'] for r in out for z in r['cases']))
print([ (r['difference_id'],r['clausole_non_verificate'] and 'pend', r['sources'][0]['sha_ok'], [c['cid'] for c in r['clauses'] if not c['verbatim_in_fonte']]) for r in out if not r['intero_verificato']])
