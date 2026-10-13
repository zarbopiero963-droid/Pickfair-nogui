"""close02 — L2: record RSR P0 fasi SAVE/LOAD/VALID/RESTART. Oracoli per chiave,
fixture concrete, comportamento osservato da probe_rsr.py (DB sqlite temporaneo
reale, 028cee9). Fixture = valore di test (default+delta), NON valore owner."""
import json,copy
B='/workspace/pickfair/bf2/'
M={x['id']:x for x in json.load(open(B+'repair01/master1035.json'))}
R=json.load(open(B+'repair01/rebuilt570.json')); P=json.load(open(B+'close02/probe_rsr.json'))
out=[];gaps=[]
for x in R:
    if M[x['difference_id']]['priority']!='P0' or x['hand_built'] or not M[x['difference_id']]['status'].startswith('PREFLIGHT'): continue
    if not x['requirement_id'].startswith('PF-RSR') or x['phase'] not in ('SAVE','LOAD','VALID','RESTART'): continue
    rid=x['control']['id']; o=P[rid]; key=o['key']; attr=o['attr']; y=copy.deepcopy(x)
    if not o.get('in_config'):
        for c in y['cases']:
            c['observed_028cee9']={'note':f'{key}: campo assente da RoserpinaConfig a 028cee9 (BLOCKED, controlli 38-41)'}
            c['expected']+=f" — ORACOLO {key}: non verificabile, campo assente a 028cee9 ⇒ BLOCKED, nessun valore inventato."
        y['record_specific']={'batch':'close02-L2','blocked':'campo assente a 028cee9'}; out.append(y); continue
    fx=o['fixture']; raw=o.get('stored_raw'); d=o['default_class']
    corr={k[8:]:v for k,v in o.items() if k.startswith('corrupt_')}
    silent=[k for k,v in corr.items() if v==d or v in ('nan','inf')]
    neg_ok=[k for k,v in corr.items() if v=='-1.0' or v=='-1']
    rg=[]
    if silent: rg.append(f"LOAD {key}: valori corrotti {silent} ⇒ {[corr[k] for k in silent]} (fallback silenzioso / NaN propagato)")
    if neg_ok: rg.append(f"LOAD {key}: '-1' accettato ⇒ {corr['-1']} (nessuna validazione di dominio al caricamento)")
    for c in y['cases']:
        k=c['kind']; ph=y['phase']; orig=c['input']
        if ph=='SAVE':
            if k=='positive':
                c['input']=f"save_roserpina_config con {attr}={fx!r} (fixture di test, non valore owner)"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: settings['{key}'] == '{raw}' e 'roserpina.config_version' = N+1 scritti nella stessa transazione."
                c['observed_028cee9']={'stored_raw':raw,'config_version':'assente (finding F6, RED test_f6)'}
            elif k=='negative':
                c['input']=f"save_roserpina_config con {attr}={fx!r} ed eccezione iniettata in Database._set_setting dopo la prima scrittura"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: rollback; settings['{key}'] e config_version invariati rispetto a N."
                c['observed_028cee9']={'note':'NON ESEGUITO in L2 (iniezione al commit da implementare)'}
            else:
                c['input']=f"secondo save_roserpina_config con {attr}={fx!r} identico al committato"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: settings['{key}'] == '{raw}' invariato; regola versione su salvataggio identico: TECNICA da fissare (proposta F6 incrementa sempre)."
                c['observed_028cee9']={'stored_raw':raw}
        elif ph=='LOAD':
            if k=='positive':
                c['input']=f"DB con settings['{key}']='{raw}' ⇒ load_roserpina_config()"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: {attr} == {fx!r} (stesso tipo del default {d})."
                c['observed_028cee9']={'load':o['load_same']}
            elif k=='negative':
                c['input']=f"settings['{key}'] ∈ {{'abc','nan','inf','-1',''}} ⇒ load_roserpina_config()"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: valore corrotto segnalato come CORROTTO (errore/flag), mai sostituito in silenzio dal default {d} né propagato come NaN/inf."
                c['observed_028cee9']=corr
            else:
                c['input']=f"DB nuovo senza '{key}' ⇒ load_roserpina_config()"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: stato ASSENTE distinguibile dal valore {d}; nessun valore fantasma."
                c['observed_028cee9']={'load':d,'note':'il loader restituisce il default senza marcare l\'assenza'}
        elif ph=='VALID':
            if k=='positive':
                c['input']=f"{attr}={fx!r} (fixture finita nel tipo del campo) — caso: "+orig
                c['expected']+=f" — ORACOLO: accettato e persistito come '{raw}'."
                c['observed_028cee9']={'stored_raw':raw}
            elif k=='negative':
                c['input']=f"{attr} ∈ {{True, float('nan'), float('inf'), 'abc'}}"+" — caso: "+orig
                c['expected']+=f" — ORACOLO: rifiuto PRIMA del salvataggio; settings['{key}'] resta '{raw}'."
                c['observed_028cee9']={'loader_su_valori_corrotti':corr,'note':'validazione al salvataggio in SettingsService non presente; GUI float() accetta NaN/Inf (mini_gui.py:1989-2006)'}
            else:
                c['expected']+=f" — ORACOLO: bordo del dominio di {key} = da Q1/Q2 (close02/q1q2q8t4.json); se PENDING_OWNER ⇒ nessun bordo inventato."
        elif ph=='RESTART':
            c['expected']+=f" — ORACOLO config: dopo nuovo processo (nuovo Database sullo stesso file) {attr} == {fx!r}."
            c['observed_028cee9']=dict(c.get('observed_028cee9') or {},config_restart=o['load_restart'],protezioni='non sondate in L2')
    for c in y['cases']:
        pass
    y['record_specific']={'batch':'close02-L2','key':key,'fixture':fx,'gap_candidates':rg}
    gaps+=[(y['difference_id'],g) for g in rg]; out.append(y)
json.dump(out,open(B+'close02/record_specific_L2.json','w'),ensure_ascii=False,indent=1)
json.dump(gaps,open(B+'close02/L2_gap_candidates.json','w'),ensure_ascii=False,indent=1)
import collections; print(len(out),collections.Counter(y['phase'] for y in out),'gap',len(gaps),len({g for _,g in gaps}))
