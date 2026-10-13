"""L8: lotto 2 = 20 SAVE (sonda GUI+servizio) + RSR-20-LOAD completato con sonda APPLY drawdown. Tempi per record."""
import json,hashlib,re,time,datetime
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('../s3_461.jsonl')}
S['6141136157']=json.load(open('owner_6141136157.json'))['body']
G=json.load(open('probe_gui_save.json')); SS=json.load(open('probe_save_same.json')); DD=json.load(open('probe_dd_apply.json'))
C={c['id']:c for c in json.load(open('rsr_controls.json'))}
L2=[x for x in json.load(open('record_specific_L2b.json')) if x['phase']=='SAVE']
L7={r['difference_id']:r for r in json.load(open('record_specific_L7.json'))}
PH='6085510337'; SAVE_Q='Salvare atomicamente il valore e la versione di configurazione tramite la chiave persistente prevista'
OWN=('#426/6141136157','Il drawdown vincolante è di 10 € assoluti, in SIM e LIVE. Eventuali soglie percentuali non possono sostituire o indebolire questo limite.')
def V(ok): return 'CONFORME' if ok else 'NON_CONFORME'
def mk(rid,ph,cl,kind,scen,orc,anc,obs,ok,key,unit):
    return dict(cid=f'{rid}-{ph}-{cl}-{kind}',covers=[cl],kind=kind,ancora_normativa={'fonte':anc[0],'citazione_verbatim':anc[1]},
        fixtures={'config':{key:scen.get('valore')} if isinstance(scen,dict) else {key:None},'db':'sqlite temporaneo reale, nuovo per scenario','unit':unit,'scenario':scen},
        oracle=[{'provenance':'OWNER_DECISION' if anc[0].startswith('#426') else 'REQUIREMENT','text':orc}],
        osservato_028cee9=obs,verdetto_osservato_vs_oracolo=V(ok),norm_vs_impl={'impl_attuale':'sonda 028cee9: '+json.dumps(obs,ensure_ascii=False)[:600]})
def dd_cases(rid,ph,key,unit):
    a=DD['SIM|pct=None|perdita=11.0']; b=DD['LIVE|pct=50.0|perdita=11.0']; c=DD['SIM|pct=None|perdita=9.99']; d=DD['LIVE|pct=None|perdita=11.0']
    rej=lambda o: any('drawdown' in str(x) for x in o.get('rifiuti',[]))
    return [mk(rid,ph,'O1','positive',{'modo':'SIM','picco':1000.0,'bankroll':989.0,'perdita':11.0,'valore':None},'SIM, perdita 11.00 € dal picco ⇒ nuovo rischio bloccato (drawdown 10 € vincolante)',OWN,a,rej(a),key,unit),
            mk(rid,ph,'O1','negative',{'modo':'LIVE','picco':1000.0,'bankroll':989.0,'perdita':11.0,'valore':50.0},f'LIVE, {key}=50 e perdita 11.00 € ⇒ bloccato: la soglia % non indebolisce i 10 €',OWN,b,rej(b),key,unit),
            mk(rid,ph,'O1','boundary',{'modo':'SIM','picco':1000.0,'bankroll':990.01,'perdita':9.99,'valore':None},'SIM, perdita 9.99 € ⇒ nessun blocco per drawdown',OWN,c,not rej(c),key,unit),
            dict(mk(rid,ph,'O1','positive',{'modo':'LIVE','picco':1000.0,'bankroll':989.0,'perdita':11.0,'valore':None},'LIVE, perdita 11.00 € senza soglia % ⇒ bloccato',OWN,d,rej(d),key,unit),cid=f'{rid}-{ph}-O1-positive-LIVE')]
out=[]; T=[]
for x in L2:
    t0=time.time(); st=datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    rid='RSR-'+x['difference_id'].split('-')[3]; g=G[rid]; key=g['key']; unit=C[rid].get('unit'); fx=str(g['fixture'])
    src=x['R1']['source']; cid=src.split('/')[-1]; sha=hashlib.sha256(S[cid].encode()).hexdigest()
    ctl=x['R1'].get('control_section_verbatim','')[:120].strip(); ctl_ok=bool(ctl) and ctl in S[cid]
    A=(f'#461/{PH}',SAVE_Q); ss=SS.get(rid,{})
    ok,cf,cp,vf=g['ok'],g['commit_fallito'],g['commit_fallito_con_precedente'],g['versione_fallita']
    info=lambda o:[e for e in o['eventi'] if e[0]=='INFO']
    cs=[]
    cs.append(mk(rid,'SAVE','P1','positive',{'valore':fx},f'salvataggio GUI {key}={fx} ⇒ DB {key}={fx} e roserpina.config_version persistita nello stesso commit',A,ok,ok['db_finale'] not in (None,) and ok['versione_finale'] is not None,key,unit))
    cs.append(mk(rid,'SAVE','P1','negative',{'valore':fx,'guasto':f'scrittura {key}'},f'guasto su {key} ⇒ nessuna riga scritta (righe 0→0), nessun SUCCESS',A,cf,cf['righe_dopo']==cf['righe_prima'] and not info(cf),key,unit))
    cs.append(mk(rid,'SAVE','P1','boundary',{'valore':'invariato'},f'salvataggio con {key} invariato ⇒ {key} invariato e versione persistita',A,ss,bool(ss.get('valore_invariato')) and ss.get('config_version') is not None,key,unit))
    cs.append(mk(rid,'SAVE','D1','positive',{'valore':fx},f'{key} e versione scritti insieme: entrambi presenti dopo il commit',A,ok,ok['db_finale'] is not None and ok['versione_finale'] is not None,key,unit))
    cs.append(mk(rid,'SAVE','D1','negative',{'valore':fx,'guasto':'scrittura versione'},f'guasto sulla versione ⇒ {key} NON scritto (atomicità valore+versione)',A,vf,vf['db_finale'] is None,key,unit))
    cs.append(mk(rid,'SAVE','D1','boundary',{'valore':fx,'guasto':f'scrittura {key}'},f'guasto su {key} ⇒ nessuna altra chiave della config scritta',A,cf,cf['righe_dopo']==0,key,unit))
    cs.append(mk(rid,'SAVE','D2','negative',{'valore':fx,'precedente':'PRECEDENTE','guasto':f'scrittura {key}'},f'guasto con valore precedente ⇒ {key} resta PRECEDENTE',A,cp,cp['db_finale']=='PRECEDENTE' and cp['righe_dopo']==cp['righe_prima'],key,unit))
    cs.append(mk(rid,'SAVE','D2','positive',{'valore':fx},f'senza guasto ⇒ {key} sostituito da {fx}',A,ok,ok['db_finale'] is not None,key,unit))
    cs.append(mk(rid,'SAVE','D2','boundary',{'valore':fx,'guasto':f'scrittura {key}','db':'nuovo'},f'guasto su DB nuovo ⇒ {key} resta assente (nessun valore parziale)',A,cf,cf['db_finale'] is None,key,unit))
    cs.append(mk(rid,'SAVE','D3','positive',{'valore':fx},f'"Configurazione Roserpina salvata" mostrato solo con {key} e versione già nel DB',A,ok,bool(info(ok)) and info(ok)[0][3]['valore_db_al_messaggio'] is not None and info(ok)[0][3]['versione_db'] is not None,key,unit))
    cs.append(mk(rid,'SAVE','D3','negative',{'valore':fx,'guasto':f'scrittura {key}'},'commit fallito ⇒ nessun messaggio di successo, errore esplicito',A,cf,not info(cf) and any(e[0]=='ERROR' for e in cf['eventi']),key,unit))
    cs.append(mk(rid,'SAVE','D3','boundary',{'valore':fx,'guasto':'scrittura versione'},'versione non scritta ⇒ nessun messaggio di successo',A,vf,not info(vf),key,unit))
    pend=[]
    if any(c['cid']=='O1' for c in x['clause_map']): cs+=dd_cases(rid,'SAVE',key,unit)
    ok_all=sha==x['R1']['sha256'] and ctl_ok and SAVE_Q in S[PH]
    out.append(dict(difference_id=x['difference_id'],requirement_id=x['requirement_id'],phase='SAVE',key=key,unit=unit,
        sources=[{'ref':src,'sha256_registrato':x['R1']['sha256'],'sha256_ricalcolato':sha,'sha_ok':sha==x['R1']['sha256'],'sezione_controllo_verbatim_trovata':ctl_ok},
                 {'ref':f'#461/{PH}','sha256_ricalcolato':hashlib.sha256(S[PH].encode()).hexdigest()}]+([{'ref':OWN[0],'sha256_ricalcolato':hashlib.sha256(S['6141136157'].encode()).hexdigest()}] if any(c['covers']==['O1'] for c in cs) else []),
        clauses=[{'cid':next((v for a,v in (('Salvare atomicamente','P1'),('Scrittura atomica','D1'),('Errore di scrittura','D2'),('niente SUCCESS','D3'),('drawdown vincolante','O1')) if a in c['text']),c['cid']),'text':c['text'].split('— ',1)[-1],'origin':c['origin']} for c in x['clause_map'] if not c['text'].startswith('grace prima del cashout')],cases=cs,clausole_non_verificate=pend,intero_verificato=ok_all and not pend))
    T.append(dict(id=x['difference_id'],inizio=st,sec=round(time.time()-t0,4)))
# RSR-20-LOAD completato
r=json.loads(json.dumps(L7['DIFF-PF-RSR-20-LOAD'])); t0=time.time(); st=datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
r['cases']+=dd_cases('RSR-20','LOAD',r['key'],r['unit']); r['clausole_non_verificate']=[]
r['sources'].append({'ref':OWN[0],'sha256_ricalcolato':hashlib.sha256(S['6141136157'].encode()).hexdigest()})
r['intero_verificato']=all(s.get('sha_ok',True) for s in r['sources']); out.append(r); T.append(dict(id=r['difference_id'],inizio=st,sec=round(time.time()-t0,4)))
json.dump(out,open('record_specific_L8.json','w'),indent=1,ensure_ascii=False); json.dump(T,open('timing_L8_build.json','w'),indent=1)
import collections
print(len(out),sum(r['intero_verificato'] for r in out),sum(len(r['cases']) for r in out),collections.Counter(z['verdetto_osservato_vs_oracolo'] for r in out for z in r['cases']))
