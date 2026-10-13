"""L8b: 20 record SAVE secondo le regole L7b (fonti verbatim/sha, predicati dichiarativi, nessun default dal codice)."""
import json,hashlib,re,time,datetime
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('../s3_461.jsonl')}
S['6141136157']=json.load(open('owner_6141136157.json'))['body']
for c in ('6141902809','6141450045'): S[c]=json.load(open(f'owner/{c}.json'))['body']
G=json.load(open('probe_gui_save.json')); C={c['id']:c for c in json.load(open('rsr_controls.json'))}
L2={x['difference_id']:x for x in json.load(open('record_specific_L2b.json')) if x['phase']=='SAVE'}
PH='6085510337'; Q='Salvare atomicamente il valore e la versione di configurazione tramite la chiave persistente prevista'
OWN_D1=('6141136157','Il drawdown vincolante è di 10 € assoluti, in SIM e LIVE. Eventuali soglie percentuali non possono sostituire o indebolire questo limite.')
OWN_Q3=('6141450045','Lo stop scatta quando la perdita è **>= 10,00 €**.')
def find_src(q): return next(c for c,b in S.items() if q in b)
def section(rid):
    for cid,b in S.items():
        m=re.search(r'^#+ '+re.escape(rid)+r' .*?(?=^#+ RSR-|\Z)',b,re.M|re.S)
        if m: return cid,m.group(0)
out=[];T=[]
for did,x in L2.items():
    t0=time.time(); st=datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    rid='RSR-'+did.split('-')[3]; g=G[rid]; key=g['key']; unit=C[rid].get('unit'); fx=str(g['fixture'])
    csrc,sec=section(rid); m=re.search(r"tipo `([^`]+)`, unità `([^`]+)`",sec); tipo=m.group(1) if m else None
    gui=g['ok']['var_gui_presente']
    clauses=[dict(cid='P1',text=Q,origin=f'verbatim #461/{find_src(Q)} (riga di fase SAVE)'),
             dict(cid='D1',text='Scrittura atomica della chiave insieme alla configurazione pertinente',origin=f'derivata, ancorata a P1 verbatim #461/{PH}'),
             dict(cid='D2',text='Errore di scrittura/commit lascia snapshot precedente',origin=f'derivata, ancorata a P1 verbatim #461/{PH} ("atomicamente")'),
             dict(cid='D3',text='niente SUCCESS prima del commit',origin=f'derivata, ancorata a P1 verbatim #461/{PH} ("atomicamente")')]
    DD=rid in ('RSR-20','RSR-41')
    if DD: clauses.append(dict(cid='O1',text=OWN_D1[1],origin=f'owner #426/{OWN_D1[0]} verbatim + Q3 #426/{OWN_Q3[0]}'))
    cases=[]
    def add(cl,kind,scen,orc,anc,pred,prov='REQUIREMENT',tag=''):
        cases.append(dict(cid=f'{rid}-SAVE-{cl}-{kind}{tag}',covers=[cl],kind=kind,
            ancora_normativa=[{'fonte':f'#{"426" if a[0] in ("6141136157","6141902809","6141450045") else "461"}/{a[0]}','citazione_verbatim':a[1]} for a in anc],
            fixtures={'config':{key:scen.get('raw')},'db':'sqlite temporaneo reale su file, nuovo per scenario','unit':unit,'tipo':tipo,'scenario':scen,'percorso':'MiniPickfairGUI._save_roserpina_settings → SettingsService.save_roserpina_config' if scen.get('nome')!='invariato' else 'SettingsService load→save senza modifiche'},
            oracle=[{'provenance':prov,'text':orc}],atteso_predicato=pred))
    A=[(PH,Q)]; nogui='' if gui else ' (nessun campo GUI per la chiave: il valore non può essere inserito)'
    add('P1','positive',{'nome':'ok','raw':fx},f'salvataggio di {key}="{fx}" ⇒ DB {key}={fx} e roserpina.config_version persistita{nogui}',A,{'op':'db_eq_con_versione','scenario':'ok','atteso':fx})
    add('P1','negative',{'nome':'commit_fallito','raw':fx,'guasto':f'scrittura {key}'},f'guasto alla scrittura di {key} ⇒ nessuna riga scritta e {key} assente',A,{'op':'nessuna_scrittura','scenario':'commit_fallito','atteso_db':None})
    add('P1','boundary',{'nome':'invariato','raw':fx},f'{key}="{fx}" già persistito, salvataggio senza modifiche ⇒ {key} invariato, nessun\'altra chiave cambiata, versione persistita',A,{'op':'same_invariato_con_versione'})
    add('D1','positive',{'nome':'ok','raw':fx},f'{key}="{fx}" e versione presenti insieme dopo il commit{nogui}',A,{'op':'db_eq_con_versione','scenario':'ok','atteso':fx},tag='')
    add('D1','negative',{'nome':'versione_fallita','raw':fx,'guasto':'scrittura roserpina.config_version'},f'guasto alla scrittura della versione ⇒ {key} NON scritto (valore e versione insieme o niente)',A,{'op':'nessuna_scrittura','scenario':'versione_fallita','atteso_db':None})
    add('D1','boundary',{'nome':'commit_fallito','raw':fx,'guasto':f'scrittura {key}'},f'guasto alla scrittura di {key} ⇒ nessun\'altra chiave della configurazione scritta (righe invariate)',A,{'op':'nessuna_scrittura','scenario':'commit_fallito','atteso_db':None})
    add('D2','negative',{'nome':'commit_fallito_con_precedente','raw':fx,'precedente':'PRECEDENTE','guasto':f'scrittura {key}'},f'{key}="PRECEDENTE" già nel DB, guasto alla scrittura ⇒ {key} resta "PRECEDENTE"',A,{'op':'nessuna_scrittura','scenario':'commit_fallito_con_precedente','atteso_db':'PRECEDENTE'})
    add('D2','positive',{'nome':'ok','raw':fx},f'senza guasto ⇒ {key} contiene {fx}{nogui}',A,{'op':'db_eq','scenario':'ok','atteso':fx})
    add('D2','boundary',{'nome':'commit_fallito','raw':fx,'db':'nuovo','guasto':f'scrittura {key}'},f'DB nuovo, guasto alla scrittura ⇒ {key} resta assente (nessun valore parziale)',A,{'op':'nessuna_scrittura','scenario':'commit_fallito','atteso_db':None})
    add('D3','positive',{'nome':'ok','raw':fx},f'messaggio "Configurazione Roserpina salvata" solo quando il DB contiene già {key}={fx} e la versione{nogui}',A,{'op':'successo_dopo_commit','scenario':'ok','atteso':fx})
    add('D3','negative',{'nome':'commit_fallito','raw':fx,'guasto':f'scrittura {key}'},'commit fallito ⇒ nessun messaggio di successo; errore esplicito',A,{'op':'no_successo','scenario':'commit_fallito'})
    add('D3','boundary',{'nome':'versione_fallita','raw':fx,'guasto':'scrittura roserpina.config_version'},'versione non scritta ⇒ nessun messaggio di successo; errore esplicito',A,{'op':'no_successo','scenario':'versione_fallita'})
    if DD:
        AO=[OWN_D1,OWN_Q3]; rj=lambda k:{'op':'dd_blocca','scenario':k}; nr=lambda k:{'op':'dd_non_blocca','scenario':k}
        add('O1','positive',{'nome':'SIM perdita 11.00','modo':'SIM','raw':None},'SIM, perdita 11.00 € ⇒ stop nuovo rischio (qualunque soglia % salvata)',AO,rj('SIM|pct=None|perdita=11.0'),'OWNER_DECISION')
        add('O1','negative',{'nome':'LIVE soglia salvata 50%','modo':'LIVE','raw':'50.0'},'soglia % salvata 50 e perdita 11.00 € in LIVE ⇒ stop: la % salvata non indebolisce i 10 €',AO,rj('LIVE|pct=50.0|perdita=11.0'),'OWNER_DECISION')
        add('O1','boundary',{'nome':'SIM perdita 10.00','modo':'SIM','raw':None},'SIM, perdita esattamente 10.00 € ⇒ stop (>= 10,00 €)',AO,rj('SIM|pct=None|perdita=10.0'),'OWNER_DECISION')
        add('O1','boundary',{'nome':'LIVE perdita 10.00','modo':'LIVE','raw':None},'LIVE, perdita esattamente 10.00 € ⇒ stop',AO,rj('LIVE|pct=None|perdita=10.0'),'OWNER_DECISION','-LIVE')
        add('O1','boundary',{'nome':'SIM perdita 9.99','modo':'SIM','raw':None},'SIM, perdita 9.99 € ⇒ nessuno stop per drawdown',AO,nr('SIM|pct=None|perdita=9.99'),'OWNER_DECISION','-999')
        add('O1','positive',{'nome':'LIVE perdita 11.00','modo':'LIVE','raw':None},'LIVE, perdita 11.00 € ⇒ stop',AO,rj('LIVE|pct=None|perdita=11.0'),'OWNER_DECISION','-LIVE')
    srcs=sorted({a['fonte'] for c in cases for a in c['ancora_normativa']}|{f'#461/{csrc}'})
    out.append(dict(difference_id=did,requirement_id=x['requirement_id'],phase='SAVE',key=key,unit=unit,tipo_fonte=tipo,campo_gui_presente=gui,
        sources=[{'ref':s,'sha256':hashlib.sha256(S[s.split('/')[-1]].encode()).hexdigest()} for s in srcs]+[{'ref':x['R1']['source'],'sha256_registrato':x['R1']['sha256']}],
        clauses=clauses,cases=cases))
    T.append(dict(id=did,inizio=st,sec=round(time.time()-t0,4)))
json.dump(out,open('record_specific_L8b.json','w'),indent=1,ensure_ascii=False); json.dump(T,open('timing_L8b_build.json','w'),indent=1)
print(len(out),sum(len(r['cases']) for r in out))
