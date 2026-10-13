"""L7b: ricostruzione dei 20 record LOAD respinti da Sol (t78u). Nessun default dal codice come atteso; predicati dichiarativi; origine = commento dove la citazione è verbatim."""
import json,hashlib,re,time,datetime
S={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('../s3_461.jsonl')}
S['6141136157']=json.load(open('owner_6141136157.json'))['body']
for c in ('6141902809','6141450045','6143637861'): S[c]=json.load(open(f'owner/{c}.json'))['body']
P=json.load(open('probe_load_v3.json')); DD=json.load(open('probe_dd_apply.json'))
C={c['id']:c for c in json.load(open('rsr_controls.json'))}
D={x['difference_id']:x for f in ('L2b','L6') for x in json.load(open(f'record_specific_{f}.json')) if x['phase']=='LOAD'}
IDS=[r['difference_id'] for r in json.load(open('lot_R_L7_LOAD20.json'))]+['DIFF-PF-RSR-20-LOAD']
PH='6085510337'
Q_P1='Ricaricare il valore persistito, distinguendo assente/corrotto/override'; Q_P2='nessun valore fantasma o fallback silenzioso'
OWN_D1=('6141136157','Il drawdown vincolante è di 10 € assoluti, in SIM e LIVE. Eventuali soglie percentuali non possono sostituire o indebolire questo limite.')
OWN_Q3=('6141450045','Lo stop scatta quando la perdita è **>= 10,00 €**.')
OWN_D2=('6141902809','If a mandatory exposure/order cap is absent, invalid, or unverifiable, block all new risk-increasing orders (fail-closed).')
OWN_D2b=('6141902809','No implicit unlimited exposure or default €10 exposure cap.')
CAP_KEYS={'RSR-17','RSR-38','RSR-39','RSR-40'}; DD_KEYS={'RSR-20','RSR-41'}
def find_src(q):
    for cid,b in S.items():
        if q in b: return cid
def locate(rid,cid):
    b=S[cid]; i=b.find('### '+rid+' ');  i=b.find(rid+' —') if i<0 else i
    j=b.find('\n### ',i+5); return b[i:j if j>0 else None]
out=[]; T=[]
for did in IDS:
    t0=time.time(); st=datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
    x=D[did]; rid='RSR-'+did.split('-')[3]; p=P[rid]; key=p['key']; ctl=C[rid]; unit=ctl.get('unit'); name=ctl.get('name','')
    csrc=x['R1']['source'].split('/')[-1]; sec=locate(rid,csrc) or ''
    m=re.search(r"tipo `[^`]+`, unità `[^`]+`",sec); TQ=m.group(0) if m else None
    tipo=re.search(r"tipo `([^`]+)`",TQ).group(1) if TQ else None
    vuoto_none='vuoto=non impostato' in sec or 'vuoto=non impostato' in name
    VQ=('vuoto=non impostato') if vuoto_none else None
    owner=[]
    if rid in CAP_KEYS: owner=[OWN_D2,OWN_D2b]
    if rid in DD_KEYS: owner=[OWN_D1,OWN_Q3]
    clauses=[dict(cid='P1',text=Q_P1,origin=f'verbatim #461/{find_src(Q_P1)} (riga di fase LOAD)'),
             dict(cid='P2',text=Q_P2,origin=f'verbatim #461/{find_src(Q_P2)} (riga di fase LOAD)'),
             dict(cid='D1',text='Rileggere il valore persistito preservando unità e tipo',origin=f'derivata, ancorata a "{TQ}" verbatim #461/{csrc} sezione {rid}'),
             dict(cid='D2',text='Distinguere chiave assente, valore vuoto, corrotto e default iniziale autorizzato',origin=f'derivata, ancorata a P1 verbatim #461/{PH}; default autorizzato solo da regola owner vigente'),
             dict(cid='D3',text='niente sostituzione silenziosa di config corrotta con valore monetario',origin=f'derivata, ancorata a P2 verbatim #461/{PH}')]
    if rid in DD_KEYS: clauses.append(dict(cid='O1',text=OWN_D1[1],origin=f'owner #426/{OWN_D1[0]} verbatim + Q3 #426/{OWN_Q3[0]}'))
    cases=[]
    def add(cl,kind,scen,orc,anc,pred,prov='REQUIREMENT',tag=''):
        cases.append(dict(cid=f'{rid}-LOAD-{cl}-{kind}{tag}',covers=[cl],kind=kind,
            ancora_normativa=[{'fonte':f'#{"426" if a[0] in ("6141136157","6141902809","6141450045") else "461"}/{a[0]}','citazione_verbatim':a[1]} for a in anc],
            fixtures={'config':{key:scen.get('raw')},'db':'sqlite temporaneo reale su file, nuovo per scenario','unit':unit,'tipo':tipo,'scenario':scen},
            oracle=[{'provenance':prov,'text':orc}],atteso_predicato=pred))
    A1=[(PH,Q_P1)]; A2=[(PH,Q_P2)]; AT=[(csrc,TQ)] if TQ else []
    fx=str(P[rid]['override']['raw']); dc=p['default_codice']
    add('P1','positive',{'nome':'override','raw':fx},f'{key}="{fx}" persistito ⇒ ricaricato con valore {fx}',A1,{'op':'valore_eq','scenario':'override','atteso':fx})
    add('P1','negative',{'nome':'corrotto_abc','raw':'abc'},f'{key}="abc" (non interpretabile come {tipo}) ⇒ riconosciuto CORROTTO (segnale o flag)',A1+AT,{'op':'segnalato','scenario':'corrotto_abc'})
    if p.get('override_uguale_default'):
        add('P1','boundary',{'nome':'override_uguale_default','raw':p['override_uguale_default']['raw']},f'{key} persistito con valore uguale al comportamento attuale del codice ⇒ riconosciuto OVERRIDE, distinguibile dall\'assenza (segnali/flag diversi)',A1,{'op':'stati_distinti','a':'assente','b':'override_uguale_default'})
    else:
        add('P1','boundary',{'nome':'assente','raw':None},f'{key} assente (DB nuovo) ⇒ riconosciuto ASSENTE, distinto da override (segnali/flag diversi)',A1,{'op':'stati_distinti','a':'assente','b':'override'})
    own_txt=f' (regola owner: {owner[0][1][:90]}…)' if owner else ' (nessuna regola owner vigente: nessun default normativo; il valore del codice è comportamento attuale non ratificato)'
    add('P2','positive',{'nome':'assente','raw':None},f'{key} assente ⇒ nessun valore fantasma: assenza segnalata o valore None{own_txt}',A2+owner,{'op':'assente_onesto','scenario':'assente'},'OWNER_DECISION' if owner else 'REQUIREMENT')
    add('P2','negative',{'nome':'corrotto_abc','raw':'abc'},f'{key}="abc" ⇒ nessun fallback silenzioso (segnale/flag obbligatorio se si usa un altro valore)',A2,{'op':'non_silenzioso','scenario':'corrotto_abc'})
    if vuoto_none:
        add('P2','boundary',{'nome':'vuoto','raw':''},f'{key}="" ⇒ NON IMPOSTATO (None), come da "vuoto=non impostato" della fonte',A2+[(csrc,'vuoto=non impostato')],{'op':'valore_eq','scenario':'vuoto','atteso':'None'})
    else:
        add('P2','boundary',{'nome':'vuoto','raw':''},f'{key}="" ⇒ nessun fallback silenzioso (segnale/flag se si usa un altro valore)',A2,{'op':'non_silenzioso','scenario':'vuoto'})
    if TQ:
        add('D1','positive',{'nome':'override','raw':fx},f'{key}="{fx}" ⇒ tipo caricato {tipo}',AT,{'op':'tipo_eq','scenario':'override','atteso':tipo})
        alt=p.get('formato_alternativo') or {}
        add('D1','negative',{'nome':'formato_alternativo','raw':alt.get('raw')},f'{key}="{alt.get("raw")}" (stesso valore, altra forma testuale) ⇒ tipo caricato resta {tipo}',AT,{'op':'tipo_eq','scenario':'formato_alternativo','atteso':tipo})
        add('D1','boundary',{'nome':'riavvio','raw':fx},f'scrittura {key}="{fx}" su DB file, poi load in PROCESSO NUOVO sullo stesso file ⇒ valore {fx} e tipo {tipo}',AT,{'op':'riavvio_eq','atteso_valore':fx,'atteso_tipo':tipo})
    add('D2','positive',{'nome':'assente vs vuoto'},f'{key}: assente e vuoto riconosciuti come stati diversi (segnali/flag diversi; un valore diverso NON basta)',A1,{'op':'stati_distinti','a':'assente','b':'vuoto'})
    add('D2','negative',{'nome':'corrotto vs assente'},f'{key}: corrotto e assente riconosciuti come stati diversi (segnali/flag diversi)',A1,{'op':'stati_distinti','a':'assente','b':'corrotto_abc'})
    add('D2','boundary',{'nome':'default iniziale','raw':None},f'{key} assente: "default iniziale autorizzato" solo da regola owner vigente{own_txt}; atteso: assenza segnalata o None, mai il valore del codice presentato come autorizzato',A1+owner,{'op':'assente_onesto','scenario':'assente'},'OWNER_DECISION' if owner else 'REQUIREMENT')
    if unit=='EUR':
        add('D3','negative',{'nome':'corrotto_abc','raw':'abc'},f'{key}="abc" (importo) ⇒ non sostituito in silenzio da un altro importo',A2,{'op':'non_silenzioso','scenario':'corrotto_abc'})
        add('D3','positive',{'nome':'override','raw':fx},f'{key}="{fx}" ⇒ importo {fx} caricato esatto',A2,{'op':'valore_eq','scenario':'override','atteso':fx})
        add('D3','boundary',{'nome':'vuoto','raw':''},f'{key}="" ⇒ nessun importo sostituito in silenzio',A2,{'op':'non_silenzioso','scenario':'vuoto'})
    else:
        add('D3','not_applicable',{'nome':'corrotto_abc','raw':'abc'},f'clausola sul valore monetario: {key} è in {unit} secondo la fonte ⇒ non applicabile',AT or A2,{'op':'non_applicabile','motivo':f'unità {unit}'})
    if rid in DD_KEYS:
        A=[OWN_D1,OWN_Q3]; rj=lambda k: {'op':'dd_blocca','scenario':k}; nr=lambda k: {'op':'dd_non_blocca','scenario':k}
        add('O1','positive',{'nome':'SIM perdita 11.00','modo':'SIM','raw':None},'SIM, perdita 11.00 € dal picco ⇒ stop nuovo rischio',A,rj('SIM|pct=None|perdita=11.0'),'OWNER_DECISION')
        add('O1','positive',{'nome':'LIVE perdita 11.00','modo':'LIVE','raw':None},'LIVE, perdita 11.00 € dal picco ⇒ stop nuovo rischio',A,rj('LIVE|pct=None|perdita=11.0'),'OWNER_DECISION','-LIVE')
        add('O1','negative',{'nome':'LIVE soglia 50%','modo':'LIVE','raw':'50.0'},'LIVE, soglia % 50 e perdita 11.00 € ⇒ stop: la % non indebolisce i 10 €',A,rj('LIVE|pct=50.0|perdita=11.0'),'OWNER_DECISION')
        add('O1','boundary',{'nome':'SIM perdita 10.00','modo':'SIM','raw':None},'SIM, perdita esattamente 10.00 € ⇒ stop (>= 10,00 €)',A,rj('SIM|pct=None|perdita=10.0'),'OWNER_DECISION')
        add('O1','boundary',{'nome':'LIVE perdita 10.00','modo':'LIVE','raw':None},'LIVE, perdita esattamente 10.00 € ⇒ stop (>= 10,00 €)',A,rj('LIVE|pct=None|perdita=10.0'),'OWNER_DECISION','-LIVE')
        add('O1','boundary',{'nome':'SIM perdita 9.99','modo':'SIM','raw':None},'SIM, perdita 9.99 € ⇒ nessuno stop per drawdown',A,nr('SIM|pct=None|perdita=9.99'),'OWNER_DECISION','-999')
    PEND={'RSR-02','RSR-03','RSR-04','RSR-12','RSR-13','RSR-15','RSR-31'}
    pend=None
    if rid in PEND:
        pend={'decisione_richiesta':f'valore normativo (o comportamento) quando {key} è ASSENTE; il codice oggi usa {dc} (non ratificato)',
              'casi_interessati':[c['cid'] for c in cases if c['fixtures']['scenario'].get('nome') in ('assente','default iniziale')],
              'lettura_oracolo_in_attesa':'P2-positive e D2-boundary restano ancorati solo a P1/P2 verbatim: chiave assente ⇒ assenza segnalata (flag/avviso) oppure None; il default del codice NON è atteso. Se l\'owner ratifica un valore, l\'oracolo diventa: assente ⇒ valore owner citato, con segnalazione.',
              'esito_attuale_sotto_oracolo_provvisorio':'NON_CONFORME (il codice applica il default in silenzio)'}
        for c in cases:
            if c['cid'] in pend['casi_interessati']: c['stato_oracolo']='PROVVISORIO_PENDING_OWNER'
    srcs=sorted({a['fonte'] for c in cases for a in c['ancora_normativa']}|{f'#461/{csrc}'})
    out.append(dict(difference_id=did,requirement_id=x['requirement_id'],phase='LOAD',key=key,unit=unit,tipo_fonte=tipo,
        sources=[{'ref':s,'sha256':hashlib.sha256(S[s.split('/')[-1]].encode()).hexdigest()} for s in srcs]+[{'ref':f'#461/{csrc}','sha256_registrato':x['R1']['sha256']}],
        clauses=clauses,cases=cases,pending_owner=pend,impl_attuale={'default_codice_non_ratificato':dc,'sonda':'probe_load_v3.json'}))
    T.append(dict(id=did,inizio=st,sec=round(time.time()-t0,4)))
json.dump(out,open('record_specific_L7b.json','w'),indent=1,ensure_ascii=False); json.dump(T,open('timing_L7b_build.json','w'),indent=1)
print(len(out),sum(len(r['cases']) for r in out))
