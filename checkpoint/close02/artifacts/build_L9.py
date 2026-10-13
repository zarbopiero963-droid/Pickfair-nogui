"""L9 RESTART: 38 record RSR-04..41, oracoli da clausola, osservati da probe_restart_v2.json (SIGKILL reale + nuovo processo + stesso DB)."""
import json,hashlib,ast
raw={str(json.loads(l)['id']):json.loads(l)['body'] for l in open('/workspace/pickfair/bf2/s3_461.jsonl')}
sha=lambda c: hashlib.sha256(raw[c].encode()).hexdigest()
L2=json.load(open('record_specific_L2b.json')); L2=L2 if isinstance(L2,list) else L2['records']
L2={r['difference_id']:r for r in L2 if r['difference_id'].endswith('-RESTART')}
import os,sys
PF=os.environ.get('L9_PROBE','probe_restart_v2.json'); OUTF=os.environ.get('L9_OUT','record_specific_L9.json')
P=json.load(open(PF)); probe_sha=hashlib.sha256(open(PF,'rb').read()).hexdigest()
PENDING={'RSR-04','RSR-12','RSR-13','RSR-15','RSR-31'}; DD={'RSR-05','RSR-06','RSR-07','RSR-20','RSR-41'}
OWN=[{'ref':'#506/6141450045','testo':'Lo stop scatta quando la perdita è >= 10,00 €.'},{'ref':'#506/6141136157','testo':'drawdown 10 € assoluti SIM e LIVE; la % non può indebolirlo'}]
def typed(fx,cfg_repr):
    if cfg_repr.startswith('<'): return cfg_repr.split('.')[1].split(':')[0] if '.' in cfg_repr else None
    v=ast.literal_eval(cfg_repr)
    if isinstance(v,bool): return fx=='True'
    return type(v)(float(fx))
def obs_cfg(b): 
    c=b.get('cfg_valore','')
    return c.split('.')[1].split(':')[0] if c.startswith('<Risk') else (None if c=='<assente>' else ast.literal_eval(c))
out=[]
for n in range(4,42):
    rid='RSR-%02d'%n; d='DIFF-PF-%s-RESTART'%rid; base=L2[d]; p=P[rid]; fx=p['fixture']; fx2=p['fixture_non_committata']
    ck,kt,inc,gh=(p[s] for s in ('commit_poi_kill','kill_in_transazione','riavvio_esito_incerto','riavvio_ordine_fantasma'))
    exp=typed(fx,ck['fase_B']['cfg_valore']) if ck['fase_B']['cfg_valore']!='<assente>' else (fx if True else None)
    cm=base['clause_map']; difetto_L2b=''
    if [c['cid'] for c in cm]!=['P1','P2','D1','D2','D3']:
        ref={c['cid']:c for c in L2['DIFF-PF-RSR-04-RESTART']['clause_map']}
        difetto_L2b='clause_map L2b malformata (%s): clausole derivate ricostruite dal testo canonico di RSR-04'%[c['cid'] for c in cm]
        cm=[c for c in cm if c['cid'] in('P1','P2')]+[ref[k] for k in('D1','D2','D3')]
    cl=[dict(cid=c['cid'],text=c['text'],origine=c['origin'],natura='PRIMARIA_VERBATIM' if c['origin'].startswith('verbatim') else 'DERIVATA_NON_VERBATIM') for c in cm]
    if rid in DD: cl.append(dict(cid='O1',text='Stop drawdown quando la perdita è >= 10,00 € (SIM e LIVE); la % non lo indebolisce',natura='OWNER',fonte=OWN))
    C=[]
    def case(cid,kind,covers,scen,pred,expected,observed,ok,nota=''):
        C.append(dict(cid=f'{rid}-RST-{cid}',kind=kind,covers=covers,fixtures=dict(scenario=scen,fixture=fx,fixture_non_committata=fx2,db='file sqlite temporaneo su disco, stesso path per A e B'),
            predicato=pred,atteso=expected,osservato_028cee9=observed,verdetto_osservato_vs_oracolo=('CONFORME' if ok else 'NON_CONFORME'),nota=nota,
            stato_oracolo='DEFINITIVO' if not cid.startswith('O1') else 'OWNER_IN_VIGORE'))
    def killed(x): return x['fase_A']['ucciso_da_SIGKILL'] and x['fase_B'].get('pid') is not None
    B=ck['fase_B']
    inruntime=B['cfg_valore']!='<assente>'
    case('01','positive',['P1','D3'],'commit_poi_kill',{'op':'riavvio_eq','campo':'cfg_valore','tipo':type(exp).__name__},exp,{'valore':obs_cfg(B),'raw_db':B['raw_db'],'tipo':B['cfg_tipo'],'sigkill':killed(ck)},
        killed(ck) and inruntime and obs_cfg(B)==exp,'' if inruntime else 'valore persistito nel DB ma la chiave non esiste in RoserpinaConfig a 028cee9: configurazione non conservata nel runtime')
    case('02','negative',['P1','P2','D3'],'kill_in_transazione',{'op':'non_committato_invisibile','campo':'cfg_valore'},{'uguale':exp,'diverso_da':fx2},{'valore':obs_cfg(kt['fase_B']),'raw_db':kt['fase_B']['raw_db'],'stdout_A':kt['fase_A']['stdout']},
        killed(kt) and kt['fase_B']['raw_db']!=fx2 and (obs_cfg(kt['fase_B'])==exp or not inruntime))
    case('03','boundary',['P1'],'commit_poi_kill',{'op':'protezione_ordine_aperto','campi':['saghe_pendenti','recover_order_pubblicato']},{'saghe_pendenti':1,'recover_order_pubblicato':1},{'saghe_pendenti':B['saghe_pendenti'],'recover_order_pubblicato':B['recover_order_pubblicato']},
        B['saghe_pendenti']==1 and B['recover_order_pubblicato']==1)
    case('04','positive',['P2'],'metodo',{'op':'metodo_db_reale','campi':['sigkill','pid_nuovo','stesso_file']},{'sigkill':True,'pid_nuovo':True},{'returncode_A':ck['fase_A']['returncode'],'pid_B':B['pid']},killed(ck))
    case('05','positive',['D1'],'commit_poi_kill',{'op':'config_prima_di_readiness','campo':'config_runtime_valore'},exp,{'config_runtime':B['config_runtime_valore'],'recovery':B['recovery'].get('reason'),'modo':B['modo_dopo_start']},
        inruntime and B['config_runtime_valore']==ck['fase_B']['cfg_valore'] and B['recovery'].get('ok') is True)
    I=inc['fase_B']
    case('06','negative',['D2','D1'],'riavvio_esito_incerto (fetch ordini exchange fallisce)',{'op':'nessuna_ammissione','campi':['modo_dopo_start','segnale_dopo_start']},{'ammesso':False},
        {'recovery':I['recovery'],'modo':I['modo_dopo_start'],'segnale_arrivato_al_sizing':I['segnale_dopo_start'].get('arrivato_al_sizing')},
        not (I['modo_dopo_start']=='RuntimeMode.ACTIVE' or I['segnale_dopo_start'].get('arrivato_al_sizing')),'CL02-RST-001')
    G=gh['fase_B']
    case('07','boundary',['D2','P1','D1','P2'],'riavvio_ordine_fantasma (ordine attivo sull exchange assente dal DB)',{'op':'fantasma_registrato_o_bloccato'},{'ghost_orders_recovered':'>=1 oppure ammissione bloccata'},
        {'recovery':G['recovery'],'modo':G['modo_dopo_start'],'segnale_arrivato_al_sizing':G['segnale_dopo_start'].get('arrivato_al_sizing')},
        G['recovery'].get('ghost_orders_recovered',0)>=1 or not G['segnale_dopo_start'].get('arrivato_al_sizing'),'CL02-RST-002')
    case('08','positive',['D2'],'commit_poi_kill (recovery completata)',{'op':'ammissione_dopo_recovery_ok'},{'ammesso':True},{'recovery':B['recovery'].get('reason'),'segnale_arrivato_al_sizing':B['segnale_dopo_start'].get('arrivato_al_sizing')},
        B['recovery'].get('ok') is True and bool(B['segnale_dopo_start'].get('arrivato_al_sizing')))
    case('09','boundary',['D3'],'riavvio_esito_incerto',{'op':'riavvio_eq','campo':'cfg_valore'},exp,{'valore':obs_cfg(I)},inruntime and obs_cfg(I)==exp)
    if rid in DD:
        for sfx,nome,kind,loss,blocca in (('989','11,00','negative',11.0,True),('990.01','9,99','positive',9.99,False),('990.00','10,00','boundary',10.0,True)):
            if rid=='RSR-41' and float(fx)<=loss: blocca=True  # max_drawdown_abs committato (più severo di 10 €) è in vigore
            M=p['memoria_drawdown_1000_poi_'+sfx]; MB=M['fase_B']
            case('O1-'+nome.replace(',',''),kind,['O1','P1'],f'memoria_drawdown: A picco 1000 € -> kill -9 -> B saldo {MB["bankroll"]} € (perdita {nome} €)',
                {'op':'dd_blocca_dopo_riavvio' if blocca else 'dd_non_blocca_dopo_riavvio'},{'ammesso':not blocca},
                {'picco_A':M['fase_A']['stdout'],'equity_peak_B':MB['equity_peak'],'segnale_arrivato_al_sizing':MB['segnale_dopo_start'].get('arrivato_al_sizing')},
                (not MB['segnale_dopo_start'].get('arrivato_al_sizing')) if blocca else bool(MB['segnale_dopo_start'].get('arrivato_al_sizing')),
                'CL02-RST-003' if blocca else 'nessuno stop atteso sotto 10,00 €'+(' (RSR-41: soglia committata %s €)'%fx if rid=='RSR-41' else ''))
    rec=dict(difference_id=d,requirement_id=base['requirement_id'],phase='RESTART',key=base['control']['key'],
        sources=[dict(ref='#461/6085510337',ruolo='riga di fase RESTART (P1,P2 verbatim)',sha256=sha('6085510337'),sha256_registrato=sha('6085510337'))]+([dict(ref='#461/'+base['control']['src_comment'],ruolo='sezione di controllo (chiave)',sha256=sha(base['control']['src_comment']),sha256_registrato=base['R1']['sha256'])] if base['control']['src_comment']!='6085510337' else []),
        difetto_sorgente_L2b=difetto_L2b,
        clauses=cl,cases=C,probe=dict(file=PF,sha256=probe_sha,commit=os.environ.get('L9_COMMIT','028cee9')),
        pending_owner=(rid in PENDING),nota_pending=('chiave PENDING_OWNER solo per il caso assente (fase LOAD); gli oracoli RESTART usano il valore committato e non dipendono dal default' if rid in PENDING else ''),
        tracciabilita={c['cid']:[x['cid'] for x in C if c['cid'] in x['covers']] for c in cl})
    out.append(rec)
json.dump(out,open(OUTF,'w'),indent=1,ensure_ascii=False)
from collections import Counter
print(len(out),sum(len(r['cases']) for r in out),Counter(x['verdetto_osservato_vs_oracolo'] for r in out for x in r['cases']))
print(Counter((x['cid'].split('-RST-')[1],x['verdetto_osservato_vs_oracolo']) for r in out for x in r['cases']))
