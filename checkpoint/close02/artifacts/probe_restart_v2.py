"""Sonda RESTART v2 a 028cee9 con terminazione REALE (SIGKILL) e nuovo processo sullo stesso DB file.
Fase A (processo figlio): commit config + ordine aperto (saga PENDING) [+ transazione in corso] poi kill -9.
Fase B (processo nuovo): load config + RuntimeController.start() SIM con DB reale; stato di recovery, ammissione, protezioni."""
import sys,json,os,subprocess,tempfile,signal
BASE=os.environ.get('PROBE_ROOT','/workspace/pickfair/bf2/close02/wt_base'); PY=sys.executable
A=r'''
import sys,os,signal,json;sys.path.insert(0,%r)
from database import Database
from services.setting_service import SettingsService
p,key,fx,fx2,mode=sys.argv[1:6]; attr=key.split('.',1)[1]
# fixture completa (owner t80u #426/6143637861): chiavi obbligatorie esplicite e valide, tranne quella sotto prova
OWNER_VALID={'roserpina.max_single_bet_pct':'5.0','roserpina.max_total_exposure_pct':'20.0','roserpina.max_event_exposure_pct':'10.0','roserpina.max_stake_abs':'50.0','roserpina.commission_pct':'5.0','roserpina.max_recovery_tables':'1','roserpina.allow_recovery':'0'}
db0=Database(p); db0.save_settings({k:v for k,v in OWNER_VALID.items() if k!=key})
db=Database(p); s=SettingsService(db); c=s.load_roserpina_config()
if hasattr(c,attr):
    cur=getattr(c,attr); v={'True':True,'False':False}.get(fx,fx)
    import enum
    if isinstance(cur,enum.Enum): v=type(cur)(fx)
    elif not isinstance(v,bool):
        v=type(cur)(float(v)) if isinstance(cur,(int,float)) and not isinstance(cur,bool) else float(v)
    setattr(c,attr,v); s.save_roserpina_config(c)
else:
    db.save_settings({key:fx})
db.create_order_saga(customer_ref='CR-RESTART-1',batch_id='B1',event_key='E1',table_id=1,market_id='1.999',selection_id=7,bet_type='BACK',price=2.0,stake=5.0,payload={'market_id':'1.999','selection_id':7,'stake':5.0,'price':2.0,'side':'BACK'},status='PENDING')
if mode=='dd':
    import io,contextlib,logging;logging.disable(logging.CRITICAL)
    from unittest.mock import MagicMock
    from core.runtime_controller import RuntimeController
    bf=MagicMock(); bf._session_invalid=False; bf.connect.return_value={'ok':True}; bf.get_account_funds.return_value={'available':1000.0}
    with contextlib.redirect_stdout(io.StringIO()):
        rc=RuntimeController(bus=MagicMock(),db=db,settings_service=s,betfair_service=bf,telegram_service=MagicMock())
        rc._catalog_sync=MagicMock(); rc._start_market_data_feed=lambda: None; rc._start_settlement_poller=lambda: None
        rc.start(execution_mode='SIMULATION')
    print('PEAK_A',float(rc.risk_desk.equity_peak),flush=True); os.kill(os.getpid(),signal.SIGKILL)
if mode=='kill_mid_tx':
    with db.transaction():
        db._set_setting(key,fx2)
        print('IN_TX',flush=True); os.kill(os.getpid(),signal.SIGKILL)
print('COMMITTED',flush=True); os.kill(os.getpid(),signal.SIGKILL)
'''%BASE
B=r'''
import sys,json,io,contextlib,logging;sys.path.insert(0,%r)
logging.disable(logging.CRITICAL)
from unittest.mock import MagicMock
from database import Database
from services.setting_service import SettingsService
p,key,scen=sys.argv[1:4]; attr=key.split('.',1)[1]
db=Database(p); s=SettingsService(db); c=s.load_roserpina_config()
out={'pid':__import__('os').getpid(),'cfg_valore':repr(getattr(c,attr)) if hasattr(c,attr) else '<assente>','cfg_tipo':type(getattr(c,attr,None)).__name__ if hasattr(c,attr) else 'n/a','raw_db':db.get_settings().get(key),'saghe_pendenti':len(db.get_pending_sagas() or [])}
from core.runtime_controller import RuntimeController
bus=MagicMock(); pubs=[]; bus.publish=lambda t,*a,**k: pubs.append(t)
bf=MagicMock(); bf._session_invalid=False; bf.connect.return_value={'ok':True}; bf.get_account_funds.return_value={'available':float(__import__('os').environ.get('FUNDS_B','989.0'))}
with contextlib.redirect_stdout(io.StringIO()):
    rc=RuntimeController(bus=bus,db=db,settings_service=s,betfair_service=bf,telegram_service=MagicMock())
class RE:
    def fetch_startup_active_orders(self):
        if scen=='incerto': raise ConnectionError('exchange non raggiungibile: esito ordini incerto')
        if scen=='fantasma': return [{'bet_id':'BX-GHOST','market_id':'1.999','selection_id':7,'side':'BACK','size_remaining':3.0,'price':2.0,'customer_ref':'CR-GHOST'}]
        return []
    def merge_startup_active_orders(self,o): return {'orders':o}
    def reconcile_all_open_batches(self): return None
    def __getattr__(self,n): return MagicMock()
rc._build_reconciliation_engine=lambda: RE()
rc._start_market_data_feed=lambda: None; rc._start_settlement_poller=lambda: None
rc._catalog_sync=MagicMock()
rec={}
try:
    import core.state_recovery as sr
    orig=sr.StateRecovery.recover
    def wrap(self): r=orig(self); rec.update(r); return r
    sr.StateRecovery.recover=wrap
    with contextlib.redirect_stdout(io.StringIO()):
        rc.start(execution_mode='SIMULATION')
    out['start']='ok'
except Exception as e:
    out['start']='eccezione:'+repr(e)[:160]
out['recovery']=rec; out['modo_dopo_start']=str(getattr(rc,'mode','')); out['recover_order_pubblicato']=pubs.count('RECOVER_ORDER')
out['config_runtime_valore']=repr(getattr(rc.config,attr)) if hasattr(rc.config,attr) else '<assente>'
out['equity_peak']=float(getattr(rc.risk_desk,'equity_peak',0) or 0); out['bankroll']=float(getattr(rc.risk_desk,'bankroll_current',0) or 0)
rej=[]; rc._reject_signal=lambda sg,r: rej.append(r); rc._release_acquired_and_reject=lambda sg,**k: rej.append(k.get('reason'))
d=MagicMock(); d.approved=True; d.recommended_stake=1.0; d.table_id=1; d.reason=''; rc.mm.calculate=MagicMock(return_value=d)
try:
    rc._on_signal_received({'market_id':'1.123','selection_id':'456','price':2.0,'signal_type':'BACK'})
    out['segnale_dopo_start']={'rifiuti':rej,'arrivato_al_sizing':rc.mm.calculate.called}
except Exception as e: out['segnale_dopo_start']={'eccezione':repr(e)[:160]}
print(json.dumps(out))
'''%BASE
def run(key,fx,fx2,mode,scen,funds_b='989.0'):
    p=os.path.join(tempfile.mkdtemp(),'r.sqlite')
    a=subprocess.run([PY,'-c',A,p,key,str(fx),str(fx2),mode],capture_output=True,text=True)
    b=subprocess.run([PY,'-c',B,p,key,scen],capture_output=True,text=True,timeout=120,env=dict(os.environ,FUNDS_B=funds_b))
    try: ob=json.loads(b.stdout.strip().splitlines()[-1])
    except Exception: ob={'errore_B':(b.stderr or b.stdout)[-400:]}
    return {'fase_A':{'returncode':a.returncode,'ucciso_da_SIGKILL':a.returncode==-signal.SIGKILL,'stdout':a.stdout.strip(),'stderr':a.stderr.strip()[-200:]},'fase_B':ob,'db_file':'stesso file, processo nuovo'}
keys=json.loads(sys.argv[1]); out={}
DD={'RSR-05','RSR-06','RSR-07','RSR-20','RSR-41'}
for rid,key,fx,fx2 in keys:
    out[rid]={'key':key,'fixture':fx,'fixture_non_committata':fx2,
      'commit_poi_kill':run(key,fx,fx2,'kill_after_commit','normale'),
      'kill_in_transazione':run(key,fx,fx2,'kill_mid_tx','normale'),
      'riavvio_esito_incerto':run(key,fx,fx2,'kill_after_commit','incerto'),
      'riavvio_ordine_fantasma':run(key,fx,fx2,'kill_after_commit','fantasma')}
    if rid in DD:
        out[rid]['memoria_drawdown_1000_poi_989']=run(key,fx,fx2,'dd','normale','989.0')
        out[rid]['memoria_drawdown_1000_poi_990.01']=run(key,fx,fx2,'dd','normale','990.01')
        out[rid]['memoria_drawdown_1000_poi_990.00']=run(key,fx,fx2,'dd','normale','990.0')
    print(rid,flush=True)
prev=json.load(open(os.environ.get('PROBE_OUT','/workspace/pickfair/bf2/close02/probe_restart_v2.json'))) if len(keys)<38 else {}
prev.update(out); out=prev
json.dump(out,open(os.environ.get('PROBE_OUT','/workspace/pickfair/bf2/close02/probe_restart_v2.json'),'w'),indent=1,ensure_ascii=False)
