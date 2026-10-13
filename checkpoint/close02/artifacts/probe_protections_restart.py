"""kill -9 + nuovo processo sullo stesso DB: daily-loss sfondato e risk-stop attivo devono sopravvivere al riavvio."""
import sys,json,os,subprocess,tempfile,signal
ROOT=sys.argv[1]; PY=sys.executable
COMMON=r'''
import sys,os,signal,json,io,contextlib,logging;sys.path.insert(0,%r);logging.disable(logging.CRITICAL)
from unittest.mock import MagicMock
from database import Database
from services.setting_service import SettingsService
from core.runtime_controller import RuntimeController
p=sys.argv[1]; scen=sys.argv[2]
db=Database(p); ss=SettingsService(db)
def mk(funds=1000.0):
    bf=MagicMock(); bf._session_invalid=False; bf.connect.return_value={'ok':True}; bf.get_account_funds.return_value={'available':funds}
    class RE:
        def fetch_startup_active_orders(self): return []
        def __getattr__(self,n): return MagicMock()
    with contextlib.redirect_stdout(io.StringIO()):
        rc=RuntimeController(bus=MagicMock(),db=db,settings_service=ss,betfair_service=bf,telegram_service=MagicMock())
        rc._catalog_sync=MagicMock(); rc._start_market_data_feed=lambda: None; rc._start_settlement_poller=lambda: None
        rc._build_reconciliation_engine=lambda: RE(); rc._route_cashout_signal=lambda *a,**k: None
    return rc
'''%ROOT
A=COMMON+r'''
c=ss.load_roserpina_config(); c.max_daily_loss=10.0; ss.save_roserpina_config(c)
rc=mk()
with contextlib.redirect_stdout(io.StringIO()): rc.start(execution_mode='SIMULATION')
if scen=='daily_loss':
    rc.risk_desk.realized_pnl=-12.0
    st=rc._monitor_daily_loss_breach(source='PROBE')
    if st.get('breached'): rc._enforce_daily_loss_hard_stop(st)
    print('A',json.dumps({'breached':st.get('breached'),'modo':str(rc.mode)}),flush=True)
else:
    r=rc.risk_stop('session_loss_breached:probe')
    print('A',json.dumps({'risk_stop':r.get('reason'),'persisted':r.get('persisted')}),flush=True)
os.kill(os.getpid(),signal.SIGKILL)
'''
B=COMMON+r'''
rc=mk()
try:
    with contextlib.redirect_stdout(io.StringIO()): res=rc.start(execution_mode='SIMULATION')
    st=str(res.get('started')) if isinstance(res,dict) else str(res)
except Exception as e: st='eccezione:'+repr(e)[:120]
rej=[]; rc._reject_signal=lambda sg,r: rej.append(r); rc._release_acquired_and_reject=lambda sg,**k: rej.append(k.get('reason'))
d=MagicMock(); d.approved=True; d.recommended_stake=1.0; d.table_id=1; d.reason=''; rc.mm.calculate=MagicMock(return_value=d)
try: rc._on_signal_received({'market_id':'1.123','selection_id':'456','price':2.0,'signal_type':'BACK'})
except Exception as e: rej.append('eccezione:'+repr(e)[:80])
print(json.dumps({'pid':os.getpid(),'start':st,'modo':str(rc.mode),'risk_stop_reason':getattr(rc,'_risk_stop_reason',None),'daily_loss_bloccato':rc._daily_loss_entry_blocked(),'ammesso_al_sizing':rc.mm.calculate.called,'rifiuti':rej}))
'''
out={}
for scen in ('daily_loss','risk_stop'):
    p=os.path.join(tempfile.mkdtemp(),'p.sqlite')
    a=subprocess.run([PY,'-c',A,p,scen],capture_output=True,text=True,timeout=120)
    b=subprocess.run([PY,'-c',B,p,scen],capture_output=True,text=True,timeout=120)
    try: ob=json.loads(b.stdout.strip().splitlines()[-1])
    except Exception: ob={'errore':b.stderr[-400:]}
    out[scen]={'A':{'rc':a.returncode,'stdout':a.stdout.strip(),'err':a.stderr[-300:]},'B':ob}
print(json.dumps(out,indent=1))
