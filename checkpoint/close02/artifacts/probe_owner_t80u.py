"""Sonda APPLY dinamica a 028cee9 per la ratifica t80u: per ciascuna delle 7 chiavi, scenario assente / non valida ('abc'),
nuovo processo, DB su file, start SIM, segnale in ingresso: si osservano il valore effettivo e l'ammissione al sizing."""
import sys,json,os,subprocess,tempfile
import os as _os; BASE=_os.environ.get('PROBE_ROOT','/workspace/pickfair/bf2/close02/wt_base'); PY=sys.executable
K={'RSR-02':'roserpina.max_single_bet_pct','RSR-03':'roserpina.max_total_exposure_pct','RSR-04':'roserpina.max_event_exposure_pct','RSR-15':'roserpina.max_stake_abs','RSR-12':'roserpina.max_recovery_tables','RSR-31':'roserpina.allow_recovery','RSR-13':'roserpina.commission_pct'}
VALID={'roserpina.max_single_bet_pct':'5.0','roserpina.max_total_exposure_pct':'20.0','roserpina.max_event_exposure_pct':'10.0','roserpina.max_stake_abs':'50.0','roserpina.max_recovery_tables':'1','roserpina.allow_recovery':'0','roserpina.commission_pct':'5.0'}
B=r'''
import sys,json,io,contextlib,logging;sys.path.insert(0,%r);logging.disable(logging.CRITICAL)
from unittest.mock import MagicMock
from database import Database
from services.setting_service import SettingsService
p,key,scen,valid=sys.argv[1:5]; valid=json.loads(valid); attr=key.split('.',1)[1]
db=Database(p); s={k:v for k,v in valid.items() if k!=key or scen=='controllo_tutte_valide'}
if scen=='non_valida': s[key]='abc'
db.save_settings(s); ss=SettingsService(db); c=ss.load_roserpina_config()
from core.runtime_controller import RuntimeController
bf=MagicMock(); bf._session_invalid=False; bf.connect.return_value={'ok':True}; bf.get_account_funds.return_value={'available':1000.0}
with contextlib.redirect_stdout(io.StringIO()):
    rc=RuntimeController(bus=MagicMock(),db=db,settings_service=ss,betfair_service=bf,telegram_service=MagicMock())
    rc._catalog_sync=MagicMock(); rc._start_market_data_feed=lambda: None; rc._start_settlement_poller=lambda: None
    class RE:
        def fetch_startup_active_orders(self): return []
        def __getattr__(self,n): return MagicMock()
    rc._build_reconciliation_engine=lambda: RE()
    rc.start(execution_mode='SIMULATION')
rej=[]; rc._reject_signal=lambda sg,r: rej.append(r); rc._release_acquired_and_reject=lambda sg,**k: rej.append(k.get('reason'))
d=MagicMock(); d.approved=True; d.recommended_stake=1.0; d.table_id=1; d.reason=''; rc.mm.calculate=MagicMock(return_value=d)
rc._on_signal_received({'market_id':'1.123','selection_id':'456','price':2.0,'signal_type':'BACK'})
print(json.dumps({'raw_db':db.get_settings().get(key),'valore_effettivo':repr(getattr(rc.config,attr,'<assente>')),'segnali_config':[],'modo':str(rc.mode),'rifiuti':rej,'nuovo_rischio_arrivato_al_sizing':rc.mm.calculate.called}))
'''%BASE
out={}
for rid,key in K.items():
    for scen in ('assente','non_valida','controllo_tutte_valide'):
        p=os.path.join(tempfile.mkdtemp(),'o.sqlite')
        r=subprocess.run([PY,'-c',B,p,key,scen,json.dumps(VALID)],capture_output=True,text=True,timeout=120)
        try: o=json.loads(r.stdout.strip().splitlines()[-1])
        except Exception: o={'errore':r.stderr[-400:]}
        out[f'{rid}|{scen}']=dict(key=key,altre_chiavi_valide=VALID,**o)
json.dump(out,open(_os.environ.get('PROBE_OUT','/workspace/pickfair/bf2/close02/probe_owner_t80u.json'),'w'),indent=1)
for k,v in out.items(): print(k,v.get('valore_effettivo'),v.get('nuovo_rischio_arrivato_al_sizing'),v.get('errore','')[-200:])
