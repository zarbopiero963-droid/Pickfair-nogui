"""Sonda APPLY/FAILSAFE drawdown (RSR-20, owner D1) a 028cee9: perdita dal picco vs limite effettivo, SIM e LIVE."""
import sys,json,io,contextlib
import os as _os; sys.path.insert(0,_os.environ.get('PROBE_ROOT','/workspace/pickfair/bf2/close02/wt_base'))
from unittest.mock import MagicMock
from core.runtime_controller import RuntimeController
from core.system_state import RuntimeMode, RoserpinaConfig
def run(mode,pct,peak,bank):
    cfg=RoserpinaConfig(); cfg.max_drawdown_hard_stop_pct=pct; cfg.allow_recovery=False; cfg.anti_duplication_enabled=False
    st=MagicMock(); st.load_roserpina_config.return_value=cfg; st.get_all_settings.return_value={}; st.load_live_enabled.return_value=False
    bus=MagicMock()
    with contextlib.redirect_stdout(io.StringIO()):
        bf=MagicMock(); bf._session_invalid=False
        c=RuntimeController(bus=bus,db=MagicMock(),settings_service=st,betfair_service=bf,telegram_service=MagicMock())
    c.config=cfg; c.mode=RuntimeMode.ACTIVE; c.execution_mode=mode; c.live_enabled=True; c.live_readiness_ok=True
    c.enforce_deploy_gate=MagicMock(return_value={"allowed":True,"reason":"GO"})
    c.table_manager.allocate=MagicMock(return_value=MagicMock(status="FREE"))
    c.risk_desk.equity_peak=peak; c.risk_desk.bankroll_current=bank
    d=MagicMock(); d.approved=True; d.recommended_stake=1.0; d.table_id=1; d.reason=''; c.mm.calculate=MagicMock(return_value=d)
    rej=[]; c._reject_signal=lambda s,r: rej.append(r)
    c._release_acquired_and_reject=lambda s,**k: rej.append(k.get('reason'))
    lock=[]; c.force_lockdown=lambda r: lock.append(r)
    try: c._on_signal_received({"market_id":"1.123","selection_id":"456","price":2.0,"signal_type":"BACK"})
    except Exception as e: return {'eccezione':repr(e)[:160]}
    sub=[str(x)[:80] for x in getattr(c,'_debug_calls',[])]
    return {'rifiuti':rej,'bf_place_chiamato':bool(bf.method_calls),'lockdown':lock,'mm_chiamato':c.mm.calculate.called}
S=[('SIM',None,1000.0,990.0),('LIVE',None,1000.0,990.0),('SIM',None,1000.0,989.0),('LIVE',None,1000.0,989.0),('LIVE',50.0,1000.0,989.0),('SIM',50.0,1000.0,989.0),
   ('LIVE',None,1000.0,990.01),('SIM',None,1000.0,990.01),('LIVE',0.5,1000.0,994.0)]
out={f'{m}|pct={p}|perdita={round(pk-b,2)}':run(m,p,pk,b) for m,p,pk,b in S}
json.dump(out,open(_os.environ.get('PROBE_OUT','/workspace/pickfair/bf2/close02/probe_dd_apply.json'),'w'),indent=1)
for k,v in out.items(): print(k,v)
