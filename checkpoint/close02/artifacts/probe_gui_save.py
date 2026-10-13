"""Sonda SAVE lato GUI a 028cee9: MiniGUI._save_roserpina_settings con var stub e DB reale; SUCCESS ('OK') solo dopo commit."""
import sys,json,re,inspect,tempfile,os,dataclasses
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
import mini_gui
from database import Database
from services.setting_service import SettingsService
from core.system_state import RoserpinaConfig
src=inspect.getsource(mini_gui.MiniPickfairGUI._save_roserpina_settings)
m=dict(re.findall(r"(\w+)=\w*\(?\s*(?:float|int|bool|RiskProfile)?\(?self\.(rs_\w+_var)\.get\(\)",src))
m={v:k for k,v in m.items()}
m.update({v:k for k,v in re.findall(r"(\w+)=self\._parse_\w+\(self\.(rs_\w+_var)\.get\(\)",src)})
extra={'rs_book_warning_var':'book_warning','rs_book_block_var':'book_block','rs_liq_multiplier_var':'liquidity_multiplier','rs_liq_min_abs_var':'min_liquidity_absolute',
 'rs_min_price_var':'min_price','rs_max_win_var':'max_win','rs_profit_epsilon_var':'profit_epsilon','rs_max_stake_pct_var':'max_stake_pct',
 'rs_auto_green_delay_enabled_var':'auto_green_delay_enabled','rs_auto_green_delay_sec_var':'auto_green_delay_sec'}
m.update(extra)
class Var:
    def __init__(s,v): s.v=v
    def get(s): return s.v
    def set(s,v): s.v=v
def gui(svc,over):
    g=object.__new__(mini_gui.MiniPickfairGUI); d=RoserpinaConfig(); ev=[]
    for var,attr in m.items():
        v=getattr(d,attr,None); v=getattr(v,'value',v); v='' if v is None else v
        v=over.get(attr,v); v=v if isinstance(v,bool) else str(v)
        setattr(g,var,Var(v))
    class _TK:
        def __getattr__(s,a): raise AttributeError('GUI attr mancante: '+a)
    g.__dict__['tk']=_TK()
    g.settings_service=svc; g.runtime=object()
    g._safe_show_error=lambda t,msg: ev.append(('ERROR',t,str(msg)[:100],dict(svc.db.get_settings())))
    g._safe_show_info=lambda t,msg: ev.append(('INFO',t,msg,dict(svc.db.get_settings())))
    g.__dict__.setdefault('_refresh_hard_stop_fields',lambda *a,**k:None)
    return g,ev
keys=json.loads(sys.argv[1]); out={}
for rid,key,fx in keys:
    attr=key.split('.',1)[1]; r={}
    for scen in ('ok','commit_fallito','commit_fallito_con_precedente','versione_fallita'):
        p=os.path.join(tempfile.mkdtemp(),'g.sqlite'); db=Database(p); svc=SettingsService(db)
        if scen=='commit_fallito_con_precedente': db.save_settings({key:'PRECEDENTE'})
        n0=len(db.get_settings())
        if scen!='ok':
            o=db._set_setting
            tgt='roserpina.config_version' if scen=='versione_fallita' else key
            def boom(k,v,o=o,key=tgt):
                if k==key: raise OSError('commit fallito iniettato su '+key)
                return o(k,v)
            db._set_setting=boom
        g,ev=gui(svc,{attr:fx})
        try: g._save_roserpina_settings(); exc=None
        except Exception as e: exc=repr(e)[:120]
        r[scen]={'eventi':[(a,b,c,{'valore_db_al_messaggio':d.get(key),'versione_db':d.get('roserpina.config_version')}) for a,b,c,d in ev],'eccezione':exc,'db_finale':Database(p).get_settings().get(key),'righe_prima':n0,'righe_dopo':len(Database(p).get_settings()),'versione_finale':Database(p).get_settings().get('roserpina.config_version'),'var_gui_presente':attr in m.values()}
    out[rid]=dict(key=key,fixture=fx,**r)
json.dump(out,open('/workspace/pickfair/bf2/close02/probe_gui_save.json','w'),indent=1,ensure_ascii=False)
print(len(out)); print(json.dumps(out[keys[0][0]],ensure_ascii=False)[:700])
