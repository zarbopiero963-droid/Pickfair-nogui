"""Sonda dinamica LOAD v2 a 028cee9 (wt_base): per chiave osserva assente/vuoto/corrotto/override/tipo/riavvio + log emessi."""
import sys,json,tempfile,os,logging,dataclasses
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
from database import Database
from services.setting_service import SettingsService
from core.system_state import RoserpinaConfig
keys=json.loads(sys.argv[1]); out={}
class H(logging.Handler):
    def __init__(s): super().__init__(logging.DEBUG); s.r=[]
    def emit(s,r): s.r.append((r.levelname,r.getMessage()[:160]))
h=H(); logging.getLogger().addHandler(h); logging.getLogger().setLevel(logging.DEBUG)
def load(p):
    h.r.clear(); c=SettingsService(Database(p)).load_roserpina_config()
    return c,[m for m in h.r if m[0] in('WARNING','ERROR','CRITICAL')]
def flags(c,attr): return {n:str(getattr(c,n)) for n in ('corrupt_keys','absent_keys','loaded_values') if hasattr(c,n)}
for rid,key,fx in keys:
    attr=key.split('.',1)[1]
    NA='<attributo assente in RoserpinaConfig>'
    if not hasattr(RoserpinaConfig(),attr):
        g=lambda c: repr(getattr(c,attr)) if hasattr(c,attr) else NA
        r={'in_config':False,'key':key,'default':NA,'type_default':'n/a'}
        p=os.path.join(tempfile.mkdtemp(),'p.sqlite'); c,lg=load(p); r['assente']={'valore':g(c),'log_warn':lg,'flags':flags(c,attr),'raw_db':Database(p).get_settings().get(key)}
        db=Database(p); db.save_settings({key:str(fx)}); c,lg=load(p); r['override']={'raw':str(fx),'valore':g(c),'tipo':type(getattr(c,attr,None)).__name__ if hasattr(c,attr) else 'n/a','log_warn':lg}
        c,lg=load(p); r['riavvio']={'valore':g(c)}
        for bad in ('','abc','nan','inf'):
            db.save_settings({key:bad}); c,lg=load(p); r['raw_'+(bad or 'vuoto')]={'valore':g(c),'log_warn':lg,'flags':flags(c,attr)}
        out[rid]=r; continue
    dflt=getattr(RoserpinaConfig(),attr); r={'in_config':True,'key':key,'default':repr(dflt),'type_default':type(dflt).__name__}
    p=os.path.join(tempfile.mkdtemp(),'p.sqlite')
    c,lg=load(p); r['assente']={'valore':repr(getattr(c,attr)),'log_warn':lg,'flags':flags(c,attr),'raw_db':Database(p).get_settings().get(key)}
    db=Database(p); db.save_settings({key:str(fx)})
    c,lg=load(p); r['override']={'raw':str(fx),'valore':repr(getattr(c,attr)),'tipo':type(getattr(c,attr)).__name__,'log_warn':lg}
    c,lg=load(p); r['riavvio']={'valore':repr(getattr(c,attr))}
    for bad in ('','abc','nan','inf'):
        db.save_settings({key:bad}); c,lg=load(p)
        r['raw_'+(bad or 'vuoto')]={'valore':repr(getattr(c,attr)),'log_warn':lg,'flags':flags(c,attr)}
    out[rid]=r
json.dump(out,open('/workspace/pickfair/bf2/close02/probe_load_v2.json','w'),indent=1,ensure_ascii=False)
print(len(out))
