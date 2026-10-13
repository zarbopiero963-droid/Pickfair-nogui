"""Sonda LOAD v3 a 028cee9: assente, override, override==default codice, formato alternativo, vuoto, corrotto, riavvio in PROCESSO NUOVO sullo stesso DB."""
import sys,json,tempfile,os,logging,subprocess
import os as _os; BASE=_os.environ.get('PROBE_ROOT','/workspace/pickfair/bf2/close02/wt_base'); sys.path.insert(0,BASE)
from database import Database
from services.setting_service import SettingsService
from core.system_state import RoserpinaConfig
NA='<attributo assente in RoserpinaConfig>'
class H(logging.Handler):
    def __init__(s): super().__init__(logging.DEBUG); s.r=[]
    def emit(s,r): s.r.append((r.levelname,r.getMessage()[:160]))
h=H(); logging.getLogger().addHandler(h); logging.getLogger().setLevel(logging.DEBUG)
def obs(p,attr):
    h.r.clear(); c=SettingsService(Database(p)).load_roserpina_config()
    v=getattr(c,attr) if hasattr(c,attr) else None
    return {'valore':repr(v) if hasattr(c,attr) else NA,'tipo':type(v).__name__ if hasattr(c,attr) else 'n/a',
            'segnali':[m for m in h.r if m[0] in('WARNING','ERROR','CRITICAL')],
            'flag':{n:str(getattr(c,n)) for n in ('corrupt_keys','absent_keys','empty_keys','absent_optional_keys','defaulted_keys') if hasattr(c,n)}}
CHILD=r'''
import sys,json;sys.path.insert(0,%r)
from database import Database
from services.setting_service import SettingsService
c=SettingsService(Database(sys.argv[1])).load_roserpina_config();a=sys.argv[2]
print(json.dumps({'valore':repr(getattr(c,a)) if hasattr(c,a) else %r,'tipo':type(getattr(c,a,None)).__name__ if hasattr(c,a) else 'n/a','pid':__import__('os').getpid()}))
'''%(BASE,NA)
keys=json.loads(sys.argv[1]); out={}
for rid,key,fx,alt in keys:
    attr=key.split('.',1)[1]; d=RoserpinaConfig(); inc=hasattr(d,attr)
    dflt=getattr(d,attr) if inc else None; dv=getattr(dflt,'value',dflt)
    r={'key':key,'in_config':inc,'default_codice':repr(dflt) if inc else NA}
    def fresh(): return os.path.join(tempfile.mkdtemp(),'p.sqlite')
    p=fresh(); r['assente']=dict(obs(p,attr),raw_db=Database(p).get_settings().get(key))
    for name,raw in (('override',str(fx)),('override_uguale_default',None if dv is None else str(dv)),('formato_alternativo',alt),('vuoto',''),('corrotto_abc','abc')):
        if raw is None: r[name]=None; continue
        p=fresh(); Database(p).save_settings({key:raw}); r[name]=dict(obs(p,attr),raw=raw)
    p=fresh(); Database(p).save_settings({key:str(fx)}); prima=obs(p,attr)
    ch=subprocess.run([sys.executable,'-c',CHILD,p,attr],capture_output=True,text=True)
    r['riavvio']={'sequenza':['scrittura raw su DB file','load processo sonda','load in PROCESSO NUOVO stesso file'],'stesso_db':p,'processo_nuovo':True,'prima':prima,'dopo':json.loads(ch.stdout.strip().splitlines()[-1]),'pid_sonda':os.getpid()}
    out[rid]=r
json.dump(out,open(_os.environ.get('PROBE_OUT','/workspace/pickfair/bf2/close02/probe_load_v3.json'),'w'),indent=1,ensure_ascii=False)
print(len(out))
