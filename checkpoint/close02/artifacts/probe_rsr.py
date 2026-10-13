import sys,json,tempfile,dataclasses,os
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
from database import Database
from services.setting_service import SettingsService
from core.system_state import RoserpinaConfig
C=json.load(open('/workspace/pickfair/bf2/close02/rsr_controls.json'))
F={f.name for f in dataclasses.fields(RoserpinaConfig)}
out={}
def fresh():
    d=tempfile.mkdtemp(); p=os.path.join(d,'p.sqlite'); return p
for c in C:
    key=c.get('key') or ''; attr=key.split('.',1)[-1]
    r={'key':key,'attr':attr,'in_config':attr in F}
    if attr not in F: out[c['id']]=r; continue
    dflt=getattr(RoserpinaConfig(),attr)
    if isinstance(dflt,bool): fx=not dflt
    elif isinstance(dflt,int) and not isinstance(dflt,bool): fx=dflt+1
    elif isinstance(dflt,float): fx=round(dflt+1.25,2)
    elif dflt is None: fx=7.5
    elif hasattr(dflt,'value'): fx=None
    else: fx=None
    r['default_class']=repr(dflt); r['fixture']=fx
    p=fresh()
    try:
        db=Database(p); s=SettingsService(db); cfg=RoserpinaConfig()
        if fx is not None: setattr(cfg,attr,fx)
        s.save_roserpina_config(cfg)
        r['stored_raw']=db.get_settings().get(key)
        r['load_same']=repr(getattr(s.load_roserpina_config(),attr))
        db2=Database(p); r['load_restart']=repr(getattr(SettingsService(db2).load_roserpina_config(),attr))
        for bad in ('abc','nan','inf','-1',''):
            db2.save_settings({key:bad}); r['corrupt_'+(bad or 'empty')]=repr(getattr(SettingsService(db2).load_roserpina_config(),attr))
    except Exception as e: r['error']=f'{type(e).__name__}: {e}'[:200]
    out[c['id']]=r
json.dump(out,open('probe_rsr.json','w'),indent=1)
for k,v in out.items(): print(k,v.get('attr'),v.get('fixture'),v.get('load_restart'),'| abc',v.get('corrupt_abc'),'nan',v.get('corrupt_nan'),'-1',v.get('corrupt_-1'),v.get('error',''))
