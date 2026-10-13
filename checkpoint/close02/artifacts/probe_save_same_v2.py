"""Sonda SAVE 'salvataggio con valore invariato' a 028cee9 per tutte le chiavi del lotto SAVE."""
import sys,json,tempfile,os
sys.path.insert(0,'/workspace/pickfair/bf2/close02/wt_base')
from database import Database
from services.setting_service import SettingsService
out={}
for rid,key,fx in json.loads(sys.argv[1]):
    p=os.path.join(tempfile.mkdtemp(),'s.sqlite'); db=Database(p); s=SettingsService(db)
    db.save_settings({key:str(fx)}); prima=dict(db.get_settings())
    c=s.load_roserpina_config(); s.save_roserpina_config(c); dopo=dict(Database(p).get_settings())
    out[rid]={'key':key,'raw_prima':prima.get(key),'raw_dopo':dopo.get(key),'versione_dopo':dopo.get('roserpina.config_version'),
              'altre_chiavi_cambiate':sorted(k for k in prima if k!=key and prima[k]!=dopo.get(k))}
json.dump(out,open('/workspace/pickfair/bf2/close02/probe_save_same_v2.json','w'),indent=1)
print(len(out))
