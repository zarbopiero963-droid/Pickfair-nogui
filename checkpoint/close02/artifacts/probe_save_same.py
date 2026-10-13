"""Sonda SAVE bordo a 028cee9: salvataggio dello stesso valore gia' committato."""
import json,os,sys,tempfile
sys.path.insert(0,os.getcwd())
from database import Database
from services.setting_service import SettingsService
PRB=json.load(open('../probe_rsr.json')); out={}
for i,p in PRB.items():
    if not p.get('in_config'): continue
    d=tempfile.mkdtemp(); s=SettingsService(Database(os.path.join(d,'s.sqlite'))); c=s.load_roserpina_config()
    s.save_roserpina_config(c); a=dict(s.db.get_settings()); s.save_roserpina_config(c); b=dict(s.db.get_settings())
    out[i]={'valore_invariato':a.get(p['key'])==b.get(p['key']),'altre_chiavi_cambiate':sorted(k for k in b if a.get(k)!=b.get(k)),'config_version':b.get('roserpina.config_version')}
json.dump(out,open('../probe_save_same.json','w'),ensure_ascii=False,indent=1); print(len(out))
