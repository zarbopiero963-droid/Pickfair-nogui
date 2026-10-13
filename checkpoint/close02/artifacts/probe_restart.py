"""Sonde dinamiche RESTART a 028cee9 (solo impl_attuale), per chiave:
pos: salva v1, nuovo processo simulato (nuovo Database sullo stesso file) -> valore;
neg: 'abc' persistito, nuovo Database -> valore caricato;
bnd: crash a meta' save_roserpina_config (eccezione dopo la prima chiave scritta) -> dopo
     riapertura: tutto vecchio, tutto nuovo, o misto?"""
import json,os,sys,tempfile
sys.path.insert(0,os.getcwd())
from database import Database
from services.setting_service import SettingsService
PRB=json.load(open('../probe_rsr.json')); out={}
for i,p in PRB.items():
    k=p['key']; attr=p.get('attr')
    if not p.get('in_config'): out[i]={'n/a':'assente in config'}; continue
    d=tempfile.mkdtemp(); f=os.path.join(d,'r.sqlite')
    s=SettingsService(Database(f)); c=s.load_roserpina_config(); cur=getattr(c,attr)
    import enum
    v1 = 50.0 if cur is None else (not cur if isinstance(cur,bool) else (list(type(cur))[0] if isinstance(cur,enum.Enum) and list(type(cur))[0]!=cur else (list(type(cur))[1] if isinstance(cur,enum.Enum) else type(cur)(cur)+1)))
    setattr(c,attr,v1); s.save_roserpina_config(c)
    pos=str(getattr(SettingsService(Database(f)).load_roserpina_config(),attr))
    db=Database(f); db.save_settings({k:'abc'})
    neg=str(getattr(SettingsService(Database(f)).load_roserpina_config(),attr))
    # crash a meta' salvataggio
    f2=os.path.join(d,'c.sqlite'); s2=SettingsService(Database(f2)); c2=s2.load_roserpina_config()
    before=dict(s2.db.get_settings())
    setattr(c2,attr,v1); c2.target_profit_cycle_pct=c2.target_profit_cycle_pct+0.25
    orig=s2.db._set_setting; n={'x':0}
    def boom(key,val):
        n['x']+=1
        if n['x']==2: raise RuntimeError('crash simulato')
        return orig(key,val)
    s2.db._set_setting=boom
    try: s2.save_roserpina_config(c2); crashed=False
    except RuntimeError: crashed=True
    after=SettingsService(Database(f2)).db.get_settings()
    changed=sorted(x for x in after if str(after.get(x))!=str(before.get(x)))
    out[i]={'restart_valore':pos,'scritto':str(v1),'restart_corrotto_abc':neg,'crash':crashed,'chiavi_cambiate_dopo_crash':changed[:5],'stato_misto':bool(changed)}
json.dump(out,open('../probe_restart.json','w'),ensure_ascii=False,indent=1)
print(sum(v.get('stato_misto',False) for v in out.values()),'stato misto;',sum(v.get('restart_valore')==v.get('scritto') for v in out.values()),'restart ok')
