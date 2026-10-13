"""Sonde a 028cee9 SOLO per impl_attuale (non requisiti):
APPLY = sonda statica: dove l'attributo della chiave e' consumato nel codice (file:riga);
RACE  = sonda dinamica: due salvataggi interleaved (A e B caricano, A salva K=v2, B salva
        un'altra chiave dal caricamento vecchio) -> K sopravvive? config_version presente?"""
import json,os,re,subprocess,sys,tempfile
sys.path.insert(0,os.getcwd())
from database import Database
from services.setting_service import SettingsService
CTL=json.load(open('../rsr_controls.json')); PRB=json.load(open('../probe_rsr.json'))
out={}
for c in CTL:
    p=PRB.get(c['id'],{}); attr=p.get('attr'); k=c['key']
    if not attr or not p.get('in_config'):
        out[c['id']]={'apply':'chiave assente in RoserpinaConfig a 028cee9','race':'n/a'}; continue
    g=subprocess.run(['rg','-n','--no-heading',r'\.'+attr+r'\b','-g','*.py','-g','!tests/**','.'],capture_output=True,text=True).stdout.splitlines()
    cons=[l.split(':')[0].lstrip('./')+':'+l.split(':')[1] for l in g if 'setting_service' not in l and 'system_state' not in l][:8]
    d=tempfile.mkdtemp(); db=Database(os.path.join(d,'p.sqlite')); s=SettingsService(db)
    a=s.load_roserpina_config(); b=s.load_roserpina_config()
    cur=getattr(a,attr)
    import enum
    new = 50.0 if cur is None else (list(type(cur))[1] if list(type(cur))[0]==cur else list(type(cur))[0]) if isinstance(cur,enum.Enum) else (not cur) if isinstance(cur,bool) else (cur.__class__(cur)+1 if isinstance(cur,(int,float)) and not isinstance(cur,bool) else cur)
    try:
        setattr(a,attr,new); s.save_roserpina_config(a)
        other='target_profit_cycle_pct' if attr!='target_profit_cycle_pct' else 'max_single_bet_pct'
        setattr(b,other,getattr(b,other)+0.5); s.save_roserpina_config(b)
        fin=getattr(s.load_roserpina_config(),attr)
        allset=s.get_all_settings() if hasattr(s,'get_all_settings') else {}
        race={'scritto_da_A':str(new),'finale_dopo_B':str(fin),'perso':str(fin)!=str(new),'config_version_persistita':'roserpina.config_version' in allset}
    except Exception as e:
        race={'errore':type(e).__name__}
    out[c['id']]={'apply':'consumatori (statico): '+(', '.join(cons) if cons else 'nessuno fuori da settings/system_state'),'race':race}
json.dump(out,open('../probe_apply_race.json','w'),ensure_ascii=False,indent=1)
print(sum(1 for v in out.values() if isinstance(v['race'],dict) and v['race'].get('perso')),'/',len(out),'chiavi con lost update')
