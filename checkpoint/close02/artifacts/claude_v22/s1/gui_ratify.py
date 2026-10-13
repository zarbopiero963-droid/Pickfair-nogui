"""close02 (Claude) — S1/t80u dal vivo: la GUI su DB vuoto, salvataggio Roserpina SENZA modifiche. Oracolo
(owner t80u #426/6143637861 e Small Bet/PF decisione «nessuna ratifica automatica»): le chiavi assenti restano
assenti (nessun default del codice diventa valore owner) e il nuovo rischio resta bloccato."""
import json, os, sys, time
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import tkinter.messagebox as mb
for n in ("showinfo", "showwarning", "showerror"):
    setattr(mb, n, lambda *a, **k: None)
for n in ("askyesno", "askokcancel"):
    setattr(mb, n, lambda *a, **k: True)
import mini_gui
from core import validators
app = mini_gui.MiniPickfairGUI(force_simulation=True)
for _ in range(10):
    app.update(); time.sleep(0.05)
ss = app.settings_service
before = {k: v for k, v in (ss.db.get_settings() or {}).items() if k.startswith("roserpina.")}
cfg0 = ss.load_roserpina_config()
app._save_roserpina_settings()
after = {k: v for k, v in (ss.db.get_settings() or {}).items() if k.startswith("roserpina.")}
cfg1 = ss.load_roserpina_config()
out = {"chiavi_prima": sorted(before), "assenti_prima": sorted(getattr(cfg0, "absent_keys", ())),
       "scritte_dal_salvataggio": {k: after[k] for k in sorted(set(after) - set(before))},
       "assenti_dopo": sorted(getattr(cfg1, "absent_keys", ())),
       "t80u_prima": validators.owner_t80u_new_risk_block_reason(cfg0) if hasattr(validators, "owner_t80u_new_risk_block_reason") else "n/a",
       "t80u_dopo": validators.owner_t80u_new_risk_block_reason(cfg1) if hasattr(validators, "owner_t80u_new_risk_block_reason") else "n/a"}
ratificate = sorted(set(out["assenti_prima"]) - set(out["assenti_dopo"]))
out["default_ratificati_senza_modifica"] = ratificate
out["verdetto"] = "CONFORME" if not ratificate else "NON_CONFORME"
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
print(out["verdetto"], "ratificate:", ratificate)
print("t80u prima:", str(out["t80u_prima"])[:80], "| dopo:", str(out["t80u_dopo"])[:80])
try:
    app.destroy()
except Exception:
    pass
