"""close02 (Claude) — L4 RSR SAVE/VALID su SettingsService reale (DB SQLite temporaneo reale). Nessuna rete.
Fonti (phase_row_verbatim L4, #461/6085510337):
- SAVE: «Salvare atomicamente il valore e la versione di configurazione tramite la chiave persistente prevista»
- VALID: «Validare dominio del campo prima di qualunque salvataggio/uso; rifiutare input malformati senza mutare
  l'ultimo stato valido».
Domini precisi non definiti in fonte (range nullo): si usano solo valori malformati per OGNI chiave numerica
(NaN, +inf, negativo, testo). Oracolo VALID: il salvataggio fallisce oppure non muta il valore persistito."""
import json, math, os, sys, tempfile
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from database import Database  # noqa: E402
from services.setting_service import SettingsService  # noqa: E402

KEYS = [k for _, k in json.load(open(os.environ["KEYS_FILE"]))]
# valori in forma canonica (float come li scrive il SettingsService): il caso "5" vs "5.0" e' CL24, separato
VALID = {"roserpina.max_single_bet_pct": "5.0", "roserpina.max_total_exposure_pct": "20.0",
         "roserpina.max_event_exposure_pct": "10.0", "roserpina.max_stake_abs": "50.0", "roserpina.commission_pct": "4.5",
         "roserpina.max_recovery_tables": "1", "roserpina.allow_recovery": "0", "roserpina.max_open_exposure": "10.0",
         "roserpina.max_drawdown_abs": "10.0", "roserpina.max_daily_loss": "20.0", "roserpina.max_order_exposure": "1.0",
         "roserpina.max_market_exposure": "2.0", "roserpina.max_event_exposure_abs": "2.0"}
out = {}


def fresh():
    p = os.path.join(tempfile.mkdtemp(), "c.sqlite"); db = Database(p); db.save_settings(dict(VALID))
    return db, SettingsService(db)


# SAVE: valore + versione nella stessa transazione
db, ss = fresh()
cfg = ss.load_roserpina_config(); v0 = db.get_settings().get("roserpina.config_version")
cfg.max_single_bet_pct = 7.5
ss.save_roserpina_config(cfg)
s = db.get_settings()
out["SAVE-OK"] = {"valore": s.get("roserpina.max_single_bet_pct"), "versione_prima": v0,
                  "versione_dopo": s.get("roserpina.config_version"),
                  "verdetto": "CONFORME" if float(s.get("roserpina.max_single_bet_pct")) == 7.5
                  and str(s.get("roserpina.config_version") or "") == str(int(v0 or 0) + 1) else "NON_CONFORME"}
# SAVE atomico: scrittura che fallisce a meta' => nulla cambia (ne' valore ne' versione)
db, ss = fresh()
cfg = ss.load_roserpina_config(); before = dict(db.get_settings())
orig = db.save_settings


def boom(payload):
    raise OSError("disk full")
db.save_settings = boom
cfg.max_single_bet_pct = 9.0
try:
    ss.save_roserpina_config(cfg); err = None
except Exception as exc:
    err = type(exc).__name__
db.save_settings = orig
out["SAVE-ATOMIC"] = {"errore": err, "invariato": dict(db.get_settings()) == before,
                      "verdetto": "CONFORME" if err and dict(db.get_settings()) == before else "NON_CONFORME"}

# VALID: per chiave numerica, valori malformati
nc = []
for key in KEYS:
    attr = key.split(".", 1)[1]
    db, ss = fresh()
    cfg = ss.load_roserpina_config()
    cur = getattr(cfg, attr, None)
    if isinstance(cur, bool) or (cur is not None and not isinstance(cur, (int, float))):
        continue  # chiavi booleane/enum: fuori da questa sonda numerica
    for bad in (float("nan"), float("inf"), -1.0, "abc"):
        db, ss = fresh()
        cfg = ss.load_roserpina_config()
        before = db.get_settings().get(key)
        setattr(cfg, attr, bad)
        try:
            ss.save_roserpina_config(cfg); err = None
        except Exception as exc:
            err = type(exc).__name__
        after = db.get_settings().get(key)
        ok = err is not None or after == before
        cid = f"VALID-{attr}-{bad}"
        out[cid] = {"chiave": key, "valore": str(bad), "prima": before, "dopo": after, "errore": err,
                    "verdetto": "CONFORME" if ok else "NON_CONFORME"}
        if not ok:
            nc.append(cid)
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
tot = [k for k in out if k.startswith("VALID-")]
print("SAVE-OK", out["SAVE-OK"]["verdetto"], "| SAVE-ATOMIC", out["SAVE-ATOMIC"]["verdetto"],
      "| VALID casi", len(tot), "non conformi", len(nc))
print("chiavi NC:", sorted({c.split("-")[1] for c in nc}))
