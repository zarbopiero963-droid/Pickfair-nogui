"""close02 (Claude) — L6 FAILSAFE: chiave Roserpina corrotta via loader reale (SettingsService su DB temporaneo)
+ segnale valido sulla catena reale (RuntimeController + MM vero + SimulationBroker). Nessuna rete.
Fonte (phase_row_verbatim dei record L6 FAILSAFE, #461/6085510337): «Dati corrotti, book/provider assenti e ordine
ambiguo non devono consentire nuovi effetti monetari non autorizzati». Oracolo uniforme: valore corrotto => nessun
ordine (BLOCCATO) e chiave riconosciuta come corrotta; controllo con config valida => AMMESSO."""
import json, os, sys, tempfile
from pathlib import Path
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from database import Database  # noqa: E402
from services.setting_service import SettingsService  # noqa: E402
from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402

VALID = {"roserpina.max_single_bet_pct": "50", "roserpina.max_total_exposure_pct": "50",
         "roserpina.max_event_exposure_pct": "50", "roserpina.max_stake_abs": "5",
         "roserpina.max_order_exposure": "1", "roserpina.max_market_exposure": "2",
         "roserpina.max_event_exposure_abs": "2", "roserpina.max_drawdown_abs": "10",
         "roserpina.max_recovery_tables": "2", "roserpina.allow_recovery": "false",
         "roserpina.commission_pct": "4.5", "roserpina.max_open_exposure": "10"}
KEYS = json.load(open(os.environ["KEYS_FILE"]))


def run(data):
    p = os.path.join(tempfile.mkdtemp(), "p.sqlite")
    Database(p).save_settings(data)
    err = None
    try:
        cfg = SettingsService(Database(p)).load_roserpina_config()
    except Exception as exc:
        return {"orders": 0, "rejects": [], "exception": f"loader {type(exc).__name__}: {exc}", "corrupt_keys": []}
    c = _catena(Path(tempfile.mkdtemp())); c.rc.config = cfg
    try:
        c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
    return {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c)[:1], "exception": err,
            "corrupt_keys": list(getattr(cfg, "corrupt_keys", ()) or ())}


out = {}
o = run(dict(VALID)); out["CTRL"] = {"osservato": o, "verdetto": "CONFORME" if o["orders"] == 1 else "NON_CONFORME"}
for rid, key in KEYS:
    for val in ("abc", "nan", "inf"):
        o = run(dict(VALID, **{key: val}))
        out[f"{rid}-FS-{val}"] = {"chiave": key, "valore": val, "osservato": o,
                                  "verdetto": "CONFORME" if o["orders"] == 0 else "NON_CONFORME"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
nc = sorted({k.rsplit("-FS-", 1)[0] for k, v in out.items() if v["verdetto"] == "NON_CONFORME" and k != "CTRL"})
print("CTRL", out["CTRL"]["verdetto"], "| casi", len(out) - 1, "| non conformi",
      sum(v["verdetto"] == "NON_CONFORME" for k, v in out.items() if k != "CTRL"), "| chiavi NC:", " ".join(nc))
