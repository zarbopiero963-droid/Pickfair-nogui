"""close02 (Claude) — L6 AUDIT / OBSERVABLE. Catena reale avviata (RuntimeController.start in SIMULATION su DB
SQLite temporaneo reale), un segnale RIFIUTATO dal cap e uno AMMESSO. Si cerca, FUORI dai log testuali:
- AUDIT (#461/6085510337, verbatim): «Ricostruire valore, decisione, origine, versione, correlation_id, esito e
  recovery senza esporre segreti» => nel DB o nello stato del runtime;
- OBSERVABLE (mandato owner CLOSE03 §3): blocchi/esposizioni/stati incerti «rilevabili senza affidarsi
  esclusivamente ai log testuali» => get_status()/readiness o righe persistite.
Nessuna rete."""
import json, os, sqlite3, sys, tempfile
ROOT = os.environ["PROBE_ROOT"]; sys.path.insert(0, ROOT); os.chdir(ROOT)
import logging; logging.disable(logging.CRITICAL)
from unittest.mock import MagicMock
import pytest  # noqa: F401
from tests.close02.test_close02_restart_t81u import OWNER_VALID, _ctl  # noqa: E402


class MP:
    def setattr(self, obj, name, val):
        setattr(obj, name, val)


d = tempfile.mkdtemp(); db = os.path.join(d, "a.sqlite")
owner = dict(OWNER_VALID, **{"roserpina.max_open_exposure": "1"})
rc = _ctl(db, MP(), owner=owner)
rc.start(execution_mode="SIMULATION")
d0 = MagicMock(approved=True, recommended_stake=5.0, table_id=1, reason="")
rc.mm.calculate = MagicMock(side_effect=lambda **kw: MagicMock(approved=True, recommended_stake=5.0, table_id=1, reason=""))
rc._on_signal_received({"market_id": "1.123", "selection_id": "456", "price": 2.0, "signal_type": "BACK",
                        "customer_ref": "sig-audit-reject"})
reason_pub = [c.args[1].get("reason") for c in rc.bus.publish.call_args_list
              if c.args and c.args[0] == "SIGNAL_REJECTED" and isinstance(c.args[1], dict)]
con = sqlite3.connect(db); con.row_factory = sqlite3.Row
tables = [r[0] for r in con.execute("select name from sqlite_master where type='table'")]
hits = {}
needle = (reason_pub[0] if reason_pub else "max_open_exposure").split(":")[0]
for t in tables:
    try:
        rows = con.execute(f"select * from {t}").fetchall()
    except Exception:
        continue
    for r in rows:
        txt = json.dumps([str(x) for x in tuple(r)])
        if needle in txt or "sig-audit-reject" in txt:
            hits.setdefault(t, 0); hits[t] += 1
status = rc.get_status() if callable(getattr(rc, "get_status", None)) else {}
st_txt = json.dumps(status, default=str)
out = {
    "rifiuto_pubblicato_sul_bus": reason_pub,
    "rifiuto_persistito_nel_db": hits,
    "rifiuto_visibile_in_get_status": needle in st_txt,
    "chiavi_get_status": sorted(status.keys()) if isinstance(status, dict) else [],
    "config_version_in_get_status": "config_version" in st_txt,
}
out["AUDIT-REJECT"] = "CONFORME" if hits else "NON_CONFORME"
out["OBS-REJECT"] = "CONFORME" if (hits or out["rifiuto_visibile_in_get_status"]) else "NON_CONFORME"
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False, default=str)
print(json.dumps({k: v for k, v in out.items() if k != "chiavi_get_status"}, ensure_ascii=False, default=str))
print("get_status keys:", out["chiavi_get_status"][:40])
