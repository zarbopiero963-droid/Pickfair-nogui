"""close02 v16 (Claude) — sonda dinamica APPLY per le chiavi monetarie Roserpina.

Esegue la catena reale (RuntimeController + money management VERO + SimulationBroker) tramite l'harness
`tests/acceptance/test_issue437_pr27._catena` dell'albero indicato da PROBE_ROOT. Nessuna rete, nessun LIVE.
Ogni caso ha un predicato atteso dichiarativo derivato dalle regole owner:
  D2  #426/6141902809: cap obbligatorio assente/non valido => blocco di ogni nuovo rischio;
  Q3  #426/6141450045: esposizione = cap ammessa, oltre bloccata; drawdown stop a perdita >= 10,00 EUR;
  Q4  #426/6141450045: rischio LAY = liability = stake x (quota - 1);
  t80u(1) #426/6143637861: max_single_bet_pct/max_total_exposure_pct/max_event_exposure_pct/max_stake_abs
      assenti/non validi => blocco di ogni nuovo rischio, nessun default del codice approvato.
Uscita: JSON {case_id: {expected, observed, verdict}} su PROBE_OUT."""
import json
import os
import sys
import tempfile
from pathlib import Path

ROOT = os.environ["PROBE_ROOT"]
sys.path.insert(0, ROOT)
os.chdir(ROOT)

from tests.acceptance.test_issue437_pr27 import _catena, _rifiuti  # noqa: E402
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal  # noqa: E402


def run(cfg, sig, setup=None):
    c = _catena(Path(tempfile.mkdtemp()))
    for k, v in cfg.items():
        setattr(c.rc.config, k, v)
    if setup:
        setup(c)
    try:
        c.rc._on_signal_received(_signal(**sig))
        err = None
    except Exception as exc:  # una sonda non deve mai mascherare un crash
        err = f"{type(exc).__name__}: {exc}"
    return {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c), "exception": err}


def occupy(event_or_market_exposure):
    def _s(c):
        sig = _signal(stake=0.5, bet_type="BACK")
        t = c.rc.table_manager._tables[2]
        t.status = "ACTIVE"
        t.current_event_key = c.rc.duplication_guard.build_event_key(sig)
        t.market_id = str(sig["market_id"])
        t.current_exposure = event_or_market_exposure
    return _s


def drawdown(peak, bankroll):
    def _s(c):
        c.rc.risk_desk.equity_peak = peak
        c.rc.risk_desk.bankroll_current = bankroll
    return _s


ADMIT, BLOCK = "AMMESSO", "BLOCCATO"
CASES = [
    # RSR-38 max_order_exposure (Q3, Q4, D2)
    ("RSR-38-A1", "BACK = cap", {"max_open_exposure": 10.0, "max_order_exposure": 1.0}, dict(stake=1.0, bet_type="BACK"), None, ADMIT, "max_order_exposure"),
    ("RSR-38-A2", "BACK cap+0.01", {"max_open_exposure": 10.0, "max_order_exposure": 1.0}, dict(stake=1.01, bet_type="BACK"), None, BLOCK, "max_order_exposure"),
    ("RSR-38-A3", "LAY liability = cap (0.50@3.00)", {"max_open_exposure": 10.0, "max_order_exposure": 1.0}, dict(stake=0.5, price=3.0, bet_type="LAY"), None, ADMIT, "max_order_exposure"),
    ("RSR-38-A4", "LAY liability > cap (0.51@3.00=1.02)", {"max_open_exposure": 10.0, "max_order_exposure": 1.0}, dict(stake=0.51, price=3.0, bet_type="LAY"), None, BLOCK, "max_order_exposure"),
    ("RSR-38-A5", "cap assente (None) => D2 blocco", {"max_open_exposure": 10.0, "max_order_exposure": None}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_order_exposure"),
    ("RSR-38-A6", "cap non valido (NaN) => D2 blocco", {"max_open_exposure": 10.0, "max_order_exposure": float("nan")}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_order_exposure"),
    # RSR-39 max_market_exposure
    ("RSR-39-A1", "esistente 1.50 + 0.50 = cap 2.00", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 100.0}, dict(stake=0.5, bet_type="BACK"), occupy(1.5), ADMIT, "max_market_exposure"),
    ("RSR-39-A2", "esistente 1.50 + 0.51 > cap", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 100.0}, dict(stake=0.51, bet_type="BACK"), occupy(1.5), BLOCK, "max_market_exposure"),
    ("RSR-39-A3", "cap mercato assente => D2 blocco", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": None, "max_event_exposure_abs": 100.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_market_exposure"),
    # RSR-40 max_event_exposure_abs
    ("RSR-40-A1", "esistente 1.50 + 0.50 = cap evento 2.00", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 100.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), occupy(1.5), ADMIT, "max_event_exposure_abs"),
    ("RSR-40-A2", "esistente 1.50 + 0.51 > cap evento", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 100.0, "max_event_exposure_abs": 2.0}, dict(stake=0.51, bet_type="BACK"), occupy(1.5), BLOCK, "max_event_exposure_abs"),
    ("RSR-40-A3", "cap evento assente => D2 blocco", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 100.0, "max_event_exposure_abs": None}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_event_exposure_abs"),
    # RSR-41 max_drawdown_abs (Q3: stop a perdita >= 10,00)
    ("RSR-41-A1", "perdita 9.99 => ammesso", {"max_open_exposure": 10.0, "max_drawdown_abs": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), drawdown(1000.0, 990.01), ADMIT, "drawdown"),
    ("RSR-41-A2", "perdita 10.00 => stop", {"max_open_exposure": 10.0, "max_drawdown_abs": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), drawdown(1000.0, 990.0), BLOCK, "drawdown"),
    ("RSR-41-A3", "perdita 11.00 => stop", {"max_open_exposure": 10.0, "max_drawdown_abs": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), drawdown(1000.0, 989.0), BLOCK, "drawdown"),
    # t80u regola 1 (RSR-02/03/04/15): assente => blocco di ogni nuovo rischio
    ("RSR-02-A1", "max_single_bet_pct assente => blocco", {"max_open_exposure": 10.0, "max_single_bet_pct": None, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_single_bet_pct"),
    ("RSR-03-A1", "max_total_exposure_pct assente => blocco", {"max_open_exposure": 10.0, "max_total_exposure_pct": None, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_total_exposure_pct"),
    ("RSR-04-A1", "max_event_exposure_pct assente => blocco", {"max_open_exposure": 10.0, "max_event_exposure_pct": None, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_event_exposure_pct"),
    ("RSR-15-A1", "max_stake_abs assente => blocco", {"max_open_exposure": 10.0, "max_stake_abs": None, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_stake_abs"),
    # RSR-17 max_open_exposure (Q3 + D2): esposizione aperta totale = cap ammessa, oltre bloccata, assente => blocco
    ("RSR-17-A1", "aperta 9.50 + 0.50 = cap 10", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 100.0, "max_event_exposure_abs": 100.0}, dict(stake=0.5, bet_type="BACK"), occupy(9.5), ADMIT, "max_open_exposure"),
    ("RSR-17-A2", "aperta 9.50 + 0.51 > cap 10", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 100.0, "max_event_exposure_abs": 100.0}, dict(stake=0.51, bet_type="BACK"), occupy(9.5), BLOCK, "max_open_exposure"),
    ("RSR-17-A3", "max_open_exposure assente => D2 blocco", {"max_open_exposure": None, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, BLOCK, "max_open_exposure"),
    # RSR-14 min_stake: nessuno stake inferiore al minimo viene inviato
    ("RSR-14-A1", "min_stake 0.50, stake 0.49 => non inviato", {"max_open_exposure": 10.0, "min_stake": 0.5, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.49, bet_type="BACK"), None, BLOCK, "min"),
    ("RSR-14-A2", "min_stake 0.50, stake 0.50 => inviato", {"max_open_exposure": 10.0, "min_stake": 0.5, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), None, ADMIT, "min"),
    # RSR-18 max_session_loss: raggiunta la perdita di sessione nessun nuovo rischio
    ("RSR-18-A1", "perdita sessione 5.00 = limite 5 => stop", {"max_open_exposure": 10.0, "max_session_loss": 5.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), lambda c: (setattr(c.rc, "_session_pnl_baseline", 0.0), setattr(c.rc.risk_desk, "realized_pnl", -5.0)), BLOCK, "session"),
    ("RSR-18-A2", "perdita sessione 4.99 < limite 5 => ammesso", {"max_open_exposure": 10.0, "max_session_loss": 5.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0}, dict(stake=0.5, bet_type="BACK"), lambda c: (setattr(c.rc, "_session_pnl_baseline", 0.0), setattr(c.rc.risk_desk, "realized_pnl", -4.99)), ADMIT, "session"),
    # controllo: configurazione completa e valida => ammesso (evita falsi CONFORME per blocco generico)
    ("CTRL-A1", "config completa valida, 0.50 BACK => ammesso", {"max_open_exposure": 10.0, "max_order_exposure": 1.0, "max_market_exposure": 2.0, "max_event_exposure_abs": 2.0, "max_drawdown_abs": 10.0}, dict(stake=0.5, bet_type="BACK"), None, ADMIT, ""),
]


def verdict(expected, obs, key):
    if obs["exception"]:
        return "ERRORE_SONDA"
    admitted = obs["orders"] == 1 and not obs["rejects"]
    blocked = obs["orders"] == 0 and bool(obs["rejects"])
    if expected == ADMIT:
        return "CONFORME" if admitted else "NON_CONFORME"
    if not blocked:
        return "NON_CONFORME"
    # il motivo deve nominare la regola che blocca (non un blocco generico per altra causa)
    return "CONFORME" if any(key in str(r) for r in obs["rejects"]) else "BLOCCATO_PER_ALTRO_MOTIVO"


out = {}
for cid, desc, cfg, sig, setup, exp, key in CASES:
    obs = run({k: v for k, v in cfg.items()}, sig, setup)
    out[cid] = {"descrizione": desc, "config": {k: (str(v) if v != v else v) for k, v in cfg.items()},
                "segnale": sig, "atteso": exp, "chiave_motivo": key, "osservato": obs,
                "verdetto": verdict(exp, obs, key)}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
from collections import Counter  # noqa: E402
print(Counter(v["verdetto"] for v in out.values()))


# ---------------------------------------------------------------------------
# Casi "loader reale": la chiave assente/corrotta passa dal vero SettingsService (absent_keys/corrupt_keys).
from database import Database  # noqa: E402
from services.setting_service import SettingsService  # noqa: E402

VALID = {
    "roserpina.max_single_bet_pct": "50", "roserpina.max_total_exposure_pct": "50",
    "roserpina.max_event_exposure_pct": "50", "roserpina.max_stake_abs": "5",
    "roserpina.max_order_exposure": "1", "roserpina.max_market_exposure": "2",
    "roserpina.max_event_exposure_abs": "2", "roserpina.max_drawdown_abs": "10",
    "roserpina.max_recovery_tables": "2", "roserpina.allow_recovery": "false",
    "roserpina.commission_pct": "4.5", "roserpina.max_open_exposure": "10",
}


def run_loader(key, mode):
    p = os.path.join(tempfile.mkdtemp(), "p.sqlite")
    data = dict(VALID)
    if mode == "assente":
        data.pop(key)
    else:
        data[key] = "abc"
    Database(p).save_settings(data)
    cfg = SettingsService(Database(p)).load_roserpina_config()
    c = _catena(Path(tempfile.mkdtemp()))
    c.rc.config = cfg
    try:
        c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
        err = None
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
    return {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c), "exception": err,
            "absent_keys": list(getattr(cfg, "absent_keys", ()) or ()),
            "corrupt_keys": list(getattr(cfg, "corrupt_keys", ()) or ()),
            "valore_caricato": repr(getattr(cfg, key.split(".", 1)[1], "<assente>"))}


LOADER = [("RSR-38", "roserpina.max_order_exposure"), ("RSR-39", "roserpina.max_market_exposure"),
          ("RSR-40", "roserpina.max_event_exposure_abs"), ("RSR-15", "roserpina.max_stake_abs"),
          ("RSR-02", "roserpina.max_single_bet_pct")]
for rid, key in LOADER:
    for mode in ("assente", "corrotto"):
        obs = run_loader(key, mode)
        blocked = obs["orders"] == 0 and bool(obs["rejects"]) and not obs["exception"]
        out[f"{rid}-L-{mode}"] = {"descrizione": f"{key} {mode} via SettingsService reale => blocco nuovo rischio (D2/t80u)",
                                  "atteso": BLOCK, "osservato": obs,
                                  "verdetto": "ERRORE_SONDA" if obs["exception"] else ("CONFORME" if blocked else "NON_CONFORME")}
# RSR-41 (D1 #426/6141902809): drawdown assente => soglia effettiva 10,00 EUR (tetto vincolante), non blocco totale.
for mode, (peak, bank, exp) in {"assente_perdita_0": (1000.0, 1000.0, ADMIT), "assente_perdita_10": (1000.0, 990.0, BLOCK),
                                "assente_perdita_9.99": (1000.0, 990.01, ADMIT)}.items():
    p = os.path.join(tempfile.mkdtemp(), "p.sqlite"); d = dict(VALID); d.pop("roserpina.max_drawdown_abs")
    Database(p).save_settings(d); cfg = SettingsService(Database(p)).load_roserpina_config()
    c = _catena(Path(tempfile.mkdtemp())); c.rc.config = cfg
    c.rc.risk_desk.equity_peak = peak; c.rc.risk_desk.bankroll_current = bank
    c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
    o = {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c)}
    ok = (o["orders"] == 1 and not o["rejects"]) if exp == ADMIT else (o["orders"] == 0 and bool(o["rejects"]))
    out[f"RSR-41-L-{mode}"] = {"descrizione": f"max_drawdown_abs assente, {mode} (D1: tetto 10 EUR)", "atteso": exp,
                               "osservato": o, "verdetto": "CONFORME" if ok else "NON_CONFORME"}
# controllo: tutte le chiavi valide => ammesso
p = os.path.join(tempfile.mkdtemp(), "p.sqlite"); Database(p).save_settings(dict(VALID))
cfg = SettingsService(Database(p)).load_roserpina_config(); c = _catena(Path(tempfile.mkdtemp())); c.rc.config = cfg
c.rc._on_signal_received(_signal(stake=0.5, bet_type="BACK"))
out["CTRL-L"] = {"descrizione": "tutte le chiavi valide via loader => ammesso", "atteso": ADMIT,
                 "osservato": {"orders": len(c.broker.state.orders), "rejects": _rifiuti(c)},
                 "verdetto": "CONFORME" if len(c.broker.state.orders) == 1 and not _rifiuti(c) else "NON_CONFORME"}
json.dump(out, open(os.environ["PROBE_OUT"], "w"), indent=1, ensure_ascii=False)
print("loader:", Counter(v["verdetto"] for k, v in out.items() if "-L" in k))
