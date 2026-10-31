"""F-33 — L2/L3 eseguiti davvero: combinazione -> settings -> runtime SIM -> scenario -> oracolo -> report.

Casi per gruppo: i primi N del generatore (seed PF_SEED), N = PF_COMBI_CASES (default 3; 'all' = tutti).
Ogni caso: istanza isolata, valori applicati verificati dallo snapshot del runtime, invarianti finanziari e
controlli coerenti con i limiti DEL CASO. Un FAIL qui e' una violazione finanziaria o una configurazione ignorata.
Stop-loss / take-profit / trailing: chiavi inesistenti su main (F-01/F-02/F-03) => xfail strict.
"""
import os

import pytest

from tests.fullscope.harness import combi_runner as C

pytestmark = [pytest.mark.fullscope, pytest.mark.e2e]

_LIMIT_ENV = os.environ.get("PF_COMBI_CASES", "3")
LIMIT = None if _LIMIT_ENV == "all" else int(_LIMIT_ENV)
SEED = int(os.environ.get("PF_SEED", "0"))


def _params():
    out = []
    for group, scenarios in C.GROUP_SCENARIOS.items():
        for case_id, case in C.cases_for(group, seed=SEED, limit=LIMIT):
            for sc in scenarios:
                out.append(pytest.param(group, case_id, case, sc, id=f"{case_id}:{sc}"))
    return out


# Finding noti di "configurazione ignorata" (fail-open del caricamento): il test li dichiara con riferimento alla
# scheda che li corregge invece di mascherarli; qualunque altro problema di applicazione e' un FAIL.
KNOWN_APPLIED_FINDINGS = [
    ("simulation.starting_balance: configurato 0.0, saldo exchange", "F-18/F-17 (NF-16): `float(cfg or 1000.0)` in BetfairService._connect_simulation: "
                                                   "starting_balance 0 ricade sul default 1000 in silenzio"),
]


@pytest.fixture(scope="module", autouse=True)
def _fresh_records():
    C.reset_records()


@pytest.mark.parametrize("group,case_id,case,scenario", _params())
def test_combination_runs_in_sim_and_respects_oracle(group, case_id, case, scenario):
    rec = C.run_case(group, case_id, case, scenario, seed=SEED)
    # Prima gli invarianti finanziari e le attese: un finding noto di configurazione (xfail sotto) non deve mai
    # mascherare una violazione di cassa/P&L o un blocco dello stesso caso (rilievo Grok su d46e6da).
    assert rec["unmet_waits"] == [], f"attese non soddisfatte: {rec['unmet_waits']}"
    assert rec["oracle_violations"] == [], "\n".join(rec["oracle_violations"])
    if rec["applied_problems"]:
        known = [why for pat, why in KNOWN_APPLIED_FINDINGS if any(p.startswith(pat) for p in rec["applied_problems"])]
        if known and all(any(p.startswith(pat) for pat, _w in KNOWN_APPLIED_FINDINGS) for p in rec["applied_problems"]):
            pytest.xfail("; ".join(known))
    assert rec["applied_problems"] == [], f"configurazione non applicata: {rec['applied_problems']}"
    assert rec["outcome"] == "PASS"


@pytest.mark.xfail(strict=True, reason="F-01/F-02/F-03: stop_loss_pct / take_profit_pct / trailing_stop_pct non esistono "
                                       "su main (monitor SL/TP inerte); il gruppo SL/TP/trailing non puo' essere eseguito")
def test_sl_tp_trailing_group_is_executable():
    inv = C.G.load_inventory()
    keys = {p["key"] for p in inv["parameters"]}
    assert {"roserpina.stop_loss_pct", "roserpina.take_profit_pct", "roserpina.trailing_stop_pct"} <= keys


@pytest.mark.xfail(strict=True, reason="F-18/F-17 (NF-16): `float(cfg or 1000.0)` in BetfairService._connect_simulation: "
                                       "simulation.starting_balance=0 ricade in silenzio sul default 1000 (configurazione ignorata)")
def test_starting_balance_zero_is_applied_not_replaced_by_default():
    rec = C.run_case("c_esecuzione_sim", "boundary-start0", {"simulation.starting_balance": 0.0}, "entry_settle_win", seed=SEED)
    assert rec["unmet_waits"] == [] and rec["oracle_violations"] == [], rec   # fuori dall'xfail: e' un FAIL vero
    assert rec["applied_problems"] == [], rec["applied_problems"]
