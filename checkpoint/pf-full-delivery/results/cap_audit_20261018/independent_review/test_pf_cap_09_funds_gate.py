"""PF-CAP-09 — prova indipendente (throwaway, fuori dal repo).

Domanda: sul percorso diretto (>= 2 EUR) esiste un confronto esplicito
stake/liability <= fondi disponibili?  Se NO, con percentuali utente permissive
(max_single_bet_pct=150, max_total_exposure_pct=200, max_event_exposure_pct=200)
un ordine da 150 EUR con 100 EUR disponibili viene APPROVATO dal MM, PASSA il
RiskGate e viene PIAZZATO dal broker SIM.

Parte A: catena unitaria reale (MM -> RiskGate -> SimulationBroker), nessun mock.
Parte B: HeadlessApp reale (SIM, SQLite su disco) via child_cap09.py.
"""
import json
import os
import subprocess
import sys
import tempfile

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
TREE = "/home/user/Pickfair-nogui"
sys.path.insert(0, TREE)

from core.money_management import RoserpinaMoneyManagement  # noqa: E402
from core.risk_gate import RiskGate  # noqa: E402
from core.system_state import RoserpinaConfig  # noqa: E402
from simulation_broker import SimulationBroker  # noqa: E402
import trading_config  # noqa: E402

PERMISSIVE = dict(max_single_bet_pct=150.0, max_total_exposure_pct=200.0, max_event_exposure_pct=200.0)


def _book(broker, market_id, price):
    broker.update_market_book({"marketId": market_id, "status": "OPEN", "inplay": False, "runners": [
        {"selectionId": s, "status": "ACTIVE",
         "ex": {"availableToBack": [{"price": price, "size": 1000.0}],
                "availableToLay": [{"price": price, "size": 1000.0}]}} for s in (11, 12)]})


@pytest.mark.parametrize("side,price,stake,expected_risk", [
    ("BACK", 2.0, 150.0, 150.0),    # stake 150 > available 100
    ("LAY", 11.0, 15.0, 150.0),     # liability 15*(11-1)=150 > available 100
])
def test_unit_chain_order_above_available_funds_is_approved_gated_and_placed(side, price, stake, expected_risk):
    available = 100.0
    mm = RoserpinaMoneyManagement(RoserpinaConfig(**PERMISSIVE))
    decision = mm.calculate(
        signal={"market_id": "1.701", "selection_id": 11, "price": price, "bet_type": side, "stake": stake},
        bankroll_current=available, equity_peak=available,
        current_total_exposure=0.0, event_current_exposure=0.0, table={"table_id": 1},
    )
    assert decision.approved is True, decision
    assert decision.recommended_stake == stake
    assert decision.metadata["exposure"] == pytest.approx(expected_risk)
    assert expected_risk > available  # il rischio supera il disponibile e il MM non lo vede

    gate = RiskGate(config=trading_config)  # stessi limiti di libreria del loader (MIN_STAKE/MAX_WIN/...)
    verdict = gate.check({"market_id": "1.701", "selection_id": 11, "bet_type": side, "price": price,
                          "stake": stake, "available_liquidity": 1000.0})
    assert verdict["allowed"] is True, verdict  # nessuna lettura dei fondi nel gate

    broker = SimulationBroker(starting_balance=available, commission_pct=4.5)
    _book(broker, "1.701", price)
    raw = broker.place_bet(market_id="1.701", selection_id=11, side=side, price=price, size=stake,
                           customer_ref="cap09")
    assert raw["status"] == "SUCCESS" and raw["instructionReports"][0]["status"] == "SUCCESS", raw
    funds = broker.get_account_funds()
    # ordine accettato: il broker SIM non solleva INSUFFICIENT_FUNDS e l'esposizione supera il saldo iniziale
    assert funds["exposure"] >= expected_risk - 1e-9 or funds["available"] < 0.0, funds


def _run_child(steps, settings):
    home = tempfile.mkdtemp(prefix="cap09_")
    out = os.path.join(tempfile.mkdtemp(prefix="cap09o_"), "o.json")
    env = dict(os.environ)
    if os.environ.get("PF_NETGUARD"):
        env["PYTHONPATH"] = os.environ["PF_NETGUARD"]
    subprocess.run([sys.executable, os.path.join(HERE, "child_cap09.py"), TREE, out,
                    json.dumps({"steps": steps, "settings": settings}), home],
                   env=env, capture_output=True, text=True, timeout=300)
    r = json.load(open(out))
    assert "error" not in r, r.get("error")
    return r


def test_real_app_sim_order_150_eur_with_100_eur_available_is_submitted():
    settings = {"roserpina.lockdown_drawdown_pct": 60.0, **{f"roserpina.{k}": v for k, v in PERMISSIVE.items()}}
    r = _run_child([{"op": "signal", "m": "1.701", "price": 2.0, "side": "BACK", "stake": 150.0},
                    {"op": "wait", "s": 2}], settings)
    final = r["snaps"][-1]
    orders = [o for o in final["orders"] if o["market_id"] == "1.701"]
    rejected = [e for e in r["events"] if e["e"] == "SIGNAL_REJECTED"]
    assert r["snaps"][0]["funds"]["available"] == 100.0, r["snaps"][0]["funds"]
    assert not rejected, rejected
    assert orders and orders[0]["stake"] == 150.0, (orders, r["events"])
    assert orders[0]["status"] not in ("REJECTED", "FAILED", "BLOCKED"), orders
    # il MM ha dimensionato contro bankroll_current=100 (disponibile) e ha approvato 150
    assert r["mm_calls"] and r["mm_calls"][0]["bankroll_current"] == 100.0, r["mm_calls"]
    print("\nCAP09 final funds:", final["funds"], "orders:", orders, "events:", [e["e"] for e in r["events"]])
