"""Gate A — T2b batch b2, componente dutching (controllers/dutching_controller.DutchingController).

Controller reale con calculate_dutching reale; runtime, bus, tavoli e anti-duplicato sono controfigure in memoria
(nessuna rete, nessun broker): si osservano validazione, anteprima e pubblicazione dei CMD_QUICK_BET.
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from controllers.dutching_controller import DutchingController
from core.order_identity import derive_customer_ref


class _Bus:
    def __init__(self, fail_on_quick_bet: int = 0) -> None:
        self.events: List[tuple] = []
        self._fail_on = fail_on_quick_bet
        self._quick = 0

    def publish(self, name, payload=None):
        if name == "CMD_QUICK_BET":
            self._quick += 1
            if self._fail_on and self._quick == self._fail_on:
                raise RuntimeError("bus giu'")
        self.events.append((name, payload))

    def quick_bets(self) -> List[Dict[str, Any]]:
        return [p for n, p in self.events if n == "CMD_QUICK_BET"]


class _Guard:
    def __init__(self) -> None:
        self.keys = set()
        self.acquired: List[str] = []

    def is_duplicate(self, key):
        return key in self.keys

    def acquire(self, key, pin=False):
        self.acquired.append(key)
        if key in self.keys:
            return False
        self.keys.add(key)
        return True

    def release(self, key):
        self.keys.discard(key)


class _Tables:
    def __init__(self) -> None:
        self.allocated = 0
        self.unlocked: List[int] = []

    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        return None

    def allocate(self, event_key=None, allow_recovery=True):
        self.allocated += 1
        return SimpleNamespace(table_id=7)

    def activate(self, **kwargs):
        return None

    def force_unlock(self, table_id):
        self.unlocked.append(table_id)


class _Runtime:
    def __init__(self) -> None:
        self.mode = SimpleNamespace(value="ACTIVE")
        self.config = SimpleNamespace(anti_duplication_enabled=True, allow_recovery=True,
                                      max_total_exposure_pct=100.0, max_event_exposure_pct=100.0,
                                      target_profit_cycle_pct=3.0, max_single_bet_pct=100.0)
        self.risk_desk = SimpleNamespace(bankroll_current=1000.0, cash_equity_current=1000.0,
                                         cash_committed_exposure=0.0)
        self.duplication_guard = _Guard()
        self.table_manager = _Tables()
        self.dutching_batch_manager = None

    def read_funds_for_controller_entry(self):
        return {"funds": {"available": 1e9, "exposure": 0.0}, "read_at": None}

    def controller_entry_funds_reason(self, funds_read, *, extra_exposure=0.0):
        return ""

    def admit_controller_entries(self, orders, *, batch_id=""):
        return ""

    def withdraw_controller_entries(self, orders):
        return None

    def controller_event_exposure(self, entry, *, event_key=""):
        return 0.0


def _payload(selections=None, **extra) -> Dict[str, Any]:
    base = {
        "market_id": "1.700", "event_name": "Gamma v Delta", "market_name": "Match Odds",
        "total_stake": 30.0, "simulation_mode": True,
        "selections": selections if selections is not None else [
            {"selectionId": 10, "price": 2.0, "side": "BACK"},
            {"selectionId": 20, "price": 4.0, "side": "BACK"},
        ],
    }
    base.update(extra)
    return base


def _controller(bus=None, runtime=None):
    return DutchingController(bus=bus if bus is not None else _Bus(),
                              runtime_controller=runtime if runtime is not None else _Runtime())


# ---------------------------------------------------------------------------
@pytest.mark.parametrize("dup", ["10", " 10 ", 10.0])
def test_trg_628_b_0876(dup):
    """ID: TRG-628-B-0876 / PF-B1-DUTCH-DUPLICATE

    selectionId duplicati DOPO la conversione intera ("10", " 10 ", 10.0 contro 10) rifiutano l'intero lotto in
    validazione; controprova: selectionId distinti passano."""
    c = _controller()
    res = c.validate(_payload([{"selectionId": 10, "price": 2.0, "side": "BACK"},
                               {"selectionId": dup, "price": 3.0, "side": "BACK"}]))
    assert res["ok"] is False
    assert res["error"] == "selectionId duplicato: 10"
    assert c.validate(_payload())["ok"] is True


# ---------------------------------------------------------------------------
def test_trg_628_b_0877():
    """ID: TRG-628-B-0877 / PF-B1-DUTCH-PRICE-FLOOR

    price <= 1.01 rifiutato sempre (floor Betfair, indipendente dalla config); price sotto il minimo configurato
    rifiutato; quota al minimo configurato ammessa."""
    def _v(price, min_price):
        c = DutchingController(bus=None, runtime_controller=SimpleNamespace(config=SimpleNamespace(min_price=min_price)))
        return c.validate(_payload([{"selectionId": 1, "price": price, "side": "BACK"}]))

    hard = _v(1.01, 1.01)
    assert hard["ok"] is False and "Quota non valida" in hard["error"]
    below = _v(1.4, 1.5)
    assert below["ok"] is False and "sotto il minimo configurato" in below["error"]
    assert _v(1.5, 1.5)["ok"] is True
    assert _v(1.02, 1.02)["ok"] is True


# ---------------------------------------------------------------------------
def test_trg_628_b_0878():
    """ID: TRG-628-B-0878 / PF-B1-DUTCH-PREVIEW

    preview() restituisce il calcolo (stake per gamba, profitto lordo e netto di commissione, book%, esposizione del
    lotto) e NON pubblica ordini ne' acquisisce anti-duplicato o tavoli. Esempio numerico: 30 EUR su quote 2.0/4.0
    => stake 20/10, lordo +10 per esito, esposizione 30."""
    bus = _Bus()
    rt = _Runtime()
    res = _controller(bus, rt).preview(_payload(commission=4.5))

    assert res["ok"] is True and res["dry_run"] is True
    stakes = {r["selectionId"]: r["stake"] for r in res["results"]}
    assert stakes == {10: 20.0, 20: 10.0}
    assert res["avg_profit"] == pytest.approx(10.0)
    assert res["avg_profit_semantics"] == "gross"
    assert res["avg_profit_net"] == pytest.approx(9.55)      # 10 - 4.5% commissione (policy Betfair Italia)
    assert res["book_pct"] == pytest.approx(75.0)
    assert res["batch_exposure"] == pytest.approx(30.0)
    assert res["commission_pct"] == 4.5
    assert res["profitable_net"] is True
    assert bus.events == [], "l'anteprima ha pubblicato eventi"
    assert rt.duplication_guard.acquired == [] and rt.duplication_guard.keys == set()
    assert rt.table_manager.allocated == 0


# ---------------------------------------------------------------------------
def test_trg_628_b_0879():
    """ID: TRG-628-B-0879 / PF-B1-DUTCH-LEG-IDENTITY

    customer_ref di ogni gamba = derive_customer_ref("dut", {batch: identita' ordinata del lotto, leg: chiave
    gamba}); stesso lotto con selezioni permutate => stessi ref per gamba; gambe distinte => ref distinti;
    stake totale diverso => ref nuovi."""
    bus = _Bus()
    res = _controller(bus).submit_dutching(_payload())
    assert res["ok"] is True, res
    refs = {o["selection_id"]: o["customer_ref"] for o in bus.quick_bets()}
    assert len(set(refs.values())) == 2

    identity = {"market_id": "1.700", "event_name": "Gamma v Delta", "market_name": "Match Odds",
                "simulation_mode": True, "total_stake": 30.0,
                "legs": sorted([(10, "BACK", 2.0), (20, "BACK", 4.0)])}
    assert refs[10] == derive_customer_ref("dut", {"batch": identity, "leg": [10, "BACK", 2.0]})
    assert refs[20] == derive_customer_ref("dut", {"batch": identity, "leg": [20, "BACK", 4.0]})

    bus2 = _Bus()
    perm = _payload(list(reversed(_payload()["selections"])))
    _controller(bus2).submit_dutching(perm)
    assert {o["selection_id"]: o["customer_ref"] for o in bus2.quick_bets()} == refs

    bus3 = _Bus()
    _controller(bus3).submit_dutching(_payload(total_stake=40.0))
    assert not set(o["customer_ref"] for o in bus3.quick_bets()) & set(refs.values())


# ---------------------------------------------------------------------------
def test_trg_628_b_0880():
    """ID: TRG-628-B-0880 / PF-B1-DUTCH-SUBMITTED

    status SUBMITTED solo dopo la pubblicazione di TUTTI i CMD_QUICK_BET del lotto (published_count == count).
    Controprova: publish fallito alla seconda gamba => nessun SUBMITTED, published_count 1, ok False."""
    bus = _Bus()
    res = _controller(bus).submit_dutching(_payload())
    assert res["ok"] is True and res["status"] == "SUBMITTED"
    assert res["published_count"] == res["count"] == 2
    assert len(bus.quick_bets()) == 2

    bus_ko = _Bus(fail_on_quick_bet=2)
    ko = _controller(bus_ko).submit_dutching(_payload())
    assert ko["ok"] is False
    assert ko.get("status") != "SUBMITTED"
    assert ko["published_count"] == 1 and ko["total_count"] == 2
