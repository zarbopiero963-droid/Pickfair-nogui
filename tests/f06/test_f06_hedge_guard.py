"""F-06 (#586, CSH-05, MM-02, NF-01) — l'hedge del cashout deve essere coerente con la posizione (RED prima della patch).

Prima: `CashoutExecutor` piazzava qualunque lato/size arrivasse nel comando (un hedge BACK su una posizione BACK la
RADDOPPIA; un LAY 50 su una posizione da 0.23 apre una nuova esposizione), e `RiskMiddleware` defaultava `side="LAY"`
quando mancava (l'auto-close del monitor SL/TP manda il comando senza lato). Atteso (Roserpina unica autorita',
PF-EXEC-03 chiusura protettiva senza inseguimento, D-582-03): lato = opposto della posizione, stake <= stake di
chiusura dell'esposizione ABBINATA sull'exchange (nessun cap di stake, solo coerenza: `RiskGate.protective_hedge`);
altrimenti `CASHOUT_FAILED` `REJECTED` `hedge_non_coerente:*`; lato mancante => rifiuto, mai LAY di default.
"""
from __future__ import annotations

import pytest

from cashout_executor import CASHOUT_FAILED, CASHOUT_SUCCESS, CashoutExecutor
from core.risk_gate import RiskGate

pytestmark = [pytest.mark.safety]


class _Bus:
    def __init__(self):
        self.published = []

    def subscribe(self, *_a):
        pass

    def publish(self, topic, payload):
        self.published.append((topic, payload))

    def last(self, topic):
        return next((p for t, p in reversed(self.published) if t == topic), None)


class _Router:
    def __init__(self):
        self.calls = []

    def place(self, payload):
        self.calls.append(payload)
        return {"ok": True, "placed": True, "order_unknown": False, "status": "SUCCESS",
                "matched": payload["stake"], "bet_id": "H1", "error": None}


def _order(side="BACK", matched=0.23, price=2.0, selection=55, ref="sig-entry", market="1.222"):
    return {"betId": "B1", "marketId": market, "selectionId": selection, "side": side, "sizeMatched": matched,
            "averagePriceMatched": price, "priceSize": {"price": price, "size": matched}, "customerOrderRef": ref}


def _cmd(side="LAY", stake=0.26, price=1.8, orig_side="BACK", orig_stake=0.23, orig_price=2.0, **over):
    p = {"market_id": "1.222", "selection_id": 55, "side": side, "stake": stake, "price": price, "green_up": 0.03,
         "original_pos": {"side": orig_side, "stake": orig_stake, "price": orig_price}, "source": "UI"}
    p.update(over)
    return p


def _run(cmd, orders=None, fetch=True):
    bus, router = _Bus(), _Router()
    kw = {}
    if fetch:
        kw["fetch_current_orders"] = (lambda: orders) if not callable(orders) else orders
    CashoutExecutor(bus, router, **kw).on_cmd_execute_cashout(cmd)
    return bus, router


def test_hedge_wrong_side_rejected():
    # D-582-03: hedge BACK 0.23 su una posizione BACK la raddoppia invece di chiuderla
    bus, router = _run(_cmd(side="BACK", stake=0.23), orders=[_order()])
    assert router.calls == [], "hedge dello stesso lato della posizione piazzato"
    failed = bus.last(CASHOUT_FAILED)
    assert failed["status"] == "REJECTED" and failed["reason"] == "hedge_non_coerente:lato"


def test_hedge_oversized_rejected():
    # D-582-03: LAY 50 su una posizione BACK da 0.23 (chiusura massima 0.23*2.0/1.8 = 0.26) apre nuova esposizione
    bus, router = _run(_cmd(stake=50.0), orders=[_order()])
    assert router.calls == [], "hedge oltre l'esposizione abbinata piazzato"
    assert bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_coherent_hedge_is_placed():
    bus, router = _run(_cmd(), orders=[_order()])
    assert len(router.calls) == 1 and router.calls[0]["bet_type"] == "LAY"
    assert bus.last(CASHOUT_SUCCESS) is not None


def test_size_is_measured_on_the_exchange_not_on_the_payload():
    # il payload dichiara una posizione da 30, l'exchange ne ha abbinata 0.23: vale l'exchange
    bus, router = _run(_cmd(stake=33.0, orig_stake=30.0), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_previous_hedges_and_other_selections_do_not_count_as_position():
    from core.hedge_ref import new_hedge_ref
    orders = [_order(side="BACK", matched=40.0, ref=new_hedge_ref()), _order(matched=40.0, selection=99),
              _order(matched=40.0, market="1.999"), _order(side="LAY", matched=40.0)]
    bus, router = _run(_cmd(stake=5.0), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_assente"


@pytest.mark.parametrize("orders", [None, "x", [1], [{"betId": "B1", "marketId": "1.222"}],
                                    [dict(_order(), sizeMatched="NaN")], [dict(_order(), sizeMatched=float("inf"))]])
def test_unreadable_exchange_position_never_places(orders):
    bus, router = _run(_cmd(), orders=orders)
    assert router.calls == []
    assert bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")


def test_exchange_read_error_never_places():
    def boom():
        raise RuntimeError("down")
    bus, router = _run(_cmd(), orders=boom)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:posizione_non_verificabile"


@pytest.mark.parametrize("orig", [None, {}, {"side": "X", "stake": 1, "price": 2}, "BACK"])
def test_missing_or_invalid_original_position_rejected(orig):
    bus, router = _run(_cmd(original_pos=orig), orders=[_order()])
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")


def test_without_exchange_source_the_payload_position_still_bounds_the_hedge():
    # doppi di test senza fetch: la coerenza si verifica comunque sulla posizione del comando, mai saltata
    bus, router = _run(_cmd(stake=50.0), fetch=False)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"
    bus, router = _run(_cmd(), fetch=False)
    assert len(router.calls) == 1


@pytest.mark.parametrize("pos_side,hedge_side,stake,ok,reason", [
    ("BACK", "LAY", 0.26, True, None),
    ("LAY", "BACK", 0.26, True, None),
    ("BACK", "BACK", 0.1, False, "hedge_non_coerente:lato"),
    ("LAY", "LAY", 0.1, False, "hedge_non_coerente:lato"),
    ("BACK", "LAY", 0.27, False, "hedge_non_coerente:size"),     # 0.46/1.8 = 0.2556 => max 0.26 al centesimo
    ("BACK", "LAY", 0.0, False, "hedge_non_coerente:size"),
    ("BACK", "LAY", float("nan"), False, "hedge_non_coerente:size"),
])
def test_risk_gate_protective_hedge_verdict(pos_side, hedge_side, stake, ok, reason):
    v = RiskGate.protective_hedge(position_side=pos_side, hedge_side=hedge_side, hedge_stake=stake,
                                  hedge_price=1.8, matched_notional=0.46)
    assert v["allowed"] is ok and v["reason"] == reason


def test_middleware_missing_side_rejected():
    # NF-01: l'auto-close del monitor manda REQ_EXECUTE_CASHOUT senza lato; prima diventava un LAY di default
    from core.risk_middleware import RiskMiddleware
    bus = _Bus()
    mw = RiskMiddleware.__new__(RiskMiddleware)
    mw.bus = bus
    mw._is_duplicate = lambda _p: False
    mw._handle_cashout({"market_id": "1.222", "selection_id": 55, "stake": 3.0, "reason": "STOP_LOSS_REACHED",
                        "source": "AUTO_MONITOR"})
    assert not [t for t, _ in bus.published if t == "CMD_EXECUTE_CASHOUT"], "hedge con lato inventato inoltrato"
    assert bus.last("CASHOUT_FAILED") is not None


@pytest.mark.parametrize("side", ["", "X", None, 1, "BACKLAY"])
def test_middleware_invalid_side_rejected(side):
    from core.risk_middleware import RiskMiddleware
    bus = _Bus()
    mw = RiskMiddleware.__new__(RiskMiddleware)
    mw.bus = bus
    mw._is_duplicate = lambda _p: False
    mw._handle_cashout({"market_id": "1.222", "selection_id": 55, "side": side, "stake": 3.0, "price": 1.8})
    assert not [t for t, _ in bus.published if t == "CMD_EXECUTE_CASHOUT"]
    assert bus.last("CASHOUT_FAILED") is not None


def test_matched_size_uses_the_average_matched_price_not_the_requested_price():
    # ingresso richiesto a 5.0 ma abbinato in media a 2.0: l'esposizione e' 0.23*2.0 = 0.46, la chiusura 0.26
    orders = [dict(_order(), averagePriceMatched=2.0, priceSize={"price": 5.0, "size": 0.23})]
    bus, router = _run(_cmd(stake=0.5), orders=orders)
    assert router.calls == [] and bus.last(CASHOUT_FAILED)["reason"] == "hedge_non_coerente:size"


def test_hedge_coherence_is_enforced_on_the_ledger_path_of_the_real_wiring(tmp_path):
    # wiring reale (cashout_wiring): executor con ledger `orders` => il controllo gira dentro il preflight del ledger
    from database import Database
    db = Database(str(tmp_path / "f06.db"))
    for cmd, placed in ((_cmd(side="BACK", stake=0.23, cashout_request_id="r1"), False),
                        (_cmd(stake=50.0, cashout_request_id="r2"), False),
                        (_cmd(cashout_request_id="r3"), True)):
        bus, router = _Bus(), _Router()
        CashoutExecutor(bus, router, ledger=db, fetch_current_orders=lambda: [_order()]).on_cmd_execute_cashout(cmd)
        assert bool(router.calls) is placed, cmd
        if not placed:
            assert bus.last(CASHOUT_FAILED)["reason"].startswith("hedge_non_coerente:")
    rows = db._execute("SELECT status FROM orders", fetch=True, commit=False) or []
    assert len(rows) == 1, "un hedge incoerente non deve lasciare righe nel ledger"
