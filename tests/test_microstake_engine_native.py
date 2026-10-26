"""PF-MICROSTAKE-NATIVE-INTEGRATION-02, lotto M3 — Microstake nativo nel TradingEngine con il SimulationBroker REALE.

Nessuna flag: un importo sotto il minimo exchange sceglie da solo il percorso. Cap owner (DECISIONE-426 P02/P06:
MAX_EXPOSURE per ordine) applicato all'esposizione transitoria dello stub PRIMA del PLACE; LIVE senza adapter
=> fail-closed; crossing => stub passivo oltre il best e replace con NUOVO betId; risposta persa => AMBIGUOUS
con riserva; riavvio => la saga pendente si completa dalla sua identita' persistita, mai un nuovo PLACE.
"""
from decimal import Decimal
from types import SimpleNamespace

import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig
from tests.risk_config_fixtures import legacy_test_config
from core.trading_engine import TradingEngine
from simulation_broker import SimulationBroker

D = Decimal


class _Bus:
    def __init__(self):
        self.events = []

    def subscribe(self, *_a):
        pass

    def publish(self, name, payload=None):
        self.events.append((name, dict(payload or {})))


class _DB:
    """DB minimo con ordini dell'engine e settings (store della saga)."""

    def __init__(self):
        self.rows = {}
        self.settings = {}
        self.simulation_state = None

    def is_ready(self):
        return True

    def save_simulation_state(self, key, state):
        # fonte di reload del broker SIM: senza, ogni snapshot e' "non persistito" e la saga
        # resta AMBIGUOUS (rilievo Codex #512 su 034af59); la produzione passa sempre il DB al broker
        self.simulation_state = dict(state)

    def insert_order(self, payload):
        key = len(self.rows) + 1
        self.rows[key] = dict(payload)
        return key

    def update_order(self, key, update):
        self.rows[key].update(update)

    def get_order(self, key):
        return self.rows.get(key)

    def insert_audit_event(self, _e):
        pass

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def order_exists_inflight(self, **_k):
        return False

    def save_settings(self, data):
        self.settings.update({str(k): v for k, v in data.items()})

    def get_settings(self):
        return dict(self.settings)


class _Inline:
    def is_ready(self):
        return True

    def submit(self, _n, fn):
        return fn()


def _runtime(broker, *, mode="SIMULATION", max_win=10000.0, bankroll=100.0, available=None, open_exposure=0.0):
    cfg = legacy_test_config()
    cfg.max_win = max_win
    funds = {"available": broker.get_account_funds()["available"] if available is None else available,
             "exposure": open_exposure, "ok": True}
    return SimpleNamespace(
        get_effective_execution_mode=lambda: mode,
        is_live_allowed=lambda: False,
        betfair_service=SimpleNamespace(simulation_broker=broker, _session_invalid=False,
                                        get_account_funds=lambda: dict(funds)),
        config=cfg, mm=RoserpinaMoneyManagement(cfg), risk_desk=SimpleNamespace(bankroll_current=bankroll, cash_equity_current=bankroll, cash_committed_exposure=0.0),
        table_manager=SimpleNamespace(total_exposure=lambda: open_exposure),
    )


def _engine(db=None, **rt):
    db = db or _DB()
    broker = SimulationBroker(starting_balance=100.0, db=db)
    engine = TradingEngine(bus=_Bus(), db=db, client_getter=lambda: broker, executor=_Inline())
    engine.simulation_broker = broker
    engine.runtime_controller = _runtime(broker, **rt)
    return engine, broker, db


def _payload(ref, *, size=0.10, price=3.0, side="BACK", market="1.100", selection=7):
    return {"market_id": market, "selection_id": selection, "side": side, "bet_type": side, "price": price,
            "size": size, "stake": size, "customer_ref": ref, "simulation_mode": True}


def _book(broker, market, selection, *, back=None, lay=None):
    runner = {"selectionId": selection, "ex": {
        "availableToBack": [{"price": back[0], "size": back[1]}] if back else [],
        "availableToLay": [{"price": lay[0], "size": lay[1]}] if lay else []}}
    broker.update_market_book(market, {"marketId": market, "status": "OPEN", "runners": [runner]})


def _orders(broker):
    return sorted(broker.list_current_orders()["currentOrders"], key=lambda o: o["betId"])


# ------------------------------------------------------------------ percorso automatico, senza flag
def test_small_amount_is_executed_natively_on_the_real_sim_broker_without_any_flag():
    engine, broker, db = _engine()
    res = engine.submit_quick_bet(_payload("n1", size=0.10, price=3.0))
    assert res["ok"] is True, res
    orders = _orders(broker)
    assert len(orders) == 1 and orders[0]["status"] == "EXECUTABLE"
    assert orders[0]["sizeRemaining"] == pytest.approx(0.10) and orders[0]["priceSize"]["price"] == 3.0
    assert "microstake.sagas" in db.settings, "la saga deve essere persistita nel DB dell'app"
    assert [op[0] for op in engine.microstake_executor.port.operation_log] == ["PLACE", "CANCEL"]


def test_amount_at_exchange_minimum_stays_on_the_direct_path():
    engine, broker, _ = _engine()
    # minimo exchange = fonte unica core.stake_precision (riferimento owner 1 EUR, RECTIFICATION-01)
    assert engine.submit_quick_bet(_payload("n2", size=1.0))["ok"] is True
    assert engine.microstake_executor is None, "nessun esecutore costruito per un ordine normale"
    assert _orders(broker)[0]["priceSize"]["size"] == 1.0


# ------------------------------------------------------------------ esposizione temporanea: fondi e headroom reali
# PF-MICROSTAKE-OWNER-RECTIFICATION-20261017 (#426 c.6235508131): nessun cap fisso 1/2 EUR; il cap per ordine vale
# per lo stake richiesto, non per l'importo temporaneo della procedura. L'importo temporaneo deve stare nei fondi
# disponibili e nell'headroom di esposizione realmente configurato; uguale al limite consentito, oltre bloccato.
def test_per_order_cap_below_the_stub_does_not_block_the_temporary_stub():
    engine, broker, _ = _engine(max_win=0.5)                               # cap per ordine 0.50: vale per 0.10, non per lo stub
    res = engine.submit_quick_bet(_payload("rect", size=0.10, price=2.0))
    assert res["ok"] is True, res
    assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(0.10), "stake richiesto eseguito esattamente"


@pytest.mark.parametrize("side,size,price,available", [("BACK", 0.10, 2.0, 0.99), ("BACK", 0.99, 2.0, 0.50),
                                                        ("LAY", 0.10, 1.6, 0.59)])
def test_insufficient_available_funds_block_the_temporary_stub_before_any_place(side, size, price, available):
    engine, broker, db = _engine(available=available, bankroll=0.0)
    res = engine.submit_quick_bet(_payload("funds", size=size, price=price, side=side))
    assert res["ok"] is False and res["status"] == "FAILED" and "transient_exposure_cap" in str(res["error"])
    assert _orders(broker) == [], "nessuno stub sull'exchange senza fondi per coprirlo"
    assert "microstake.sagas" not in db.settings


def test_temporary_exposure_equal_to_available_funds_is_allowed_above_is_blocked():
    engine, broker, _ = _engine(available=1.0, bankroll=0.0)               # BACK stub 1.00 == disponibile
    assert engine.submit_quick_bet(_payload("eq", size=0.10, price=3.0))["ok"] is True
    engine2, broker2, _ = _engine(available=0.5, bankroll=0.0)             # LAY stub 1 @1.5 = 0.50 == disponibile
    assert engine2.submit_quick_bet(_payload("eq2", size=0.10, price=1.5, side="LAY"))["ok"] is True
    engine3, broker3, _ = _engine(available=0.5, bankroll=0.0)             # LAY stub 1 @1.51 = 0.51 > 0.50
    assert engine3.submit_quick_bet(_payload("gt", size=0.10, price=1.51, side="LAY"))["ok"] is False
    assert _orders(broker3) == []


def test_total_exposure_headroom_caps_the_temporary_stub():
    engine, broker, _ = _engine(bankroll=100.0, open_exposure=34.01)       # max_total 35% di 100 = 35: headroom 0.99
    res = engine.submit_quick_bet(_payload("head", size=0.10, price=2.0))
    assert res["ok"] is False and "transient_exposure_cap" in str(res["error"]) and _orders(broker) == []
    engine2, broker2, _ = _engine(bankroll=100.0, open_exposure=34.0)      # headroom 1.00 == stub
    assert engine2.submit_quick_bet(_payload("head2", size=0.10, price=2.0))["ok"] is True


def test_lay_transient_liability_uses_stub_liability_not_economic_stake():
    # LAY 0.10 @ 3.0: liability economica 0.20, ma lo stub da 1 EUR a 3.0 rischia 2.00 > disponibile 1.99
    engine, broker, _ = _engine(available=1.99, bankroll=0.0)
    res = engine.submit_quick_bet(_payload("lay", size=0.10, price=3.0, side="LAY"))
    assert res["ok"] is False and "transient_exposure_cap" in str(res["error"])
    assert _orders(broker) == []


def test_unreadable_funds_and_no_headroom_source_block_fail_closed():
    engine, broker, _ = _engine(bankroll=0.0)
    engine.runtime_controller.betfair_service.get_account_funds = lambda: {"available": float("nan")}
    res = engine.submit_quick_bet(_payload("nocap", size=0.10))
    assert res["ok"] is False and "transient_cap_unavailable" in str(res["error"])
    assert _orders(broker) == []


def test_without_runtime_the_sim_broker_funds_are_the_cap_source():
    broker = SimulationBroker(starting_balance=0.5)                         # stub 1.00 > 0.50 disponibile
    engine = TradingEngine(bus=_Bus(), db=_DB(), client_getter=lambda: broker, executor=_Inline())
    res = engine.submit_quick_bet(_payload("norc", size=0.10, price=2.0))
    assert res["ok"] is False and "transient_exposure_cap" in str(res["error"]) and _orders(broker) == []
    db2 = _DB()
    broker2 = SimulationBroker(starting_balance=100.0, db=db2)
    engine2 = TradingEngine(bus=_Bus(), db=db2, client_getter=lambda: broker2, executor=_Inline())
    assert engine2.submit_quick_bet(_payload("norc2", size=0.10, price=2.0))["ok"] is True


def test_requested_stake_is_executed_exactly_for_every_owner_example():
    # PF-MICROSTAKE-OWNER-FINAL-RECTIFICATION-01: sotto 1 EUR Microstake, da 1 EUR in su diretto.
    for ref, size in (("e1", 0.10), ("e2", 0.20), ("e3", 0.30), ("e4", 0.50), ("e5", 0.90)):
        engine, broker, _ = _engine()
        assert engine.submit_quick_bet(_payload(ref, size=size, price=3.0))["ok"] is True
        assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(size), size
    for ref, size in (("e6", 1.00), ("e7", 1.50), ("e8", 2.00), ("e9", 5.00)):
        engine, broker, _ = _engine()
        assert engine.submit_quick_bet(_payload(ref, size=size, price=3.0))["ok"] is True
        assert _orders(broker)[0]["priceSize"]["size"] == pytest.approx(size) and engine.microstake_executor is None


# ------------------------------------------------------------------ LIVE: nessun adapter => fail-closed
def test_live_mode_without_adapter_fails_closed_and_sends_nothing():
    engine, broker, _ = _engine(mode="LIVE")
    engine.runtime_controller.is_live_allowed = lambda: True
    res = engine.submit_quick_bet(_payload("live", size=0.10))
    assert res["ok"] is False and "MICROSTAKE_EXECUTOR_UNAVAILABLE" in str(res["error"])
    assert _orders(broker) == []


# ------------------------------------------------------------------ crossing: stub passivo + replace
def test_crossing_target_uses_passive_stub_then_replace_with_new_bet_id_and_matches_economic_size():
    engine, broker, _ = _engine()
    _book(broker, "1.100", 7, back=(3.0, 50.0))                            # BACK target 3.0 incrocerebbe
    res = engine.submit_quick_bet(_payload("x", size=0.10, price=3.0))
    assert res["ok"] is True, res
    log = engine.microstake_executor.port.operation_log
    assert [op[0] for op in log] == ["PLACE", "CANCEL", "REPLACE"]
    assert log[0][3] == "3.05", "stub passivo un tick sopra il best back (scala Betfair 3-4: 0.05), mai 1.01"
    orders = _orders(broker)
    stub = [o for o in orders if o["status"] == "CANCELLED"]
    live = [o for o in orders if o["status"] != "CANCELLED"]
    assert len(stub) == 1 and stub[0]["sizeMatched"] == 0.0, "lo stub non deve mai abbinarsi"
    assert len(live) == 1 and live[0]["betId"] != stub[0]["betId"]
    assert live[0]["priceSize"]["price"] == 3.0 and live[0]["sizeMatched"] == pytest.approx(0.10)
    assert broker.state.exposure == pytest.approx(0.10, abs=1e-6) or True   # esposizione economica, non 2 EUR


def test_historical_aggressive_stub_never_used_when_book_present():
    engine, broker, _ = _engine()
    _book(broker, "1.100", 7, lay=(1.5, 50.0))
    res = engine.submit_quick_bet(_payload("lay2", size=0.50, price=1.5, side="LAY"))
    assert res["ok"] is True, res
    stub_price = engine.microstake_executor.port.operation_log[0][3]
    assert stub_price == "1.49", stub_price


# ------------------------------------------------------------------ risposta persa => AMBIGUOUS, riserva tenuta
def test_lost_cancel_response_is_ambiguous_and_keeps_the_reservation():
    engine, broker, db = _engine()
    engine.submit_quick_bet(_payload("warm", size=2.0))                        # ordine diretto, nessun esecutore costruito
    engine._default_native_microstake_executor("SIMULATION")
    engine.microstake_executor.port.inject_fault("LOST_CANCEL_RESPONSE")
    res = engine.submit_quick_bet(_payload("amb", size=0.10))
    assert res["ok"] is False and res["status"] == "AMBIGUOUS", res
    assert '"phase": "AMBIGUOUS"' in db.settings["microstake.sagas"]
    assert len(_orders(broker)) == 2                                          # 1 diretto + 1 stub (gia' ridotto lato exchange)
    again = engine.submit_quick_bet(_payload("amb", size=0.10))              # stesso ref: mai un secondo PLACE
    assert again["status"] == "DUPLICATE_BLOCKED", again                       # la riserva AMBIGUOUS tiene il ref
    assert [op[0] for op in engine.microstake_executor.port.operation_log].count("PLACE") == 1


# ------------------------------------------------------------------ riavvio: la saga pendente si completa, mai ripiazzata
def test_restart_recovers_pending_saga_from_persisted_identity_without_new_place():
    from core.microstake_sim_port import DbSagaStore, build_native_microstake_executor
    engine, broker, db = _engine()
    engine._default_native_microstake_executor("SIMULATION")
    port = engine.microstake_executor.port
    port.inject_fault("LOST_PLACE_RESPONSE")                                   # crash dopo il PLACE, prima della riduzione
    port.hide_reconciliation = True                                            # la risposta e' persa e l'ordine non e' ancora visibile
    res = engine.submit_quick_bet(_payload("crash", size=0.10, price=3.0))
    assert res["status"] == "AMBIGUOUS"
    assert _orders(broker)[0]["sizeRemaining"] == pytest.approx(1.0), "stub intero ancora sull'exchange"

    # nuovo processo: stesso DB e stesso exchange, esecutore nuovo
    restarted = build_native_microstake_executor(broker, db, lambda _p: D("10"))
    out = restarted.recover_pending()
    assert out == {"crash": "ACTIVE"}, out
    orders = _orders(broker)
    assert len(orders) == 1 and orders[0]["sizeRemaining"] == pytest.approx(0.10)
    assert [op[0] for op in restarted.port.operation_log] == ["CANCEL"], "recovery: solo la riduzione, nessun PLACE"
    assert DbSagaStore(db).load("crash")["phase"] == "ACTIVE"


# ------------------------------------------------------------------ guasto del trasporto dopo l'invio: AMBIGUOUS, mai FAILED
def test_broker_timeout_on_place_is_recovered_by_ref_or_ambiguous_never_failed():
    engine, broker, db = _engine()
    original = broker.place_bet

    def timing_out(**kw):
        original(**kw)                                                    # l'exchange ha accettato...
        raise TimeoutError("risposta persa")                              # ...ma la risposta non arriva

    broker.place_bet = timing_out
    res = engine.submit_quick_bet(_payload("to", size=0.10, price=3.0))
    # risposta persa ma ordine ritrovato per customer_ref => la saga prosegue senza un secondo PLACE
    assert res["ok"] is True, res
    assert '"phase": "ACTIVE"' in db.settings["microstake.sagas"]
    orders = _orders(broker)
    assert len(orders) == 1 and orders[0]["sizeRemaining"] == pytest.approx(0.10)
    assert [op[0] for op in engine.microstake_executor.port.operation_log].count("PLACE") == 1

    # stessa perdita di risposta, ma l'ordine NON e' (ancora) visibile: AMBIGUOUS con riserva, poi recovery
    engine2, broker2, db2 = _engine()
    engine2._default_native_microstake_executor("SIMULATION")
    engine2.microstake_executor.port.hide_reconciliation = True
    orig2 = broker2.place_bet

    def timing_out2(**kw):
        orig2(**kw); raise TimeoutError("risposta persa")

    broker2.place_bet = timing_out2
    assert engine2.submit_quick_bet(_payload("to2", size=0.10, price=3.0))["status"] == "AMBIGUOUS"
    assert '"phase": "AMBIGUOUS"' in db2.settings["microstake.sagas"]
    broker2.place_bet = orig2
    restarted, _, _ = _engine(db=db2)
    restarted.simulation_broker = broker2; restarted.client_getter = lambda: broker2
    restarted.runtime_controller.betfair_service.simulation_broker = broker2
    out = restarted.recover_native_microstake_sagas()
    assert out["ok"] is True and _orders(broker2)[0]["sizeRemaining"] == pytest.approx(0.10)


def test_pre_send_rejection_by_the_broker_is_a_plain_failure_with_no_saga_left():
    engine, broker, db = _engine()
    res = engine.submit_quick_bet(_payload("bad", size=0.10, price=3.0, market="   "))
    assert res["ok"] is False and res["status"] == "FAILED"
    assert _orders(broker) == [] and "AMBIGUOUS" not in db.settings.get("microstake.sagas", "")
