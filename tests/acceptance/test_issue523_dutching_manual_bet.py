"""#523 — corsie Dutching e ``manual_bet`` sicure prima dell'attivazione.

Catena reale: DutchingController -> RuntimeController (barriera cashout, fence d'ammissione, tavoli,
anti-duplicato) -> TradingEngine -> SimulationBroker, con il money management vero (``_catena``).
Le corsie restano dormienti in produzione (``tests/guardrails/test_dutching_lane_dormant_523.py``): qui
le si esercita direttamente, come farebbe un futuro chiamante.

Requisiti della #523:
1. lato BACK/LAY obbligatorio, nessun BACK di default, lotto rifiutato per intero;
2. barriera cashout e fence d'ammissione per ogni gamba e per l'ordine manuale;
3. esposizione del manuale nelle misure di mercato/evento (tavolo, liability LAY);
4. lotto parziale (PF-EXEC-02): tavolo e chiave trattenuti finche' ogni gamba non ha un esito certo,
   un AMBIGUOUS non cancellato dall'esito di un'altra gamba, lotto parziale segnalato.
"""
from __future__ import annotations

import json
import threading
import time

import pytest

from tests.acceptance.test_issue437_pr27 import _catena
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

EVENT = "Gamma v Delta"


def _dutching(c):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=c.bus, runtime_controller=c.rc)


def _batch(*, sides=("BACK", "BACK"), market_id="1.700", total_stake=10.0, **overrides):
    prices = (2.0, 3.0, 5.0)
    selections = []
    for idx, side in enumerate(sides):
        sel = {"selectionId": 10 * (idx + 1), "price": prices[idx]}
        if side is not None:
            sel["side"] = side
        selections.append(sel)
    payload = {"market_id": market_id, "event_name": EVENT, "market_name": "Match Odds",
               "total_stake": total_stake, "simulation_mode": True, "selections": selections}
    payload.update(overrides)
    return payload


def _manual(**overrides):
    payload = {"market_id": "1.234", "selection_id": 11, "price": 2.0, "stake": 4.0,
               "bet_type": "BACK", "event_name": "Alpha v Beta", "simulation_mode": True}
    payload.update(overrides)
    return payload


def _busy_tables(c):
    return [t for t in c.rc.table_manager._tables.values() if t.status != "FREE"]


def _barrier(rc, signal):
    with rc._risk_admission_lock:
        return rc._begin_cashout_barrier(signal)


def _no_side_effects(c):
    assert c.bus.payloads("CMD_QUICK_BET") == []
    assert c.broker.state.orders == {}
    assert _busy_tables(c) == []
    assert c.rc.duplication_guard._active == {}


def _queueing(c):
    """Bus di produzione non bloccante: CMD_QUICK_BET solo accodato, eseguito piu' tardi."""
    held = []
    original = c.bus.publish

    def publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            held.append(payload)
            c.bus.events.append((topic, dict(payload)))
            return None
        return original(topic, payload)

    c.bus.publish = publish

    def run_queued():
        c.bus.publish = original
        for order in held:
            for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
                handler(order)

    return held, run_queued


def _leg_event(c, order, event_name, **extra):
    handler = {
        "QUICK_BET_SUCCESS": c.rc._on_quick_bet_success, "QUICK_BET_FAILED": c.rc._on_quick_bet_failed,
        "QUICK_BET_AMBIGUOUS": c.rc._on_quick_bet_ambiguous, "QUICK_BET_FILLED": c.rc._on_quick_bet_filled,
        "QUICK_BET_ROLLBACK_DONE": c.rc._on_quick_bet_rollback_done,
    }[event_name]
    handler({"customer_ref": order["customer_ref"], "event_key": order["event_key"],
             "table_id": order["table_id"], "market_id": order["market_id"], **extra})


# ============================================================ 1. lato obbligatorio
@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_batch_with_explicit_side_reaches_the_broker_with_that_side(tmp_path, side):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(sides=(side, side)))
    assert res["ok"] is True, res
    legs = c.bus.payloads("CMD_QUICK_BET")
    assert [leg["bet_type"] for leg in legs] == [side, side]
    assert len(c.broker.state.orders) == 2


@pytest.mark.parametrize("sides", [(None, "BACK"), ("BACK", None), ("SIDEWAYS", "BACK"), ("BACK", ""),
                                   ("BACKK", "LAY"), ("BACK", 1)])
def test_batch_with_a_missing_or_invalid_side_is_rejected_whole(tmp_path, sides):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(sides=sides))
    assert res["ok"] is False
    assert "INVALID_SIDE" in res["error"] or "side" in res["error"].lower()
    _no_side_effects(c)


def test_preview_and_validate_refuse_a_missing_side_too(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    assert ctl.validate(_batch(sides=(None, "BACK")))["ok"] is False
    assert ctl.preview(_batch(sides=(None, "BACK")))["ok"] is False


@pytest.mark.parametrize("side", ["BACK", "LAY"])
def test_manual_bet_with_explicit_side_reaches_the_broker(tmp_path, side):
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(bet_type=side))
    assert res["ok"] is True, res
    (order,) = c.bus.payloads("CMD_QUICK_BET")
    assert order["bet_type"] == side
    assert len(c.broker.state.orders) == 1


@pytest.mark.parametrize("bet_type", [None, "", "SIDEWAYS", "LAYY", 1])
def test_manual_bet_with_a_missing_or_invalid_side_is_rejected(tmp_path, bet_type):
    c = _catena(tmp_path)
    payload = _manual()
    if bet_type is None:
        payload.pop("bet_type")
    else:
        payload["bet_type"] = bet_type
    res = _dutching(c).manual_bet(payload)
    assert res["ok"] is False
    _no_side_effects(c)


# ============================================================ 2. barriera cashout e fence
def test_batch_is_rejected_whole_while_a_cashout_is_active_in_its_scope(tmp_path):
    c = _catena(tmp_path)
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": EVENT})
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False and "cashout_invalidation_active" in res["error"]
    _no_side_effects(c)


def test_manual_bet_is_rejected_while_a_cashout_is_active_in_its_scope(tmp_path):
    c = _catena(tmp_path)
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})
    res = _dutching(c).manual_bet(_manual())
    assert res["ok"] is False and "cashout_invalidation_active" in res["error"]
    _no_side_effects(c)


def test_cashout_on_another_event_does_not_block_the_batch(tmp_path):
    c = _catena(tmp_path)
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    assert len(c.broker.state.orders) == 2


def test_queued_legs_are_never_placed_after_a_cashout_started_in_their_scope(tmp_path):
    c = _catena(tmp_path)
    held, run_queued = _queueing(c)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    assert len(held) == 2 and c.broker.state.orders == {}

    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": EVENT})
    run_queued()

    assert c.broker.state.orders == {}
    failed = c.bus.payloads("QUICK_BET_FAILED")
    assert [p["status"] for p in failed] == ["DENIED", "DENIED"]
    # #529 scelta owner F: nessun rilascio del lotto da eventi, anche con gambe negate (fail-closed, PR30)
    assert len(_busy_tables(c)) == 1


def test_queued_manual_bet_is_never_placed_after_a_cashout_started(tmp_path):
    c = _catena(tmp_path)
    held, run_queued = _queueing(c)
    assert _dutching(c).manual_bet(_manual())["ok"] is True
    _barrier(c.rc, {"signal_type": "CASHOUT", "event_name": "Alpha v Beta"})
    run_queued()
    assert c.broker.state.orders == {}
    assert [p["status"] for p in c.bus.payloads("QUICK_BET_FAILED")] == ["DENIED"]
    assert _busy_tables(c) == []


def test_queued_legs_without_a_cashout_are_placed_and_registered_in_flight(tmp_path):
    c = _catena(tmp_path)
    held, run_queued = _queueing(c)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    assert set(c.rc._queued_entry_fences) == {leg["customer_ref"] for leg in held}
    run_queued()
    assert len(c.broker.state.orders) == 2
    assert c.rc._queued_entry_fences == {}


# ============================================================ 3. esposizione del manuale
@pytest.mark.parametrize("side,expected", [("BACK", 4.0), ("LAY", 4.0 * (3.0 - 1.0))])
def test_manual_bet_exposure_enters_market_event_and_total_measures(tmp_path, side, expected):
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(bet_type=side, price=3.0))
    assert res["ok"] is True, res
    (table,) = _busy_tables(c)
    assert table.current_exposure == pytest.approx(expected)
    assert c.rc.table_manager.total_exposure() == pytest.approx(expected)
    assert c.rc._market_current_exposure("1.234") == pytest.approx(expected)
    assert c.rc._event_current_exposure(_signal(event="Alpha v Beta")) == pytest.approx(expected)


def test_manual_lay_cap_check_uses_the_liability_not_the_stake(tmp_path):
    # bankroll 1000, max_single_bet 18% = 180: stake 50 a quota 5 LAY = liability 200 > 180
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(bet_type="LAY", price=5.0, stake=50.0))
    assert res["ok"] is False
    _no_side_effects(c)


def test_failed_manual_bet_releases_its_table(tmp_path):
    c = _catena(tmp_path)
    held, run_queued = _queueing(c)
    assert _dutching(c).manual_bet(_manual())["ok"] is True
    (order,) = held
    _leg_event(c, order, "QUICK_BET_FAILED", status="FAILED")
    assert _busy_tables(c) == []


# ============================================================ 4. esito del lotto: trattenuto (#529, scelta owner F)
def _held_batch(c):
    held, _run = _queueing(c)
    assert _dutching(c).submit_dutching(_batch(sides=("BACK", "BACK", "BACK"), total_stake=15.0))["ok"] is True
    assert len(held) == 3
    return held


# ============================================================ duplicati, fondi, broker SIM reale
def test_same_batch_submitted_twice_is_blocked(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    assert ctl.submit_dutching(_batch())["ok"] is True
    res = ctl.submit_dutching(_batch())
    assert res["ok"] is False
    assert len(c.broker.state.orders) == 2


def test_duplication_guard_error_rejects_the_batch_instead_of_continuing(tmp_path):
    c = _catena(tmp_path)

    def boom(_key):
        raise RuntimeError("guard down")

    c.rc.duplication_guard.acquire = boom
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_manual_bet_with_the_same_customer_ref_is_sent_once(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    assert ctl.manual_bet(_manual(customer_ref="MAN-523-A", event_key="k1"))["ok"] is True
    ctl.manual_bet(_manual(customer_ref="MAN-523-A", event_key="k2"))
    assert len(c.broker.state.orders) == 1


def test_side_is_normalised_like_the_signals(tmp_path):
    """Stesso contratto di `validators.signal_side`: maiuscole/spazi normalizzati, nient'altro."""
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch(sides=("back ", " Back")))["ok"] is True
    assert [leg["bet_type"] for leg in c.bus.payloads("CMD_QUICK_BET")] == ["BACK", "BACK"]


def test_manual_bet_with_conflicting_side_aliases_is_rejected(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(bet_type="BACK", side="LAY"))
    assert res["ok"] is False
    _no_side_effects(c)


# ============================================================ fail-closed, pubblicazione parziale, timeout reale
def test_runtime_without_the_barrier_api_rejects_the_batch(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    ctl.runtime = type("RuntimeSenzaBarriera", (), {
        "__getattr__": lambda self, name: getattr(c.rc, name) if name != "admit_controller_entries"
        else (_ for _ in ()).throw(AttributeError(name))})()
    res = ctl.submit_dutching(_batch())
    assert res["ok"] is False and "barriera_cashout_non_disponibile" in res["error"]
    assert c.bus.payloads("CMD_QUICK_BET") == []


def test_lost_broker_response_on_one_leg_keeps_the_batch_uncertain(tmp_path):
    """Catena reale: il broker SIM piazza la gamba e la risposta si perde (timeout) -> AMBIGUOUS dal
    TradingEngine; l'esito certo delle altre gambe non libera il lotto."""
    import functools

    c = _catena(tmp_path)
    original = c.broker.place_bet
    calls = []

    @functools.wraps(original)
    def second_leg_times_out(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(1)
        if len(calls) == 2:
            raise TimeoutError("read timeout dopo l'invio")
        return result

    c.broker.place_bet = second_leg_times_out
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    assert len(c.broker.state.orders) == 2                       # entrambe sull'exchange
    assert c.bus.payloads("QUICK_BET_AMBIGUOUS")
    assert len(_busy_tables(c)) == 1
    key = c.bus.payloads("CMD_QUICK_BET")[0]["event_key"]
    assert key in c.rc.duplication_guard._active
    assert key in c.rc._uncertain_reservations_map()


def test_same_batch_after_an_uncertain_leg_is_never_sent_twice(tmp_path):
    import functools

    c = _catena(tmp_path)
    original = c.broker.place_bet

    @functools.wraps(original)
    def lose_response(*args, **kwargs):
        original(*args, **kwargs)
        raise TimeoutError("read timeout dopo l'invio")

    c.broker.place_bet = lose_response
    ctl = _dutching(c)
    ctl.submit_dutching(_batch())
    before = len(c.broker.state.orders)
    c.broker.place_bet = original
    assert ctl.submit_dutching(_batch())["ok"] is False          # chiave del lotto trattenuta
    assert len(c.broker.state.orders) == before


# ============================================================ catena del bus: middleware -> controller
def _bus_lane(c):
    from core.risk_middleware import RiskMiddleware

    RiskMiddleware(c.bus, topics=("REQ_PLACE_DUTCHING",))
    _dutching(c).attach_bus_consumer()


def test_bus_lane_with_a_leg_without_side_places_nothing(tmp_path):
    """Il RiskMiddleware non inventa piu' BACK: la gamba senza lato arriva al controller senza lato e il
    lotto e' rifiutato per intero."""
    c = _catena(tmp_path)
    _bus_lane(c)
    c.bus.publish("REQ_PLACE_DUTCHING", {
        "market_id": "1.700", "event_name": EVENT, "total_stake": 10.0, "simulation_mode": True,
        "results": [{"selectionId": 10, "price": 2.0, "side": "BACK"}, {"selectionId": 20, "price": 3.0}]})
    (cmd,) = c.bus.payloads("CMD_PLACE_DUTCHING")
    assert [r["side"] for r in cmd["results"]] == ["BACK", None]
    _no_side_effects(c)


def test_bus_lane_with_an_explicit_batch_side_reaches_the_broker(tmp_path):
    c = _catena(tmp_path)
    _bus_lane(c)
    c.bus.publish("REQ_PLACE_DUTCHING", {
        "market_id": "1.700", "event_name": EVENT, "total_stake": 10.0, "simulation_mode": True,
        "bet_type": "BACK",
        "results": [{"selectionId": 10, "price": 2.0}, {"selectionId": 20, "price": 3.0}]})
    assert [leg["bet_type"] for leg in c.bus.payloads("CMD_QUICK_BET")] == ["BACK", "BACK"]
    assert len(c.broker.state.orders) == 2


# ============================================================ crash/riavvio durante gli ingressi
def test_restart_with_legs_live_and_one_uncertain_keeps_every_leg_held(tmp_path):
    """Crash dopo l'invio delle gambe (una con risposta persa): il processo nuovo, con solo DB ed exchange,
    ricostruisce ogni gamba come riserva trattenuta (F018), l'incerta come incerta. Nessun tavolo libero
    per una gamba forse viva, nessun reinvio del lotto."""
    import functools

    from core.duplication_guard import DuplicationGuard
    from core.runtime_controller import RuntimeController
    from core.table_manager import TableManager

    c = _catena(tmp_path)
    original = c.broker.place_bet
    calls = []

    @functools.wraps(original)
    def second_leg_times_out(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(1)
        if len(calls) == 2:
            raise TimeoutError("read timeout dopo l'invio")
        return result

    c.broker.place_bet = second_leg_times_out
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    refs = [leg["customer_ref"] for leg in c.bus.payloads("CMD_QUICK_BET")]

    restarted = RuntimeController.__new__(RuntimeController)        # processo nuovo: solo DB ed exchange
    restarted.db = c.db
    restarted.betfair_service = c.rc.betfair_service
    restarted.table_manager = TableManager(table_count=5)
    restarted.duplication_guard = DuplicationGuard()
    result = restarted._rebuild_open_positions()

    assert result["ok"] is True
    assert refs[1] in result["uncertain"]
    held = [t for t in restarted.table_manager.snapshot() if t.get("status") != "FREE"]
    assert held and all(t["current_exposure"] > 0 for t in held)
    # l'intento di ogni gamba e' consumato in modo durevole: il processo nuovo non puo' rinviarla
    assert all(c.db.is_order_intent_consumed(ref) for ref in refs)


# ============================================================ concorrenza
def test_same_batch_from_two_threads_is_sent_once(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    start = threading.Barrier(2)
    results = []

    def submit():
        start.wait(timeout=5)
        results.append(ctl.submit_dutching(_batch()))

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert sorted(r["ok"] for r in results) == [False, True]
    assert len(c.broker.state.orders) == 2


def test_two_batches_on_different_events_are_independent(tmp_path):
    c = _catena(tmp_path)
    ctl = _dutching(c)
    assert ctl.submit_dutching(_batch())["ok"] is True
    assert ctl.submit_dutching(_batch(market_id="1.701", event_name="Epsilon v Zeta"))["ok"] is True
    assert len(c.broker.state.orders) == 4
    assert len(_busy_tables(c)) == 2


# ============================================================ rilievi Codex su #529 (0cef6b4)
def test_concurrent_manual_bets_cannot_exceed_the_total_exposure_cap_together(tmp_path):
    # bankroll 1000: max_single_bet 18% = 180, max_total 35% = 350. LAY 90 a quota 3 = liability 180 ciascuno:
    # uno solo passa, due insieme (360) superano il cap. Prima i due leggevano la stessa esposizione.
    c = _catena(tmp_path)
    ctrl = _dutching(c)
    results = {}
    with c.rc._risk_admission_lock:              # RLock: T resta fermo finche' il primo non ha riservato
        late = threading.Thread(target=lambda: results.setdefault("late", ctrl.manual_bet(
            _manual(bet_type="LAY", price=3.0, stake=90.0, selection_id=22, event_name="Eta v Theta"))))
        late.start()
        time.sleep(0.3)
        assert _busy_tables(c) == []             # T non ha riservato nulla fuori dal lock
        first = ctrl.manual_bet(_manual(bet_type="LAY", price=3.0, stake=90.0))
    late.join(5)
    assert first["ok"] is True, first
    assert results["late"]["ok"] is False
    assert "Esposizione globale oltre limite" in results["late"]["error"]
    assert len(_busy_tables(c)) == 1
    assert c.rc.table_manager.total_exposure() == pytest.approx(180.0)


def test_manual_bet_reserves_its_own_table_even_with_a_caller_table_id(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(table_id=999, bet_type="LAY", price=3.0))
    assert res["ok"] is True, res
    (table,) = _busy_tables(c)
    assert res["order"]["table_id"] == table.table_id != 999
    assert table.current_exposure == pytest.approx(8.0)


def test_dutching_reserves_its_own_table_even_with_a_caller_table_id(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(table_id=999))
    assert res["ok"] is True, res
    (table,) = _busy_tables(c)
    assert res["table_id"] == table.table_id != 999
    assert table.current_exposure > 0


@pytest.mark.parametrize("failing", ["allocate", "activate"])
def test_manual_bet_table_failure_releases_the_dedupe_key(tmp_path, monkeypatch, failing):
    c = _catena(tmp_path)

    def boom(*a, **k):
        raise RuntimeError("tavolo KO")

    monkeypatch.setattr(c.rc.table_manager, failing, boom)
    res = _dutching(c).manual_bet(_manual())
    assert res["ok"] is False
    _no_side_effects(c)                          # chiave rilasciata, nessun tavolo, nessun ordine
    monkeypatch.undo()
    assert _dutching(c).manual_bet(_manual())["ok"] is True   # retry sicuro


def test_dutching_table_failure_releases_the_dedupe_key(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    monkeypatch.setattr(c.rc.table_manager, "activate", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("KO")))
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False
    _no_side_effects(c)
    monkeypatch.undo()
    assert _dutching(c).submit_dutching(_batch())["ok"] is True


@pytest.mark.parametrize("side,alias", [("BACK", "LAY"), ("LAY", "BACK")])
def test_dutching_selection_with_conflicting_side_aliases_is_rejected(tmp_path, side, alias):
    c = _catena(tmp_path)
    payload = _batch(sides=(side, side))
    payload["selections"][0]["effectiveType"] = alias
    res = _dutching(c).submit_dutching(payload)
    assert res["ok"] is False
    assert "INVALID_SIDE" in res["error"]
    _no_side_effects(c)


def test_dutching_selection_side_from_alias_alone_is_accepted(tmp_path):
    c = _catena(tmp_path)
    payload = _batch(sides=(None, None))
    for sel in payload["selections"]:
        sel["effectiveType"] = "LAY"
    res = _dutching(c).submit_dutching(payload)
    assert res["ok"] is True, res
    assert [leg["bet_type"] for leg in c.bus.payloads("CMD_QUICK_BET")] == ["LAY", "LAY"]


# ============================================================ rilievi Codex su #529 (ace631a)
def test_rejected_batch_never_releases_a_key_it_did_not_acquire(tmp_path):
    # anti-duplicato disattivato: la chiave dell'evento appartiene a un'altra posizione e non va rilasciata.
    c = _catena(tmp_path, anti_duplication_enabled=False)
    event_key = _dutching(c).precheck(_batch())["event_key"]
    assert c.rc.duplication_guard.acquire(event_key)
    _barrier(c.rc, _signal(event=EVENT))
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False
    assert event_key in c.rc.duplication_guard._active


def test_batch_rejected_by_a_cap_does_not_keep_the_dedupe_key(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).submit_dutching(_batch(total_stake=400.0))   # 400 > cap totale 350 su bankroll 1000
    assert res["ok"] is False
    assert "oltre limite" in res["error"]
    assert c.rc.duplication_guard._active == {}
    assert _dutching(c).submit_dutching(_batch())["ok"] is True


def test_manual_bet_table_records_its_selection(tmp_path):
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(selection_id=11))
    assert res["ok"] is True, res
    (table,) = _busy_tables(c)
    assert str(table.selection_id) == "11"


# ============================================================ riesame indipendente su #529 (ace631a)
def test_batch_failing_before_any_publish_releases_table_and_key_once(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    releases, unlocks = [], []
    real_release, real_unlock = c.rc.duplication_guard.release, c.rc.table_manager.force_unlock
    monkeypatch.setattr(c.rc.duplication_guard, "release", lambda k: (releases.append(k), real_release(k)))
    monkeypatch.setattr(c.rc.table_manager, "force_unlock",
                        lambda t, *a, **k: (unlocks.append(t), real_unlock(t, *a, **k))[1])
    original = c.bus.publish

    def publish(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            raise RuntimeError("bus KO")
        return original(topic, payload)

    c.bus.publish = publish
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False
    assert len(releases) == 1 and len(unlocks) <= 1               # una sola volta (prima: runtime + controller)
    _no_side_effects(c)
    assert getattr(c.rc, "_held_batch_legs", {}) == {}                 # lotto ritirato: fuori dal registro


def test_preflight_does_not_keep_the_dedupe_key(tmp_path):
    c = _catena(tmp_path)
    pre = _dutching(c).submit_dutching(_batch(), preflight=True)
    assert pre["ok"] is True, pre
    assert c.rc.duplication_guard._active == {}
    assert _dutching(c).submit_dutching(_batch())["ok"] is True


# ============================================================ rilievi Grok su #529
def test_single_order_admission_cannot_take_over_an_active_batch_leg(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    single = {**legs[0], "table_id": 77, "batch_id": ""}
    assert c.rc.admit_controller_entries([single]) == "lotto_gia_attivo"
    assert c.rc.admit_controller_entries([single], batch_id=legs[0]["batch_id"]) == "lotto_gia_attivo"
    other = {**legs[0], "customer_ref": "man-nuovo", "batch_id": ""}
    assert c.rc.admit_controller_entries([other], batch_id=legs[0]["batch_id"]) == "lotto_gia_attivo"


# ============================================================ ciclo 6 #529 (deroga owner): cache dei lotti chiusi
def _record_releases(c, monkeypatch):
    calls = []
    for name in ("force_unlock", "force_unlock_if_event"):
        if hasattr(c.rc.table_manager, name):
            real = getattr(c.rc.table_manager, name)
            monkeypatch.setattr(c.rc.table_manager, name,
                                lambda *a, _n=name, _r=real, **k: (calls.append(_n), _r(*a, **k))[1])
    real_release = c.rc.duplication_guard.release
    monkeypatch.setattr(c.rc.duplication_guard, "release",
                        lambda k: (calls.append("release"), real_release(k))[1])
    return calls


@pytest.mark.parametrize("path", ["manual_bet", "signal"])
def test_upstream_ref_with_the_dutching_prefix_is_released_like_any_single_order(tmp_path, path):
    # Riesame su 6e0e63a: un ref a monte "dut-..." (bet manuale, segnale, REQ_QUICK_BET 1:1) non e' una gamba
    # Dutching e non deve trattenere tavolo e chiave per sempre.
    c = _catena(tmp_path)
    if path == "manual_bet":
        res = _dutching(c).manual_bet(_manual(customer_ref="dut-upstream-ref-1"))
        assert res["ok"] is True, res
    else:
        c.rc._on_signal_received(_signal(customer_ref="dut-upstream-ref-2"))
    (order,) = c.bus.payloads("CMD_QUICK_BET")
    assert order["customer_ref"].startswith("dut-")
    assert len(_busy_tables(c)) == 1
    c.rc._on_quick_bet_failed({"customer_ref": order["customer_ref"], "event_key": order["event_key"],
                               "table_id": order["table_id"], "status": "FAILED"})
    assert _busy_tables(c) == []
    assert c.rc.duplication_guard._active == {}


def test_leg_event_after_a_restart_never_releases_the_rebuilt_table(tmp_path):
    from core.duplication_guard import DuplicationGuard
    from core.runtime_controller import RuntimeController
    from core.table_manager import TableManager

    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True          # SIM reale: gambe piazzate
    legs = c.bus.payloads("CMD_QUICK_BET")

    restarted = RuntimeController.__new__(RuntimeController)            # processo nuovo: solo DB ed exchange
    restarted.db = c.db
    restarted.betfair_service = c.rc.betfair_service
    restarted.table_manager = TableManager(table_count=5)
    restarted.duplication_guard = DuplicationGuard()
    restarted._rebuild_open_positions()
    held = [t for t in restarted.table_manager._tables.values() if t.status != "FREE"]
    assert held

    for leg in legs:                                                   # esiti tardivi delle gambe pre-riavvio
        restarted._release_if_terminal({"customer_ref": leg["customer_ref"], "event_key": leg["event_key"],
                                        "table_id": held[0].table_id, "status": "SUCCESS"},
                                       event_name="QUICK_BET_SUCCESS")
    assert [t for t in restarted.table_manager._tables.values() if t.status != "FREE"] == held


def test_same_batch_resubmitted_is_blocked_by_the_dedupe_key_first(tmp_path):
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    again = _dutching(c).submit_dutching(_batch())
    assert again["ok"] is False and again["error"] == "Duplicato bloccato"


def test_historical_batch_identifiers_are_rejected_at_admission_even_after_a_restart(tmp_path):
    # Con l'anti-duplicato disattivato nessuna chiave ferma il lotto ripetuto: i ref storici arrivavano
    # all'engine, che li bloccava come duplicati senza esito di gamba => tavolo nuovo trattenuto per sempre.
    from core.runtime_controller import RuntimeController

    c = _catena(tmp_path, anti_duplication_enabled=False)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True          # gambe piazzate sul broker SIM
    legs = c.bus.payloads("CMD_QUICK_BET")
    for leg in legs:
        _leg_event(c, leg, "QUICK_BET_FILLED", status="FILLED")
    tables_before = [(t.table_id, t.current_exposure) for t in _busy_tables(c)]
    keys_before = dict(c.rc.duplication_guard._active)

    again = _dutching(c).submit_dutching(_batch())                       # stesso lotto: stessi ref storici
    assert again["ok"] is False
    assert "lotto_gia_attivo" in again["error"] or "ingresso_gia_eseguito" in again["error"]
    # nessun tavolo nuovo trattenuto per sempre, chiavi come prima, nessun ordine in piu'
    assert [(t.table_id, t.current_exposure) for t in _busy_tables(c)] == tables_before
    assert dict(c.rc.duplication_guard._active) == keys_before
    assert len(c.broker.state.orders) == len(legs)

    restarted = RuntimeController.__new__(RuntimeController)
    restarted.db = c.db
    assert restarted.admit_controller_entries(legs, batch_id=legs[0]["batch_id"]) == "ingresso_gia_eseguito"


def test_admission_is_rejected_when_consumed_intents_cannot_be_verified(tmp_path, monkeypatch):
    c = _catena(tmp_path)

    def boom(_ref):
        raise RuntimeError("db KO")

    monkeypatch.setattr(c.db, "is_order_intent_consumed", boom)
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False
    assert "ammissione_non_verificabile" in res["error"]
    _no_side_effects(c)


# ============================================================ #529 scelta owner F: lotto trattenuto, mai rilasciato da eventi
TERMINAL = ["QUICK_BET_SUCCESS", "QUICK_BET_FILLED", "QUICK_BET_FAILED", "QUICK_BET_ROLLBACK_DONE",
            "QUICK_BET_AMBIGUOUS"]


def _status(event_name):
    return event_name.rsplit("_", 1)[1]


@pytest.mark.parametrize("event_name", TERMINAL)
def test_no_lifecycle_event_of_a_leg_releases_the_batch(tmp_path, event_name):
    # Su main il primo FAILED/SUCCESS di una gamba liberava tavolo e chiave dell'intero lotto.
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], event_name, status=_status(event_name))
    (table,) = _busy_tables(c)
    assert table.current_exposure > 0
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


@pytest.mark.parametrize("event_name", TERMINAL)
def test_every_leg_with_the_same_outcome_still_does_not_release_the_batch(tmp_path, event_name):
    # Anche tutte FAILED: lo stato terminale del lotto non e' provato dagli eventi (PF-EXEC-02 -> PR30).
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for leg in legs:
        _leg_event(c, leg, event_name, status=_status(event_name))
    assert len(_busy_tables(c)) == 1
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


@pytest.mark.parametrize("sequence", [
    ["SUCCESS", "FAILED", "SUCCESS", "SUCCESS:1"],          # Codex r4325463093: SUCCESS tardivo su gamba FAILED
    ["FILLED", "FAILED", "FAILED"],                          # Codex r4325463111: parziale con gamba abbinata
    ["FAILED", "FAILED", "FAILED", "AMBIGUOUS:0"],           # Codex r4325463120: AMBIGUOUS tardivo
    ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED:0", "ROLLBACK_DONE:1"],
])
def test_mixed_late_and_contradictory_outcomes_keep_the_batch_held(tmp_path, sequence):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for idx, step in enumerate(sequence):
        status, _, which = step.partition(":")
        leg = legs[int(which)] if which else legs[idx]
        _leg_event(c, leg, f"QUICK_BET_{status}", status=status)
    assert len(_busy_tables(c)) == 1
    assert legs[0]["event_key"] in c.rc.duplication_guard._active
    assert c.bus.payloads("DUTCHING_BATCH_PARTIAL") == []                # nessun esito di lotto dichiarato


def test_ambiguous_leg_marks_the_reservation_uncertain_on_the_batch_table(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, {**legs[1], "table_id": 99}, "QUICK_BET_AMBIGUOUS", status="AMBIGUOUS")
    assert c.rc._uncertain_tables.get(legs[1]["event_key"]) == {legs[1]["table_id"]}


def test_publish_failure_mid_batch_keeps_the_whole_batch_held(tmp_path):
    c = _catena(tmp_path)
    original = c.bus.publish
    sent = []

    def flaky(topic, payload=None):
        if topic == "CMD_QUICK_BET":
            if sent:
                raise RuntimeError("bus giu' dopo la prima gamba")
            sent.append(payload)
            c.bus.events.append((topic, dict(payload)))
            return None
        return original(topic, payload)

    c.bus.publish = flaky
    res = _dutching(c).submit_dutching(_batch())
    assert res["ok"] is False and res["published_count"] == 1
    c.bus.publish = original
    _leg_event(c, sent[0], "QUICK_BET_FAILED", status="FAILED")
    assert len(_busy_tables(c)) == 1                                    # una gamba e' partita: trattenuto
    assert sent[0]["event_key"] in c.rc.duplication_guard._active


def test_concurrent_leg_outcomes_never_release_the_batch(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    start = threading.Barrier(len(legs))
    errors = []

    def deliver(leg, name):
        try:
            start.wait(timeout=5)
            _leg_event(c, leg, name, status=_status(name))
        except Exception as exc:                        # pragma: no cover - fallimento del test
            errors.append(exc)

    threads = [threading.Thread(target=deliver, args=(leg, name))
               for leg, name in zip(legs, ["QUICK_BET_SUCCESS", "QUICK_BET_FAILED", "QUICK_BET_AMBIGUOUS"])]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    assert len(_busy_tables(c)) == 1
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


def test_admission_of_an_already_registered_batch_is_rejected(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    copies = [{**leg, "customer_ref": leg["customer_ref"] + "-bis", "table_id": 77} for leg in legs]
    assert c.rc.admit_controller_entries(copies, batch_id=legs[0]["batch_id"]) == "lotto_gia_attivo"
    assert c.rc.admit_controller_entries(legs, batch_id="altro") == "lotto_gia_attivo"
    assert c.rc._held_batch_legs[legs[0]["customer_ref"]]["table_id"] == legs[0]["table_id"]


def test_a_forged_batch_id_on_a_dut_signal_can_only_hold_its_own_order(tmp_path):
    # Un segnale puo' inoltrare un batch_id (rilievo F1 del riesame): con la scelta F l'effetto e' solo trattenere
    # quell'ordine fino a settlement/riconciliazione, mai rilasciare qualcosa senza prova.
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal(customer_ref="dut-forged-1", batch_id="falso"))
    (order,) = c.bus.payloads("CMD_QUICK_BET")
    c.rc._on_quick_bet_failed({"customer_ref": order["customer_ref"], "event_key": order["event_key"],
                               "table_id": order["table_id"], "status": "FAILED"})
    assert len(_busy_tables(c)) == 1                                    # trattenuto: fail-closed
    assert c.broker.state.orders and len(c.broker.state.orders) == 1


def test_unregistered_leg_with_an_unreadable_db_is_held(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    legs = c.bus.payloads("CMD_QUICK_BET")
    c.rc._held_batch_legs.clear()                                       # come dopo un riavvio
    real = c.db._execute

    def unreadable(sql, *a, **k):
        if "FROM orders WHERE customer_ref" in sql:
            raise RuntimeError("db KO")
        return real(sql, *a, **k)

    monkeypatch.setattr(c.db, "_execute", unreadable)
    _leg_event(c, legs[0], "QUICK_BET_FAILED", status="FAILED")
    assert len(_busy_tables(c)) == 1



# ============================================================ #529 ciclo A (owner): F028, chiave pinnata, identificativi
def test_f028_sweep_never_releases_the_shared_table_of_a_batch_for_one_unmatched_leg(tmp_path, monkeypatch):
    # Riesame su 922bde3: una gamba annullata/decaduta senza abbinato liberava il tavolo CONDIVISO del lotto,
    # togliendo dai cap l'esposizione delle gambe ancora vive.
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True          # gambe SUBMITTED con betId (SIM)
    rows = [dict(r) for r in c.db._execute("SELECT customer_ref, response_json FROM orders", fetch=True,
                                           commit=False)]
    first_bet = json.loads(rows[0]["response_json"])["instructionReports"][0]["betId"]
    monkeypatch.setattr(c.rc, "_final_unmatched_bets", lambda markets, bets: {str(first_bet)})
    (table,) = _busy_tables(c)
    exposure = table.current_exposure

    assert c.rc._release_final_unmatched_orders() == []

    (table,) = _busy_tables(c)
    assert table.current_exposure == pytest.approx(exposure)


def test_f028_sweep_still_releases_a_single_unmatched_order(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    c.rc._on_signal_received(_signal())
    (row,) = [dict(r) for r in c.db._execute("SELECT customer_ref, response_json FROM orders", fetch=True,
                                             commit=False)]
    bet = json.loads(row["response_json"])["instructionReports"][0]["betId"]
    monkeypatch.setattr(c.rc, "_final_unmatched_bets", lambda markets, bets: {str(bet)})
    assert c.rc._release_final_unmatched_orders() == [row["customer_ref"]]
    assert _busy_tables(c) == []


def test_batch_dedupe_key_is_pinned_while_the_batch_is_held(tmp_path):
    # Rilievo Codex r4325985989: scaduto il TTL della chiave, lo stesso evento con un prezzo diverso (stesso
    # event_key, altro batch_id e altri ref) piazzava un secondo lotto mentre il primo era trattenuto.
    c = _catena(tmp_path)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    c.rc.duplication_guard.ttl = 0                                      # TTL scaduto per ogni chiave non fissata
    changed = _batch()
    changed["selections"][0]["price"] = 2.2
    again = _dutching(c).submit_dutching(changed)
    assert again["ok"] is False and again["error"] == "Duplicato bloccato"
    assert len(_busy_tables(c)) == 1


def test_pinned_batch_key_is_released_by_a_rollback_before_publishing(tmp_path, monkeypatch):
    c = _catena(tmp_path)
    monkeypatch.setattr(c.rc.table_manager, "activate", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("KO")))
    assert _dutching(c).submit_dutching(_batch())["ok"] is False
    assert c.rc.duplication_guard._active == {}
    assert not c.rc.duplication_guard._pinned


@pytest.mark.parametrize("field,value", [("market_id", ""), ("market_id", None), ("selection_id", 0),
                                         ("selection_id", -3), ("selection_id", "abc"), ("selection_id", 5.5),
                                         ("stake", float("nan")), ("price", float("inf"))])
def test_manual_bet_with_an_invalid_identifier_or_amount_takes_no_resource(tmp_path, field, value):
    # Rilievo Codex r4325986004: il tavolo veniva riservato prima della validazione; l'engine rifiutava in
    # normalizzazione con un FAILED senza table_id e il tavolo restava occupato.
    c = _catena(tmp_path)
    res = _dutching(c).manual_bet(_manual(**{field: value}))
    assert res["ok"] is False
    _no_side_effects(c)


# ============================================================ #529 ciclo A2 (owner): identificativi, lotto atomico, precheck
@pytest.mark.parametrize("mutate", [
    lambda b: b["selections"][1].update(selectionId=0),
    lambda b: b["selections"][1].update(selectionId=-1),
    lambda b: b["selections"][1].update(selectionId="abc"),
    lambda b: b["selections"][1].update(selectionId=20.5),
    lambda b: b["selections"][1].update(price=float("inf")),
    lambda b: b["selections"][1].update(price=float("nan")),
    lambda b: b.update(market_id="   "),
    lambda b: b.update(total_stake=float("nan")),
], ids=["sel0", "sel-neg", "sel-str", "sel-float", "price-inf", "price-nan", "market-blank", "total-nan"])
def test_batch_with_one_engine_invalid_leg_is_rejected_whole(tmp_path, mutate):
    # Rilievo Codex r4326488228: selectionId=0 passava int(); l'engine rifiutava quella gamba e piazzava le altre.
    c = _catena(tmp_path)
    payload = _batch()
    mutate(payload)
    res = _dutching(c).submit_dutching(payload)
    assert res["ok"] is False
    _no_side_effects(c)


@pytest.mark.parametrize("stale_selection", [10, 20])
def test_selection_scoped_cashout_after_admission_places_no_leg_of_the_batch(tmp_path, stale_selection):
    # Rilievo Codex r4326488240: il fence di ogni gamba era indipendente; un cashout limitato a una selezione negava
    # solo quella gamba e l'altra partiva: esposizione su una gamba sola.
    c = _catena(tmp_path)
    held, run_queued = _queueing(c)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    assert len(held) == 2 and c.broker.state.orders == {}

    _barrier(c.rc, {"signal_type": "CASHOUT", "market_id": "1.700", "selection_id": stale_selection})
    run_queued()

    assert c.broker.state.orders == {}                                   # nessuna gamba piazzata
    assert len(_busy_tables(c)) == 1                                     # lotto trattenuto (fail-closed)


def test_concurrent_delivery_of_legs_after_a_selection_cashout_places_nothing(tmp_path):
    c = _catena(tmp_path)
    held, _run = _queueing(c)
    assert _dutching(c).submit_dutching(_batch(sides=("BACK", "BACK", "BACK"), total_stake=15.0))["ok"] is True
    _barrier(c.rc, {"signal_type": "CASHOUT", "market_id": "1.700", "selection_id": 30})
    c.bus.publish = c.bus.__class__.publish.__get__(c.bus)
    start = threading.Barrier(len(held))
    errors = []

    def deliver(order):
        try:
            start.wait(timeout=5)
            for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
                handler(order)
        except Exception as exc:                        # pragma: no cover - fallimento del test
            errors.append(exc)

    threads = [threading.Thread(target=deliver, args=(order,)) for order in held]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    assert c.broker.state.orders == {}


def test_leg_already_sent_before_the_cashout_stays_counted_and_held(tmp_path):
    c = _catena(tmp_path)
    held, _run = _queueing(c)
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
    c.bus.publish = c.bus.__class__.publish.__get__(c.bus)
    for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
        handler(held[0])                                                 # prima gamba inviata
    assert len(c.broker.state.orders) == 1
    _barrier(c.rc, {"signal_type": "CASHOUT", "market_id": "1.700", "selection_id": 20})
    for handler in list(c.bus.subscribers.get("CMD_QUICK_BET", [])):
        handler(held[1])                                                 # seconda: negata
    assert len(c.broker.state.orders) == 1
    (table,) = _busy_tables(c)
    assert table.current_exposure > 0                                    # la gamba inviata resta contabilizzata


def test_public_precheck_has_no_permanent_side_effect(tmp_path):
    # Rilievo Codex r4326488216: il precheck pubblico prendeva una chiave fissata mai rilasciata.
    c = _catena(tmp_path)
    pre = _dutching(c).precheck(_batch())
    assert pre["ok"] is True, pre
    assert c.rc.duplication_guard._active == {} and not c.rc.duplication_guard._pinned
    assert _busy_tables(c) == []
    assert _dutching(c).submit_dutching(_batch())["ok"] is True
