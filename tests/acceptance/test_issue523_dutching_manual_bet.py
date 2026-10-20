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
    # nessuna gamba viva: tavolo e chiave del lotto liberati
    assert _busy_tables(c) == []
    assert c.rc.duplication_guard._active == {}


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


# ============================================================ 4. lotto parziale (PF-EXEC-02)
def _held_batch(c):
    held, _run = _queueing(c)
    assert _dutching(c).submit_dutching(_batch(sides=("BACK", "BACK", "BACK"), total_stake=15.0))["ok"] is True
    assert len(held) == 3
    return held


def test_one_failed_leg_does_not_release_the_batch_while_other_legs_are_open(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_FAILED", status="FAILED")
    (table,) = _busy_tables(c)
    assert table.current_exposure > 0
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


def test_partial_batch_is_signalled_and_keeps_its_table_and_key(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_SUCCESS", status="SUCCESS")
    _leg_event(c, legs[1], "QUICK_BET_FAILED", status="FAILED")
    _leg_event(c, legs[2], "QUICK_BET_SUCCESS", status="SUCCESS")

    (partial,) = c.bus.payloads("DUTCHING_BATCH_PARTIAL")
    assert partial["batch_id"] == legs[0]["batch_id"]
    assert sorted(partial["live_legs"]) == sorted([legs[0]["customer_ref"], legs[2]["customer_ref"]])
    assert partial["failed_legs"] == [legs[1]["customer_ref"]]
    assert len(_busy_tables(c)) == 1                               # nuove esposizioni sospese
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


@pytest.mark.parametrize("late", ["QUICK_BET_FILLED", "QUICK_BET_SUCCESS", "QUICK_BET_FAILED",
                                  "QUICK_BET_AMBIGUOUS"])
def test_late_event_of_a_leg_after_a_partial_batch_does_not_release_it(tmp_path, late):
    # Rilievo GPT-6.1 Sol su #529: SUCCESS(A) -> FAILED(B) -> ... chiude il lotto PARZIALE; un evento successivo
    # della gamba viva A non deve ricadere nel rilascio ordinario di tavolo e chiave.
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_SUCCESS", status="SUCCESS")
    _leg_event(c, legs[1], "QUICK_BET_FAILED", status="FAILED")
    _leg_event(c, legs[2], "QUICK_BET_SUCCESS", status="SUCCESS")
    assert len(c.bus.payloads("DUTCHING_BATCH_PARTIAL")) == 1

    _leg_event(c, legs[0], late, status=late.rsplit("_", 1)[1])

    (table,) = _busy_tables(c)
    assert table.current_exposure > 0
    assert legs[0]["event_key"] in c.rc.duplication_guard._active
    assert len(c.bus.payloads("DUTCHING_BATCH_PARTIAL")) == 1


def test_late_leg_event_racing_with_the_batch_closing_does_not_release_it(tmp_path):
    # Rilievo Grok 4.7 su #529: l'indice delle gambe era letto fuori dal lock. Un evento tardivo di A letto prima
    # che il lotto si chiuda (PARZIALE) e servito dopo non trovava piu' il lotto e rilasciava come ordine singolo.
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_SUCCESS", status="SUCCESS")
    _leg_event(c, legs[1], "QUICK_BET_FAILED", status="FAILED")

    lock = c.rc._risk_admission_lock
    with lock:                                   # RLock: il thread principale chiude il lotto mentre T aspetta
        late = threading.Thread(target=_leg_event, args=(c, legs[0], "QUICK_BET_FILLED"),
                                kwargs={"status": "FILLED"})
        late.start()
        time.sleep(0.3)                          # T e' fermo sul lock
        _leg_event(c, legs[2], "QUICK_BET_SUCCESS", status="SUCCESS")
    late.join(5)
    assert not late.is_alive()

    assert len(c.bus.payloads("DUTCHING_BATCH_PARTIAL")) == 1
    (table,) = _busy_tables(c)
    assert table.current_exposure > 0
    assert legs[0]["event_key"] in c.rc.duplication_guard._active


def test_late_success_after_an_all_filled_batch_does_not_release_its_table(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for leg in legs:
        _leg_event(c, leg, "QUICK_BET_FILLED", status="FILLED")
    assert len(_busy_tables(c)) == 1                               # FILLED: tavolo fino al settlement

    _leg_event(c, legs[0], "QUICK_BET_SUCCESS", status="SUCCESS")

    assert len(_busy_tables(c)) == 1


def test_ambiguous_leg_is_not_cleared_by_another_legs_certain_outcome(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_AMBIGUOUS", status="AMBIGUOUS")
    _leg_event(c, legs[1], "QUICK_BET_FAILED", status="FAILED")
    _leg_event(c, legs[2], "QUICK_BET_FAILED", status="FAILED")

    assert len(_busy_tables(c)) == 1
    assert legs[0]["event_key"] in c.rc._uncertain_reservations_map()
    assert legs[0]["event_key"] in c.rc.duplication_guard._active
    assert c.bus.payloads("DUTCHING_BATCH_PARTIAL") == []


def test_ambiguous_leg_resolved_later_completes_the_batch(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    _leg_event(c, legs[0], "QUICK_BET_AMBIGUOUS", status="AMBIGUOUS")
    for leg in legs[1:]:
        _leg_event(c, leg, "QUICK_BET_FAILED", status="FAILED")
    _leg_event(c, legs[0], "QUICK_BET_FAILED", status="FAILED")       # esito certo dalla riconciliazione
    assert _busy_tables(c) == []
    assert c.rc.duplication_guard._active == {}
    assert c.rc._uncertain_reservations_map() == {}


def test_all_legs_failed_releases_the_batch(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for leg in legs:
        _leg_event(c, leg, "QUICK_BET_FAILED", status="FAILED")
    assert _busy_tables(c) == []
    assert c.rc.duplication_guard._active == {}
    assert c.bus.payloads("DUTCHING_BATCH_PARTIAL") == []


def test_all_legs_filled_keeps_the_position_open(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for leg in legs:
        _leg_event(c, leg, "QUICK_BET_FILLED", status="FILLED")
    (table,) = _busy_tables(c)                                   # posizione abbinata: il tavolo resta
    assert table.current_exposure > 0
    assert c.rc.duplication_guard._active == {}                  # come un singolo FILLED: chiave rilasciata


def test_duplicate_blocked_outcome_is_not_a_leg_outcome(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    for leg in legs:
        _leg_event(c, leg, "QUICK_BET_FAILED", status="DUPLICATE_BLOCKED")
    assert len(_busy_tables(c)) == 1


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


def test_publish_failure_mid_batch_keeps_the_batch_until_the_published_legs_close(tmp_path):
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
    assert len(_busy_tables(c)) == 1                           # la gamba pubblicata puo' essere viva
    c.bus.publish = original
    _leg_event(c, sent[0], "QUICK_BET_FAILED", status="FAILED")  # unica gamba partita: chiusa
    assert _busy_tables(c) == []                               # la gamba mai pubblicata e' gia' contata chiusa
    assert c.rc.duplication_guard._active == {}


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
def test_leg_outcomes_from_concurrent_threads_settle_the_batch_once(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    start = threading.Barrier(len(legs))
    errors = []

    def deliver(leg, name, status):
        try:
            start.wait(timeout=5)
            _leg_event(c, leg, name, status=status)
        except Exception as exc:                        # pragma: no cover - fallimento del test
            errors.append(exc)

    threads = [threading.Thread(target=deliver, args=(legs[0], "QUICK_BET_SUCCESS", "SUCCESS")),
               threading.Thread(target=deliver, args=(legs[1], "QUICK_BET_FAILED", "FAILED")),
               threading.Thread(target=deliver, args=(legs[2], "QUICK_BET_SUCCESS", "SUCCESS"))]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert errors == []
    assert len(c.bus.payloads("DUTCHING_BATCH_PARTIAL")) == 1      # un solo esito di lotto
    assert len(_busy_tables(c)) == 1
    assert getattr(c.rc, "_entry_batches", {}) == {}


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


def test_admission_of_an_already_active_batch_id_is_rejected(tmp_path):
    c = _catena(tmp_path)
    legs = _held_batch(c)
    copies = [{**leg, "customer_ref": leg["customer_ref"] + "-bis", "table_id": 77} for leg in legs]
    assert c.rc.admit_controller_entries(copies, batch_id=legs[0]["batch_id"]) == "lotto_gia_attivo"
    assert c.rc.admit_controller_entries(legs, batch_id="altro") == "lotto_gia_attivo"
    batch = c.rc._entry_batches[legs[0]["batch_id"]]
    assert batch["table_id"] == legs[0]["table_id"]                  # il primo lotto non e' sovrascritto


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
