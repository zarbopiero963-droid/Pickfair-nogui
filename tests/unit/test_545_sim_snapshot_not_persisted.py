"""#545 (PFC-006 follow-up, P1): esito SIM `{"status": "SUCCESS", "snapshot_persisted": False}` mai come esito certo.

Prove di fault injection sulla catena reale: SimulationBroker (con il Database sqlite del runtime come nel
cablaggio di produzione, `BetfairService._connect_simulation` -> `db=settings_service.db`) -> TradingEngine ->
RuntimeController/TableManager -> crash -> riavvio (broker ricaricato SOLO dallo snapshot `simulation_state`,
come `_connect_simulation`) -> ricostruzione F018 (`_rebuild_open_positions`).

L'errore iniettato e' un errore ordinario del DB (`sqlite3.OperationalError: database is locked`) sulla sola
scrittura dello snapshot di reload: il broker lo trasforma in `snapshot_persisted: False` (non un'eccezione di
trasporto). Su origin/main 515813f (verifica c.6371809453) l'esito era trattato come certo: riga SUBMITTED, dopo
un crash ordine sparito e riserva liberata, cancellazione confermata e poi annullata dal riavvio.

Comportamento richiesto (mandato owner #545): AMBIGUOUS + riserva e chiavi trattenute + riconciliazione; nessuna
conferma di cancel non durevole; nessuna nuova esposizione finche' il broker SIM non torna durevole.
"""
from __future__ import annotations

import sqlite3

import pytest

from core.order_router import OrderRouter
from core.trading_constants import AMBIGUITY_PERSISTED_NOT_CONFIRMED
from core.trading_engine import ExecutionError, TradingEngine
from cashout_cancel_adapter import response_confirms_cancel
from simulation_broker import SimulationBroker
from tests.acceptance.test_issue437_pr27 import _catena
from tests.integration.test_issue461_pr26a_customer_ref_provenance import _signal

pytestmark = [pytest.mark.integration, pytest.mark.safety]

MARKET = "1.234"


# ----------------------------------------------------------------------------- harness
def _book(broker, market_id=MARKET, back=1.5, lay=4.0, size=1000.0, status="OPEN", sel=11):
    return broker.update_market_book(market_id, {"status": status, "runners": [{
        "selectionId": sel, "status": "ACTIVE",
        "ex": {"availableToBack": [{"price": back, "size": size}],
               "availableToLay": [{"price": lay, "size": size}]}}]})


def _chain_with_durable_broker(tmp_path):
    """La catena di #524/#437 con il broker SIM che scrive sullo STESSO Database del runtime."""
    c = _catena(tmp_path)
    broker = SimulationBroker(starting_balance=1000.0, db=c.db)
    c.rc.betfair_service.simulation_broker = broker
    svc = c.rc.betfair_service
    svc.get_simulation_broker = lambda: svc.simulation_broker
    svc.is_simulation_mode = lambda: True
    c.broker = broker
    c.rc.set_simulation_mode(True)     # come headless_main in SIM (TTL/cashout usano il broker simulato)
    _book(broker)                      # back 1.5 < 2.0: l'ordine BACK a 2.0 resta a riposo (EXECUTABLE)
    return c


class _SnapshotFault:
    """Errore del DB SOLO sulla scrittura dello snapshot di reload (save_simulation_state)."""

    def __init__(self, db):
        self.db = db
        self.original = db.save_simulation_state
        self.active = False
        self.calls = 0

    def __call__(self, state_key, state):
        self.calls += 1
        if self.active:
            raise sqlite3.OperationalError("database is locked")
        return self.original(state_key, state)


def _inject(c):
    fault = _SnapshotFault(c.db)
    c.db.save_simulation_state = fault
    return fault


def _crash_and_restart(c):
    """Il processo muore: nuovo broker dallo snapshot su disco, nuovo engine e runtime sullo stesso DB."""
    from types import SimpleNamespace
    from tests.integration.test_issue461_pr26a_customer_ref_provenance import (
        _BetfairService, _Executor, _Settings, _SyncBus)
    from core.runtime_controller import RuntimeController
    from core.system_state import RuntimeMode

    broker = SimulationBroker(starting_balance=1000.0, db=c.db)
    persisted = c.db.load_simulation_state("default")
    if persisted:
        broker.state.load_from_dict(persisted)
    bus = _SyncBus()
    svc = _BetfairService(broker)
    svc.get_simulation_broker = lambda: svc.simulation_broker
    svc.is_simulation_mode = lambda: True
    engine = TradingEngine(bus=bus, db=c.db, client_getter=svc.get_client, executor=_Executor())
    rc = RuntimeController(bus=bus, db=c.db, settings_service=_Settings(True), betfair_service=svc,
                           telegram_service=c.rc.telegram_service, trading_engine=engine)
    engine.runtime_controller = rc
    rc.mode = RuntimeMode.ACTIVE          # money management VERO (nessun override di calculate)
    rc.set_simulation_mode(True)
    rc.risk_desk.bankroll_current = 1000.0
    rc.risk_desk.equity_peak = 1000.0
    import time as _time
    rc._applica_fondi_avvio(svc.get_account_funds(), read_at=_time.monotonic())  # cassa verificata (PFC-008)
    rebuild = rc._rebuild_open_positions()
    return SimpleNamespace(bus=bus, broker=broker, engine=engine, rc=rc, db=c.db, rebuild=rebuild)


def _db_rows(c):
    rows = c.db._execute("SELECT customer_ref, status, response_json FROM orders ORDER BY id", fetch=True,
                         commit=False) or []
    return [dict(r) for r in rows]


def _occupied(c):
    return [t.table_id for t in c.rc.table_manager._tables.values() if t.status != "FREE"]


def _events(c, *topics):
    return [(t, p) for t, p in c.bus.events if t in topics]


def _funds(b):
    f = b.get_account_funds()
    return round(f["available"], 6), round(f["exposure"], 6)



def _rejections(c):
    return [p.get("reason") for t, p in c.bus.events if t == "SIGNAL_REJECTED" and isinstance(p, dict)]


def _other_market_signal(**over):
    campi = {"market_id": "1.555", "selection_id": 22, "event": "Gamma v Delta",
             "received_at": "2026-10-08T10:07:00+00:00"}
    return _signal(**{**campi, **over})


# ----------------------------------------------------------------------------- 0. controllo (snapshot OK)
def test_control_place_without_fault_survives_restart_and_table_is_rebuilt(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    c.rc._on_signal_received(_signal())
    assert len(c.broker.state.orders) == 1
    assert [r["status"] for r in _db_rows(c)] == ["SUBMITTED"]
    assert c.broker.is_durable() is True
    r = _crash_and_restart(c)
    assert len(r.broker.state.orders) == 1
    assert r.rebuild["ok"] is True and len(r.rebuild["rebuilt"]) == 1
    assert _occupied(r)


# ----------------------------------------------------------------------------- 1. place ordinario
def test_place_snapshot_unpersisted_is_ambiguous_not_certain(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True

    c.rc._on_signal_received(_signal())

    assert len(c.broker.state.orders) == 1                         # ordine vivo in memoria
    rows = _db_rows(c)
    assert [r["status"] for r in rows] == ["AMBIGUOUS"]            # mai un esito certo
    assert _events(c, "QUICK_BET_AMBIGUOUS")
    assert not _events(c, "QUICK_BET_SUCCESS", "QUICK_BET_FAILED")
    assert _occupied(c)                                            # riserva trattenuta
    assert c.broker.is_durable() is False


def test_place_snapshot_unpersisted_then_crash_reserve_is_held_as_uncertain(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())
    ref = _db_rows(c)[0]["customer_ref"]

    r = _crash_and_restart(c)

    assert r.broker.state.orders == {}                             # il broker riparte dallo snapshot precedente
    assert ref in r.rebuild["uncertain"]                           # incerto, non "chiuso"
    assert ref not in r.rebuild["closed"]
    assert _occupied(r)                                            # riserva trattenuta fino alla riconciliazione
    assert [x["status"] for x in _db_rows(r)] == ["AMBIGUOUS"]


def test_place_unpersisted_after_restart_same_intent_is_not_resent(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())
    r = _crash_and_restart(c)

    r.rc._on_signal_received(_signal())                            # riconsegna dello stesso messaggio

    assert r.broker.state.orders == {}                             # mai un reinvio cieco
    assert len(_db_rows(r)) == 1


def test_place_unpersisted_after_restart_event_stays_held(tmp_path):
    """Prima: la chiave dell'evento era libera e un intento diverso sullo stesso evento entrava."""
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())
    r = _crash_and_restart(c)

    r.rc._on_signal_received(_signal(price=2.2, received_at="2026-10-08T10:05:00+00:00"))

    assert r.broker.state.orders == {}                             # evento trattenuto dalla riserva incerta
    assert len(_db_rows(r)) == 1
    assert r.rc.risk_desk.cash_equity_current is not None          # non e' un rifiuto per cassa non verificata


def test_control_after_clean_restart_a_new_intent_on_another_event_enters(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    r = _crash_and_restart(c)
    r.rc._on_signal_received(_other_market_signal())
    assert len(r.broker.state.orders) == 1


# ----------------------------------------------------------------------------- 2. gate di durabilita'
def test_no_new_entry_while_broker_is_not_durable_then_heals(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())                            # snapshot non scritto: broker non durevole
    assert c.broker.is_durable() is False

    c.rc._on_signal_received(_other_market_signal())               # guasto ancora attivo
    assert len(c.broker.state.orders) == 1                         # nessuna nuova esposizione
    assert len(_db_rows(c)) == 1                                   # negato prima di persistere l'intento
    assert [p.get("status") for t, p in c.bus.events if t == "QUICK_BET_FAILED"] == ["DENIED"]

    fault.active = False                                           # il DB torna scrivibile
    c.rc._on_signal_received(_other_market_signal(price=2.4, received_at="2026-10-08T10:09:00+00:00"))
    assert c.broker.is_durable() is True                           # il gate ha ritentato lo snapshot
    assert len(c.broker.state.orders) == 2                         # l'ingresso passa
    r = _crash_and_restart(c)
    assert len(r.broker.state.orders) == 2                         # e lo snapshot ripara anche il primo ordine


def test_engine_denies_entry_with_explicit_reason_when_not_durable(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    assert c.broker.persist_snapshot() is False
    assert c.engine._sim_durability_admission_block() == "SIM_SNAPSHOT_NON_DUREVOLE"
    fault.active = False
    assert c.engine._sim_durability_admission_block() == ""


# ----------------------------------------------------------------------------- 3. batch
def test_batch_snapshot_unpersisted_is_ambiguous_for_the_engine(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    res = c.broker.place_orders(market_id="1.9", instructions=[
        {"selection_id": 1, "side": "BACK", "price": 3.0, "size": 30.0},
        {"selection_id": 2, "side": "BACK", "price": 3.0, "size": 30.0}])
    assert res["status"] == "SUCCESS" and res["snapshot_persisted"] is False
    with pytest.raises(ExecutionError) as exc:
        TradingEngine._raise_if_failed_semantic_response(c.engine, res)
    assert exc.value.ambiguity_reason == AMBIGUITY_PERSISTED_NOT_CONFIRMED


# ----------------------------------------------------------------------------- 4. cancel
@pytest.mark.parametrize("market_wide", [False, True])
def test_cancel_with_snapshot_unpersisted_is_not_confirmed(tmp_path, market_wide):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    c.rc._on_signal_received(_signal())                            # snapshot OK
    bet = next(iter(c.broker.state.orders))
    fault.active = True
    if market_wide:
        res = c.broker.cancel_orders(market_id=MARKET)
        assert res["snapshot_persisted"] is False
        assert response_confirms_cancel(res) is False
    else:
        confirmed, not_attempted = c.rc._direct_ttl_cancel_adapter().cancel_with_outcome(MARKET, [bet])
        assert confirmed is False and not_attempted == ""          # prima: True
    assert c.broker.is_durable() is False


def test_cancel_with_durable_snapshot_is_still_confirmed(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    c.rc._on_signal_received(_signal())
    bet = next(iter(c.broker.state.orders))
    confirmed, _ = c.rc._direct_ttl_cancel_adapter().cancel_with_outcome(MARKET, [bet])
    assert confirmed is True


# ----------------------------------------------------------------------------- 5. replace / fill / settlement
@pytest.mark.parametrize("operazione", ["replace", "fill", "settlement"])
def test_broker_side_operations_unpersisted_block_new_entries_until_healed(tmp_path, operazione):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    c.rc._on_signal_received(_signal())
    bet = next(iter(c.broker.state.orders))
    if operazione == "settlement":
        _book(c.broker, back=2.5)                                  # fill, snapshot OK
    fault.active = True
    if operazione == "replace":
        esito = c.broker.replace_orders(market_id=MARKET, bet_id=bet, new_price=2.1)
    elif operazione == "fill":
        esito = _book(c.broker, back=2.5)
    else:
        esito = c.broker.update_market_book(MARKET, {"status": "CLOSED",
                                                     "runners": [{"selectionId": 11, "status": "WINNER"}]})
    assert esito["snapshot_persisted"] is False
    assert c.broker.is_durable() is False
    prima = len(c.broker.state.orders)
    c.rc._on_signal_received(_other_market_signal())
    assert len(c.broker.state.orders) == prima                     # nessuna nuova esposizione su stato non durevole
    fault.active = False
    assert c.broker.ensure_durable() is True                       # una scrittura riuscita ripara il buco


# ----------------------------------------------------------------------------- 6. hedge del cashout (OrderRouter)
def test_order_router_sim_hedge_with_snapshot_unpersisted_is_ambiguous():
    raw = {"status": "SUCCESS", "snapshot_persisted": False,
           "instructionReports": [{"status": "SUCCESS", "betId": "B1", "sizeMatched": 2.0}]}
    out = OrderRouter._normalize_sim(raw)
    assert out["status"] == "AMBIGUOUS" and out["order_unknown"] is True and out["ok"] is False
    assert out["placed"] is True and out["bet_id"] == "B1"          # bet registrata: niente retry duplicato
    durable = OrderRouter._normalize_sim({k: v for k, v in raw.items() if k != "snapshot_persisted"})
    assert durable["status"] == "SUCCESS" and durable["ok"] is True


# ----------------------------------------------------------------------------- 7. auto-riparazione
def test_later_successful_snapshot_heals_the_previous_unpersisted_operation(tmp_path):
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())
    fault.active = False
    c.broker.place_bet(market_id="1.77", selection_id=1, side="BACK", price=5.0, size=1.0)
    assert c.broker.is_durable() is True
    r = _crash_and_restart(c)
    assert len(r.broker.state.orders) == 2


def test_production_readers_of_snapshot_persisted():
    """Prova statica: i moduli di produzione che interpretano il flag."""
    import pathlib
    root = pathlib.Path(__file__).resolve().parents[2]
    readers = []
    for p in root.rglob("*.py"):
        rel = p.relative_to(root).as_posix()
        if rel.startswith(("tests/", ".venv", "venv")) or "/site-packages/" in rel:
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        if "snapshot_persisted" in text:
            readers.append(rel)
    # core/microstake_live_port.py: dichiara solo che l'exchange LIVE non ha uno snapshot locale (sempre True).
    assert sorted(readers) == ["cashout_cancel_adapter.py", "core/microstake_live_port.py", "core/microstake_safe.py",
                               "core/microstake_sim_port.py",
                               "core/order_router.py", "core/trading_engine.py", "simulation_broker.py"]


# ----------------------------------------------------------------------------- 8. riconsegna (rilievo Codex P1 su 6c2d462)
def test_replay_of_ambiguous_command_while_not_durable_keeps_the_original_reservation(tmp_path):
    """La riconsegna dello STESSO comando, con il broker ancora non durevole, passa dal percorso duplicato: il gate di
    durabilita' non deve emettere un DENIED terminale che rilascerebbe la riserva incerta dell'originale."""
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    fault.active = True
    c.rc._on_signal_received(_signal())
    (cmd,) = c.bus.payloads("CMD_QUICK_BET")
    occupati, incerti = _occupied(c), dict(c.rc._uncertain_reservations_map())
    assert occupati and incerti

    c.bus.publish("CMD_QUICK_BET", dict(cmd))                     # riconsegna con il guasto ancora attivo

    assert _occupied(c) == occupati                                # riserva dell'originale trattenuta
    assert dict(c.rc._uncertain_reservations_map()) == incerti     # incertezza intatta
    assert len(c.broker.state.orders) == 1                         # nessun secondo invio
    assert [r["status"] for r in _db_rows(c)] == ["AMBIGUOUS", "DUPLICATE_BLOCKED"]   # traccia del doppione


# ----------------------------------------------------------------------------- 9. rilievi su 6c2d462 e audit #551
class _RecordingLock:
    """RLock reale che registra il flag di durabilita' a ogni rilascio: cio' che vede il primo lettore successivo."""

    def __init__(self, broker):
        import threading
        self._inner = threading.RLock()
        self.broker = broker
        self.on_release = []

    def __enter__(self):
        self._inner.acquire()
        return self

    def __exit__(self, *exc):
        self.on_release.append(getattr(self.broker, "_snapshot_durevole", None))
        self._inner.release()
        return False

    def acquire(self, *a, **k):
        return self._inner.acquire(*a, **k)

    def release(self):
        self.on_release.append(getattr(self.broker, "_snapshot_durevole", None))
        self._inner.release()


def test_failed_snapshot_flag_is_set_before_the_lock_is_released(tmp_path):
    """Rilievo Grok su 6c2d462: fra la scrittura fallita e il flag nessun lettore deve poter vedere «durevole»."""
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    lock = _RecordingLock(c.broker)
    c.broker._lock = lock
    fault.active = True
    assert c.broker._salva_snapshot() is False
    assert lock.on_release[0] is False                             # prima: True al primo rilascio
    assert c.broker.is_durable() is False


@pytest.mark.parametrize("wrapper", ["top", "nested"])
def test_cancel_unpersisted_in_a_nested_container_is_not_confirmed(wrapper):
    """Rilievo Sol su 6c2d462: il flag annidato sotto `result` (wrapper) non deve lasciar passare una conferma."""
    inner = {"status": "SUCCESS", "instructionReports": [{"status": "SUCCESS", "betId": "B1", "sizeCancelled": 2.0}]}
    durable = {"ok": True, "result": dict(inner)}
    assert response_confirms_cancel(durable) is True                # controllo: senza flag la conferma regge
    if wrapper == "top":
        res = {"ok": True, "snapshot_persisted": False, "result": dict(inner)}
    else:
        res = {"ok": True, "result": dict(inner, snapshot_persisted=False)}
    assert response_confirms_cancel(res) is False                   # prima: True con il flag annidato


def _microstake_engine():
    from tests.test_microstake_engine_native import _engine, _payload
    engine, broker, _db = _engine()
    return engine, broker, _payload


def test_microstake_intermediate_snapshot_repaired_by_the_final_one_is_not_ambiguous():
    """Audit PF-FINANCIAL-IMPACT-GATE-01 su #551: snapshot intermedio fallito, finale scritto (stato intero) =>
    saga ACTIVE e broker durevole; l'engine non deve trasformarlo in AMBIGUOUS trattenendo tavolo e chiavi."""
    engine, broker, payload = _microstake_engine()
    calls, orig = [], broker.persist_snapshot
    broker.persist_snapshot = lambda: (calls.append(1), False if len(calls) == 1 else orig())[1]
    res = engine.submit_quick_bet(payload("ms-int", size=0.10, price=3.0))
    saga = engine.microstake_executor.store.load("ms-int")
    assert saga["phase"] == "ACTIVE" and broker.is_durable() is True
    assert res.get("status") != "AMBIGUOUS", res                    # prima: AMBIGUOUS (SIM_SNAPSHOT_NOT_PERSISTED)


def test_microstake_final_snapshot_unpersisted_stays_ambiguous_for_the_engine():
    engine, _broker, _ = _microstake_engine()
    for res in ({"status": "SUCCESS", "snapshot_persisted": False, "snapshot_final_persisted": False},
                {"status": "SUCCESS", "snapshot_persisted": True, "snapshot_final_persisted": False},
                {"status": "SUCCESS", "snapshot_persisted": False}):
        with pytest.raises(ExecutionError) as exc:
            TradingEngine._raise_if_failed_semantic_response(engine, res)
        assert exc.value.ambiguity_reason == AMBIGUITY_PERSISTED_NOT_CONFIRMED, res
    ok = {"status": "SUCCESS", "betId": "MS1", "snapshot_persisted": False, "snapshot_final_persisted": True}
    TradingEngine._raise_if_failed_semantic_response(engine, ok)  # finale scritto: nessuna eccezione


@pytest.mark.parametrize("entry_snapshot_ok", [True, False])
def test_concurrent_unpersisted_fill_after_the_gate_is_covered_by_the_entry_own_snapshot(tmp_path, entry_snapshot_ok):
    """Rilievo Sol su 6c2d462 (gate non atomico col piazzamento): un fill concorrente fallisce lo snapshot DOPO
    `ensure_durable()`. L'ingresso gia' ammesso scrive lo snapshot dello stato INTERO: se riesce ripara anche il
    fill (nulla si perde al riavvio); se fallisce l'ingresso e' AMBIGUOUS, mai certo."""
    c = _chain_with_durable_broker(tmp_path)
    fault = _inject(c)
    c.rc._on_signal_received(_signal())                            # ordine a riposo, snapshot OK
    orig, chiamate = c.broker.ensure_durable, []

    def ensure_then_concurrent_fill():
        chiamate.append(1)
        ok = orig()
        fault.active = True
        fill = _book(c.broker, back=2.5)                           # fill concorrente nella finestra
        assert fill["snapshot_persisted"] is False
        fault.active = not entry_snapshot_ok
        return ok

    c.broker.ensure_durable = ensure_then_concurrent_fill
    c.rc._on_signal_received(_other_market_signal())
    stati = [r["status"] for r in _db_rows(c)]
    assert chiamate == [1]                                         # il gate e' passato davvero dalla finestra
    assert len(c.broker.state.orders) == 2                         # l'ingresso ammesso e' stato inviato
    if entry_snapshot_ok:
        assert c.broker.is_durable() is True and "AMBIGUOUS" not in stati
        fault.active = False
        r = _crash_and_restart(c)
        assert r.broker.state.to_dict()["orders"] == c.broker.state.to_dict()["orders"]   # fill e ingresso salvi
    else:
        assert c.broker.is_durable() is False and stati[-1] == "AMBIGUOUS"
