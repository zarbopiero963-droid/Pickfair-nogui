"""#461 PR29-b — un lotto Dutching a mercato non libera la riserva (PF-C29-01.17).

Contratto PF-C29-01.17 (#506 c.6123640661): un lotto le cui gambe sono PLACED (accettate, non abbinate) mantiene
l'intera riserva; il rilascio avviene solo su esito terminale certo e privo di rischio di OGNI gamba. Oracolo: lotto
di 2 gambe PLACED con sizeMatched=0 -> esposizione del tavolo invariata, tavolo ACTIVE; gambe CANCELLED con
sizeCancelled=size -> rilascio una volta sola. Prima di PR29-b `recompute_batch_status` classificava PLACED/MATCHED
come EXECUTED (terminale) e la riconciliazione liberava tavolo e chiave con le gambe ancora a mercato.
Componenti reali: ReconciliationEngine, DutchingBatchManager, Database (SQLite), TableManager, DuplicationGuard.
"""
from __future__ import annotations

from core.duplication_guard import DuplicationGuard
from core.dutching_batch_manager import DutchingBatchManager
from core.reconciliation_engine import ReconcileConfig, ReconciliationEngine
from core.table_manager import TableManager
from database import Database

EVENT_KEY = "1.777:DUTCH"


class _DB(Database):
    """Database reale + sink del registro decisioni (la riconciliazione e' fail-closed sull'audit)."""

    def persist_decision_log(self, batch_id, entries):
        self.__dict__.setdefault("decisioni", []).extend(entries)


class _Client:
    def __init__(self):
        self.orders = []

    def get_current_orders(self, market_ids=None):
        return [dict(o) for o in self.orders if not market_ids or o["marketId"] in market_ids]


def _ordine(ref, sel, *, stato="EXECUTABLE", abbinato=0.0, residuo=5.0, annullato=0.0):
    return {"customerOrderRef": ref, "betId": f"BET-{ref}", "marketId": "1.777", "selectionId": sel,
            "status": stato, "sizeMatched": abbinato, "sizeRemaining": residuo, "sizeCancelled": annullato}


def _lotto(tmp_path):
    db = _DB(str(tmp_path / "pr29b.db"))
    bm, tm, guard, client = DutchingBatchManager(db), TableManager(table_count=3), DuplicationGuard(), _Client()
    tm.activate(table_id=1, event_key=EVENT_KEY, exposure=10.0, market_id="1.777", selection_id=11, meta={})
    assert guard.acquire(EVENT_KEY)
    bm.create_batch(batch_id="B-29B", event_key=EVENT_KEY, market_id="1.777", table_id=1, total_legs=2,
                    batch_exposure=10.0,
                    legs=[{"leg_index": 0, "customer_ref": "REF-A", "market_id": "1.777", "selection_id": 11,
                           "side": "BACK", "price": 2.0, "stake": 5.0, "liability": 5.0, "status": "SUBMITTED"},
                          {"leg_index": 1, "customer_ref": "REF-B", "market_id": "1.777", "selection_id": 12,
                           "side": "BACK", "price": 2.0, "stake": 5.0, "liability": 5.0, "status": "SUBMITTED"}])
    for leg in bm.get_batch_legs("B-29B"):                          # inviate al trasporto, come in produzione
        bm.update_leg_submission(batch_id="B-29B", leg_index=leg["leg_index"], customer_ref=leg["customer_ref"])
    engine = ReconciliationEngine(db=db, batch_manager=bm, client_getter=lambda: client, table_manager=tm,
                                  duplication_guard=guard,
                                  config=ReconcileConfig(max_convergence_cycles=2, convergence_sleep_secs=0.0,
                                                        transient_retry_base_delay=0.0))
    return engine, bm, tm, guard, client


def _tavolo(tm):
    return tm.get_table(1)


def test_gambe_a_mercato_non_abbinate_mantengono_l_intera_riserva(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11), _ordine("REF-B", 12)]
    engine.reconcile_batch("B-29B")
    assert {leg["status"] for leg in bm.get_batch_legs("B-29B")} == {"PLACED"}
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 10.0, "riserva liberata con gambe a mercato"
    assert EVENT_KEY in guard._active


def test_gambe_abbinate_mantengono_la_riserva_fino_al_settlement(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11, stato="EXECUTION_COMPLETE", abbinato=5.0, residuo=0.0),
                     _ordine("REF-B", 12, stato="EXECUTION_COMPLETE", abbinato=5.0, residuo=0.0)]
    engine.reconcile_batch("B-29B")
    t = _tavolo(tm)
    assert t.current_event_key == EVENT_KEY and t.current_exposure == 10.0, "posizione abbinata uscita dai cap"
    assert EVENT_KEY in guard._active


def test_gambe_annullate_per_intero_rilasciano_una_volta(tmp_path):
    engine, bm, tm, guard, client = _lotto(tmp_path)
    client.orders = [_ordine("REF-A", 11, stato="CANCELLED", residuo=0.0, annullato=5.0),
                     _ordine("REF-B", 12, stato="CANCELLED", residuo=0.0, annullato=5.0)]
    engine.reconcile_batch("B-29B")
    assert _tavolo(tm).current_event_key == ""
    assert EVENT_KEY not in guard._active
    tm.activate(table_id=1, event_key="1.888:ALTRO", exposure=4.0, market_id="1.888", selection_id=1, meta={})
    engine.reconcile_batch("B-29B")                                 # lotto gia' terminale: nessun secondo rilascio
    assert _tavolo(tm).current_event_key == "1.888:ALTRO"
