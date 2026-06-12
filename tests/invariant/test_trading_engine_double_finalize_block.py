"""PR-B programma test hedge-fund grade: invarianti hard del lifecycle ordini.

Gap chiusi (guard di produzione ESISTENTI ma mai testati prima):
- doppia finalize bloccata per TUTTI e 5 gli status terminali
  (ORDER_ALREADY_FINALIZED, 25 coppie)
- finalize su stato DB non terminale bloccata (FINALIZE_ON_NON_TERMINAL_DB_STATE)
- status non terminale/farlocco respinto (NON_TERMINAL_STATUS)
- "transizione" vietata FAILED->COMPLETED a livello engine: un ordine gia'
  finalizzato FAILED non puo' essere ri-finalizzato COMPLETED

Dedup verificato: transizioni FSM order_manager, terminali senza uscita,
AMBIGUOUS senza reason, COMPLETED con ambiguity e rollback sono GIA' coperti
in tests/invariant/test_order_manager_state_machine.py e
test_trading_engine_finalize_invariants.py — qui solo signal nuovo.
"""
from __future__ import annotations

import pytest

from core.trading_engine import TradingEngine, _ExecutionContext


class FakeBus:
    def __init__(self):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, topic, handler):
        self.subscriptions[topic] = handler

    def publish(self, name, payload):
        self.events.append((name, payload))


class FakeDB:
    """DB fake con stato ordine controllabile per i guard di finalize."""

    def __init__(self, order: dict | None = None):
        self.audit_events = []
        self.order = order

    @staticmethod
    def is_ready():
        return True

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    @staticmethod
    def order_exists_inflight(*, customer_ref, correlation_id):
        return False

    @staticmethod
    def load_pending_customer_refs():
        return []

    @staticmethod
    def load_pending_correlation_ids():
        return []

    def get_order(self, order_id):
        _ = order_id
        return self.order


class InlineExecutor:
    @staticmethod
    def is_ready():
        return True

    @staticmethod
    def submit(_name, fn):
        return fn()


def _engine(db: FakeDB) -> tuple[TradingEngine, _ExecutionContext, dict]:
    engine = TradingEngine(
        bus=FakeBus(),
        db=db,
        client_getter=lambda: None,
        executor=InlineExecutor(),
    )
    ctx = _ExecutionContext(
        correlation_id="CID-FSM-HARD-1",
        customer_ref="REF-FSM-HARD-1",
        created_at=0.0,
    )
    return engine, ctx, engine._new_audit(ctx)


_ALL_TERMINAL = ["COMPLETED", "FAILED", "DENIED", "AMBIGUOUS", "DUPLICATE_BLOCKED"]


@pytest.mark.invariant
@pytest.mark.parametrize("first_status", _ALL_TERMINAL)
def test_double_finalize_is_blocked_for_every_terminal_status(first_status):
    """Un ordine gia' finalizzato non puo' MAI essere ri-finalizzato,
    per TUTTI e 5 gli status terminali del contratto (25 coppie):
    copre anche le 'transizioni' vietate tipo FAILED->COMPLETED."""
    db = FakeDB(order={"finalized": True, "status": first_status})
    engine, ctx, audit = _engine(db)

    # payload coerenti con gli invariant terminali (che girano PRIMA
    # del guard di doppia finalize): AMBIGUOUS richiede reason, DENIED
    # non puo' portare errore tecnico.
    second_attempts = [
        ("COMPLETED", None, None),
        ("FAILED", "boom", None),
        ("DENIED", None, None),
        ("AMBIGUOUS", None, "timeout_after_submit"),
        ("DUPLICATE_BLOCKED", None, None),
    ]
    for second_status, error, ambiguity in second_attempts:
        with pytest.raises(RuntimeError, match="ORDER_ALREADY_FINALIZED"):
            engine._finalize(
                ctx=ctx,
                audit=audit,
                order_id="ORD-1",
                status=second_status,
                outcome="SUCCESS",
                error=error,
                ambiguity_reason=ambiguity,
            )


@pytest.mark.invariant
@pytest.mark.parametrize("db_status", ["INFLIGHT", "SUBMITTED", "PENDING"])
def test_finalize_on_non_terminal_db_state_is_blocked(db_status):
    """Il DB e' la verita': se l'ordine non e' in stato terminale sul DB,
    la finalize viene rifiutata (niente COMPLETED 'in anticipo')."""
    db = FakeDB(order={"finalized": False, "status": db_status})
    engine, ctx, audit = _engine(db)

    with pytest.raises(RuntimeError, match="FINALIZE_ON_NON_TERMINAL_DB_STATE"):
        engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id="ORD-2",
            status="COMPLETED",
            outcome="SUCCESS",
        )


@pytest.mark.invariant
@pytest.mark.parametrize("bogus_status", ["DONE", "OK", "", "completed", "MATCHED"])
def test_non_terminal_or_bogus_status_is_blocked(bogus_status):
    """Solo gli status terminali del contratto sono finalizzabili: tutto
    il resto (case sbagliati, status del layer order_manager, spazzatura)
    viene respinto da _assert_terminal_status. NOTA: il secondo guard
    UNKNOWN_STATUS_IN_LIFECYCLE e' oggi irraggiungibile (ogni status
    terminale ha un mapping outcome) - difesa in profondita'."""
    engine, ctx, audit = _engine(FakeDB())

    with pytest.raises(RuntimeError, match="NON_TERMINAL_STATUS"):
        engine._finalize(
            ctx=ctx,
            audit=audit,
            order_id=None,
            status=bogus_status,
            outcome="SUCCESS",
        )


@pytest.mark.invariant
def test_finalize_on_terminal_db_state_with_flag_false_is_allowed_once():
    """Caso PASS di controllo: ordine su stato DB terminale e non ancora
    finalizzato -> la finalize procede (il guard non e' troppo largo)."""
    db = FakeDB(order={"finalized": False, "status": "COMPLETED"})
    engine, ctx, audit = _engine(db)

    result = engine._finalize(
        ctx=ctx,
        audit=audit,
        order_id="ORD-3",
        status="COMPLETED",
        outcome="IGNORED_BY_DESIGN",  # _finalize ri-deriva l'outcome dallo status
    )
    assert isinstance(result, dict)
    assert result["status"] == "COMPLETED"
    # L'outcome viene dal mapping _STATUS_TO_OUTCOME, non dal parametro:
    # se il mapping driftasse, questa asserzione lo cattura.
    assert result["outcome"] == "SUCCESS"
    assert result["ok"] is True
    assert result["is_terminal"] is True
    assert result["finalization_persisted"] is True
    # Effetto collaterale: la finalize legittima lascia traccia nell'audit.
    assert db.audit_events, "la finalize deve persistere eventi di audit"
