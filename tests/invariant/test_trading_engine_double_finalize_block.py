"""PR-B programma test hedge-fund grade: invarianti hard del lifecycle ordini.

Gap chiusi (guard di produzione ESISTENTI ma mai testati prima):
- doppia finalize bloccata (ORDER_ALREADY_FINALIZED)
- finalize su stato DB non terminale bloccata (FINALIZE_ON_NON_TERMINAL_DB_STATE)
- status sconosciuto nel lifecycle bloccato (UNKNOWN_STATUS_IN_LIFECYCLE)
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

    def is_ready(self):
        return True

    def insert_audit_event(self, event):
        self.audit_events.append(event)

    def order_exists_inflight(self, *, customer_ref, correlation_id):
        return False

    def load_pending_customer_refs(self):
        return []

    def load_pending_correlation_ids(self):
        return []

    def get_order(self, order_id):
        _ = order_id
        return self.order


class InlineExecutor:
    def is_ready(self):
        return True

    def submit(self, _name, fn):
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


@pytest.mark.invariant
@pytest.mark.parametrize("first_status", ["COMPLETED", "FAILED", "AMBIGUOUS"])
def test_double_finalize_is_blocked_for_every_terminal_status(first_status):
    """Un ordine gia' finalizzato non puo' MAI essere ri-finalizzato:
    copre anche le 'transizioni' vietate FAILED->COMPLETED e
    COMPLETED->FAILED a livello engine."""
    db = FakeDB(order={"finalized": True, "status": first_status})
    engine, ctx, audit = _engine(db)

    for second_status, ambiguity in [
        ("COMPLETED", None),
        ("FAILED", None),
        ("AMBIGUOUS", "timeout_after_submit"),
    ]:
        with pytest.raises(RuntimeError, match="ORDER_ALREADY_FINALIZED"):
            engine._finalize(
                ctx=ctx,
                audit=audit,
                order_id="ORD-1",
                status=second_status,
                outcome="SUCCESS" if second_status == "COMPLETED" else second_status,
                error="boom" if second_status == "FAILED" else None,
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
def test_unknown_status_in_lifecycle_is_blocked(bogus_status):
    """Solo gli status del contratto sono finalizzabili: tutto il resto
    (inclusi case sbagliati e status del layer order_manager) esplode.
    Due guard in cascata: _assert_terminal_status (NON_TERMINAL_STATUS)
    e il mapping del lifecycle (UNKNOWN_STATUS_IN_LIFECYCLE)."""
    engine, ctx, audit = _engine(FakeDB())

    with pytest.raises(RuntimeError, match="UNKNOWN_STATUS_IN_LIFECYCLE|NON_TERMINAL_STATUS"):
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
        outcome="SUCCESS",
    )
    assert isinstance(result, dict)
    assert result.get("status") == "COMPLETED"
