import time

import pytest

from core.async_db_writer import AsyncDBWriter


class _DBWithAudit:
    def __init__(self):
        self.audit_events = []
        self.bets = []

    def insert_audit_event(self, event):
        self.audit_events.append(dict(event))

    def save_bet(self, **payload):
        self.bets.append(dict(payload))

    def save_cashout_transaction(self, **payload):
        return None

    def save_simulation_bet(self, **payload):
        return None


@pytest.mark.observability
def test_async_db_writer_exposes_trading_engine_write_contract():
    db = _DBWithAudit()
    writer = AsyncDBWriter(db, retry_delay=0.01)

    writer.start()
    assert writer.write({"type": "ORDER_SUBMITTED", "order_id": "o1"}) is True
    writer.stop()

    assert db.audit_events and db.audit_events[0]["type"] == "ORDER_SUBMITTED"


@pytest.mark.observability
def test_async_db_writer_write_contract_is_truthful_when_no_audit_backend():
    class _NoAuditDB:
        def save_bet(self, **payload):
            return None

        def save_cashout_transaction(self, **payload):
            return None

        def save_simulation_bet(self, **payload):
            return None

    db = _NoAuditDB()
    writer = AsyncDBWriter(db, max_retries=0, retry_delay=0.01)

    writer.start()
    assert writer.write({"type": "ORDER_SUBMITTED", "order_id": "o2"}) is True
    writer.stop()

    stats = writer.stats()
    assert stats["failed"] >= 1
