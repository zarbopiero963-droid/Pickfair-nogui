import pytest

from core.reconciliation_engine import ReconciliationEngine
from tests.reconciliation.test_reconciliation_hardening import FakeBatchManager, FakeClient, FakeDB


@pytest.fixture
def engine():
    db = FakeDB()
    bm = FakeBatchManager()
    bm.create_batch("B1")
    return ReconciliationEngine(
        db=db,
        batch_manager=bm,
        client_getter=lambda: FakeClient(),
    )
