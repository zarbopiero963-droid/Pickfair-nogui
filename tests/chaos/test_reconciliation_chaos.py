import random
import time
import pytest


class ChaosClient:
    def __init__(self, seed=42):
        self.rng = random.Random(seed)

    def get_current_orders(self, **kwargs):
        roll = self.rng.random()

        # 20% timeout
        if roll < 0.2:
            raise TimeoutError("timeout")

        # 10% auth error
        if roll < 0.3:
            raise Exception("401 unauthorized")

        # 10% permanent error
        if roll < 0.4:
            raise Exception("invalid market")

        # 60% ok response
        return [
            {
                "betId": "B123",
                "customerOrderRef": "REF1",
                "selectionId": "1",
                "marketId": "1.100",
                "status": "EXECUTION_COMPLETE",
                "sizeMatched": 10,
                "sizeRemaining": 0,
            }
        ]


@pytest.fixture
def chaos_engine():
    from core.reconciliation_engine import ReconciliationEngine
    from tests.reconciliation.test_reconciliation_hardening import FakeDB, FakeBatchManager

    db = FakeDB()
    bm = FakeBatchManager()
    bm.create_batch("BCHAOS")

    return ReconciliationEngine(
        db=db,
        batch_manager=bm,
        client_getter=lambda: ChaosClient(),
    )


# =========================================================
# 🔥 CHAOS TEST — NON DEVE CRASHARE MAI
# =========================================================

def test_reconciliation_survives_chaos(chaos_engine):
    for _ in range(50):
        result = chaos_engine.reconcile_batch("BCHAOS")

        assert "ok" in result
        assert result["batch_id"] == "BCHAOS"