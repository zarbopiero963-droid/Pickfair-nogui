import pytest
import time


def test_reconciliation_10000_cycles(engine):
    start = time.time()

    for _ in range(10000):
        engine.reconcile_batch("B1")

    duration = time.time() - start

    # HARD LIMIT: deve stare sotto 3 secondi
    assert duration < 3.0