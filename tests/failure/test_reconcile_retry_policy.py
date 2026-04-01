from __future__ import annotations

import time
import pytest

from core.reconciliation_engine import ReconciliationEngine, ReconcileConfig


# =========================================================
# FAKES
# =========================================================

class FakeDB:
    def persist_decision_log(self, *args, **kwargs):
        pass


class FakeBatchManager:
    def get_batch(self, _):
        return {"batch_id": "B400", "market_id": "1.400", "status": "LIVE"}

    def get_batch_legs(self, _):
        return [{"leg_index": 0, "status": "SUBMITTED", "customer_ref": "R1"}]

    def update_leg_status(self, **kwargs):
        pass

    def recompute_batch_status(self, _):
        return {"status": "LIVE"}

    def release_runtime_artifacts(self, **kwargs):
        pass


# =========================================================
# ENGINE WITH CONTROLLED FAILURES
# =========================================================

class RetryEngine(ReconciliationEngine):
    def __init__(self, *args, fail_sequence=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.calls = 0
        self.fail_sequence = fail_sequence or []

    def _fetch_current_orders_by_market(self, market_id: str, *, _attempt: int = 0):
        if self.calls < len(self.fail_sequence):
            exc = self.fail_sequence[self.calls]
            self.calls += 1
            raise exc

        return []


# =========================================================
# TESTS
# =========================================================

def build_engine(fail_sequence):
    return RetryEngine(
        db=FakeDB(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: None,
        config=ReconcileConfig(
            max_transient_retries=3,
            transient_retry_base_delay=0.01,
            transient_retry_max_delay=0.05,
        ),
        fail_sequence=fail_sequence,
    )


# ---------------------------------------------------------
# 1. RETRY ON TIMEOUT
# ---------------------------------------------------------

def test_retry_on_timeout():
    engine = build_engine([TimeoutError(), TimeoutError()])

    result = engine.reconcile_batch("B400")

    assert result["ok"] is True
    assert engine.calls >= 2


# ---------------------------------------------------------
# 2. RETRY ON CONNECTION ERROR
# ---------------------------------------------------------

def test_retry_on_connection_error():
    engine = build_engine([ConnectionError(), ConnectionError()])

    result = engine.reconcile_batch("B400")

    assert result["ok"] is True


# ---------------------------------------------------------
# 3. NO RETRY ON PERMANENT ERROR
# ---------------------------------------------------------

def test_no_retry_on_invalid_market():
    class PermanentError(Exception):
        pass

    engine = build_engine([PermanentError()])

    # deve fallire subito, non retry
    with pytest.raises(PermanentError):
        engine.reconcile_batch("B400")


# ---------------------------------------------------------
# 4. BACKOFF GROWTH
# ---------------------------------------------------------

def test_retry_backoff_growth():
    delays = []

    original_sleep = time.sleep

    def fake_sleep(d):
        delays.append(d)

    time.sleep = fake_sleep

    engine = build_engine([TimeoutError(), TimeoutError(), TimeoutError()])

    engine.reconcile_batch("B400")

    time.sleep = original_sleep

    assert delays[1] > delays[0]


# ---------------------------------------------------------
# 5. MAX RETRY RESPECTED
# ---------------------------------------------------------

def test_max_retry_respected():
    engine = build_engine([
        TimeoutError(),
        TimeoutError(),
        TimeoutError(),
        TimeoutError(),  # oltre il limite
    ])

    result = engine.reconcile_batch("B400")

    # non deve loopare infinito
    assert engine.calls <= 4
    assert result["ok"] is True