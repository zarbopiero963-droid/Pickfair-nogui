import pytest


VALID_STATUSES = {
    "CREATED", "SUBMITTED", "PLACED", "PARTIAL",
    "MATCHED", "FAILED", "CANCELLED", "LAPSED", "VOIDED"
}


def assert_no_invalid_status(engine, batch_id):
    legs = engine.batch_manager.get_batch_legs(batch_id)

    for leg in legs:
        assert leg["status"] in VALID_STATUSES


def assert_no_orphan_legs(engine, batch_id):
    batch = engine.batch_manager.get_batch(batch_id)
    legs = engine.batch_manager.get_batch_legs(batch_id)

    assert batch is not None
    assert len(legs) > 0


def assert_terminal_consistency(engine, batch_id):
    batch = engine.batch_manager.get_batch(batch_id)
    legs = engine.batch_manager.get_batch_legs(batch_id)

    if batch["status"] in {"EXECUTED", "FAILED"}:
        assert all(
            l["status"] in {"MATCHED", "FAILED", "CANCELLED"}
            for l in legs
        )


# =========================================================
# 🔥 GLOBAL INVARIANTS TEST
# =========================================================

def test_global_invariants(engine, batch):
    for _ in range(20):
        engine.reconcile_batch(batch["batch_id"])

        assert_no_invalid_status(engine, batch["batch_id"])
        assert_no_orphan_legs(engine, batch["batch_id"])
        assert_terminal_consistency(engine, batch["batch_id"])