import copy


def test_replay_is_deterministic(engine, batch):
    snapshots = []

    # run 1
    for _ in range(5):
        engine.reconcile_batch(batch["batch_id"])
        snapshots.append(copy.deepcopy(
            engine.batch_manager.get_batch_legs(batch["batch_id"])
        ))

    # reset engine
    from core.reconciliation_engine import ReconciliationEngine
    new_engine = ReconciliationEngine(
        db=engine.db,
        batch_manager=engine.batch_manager,
        client_getter=engine.client_getter,
    )

    snapshots2 = []

    # run 2 (identico)
    for _ in range(5):
        new_engine.reconcile_batch(batch["batch_id"])
        snapshots2.append(copy.deepcopy(
            new_engine.batch_manager.get_batch_legs(batch["batch_id"])
        ))

    assert snapshots == snapshots2