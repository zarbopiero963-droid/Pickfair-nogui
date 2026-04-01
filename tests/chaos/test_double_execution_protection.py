def test_no_double_execution(engine, batch):
    leg = engine.batch_manager.get_batch_legs(batch["batch_id"])[0]

    # simula exchange già matched
    def fake_apply(*args, **kwargs):
        return "MATCHED", engine.ReasonCode.EXCHANGE_WINS_MATCHED, "EXCHANGE"

    engine._apply_merge_policy = fake_apply

    engine.reconcile_batch(batch["batch_id"])
    first = leg["status"]

    engine.reconcile_batch(batch["batch_id"])
    second = leg["status"]

    # NON deve cambiare o duplicare
    assert first == "MATCHED"
    assert second == "MATCHED"