import threading


def test_parallel_reconcile_same_batch(engine):
    results = []

    def worker():
        res = engine.reconcile_batch("B1")
        results.append(res)

    threads = [threading.Thread(target=worker) for _ in range(5)]

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    allowed = {"RECONCILE_ALREADY_RUNNING", "CONVERGED", "IDEMPOTENT_SKIP"}
    assert len(results) == 5
    assert all(r["reason_code"] in allowed for r in results)
    assert any(r["reason_code"] != "RECONCILE_ALREADY_RUNNING" for r in results)
