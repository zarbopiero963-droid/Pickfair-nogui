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

    # solo 1 deve lavorare davvero
    running = [
        r for r in results
        if r["reason_code"] != "RECONCILE_ALREADY_RUNNING"
    ]

    assert len(running) == 1