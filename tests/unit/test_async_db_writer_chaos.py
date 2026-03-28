import inspect
import threading
import time

import pytest

from core.async_db_writer import AsyncDBWriter


class FlakyDB:
    def __init__(self):
        self.saved_bets = []
        self.saved_cashouts = []
        self.saved_sim_bets = []
        self.lock = threading.Lock()
        self.bet_failures_left = 0
        self.block_time = 0.0

    def save_bet(self, **payload):
        with self.lock:
            if self.block_time > 0:
                time.sleep(self.block_time)
            if self.bet_failures_left > 0:
                self.bet_failures_left -= 1
                raise RuntimeError("temporary bet failure")
            self.saved_bets.append(payload)

    def save_cashout_transaction(self, **payload):
        with self.lock:
            self.saved_cashouts.append(payload)

    def save_simulation_bet(self, **payload):
        with self.lock:
            self.saved_sim_bets.append(payload)


def wait_until(condition, timeout=5.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


def make_writer(db, **overrides):
    sig = inspect.signature(AsyncDBWriter.__init__)
    kwargs = {}

    if "maxlen" in sig.parameters:
        kwargs["maxlen"] = overrides.get("maxlen", 5000)
    if "maxsize" in sig.parameters:
        kwargs["maxsize"] = overrides.get("maxsize", overrides.get("maxlen", 5000))
    if "sleep_idle" in sig.parameters:
        kwargs["sleep_idle"] = overrides.get("sleep_idle", 0.01)
    if "workers" in sig.parameters:
        kwargs["workers"] = overrides.get("workers", 1)
    if "batch_size" in sig.parameters:
        kwargs["batch_size"] = overrides.get("batch_size", 1)
    if "max_retries" in sig.parameters:
        kwargs["max_retries"] = overrides.get("max_retries", 3)
    if "retry_delay" in sig.parameters:
        kwargs["retry_delay"] = overrides.get("retry_delay", 0.01)

    return AsyncDBWriter(db, **kwargs)


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.failure
def test_async_db_writer_retry_exhaustion_counts_failure_and_does_not_crash():
    db = FlakyDB()
    db.bet_failures_left = 100

    writer = make_writer(db, max_retries=2, retry_delay=0.01)
    writer.start()

    try:
        assert writer.submit("bet", {"bet_id": "dead", "stake": 10}) is True

        ok = wait_until(lambda: writer.stats()["failed"] >= 1)
        assert ok, "Retry exhaustion deve essere contato come failure"

        assert writer.submit("bet", {"bet_id": "alive", "stake": 11}) is True

        # rendiamo il db di nuovo sano
        db.bet_failures_left = 0

        ok2 = wait_until(lambda: any(x["bet_id"] == "alive" for x in db.saved_bets))
        assert ok2, "Il writer deve restare vivo e processare item nuovi anche dopo retry exhaustion"
    finally:
        writer.stop()


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.concurrency
def test_async_db_writer_parallel_submit_under_load():
    db = FlakyDB()
    writer = make_writer(db)
    writer.start()

    total = 100

    def submit_one(i):
        writer.submit("bet", {"bet_id": f"b{i}", "stake": i})

    threads = [threading.Thread(target=submit_one, args=(i,)) for i in range(total)]

    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        ok = wait_until(lambda: len(db.saved_bets) == total, timeout=6.0)
        assert ok, "Sotto submit concorrente pesante, AsyncDBWriter non deve perdere item"
    finally:
        writer.stop()


@pytest.mark.core
@pytest.mark.chaos
@pytest.mark.failure
def test_async_db_writer_slow_db_does_not_lose_queue_on_stop():
    db = FlakyDB()
    db.block_time = 0.02

    writer = make_writer(db)
    writer.start()

    try:
        for i in range(20):
            assert writer.submit("bet", {"bet_id": f"s{i}", "stake": i}) is True
    finally:
        writer.stop()

    assert len(db.saved_bets) == 20, "Con DB lento, stop deve comunque drenare la queue senza perdita"