import inspect
import threading
import time

import pytest

from core.async_db_writer import AsyncDBWriter


class DummyDB:
    def __init__(self):
        self.saved_bets = []
        self.saved_cashouts = []
        self.saved_sim_bets = []
        self.failures_left = {
            "bet": 0,
            "cashout": 0,
            "simulation_bet": 0,
        }
        self.lock = threading.Lock()

    def save_bet(self, **payload):
        with self.lock:
            if self.failures_left["bet"] > 0:
                self.failures_left["bet"] -= 1
                raise RuntimeError("temporary bet failure")
            self.saved_bets.append(payload)

    def save_cashout_transaction(self, **payload):
        with self.lock:
            if self.failures_left["cashout"] > 0:
                self.failures_left["cashout"] -= 1
                raise RuntimeError("temporary cashout failure")
            self.saved_cashouts.append(payload)

    def save_simulation_bet(self, **payload):
        with self.lock:
            if self.failures_left["simulation_bet"] > 0:
                self.failures_left["simulation_bet"] -= 1
                raise RuntimeError("temporary sim failure")
            self.saved_sim_bets.append(payload)


def wait_until(condition, timeout=4.0, interval=0.02):
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
@pytest.mark.concurrency
def test_async_db_writer_accepts_parallel_submit_without_losing_items():
    db = DummyDB()
    writer = make_writer(db)
    writer.start()

    total = 40

    def submit_one(i):
        assert writer.submit("bet", {"bet_id": f"b{i}", "stake": i + 1}) is True

    threads = []
    for i in range(total):
        t = threading.Thread(target=submit_one, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    try:
        ok = wait_until(lambda: len(db.saved_bets) == total)
        assert ok, "AsyncDBWriter non deve perdere item sotto submit concorrente"
    finally:
        writer.stop()


@pytest.mark.core
@pytest.mark.failure
def test_async_db_writer_retries_then_persists_after_temporary_failure():
    db = DummyDB()
    db.failures_left["bet"] = 2
    writer = make_writer(db, max_retries=3, retry_delay=0.01)
    writer.start()

    try:
        assert writer.submit("bet", {"bet_id": "r1", "stake": 10}) is True
        ok = wait_until(lambda: len(db.saved_bets) == 1)
        assert ok, "AsyncDBWriter deve ritentare e persistere dopo un failure temporaneo"

        stats = writer.stats()
        assert stats["failed"] >= 2, "I retry falliti devono essere conteggiati"
        assert stats["written"] >= 1, "Il write finale riuscito deve essere conteggiato"
    finally:
        writer.stop()


@pytest.mark.core
@pytest.mark.failure
@pytest.mark.invariant
def test_async_db_writer_unknown_kind_does_not_kill_worker():
    db = DummyDB()
    writer = make_writer(db, max_retries=1, retry_delay=0.01)
    writer.start()

    try:
        assert writer.submit("unknown", {"x": 1}) is True
        ok = wait_until(lambda: writer.stats()["failed"] >= 1)
        assert ok, "Kind sconosciuto deve essere contato come failure"

        assert writer.submit("bet", {"bet_id": "after", "stake": 3}) is True
        ok2 = wait_until(lambda: len(db.saved_bets) == 1)
        assert ok2, "Il worker deve restare vivo dopo un errore precedente"
    finally:
        writer.stop()


@pytest.mark.core
@pytest.mark.invariant
def test_async_db_writer_stop_drains_queue():
    db = DummyDB()
    writer = make_writer(db)
    writer.start()

    try:
        for i in range(10):
            assert writer.submit("bet", {"bet_id": f"x{i}", "stake": i}) is True
    finally:
        writer.stop()

    assert len(db.saved_bets) == 10, "stop deve drenare la queue residua" 