import inspect
import time

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

    def save_bet(self, **payload):
        if self.failures_left["bet"] > 0:
            self.failures_left["bet"] -= 1
            raise RuntimeError("temporary bet failure")
        self.saved_bets.append(payload)

    def save_cashout_transaction(self, **payload):
        if self.failures_left["cashout"] > 0:
            self.failures_left["cashout"] -= 1
            raise RuntimeError("temporary cashout failure")
        self.saved_cashouts.append(payload)

    def save_simulation_bet(self, **payload):
        if self.failures_left["simulation_bet"] > 0:
            self.failures_left["simulation_bet"] -= 1
            raise RuntimeError("temporary sim failure")
        self.saved_sim_bets.append(payload)


def wait_until(condition, timeout=3.0, interval=0.02):
    start = time.time()
    while time.time() - start < timeout:
        if condition():
            return True
        time.sleep(interval)
    return False


def make_writer(db, **overrides):
    """
    Compatibilità con versioni vecchie e nuove di AsyncDBWriter.
    """
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


def test_submit_and_write_bet_successfully():
    db = DummyDB()
    writer = make_writer(db, sleep_idle=0.01)
    writer.start()

    try:
        ok = writer.submit("bet", {"bet_id": "b1", "stake": 10})
        assert ok is True, "submit deve restituire True quando l'item viene accodato"

        done = wait_until(lambda: len(db.saved_bets) == 1)
        assert done, "il writer deve salvare il bet nel DB in background"

        stats = writer.stats()
        assert stats["written"] >= 1, "stats deve contare gli item scritti"
    finally:
        writer.stop()


def test_submit_and_write_cashout_successfully():
    db = DummyDB()
    writer = make_writer(db, sleep_idle=0.01)
    writer.start()

    try:
        assert writer.submit("cashout", {"cashout_id": "c1", "pnl": 4.2}) is True

        done = wait_until(lambda: len(db.saved_cashouts) == 1)
        assert done, "il writer deve salvare il cashout nel DB"

        assert db.saved_cashouts[0]["cashout_id"] == "c1"
    finally:
        writer.stop()


def test_submit_and_write_simulation_bet_successfully():
    db = DummyDB()
    writer = make_writer(db, sleep_idle=0.01)
    writer.start()

    try:
        assert writer.submit("simulation_bet", {"bet_id": "s1", "stake": 5}) is True

        done = wait_until(lambda: len(db.saved_sim_bets) == 1)
        assert done, "il writer deve salvare la simulation bet nel DB"

        assert db.saved_sim_bets[0]["bet_id"] == "s1"
    finally:
        writer.stop()


def test_queue_full_drops_item_and_reports_it():
    db = DummyDB()
    writer = make_writer(db, maxlen=1, maxsize=1, sleep_idle=0.2)
    # non avvio il thread così la queue resta piena

    ok1 = writer.submit("bet", {"bet_id": "b1"})
    ok2 = writer.submit("bet", {"bet_id": "b2"})

    assert ok1 is True, "il primo item deve entrare in queue"
    assert ok2 is False, "quando la queue è piena submit deve restituire False"

    stats = writer.stats()
    assert stats["queued"] == 1, "la queue deve contenere solo il primo item"
    assert stats["dropped"] >= 1, "gli item scartati devono essere contati"


def test_retry_eventually_succeeds_before_max_retries():
    db = DummyDB()
    db.failures_left["bet"] = 2

    writer = make_writer(
        db,
        sleep_idle=0.01,
        max_retries=3,
        retry_delay=0.01,
    )
    writer.start()

    try:
        assert writer.submit("bet", {"bet_id": "retry-bet", "stake": 11}) is True

        done = wait_until(lambda: len(db.saved_bets) == 1, timeout=4.0)
        assert done, "il writer deve ritentare e poi salvare l'item se il DB torna disponibile"

        stats = writer.stats()
        assert stats["failed"] >= 2, "i tentativi falliti devono essere conteggiati"
        assert stats["written"] >= 1, "dopo i retry l'item deve risultare scritto"
    finally:
        writer.stop()


def test_stop_drains_remaining_queue_before_exit():
    db = DummyDB()
    writer = make_writer(db, sleep_idle=0.01)
    writer.start()

    try:
        for i in range(5):
            assert writer.submit("bet", {"bet_id": f"b{i}", "stake": i + 1}) is True
    finally:
        writer.stop()

    assert len(db.saved_bets) == 5, "stop deve drenare la queue residua prima di terminare"


def test_unknown_kind_counts_failure_but_does_not_crash_worker():
    db = DummyDB()
    writer = make_writer(db, sleep_idle=0.01, max_retries=1, retry_delay=0.01)
    writer.start()

    try:
        assert writer.submit("unknown", {"x": 1}) is True

        done = wait_until(lambda: writer.stats()["failed"] >= 1)
        assert done, "kind sconosciuto deve risultare come failure"

        assert writer.submit("bet", {"bet_id": "after-error", "stake": 3}) is True
        written = wait_until(lambda: len(db.saved_bets) == 1)
        assert written, "il worker non deve morire dopo un errore su un item precedente"
    finally:
        writer.stop()