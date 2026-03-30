import threading

import pytest


class ThreadSafeBus:
    def __init__(self):
        self.events = []
        self.lock = threading.Lock()

    def publish(self, event_name, payload=None):
        with self.lock:
            self.events.append((event_name, payload))


class FakeMode:
    value = "ACTIVE"


class FakeConfig:
    anti_duplication_enabled = False
    allow_recovery = True
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0


class FakeRiskDesk:
    bankroll_current = 1000.0


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = None
        self.table_manager = None
        self.dutching_batch_manager = None


@pytest.mark.chaos
@pytest.mark.concurrency
def test_many_parallel_dry_runs_do_not_crash(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"},
                {"selectionId": 2, "price": 3.0, "stake": 50.0, "side": "BACK"},
            ],
            2.0,
            91.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    controller = DutchingController(bus=ThreadSafeBus(), runtime_controller=FakeRuntime())

    payload = {
        "market_id": "1.700",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 1, "price": 2.0},
            {"selectionId": 2, "price": 3.0},
        ],
    }

    results = []

    def worker():
        results.append(controller.submit_dutching(payload, dry_run=True))

    threads = [threading.Thread(target=worker) for _ in range(25)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(results) == 25
    assert all(r["ok"] is True for r in results)
    assert all(r["dry_run"] is True for r in results)