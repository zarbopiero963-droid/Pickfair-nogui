import pytest


class ExplodingBus:
    def __init__(self):
        self.events = []
        self.calls = 0

    def publish(self, event_name, payload=None):
        self.calls += 1
        self.events.append((event_name, payload))
        if event_name == "CMD_QUICK_BET" and self.calls >= 2:
            raise RuntimeError("publish exploded")


class FakeMode:
    value = "ACTIVE"


class FakeConfig:
    anti_duplication_enabled = True
    allow_recovery = True
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0


class FakeRiskDesk:
    bankroll_current = 1000.0


class FakeDuplicationGuard:
    def __init__(self):
        self.keys = set()
        self.released = []

    def is_duplicate(self, key):
        return key in self.keys

    def register(self, key):
        self.keys.add(key)

    def release(self, key):
        self.released.append(key)
        self.keys.discard(key)


class FakeTableManager:
    def __init__(self):
        self.unlocked = []

    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        _ = event_key
        return None

    def allocate(self, event_key=None, allow_recovery=True):
        _ = event_key, allow_recovery

        class T:
            table_id = 9

        return T()

    def activate(self, **kwargs):
        _ = kwargs

    def force_unlock(self, table_id):
        self.unlocked.append(table_id)


class FakeBatchManager:
    def __init__(self):
        self.failed = []

    def create_batch(self, **kwargs):
        _ = kwargs

    def mark_batch_failed(self, batch_id, error):
        self.failed.append((batch_id, error))


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = FakeDuplicationGuard()
        self.table_manager = FakeTableManager()
        self.dutching_batch_manager = FakeBatchManager()


@pytest.mark.failure
def test_rollback_clean_on_intermediate_failure(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"},
                {"selectionId": 2, "price": 3.0, "stake": 50.0, "side": "BACK"},
            ],
            1.0,
            90.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    payload = {
        "market_id": "1.300",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "BACK"},
            {"selectionId": 2, "price": 3.0, "side": "BACK"},
        ],
    }

    bus = ExplodingBus()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result = controller.submit_dutching(payload)

    assert result["ok"] is False
    assert result["published_count"] == 1
    assert result["total_count"] == 2
    assert runtime.table_manager.unlocked == [9]
    assert len(runtime.dutching_batch_manager.failed) == 1
    assert len(runtime.duplication_guard.released) == 1


@pytest.mark.failure
def test_error_contract_is_coherent(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def exploding_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        raise RuntimeError("calc exploded")

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        exploding_calculate_dutching,
    )

    payload = {
        "market_id": "1.400",
        "total_stake": 100.0,
        "selections": [
            {"selectionId": 1, "price": 2.0},
            {"selectionId": 2, "price": 3.0},
        ],
    }

    controller = DutchingController(bus=ExplodingBus(), runtime_controller=FakeRuntime())
    result = controller.submit_dutching(payload, dry_run=True)

    assert result["ok"] is False
    assert isinstance(result["error"], str)