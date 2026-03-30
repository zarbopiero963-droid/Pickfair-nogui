import pytest


class FakeBus:
    def __init__(self):
        self.events = []

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


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

    def is_duplicate(self, key):
        return key in self.keys

    def register(self, key):
        self.keys.add(key)

    def release(self, key):
        self.keys.discard(key)


class FakeTableManager:
    def __init__(self):
        self.unlocked = []
        self.activated = []

    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        _ = event_key
        return None

    def allocate(self, event_key=None, allow_recovery=True):
        _ = event_key, allow_recovery

        class T:
            table_id = 7

        return T()

    def activate(self, **kwargs):
        self.activated.append(kwargs)

    def force_unlock(self, table_id):
        self.unlocked.append(table_id)


class FakeBatchManager:
    def __init__(self):
        self.created = []
        self.failed = []

    def create_batch(self, **kwargs):
        self.created.append(kwargs)

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


@pytest.fixture
def payload():
    return {
        "market_id": "1.200",
        "event_name": "A vs B",
        "market_name": "Match Odds",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 10, "price": 2.0, "side": "BACK"},
            {"selectionId": 20, "price": 3.0, "side": "BACK"},
        ],
    }


@pytest.mark.integration
def test_submit_dutching_path_complete(monkeypatch, payload):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 10, "price": 2.0, "stake": 40.0, "side": "BACK"},
                {"selectionId": 20, "price": 3.0, "stake": 60.0, "side": "BACK"},
            ],
            4.2,
            91.5,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    bus = FakeBus()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result = controller.submit_dutching(payload)

    assert result["ok"] is True
    assert result["status"] == "SUBMITTED"
    assert result["table_id"] == 7
    assert result["count"] == 2
    assert result["published_count"] == 2
    assert len(result["orders"]) == 2

    event_names = [name for name, _ in bus.events]
    assert "DUTCHING_BATCH_APPROVED" in event_names
    assert event_names.count("CMD_QUICK_BET") == 2

    assert len(runtime.dutching_batch_manager.created) == 1
    assert len(runtime.table_manager.activated) == 1


@pytest.mark.integration
def test_duplicate_guard_blocks_second_batch(monkeypatch, payload):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 10, "price": 2.0, "stake": 50.0, "side": "BACK"},
                {"selectionId": 20, "price": 3.0, "stake": 50.0, "side": "BACK"},
            ],
            2.0,
            92.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    bus = FakeBus()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result1 = controller.submit_dutching(payload)
    result2 = controller.submit_dutching(payload)

    assert result1["ok"] is True
    assert result2["ok"] is False
    assert "già inviato" in result2["error"] or "Duplicato" in result2["error"]