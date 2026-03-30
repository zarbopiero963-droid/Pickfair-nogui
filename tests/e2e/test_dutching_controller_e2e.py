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
    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        _ = event_key
        return None

    def allocate(self, event_key=None, allow_recovery=True):
        _ = event_key, allow_recovery

        class T:
            table_id = 3

        return T()

    def activate(self, **kwargs):
        _ = kwargs

    def force_unlock(self, table_id):
        _ = table_id


class FakeBatchManager:
    def __init__(self):
        self.created = []

    def create_batch(self, **kwargs):
        self.created.append(kwargs)


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = FakeDuplicationGuard()
        self.table_manager = FakeTableManager()
        self.dutching_batch_manager = FakeBatchManager()


@pytest.mark.e2e
def test_e2e_with_batch_manager_bus_and_service_mock(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 100, "price": 2.5, "stake": 45.0, "side": "BACK"},
                {"selectionId": 200, "price": 3.1, "stake": 55.0, "side": "BACK"},
            ],
            3.5,
            93.1,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    payload = {
        "market_id": "1.777",
        "event_name": "Home v Away",
        "market_name": "Match Odds",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 100, "price": 2.5, "side": "BACK"},
            {"selectionId": 200, "price": 3.1, "side": "BACK"},
        ],
    }

    bus = FakeBus()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result = controller.submit_dutching(payload)

    assert result["ok"] is True
    assert result["status"] == "SUBMITTED"
    assert result["published_count"] == 2
    assert len(runtime.dutching_batch_manager.created) == 1

    quick_bets = [p for name, p in bus.events if name == "CMD_QUICK_BET"]
    assert len(quick_bets) == 2
    assert all(x["batch_id"] == result["batch_id"] for x in quick_bets)