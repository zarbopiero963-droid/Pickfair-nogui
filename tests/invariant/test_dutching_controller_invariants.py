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


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = None
        self.table_manager = None
        self.dutching_batch_manager = None


@pytest.mark.invariant
def test_return_contract_preview_precheck_execute(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 1, "price": 2.0, "stake": 40.0, "side": "BACK"},
                {"selectionId": 2, "price": 3.0, "stake": 60.0, "side": "BACK"},
            ],
            2.5,
            92.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    controller = DutchingController(bus=FakeBus(), runtime_controller=FakeRuntime())
    payload = {
        "market_id": "1.500",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 1, "price": 2.0},
            {"selectionId": 2, "price": 3.0},
        ],
    }

    preview = controller.submit_dutching(payload, dry_run=True)
    preflight = controller.submit_dutching(payload, preflight=True)
    execute = controller.submit_dutching(payload)

    for result in [preview, preflight, execute]:
        assert "ok" in result
        if result["ok"]:
            assert "dry_run" in result
            assert "preflight" in result

    assert preview["dry_run"] is True
    assert preview["preflight"] is False
    assert preflight["dry_run"] is False
    assert preflight["preflight"] is True
    assert execute["dry_run"] is False
    assert execute["preflight"] is False


@pytest.mark.invariant
def test_output_stable_structure(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 7, "price": 2.2, "stake": 50.0, "side": "BACK"},
                {"selectionId": 8, "price": 2.8, "stake": 50.0, "side": "BACK"},
            ],
            1.7,
            93.4,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    controller = DutchingController(bus=FakeBus(), runtime_controller=FakeRuntime())
    payload = {
        "market_id": "1.600",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 7, "price": 2.2},
            {"selectionId": 8, "price": 2.8},
        ],
    }

    result = controller.submit_dutching(payload)
    assert set(result.keys()) >= {
        "ok",
        "dry_run",
        "preflight",
        "status",
        "batch_id",
        "event_key",
        "orders",
        "published_count",
        "count",
        "avg_profit",
        "book_pct",
        "batch_exposure",
    }