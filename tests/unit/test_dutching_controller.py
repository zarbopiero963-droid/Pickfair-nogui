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


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = FakeDuplicationGuard()
        self.table_manager = None
        self.dutching_batch_manager = None


@pytest.fixture
def controller(monkeypatch):
    from controllers.dutching_controller import DutchingController

    def fake_calculate_dutching(selections, total_stake):
        _ = total_stake
        return (
            [
                {
                    "selectionId": int(selections[0]["selectionId"]),
                    "price": float(selections[0]["price"]),
                    "stake": 40.0,
                    "side": "BACK",
                },
                {
                    "selectionId": int(selections[1]["selectionId"]),
                    "price": float(selections[1]["price"]),
                    "stake": 60.0,
                    "side": "BACK",
                },
            ],
            5.5,
            92.3,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    return DutchingController(bus=FakeBus(), runtime_controller=FakeRuntime())


def valid_payload():
    return {
        "market_id": "1.100",
        "event_name": "Team A v Team B",
        "market_name": "Match Odds",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 11, "price": 2.2, "side": "BACK"},
            {"selectionId": 22, "price": 3.4, "side": "BACK"},
        ],
    }


@pytest.mark.unit
def test_validate_ok(controller):
    result = controller.validate(valid_payload())
    assert result == {"ok": True}


@pytest.mark.unit
def test_validate_invalid_selection_duplicate(controller):
    payload = valid_payload()
    payload["selections"].append({"selectionId": 11, "price": 4.0, "side": "BACK"})
    result = controller.validate(payload)
    assert result["ok"] is False
    assert "duplicato" in result["error"]


@pytest.mark.unit
def test_preview_contract(controller):
    result = controller.preview(valid_payload())
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["preflight"] is False
    assert isinstance(result["results"], list)
    assert "batch_id" in result
    assert "event_key" in result
    assert "avg_profit" in result
    assert "book_pct" in result


@pytest.mark.unit
def test_precheck_contract(controller):
    result = controller.precheck(valid_payload())
    assert result["ok"] is True
    assert result["dry_run"] is False
    assert result["preflight"] is True
    assert isinstance(result["results"], list)
    assert "batch_id" in result
    assert "event_key" in result


@pytest.mark.unit
def test_submit_dutching_dry_run_path(controller):
    result = controller.submit_dutching(valid_payload(), dry_run=True)
    assert result["ok"] is True
    assert result["dry_run"] is True
    assert result["preflight"] is False


@pytest.mark.unit
def test_submit_dutching_preflight_path(controller):
    result = controller.submit_dutching(valid_payload(), preflight=True)
    assert result["ok"] is True
    assert result["dry_run"] is False
    assert result["preflight"] is True


@pytest.mark.unit
def test_no_bus_safe(monkeypatch):
    from controllers.dutching_controller import DutchingController

    runtime = FakeRuntime()

    def fake_calculate_dutching(selections, total_stake):
        _ = total_stake
        return (
            [
                {"selectionId": int(selections[0]["selectionId"]), "price": 2.2, "stake": 50.0, "side": "BACK"},
                {"selectionId": int(selections[1]["selectionId"]), "price": 3.4, "stake": 50.0, "side": "BACK"},
            ],
            1.0,
            90.0,
        )

    monkeypatch.setattr(
        "controllers.dutching_controller.calculate_dutching",
        fake_calculate_dutching,
    )

    controller = DutchingController(bus=None, runtime_controller=runtime)
    result = controller.submit_dutching(valid_payload())
    assert result["ok"] is False
    assert result["error"] == "EventBus non disponibile"