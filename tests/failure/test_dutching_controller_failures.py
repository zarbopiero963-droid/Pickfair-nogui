import pytest


class ExplodingBusOnFirstQuickBet:
    def __init__(self):
        self.events = []
        self.calls = 0
        self.quick_bet_calls = 0

    def publish(self, event_name, payload=None):
        self.calls += 1
        self.events.append((event_name, payload))

        if event_name == "CMD_QUICK_BET":
            self.quick_bet_calls += 1
            if self.quick_bet_calls >= 1:
                raise RuntimeError("publish exploded on first quick bet")


class ExplodingBusOnSecondQuickBet:
    def __init__(self):
        self.events = []
        self.calls = 0
        self.quick_bet_calls = 0

    def publish(self, event_name, payload=None):
        self.calls += 1
        self.events.append((event_name, payload))

        if event_name == "CMD_QUICK_BET":
            self.quick_bet_calls += 1
            if self.quick_bet_calls >= 2:
                raise RuntimeError("publish exploded on second quick bet")


class FakeMode:
    value = "ACTIVE"


class FakeConfig:
    anti_duplication_enabled = True
    allow_recovery = True
    target_profit_cycle_pct = 3.0     # PFC-008: le quattro percentuali sono obbligatorie anche nel Dutching
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0


class FakeRiskDesk:
    bankroll_current = 1000.0
    cash_equity_current = 1000.0  # PFC-008: cassa verificata
    cash_external_exposure = 0.0  # PFC-008: nessun impegnato fuori dai tavoli


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

    def acquire(self, key):
        # #523: acquire mancante/eccezione ora rifiuta (prima fail-open): il double espone la API reale
        if key in self.keys:
            return False
        self.keys.add(key)
        return True


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
        self.admitted = []
        self.withdrawn = []

    def admit_controller_entries(self, orders, *, batch_id=""):
        # #523: il controller ammette le gambe tramite il runtime; "" = ammesso
        self.admitted.append((batch_id, list(orders)))
        return ""

    def withdraw_controller_entries(self, orders):
        # #523: gambe non pubblicate ritirate dopo un publish fallito
        self.withdrawn.extend(orders)


def _payload():
    return {
        "market_id": "1.300",
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "BACK"},
            {"selectionId": 2, "price": 3.0, "side": "BACK"},
        ],
    }


def _patch_calc(monkeypatch):
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


@pytest.mark.failure
def test_rollback_clean_on_first_publish_failure(monkeypatch):
    from controllers.dutching_controller import DutchingController

    _patch_calc(monkeypatch)

    bus = ExplodingBusOnFirstQuickBet()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result = controller.submit_dutching(_payload())

    assert result["ok"] is False
    assert result["published_count"] == 0
    assert result["total_count"] == 2
    # #523: nessuna gamba pubblicata => tavolo e chiave rilasciati come prima
    assert runtime.table_manager.unlocked == [9]
    assert len(runtime.dutching_batch_manager.failed) == 1
    assert len(runtime.duplication_guard.released) == 1
    # #523: entrambe le gambe ammesse insieme e, non pubblicate, entrambe ritirate
    assert len(runtime.admitted) == 1 and len(runtime.admitted[0][1]) == 2
    assert [o["selection_id"] for o in runtime.withdrawn] == [1, 2]

    event_names = [name for name, _ in bus.events]
    assert "DUTCHING_BATCH_APPROVED" in event_names
    assert event_names.count("CMD_QUICK_BET") == 1
    assert "DUTCHING_BATCH_PARTIAL_FAILURE" in event_names


@pytest.mark.failure
def test_rollback_clean_on_intermediate_failure(monkeypatch):
    from controllers.dutching_controller import DutchingController

    _patch_calc(monkeypatch)

    bus = ExplodingBusOnSecondQuickBet()
    runtime = FakeRuntime()
    controller = DutchingController(bus=bus, runtime_controller=runtime)

    result = controller.submit_dutching(_payload())

    assert result["ok"] is False
    assert result["published_count"] == 1
    assert result["total_count"] == 2
    # #523: una gamba gia' pubblicata puo' essere viva: tavolo e chiave del lotto NON rilasciati
    # (il runtime chiude il lotto sugli esiti delle gambe)
    assert runtime.table_manager.unlocked == []
    assert len(runtime.dutching_batch_manager.failed) == 1
    assert runtime.duplication_guard.released == []
    # #523: solo la gamba non pubblicata viene ritirata dal runtime
    assert [o["selection_id"] for o in runtime.withdrawn] == [2]

    event_names = [name for name, _ in bus.events]
    assert "DUTCHING_BATCH_APPROVED" in event_names
    assert event_names.count("CMD_QUICK_BET") == 2
    assert "DUTCHING_BATCH_PARTIAL_FAILURE" in event_names


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
            {"selectionId": 1, "price": 2.0, "side": "BACK"},  # #523: side esplicito obbligatorio
            {"selectionId": 2, "price": 3.0, "side": "BACK"},  # #523: side esplicito obbligatorio
        ],
    }

    controller = DutchingController(
        bus=ExplodingBusOnFirstQuickBet(),
        runtime_controller=FakeRuntime(),
    )
    result = controller.submit_dutching(payload, dry_run=True)

    assert result["ok"] is False
    assert isinstance(result["error"], str)