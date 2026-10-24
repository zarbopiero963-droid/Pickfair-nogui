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
    target_profit_cycle_pct = 3.0  # PFC-008: obbligatoria anche nel Dutching
    max_single_bet_pct = 100.0


class FakeRiskDesk:
    bankroll_current = 1000.0
    cash_equity_current = 1000.0  # PFC-008: cassa verificata
    cash_committed_exposure = 0.0  # PFC-008: nessun impegnato fuori dai tavoli


class FakeRuntime:
    def __init__(self):
        self.mode = FakeMode()
        self.config = FakeConfig()
        self.risk_desk = FakeRiskDesk()
        self.duplication_guard = None
        self.table_manager = None
        self.dutching_batch_manager = None

    def read_funds_for_controller_entry(self):
        # #546 D2: la controfigura non modella il conto: lettura fresca verificata e capiente.
        return {"funds": {"available": 1e9, "exposure": 0.0}, "read_at": None}

    def controller_entry_funds_reason(self, funds_read, *, extra_exposure=0.0):
        return ""

    def admit_controller_entries(self, orders, *, batch_id=""):
        # #523: il controller ammette le gambe tramite il runtime; "" = ammesso
        return ""

    def withdraw_controller_entries(self, orders):
        return None

    def controller_event_exposure(self, entry, *, event_key=""):
        # #554: misura evento del runtime (qui: il tavolo con la chiave, come la vecchia misura del controller)
        table_manager = getattr(self, "table_manager", None)
        table = table_manager.find_by_event_key(event_key) if table_manager is not None else None
        return float(getattr(table, "current_exposure", 0.0) or 0.0) if table else 0.0


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
        "event_name": "Casa v Ospite",  # #554: il cap evento richiede l'identita' dell'evento
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "BACK"},  # #523: side esplicito obbligatorio
            {"selectionId": 2, "price": 3.0, "side": "BACK"},  # #523: side esplicito obbligatorio
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
        "event_name": "Casa v Ospite",  # #554: il cap evento richiede l'identita' dell'evento
        "total_stake": 100.0,
        "simulation_mode": True,
        "selections": [
            {"selectionId": 7, "price": 2.2, "side": "BACK"},  # #523: side esplicito obbligatorio
            {"selectionId": 8, "price": 2.8, "side": "BACK"},  # #523: side esplicito obbligatorio
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