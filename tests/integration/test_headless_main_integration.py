import pytest


class FakeBus:
    def __init__(self, *args, **kwargs):
        self.subscriptions = {}
        self.events = []

    def subscribe(self, event_name, handler):
        self.subscriptions.setdefault(event_name, []).append(handler)

    def publish(self, event_name, payload=None):
        self.events.append((event_name, payload))


class FakeDB:
    def __init__(self, *args, **kwargs):
        self.closed = False

    def close_all_connections(self):
        self.closed = True


class FakeExecutor:
    def __init__(self, *args, **kwargs):
        self.shutdown_called = False

    def shutdown(self, wait=True, cancel_futures=False):
        self.shutdown_called = True


class FakeShutdownManager:
    def __init__(self, *args, **kwargs):
        self.hooks = []
        self.shutdown_called = False

    def register(self, name, fn, priority=100):
        self.hooks.append((priority, name, fn))

    def shutdown(self):
        self.shutdown_called = True
        for _, _, fn in sorted(self.hooks):
            fn()


class FakeSettingsService:
    def __init__(self, db):
        self.db = db

    def load_simulation_config(self):
        return {"enabled": True}


class FakeBetfairService:
    def __init__(self, settings_service):
        self.settings_service = settings_service
        self.disconnected = False

    def get_client(self):
        return None

    def disconnect(self):
        self.disconnected = True


class FakeTelegramService:
    def __init__(self, settings_service, db, bus):
        self.settings_service = settings_service
        self.db = db
        self.bus = bus
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeTradingEngine:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.recovery_calls = 0

    def recover_after_restart(self):
        self.recovery_calls += 1
        return {"ok": True, "status": "RECOVERY_TRIGGERED"}


class FakeRuntimeController:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.started = False
        self.stopped = False

    def start(self, password=None, simulation_mode=False):
        self.started = True
        return {"ok": True, "simulation_mode": simulation_mode}

    def stop(self):
        self.stopped = True


@pytest.mark.integration
def test_build_wires_all_components(monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(headless_main, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", FakeRuntimeController)

    app = headless_main.HeadlessApp()
    app.build()

    assert app.db is not None
    assert app.bus is not None
    assert app.executor is not None
    assert app.shutdown is not None
    assert app.settings_service is not None
    assert app.betfair_service is not None
    assert app.telegram_service is not None
    assert app.trading_engine is not None
    assert app.runtime is not None
    assert app._built is True


@pytest.mark.integration
def test_parse_args_defaults_to_simulation(monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main.sys, "argv", ["headless_main.py"])

    app = headless_main.HeadlessApp()
    app.settings_service = FakeSettingsService(None)

    args = app._parse_args()
    assert args["simulation_mode"] is True
    assert args["password"] is None


@pytest.mark.integration
def test_parse_args_live_and_password(monkeypatch):
    import headless_main

    monkeypatch.setattr(
        headless_main.sys,
        "argv",
        ["headless_main.py", "--live", "--password=secret"],
    )

    app = headless_main.HeadlessApp()
    app.settings_service = FakeSettingsService(None)

    args = app._parse_args()
    assert args["simulation_mode"] is False
    assert args["password"] == "secret"