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
        self.stopped = False

    def stop(self):
        self.stopped = True


class FakeTradingEngine:
    def __init__(self, **kwargs):
        self.recovery_calls = 0

    def recover_after_restart(self):
        self.recovery_calls += 1
        return {"ok": True, "status": "RECOVERY_TRIGGERED"}


class FakeRuntimeController:
    def __init__(self, **kwargs):
        self.started = False
        self.stopped = False

    def start(self, password=None, simulation_mode=False):
        self.started = True
        return {"ok": True, "simulation_mode": simulation_mode}

    def stop(self):
        self.stopped = True


@pytest.mark.e2e
def test_start_stop_start_is_clean(monkeypatch):
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
    assert app._built is True

    app.stop()
    assert app._built is False
    assert app.runtime is None

    app.build()
    assert app._built is True
    assert app.runtime is not None


@pytest.mark.e2e
def test_simulation_mode_flows_into_runtime_start(monkeypatch):
    import headless_main

    started = {}

    class CaptureRuntime(FakeRuntimeController):
        def start(self, password=None, simulation_mode=False):
            started["simulation_mode"] = simulation_mode
            return {"ok": True, "simulation_mode": simulation_mode}

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(headless_main, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", CaptureRuntime)

    app = headless_main.HeadlessApp()
    app.build()
    app._run_boot_recovery()

    result = app.runtime.start(password=None, simulation_mode=True)
    app._validate_runtime_start_result(result)

    assert started["simulation_mode"] is True