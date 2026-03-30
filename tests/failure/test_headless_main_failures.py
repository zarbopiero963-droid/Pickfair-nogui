import pytest


class FakeBus:
    def __init__(self, *args, **kwargs):
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions.setdefault(event_name, []).append(handler)


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

    def register(self, name, fn, priority=100):
        self.hooks.append((priority, name, fn))

    def shutdown(self):
        for _, _, fn in sorted(self.hooks):
            fn()


class FakeSettingsService:
    def __init__(self, db):
        self.db = db

    def load_simulation_config(self):
        return {"enabled": True}


class ExplodingBetfairService:
    def __init__(self, settings_service):
        raise RuntimeError("betfair init exploded")


class FakeBetfairService:
    def __init__(self, settings_service):
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
        pass

    def recover_after_restart(self):
        return {"ok": True}


class ExplodingRuntimeController:
    def __init__(self, **kwargs):
        pass

    def start(self, password=None, simulation_mode=False):
        raise RuntimeError("runtime start exploded")

    def stop(self):
        return None


@pytest.mark.failure
def test_partial_bootstrap_failure_leaves_clean_state(monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", ExplodingBetfairService)

    app = headless_main.HeadlessApp()

    with pytest.raises(RuntimeError):
        app.build()

    assert app.db is None
    assert app.bus is None
    assert app.executor is None
    assert app.runtime is None
    assert app._built is False
    assert app._running is False


@pytest.mark.failure
def test_runtime_start_failure_returns_1_and_cleans(monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(headless_main, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", ExplodingRuntimeController)

    monkeypatch.setattr(headless_main.HeadlessApp, "_install_signal_handlers", lambda self: None)

    app = headless_main.HeadlessApp()
    result = app.start()

    assert result == 1
    assert app.runtime is None
    assert app._built is False
    assert app._running is False