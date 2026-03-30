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


class RecoveryTrackingTradingEngine:
    def __init__(self, **kwargs):
        self.calls = 0

    def recover_after_restart(self):
        self.calls += 1
        return {"ok": True, "status": "RECOVERY_TRIGGERED", "restored_inflight": 2}


class BadRecoveryTradingEngine:
    def __init__(self, **kwargs):
        pass

    def recover_after_restart(self):
        return {"ok": False, "error": "recovery failed"}


class FakeRuntimeController:
    def __init__(self, **kwargs):
        self.started = False
        self.stopped = False

    def start(self, password=None, simulation_mode=False):
        self.started = True
        return {"ok": True, "simulation_mode": simulation_mode}

    def stop(self):
        self.stopped = True


@pytest.mark.recovery
def test_boot_runs_recovery_before_runtime_start(monkeypatch):
    import headless_main

    runtime_started = {"value": False}
    recovery_ref = {"engine": None}

    class RuntimeAfterRecovery(FakeRuntimeController):
        def start(self, password=None, simulation_mode=False):
            runtime_started["value"] = True
            assert recovery_ref["engine"] is not None
            assert recovery_ref["engine"].calls == 1
            return {"ok": True, "simulation_mode": simulation_mode}

    class TrackingTradingEngine(RecoveryTrackingTradingEngine):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            recovery_ref["engine"] = self

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(headless_main, "TradingEngine", TrackingTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", RuntimeAfterRecovery)
    monkeypatch.setattr(headless_main.HeadlessApp, "_install_signal_handlers", lambda self: None)

    app = headless_main.HeadlessApp()
    app.build()
    app._run_boot_recovery()
    result = app.runtime.start(password=None, simulation_mode=True)
    app._validate_runtime_start_result(result)

    assert runtime_started["value"] is True
    assert recovery_ref["engine"].calls == 1


@pytest.mark.recovery
def test_failed_boot_recovery_raises(monkeypatch):
    import headless_main

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdownManager)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(headless_main, "TradingEngine", BadRecoveryTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", FakeRuntimeController)

    app = headless_main.HeadlessApp()
    app.build()

    with pytest.raises(RuntimeError):
        app._run_boot_recovery()