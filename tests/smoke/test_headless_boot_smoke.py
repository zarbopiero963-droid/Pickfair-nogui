import types

import pytest


@pytest.mark.smoke
def test_headless_build_wires_observability(monkeypatch):
    import headless_main as hm

    class FakeDB:
        def __init__(self):
            self.closed = False

        def close_all_connections(self):
            self.closed = True
            return None

    class FakeBus:
        def subscribe(self, *_a, **_k):
            return None

        def publish(self, *_a, **_k):
            return None

    class FakeExecutor:
        def __init__(self):
            self.shutdown_called = False

        def shutdown(self, **_kwargs):
            self.shutdown_called = True
            return None

    class FakeShutdown:
        def register(self, **_kwargs):
            return None

        def shutdown(self):
            return None

    class FakeSettings:
        def __init__(self, _db):
            pass

    class FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def disconnect(self):
            return None

    class FakeSender:
        def send_alert_message(self, *_a, **_k):
            return None

    class FakeTelegram:
        def __init__(self, _settings, _db, _bus):
            self.sender = FakeSender()

        def get_sender(self):
            return self.sender

        def stop(self):
            return None

    class FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.metrics_registry = None

    class FakeRuntime:
        def __init__(self, **_kwargs):
            self.stopped = False

        def stop(self):
            self.stopped = True
            return None

    class FakeWatchdog:
        def __init__(self, **_kwargs):
            self.started = False
            self.stopped = False

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

    class FakeCleanup:
        def __init__(self, **_kwargs):
            self.started = False
            self.stopped = False

        def start(self):
            self.started = True

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(hm, "Database", FakeDB)
    monkeypatch.setattr(hm, "EventBus", FakeBus)
    fake_executor = FakeExecutor()
    monkeypatch.setattr(hm, "ExecutorManager", lambda **_k: fake_executor)
    monkeypatch.setattr(hm, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(hm, "SettingsService", FakeSettings)
    monkeypatch.setattr(hm, "BetfairService", FakeBetfair)
    monkeypatch.setattr(hm, "TelegramService", FakeTelegram)
    monkeypatch.setattr(hm, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(hm, "RuntimeController", FakeRuntime)
    monkeypatch.setattr(hm, "WatchdogService", FakeWatchdog)
    monkeypatch.setattr(hm, "CleanupService", FakeCleanup)

    app = hm.HeadlessApp()
    app.build()

    assert app.watchdog_service is not None
    assert app.cleanup_service is not None
    assert app.watchdog_service.started is True
    assert app.cleanup_service.started is True
    assert app.metrics_registry is not None
    assert app.alerts_manager is not None
    assert app.incidents_manager is not None
    assert app.diagnostics_service is not None
    assert app.runtime is not None
    assert app.runtime_probe is not None
    assert app.runtime.runtime_probe is app.runtime_probe
    assert app.runtime.enforce_probe_readiness_gate is True

    runtime = app.runtime
    runtime_probe = app.runtime_probe
    diagnostics_service = app.diagnostics_service
    watchdog_service = app.watchdog_service
    cleanup_service = app.cleanup_service
    app.stop()

    assert app.watchdog_service is None
    assert app.cleanup_service is None
    assert fake_executor.shutdown_called is False
    assert runtime is not None
    assert runtime.stopped is True
    assert runtime_probe is not None
    assert diagnostics_service is not None
    assert watchdog_service is not None
    assert cleanup_service is not None
    assert watchdog_service.stopped is True
    assert cleanup_service.stopped is True
    runtime_state = runtime_probe.collect_runtime_state()
    assert isinstance(runtime_state, dict)
    assert "alert_pipeline" in runtime_state
    assert "forensics" in runtime_state
    assert isinstance(runtime_state["alert_pipeline"], dict)
    assert isinstance(runtime_state["forensics"], dict)
    assert getattr(diagnostics_service, "probe", None) is not None
