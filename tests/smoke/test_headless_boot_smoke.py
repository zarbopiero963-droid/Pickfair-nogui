import types

import pytest


@pytest.mark.smoke
def test_headless_build_wires_observability(monkeypatch):
    import headless_main as hm

    class FakeDB:
        def close_all_connections(self):
            return None

    class FakeBus:
        def subscribe(self, *_a, **_k):
            return None

        def publish(self, *_a, **_k):
            return None

    class FakeExecutor:
        def shutdown(self, **_kwargs):
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
            pass

        def stop(self):
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
    monkeypatch.setattr(hm, "ExecutorManager", lambda **_k: FakeExecutor())
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
    assert app.metrics_registry is not None
    assert app.alerts_manager is not None
    assert app.incidents_manager is not None
    assert app.diagnostics_service is not None

    app.stop()
