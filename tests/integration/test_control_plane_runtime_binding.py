import pytest


@pytest.mark.integration
def test_headless_bootstrap_wires_runtime_control_plane_dependencies(monkeypatch):
    import headless_main

    class FakeDB:
        def close_all_connections(self):
            return None

    class FakeBus:
        def subscribe(self, *_args, **_kwargs):
            return None

    class FakeExecutor:
        def shutdown(self, **_kwargs):
            return None

    class FakeShutdown:
        def register(self, *_args, **_kwargs):
            return None

    class FakeSettings:
        def __init__(self, _db):
            pass

        def load_anomaly_enabled(self):
            return False

        def load_anomaly_alerts_enabled(self):
            return False

        def load_anomaly_actions_enabled(self):
            return False

    class FakeBetfair:
        def __init__(self, _settings):
            self.client = object()

        def get_client(self):
            return self.client

        def disconnect(self):
            return None

    class FakeTelegram:
        def __init__(self, _settings, _db, _bus):
            self.sender = None

        def get_sender(self):
            return None

        def stop(self):
            return None

    class FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.runtime_controller = None
            self.simulation_broker = None
            self.betfair_client = None

    class FakeRuntime:
        def __init__(self, **_kwargs):
            self.runtime_probe = None
            self.enforce_probe_readiness_gate = False

    class FakeProbe:
        def __init__(self, **_kwargs):
            return None

    class FakeWatchdog:
        def __init__(self, **_kwargs):
            return None

        def start(self):
            return None

    class FakeCleanup:
        def __init__(self, **_kwargs):
            return None

        def start(self):
            return None

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", lambda **_kwargs: FakeExecutor())
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettings)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfair)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegram)
    monkeypatch.setattr(headless_main, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", FakeRuntime)
    monkeypatch.setattr(headless_main, "RuntimeProbe", FakeProbe)
    monkeypatch.setattr(headless_main, "WatchdogService", FakeWatchdog)
    monkeypatch.setattr(headless_main, "CleanupService", FakeCleanup)

    app = headless_main.HeadlessApp()
    app.build()

    assert app.trading_engine.runtime_controller is app.runtime
    assert app.trading_engine.betfair_client is app.betfair_service.client


@pytest.mark.integration
def test_mini_gui_bootstrap_wires_runtime_control_plane_dependencies(monkeypatch):
    import mini_gui

    class FakeDB:
        def close_all_connections(self):
            return None

    class FakeBus:
        def subscribe(self, *_args, **_kwargs):
            return None

    class FakeExecutor:
        def shutdown(self, **_kwargs):
            return None

    class FakeShutdown:
        def register(self, *_args, **_kwargs):
            return None

    class FakeSettings:
        def __init__(self, _db):
            pass

    class FakeBetfair:
        def __init__(self, _settings):
            self.client = object()

        def get_client(self):
            return self.client

        def disconnect(self):
            return None

    class FakeTelegram:
        def __init__(self, _settings, _db, _bus):
            return None

        def stop(self):
            return None

    class FakeTradingEngine:
        def __init__(self, **_kwargs):
            self.runtime_controller = None
            self.simulation_broker = None
            self.betfair_client = None

    class FakeRuntime:
        def __init__(self, **_kwargs):
            self.runtime_probe = None
            self.enforce_probe_readiness_gate = False

    class FakeProbe:
        def __init__(self, **_kwargs):
            return None

    class FakeTelegramController:
        def __init__(self, _app):
            return None

    monkeypatch.setattr(mini_gui, "Database", FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", lambda **_kwargs: FakeExecutor())
    monkeypatch.setattr(mini_gui, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", FakeSettings)
    monkeypatch.setattr(mini_gui, "BetfairService", FakeBetfair)
    monkeypatch.setattr(mini_gui, "TelegramService", FakeTelegram)
    monkeypatch.setattr(mini_gui, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", FakeRuntime)
    monkeypatch.setattr(mini_gui, "RuntimeProbe", FakeProbe)
    monkeypatch.setattr(mini_gui, "TelegramController", FakeTelegramController)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_build_vars", lambda self: None)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_build_ui", lambda self: None)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_load_initial_settings", lambda self: None)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_wire_bus", lambda self: None)
    monkeypatch.setattr(mini_gui.MiniPickfairGUI, "_apply_simulation_mode_to_runtime", lambda self: None)

    app = mini_gui.MiniPickfairGUI(test_mode=True)

    assert app.trading_engine.runtime_controller is app.runtime
    assert app.trading_engine.betfair_client is app.betfair_service.client
