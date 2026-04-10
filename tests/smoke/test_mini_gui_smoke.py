import pytest


@pytest.mark.smoke
def test_import_mini_gui_module():
    import mini_gui  # noqa: F401


@pytest.mark.smoke
def test_mini_gui_build_enforces_probe_gate_and_uses_settings_service_contract(monkeypatch):
    import mini_gui

    assert mini_gui.SettingsService.__module__ == "services.settings_service"

    class FakeDB:
        def close_all_connections(self):
            return None

    class FakeBus:
        pass

    class FakeExecutor:
        def shutdown(self, **_kwargs):
            return None

    class FakeShutdown:
        def register(self, *_args, **_kwargs):
            return None

    class FakeSettings:
        def __init__(self, _db):
            self.compatibility_contract = "services.settings_service.SettingsService"

    class FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def disconnect(self):
            return None

    class FakeTelegram:
        def __init__(self, _settings, _db, _bus):
            pass

        def stop(self):
            return None

    class FakeTradingEngine:
        def __init__(self, **_kwargs):
            pass

    class FakeRuntime:
        def __init__(self, **_kwargs):
            self.runtime_probe = None
            self.enforce_probe_readiness_gate = False

    class FakeProbe:
        def __init__(self, **kwargs):
            self.settings_service = kwargs.get("settings_service")

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

    assert app.runtime is not None
    assert app.runtime_probe is not None
    assert app.runtime.runtime_probe is app.runtime_probe
    assert app.runtime.enforce_probe_readiness_gate is True
    assert app.runtime_probe.settings_service is app.settings_service
    assert app.settings_service.compatibility_contract == "services.settings_service.SettingsService"
