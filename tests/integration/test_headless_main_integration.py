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
    assert app.safe_mode is not None
    assert app.trading_engine.kwargs["safe_mode"] is app.safe_mode


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


@pytest.mark.integration
def test_headless_and_mini_gui_explicit_probe_gate_parity(monkeypatch):
    import headless_main
    import mini_gui

    class FakeDB:
        def close_all_connections(self):
            return None

    class FakeBus:
        def __init__(self, *_args, **_kwargs):
            self.subscriptions = {}

        def subscribe(self, event_name, handler):
            self.subscriptions.setdefault(event_name, []).append(handler)

    class FakeExecutor:
        def __init__(self, *_args, **_kwargs):
            pass

        def shutdown(self, **_kwargs):
            return None

    class FakeShutdown:
        def register(self, *_args, **_kwargs):
            return None

    class FakeSettings:
        def __init__(self, _db):
            self.contract_path = "services.settings_service.SettingsService"

        def load_anomaly_enabled(self):
            return False

        def load_anomaly_alerts_enabled(self):
            return False

        def load_anomaly_actions_enabled(self):
            return False

    class FakeBetfair:
        def __init__(self, _settings):
            pass

        def get_client(self):
            return None

        def disconnect(self):
            return None

    class FakeSender:
        def send_alert_message(self, *_args, **_kwargs):
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

    class FakeWatchdog:
        def start(self):
            return None

        def stop(self):
            return None

    class FakeCleanup:
        def start(self):
            return None

        def stop(self):
            return None

    monkeypatch.setattr(headless_main, "Database", FakeDB)
    monkeypatch.setattr(headless_main, "EventBus", FakeBus)
    monkeypatch.setattr(headless_main, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(headless_main, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(headless_main, "SettingsService", FakeSettings)
    monkeypatch.setattr(headless_main, "BetfairService", FakeBetfair)
    monkeypatch.setattr(headless_main, "TelegramService", FakeTelegram)
    monkeypatch.setattr(headless_main, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(headless_main, "RuntimeController", FakeRuntime)
    monkeypatch.setattr(headless_main, "RuntimeProbe", FakeProbe)
    monkeypatch.setattr(headless_main, "SnapshotService", lambda **_kwargs: object())
    monkeypatch.setattr(headless_main, "WatchdogService", lambda **_kwargs: FakeWatchdog())
    monkeypatch.setattr(headless_main, "DiagnosticsService", lambda **_kwargs: object())
    monkeypatch.setattr(headless_main, "HealthRegistry", lambda: object())
    monkeypatch.setattr(headless_main, "MetricsRegistry", lambda: object())
    monkeypatch.setattr(headless_main, "AlertsManager", lambda: object())
    monkeypatch.setattr(headless_main, "IncidentsManager", lambda: object())
    monkeypatch.setattr(headless_main, "DiagnosticBundleBuilder", lambda **_kwargs: object())
    monkeypatch.setattr(headless_main, "TelegramAlertsService", lambda **_kwargs: object())
    monkeypatch.setattr(headless_main, "RetentionManager", lambda **_kwargs: object())
    monkeypatch.setattr(headless_main, "CleanupService", lambda **_kwargs: FakeCleanup())

    monkeypatch.setattr(mini_gui, "Database", FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", FakeExecutor)
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

    headless_app = headless_main.HeadlessApp()
    headless_app.build()
    gui_app = mini_gui.MiniPickfairGUI(test_mode=True)

    assert headless_app.runtime.enforce_probe_readiness_gate is True
    assert gui_app.runtime.enforce_probe_readiness_gate is True
    assert headless_app.runtime.runtime_probe is headless_app.runtime_probe
    assert gui_app.runtime.runtime_probe is gui_app.runtime_probe
    assert type(headless_app.settings_service) is type(gui_app.settings_service)
    assert headless_app.settings_service.contract_path == "services.settings_service.SettingsService"

@pytest.mark.integration
def test_runtime_controller_subscribes_canonical_terminal_lifecycle_events():
    from core.runtime_controller import RuntimeController

    class Settings:
        def load_roserpina_config(self):
            class Cfg:
                table_count = 2
                anti_duplication_enabled = False
                allow_recovery = False
                auto_reset_drawdown_pct = 99
                lockdown_drawdown_pct = 100
            return Cfg()

    class DbStub:
        def _execute(self, *_args, **_kwargs):
            return None

    class Betfair:
        def get_account_funds(self):
            return {"available": 100.0}

        def status(self):
            return {"connected": True}

    class Telegram:
        def status(self):
            return {"connected": True}

    bus = FakeBus()
    _ = RuntimeController(
        bus=bus,
        db=DbStub(),
        settings_service=Settings(),
        betfair_service=Betfair(),
        telegram_service=Telegram(),
    )

    for event_name in (
        "QUICK_BET_FAILED",
        "QUICK_BET_FILLED",
        "QUICK_BET_ROLLBACK_DONE",
        "QUICK_BET_SUCCESS",
        "QUICK_BET_AMBIGUOUS",
    ):
        assert event_name in bus.subscriptions
