import pytest


class FakeBus:
    def __init__(self, *args, **kwargs):
        self.subscriptions = {}

    def subscribe(self, event_name, handler):
        self.subscriptions.setdefault(event_name, []).append(handler)

    def publish(self, event_name, payload=None):
        return None


class FakeDB:
    def __init__(self, *args, **kwargs):
        self.closed = False

    def close_all_connections(self):
        self.closed = True


class FakeExecutor:
    def __init__(self, *args, **kwargs):
        self.stopped = False

    def shutdown(self, wait=True, cancel_futures=False):
        self.stopped = True


class FakeShutdown:
    def __init__(self, *args, **kwargs):
        self.hooks = []

    def register(self, name, fn, priority=100):
        self.hooks.append((name, fn, priority))

    def shutdown(self):
        for _, fn, _ in self.hooks:
            fn()


class FakeBetfairCfg:
    username = "u"
    app_key = "k"
    certificate = "c"
    private_key = "p"


class FakeRiskProfile:
    value = "BALANCED"


class FakeRoserpinaCfg:
    target_profit_cycle_pct = 3.0
    max_single_bet_pct = 18.0
    max_total_exposure_pct = 35.0
    max_event_exposure_pct = 18.0
    auto_reset_drawdown_pct = 15.0
    defense_drawdown_pct = 7.5
    lockdown_drawdown_pct = 20.0
    expansion_profit_pct = 5.0
    expansion_multiplier = 1.10
    defense_multiplier = 0.80
    table_count = 5
    max_recovery_tables = 2
    commission_pct = 4.5
    min_stake = 0.10
    max_stake_abs = 10000.0
    allow_recovery = True
    anti_duplication_enabled = True
    risk_profile = FakeRiskProfile()


class FakeSettingsService:
    def __init__(self, db):
        self.db = db
        self.saved_sim = None

    def load_betfair_config(self):
        return FakeBetfairCfg()

    def load_roserpina_config(self):
        return FakeRoserpinaCfg()

    def load_simulation_config(self):
        return {"enabled": True}

    def save_simulation_config(self, payload):
        self.saved_sim = dict(payload)

    def save_betfair_config(self, cfg, password=None):
        return None

    def save_roserpina_config(self, cfg):
        return None


class FakeBetfairService:
    def __init__(self, settings_service):
        self.mode = None

    def get_client(self):
        return None

    def set_simulation_mode(self, value):
        self.mode = bool(value)

    def status(self):
        return {"connected": False}

    def disconnect(self):
        return None


class FakeTelegramService:
    def __init__(self, settings_service, db, bus):
        self._status = {"connected": False}

    def status(self):
        return dict(self._status)

    def stop(self):
        return None


class FakeTradingEngine:
    def __init__(self, **kwargs):
        pass


class FakeRuntimeController:
    def __init__(self, **kwargs):
        self.mode = None

    def set_simulation_mode(self, value):
        self.mode = bool(value)

    def get_status(self):
        return {}

    def start(self, password=None, simulation_mode=False):
        return {"ok": True, "simulation_mode": simulation_mode}

    def pause(self):
        return {"ok": True}

    def resume(self):
        return {"ok": True}

    def stop(self):
        return {"ok": True}

    def reset_cycle(self):
        return {"ok": True}

    def reload_config(self):
        return None


class FakeTelegramController:
    def __init__(self, app):
        self.app = app


class FakeTelegramTabUI:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app


@pytest.fixture
def gui(monkeypatch):
    import mini_gui

    monkeypatch.setattr(mini_gui, "Database", FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(mini_gui, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(mini_gui, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(mini_gui, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(mini_gui, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", FakeRuntimeController)
    monkeypatch.setattr(mini_gui, "TelegramController", FakeTelegramController)
    monkeypatch.setattr(mini_gui, "TelegramTabUI", FakeTelegramTabUI)

    app = mini_gui.MiniPickfairGUI(test_mode=True)
    yield app
    try:
        app.destroy()
    except Exception:
        pass


@pytest.mark.integration
def test_build_window_in_test_environment(gui):
    assert gui is not None
    assert gui.tabs is not None
    assert gui.tab_dashboard is not None
    assert gui.tab_settings is not None
    assert gui.tab_telegram is not None


@pytest.mark.integration
def test_toggle_simulation_live(gui):
    assert gui.simulation_mode is True
    gui.simulation_mode_var.set(False)
    gui._toggle_simulation_mode()

    assert gui.simulation_mode is False
    assert gui.sim_label_var.get() == "LIVE"
    assert gui.status_broker_var.get() == "LIVE"


@pytest.mark.integration
def test_main_buttons_are_wired(gui):
    assert callable(gui.btn_start.cget("command"))
    assert callable(gui.btn_pause.cget("command"))
    assert callable(gui.btn_resume.cget("command"))
    assert callable(gui.btn_stop.cget("command"))
    assert callable(gui.btn_reset.cget("command"))
    assert callable(gui.btn_refresh.cget("command"))


@pytest.mark.integration
def test_refresh_without_data_does_not_crash(gui):
    gui._refresh_runtime_status()
    assert gui.status_mode_var.get() == "STOPPED"
    assert gui.status_bankroll_var.get() == "0.00"


@pytest.mark.integration
def test_no_crash_with_empty_state(gui):
    gui.runtime.get_status = lambda: {}
    gui.betfair_service.status = lambda: {}
    gui.telegram_service.status = lambda: {}

    gui._refresh_runtime_status()

    assert gui.status_mode_var.get() == "STOPPED"
    assert gui.status_betfair_var.get() == "DISCONNECTED"