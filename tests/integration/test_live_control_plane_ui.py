import pytest


class FakeBus:
    def subscribe(self, *_args, **_kwargs):
        return None


class FakeDB:
    def close_all_connections(self):
        return None


class FakeExecutor:
    def __init__(self, *args, **kwargs):
        pass

    def shutdown(self, *args, **kwargs):
        return None


class FakeShutdown:
    def register(self, *args, **kwargs):
        return None


class FakeSettingsService:
    def __init__(self, db):
        self.db = db
        self.saved_control = None

    def load_betfair_config(self):
        return type("Cfg", (), {"username": "", "app_key": "", "certificate": "", "private_key": ""})()

    def load_roserpina_config(self):
        rp = type("Risk", (), {"value": "BALANCED"})()
        return type(
            "RsCfg",
            (),
            {
                "target_profit_cycle_pct": 3.0,
                "max_single_bet_pct": 18.0,
                "max_total_exposure_pct": 35.0,
                "max_event_exposure_pct": 18.0,
                "auto_reset_drawdown_pct": 15.0,
                "defense_drawdown_pct": 7.5,
                "lockdown_drawdown_pct": 20.0,
                "expansion_profit_pct": 5.0,
                "expansion_multiplier": 1.1,
                "defense_multiplier": 0.8,
                "table_count": 5,
                "max_recovery_tables": 2,
                "commission_pct": 4.5,
                "min_stake": 0.1,
                "max_stake_abs": 10000.0,
                "allow_recovery": True,
                "anti_duplication_enabled": True,
                "risk_profile": rp,
            },
        )()

    def load_simulation_config(self):
        return {"enabled": True}

    def save_simulation_config(self, payload):
        return None

    def load_live_control_plane(self):
        return {"execution_mode": "SIMULATION", "live_enabled": False, "kill_switch": False}

    def save_live_control_plane(self, payload):
        self.saved_control = dict(payload)


class FakeBetfairService:
    def __init__(self, *_args, **_kwargs):
        self.mode = True

    def get_client(self):
        return None

    def set_simulation_mode(self, value):
        self.mode = bool(value)

    def status(self):
        return {"connected": False}

    def disconnect(self):
        return None


class FakeTelegramService:
    def __init__(self, *_args, **_kwargs):
        pass

    def status(self):
        return {"connected": False}

    def disconnect(self):
        return None

    def stop(self):
        return None


class FakeTradingEngine:
    def __init__(self, **_kwargs):
        pass


class FakeRuntimeController:
    def __init__(self, **_kwargs):
        pass

    def set_simulation_mode(self, value):
        return None

    def get_status(self):
        return {}


class FakeTelegramController:
    def __init__(self, app):
        self.app = app


class FakeTelegramTabUI:
    def __init__(self, *_args, **_kwargs):
        pass


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
    app.destroy()


@pytest.mark.integration
def test_default_ui_state_safe(gui):
    assert gui.execution_mode_var.get() == "SIMULATION"
    assert gui.live_enabled_var.get() is False
    assert gui.control_plane_summary_var.get() == "SIMULATION"


@pytest.mark.integration
def test_live_request_does_not_imply_ready(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui._on_control_plane_change()

    assert gui.control_plane_summary_var.get() == "LIVE richiesto ma BLOCCATO"
    assert gui.readiness_level_var.get() == "Readiness: UNKNOWN"


@pytest.mark.integration
def test_kill_switch_overrides_display(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui.kill_switch_var.set(True)
    gui._on_control_plane_change()

    assert gui.control_plane_summary_var.get() == "LIVE BLOCCATO (KILL SWITCH)"


@pytest.mark.integration
def test_readiness_blockers_visible(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui.kill_switch_var.set(False)
    gui._update_control_plane_display(
        {
            "live_readiness": {
                "level": "NOT_READY",
                "blockers": ["Betfair disconnected", "Telegram offline"],
            }
        }
    )

    assert gui.readiness_level_var.get() == "Readiness: NOT_READY"
    assert "Betfair disconnected" in gui.readiness_blockers_var.get()
    assert gui.readiness_blockers_list.items == ["Betfair disconnected", "Telegram offline"]
