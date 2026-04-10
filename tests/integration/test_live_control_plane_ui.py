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
        pass

    def shutdown(self, wait=True, cancel_futures=False):
        return None


class FakeShutdown:
    def __init__(self, *args, **kwargs):
        self.hooks = []

    def register(self, name, fn, priority=100):
        self.hooks.append((name, fn, priority))


class FakeSettingsService:
    def __init__(self, db):
        self.db = db
        self.saved_execution = None

    def load_betfair_config(self):
        class Cfg:
            username = ""
            app_key = ""
            certificate = ""
            private_key = ""

        return Cfg()

    def load_roserpina_config(self):
        class RiskProfile:
            value = "BALANCED"

        class Cfg:
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
            risk_profile = RiskProfile()

        return Cfg()

    def load_execution_settings(self):
        return {
            "execution_mode": "SIMULATION",
            "live_enabled": False,
            "kill_switch": False,
        }

    def save_execution_settings(self, *, execution_mode, live_enabled, kill_switch=False):
        self.saved_execution = {
            "execution_mode": execution_mode,
            "live_enabled": bool(live_enabled),
            "kill_switch": bool(kill_switch),
        }


class FakeBetfairService:
    def __init__(self, settings_service):
        self.mode = None

    def get_client(self):
        return None

    def set_simulation_mode(self, value):
        self.mode = bool(value)

    def status(self):
        return {"connected": False, "broker_type": "SIMULATION"}

    def get_account_funds(self):
        return {"available": 0.0, "exposure": 0.0}

    def disconnect(self):
        return None


class FakeTelegramService:
    def __init__(self, settings_service, db, bus):
        pass

    def status(self):
        return {"connected": False}

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

    def evaluate_live_readiness(self, **kwargs):
        execution_mode = kwargs.get("execution_mode")
        live_enabled = bool(kwargs.get("live_enabled"))
        if execution_mode != "LIVE":
            return {"level": "DEGRADED", "ready": False, "blockers": ["SIMULATION_MODE"]}
        if not live_enabled:
            return {"level": "NOT_READY", "ready": False, "blockers": ["LIVE_NOT_ENABLED"]}
        return {"level": "NOT_READY", "ready": False, "blockers": ["RUNTIME_NOT_INITIALIZED"]}


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
def test_default_ui_state_is_safe(gui):
    assert gui.execution_mode_var.get() == "SIMULATION"
    assert gui.live_enabled_var.get() is False
    assert gui.live_control_state_var.get() == "SIMULATION"
    assert gui.live_effective_status_var.get() == "SAFE_MODE"
    assert gui.live_requested_mode_var.get() == "SIMULATION"
    assert gui.live_last_decision_var.get() == "Last decision: N/A"


@pytest.mark.integration
def test_live_request_reflects_requested_but_not_ready(gui):
    gui.execution_mode_var.set("LIVE")
    gui._on_execution_mode_changed("LIVE")

    assert gui.execution_mode_var.get() == "LIVE"
    assert gui.live_requested_mode_var.get() == "LIVE"
    assert gui.live_control_state_var.get() == "LIVE requested but blocked (gate OFF)"
    assert gui.live_effective_status_var.get() == "LIVE_REQUESTED_BLOCKED"
    assert gui.live_readiness_level_var.get() in {"NOT_READY", "UNKNOWN"}


@pytest.mark.integration
def test_kill_switch_overrides_display_state(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui.kill_switch_var.set(True)
    gui._refresh_live_control_plane_status({})

    assert gui.live_control_state_var.get() == "LIVE blocked by kill switch"
    assert gui.live_effective_status_var.get() == "LIVE_BLOCKED"
    assert "KILL_SWITCH_ACTIVE" in gui.live_readiness_blockers_var.get()
    assert gui.live_last_decision_var.get() == "Last decision: NO-GO"


@pytest.mark.integration
def test_readiness_blockers_are_visible(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui.kill_switch_var.set(False)
    gui._refresh_live_control_plane_status({})

    assert gui.live_readiness_level_var.get() == "NOT_READY"
    assert "RUNTIME_NOT_INITIALIZED" in gui.live_readiness_blockers_var.get()


@pytest.mark.integration
def test_blocked_live_state_visible_with_gate_on_and_readiness_blocker(gui):
    gui.execution_mode_var.set("LIVE")
    gui.live_enabled_var.set(True)
    gui.kill_switch_var.set(False)
    gui._refresh_live_control_plane_status({})

    assert gui.live_effective_status_var.get() == "LIVE_REQUESTED_BLOCKED"
    assert gui.live_control_state_var.get() == "LIVE requested but blocked"
    assert gui.live_last_reason_var.get() == "Last reason: RUNTIME_NOT_INITIALIZED"
