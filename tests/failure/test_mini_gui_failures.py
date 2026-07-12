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
        pass

    def close_all_connections(self):
        return None


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

    def shutdown(self):
        for _, fn, _ in self.hooks:
            fn()


class FakeSettingsService:
    def __init__(self, db):
        pass

    def load_betfair_config(self):
        raise RuntimeError("bf config missing")

    def load_roserpina_config(self):
        raise RuntimeError("rs config missing")

    def load_simulation_config(self):
        raise RuntimeError("sim config missing")

    def save_simulation_config(self, payload):
        raise RuntimeError("save sim failed")

    def save_betfair_config(self, cfg, password=None):
        raise RuntimeError("save betfair failed")

    def save_roserpina_config(self, cfg):
        raise RuntimeError("save roserpina failed")


class FakeBetfairService:
    def __init__(self, settings_service):
        pass

    def get_client(self):
        return None

    def set_simulation_mode(self, value):
        return None

    def status(self):
        raise RuntimeError("betfair status failed")

    def disconnect(self):
        return None


class FakeTelegramService:
    def __init__(self, settings_service, db, bus):
        pass

    def status(self):
        raise RuntimeError("telegram status failed")

    def stop(self):
        return None


class FakeTradingEngine:
    def __init__(self, **kwargs):
        pass


class FakeRuntimeController:
    def __init__(self, **kwargs):
        pass

    def set_simulation_mode(self, value):
        raise RuntimeError("runtime set sim failed")

    def get_status(self):
        raise RuntimeError("runtime status failed")

    def start(self, password=None, simulation_mode=False):
        raise RuntimeError("start failed")

    def pause(self):
        raise RuntimeError("pause failed")

    def resume(self):
        raise RuntimeError("resume failed")

    def stop(self):
        raise RuntimeError("stop failed")

    def reset_cycle(self):
        raise RuntimeError("reset failed")


class FakeTelegramController:
    def __init__(self, app):
        pass


class FakeTelegramTabUI:
    def __init__(self, parent, app):
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
    try:
        app.destroy()
    except Exception:
        pass


@pytest.mark.failure
def test_refresh_with_failures_does_not_crash(gui):
    gui._refresh_runtime_status()
    assert gui.status_mode_var.get() == "STOPPED"


@pytest.mark.failure
def test_toggle_simulation_with_save_failure_does_not_crash(gui):
    gui.simulation_mode_var.set(False)
    gui._toggle_simulation_mode()
    # Fail-closed #350 (policy owner "2+guardia", concorde GPT/Fable/GLM):
    # la sync col runtime e' FALLITA, quindi l'intento LIVE NON persiste —
    # rollback allo stato confermato (SIM) su flag, var e label, fallimento
    # marcato. Mai split-brain GUI-SIM/runtime-LIVE.
    assert gui.simulation_mode is True
    assert bool(gui.simulation_mode_var.get()) is True
    assert gui._mode_sync_failed is True
    assert gui.sim_label_var.get() == "SIMULAZIONE"


@pytest.mark.failure
def test_runtime_buttons_do_not_crash_on_failures(gui):
    gui._runtime_start()
    gui._runtime_pause()
    gui._runtime_resume()
    gui._runtime_stop()
    gui._runtime_reset()

    assert True


class FlakyModeRuntime:
    """Runtime la cui sync di modalita' e' pilotabile: OK finche' `fail_sync`
    e' False, poi fallisce. Serve a portare la GUI in uno stato CONFERMATO e
    poi a rompere la sync per provare guardia start + emergency stop (#350)."""

    def __init__(self, **_kwargs):
        self.fail_sync = False
        self.start_calls = 0
        self.emergency_stop_calls = 0
        self.emergency_stop_reasons = []

    def set_simulation_mode(self, _value):
        if self.fail_sync:
            raise RuntimeError("mode sync broken")

    def start(self, **_kwargs):
        self.start_calls += 1
        return {"started": True}

    def emergency_stop(self, reason=""):
        self.emergency_stop_calls += 1
        self.emergency_stop_reasons.append(str(reason))
        return {"stopped": True}

    @staticmethod
    def get_status():
        return {"mode": "STOPPED", "tables": []}


def _make_flaky_gui(monkeypatch):
    import mini_gui

    monkeypatch.setattr(mini_gui, "Database", FakeDB)
    monkeypatch.setattr(mini_gui, "EventBus", FakeBus)
    monkeypatch.setattr(mini_gui, "ExecutorManager", FakeExecutor)
    monkeypatch.setattr(mini_gui, "ShutdownManager", FakeShutdown)
    monkeypatch.setattr(mini_gui, "SettingsService", FakeSettingsService)
    monkeypatch.setattr(mini_gui, "BetfairService", FakeBetfairService)
    monkeypatch.setattr(mini_gui, "TelegramService", FakeTelegramService)
    monkeypatch.setattr(mini_gui, "TradingEngine", FakeTradingEngine)
    monkeypatch.setattr(mini_gui, "RuntimeController", FlakyModeRuntime)
    monkeypatch.setattr(mini_gui, "TelegramController", FakeTelegramController)
    monkeypatch.setattr(mini_gui, "TelegramTabUI", FakeTelegramTabUI)
    return mini_gui.MiniPickfairGUI(test_mode=True)


@pytest.mark.failure
def test_start_live_refused_when_mode_sync_unconfirmed(monkeypatch):
    # BLOCK guardia #350: con l'ultima sync di modalita' NON confermata,
    # _runtime_start in LIVE deve RIFIUTARE l'avvio (mai runtime.start).
    app = _make_flaky_gui(monkeypatch)
    try:
        # 1) toggle a LIVE con sync OK => LIVE CONFERMATO
        app.simulation_mode_var.set(False)
        app._toggle_simulation_mode()
        assert app.simulation_mode is False
        assert app._mode_sync_failed is False

        # 2) la sync si rompe; toggle a SIM fallisce => rollback a LIVE
        #    confermato + _mode_sync_failed (stato runtime incerto)
        app.runtime.fail_sync = True
        app.simulation_mode_var.set(True)
        app._toggle_simulation_mode()
        assert app.simulation_mode is False  # resta l'ultimo confermato
        assert app._mode_sync_failed is True

        # 3) start in LIVE con sync non confermata => RIFIUTATO fail-closed
        app._runtime_start()
        assert app._start_refused_mode_unconfirmed_total == 1
        assert app.runtime.start_calls == 0
    finally:
        try:
            app.destroy()
        except Exception:
            pass


@pytest.mark.failure
def test_start_sim_also_refused_when_mode_sync_unconfirmed(monkeypatch):
    # BLOCK punto B (decisione owner #350 "applica entrambe"): con sync NON
    # confermata anche lo start in SIMULATION e' rifiutato — nessun avvio con
    # stato runtime incerto. Recovery: solo una sync riuscita sblocca.
    app = _make_flaky_gui(monkeypatch)
    try:
        # stato confermato SIM (init con sync OK); poi la sync si rompe e il
        # toggle a LIVE fallisce => rollback a SIM confermato, sync incerta
        assert app.simulation_mode is True
        app.runtime.fail_sync = True
        app.simulation_mode_var.set(False)
        app._toggle_simulation_mode()
        assert app.simulation_mode is True  # rollback al confermato SIM
        assert app._mode_sync_failed is True

        # start in SIMULATION con sync non confermata => RIFIUTATO
        app._runtime_start()
        assert app._start_refused_mode_unconfirmed_total == 1
        assert app.runtime.start_calls == 0
    finally:
        try:
            app.destroy()
        except Exception:
            pass


@pytest.mark.failure
def test_emergency_stop_fired_when_live_confirmed_and_sync_fails(monkeypatch):
    # BLOCK punto A (decisione owner #350 "applica entrambe"): se lo stato
    # CONFERMATO e' LIVE e la sync fallisce, il trading live potenzialmente
    # attivo va fermato SUBITO: emergency_stop(reason=mode_sync_failed_fail_closed).
    app = _make_flaky_gui(monkeypatch)
    try:
        # LIVE confermato con sync OK
        app.simulation_mode_var.set(False)
        app._toggle_simulation_mode()
        assert app.simulation_mode is False
        assert app.runtime.emergency_stop_calls == 0

        # sync rotta: toggle a SIM fallisce => rollback a LIVE + KILL-SWITCH
        app.runtime.fail_sync = True
        app.simulation_mode_var.set(True)
        app._toggle_simulation_mode()
        assert app._mode_sync_failed is True
        assert app.runtime.emergency_stop_calls == 1
        assert app.runtime.emergency_stop_reasons == ["mode_sync_failed_fail_closed"]
    finally:
        try:
            app.destroy()
        except Exception:
            pass


@pytest.mark.failure
def test_emergency_stop_not_fired_when_sim_confirmed(monkeypatch):
    # Contro-prova punto A: con stato confermato SIM (nessun trading live
    # possibile autorizzato dalla GUI) la sync fallita NON spara l'emergency
    # stop — rollback + blocco avvii bastano, niente lockdown superfluo.
    app = _make_flaky_gui(monkeypatch)
    try:
        assert app.simulation_mode is True  # SIM confermato dall'init
        app.runtime.fail_sync = True
        app.simulation_mode_var.set(False)
        app._toggle_simulation_mode()  # tentativo LIVE fallisce
        assert app._mode_sync_failed is True
        assert app.runtime.emergency_stop_calls == 0
    finally:
        try:
            app.destroy()
        except Exception:
            pass