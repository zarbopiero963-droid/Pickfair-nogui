"""G5 PR 4/5 (redesign post-#397) — AUTO_GREEN_DELAY_SEC: grace NON-bloccante a monte.

Decisione owner 'B': il grace prima del cashout va cablato in
`core/runtime_controller._route_cashout_signal` come route DIFFERITA su
`threading.Timer` (non blocca il bus; prezzo green-up ricalcolato FRESCO dopo
l'attesa), OPT-IN e default disarmato.

Qui si testano le unita' reali di RuntimeController montandole su un oggetto
leggero (evita di costruire un controller completo): il dispatch inline vs
differito, la pending-guard, il ri-check dei gate fail-closed, il fail-safe/clamp/
fail-open dei secondi, e il round-trip config + GUI.
"""
from types import SimpleNamespace

import pytest

import trading_config
import core.runtime_controller as rc_mod
from core.runtime_controller import RuntimeController, CASHOUT_FAILED


class _FakeTimer:
    """Sostituto di threading.Timer: registra e NON parte da solo (fire manuale)."""

    instances = []

    def __init__(self, delay, fn, args=()):
        self.delay = delay
        self.fn = fn
        self.args = tuple(args)
        self.daemon = False
        self.started = False
        self.cancelled = False
        _FakeTimer.instances.append(self)

    def start(self):
        self.started = True

    def cancel(self):
        self.cancelled = True

    def fire(self):
        self.fn(*self.args)


@pytest.fixture(autouse=True)
def _patch_timer(monkeypatch):
    _FakeTimer.instances = []
    monkeypatch.setattr(rc_mod.threading, "Timer", _FakeTimer)
    yield


class _RC:
    """Oggetto minimale che RIUSA i metodi reali di RuntimeController."""

    # metodi under-test (reali)
    _auto_green_delay_seconds = RuntimeController._auto_green_delay_seconds
    # _auto_green_key e' @staticmethod: copiandolo come attr di classe perderebbe
    # lo statico => ri-wrappa per non ricevere self.
    _auto_green_key = staticmethod(RuntimeController._auto_green_key)
    _auto_green_reserve = RuntimeController._auto_green_reserve
    _auto_green_arm_timer = RuntimeController._auto_green_arm_timer
    _auto_green_is_stale = RuntimeController._auto_green_is_stale
    _auto_green_release = RuntimeController._auto_green_release
    _cancel_pending_auto_green = RuntimeController._cancel_pending_auto_green
    _cashout_gates_still_open = RuntimeController._cashout_gates_still_open
    _deferred_cashout_route = RuntimeController._deferred_cashout_route
    _publish_cashout_failed = RuntimeController._publish_cashout_failed
    _route_cashout_signal = RuntimeController._route_cashout_signal

    def __init__(self, *, enabled=False, sec=2.5, chain_wired=True):
        import threading
        self.config = SimpleNamespace(auto_green_delay_enabled=enabled, auto_green_delay_sec=sec)
        self._auto_green_pending = {}
        self._auto_green_pending_lock = threading.Lock()
        self._auto_green_generation = 0
        self._daily_loss_stop_lock = threading.Lock()
        self._emergency_stopped = False
        self._daily_loss_pending_stop = False
        self.execution_mode = "SIMULATION"
        self.betfair_service = SimpleNamespace(_session_invalid=False)
        self.mode = SimpleNamespace(value="ACTIVE")
        self._runtime_active_flag = True
        self._kill_switch = False
        self._chain_wired = chain_wired
        # spie
        self.executed = []
        self.rejected = []
        self.published = []

    # stub delle dipendenze non under-test
    def _cashout_chain_wired(self):
        return self._chain_wired

    def _reject_signal(self, signal, reason):
        self.rejected.append((signal, reason))

    def _runtime_active(self):
        return self._runtime_active_flag

    def _is_kill_switch_active(self):
        return self._kill_switch

    def _execute_cashout_route(self, signal):
        self.executed.append(signal)

    @property
    def bus(self):
        rc = self

        class _Bus:
            def publish(self, topic, payload):
                rc.published.append((topic, payload))

        return _Bus()


def _sig(stype="CASHOUT", market="1.1", sel=7):
    return {"signal_type": stype, "market_id": market, "selection_id": sel}


# ==========================================================================
# 1) Dispatch: disarmato => route INLINE (no Timer); armato => Timer differito
# ==========================================================================
def test_disabled_routes_inline_no_timer():
    rc = _RC(enabled=False)
    rc._route_cashout_signal(_sig())
    assert rc.executed == [_sig()]              # route inline
    assert _FakeTimer.instances == []           # nessun Timer


def test_enabled_defers_on_timer_not_inline():
    rc = _RC(enabled=True, sec=2.5)
    rc._route_cashout_signal(_sig())
    assert rc.executed == []                     # NON inline
    assert len(_FakeTimer.instances) == 1
    t = _FakeTimer.instances[0]
    assert t.delay == 2.5 and t.daemon is True and t.started is True
    # firing del timer => route eseguita coi gate aperti
    t.fire()
    assert rc.executed == [_sig()]
    # pending rilasciata dopo il fire
    assert rc._auto_green_pending == {}


def test_timer_start_failure_falls_back_inline_and_releases(monkeypatch):
    # Se lo scheduling del Timer solleva (thread esauriti), il cashout NON e' perso
    # (route inline) e la pending-guard NON resta bloccata (Fugu/Greptile P1).
    def _boom(*a, **k):
        raise RuntimeError("can't start new thread")

    monkeypatch.setattr(rc_mod.threading, "Timer", _boom)
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())
    assert rc.executed == [_sig()]              # fallback inline: cashout eseguito
    assert rc._auto_green_pending == {}      # chiave rilasciata, non bloccata
    # e un successivo cashout sullo stesso target NON e' soppresso
    rc._route_cashout_signal(_sig())
    assert rc.executed == [_sig(), _sig()]


def test_chain_not_wired_rejects_before_grace():
    rc = _RC(enabled=True, chain_wired=False)
    rc._route_cashout_signal(_sig())
    assert rc.rejected and rc.rejected[0][1] == "cashout_chain_not_wired"
    assert _FakeTimer.instances == []


# ==========================================================================
# 2) Pending-guard: un secondo grace sullo stesso target e' soppresso
# ==========================================================================
def test_duplicate_grace_accorpato_solo_log():
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())            # arma il primo grace
    rc._route_cashout_signal(_sig())            # stesso target => accorpato (solo log)
    assert len(_FakeTimer.instances) == 1        # un solo timer
    assert rc.published == []                     # NESSUN CASHOUT_FAILED (non e' un fallimento)
    # dopo il fire (release), un nuovo grace e' di nuovo ammesso
    _FakeTimer.instances[0].fire()
    rc._route_cashout_signal(_sig())
    assert len(_FakeTimer.instances) == 2


def test_cashout_all_routes_inline_no_grace():
    # CASHOUT_ALL ("chiudi tutto") non e' mai ritardato: route inline, nessun timer.
    rc = _RC(enabled=True)
    sig = _sig(stype="CASHOUT_ALL", market="", sel="")
    rc._route_cashout_signal(sig)
    assert rc.executed == [sig]
    assert _FakeTimer.instances == []


def test_cross_type_same_target_no_double_schedule():
    # Un CASHOUT_ALL inline + un CASHOUT singolo sullo stesso market: l'ALL non
    # schedula timer (inline), il singolo grazia una sola volta => nessun doppio.
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig(stype="CASHOUT_ALL", market="1.1", sel=""))  # inline
    rc._route_cashout_signal(_sig(stype="CASHOUT", market="1.1", sel=7))       # grace
    assert len(_FakeTimer.instances) == 1


def test_cashout_all_cancels_pending_grace_timers():
    # CASHOUT singolo in grace, poi CASHOUT_ALL: l'ALL chiude tutto inline e
    # CANCELLA il timer pendente => nessun secondo green-up su target gia' chiuso
    # (race ALL vs grace, GPT-5.6 Terra/Fugu).
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig(market="1.1", sel=7))     # arma il grace
    t = _FakeTimer.instances[0]
    rc._route_cashout_signal(_sig(stype="CASHOUT_ALL", market="", sel=""))  # ALL inline
    assert t.cancelled is True                               # timer pendente cancellato
    assert rc._auto_green_pending == {}                      # guardia svuotata
    # l'ALL e' stato eseguito inline
    assert any(s.get("signal_type") == "CASHOUT_ALL" for s in rc.executed)


def test_cashout_without_target_routes_inline():
    # Un CASHOUT senza selection valorizzata non viene graziato (chiave
    # collasserebbe): route inline, nessun timer, nessun dedup.
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig(market="1.1", sel=""))
    assert rc.executed and _FakeTimer.instances == []


def test_signal_snapshot_deepcopy_isolates_from_caller_mutation():
    # Il payload differito e' una COPIA PROFONDA: mutare il dict del chiamante
    # dopo lo scheduling — anche strutture ANNIDATE — non cambia la route.
    rc = _RC(enabled=True)
    sig = _sig(market="1.1", sel=7)
    sig["legs"] = [{"price": 2.0, "stake": 10.0}]     # struttura annidata
    rc._route_cashout_signal(sig)
    sig["market_id"] = "9.9"                            # mutazione top-level
    sig["legs"][0]["price"] = 99.0                      # mutazione ANNIDATA
    _FakeTimer.instances[0].fire()
    assert rc.executed[0]["market_id"] == "1.1"
    assert rc.executed[0]["selection_id"] == 7
    assert rc.executed[0]["legs"][0]["price"] == 2.0   # deepcopy: annidato isolato


def test_pending_guard_reserve_release():
    rc = _RC()
    k = ("1.1", "7")
    assert rc._auto_green_reserve(k)[0] is True
    assert rc._auto_green_reserve(k)[0] is False   # gia' in volo
    rc._auto_green_release(k)
    assert rc._auto_green_reserve(k)[0] is True     # riammesso dopo release


def test_generational_tombstone_invalidates_pending_grace():
    # Race reserve->start: dopo reserve ma prima di arm/start, un CASHOUT_ALL
    # bumpa la generazione => arm rifiuta e il callback differito abortisce.
    rc = _RC(enabled=True)
    k = ("1.1", "7")
    acquired, gen = rc._auto_green_reserve(k)
    assert acquired is True
    rc._cancel_pending_auto_green()                 # CASHOUT_ALL nel mezzo
    # arm rifiuta (generazione avanzata) => non si avvia il timer
    assert rc._auto_green_arm_timer(k, _FakeTimer(1.0, lambda: None), gen) is False
    # e un callback differito con la vecchia gen si riconosce stale
    assert rc._auto_green_is_stale(gen) is True


def test_deferred_route_aborts_when_generation_stale():
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig(market="1.1", sel=7))   # arma il grace
    t = _FakeTimer.instances[0]
    rc._cancel_pending_auto_green()                        # ALL invalida la gen
    t.fire()                                               # il timer scatta comunque
    assert rc.executed == []                               # route NON eseguita (stale)


def test_stale_arm_does_not_clobber_re_reservation():
    # Sequenza reserve->ALL->re-reserve->arm-stale (Fugu/Fable): A prenota (gen0),
    # un CASHOUT_ALL svuota+bumpa (gen1), B RI-prenota lo STESSO target (gen1).
    # L'arm STALE di A (gen0) deve tornare False MA NON rimuovere la prenotazione
    # di B — un pop qui scarterebbe in silenzio il green-up legittimo di B.
    rc = _RC(enabled=True)
    k = ("1.1", "7")
    acquired_a, gen_a = rc._auto_green_reserve(k)          # A: gen0
    assert acquired_a is True and gen_a == 0
    rc._cancel_pending_auto_green()                        # CASHOUT_ALL: svuota + bump -> gen1
    acquired_b, gen_b = rc._auto_green_reserve(k)          # B ri-prenota lo stesso target
    assert acquired_b is True and gen_b == 1
    # arm STALE di A: rifiutato E NON deve fare pop della prenotazione di B.
    assert rc._auto_green_arm_timer(k, _FakeTimer(1.0, lambda: None), gen_a) is False
    assert k in rc._auto_green_pending                     # B ancora prenotato
    # B arma regolarmente sulla SUA generazione => e' il suo timer a restare.
    timer_b = _FakeTimer(1.0, lambda: None)
    assert rc._auto_green_arm_timer(k, timer_b, gen_b) is True
    assert rc._auto_green_pending[k] is timer_b


def test_stale_deferred_release_does_not_clobber_re_reservation():
    # Stessa classe sul path di RELEASE: la route differita STALE di A (gen0)
    # rilascia nel finally, ma un CASHOUT_ALL ha bumpato la generazione e B ha
    # RI-prenotato+armato lo stesso target (gen1). Il release stale NON deve
    # rimuovere il timer di B (altrimenti un ALL successivo non lo cancellerebbe,
    # o un duplicato passerebbe => doppio green-up).
    rc = _RC(enabled=True)
    k = ("1.1", "7")
    acquired_a, gen_a = rc._auto_green_reserve(k)          # A prenota (gen0)
    assert acquired_a is True                              # premessa: A prenota davvero
    assert rc._auto_green_arm_timer(k, _FakeTimer(1.0, lambda: None), gen_a) is True
    rc._cancel_pending_auto_green()                        # ALL: svuota + bump -> gen1
    acquired_b, gen_b = rc._auto_green_reserve(k)          # B ri-prenota (gen1)
    assert acquired_b is True                              # premessa: B ri-prenota dopo l'ALL
    timer_b = _FakeTimer(1.0, lambda: None)
    assert rc._auto_green_arm_timer(k, timer_b, gen_b) is True
    # release STALE di A (vecchia gen): la prenotazione di B DEVE sopravvivere.
    rc._auto_green_release(k, gen_a)
    assert rc._auto_green_pending.get(k) is timer_b
    # un release con la generazione CORRENTE (B) libera invece regolarmente.
    rc._auto_green_release(k, gen_b)
    assert k not in rc._auto_green_pending


def test_auto_green_key_shape():
    # Chiave = solo target (market, selection), indipendente dal signal_type.
    assert _RC._auto_green_key({"signal_type": "cashout", "market_id": "1.9", "selection_id": 3}) == ("1.9", "3")
    assert _RC._auto_green_key({"signal_type": "CASHOUT_ALL", "market_id": "1.9", "selection_id": 3}) == ("1.9", "3")


# ==========================================================================
# 3) Ri-check gate fail-closed dopo l'attesa
# ==========================================================================
def test_deferred_aborts_on_emergency_stop():
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())
    rc._emergency_stopped = True                 # stop scattato durante la grace
    _FakeTimer.instances[0].fire()
    assert rc.executed == []                      # route NON eseguita
    topic, payload = rc.published[-1]
    assert topic == CASHOUT_FAILED
    assert payload["reason"].startswith("grace_aborted:emergency_stop_active")
    assert rc._auto_green_pending == {}        # comunque rilasciata


def test_deferred_aborts_on_runtime_inactive():
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())
    rc._runtime_active_flag = False
    _FakeTimer.instances[0].fire()
    assert rc.executed == []
    assert rc.published[-1][1]["reason"].startswith("grace_aborted:runtime_non_attivo")


def test_deferred_aborts_on_session_invalid_live():
    rc = _RC(enabled=True)
    rc.execution_mode = "LIVE"
    rc._route_cashout_signal(_sig())
    rc.betfair_service._session_invalid = True
    _FakeTimer.instances[0].fire()
    assert rc.executed == []
    assert rc.published[-1][1]["reason"].startswith("grace_aborted:session_invalid")


def test_gates_open_when_all_clear():
    rc = _RC()
    ok, reason = rc._cashout_gates_still_open(_sig(), "SIMULATION")
    assert ok is True and reason == ""


def test_deferred_aborts_on_kill_switch():
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())
    rc._kill_switch = True                        # kill-switch armato durante la grace
    _FakeTimer.instances[0].fire()
    assert rc.executed == []
    assert rc.published[-1][1]["reason"].startswith("grace_aborted:kill_switch_active")


def test_deferred_aborts_on_execution_mode_flip():
    # Mode all'enqueue = SIMULATION; durante la grace flippa a LIVE => abort
    # (un cashout SIM non deve instradarsi live bypassando il deploy-gate).
    rc = _RC(enabled=True)
    rc._route_cashout_signal(_sig())              # enqueue_mode snapshot = SIMULATION
    rc.execution_mode = "LIVE"
    _FakeTimer.instances[0].fire()
    assert rc.executed == []
    assert rc.published[-1][1]["reason"].startswith("grace_aborted:execution_mode_changed")


# ==========================================================================
# 4) Eccezione nella route differita => CASHOUT_FAILED + release (Timer inghiotte)
# ==========================================================================
def test_deferred_route_exception_publishes_failed_and_releases():
    rc = _RC(enabled=True)

    def _boom(signal):
        raise RuntimeError("place kaput")

    rc._execute_cashout_route = _boom
    rc._route_cashout_signal(_sig())
    _FakeTimer.instances[0].fire()
    topic, payload = rc.published[-1]
    assert topic == CASHOUT_FAILED
    assert payload["reason"].startswith("grace_route_error:") and payload["status"] == "ERROR"
    assert rc._auto_green_pending == {}


# ==========================================================================
# 5) Helper _auto_green_delay_seconds: opt-in, fail-safe, clamp, fail-open
# ==========================================================================
def test_delay_seconds_disabled_zero():
    assert _RC(enabled=False, sec=2.5)._auto_green_delay_seconds() == 0.0


def test_delay_seconds_enabled_value():
    assert _RC(enabled=True, sec=1.25)._auto_green_delay_seconds() == 1.25


def test_delay_seconds_failsafe_to_constant():
    const = float(trading_config.AUTO_GREEN_DELAY_SEC)
    for bad in (None, 0.0, -3.0, float("nan"), float("inf"), "abc"):
        assert _RC(enabled=True, sec=bad)._auto_green_delay_seconds() == const


def test_delay_seconds_clamped_to_30():
    assert _RC(enabled=True, sec=3600.0)._auto_green_delay_seconds() == 30.0
    assert _RC(enabled=True, sec=30.0)._auto_green_delay_seconds() == 30.0


def test_delay_seconds_fail_open_on_config_error():
    rc = _RC(enabled=True)

    class _Boom:
        @property
        def auto_green_delay_enabled(self):
            raise RuntimeError("config illeggibile")

    rc.config = _Boom()
    assert rc._auto_green_delay_seconds() == 0.0


# ==========================================================================
# 6) CONFIG round-trip + GUI wiring
# ==========================================================================
def test_config_round_trip():
    from core.system_state import RoserpinaConfig
    from services.setting_service import SettingsService

    class _DB:
        def __init__(self):
            self._s = {}

        def get_settings(self):
            return dict(self._s)

        def save_settings(self, payload):
            self._s.update(dict(payload or {}))

    db = _DB()
    SettingsService(db).save_roserpina_config(
        RoserpinaConfig(table_count=3, auto_green_delay_enabled=True, auto_green_delay_sec=4.0)
    )
    r = SettingsService(db).load_roserpina_config()
    assert r.auto_green_delay_enabled is True and r.auto_green_delay_sec == 4.0


def test_config_default_disarmed():
    from services.setting_service import SettingsService

    class _Empty:
        def get_settings(self):
            return {}

        def save_settings(self, payload):
            pass

    cfg = SettingsService(_Empty()).load_roserpina_config()
    assert cfg.auto_green_delay_enabled is False
    assert cfg.auto_green_delay_sec == float(trading_config.AUTO_GREEN_DELAY_SEC)


# --- GUI (riusa l'harness fakes dell'integrazione) ---
import os  # noqa: E402
import sys  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "integration"))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402


class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(auto_green_delay_enabled=True, auto_green_delay_sec=6.0)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_auto_green_delay(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_auto_green_delay_enabled_var.get() is True
        assert app.rs_auto_green_delay_sec_var.get() == "6.0"
    finally:
        app.destroy()


def test_gui_saves_auto_green_delay(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_auto_green_delay_enabled_var.set(True)
        app.rs_auto_green_delay_sec_var.set("3")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.auto_green_delay_enabled is True
        assert app.settings_service.saved_cfg.auto_green_delay_sec == 3.0
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-1", "nan", "inf", "abc", "31", "60"])
def test_gui_save_blocks_invalid_delay_when_enabled(monkeypatch, bad):
    # Con grace ARMATO, un delay invalido blocca il salvataggio.
    app = _make_gui(monkeypatch)
    try:
        app.rs_auto_green_delay_enabled_var.set(True)
        app.rs_auto_green_delay_sec_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "abc", "60"])
def test_gui_save_tolerates_invalid_delay_when_disabled(monkeypatch, bad):
    # Con grace DISATTIVATO, un delay irrilevante NON blocca il salvataggio
    # (Greptile P2): si salva con fallback alla costante.
    app = _make_gui(monkeypatch)
    try:
        app.rs_auto_green_delay_enabled_var.set(False)
        app.rs_auto_green_delay_sec_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None, bad
        assert app.settings_service.saved_cfg.auto_green_delay_enabled is False
        assert app.settings_service.saved_cfg.auto_green_delay_sec == float(trading_config.AUTO_GREEN_DELAY_SEC)
    finally:
        app.destroy()
