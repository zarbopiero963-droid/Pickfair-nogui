"""G5 PR 4/5 — AUTO_GREEN_DELAY_SEC: grace OPT-IN prima del green-up.

`trading_config.AUTO_GREEN_DELAY_SEC` (2.5) era DEAD. Qui la CABLA come grace
OPT-IN in `CashoutExecutor.on_cmd_execute_cashout`:

1. DEFAULT DISARMATO: senza provider / con `auto_green_delay_enabled=False` =>
   NESSUNA attesa, place immediato (comportamento storico invariato).
2. ARMATO: attende `auto_green_delay_sec` DOPO la validazione fail-closed e PRIMA
   del place; il green-up viene comunque piazzato (mai strandato).
3. Payload INVALIDO => reject immediato, NESSUNA attesa.
4. FAIL-SAFE: sec assente/non-finito/<=0 => trading_config.AUTO_GREEN_DELAY_SEC.
5. CLAMP a [0, 30]s. FAIL-OPEN: provider che solleva => 0.0 (no-crash).
"""
from types import SimpleNamespace

import pytest

import trading_config
from cashout_executor import (
    CASHOUT_SUCCESS,
    CashoutExecutor,
    _MAX_AUTO_GREEN_DELAY_SEC,
)

from tests.unit.test_cashout_executor import (
    _FakeBus,
    _FakeRouter,
    _cmd_payload,
    _ok_result,
)


class _SleepSpy:
    """Cattura le chiamate a sleep senza attendere davvero."""

    def __init__(self):
        self.calls = []

    def __call__(self, seconds):
        self.calls.append(seconds)


def _cfg(enabled=True, sec=None):
    ns = SimpleNamespace(auto_green_delay_enabled=enabled)
    if sec is not None:
        ns.auto_green_delay_sec = sec
    return ns


def _run(*, delay_provider=None, sleep_spy=None, router=None, payload=None):
    bus = _FakeBus()
    router = router or _FakeRouter(_ok_result())
    sleep_spy = sleep_spy or _SleepSpy()
    ex = CashoutExecutor(bus, router, delay_provider=delay_provider, sleep_fn=sleep_spy)
    ex.on_cmd_execute_cashout(payload if payload is not None else _cmd_payload())
    return bus, router, sleep_spy


# ==========================================================================
# 1) Default disarmato => nessuna attesa, place immediato
# ==========================================================================
def test_no_provider_no_delay_places_immediately():
    bus, router, sleep_spy = _run(delay_provider=None)
    assert sleep_spy.calls == []                 # nessuna attesa
    assert len(router.calls) == 1                # green-up piazzato
    assert bus.last(CASHOUT_SUCCESS) is not None


def test_disabled_config_no_delay():
    bus, router, sleep_spy = _run(delay_provider=lambda: _cfg(enabled=False, sec=2.5))
    assert sleep_spy.calls == []
    assert len(router.calls) == 1


# ==========================================================================
# 2) Armato => attende PRIMA del place, ma piazza comunque
# ==========================================================================
def test_enabled_waits_then_places():
    order = []
    sleep_spy = _SleepSpy()

    class _RecordingRouter(_FakeRouter):
        def place(self, payload):
            order.append("place")
            return super().place(payload)

    real_sleep = sleep_spy.__call__

    def sleep_and_record(sec):
        order.append("sleep")
        real_sleep(sec)

    bus, router, _ = _run(
        delay_provider=lambda: _cfg(enabled=True, sec=2.5),
        sleep_spy=sleep_and_record,
        router=_RecordingRouter(_ok_result()),
    )
    assert sleep_spy.calls == [2.5]
    assert order == ["sleep", "place"]           # attende PRIMA di piazzare
    assert bus.last(CASHOUT_SUCCESS) is not None  # green-up comunque eseguito


def test_enabled_uses_configured_seconds():
    _, _, sleep_spy = _run(delay_provider=lambda: _cfg(enabled=True, sec=1.25))
    assert sleep_spy.calls == [1.25]


# ==========================================================================
# 3) Payload invalido => reject immediato, nessuna attesa
# ==========================================================================
def test_invalid_payload_rejects_without_waiting():
    sleep_spy = _SleepSpy()
    bus, router, _ = _run(
        delay_provider=lambda: _cfg(enabled=True, sec=2.5),
        sleep_spy=sleep_spy,
        payload=_cmd_payload(price=1.0),   # price<=1 => REJECTED hard-invariant
    )
    assert sleep_spy.calls == []           # non ha atteso su un payload invalido
    assert router.calls == []              # e non ha piazzato nulla


# ==========================================================================
# 4) Helper _auto_green_delay_seconds: fail-safe, clamp, fail-open
# ==========================================================================
def _delay(provider):
    ex = CashoutExecutor(_FakeBus(), _FakeRouter(), delay_provider=provider)
    return ex._auto_green_delay_seconds()


def test_delay_seconds_disabled_is_zero():
    assert _delay(lambda: _cfg(enabled=False, sec=2.5)) == 0.0
    assert _delay(None) == 0.0


def test_delay_seconds_failsafe_to_constant():
    const = float(trading_config.AUTO_GREEN_DELAY_SEC)
    for bad in (None, 0.0, -3.0, float("nan"), float("inf"), "abc"):
        assert _delay(lambda b=bad: _cfg(enabled=True, sec=b)) == const
    # attributo del tutto assente => fail-safe costante
    assert _delay(lambda: SimpleNamespace(auto_green_delay_enabled=True)) == const


def test_delay_seconds_clamped_to_max():
    assert _delay(lambda: _cfg(enabled=True, sec=3600.0)) == _MAX_AUTO_GREEN_DELAY_SEC
    assert _delay(lambda: _cfg(enabled=True, sec=_MAX_AUTO_GREEN_DELAY_SEC)) == _MAX_AUTO_GREEN_DELAY_SEC
    assert _delay(lambda: _cfg(enabled=True, sec=2.5)) == 2.5


def test_delay_seconds_fail_open_on_provider_error():
    def boom():
        raise RuntimeError("config illeggibile")

    assert _delay(boom) == 0.0                       # mai bloccare il cashout
    # e nel flusso completo: nessuna attesa, place immediato
    _, router, sleep_spy = _run(delay_provider=boom)
    assert sleep_spy.calls == []
    assert len(router.calls) == 1


# ==========================================================================
# 5) CONFIG round-trip + GUI wiring
# ==========================================================================
def test_config_round_trip():
    from core.system_state import RoserpinaConfig
    from services.setting_service import SettingsService

    class _InMemoryDB:
        def __init__(self):
            self._s = {}

        def get_settings(self):
            return dict(self._s)

        def save_settings(self, payload):
            self._s.update(dict(payload or {}))

    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(
        RoserpinaConfig(table_count=3, auto_green_delay_enabled=True, auto_green_delay_sec=4.0)
    )
    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.auto_green_delay_enabled is True
    assert reloaded.auto_green_delay_sec == 4.0


def test_config_default_disarmed():
    from services.setting_service import SettingsService

    class _EmptyDB:
        def get_settings(self):
            return {}

        def save_settings(self, payload):
            pass

    cfg = SettingsService(_EmptyDB()).load_roserpina_config()
    assert cfg.auto_green_delay_enabled is False
    assert cfg.auto_green_delay_sec == float(trading_config.AUTO_GREEN_DELAY_SEC)


# ==========================================================================
# 6) GUI wiring (tab Roserpina) — riusa l'harness fakes dell'integrazione
# ==========================================================================
import os  # noqa: E402
import sys  # noqa: E402

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "integration"))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402


class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(auto_green_delay_enabled=True, auto_green_delay_sec=7.0)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_auto_green_delay(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_auto_green_delay_enabled_var.get() is True
        assert app.rs_auto_green_delay_sec_var.get() == "7.0"
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
def test_gui_save_blocks_invalid_auto_green_delay(monkeypatch, bad):
    # finito > 0 e <= 30 (coerente col clamp runtime); fuori range => nessun save.
    app = _make_gui(monkeypatch)
    try:
        app.rs_auto_green_delay_sec_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()
