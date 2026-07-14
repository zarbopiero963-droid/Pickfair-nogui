"""G5 PR 3/5 — MAX_STAKE_PCT: WARNING non-bloccante sull'esposizione dell'operazione.

`trading_config.MAX_STAKE_PCT` (0.30) era DEAD (solo display). Qui:
1. WARNING (mai blocco): `DutchingController.precheck` espone `stake_pct_warning`
   nel risultato _ok se l'esposizione reale di QUESTA operazione (batch_exposure:
   BACK stake / LAY liability) supera bankroll * max_stake_pct (default 30%).
2. DISTINTO dai gate bloccanti (max_total_exposure_pct 35% e' il BLOCCO cumulativo;
   qui si misura la sola operazione, stand-alone). NON blocca MAI.
3. FAIL-OPEN su bankroll<=0. FAIL-SAFE config rotta => trading_config.MAX_STAKE_PCT.
4. Scala: config in percentuale (0-100), costante fallback in frazione (0.30).
5. CONFIG round-trip + GUI.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

import trading_config
from core.system_state import RoserpinaConfig
from services.setting_service import SettingsService

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration


# ==========================================================================
# Harness minimale (self-contained) per pilotare precheck.
# ==========================================================================
class _Mode:
    value = "ACTIVE"


class _Config:
    anti_duplication_enabled = True
    allow_recovery = True
    # Gate bloccanti RILASSATI (100%) => non interferiscono col warning testato.
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0
    # max_stake_pct volutamente ASSENTE di default: verifica fallback costante.


class _RiskDesk:
    bankroll_current = 1000.0


class _DupGuard:
    def acquire(self, key):
        return True

    def release(self, key):
        pass


class _TableManager:
    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        return None


class _Runtime:
    def __init__(self):
        self.mode = _Mode()
        self.config = _Config()
        self.risk_desk = _RiskDesk()
        self.duplication_guard = _DupGuard()
        self.table_manager = _TableManager()
        self.dutching_batch_manager = None


def _payload():
    return {
        "market_id": "1.300",
        "total_stake": 100.0,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "BACK"},
            {"selectionId": 2, "price": 3.0, "side": "BACK"},
        ],
    }


def _patch_results(monkeypatch, results, book_pct=100.0):
    def fake_calc(selections, total_stake):
        _ = selections, total_stake
        return (list(results), 1.0, float(book_pct))

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calc)


def _controller(runtime=None):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=None, runtime_controller=runtime or _Runtime())


def _runtime_with(max_stake_pct=None, bankroll=None):
    rt = _Runtime()
    if max_stake_pct is not None:
        rt.config.max_stake_pct = max_stake_pct
    if bankroll is not None:
        rt.risk_desk = _RiskDesk()
        rt.risk_desk.bankroll_current = bankroll
    return rt


# Bankroll 1000, default 30% => soglia 300.
_BACK_OVER = [{"selectionId": 1, "price": 2.0, "stake": 200.0, "side": "BACK"},
              {"selectionId": 2, "price": 3.0, "stake": 200.0, "side": "BACK"}]  # batch 400 > 300
_BACK_UNDER = [{"selectionId": 1, "price": 2.0, "stake": 100.0, "side": "BACK"},
               {"selectionId": 2, "price": 3.0, "stake": 100.0, "side": "BACK"}]  # batch 200 < 300


# ==========================================================================
# 1) WARNING (mai blocco)
# ==========================================================================
def test_warns_when_operation_exposure_over_pct(monkeypatch):
    # batch_exposure 400 > 30% di 1000 (=300) => stake_pct_warning=True, MA ok=True.
    _patch_results(monkeypatch, _BACK_OVER)
    res = _controller().precheck(_payload())
    assert res["ok"] is True, res
    assert res["stake_pct_warning"] is True
    assert res["stake_pct_ratio"] == 0.4
    assert res["max_stake_pct"] == round(float(trading_config.MAX_STAKE_PCT), 4)


def test_no_warning_when_under_pct(monkeypatch):
    # batch 200 < 300 => nessun warning.
    _patch_results(monkeypatch, _BACK_UNDER)
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["stake_pct_warning"] is False


def test_never_blocks_even_far_over_threshold(monkeypatch):
    # Anche con soglia bassissima (1% => 10) e batch 400, il warning NON blocca MAI.
    _patch_results(monkeypatch, _BACK_OVER)
    res = _controller(_runtime_with(max_stake_pct=1.0)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["stake_pct_warning"] is True


def test_max_stake_pct_is_config_editable(monkeypatch):
    # Con soglia 50% (=500), batch 400 NON warna; con 30% (=300) warna.
    _patch_results(monkeypatch, _BACK_OVER)
    assert _controller(_runtime_with(max_stake_pct=50.0)).precheck(_payload())["stake_pct_warning"] is False
    _patch_results(monkeypatch, _BACK_OVER)
    assert _controller(_runtime_with(max_stake_pct=30.0)).precheck(_payload())["stake_pct_warning"] is True


def test_lay_uses_liability_for_stake_pct(monkeypatch):
    # LAY stake 100 @ 5: liability = 100*(5-1) = 400 > 300 => warna. Il backer-stake
    # 100 (< 300) NON basterebbe: prova che si usa l'esposizione reale (liability).
    _patch_results(monkeypatch, [{"selectionId": 7, "price": 5.0, "stake": 100.0, "side": "LAY"}])
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["stake_pct_warning"] is True
    assert res["stake_pct_ratio"] == 0.4


def test_fail_open_bankroll_zero(monkeypatch):
    # bankroll 0 => nessun warning (fail-open), nessun blocco.
    _patch_results(monkeypatch, _BACK_OVER)
    res = _controller(_runtime_with(bankroll=0.0)).precheck(_payload())
    assert res["ok"] is True
    assert res["stake_pct_warning"] is False
    assert res["stake_pct_ratio"] == 0.0


# ==========================================================================
# 2) Helper _max_stake_pct (scala + fail-safe)
# ==========================================================================
def test_max_stake_pct_scale_and_failsafe():
    from controllers.dutching_controller import DutchingController

    f = DutchingController._max_stake_pct
    assert f(SimpleNamespace(max_stake_pct=30.0)) == 0.30   # percentuale => frazione
    assert f(SimpleNamespace(max_stake_pct=50.0)) == 0.50
    assert f(SimpleNamespace(max_stake_pct=100.0)) == 1.0    # boundary superiore accettato
    assert f(None) == float(trading_config.MAX_STAKE_PCT)    # config None => fail-safe, no TypeError
    # assente / rotta / FUORI 0-100 => fallback costante (frazione). Il > 100 e'
    # fail-safe anche a runtime (non solo GUI): non deve diventare frazione > 1.
    assert f(SimpleNamespace()) == float(trading_config.MAX_STAKE_PCT)
    for bad in (0.0, -5.0, float("nan"), float("inf"), "abc", 100.01, 200.0, 1000.0):
        assert f(SimpleNamespace(max_stake_pct=bad)) == float(trading_config.MAX_STAKE_PCT)


# ==========================================================================
# 3) CONFIG round-trip
# ==========================================================================
class _InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_max_stake_pct_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(RoserpinaConfig(table_count=3, max_stake_pct=22.0))
    assert SettingsService(db).load_roserpina_config().max_stake_pct == 22.0


def test_max_stake_pct_default_from_constant():
    # Assente => default = trading_config.MAX_STAKE_PCT * 100 (percentuale).
    reloaded = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert reloaded.max_stake_pct == float(trading_config.MAX_STAKE_PCT) * 100.0


# ==========================================================================
# 4) GUI wiring (tab Roserpina)
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(max_stake_pct=27.0)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_max_stake_pct(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_max_stake_pct_var.get() == "27.0"
    finally:
        app.destroy()


def test_gui_saves_max_stake_pct(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_stake_pct_var.set("40")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.max_stake_pct == 40.0
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-5", "nan", "inf", "abc", "200", "150.5", "100.01"])
def test_gui_save_blocks_invalid_max_stake_pct(monkeypatch, bad):
    # Include > 100: scala percentuale 0-100 => un valore oltre 100 disabiliterebbe
    # silenziosamente l'avviso (frazione > 1) => rifiuto esplicito.
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_stake_pct_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()


def test_gui_saves_max_stake_pct_boundary_100(monkeypatch):
    # 100 (estremo superiore del contratto 0-100) e' ACCETTATO.
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_stake_pct_var.set("100")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.max_stake_pct == 100.0
    finally:
        app.destroy()
