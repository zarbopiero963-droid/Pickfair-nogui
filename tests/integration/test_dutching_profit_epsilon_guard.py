"""G5 PR 5/5 — PROFIT_EPSILON: WARNING non-bloccante sulla varianza di profitto tra esiti.

`trading_config.PROFIT_EPSILON` (0.50) era DEAD. Qui (decisioni owner: SOLO AVVISO;
netti, solo con equalize=ON):
1. WARNING (mai blocco): `DutchingController.precheck` espone `profit_epsilon_warning`
   + `profit_spread` + `profit_epsilon` nel risultato _ok se
   max(profitIfWinsNet) - min(profitIfWinsNet) tra gli esiti equalizzati supera la
   tolleranza (€, default 0.50). NON blocca MAI.
2. Netti (post-commission): usa profitIfWinsNet. Il controller usa sempre il calcolo
   equalizzato (calculate_dutching default equalize=True), quindi lo spread e' quello
   post-equalizzazione.
3. FAIL-OPEN: netti incompleti / < 2 esiti => nessun avviso. FAIL-SAFE config rotta
   => trading_config.PROFIT_EPSILON.
4. CONFIG round-trip + GUI.
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


class _Mode:
    value = "ACTIVE"


class _Config:
    anti_duplication_enabled = True
    allow_recovery = True
    # Gate bloccanti RILASSATI (100%) => non interferiscono col warning testato.
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0
    # profit_epsilon volutamente ASSENTE di default: verifica fallback costante.


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


def _res(nets, *, with_net=True):
    """Costruisce results con profitIfWinsNet controllati (uno spread noto).

    ``with_net=False`` OMETTE profitIfWinsNet dall'ULTIMO item => netti incompleti
    (verifica il FAIL-OPEN di _profit_spread_net).
    """
    out = []
    for i, net in enumerate(nets):
        item = {
            "selectionId": i + 1,
            "price": 2.0,
            "stake": 50.0,
            "side": "BACK",
            "profitIfWins": float(net),
        }
        if with_net or i < len(nets) - 1:
            item["profitIfWinsNet"] = float(net)
        out.append(item)
    return out


def _patch_results(monkeypatch, results, book_pct=50.0):
    def fake_calc(selections, total_stake):
        _ = selections, total_stake
        return (list(results), 1.0, float(book_pct))

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calc)


def _controller(runtime=None):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=None, runtime_controller=runtime or _Runtime())


def _runtime_with(profit_epsilon=None, enabled=None):
    rt = _Runtime()
    if profit_epsilon is not None:
        rt.config.profit_epsilon = profit_epsilon
    if enabled is not None:
        rt.config.profit_epsilon_enabled = enabled
    return rt


# ==========================================================================
# 1) WARNING (mai blocco)
# ==========================================================================
def test_warns_when_profit_spread_over_epsilon(monkeypatch):
    # spread netto = 5.60 - 5.00 = 0.60 > 0.50 (costante) => warning, MA ok=True.
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    res = _controller().precheck(_payload())
    assert res["ok"] is True, res
    assert res["profit_epsilon_warning"] is True
    assert res["profit_spread"] == 0.60
    assert res["profit_epsilon"] == round(float(trading_config.PROFIT_EPSILON), 2)


def test_disabled_suppresses_warning(monkeypatch):
    # Checkbox OFF (profit_epsilon_enabled=False): anche con spread 0.60 > 0.50 NON
    # warna. profit_spread resta comunque esposto (informativo), profit_epsilon_enabled
    # riportato nel risultato.
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    res = _controller(_runtime_with(enabled=False)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["profit_epsilon_warning"] is False
    assert res["profit_epsilon_enabled"] is False
    assert res["profit_spread"] == 0.60


def test_enabled_by_default_warns(monkeypatch):
    # Config senza profit_epsilon_enabled => default ON => warna (spread 0.60 > 0.50).
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    res = _controller().precheck(_payload())
    assert res["profit_epsilon_enabled"] is True
    assert res["profit_epsilon_warning"] is True


def test_no_warning_when_spread_under_epsilon(monkeypatch):
    # spread 5.30 - 5.00 = 0.30 < 0.50 => nessun warning.
    _patch_results(monkeypatch, _res([5.00, 5.30]))
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["profit_epsilon_warning"] is False
    assert res["profit_spread"] == 0.30


def test_spread_display_matches_decision_at_cent(monkeypatch):
    # Spread reale 0.504 => arrotondato a 0.50: NON warna, e profit_spread mostrato
    # 0.50 e' coerente col non-avviso (niente "0.50 con avviso attivo"). 0.506 =>
    # 0.51 > 0.50 => warna, mostrato 0.51. Display e decisione coincidono (Fable).
    _patch_results(monkeypatch, _res([5.000, 5.504]))
    res = _controller().precheck(_payload())
    assert res["profit_spread"] == 0.50
    assert res["profit_epsilon_warning"] is False
    _patch_results(monkeypatch, _res([5.000, 5.506]))
    res2 = _controller().precheck(_payload())
    assert res2["profit_spread"] == 0.51
    assert res2["profit_epsilon_warning"] is True


def test_never_blocks_even_far_over_threshold(monkeypatch):
    # Epsilon minuscolo (0.01) e spread 0.60: il warning NON blocca MAI.
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    res = _controller(_runtime_with(profit_epsilon=0.01)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["profit_epsilon_warning"] is True


def test_profit_epsilon_is_config_editable(monkeypatch):
    # Con tolleranza 1.00 (> spread 0.60) NON warna; con 0.50 (< 0.60) warna.
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    assert _controller(_runtime_with(profit_epsilon=1.00)).precheck(_payload())["profit_epsilon_warning"] is False
    _patch_results(monkeypatch, _res([5.00, 5.60]))
    assert _controller(_runtime_with(profit_epsilon=0.50)).precheck(_payload())["profit_epsilon_warning"] is True


def test_fail_open_incomplete_net_profits(monkeypatch):
    # Un esito SENZA profitIfWinsNet => spread non calcolabile => None => nessun
    # warning (fail-open), profit_spread=None, nessun blocco.
    _patch_results(monkeypatch, _res([5.00, 5.60], with_net=False))
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["profit_epsilon_warning"] is False
    assert res["profit_spread"] is None


def test_fail_open_explicit_none_net(monkeypatch):
    # Un esito con profitIfWinsNet=None ESPLICITO (chiave presente, valore None):
    # netto assente => FAIL-OPEN (NON coerciito a 0.0 => nessuno spread fittizio, nessun
    # avviso spurio). Fable/Greptile P2.
    results = _res([5.00, 5.60])
    results[1]["profitIfWinsNet"] = None
    _patch_results(monkeypatch, results)
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["profit_epsilon_warning"] is False
    assert res["profit_spread"] is None


def test_fail_open_single_outcome(monkeypatch):
    # Meno di 2 esiti comparabili => nessuno spread => nessun warning.
    _patch_results(monkeypatch, _res([5.00]))
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["profit_epsilon_warning"] is False
    assert res["profit_spread"] is None


# ==========================================================================
# 2) Helper _profit_epsilon (fail-safe) e _profit_spread_net (fail-open)
# ==========================================================================
def test_profit_epsilon_helper_failsafe():
    from controllers.dutching_controller import DutchingController

    f = DutchingController._profit_epsilon
    assert f(SimpleNamespace(profit_epsilon=0.75)) == 0.75
    assert f(None) == float(trading_config.PROFIT_EPSILON)      # config None => no crash
    assert f(SimpleNamespace()) == float(trading_config.PROFIT_EPSILON)
    for bad in (0.0, -1.0, float("nan"), float("inf"), "abc"):
        assert f(SimpleNamespace(profit_epsilon=bad)) == float(trading_config.PROFIT_EPSILON)


def test_profit_epsilon_enabled_helper():
    from controllers.dutching_controller import DutchingController

    f = DutchingController._profit_epsilon_enabled
    assert f(None) is True                                   # config None => default ON
    assert f(SimpleNamespace()) is True                      # attributo assente => ON
    assert f(SimpleNamespace(profit_epsilon_enabled=True)) is True
    assert f(SimpleNamespace(profit_epsilon_enabled=False)) is False
    for off in ("0", "false", "False", "no", "off", ""):
        assert f(SimpleNamespace(profit_epsilon_enabled=off)) is False
    for on in ("1", "true", "yes", "on"):
        assert f(SimpleNamespace(profit_epsilon_enabled=on)) is True


def test_profit_spread_net_helper():
    from controllers.dutching_controller import DutchingController

    f = DutchingController._profit_spread_net
    assert f(_res([5.0, 5.6])) == pytest.approx(0.60)
    assert f(_res([1.0, 2.0, 1.5])) == pytest.approx(1.0)
    assert f(_res([5.0])) is None                       # < 2 esiti
    assert f(_res([5.0, 5.6], with_net=False)) is None  # netti incompleti
    assert f([]) is None
    r_none = _res([5.0, 5.6]); r_none[1]["profitIfWinsNet"] = None
    assert f(r_none) is None                            # None esplicito => fail-open
    r_inf = _res([5.0, 5.6]); r_inf[1]["profitIfWinsNet"] = float("inf")
    assert f(r_inf) is None                             # non-finito => fail-open


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


def test_profit_epsilon_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(RoserpinaConfig(table_count=3, profit_epsilon=0.75))
    assert SettingsService(db).load_roserpina_config().profit_epsilon == 0.75


def test_profit_epsilon_default_from_constant():
    reloaded = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert reloaded.profit_epsilon == float(trading_config.PROFIT_EPSILON)


def test_profit_epsilon_enabled_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(RoserpinaConfig(table_count=3, profit_epsilon_enabled=False))
    assert SettingsService(db).load_roserpina_config().profit_epsilon_enabled is False
    # Default (assente) => True.
    assert SettingsService(_InMemoryDB()).load_roserpina_config().profit_epsilon_enabled is True


# ==========================================================================
# 4) GUI wiring (tab Roserpina)
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(profit_epsilon=0.80, profit_epsilon_enabled=False)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_profit_epsilon(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_profit_epsilon_var.get() == "0.8"
    finally:
        app.destroy()


def test_gui_saves_profit_epsilon(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_profit_epsilon_var.set("0.25")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.profit_epsilon == 0.25
    finally:
        app.destroy()


def test_gui_loads_profit_epsilon_enabled(monkeypatch):
    # Il service finto ritorna profit_epsilon_enabled=False => checkbox non spuntata.
    app = _make_gui(monkeypatch)
    try:
        assert bool(app.rs_profit_epsilon_enabled_var.get()) is False
    finally:
        app.destroy()


def test_gui_saves_profit_epsilon_enabled(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_profit_epsilon_enabled_var.set(True)
        app.rs_profit_epsilon_var.set("0.5")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert bool(app.settings_service.saved_cfg.profit_epsilon_enabled) is True
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-5", "nan", "inf", "abc"])
def test_gui_save_blocks_invalid_profit_epsilon(monkeypatch, bad):
    # Tolleranza € > 0 (finita): vuoto / <=0 / non numerico => rifiuto (no save).
    app = _make_gui(monkeypatch)
    try:
        app.rs_profit_epsilon_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()
