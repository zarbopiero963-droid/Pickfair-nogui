"""PR2c — Min price (quota minima) editabile: floor di STRATEGIA sopra il minimo
Betfair inviolabile 1.01.

`MIN_PRICE` (1.02) era dead. Ora:
1. ENFORCEMENT: `DutchingController.validate` rifiuta `price < min_price` (config,
   editabile) OLTRE al floor hard `price <= 1.01` (immune-da-config, fail-safe).
2. CONFIG: RoserpinaConfig.min_price round-trip sul servizio reale.
3. GUI: campo "Quota minima" caricato/salvato, validato (>= 1.01).

Default 1.02 con comparatore `<` => behavior-preserving (sulla ladder Betfair non
esiste tick tra 1.01 e 1.02); morde solo se l'operatore alza il floor.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

import trading_config
from core.system_state import RoserpinaConfig
from services.settings_service import SettingsService

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration

_MP = max(1.01, float(trading_config.MIN_PRICE))  # floor fail-safe atteso


def _controller(config=None):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=None, runtime_controller=SimpleNamespace(config=config))


def _payload(price):
    return {
        "market_id": "1.3",
        "total_stake": 100.0,
        "selections": [{"selectionId": 1, "price": price, "side": "BACK"}],
    }


# ==========================================================================
# 1) ENFORCEMENT (validate)
# ==========================================================================
def test_validate_blocks_price_below_min_price():
    res = _controller(SimpleNamespace(min_price=1.5)).validate(_payload(1.2))
    assert res["ok"] is False
    assert "minimo configurato" in res["error"]


def test_validate_passes_price_at_or_above_min_price():
    res = _controller(SimpleNamespace(min_price=1.5)).validate(_payload(2.0))
    assert res["ok"] is True


def test_hard_floor_1_01_independent_of_config():
    # Anche con min_price valido = 1.01, la quota 1.01 e' rifiutata dal floor HARD
    # (price <= 1.01), immune-da-config.
    res = _controller(SimpleNamespace(min_price=1.01)).validate(_payload(1.01))
    assert res["ok"] is False


def test_default_config_is_behavior_preserving():
    cfg = SimpleNamespace(min_price=trading_config.MIN_PRICE)
    assert _controller(cfg).validate(_payload(1.02))["ok"] is True   # 1.02 non < 1.02
    assert _controller(cfg).validate(_payload(1.01))["ok"] is False  # hard floor


def test_failsafe_broken_config_never_drops_below_hard_floor():
    # Config rotta (min_price=1.0) => clamp a max(1.01, MIN_PRICE); il floor hard
    # 1.01 blocca comunque 1.01 e una quota sopra il clamp passa.
    cfg = SimpleNamespace(min_price=1.0)
    assert _controller(cfg).validate(_payload(1.01))["ok"] is False
    assert _controller(cfg).validate(_payload(2.0))["ok"] is True


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, _MP),
        (1.5, 1.5),
        (1.01, 1.01),
        (1.0, _MP),        # < 1.01 => clamp
        (1.005, _MP),      # < 1.01 => clamp
        (float("nan"), _MP),
        (float("inf"), _MP),
        ("abc", _MP),
    ],
)
def test_min_price_fail_safe(raw, expected):
    from controllers.dutching_controller import DutchingController

    cfg = SimpleNamespace() if raw is None else SimpleNamespace(min_price=raw)
    assert DutchingController._min_price(cfg) == expected


# ==========================================================================
# 2) CONFIG round-trip
# ==========================================================================
class _InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_min_price_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(RoserpinaConfig(table_count=3, min_price=1.35))
    assert SettingsService(db).load_roserpina_config().min_price == 1.35


def test_min_price_default_from_trading_config():
    assert SettingsService(_InMemoryDB()).load_roserpina_config().min_price == float(trading_config.MIN_PRICE)


# ==========================================================================
# 3) GUI wiring
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(min_price=1.40)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_min_price(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_min_price_var.get() == "1.4"
    finally:
        app.destroy()


def test_gui_saves_min_price(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_min_price_var.set("1.25")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.min_price == 1.25
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "1.0", "1.01", "1.005", "-1", "abc"])
def test_gui_save_blocks_invalid_min_price(monkeypatch, bad):
    # La GUI accetta solo >= 1.02 (minimo realmente raggiungibile: 1.01 e'
    # rifiutato dal floor hard del controller). 1.01 => niente save.
    app = _make_gui(monkeypatch)
    try:
        app.rs_min_price_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()
