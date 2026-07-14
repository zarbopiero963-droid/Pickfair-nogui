"""#383 — Liquidity guard: BLOCCO reale (opt-in) al submit dutching.

Il guard calcola la liquidita' ESEGUIBILE per gamba sul book REALE (lato
Betfair-standard: BACK -> availableToBack, LAY -> availableToLay, filtrata per
prezzo eseguibile) e:
- se `liquidity_warning_only=False` (blocco armato) => BLOCCA il submit;
- se `liquidity_warning_only=True` (default opt-in) => solo AVVISO
  (`liquidity_warning`/`liquidity_shortfall`).
Book assente => FAIL-OPEN (nessuno shortfall => nessun blocco).

Coperto: blocco quando insufficiente (block mode), avviso senza blocco (warning
mode), nessun blocco quando sufficiente, soglia block/allow, gambe LAY
(availableToLay), fail-open (book/selezione mancante), guard-off, filtro prezzo,
side-mapping (BACK legge availableToBack), helper fail-safe, round-trip config,
wiring + validazione GUI.
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


# ==========================================================================
# Harness precheck (self-contained) con market book pilotabile.
# ==========================================================================
class _Mode:
    value = "ACTIVE"


class _Config:
    anti_duplication_enabled = True
    allow_recovery = True
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0
    book_warning = 105.0
    book_block = 110.0
    liquidity_guard_enabled = True
    liquidity_multiplier = 3.0
    min_liquidity_absolute = 50.0
    liquidity_warning_only = False  # block mode di default in questi test


class _RiskDesk:
    bankroll_current = 1000.0


class _DupGuard:
    @staticmethod
    def acquire(key):
        return True


class _TableManager:
    @staticmethod
    def total_exposure():
        return 0.0

    @staticmethod
    def find_by_event_key(event_key):
        return None


class _MarketTracker:
    def __init__(self, book):
        self._book = book

    def get_market(self, market_id):
        return self._book


class _Runtime:
    def __init__(self, book=None, config=None):
        self.mode = _Mode()
        self.config = config if config is not None else _Config()
        self.risk_desk = _RiskDesk()
        self.duplication_guard = _DupGuard()
        self.table_manager = _TableManager()
        self.dutching_batch_manager = None
        self.market_tracker = _MarketTracker(book)


def _cfg(warning_only=False, guard_enabled=True):
    c = _Config()
    c.liquidity_warning_only = warning_only
    c.liquidity_guard_enabled = guard_enabled
    return c


def _book(back_size, selection_ids=(1, 2), back_price=2.0):
    # Gambe di test = BACK => il gate legge availableToBack (Betfair-standard).
    # availableToLay e' volutamente ALTO: se il mapping fosse errato (lato sim)
    # leggerebbe 9999 e non bloccherebbe/segnalerebbe mai.
    return {
        "runners": [
            {
                "selectionId": sid,
                "ex": {
                    "availableToBack": [{"price": float(back_price), "size": float(back_size)}],
                    "availableToLay": [{"price": 2.0, "size": 9999.0}],
                },
            }
            for sid in selection_ids
        ]
    }


def _book_two_levels():
    # BACK@2.0: un livello eseguibile (price 2.0, size 5) + uno NON eseguibile
    # (price 1.5 < 2.0, size 9999). Solo il primo conta (BACK: book_price>=order).
    return {
        "runners": [
            {
                "selectionId": sid,
                "ex": {
                    "availableToBack": [
                        {"price": 2.0, "size": 5.0},
                        {"price": 1.5, "size": 9999.0},
                    ],
                    "availableToLay": [{"price": 2.0, "size": 9999.0}],
                },
            }
            for sid in (1, 2)
        ]
    }


def _lay_book(lay_size, selection_ids=(1, 2)):
    # Gambe LAY => il gate legge availableToLay. availableToBack alto come decoy.
    return {
        "runners": [
            {
                "selectionId": sid,
                "ex": {
                    "availableToLay": [{"price": 2.0, "size": float(lay_size)}],
                    "availableToBack": [{"price": 2.0, "size": 9999.0}],
                },
            }
            for sid in selection_ids
        ]
    }


def _payload():
    return {
        "market_id": "1.300",
        "total_stake": 100.0,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "BACK"},
            {"selectionId": 2, "price": 2.0, "side": "BACK"},
        ],
    }


def _payload_lay():
    return {
        "market_id": "1.300",
        "total_stake": 100.0,
        "selections": [
            {"selectionId": 1, "price": 2.0, "side": "LAY"},
            {"selectionId": 2, "price": 2.0, "side": "LAY"},
        ],
    }


def _patch_calc(monkeypatch, side="BACK", book_pct=90.0):
    def fake(selections, total_stake):
        _ = selections, total_stake
        leg = {"price": 2.0, "stake": 50.0, "side": side}
        if side == "LAY":
            leg["liability"] = 50.0  # stake*(price-1)
        return (
            [dict(leg, selectionId=1), dict(leg, selectionId=2)],
            1.0,
            float(book_pct),
        )

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake)


def _controller(runtime):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=None, runtime_controller=runtime)


# ==========================================================================
# 1) BLOCCO (block mode: liquidity_warning_only=False)
# ==========================================================================
def test_blocks_when_insufficient(monkeypatch):
    # stake 50 * mult 3 = 150 richiesti; eseguibile (availableToBack@2.0) = 10 => BLOCCA.
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(10.0), config=_cfg(warning_only=False))).precheck(_payload())
    assert res["ok"] is False, res
    assert res.get("liquidity_shortfall"), res


def test_no_block_when_sufficient(monkeypatch):
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(1000.0), config=_cfg(warning_only=False))).precheck(_payload())
    assert res["ok"] is True
    assert res.get("liquidity_warning") is False


def test_block_threshold_boundary(monkeypatch):
    # required = 50*3 = 150; blocca se available < 150.
    _patch_calc(monkeypatch)
    assert _controller(_Runtime(book=_book(149.0), config=_cfg())).precheck(_payload())["ok"] is False
    assert _controller(_Runtime(book=_book(150.0), config=_cfg())).precheck(_payload())["ok"] is True


def test_lay_leg_reads_available_to_lay_and_blocks(monkeypatch):
    # Gambe LAY: legge availableToLay (10 insufficiente). stake=backer-stake.
    _patch_calc(monkeypatch, side="LAY")
    res = _controller(_Runtime(book=_lay_book(10.0), config=_cfg(warning_only=False))).precheck(_payload_lay())
    assert res["ok"] is False, res


def test_lay_leg_sufficient_allows(monkeypatch):
    _patch_calc(monkeypatch, side="LAY")
    res = _controller(_Runtime(book=_lay_book(1000.0), config=_cfg(warning_only=False))).precheck(_payload_lay())
    assert res["ok"] is True, res


# ==========================================================================
# 2) AVVISO (warning mode: liquidity_warning_only=True) => non blocca
# ==========================================================================
def test_warns_but_allows_in_warning_only_mode(monkeypatch):
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(10.0), config=_cfg(warning_only=True))).precheck(_payload())
    assert res["ok"] is True, res
    assert res.get("liquidity_warning") is True
    assert res["liquidity_shortfall"], res


def test_side_mapping_back_reads_available_to_back(monkeypatch):
    # BACK legge availableToBack (10, insufficiente), NON availableToLay (9999).
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(10.0), config=_cfg(warning_only=True))).precheck(_payload())
    assert res.get("liquidity_warning") is True


def test_price_filter_excludes_non_executable_levels(monkeypatch):
    # Solo il livello eseguibile (5.0 @2.0) conta; il non eseguibile (9999 @1.5)
    # e' escluso => 5 < 150 => warning.
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book_two_levels(), config=_cfg(warning_only=True))).precheck(_payload())
    assert res.get("liquidity_warning") is True
    assert res["liquidity_shortfall"][0]["available"] == 5.0


# ==========================================================================
# 3) FAIL-OPEN e guard-off (mai blocco anche in block mode)
# ==========================================================================
def test_fail_open_when_book_missing_block_mode(monkeypatch):
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=None, config=_cfg(warning_only=False))).precheck(_payload())
    assert res["ok"] is True
    assert res.get("liquidity_warning") is False


def test_fail_open_when_selection_absent_block_mode(monkeypatch):
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(10.0, selection_ids=(999,)), config=_cfg(warning_only=False))).precheck(_payload())
    assert res["ok"] is True


def test_guard_disabled_no_block(monkeypatch):
    _patch_calc(monkeypatch)
    res = _controller(_Runtime(book=_book(10.0), config=_cfg(warning_only=False, guard_enabled=False))).precheck(_payload())
    assert res["ok"] is True
    assert res.get("liquidity_warning") is False
    assert res.get("liquidity_shortfall") == []


# ==========================================================================
# 4) Helper fail-safe
# ==========================================================================
@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, 50.0),
        (0.0, 0.0),
        (25.0, 25.0),
        (-5.0, 50.0),
        (float("nan"), 50.0),
        (float("inf"), 50.0),
        ("abc", 50.0),
    ],
)
def test_min_liquidity_absolute_fail_safe(raw, expected):
    from controllers.dutching_controller import DutchingController

    cfg = SimpleNamespace() if raw is None else SimpleNamespace(min_liquidity_absolute=raw)
    assert DutchingController._min_liquidity_absolute(cfg) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, bool(trading_config.LIQUIDITY_WARNING_ONLY)),  # default OPT-IN (True)
        (True, True),
        (False, False),
    ],
)
def test_liquidity_warning_only_default(raw, expected):
    from controllers.dutching_controller import DutchingController

    cfg = SimpleNamespace() if raw is None else SimpleNamespace(liquidity_warning_only=raw)
    assert DutchingController._liquidity_warning_only(cfg) is expected


# ==========================================================================
# 5) CONFIG round-trip sul servizio reale
# ==========================================================================
class _InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_liquidity_config_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(
        RoserpinaConfig(
            table_count=3,
            liquidity_guard_enabled=False,
            liquidity_multiplier=4.5,
            min_liquidity_absolute=0.0,
            liquidity_warning_only=False,
        )
    )
    rs = SettingsService(db).load_roserpina_config()
    assert rs.liquidity_guard_enabled is False
    assert rs.liquidity_multiplier == 4.5
    assert rs.min_liquidity_absolute == 0.0
    assert rs.liquidity_warning_only is False


def test_liquidity_config_defaults_from_trading_config():
    rs = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert rs.liquidity_guard_enabled == bool(trading_config.LIQUIDITY_GUARD_ENABLED)
    assert rs.liquidity_multiplier == float(trading_config.LIQUIDITY_MULTIPLIER)
    assert rs.min_liquidity_absolute == float(trading_config.MIN_LIQUIDITY_ABSOLUTE)
    # Default opt-in: il blocco parte in avviso.
    assert rs.liquidity_warning_only == bool(trading_config.LIQUIDITY_WARNING_ONLY)
    assert rs.liquidity_warning_only is True


# ==========================================================================
# 6) GUI wiring (checkbox "solo avviso" ri-esposta)
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(
            liquidity_guard_enabled=False,
            liquidity_multiplier=2.5,
            min_liquidity_absolute=0.0,
            liquidity_warning_only=True,
        )

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_liquidity_fields(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_liq_multiplier_var.get() == "2.5"
        assert app.rs_liq_min_abs_var.get() == "0.0"
        assert bool(app.rs_liq_guard_enabled_var.get()) is False
        assert bool(app.rs_liq_warning_only_var.get()) is True
    finally:
        app.destroy()


def test_gui_saves_liquidity_fields_arming_block(monkeypatch):
    # Togliere "solo avviso" (=> False) arma il blocco reale.
    app = _make_gui(monkeypatch)
    try:
        app.rs_liq_multiplier_var.set("4")
        app.rs_liq_min_abs_var.set("0")
        app.rs_liq_guard_enabled_var.set(True)
        app.rs_liq_warning_only_var.set(False)
        app._save_roserpina_settings()
        cfg = app.settings_service.saved_cfg
        assert cfg is not None
        assert cfg.liquidity_multiplier == 4.0
        assert cfg.min_liquidity_absolute == 0.0
        assert cfg.liquidity_guard_enabled is True
        assert cfg.liquidity_warning_only is False
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-1", "nan", "abc"])
def test_gui_save_blocks_invalid_multiplier(monkeypatch, bad):
    app = _make_gui(monkeypatch)
    try:
        app.rs_liq_multiplier_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()
