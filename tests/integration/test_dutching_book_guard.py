"""PR2a — Book% gate reale al submit dutching (enforce-first) + config + GUI.

Le costanti BOOK_WARNING/BOOK_BLOCK erano DEAD (nessun enforcement). Qui:
1. ENFORCEMENT: `DutchingController.precheck` blocca il submit se `book_pct` >=
   soglia di blocco (config-editabile, fallback fail-safe a trading_config.BOOK_BLOCK);
2. CONFIG: RoserpinaConfig.book_warning/book_block round-trip su SettingsService reale;
3. GUI: i campi book sono caricati/salvati nel tab Roserpina.

Test PASS (il gate blocca quando deve) + BLOCK (non blocca sotto soglia; il
fallback fail-safe non disattiva il gate).
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
# Harness minimale per pilotare DutchingController.precheck (self-contained).
# ==========================================================================
class _Mode:
    value = "ACTIVE"


class _Config:
    anti_duplication_enabled = True
    allow_recovery = True
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0
    # book_block / book_warning volutamente ASSENTI di default: verifica fallback.


class _RiskDesk:
    bankroll_current = 1000.0


class _DupGuard:
    def __init__(self):
        self.keys = set()

    def acquire(self, key):
        # mimica un guard che concede sempre (non blocca il test)
        return True


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


def _patch_book(monkeypatch, book_pct):
    def fake_calc(selections, total_stake):
        _ = selections, total_stake
        return (
            [
                {"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"},
                {"selectionId": 2, "price": 3.0, "stake": 50.0, "side": "BACK"},
            ],
            1.0,
            float(book_pct),
        )

    monkeypatch.setattr("controllers.dutching_controller.calculate_dutching", fake_calc)


def _controller(runtime=None):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=None, runtime_controller=runtime or _Runtime())


# ==========================================================================
# 1) ENFORCEMENT (il cuore: gate reale al submit)
# ==========================================================================
def test_precheck_blocks_when_book_at_or_above_block(monkeypatch):
    # book 130 >= default 110 (fallback) => BLOCCA.
    _patch_book(monkeypatch, 130.0)
    res = _controller().precheck(_payload())
    assert res["ok"] is False
    assert "Book troppo alto" in res["error"]
    assert res["book_block"] == float(trading_config.BOOK_BLOCK)


def test_precheck_allows_when_book_below_block(monkeypatch):
    # book 108 < 110 => il gate book NON blocca (passa il precheck).
    _patch_book(monkeypatch, 108.0)
    res = _controller().precheck(_payload())
    assert res["ok"] is True, res
    # 108 >= 105 (warning default) => flag di avviso alzato, ma non bloccante.
    assert res.get("book_warning_exceeded") is True


def test_book_block_is_config_editable(monkeypatch):
    _patch_book(monkeypatch, 115.0)

    rt_low = _Runtime()
    rt_low.config.book_block = 112.0  # soglia abbassata via config
    res = _controller(rt_low).precheck(_payload())
    assert res["ok"] is False and "Book troppo alto" in res["error"]
    assert res["book_block"] == 112.0

    rt_high = _Runtime()
    rt_high.config.book_block = 120.0  # soglia alzata => lo stesso book passa
    assert _controller(rt_high).precheck(_payload())["ok"] is True


@pytest.mark.parametrize("bad", [0.0, -5.0, float("nan"), float("inf"), "abc", None])
def test_book_threshold_fail_safe_falls_back_to_constant(bad):
    # Un valore config assurdo/rotto NON deve disattivare il gate: fallback alla
    # costante di sicurezza.
    from controllers.dutching_controller import DutchingController

    T = DutchingController._book_threshold
    assert T(SimpleNamespace(book_block=bad), "book_block", 110.0) == 110.0
    assert T(SimpleNamespace(), "book_block", 110.0) == 110.0
    # valore valido rispettato
    assert T(SimpleNamespace(book_block=125.0), "book_block", 110.0) == 125.0


def test_bad_config_book_block_still_blocks_high_book(monkeypatch):
    # BLOCK-proof: config con book_block corrotto (=0) NON deve far passare un
    # book 200 — il fallback fail-safe (110) blocca comunque.
    _patch_book(monkeypatch, 200.0)
    rt = _Runtime()
    rt.config.book_block = 0.0  # valore rotto
    res = _controller(rt).precheck(_payload())
    assert res["ok"] is False and "Book troppo alto" in res["error"]
    assert res["book_block"] == 110.0  # fallback applicato


# ==========================================================================
# 2) CONFIG round-trip sul SERVIZIO REALE
# ==========================================================================
class _InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_book_thresholds_persist_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(
        RoserpinaConfig(table_count=3, book_warning=103.0, book_block=108.0)
    )
    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.book_warning == 103.0
    assert reloaded.book_block == 108.0


def test_book_thresholds_default_to_trading_config_when_absent():
    reloaded = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert reloaded.book_warning == float(trading_config.BOOK_WARNING)
    assert reloaded.book_block == float(trading_config.BOOK_BLOCK)


# ==========================================================================
# 3) GUI wiring (tab Roserpina)
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(book_warning=104.0, book_block=112.0)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_book_thresholds(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_book_warning_var.get() == "104.0"
        assert app.rs_book_block_var.get() == "112.0"
    finally:
        app.destroy()


def test_gui_saves_book_thresholds(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_book_warning_var.set("103")
        app.rs_book_block_var.set("109")
        app._save_roserpina_settings()
        cfg = app.settings_service.saved_cfg
        assert cfg is not None
        assert cfg.book_warning == 103.0
        assert cfg.book_block == 109.0
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-5", "nan", "inf", "abc"])
def test_gui_save_blocks_invalid_book_value(monkeypatch, bad):
    # BLOCK (rilievo GLM/Fable/Fugu/Greptile): un valore book non valido NON deve
    # essere persistito (niente drift GUI-vs-enforcement), ne' salvare a meta'.
    app = _make_gui(monkeypatch)
    try:
        app.rs_book_block_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()


def test_gui_save_blocks_warning_over_block(monkeypatch):
    # BLOCK: warning > block e' incoerente (l'avviso non scatterebbe mai prima
    # del blocco) => salvataggio rifiutato.
    app = _make_gui(monkeypatch)
    try:
        app.rs_book_warning_var.set("115")
        app.rs_book_block_var.set("110")
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None
    finally:
        app.destroy()
