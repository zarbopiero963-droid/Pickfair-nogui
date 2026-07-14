"""G5 PR 2/5 — MAX_WIN cap reale su vincita/payout per gamba (enforce-first).

`trading_config.MAX_WIN` (10000.0) era DEAD (solo display). Qui:
1. ENFORCEMENT: `DutchingController.precheck` e `manual_bet` bloccano il submit se
   la vincita potenziale di UNA gamba supera il cap configurato, PRIMA di ogni
   side-effect (duplication acquire). Enforce-first OPT-IN: default warning_only
   => solo avviso (max_win_warning/max_win_breaches), l'owner arma il blocco.
2. GRANDEZZA: BACK => payout stake*price; LAY => liability stake*(price-1) (il
   RISCHIO reale, decisione owner #393). Per-gamba (esiti mutuamente esclusivi
   => MAX, non somma).
3. FAIL-SAFE: config assente/corrotta ricade su trading_config.MAX_WIN.
4. CONFIG: RoserpinaConfig.max_win/max_win_warning_only round-trip sul servizio.
5. GUI: campo "Max Win" + checkbox "solo avviso" caricati/salvati/validati.

Test PASS (blocca quando armato e sopra cap) + BLOCK (non blocca in avviso / sotto
cap / LAY liability sotto cap; il fallback fail-safe non disattiva il cap).
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
# Harness minimale per pilotare precheck / manual_bet (self-contained).
# ==========================================================================
class _Mode:
    value = "ACTIVE"


class _Config:
    anti_duplication_enabled = True
    allow_recovery = True
    max_total_exposure_pct = 100.0
    max_event_exposure_pct = 100.0
    max_single_bet_pct = 100.0
    # max_win / max_win_warning_only volutamente ASSENTI di default: fallback.


class _RiskDesk:
    bankroll_current = 1000.0


class _DupGuard:
    def __init__(self):
        self.keys = set()

    def acquire(self, key):
        return True

    def release(self, key):
        self.keys.discard(key)


class _TableManager:
    def total_exposure(self):
        return 0.0

    def find_by_event_key(self, event_key):
        return None


class _Bus:
    def __init__(self):
        self.published = []

    def publish(self, topic, payload):
        self.published.append((topic, payload))


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


def _controller(runtime=None, bus=None):
    from controllers.dutching_controller import DutchingController

    return DutchingController(bus=bus, runtime_controller=runtime or _Runtime())


def _runtime_with(max_win=None, warning_only=None):
    rt = _Runtime()
    if max_win is not None:
        rt.config.max_win = max_win
    if warning_only is not None:
        rt.config.max_win_warning_only = warning_only
    return rt


# ==========================================================================
# 1) ENFORCEMENT (precheck) — cuore del gate
# ==========================================================================
def test_precheck_blocks_leg_over_cap_when_armed(monkeypatch):
    # BACK stake 50 @ price 2 => payout 100 > cap 50, armato (warning_only=False) => BLOCCA.
    _patch_results(monkeypatch, [{"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"}])
    res = _controller(_runtime_with(max_win=50.0, warning_only=False)).precheck(_payload())
    assert res["ok"] is False
    assert "oltre il cap" in res["error"]
    assert res["max_win"] == 50.0
    assert res["max_win_breaches"] and res["max_win_breaches"][0]["potential_win"] == 100.0


def test_precheck_warns_but_does_not_block_in_warning_mode(monkeypatch):
    # Stesso sforamento, ma warning_only=True (default opt-in) => NON blocca, avvisa.
    _patch_results(monkeypatch, [{"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"}])
    res = _controller(_runtime_with(max_win=50.0, warning_only=True)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["max_win_warning"] is True
    assert res["max_win_breaches"] and res["max_win_breaches"][0]["selectionId"] == 1


def test_precheck_default_warning_only_does_not_block(monkeypatch):
    # config senza il flag => default OPT-IN True => nessun blocco a sorpresa.
    _patch_results(monkeypatch, [{"selectionId": 1, "price": 2.0, "stake": 50.0, "side": "BACK"}])
    rt = _Runtime()
    rt.config.max_win = 50.0  # solo il cap, flag assente
    res = _controller(rt).precheck(_payload())
    assert res["ok"] is True
    assert res["max_win_warning"] is True


def test_precheck_allows_when_under_cap(monkeypatch):
    # payout 150 < cap default 10000 => nessun avviso, nessun blocco.
    _patch_results(monkeypatch, [{"selectionId": 1, "price": 3.0, "stake": 50.0, "side": "BACK"}])
    res = _controller().precheck(_payload())
    assert res["ok"] is True
    assert res["max_win_warning"] is False
    assert res["max_win_breaches"] == []


def test_max_win_is_config_editable(monkeypatch):
    results = [{"selectionId": 1, "price": 2.0, "stake": 100.0, "side": "BACK"}]  # payout 200
    _patch_results(monkeypatch, results)
    # cap basso armato => blocca
    assert _controller(_runtime_with(max_win=150.0, warning_only=False)).precheck(_payload())["ok"] is False
    # cap alto => lo stesso payout passa
    _patch_results(monkeypatch, results)
    assert _controller(_runtime_with(max_win=500.0, warning_only=False)).precheck(_payload())["ok"] is True


def test_lay_uses_liability_not_backer_stake(monkeypatch):
    # LAY stake 200 @ price 5: liability = 200*(5-1) = 800. Il cap confronta la
    # LIABILITY (rischio), non il backer-stake (200). Cap 500: backer-stake 200 <
    # 500 NON bloccherebbe, ma liability 800 > 500 => BLOCCA (armato). Prova che la
    # semantica LAY e' liability (decisione owner #393).
    _patch_results(monkeypatch, [{"selectionId": 7, "price": 5.0, "stake": 200.0, "side": "LAY"}])
    res = _controller(_runtime_with(max_win=500.0, warning_only=False)).precheck(_payload())
    assert res["ok"] is False
    assert "oltre il cap" in res["error"]
    assert res["max_win_breaches"][0]["potential_win"] == 800.0


def test_lay_liability_under_cap_passes(monkeypatch):
    # liability 800 < cap 1000 => passa (nessun blocco, nessun avviso).
    _patch_results(monkeypatch, [{"selectionId": 7, "price": 5.0, "stake": 200.0, "side": "LAY"}])
    res = _controller(_runtime_with(max_win=1000.0, warning_only=False)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["max_win_warning"] is False


def test_lay_liability_none_field_still_blocks(monkeypatch):
    # FAIL-CLOSED end-to-end: un item LAY con liability=None NON deve sfuggire al
    # cap: il gate ricalcola stake*(price-1)=800 e blocca (armato, cap 500).
    _patch_results(
        monkeypatch,
        [{"selectionId": 7, "price": 5.0, "stake": 200.0, "side": "LAY", "liability": None}],
    )
    res = _controller(_runtime_with(max_win=500.0, warning_only=False)).precheck(_payload())
    assert res["ok"] is False and "oltre il cap" in res["error"]
    assert res["max_win_breaches"][0]["potential_win"] == 800.0


def test_cap_is_per_leg_not_sum(monkeypatch):
    # Due gambe da payout 60 ciascuna: la somma (120) supera cap 100, ma nessuna
    # SINGOLA gamba lo supera => NON blocca (esiti mutuamente esclusivi).
    _patch_results(
        monkeypatch,
        [
            {"selectionId": 1, "price": 2.0, "stake": 30.0, "side": "BACK"},  # 60
            {"selectionId": 2, "price": 3.0, "stake": 20.0, "side": "BACK"},  # 60
        ],
    )
    res = _controller(_runtime_with(max_win=100.0, warning_only=False)).precheck(_payload())
    assert res["ok"] is True, res
    assert res["max_win_breaches"] == []


def test_bad_config_max_win_still_blocks_huge_payout(monkeypatch):
    # BLOCK-proof: config con max_win corrotto (=0) NON deve far passare un payout
    # 15000 — il fallback fail-safe (trading_config.MAX_WIN=10000) blocca comunque.
    _patch_results(monkeypatch, [{"selectionId": 1, "price": 15.0, "stake": 1000.0, "side": "BACK"}])
    rt = _runtime_with(max_win=0.0, warning_only=False)
    res = _controller(rt).precheck(_payload())
    assert res["ok"] is False and "oltre il cap" in res["error"]
    assert res["max_win"] == float(trading_config.MAX_WIN)


# ==========================================================================
# 2) Helper puri (fail-safe / semantica)
# ==========================================================================
@pytest.mark.parametrize("bad", [0.0, -5.0, float("nan"), float("inf"), "abc", None])
def test_max_win_fail_safe_falls_back_to_constant(bad):
    from controllers.dutching_controller import DutchingController

    cfg = SimpleNamespace() if bad is None else SimpleNamespace(max_win=bad)
    assert DutchingController._max_win(cfg) == float(trading_config.MAX_WIN)
    assert DutchingController._max_win(SimpleNamespace(max_win=250.0)) == 250.0


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, True),          # assente => default OPT-IN
        (True, True),
        (False, False),
        ("1", True),
        ("true", True),
        ("on", True),
        ("0", False),          # stringa "0" NON deve valere True
        ("false", False),
        ("", False),
    ],
)
def test_max_win_warning_only_parsing(raw, expected):
    from controllers.dutching_controller import DutchingController

    cfg = SimpleNamespace() if raw is None else SimpleNamespace(max_win_warning_only=raw)
    assert DutchingController._max_win_warning_only(cfg) is expected


def test_leg_potential_win_semantics():
    from controllers.dutching_controller import DutchingController

    f = DutchingController._leg_potential_win
    assert f({"side": "BACK", "stake": 40.0, "price": 2.5}) == 100.0   # payout stake*price
    assert f({"side": "LAY", "stake": 40.0, "price": 2.5}) == 60.0     # liability stake*(price-1)
    assert f({"side": "BACK"}) == 0.0                                  # fail-safe
    # precomputato valido e >= ricalcolo => usato; se < ricalcolo vince il ricalcolo (fail-closed)
    assert f({"side": "LAY", "stake": 40.0, "price": 2.5, "liability": 70.0}) == 70.0
    assert f({"side": "LAY", "stake": 40.0, "price": 2.5, "liability": 55.0}) == 60.0


def test_leg_potential_win_lay_liability_failclosed():
    # FAIL-CLOSED (rilievo convergente GPT-5.6 Terra + Fable 5 + Fugu Ultra): una
    # liability precomputata None / 0 / non-numerica / non-finita / sottostimata NON
    # deve abbassare il rischio LAY: si ricalcola stake*(price-1) e si prende il MAX.
    from controllers.dutching_controller import DutchingController

    f = DutchingController._leg_potential_win
    base = {"side": "LAY", "stake": 200.0, "price": 5.0}  # ricalcolo = 800
    assert f(base) == 800.0
    assert f({**base, "liability": None}) == 800.0
    assert f({**base, "liability": 0.0}) == 800.0
    assert f({**base, "liability": "x"}) == 800.0
    assert f({**base, "liability": float("nan")}) == 800.0
    assert f({**base, "liability": float("inf")}) == 800.0
    assert f({**base, "liability": 100.0}) == 800.0   # sottostimata => vince il ricalcolo
    assert f({**base, "liability": 900.0}) == 900.0   # sovrastimata valida => piu' conservativa


def test_max_win_breaches_failsafe_non_numeric_selection_id():
    # Un selectionId non numerico NON deve sollevare (romperebbe il gate); degrada a 0.
    from controllers.dutching_controller import DutchingController

    results = [{"selectionId": "abc", "price": 2.0, "stake": 100.0, "side": "BACK"}]  # payout 200
    breaches = DutchingController._max_win_breaches(results, 50.0)
    assert len(breaches) == 1
    assert breaches[0]["selectionId"] == 0
    assert breaches[0]["potential_win"] == 200.0


# ==========================================================================
# 3) ENFORCEMENT (manual_bet) — chiude la via non-gated
# ==========================================================================
def _manual_payload(price, stake, bet_type="BACK"):
    return {
        "market_id": "1.42",
        "selection_id": 5,
        "price": price,
        "stake": stake,
        "bet_type": bet_type,
    }


def test_manual_bet_blocks_over_cap_when_armed():
    # BACK 100 @ 2 => payout 200 > cap 50, armato => BLOCCA (nessun publish).
    bus = _Bus()
    ctrl = _controller(_runtime_with(max_win=50.0, warning_only=False), bus=bus)
    res = ctrl.manual_bet(_manual_payload(2.0, 100.0))
    assert res["ok"] is False and "oltre il cap" in res["error"]
    assert not bus.published


def test_manual_bet_warns_does_not_block_by_default():
    # default warning_only True => il bet passa (order pubblicato) MA lo sforamento
    # e' OSSERVABILE nel risultato (max_win_warning + potential_win), non silenzioso.
    bus = _Bus()
    rt = _Runtime()
    rt.config.max_win = 50.0  # flag assente => opt-in
    res = _controller(rt, bus=bus).manual_bet(_manual_payload(2.0, 100.0))  # payout 200
    assert res["ok"] is True, res
    assert any(topic == "CMD_QUICK_BET" for topic, _ in bus.published)
    assert res["max_win_warning"] is True
    assert res["potential_win"] == 200.0


def test_manual_bet_no_warning_when_under_cap():
    bus = _Bus()
    res = _controller(_Runtime(), bus=bus).manual_bet(_manual_payload(2.0, 100.0))  # payout 200 < 10000
    assert res["ok"] is True
    assert res["max_win_warning"] is False


def test_manual_bet_lay_uses_liability():
    # LAY 100 @ 5: liability = 100*(5-1) = 400 > cap 200 => BLOCCA (armato). Prova
    # che il buco liability sul LAY manuale e' chiuso (decisione owner #393): il
    # backer-stake 100 < 200 non basterebbe a bloccare.
    bus = _Bus()
    ctrl = _controller(_runtime_with(max_win=200.0, warning_only=False), bus=bus)
    res = ctrl.manual_bet(_manual_payload(5.0, 100.0, bet_type="LAY"))
    assert res["ok"] is False and "oltre il cap" in res["error"]
    assert bus.published == []


def test_manual_bet_lay_liability_under_cap_passes():
    # LAY 100 @ 5: liability 400 < cap 1000 => passa (order pubblicato).
    bus = _Bus()
    ctrl = _controller(_runtime_with(max_win=1000.0, warning_only=False), bus=bus)
    res = ctrl.manual_bet(_manual_payload(5.0, 100.0, bet_type="LAY"))
    assert res["ok"] is True, res
    assert any(topic == "CMD_QUICK_BET" for topic, _ in bus.published)


# ==========================================================================
# 4) CONFIG round-trip sul SERVIZIO REALE
# ==========================================================================
class _InMemoryDB:
    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_max_win_round_trip():
    db = _InMemoryDB()
    SettingsService(db).save_roserpina_config(
        RoserpinaConfig(table_count=3, max_win=7500.0, max_win_warning_only=False)
    )
    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.max_win == 7500.0
    assert reloaded.max_win_warning_only is False


def test_max_win_defaults_when_absent():
    reloaded = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert reloaded.max_win == float(trading_config.MAX_WIN)
    assert reloaded.max_win_warning_only is True  # default OPT-IN


# ==========================================================================
# 5) GUI wiring (tab Roserpina)
# ==========================================================================
class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(max_win=8200.0, max_win_warning_only=False)

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_max_win(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_max_win_var.get() == "8200.0"
        assert app.rs_max_win_warning_only_var.get() is False
    finally:
        app.destroy()


def test_gui_saves_max_win(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_win_var.set("6000")
        app.rs_max_win_warning_only_var.set(False)
        app._save_roserpina_settings()
        cfg = app.settings_service.saved_cfg
        assert cfg is not None
        assert cfg.max_win == 6000.0
        assert cfg.max_win_warning_only is False
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-5", "nan", "inf", "abc"])
def test_gui_save_blocks_invalid_max_win(monkeypatch, bad):
    # Un cap non valido NON deve essere persistito (niente drift GUI-vs-enforcement).
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_win_var.set(bad)
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, bad
    finally:
        app.destroy()
