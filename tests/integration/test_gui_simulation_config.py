"""G1 — Config Simulazione editabile in GUI (tab Simulazione).

I parametri del broker simulato erano configurabili solo via DB. Ora il tab
"Simulazione" espone: bankroll iniziale, partial_fill, consume_liquidity,
persist_state. Sono GIA' enforced (betfair_service -> SimulationBroker): questa
e' pura esposizione + validazione. `commission_pct` (policy-lock 4.5%) e
`enabled` NON sono esposti e vengono PRESERVATI al salvataggio.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace  # noqa: F401

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration


class _CapturingService(FakeSettingsService):
    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_sim = None

    def load_simulation_config(self):
        return {
            "enabled": True,
            "starting_balance": 2500.0,
            "commission_pct": 4.5,
            "partial_fill_enabled": False,
            "consume_liquidity": False,
            "persist_state": True,
        }

    def save_simulation_config(self, cfg):
        self.saved_sim = dict(cfg)


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_simulation_fields(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.sim_starting_balance_var.get() == "2500.0"
        assert bool(app.sim_partial_fill_var.get()) is False
        assert bool(app.sim_consume_liq_var.get()) is False
        assert bool(app.sim_persist_state_var.get()) is True
    finally:
        app.destroy()


def test_gui_saves_simulation_and_preserves_locked_fields(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.sim_starting_balance_var.set("3000")
        app.sim_partial_fill_var.set(True)
        app.sim_consume_liq_var.set(True)
        app.sim_persist_state_var.set(False)
        app._save_simulation_settings()
        cfg = app.settings_service.saved_sim
        assert cfg is not None
        assert cfg["starting_balance"] == 3000.0
        assert cfg["partial_fill_enabled"] is True
        assert cfg["consume_liquidity"] is True
        assert cfg["persist_state"] is False
        # commission_pct (policy-lock 4.5) ed enabled: NON esposti => PRESERVATI.
        assert cfg["commission_pct"] == 4.5
        assert cfg["enabled"] is True
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["", "0", "-100", "nan", "inf", "abc"])
def test_gui_save_blocks_invalid_balance(monkeypatch, bad):
    app = _make_gui(monkeypatch)
    try:
        app.sim_starting_balance_var.set(bad)
        app._save_simulation_settings()
        assert app.settings_service.saved_sim is None, bad
    finally:
        app.destroy()


def test_parse_sim_balance_helper(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    parse = mg.MiniPickfairGUI._parse_sim_balance
    assert parse("1500", "b") == 1500.0
    assert parse("0.5", "b") == 0.5
    for bad in ["", "0", "-1", "nan", "inf", "abc"]:
        with pytest.raises(ValueError):
            parse(bad, "b")
