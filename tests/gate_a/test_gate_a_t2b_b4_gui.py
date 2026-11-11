"""Gate A T2b batch b4 — gui: MiniPickfairGUI._save_simulation_settings, headless.

Nessun tkinter/X11: mini_gui usa i fallback headless; l'istanza e' creata con __new__ e riceve solo le
variabili e il settings_service stub usati dal metodo reale."""
from __future__ import annotations

import types

import mini_gui


class _Var:
    def __init__(self, v):
        self.v = v

    def get(self):
        return self.v


class _Svc:
    def __init__(self, loaded):
        self.loaded = loaded
        self.saved = []

    def load_simulation_config(self):
        return self.loaded

    def save_simulation_config(self, cfg):
        self.saved.append(cfg)


def _gui(svc):
    g = mini_gui.MiniPickfairGUI.__new__(mini_gui.MiniPickfairGUI)
    g.settings_service = svc
    g.sim_starting_balance_var = _Var("250")
    g.sim_partial_fill_var = _Var(True)
    g.sim_consume_liq_var = _Var(False)
    g.sim_persist_state_var = _Var(True)
    g.errors, g.infos = [], []
    g._safe_show_error = lambda t, m: g.errors.append(m)
    g._safe_show_info = lambda t, m: g.infos.append(m)
    return g


def test_trg_628_b_0784():
    """ID: TRG-628-B-0784 / PF-B1-GUI-SIM-PRESERVE

    Il save della tab Simulazione rilegge la config e conserva commission_pct/enabled (e ogni altro campo non
    esposto) sovrascrivendo solo i campi della tab; reload assente/None/parziale => salvataggio annullato
    (fail-closed), nessuna scrittura."""
    loaded = {"commission_pct": 4.5, "enabled": False, "other": "keep", "starting_balance": 1.0}
    svc = _Svc(dict(loaded))
    g = _gui(svc)
    mini_gui.MiniPickfairGUI._save_simulation_settings(g)
    assert g.errors == [] and len(svc.saved) == 1
    cfg = svc.saved[0]
    assert cfg["commission_pct"] == 4.5 and cfg["enabled"] is False and cfg["other"] == "keep"
    assert cfg["starting_balance"] == 250.0 and cfg["partial_fill_enabled"] is True
    assert cfg["consume_liquidity"] is False and cfg["persist_state"] is True

    for bad in (None, {}, {"enabled": False}, {"commission_pct": 4.5}, ["x"]):
        svc = _Svc(bad)
        g = _gui(svc)
        mini_gui.MiniPickfairGUI._save_simulation_settings(g)
        assert svc.saved == [] and g.errors, bad

    no_reload = types.SimpleNamespace(saved=[])
    no_reload.save_simulation_config = lambda cfg: no_reload.saved.append(cfg)
    g = _gui(no_reload)
    mini_gui.MiniPickfairGUI._save_simulation_settings(g)
    assert no_reload.saved == [] and g.errors
