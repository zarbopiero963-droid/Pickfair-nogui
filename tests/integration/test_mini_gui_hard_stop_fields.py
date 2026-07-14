"""Hard-stop giornalieri editabili nella GUI (Roserpina tab) — PASS + BLOCK.

Espone `max_daily_loss` / `max_open_exposure` / `max_drawdown_hard_stop_pct`
(prerequisiti del gate LIVE) come campi editabili, con semantica FAIL-CLOSED:
- campo vuoto => None => il save preserva il valore persistito e il gate LIVE
  resta bloccante se non configurato (non si azzera un limite di sicurezza);
- valore presente => numerico finito > 0 (per la % anche <= 100), coerente con
  `_validate_live_hard_stop_config`; altrimenti errore, nessun valore fasullo.

Tre livelli:
1. logica pura di parsing/validazione (`_parse_hard_stop`/`_hard_stop_to_str`)
   — non richiede GUI (staticmethod);
2. preserve sul SERVIZIO REALE (`SettingsService` + DB in-memory): un campo
   vuoto (=> None) NON azzera un hard-stop gia' persistito;
3. wiring end-to-end nel salvataggio Roserpina via GUI in test_mode.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest

import mini_gui
from core.system_state import RoserpinaConfig
from services.settings_service import SettingsService

# La dir dei test (senza __init__) e' su sys.path in prepend-mode; l'inserimento
# esplicito rende l'import del harness sibling robusto anche con importmode
# diversi in CI, cosi' la BLOCK-proof safety-critical non fallisce in collection.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration

_P = mini_gui.MiniPickfairGUI._parse_hard_stop
_S = mini_gui.MiniPickfairGUI._hard_stop_to_str


# --------------------------------------------------------------------------
# 1) Logica pura (fail-closed) — la parte piu' importante come BLOCK-proof.
# --------------------------------------------------------------------------
@pytest.mark.parametrize("blank", ["", "   ", None])
def test_empty_field_is_none_preserve(blank):
    # Vuoto => None: il save preserva il valore persistito, non azzera il limite.
    assert _P(blank, "x") is None


def test_valid_values_parsed():
    assert _P("250", "x") == 250.0
    assert _P("0.5", "x") == 0.5
    assert _P("100", "x", is_pct=True) == 100.0  # bordo % ammesso
    assert _P("150", "x") == 150.0  # non-% puo' superare 100 (es. euro)


@pytest.mark.parametrize("bad", ["abc", "-1", "0", "-0.0", "nan", "inf", "-inf"])
def test_invalid_values_raise_blockproof(bad):
    # BLOCK: numeri non validi / <= 0 / non finiti devono SOLLEVARE, cosi' non
    # entrano nel gate come valori fasulli. Se la validazione sparisse, questi
    # passerebbero => il test fallisce.
    with pytest.raises(ValueError):
        _P(bad, "x")


def test_pct_over_100_raises_but_euro_ok():
    with pytest.raises(ValueError):
        _P("100.1", "x", is_pct=True)
    assert _P("100.1", "x", is_pct=False) == 100.1


def test_hard_stop_to_str_none_is_empty():
    assert _S(None) == ""
    assert _S(30.0) == "30.0"


# --------------------------------------------------------------------------
# 2) Preserve sul SERVIZIO REALE (no fake) — risponde ai rilievi forti:
#    un campo vuoto (=> None) NON deve azzerare un hard-stop persistito.
# --------------------------------------------------------------------------
class _InMemoryDB:
    """DB minimale compatibile con SettingsService (get/save settings)."""

    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


def test_empty_field_does_not_overwrite_persisted_hard_stop_real_service():
    # Percorso REALE (SettingsService vero, niente fake): l'output della GUI per
    # un campo vuoto e' None (_parse_hard_stop("") is None) e il save reale deve
    # PRESERVARE gli hard-stop gia' persistiti, non azzerarli.
    assert _P("", "Esposizione aperta max") is None  # cio' che la GUI produce

    db = _InMemoryDB(
        {
            "roserpina.max_daily_loss": 125.0,
            "roserpina.max_open_exposure": 300.0,
            "roserpina.max_drawdown_hard_stop_pct": 20.0,
        }
    )
    svc = SettingsService(db)

    # Save "GUI-like": solo max_daily_loss modificato (campo pieno); gli altri due
    # lasciati vuoti => None (come li produce _parse_hard_stop per il campo vuoto).
    svc.save_roserpina_config(
        RoserpinaConfig(
            table_count=3,
            max_daily_loss=_P("150", "x"),                        # campo pieno
            max_open_exposure=_P("", "x"),                        # vuoto => None
            max_drawdown_hard_stop_pct=_P("", "x", is_pct=True),  # vuoto => None
        )
    )

    reloaded = SettingsService(db).load_roserpina_config()
    assert reloaded.max_daily_loss == 150.0             # aggiornato
    assert reloaded.max_open_exposure == 300.0          # PRESERVATO (non azzerato)
    assert reloaded.max_drawdown_hard_stop_pct == 20.0  # PRESERVATO (non azzerato)


# --------------------------------------------------------------------------
# 3) Wiring end-to-end nel salvataggio Roserpina (GUI test_mode).
# --------------------------------------------------------------------------
class _CapturingService(FakeSettingsService):
    """Cattura il RoserpinaConfig salvato e restituisce hard-stop noti al load."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_cfg = None

    def load_roserpina_config(self):
        return SimpleNamespace(
            max_daily_loss=250.0,
            max_open_exposure=None,
            max_drawdown_hard_stop_pct=30.0,
        )

    def save_roserpina_config(self, cfg):
        self.saved_cfg = cfg


def _make_gui(monkeypatch):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=_CapturingService)
    return mg.MiniPickfairGUI(test_mode=True)


def test_load_maps_hard_stops_none_to_empty(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        assert app.rs_max_daily_loss_var.get() == "250.0"
        # None persistito => campo vuoto (non "None", non "0").
        assert app.rs_max_open_exposure_var.get() == ""
        assert app.rs_max_drawdown_hard_stop_var.get() == "30.0"
    finally:
        app.destroy()


def test_save_writes_values_and_preserves_empty(monkeypatch):
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_daily_loss_var.set("300")
        app.rs_max_open_exposure_var.set("")  # vuoto => None => preserva
        app.rs_max_drawdown_hard_stop_var.set("40")
        app._save_roserpina_settings()
        cfg = app.settings_service.saved_cfg
        assert cfg is not None, "save_roserpina_config non e' stato chiamato"
        assert cfg.max_daily_loss == 300.0
        assert cfg.max_open_exposure is None  # empty => None (preserve semantics)
        assert cfg.max_drawdown_hard_stop_pct == 40.0
    finally:
        app.destroy()


def test_reload_failure_after_save_is_not_reported_as_save_failure(monkeypatch):
    # BLOCK/regressione (rilievo CodeRabbit): se reload_config() fallisce DOPO un
    # save andato a buon fine, il salvataggio NON deve apparire come fallito (il
    # dato e' su DB) e il refresh dei campi deve comunque avvenire.
    app = _make_gui(monkeypatch)
    try:
        def _boom():
            raise RuntimeError("reload boom")

        app.runtime.reload_config = _boom

        infos, errors = [], []
        app._safe_show_info = lambda *a, **k: infos.append(a)
        app._safe_show_error = lambda *a, **k: errors.append(a)

        app.rs_max_daily_loss_var.set("300")
        app._save_roserpina_settings()

        # Il save e' avvenuto (persistito) nonostante il reload fallito.
        assert app.settings_service.saved_cfg is not None
        assert app.settings_service.saved_cfg.max_daily_loss == 300.0
        # NON e' stato mostrato l'errore generico di "salvataggio fallito".
        assert not any("Errore salvataggio Roserpina" in a[0] for a in errors), errors
        # E' stato segnalato il reload fallito (con save ok), non un save-error.
        assert any("reload runtime fallito" in a[0] for a in errors), errors
    finally:
        app.destroy()


def test_save_blocks_on_invalid_hard_stop(monkeypatch):
    # BLOCK: un valore invalido interrompe il salvataggio PRIMA di scrivere,
    # cosi' nessun hard-stop fasullo raggiunge il gate.
    app = _make_gui(monkeypatch)
    try:
        app.rs_max_drawdown_hard_stop_var.set("150")  # % > 100 => invalido
        app._save_roserpina_settings()
        assert app.settings_service.saved_cfg is None, (
            "un hard-stop invalido non deve essere salvato"
        )
    finally:
        app.destroy()
