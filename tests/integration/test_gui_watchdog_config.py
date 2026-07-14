"""G2 — Toggle watchdog anomalie editabili in GUI (tab Watchdog).

I tre toggle (anomaly_enabled, anomaly_alerts_enabled, anomaly_actions_enabled)
erano configurabili solo via DB. Ora il tab "Watchdog" li espone. Sono GIA'
enforced (watchdog_service li rilegge live ad ogni tick): pura esposizione +
validazione. Il service (settings_service) e' gia' completo: nessuna modifica al
backend.

Semantica tri-state: anomaly_enabled non configurato (None) = default-ON =>
checkbox spuntata; salvando si scrive un booleano esplicito (togliere la spunta
=> False = watchdog disattivato).
"""
from __future__ import annotations

import os
import sys

import pytest

from services.settings_service import SettingsService

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration


class _InMemorySettingsDb:
    def __init__(self):
        self.data = {}

    def get_settings(self):
        return dict(self.data)

    def save_settings(self, payload):
        self.data.update({str(k): str(v) for k, v in (payload or {}).items()})


class _CapturingService(FakeSettingsService):
    anomaly_enabled_return = None
    anomaly_alerts_return = False
    anomaly_actions_return = False

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved_settings = None
        self.save_calls = 0

    def load_anomaly_enabled(self):
        return type(self).anomaly_enabled_return

    def load_anomaly_alerts_enabled(self):
        return type(self).anomaly_alerts_return

    def load_anomaly_actions_enabled(self):
        return type(self).anomaly_actions_return

    def save_settings(self, data):
        # Scrittura ATOMICA: un solo save_settings con i tre toggle.
        self.save_calls += 1
        self.saved_settings = dict(data)


class _ServiceSaveRaises(_CapturingService):
    """save_settings solleva (es. errore DB transitorio)."""

    def save_settings(self, data):
        raise RuntimeError("write error simulato")


class _ServiceEnabledTrue(_CapturingService):
    anomaly_enabled_return = True
    anomaly_alerts_return = True
    anomaly_actions_return = False


class _ServiceEnabledFalse(_CapturingService):
    anomaly_enabled_return = False


class _ServiceEnabledNone(_CapturingService):
    anomaly_enabled_return = None


class _ServiceNoAnomaly(FakeSettingsService):
    """Nessun metodo save_settings/load_anomaly_* (servizio non compatibile)."""


class _RealBackedService(FakeSettingsService):
    """Delega save_settings e load_anomaly_* al SettingsService REALE (+ DB
    in-memory): prova il contratto chiavi/coercizione end-to-end attraverso il
    codice di produzione, non solo il fake."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.real = SettingsService(_InMemorySettingsDb())

    def load_anomaly_enabled(self):
        return self.real.load_anomaly_enabled()

    def load_anomaly_alerts_enabled(self):
        return self.real.load_anomaly_alerts_enabled()

    def load_anomaly_actions_enabled(self):
        return self.real.load_anomaly_actions_enabled()

    def save_settings(self, data):
        self.real.save_settings(data)


def _make(monkeypatch, cls):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=cls)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_anomaly_toggles(monkeypatch):
    app = _make(monkeypatch, _ServiceEnabledTrue)
    try:
        assert bool(app.anomaly_enabled_var.get()) is True
        assert bool(app.anomaly_alerts_enabled_var.get()) is True
        assert bool(app.anomaly_actions_enabled_var.get()) is False
    finally:
        app.destroy()


def test_gui_loads_enabled_false(monkeypatch):
    app = _make(monkeypatch, _ServiceEnabledFalse)
    try:
        assert bool(app.anomaly_enabled_var.get()) is False
    finally:
        app.destroy()


def test_gui_none_maps_to_checked(monkeypatch):
    # None (non configurato) => watchdog default-ON => checkbox spuntata.
    app = _make(monkeypatch, _ServiceEnabledNone)
    try:
        assert bool(app.anomaly_enabled_var.get()) is True
    finally:
        app.destroy()


def test_gui_saves_anomaly_toggles(monkeypatch):
    app = _make(monkeypatch, _ServiceEnabledTrue)
    try:
        app.anomaly_enabled_var.set(True)
        app.anomaly_alerts_enabled_var.set(True)
        app.anomaly_actions_enabled_var.set(True)
        app._save_watchdog_settings()
        # Scrittura ATOMICA: UN solo save_settings con tutti e 3 i toggle (int).
        assert app.settings_service.save_calls == 1
        assert app.settings_service.saved_settings == {
            "anomaly_enabled": 1,
            "anomaly_alerts_enabled": 1,
            "anomaly_actions_enabled": 1,
        }
    finally:
        app.destroy()


def test_gui_disable_persists_explicit_false(monkeypatch):
    # Togliere la spunta al watchdog (partito ON via None) => persiste 0 (False).
    app = _make(monkeypatch, _ServiceEnabledNone)
    try:
        assert bool(app.anomaly_enabled_var.get()) is True
        app.anomaly_enabled_var.set(False)
        app._save_watchdog_settings()
        assert app.settings_service.saved_settings["anomaly_enabled"] == 0
    finally:
        app.destroy()


def test_gui_save_without_service_methods_no_crash(monkeypatch):
    # Servizio privo di save_settings: la guardia scatta (nessun crash, no save).
    app = _make(monkeypatch, _ServiceNoAnomaly)
    try:
        assert not hasattr(app.settings_service, "save_settings")  # guard e' esercitata
        app._save_watchdog_settings()  # non deve sollevare
        assert not hasattr(app.settings_service, "saved_settings")
    finally:
        app.destroy()


def test_gui_save_round_trips_through_real_service(monkeypatch):
    # Contratto chiavi GUI<->service REALE: il save handler scrive via
    # SettingsService.save_settings e i load_anomaly_* reali rileggono i valori
    # (incluso 0/1 -> False/True e None su chiave assente). Prova end-to-end che le
    # chiavi hardcoded e la coercizione int(bool()) combaciano col codice reale.
    app = _make(monkeypatch, _RealBackedService)
    try:
        real = app.settings_service.real
        assert real.load_anomaly_enabled() is None  # nessuna chiave => default-ON
        app.anomaly_enabled_var.set(False)          # disabilita il watchdog
        app.anomaly_alerts_enabled_var.set(True)
        app.anomaly_actions_enabled_var.set(False)
        app._save_watchdog_settings()
        assert real.load_anomaly_enabled() is False
        assert real.load_anomaly_alerts_enabled() is True
        assert real.load_anomaly_actions_enabled() is False
        # ri-attivazione esplicita
        app.anomaly_enabled_var.set(True)
        app._save_watchdog_settings()
        assert real.load_anomaly_enabled() is True
    finally:
        app.destroy()


def test_gui_save_error_no_crash(monkeypatch):
    # save_settings che solleva => gestito, nessun crash (errore all'utente).
    app = _make(monkeypatch, _ServiceSaveRaises)
    try:
        app.anomaly_enabled_var.set(True)
        app._save_watchdog_settings()  # non deve propagare l'eccezione
    finally:
        app.destroy()
