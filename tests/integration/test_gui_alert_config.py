"""G3 — Impostazioni Alert Telegram editabili in GUI (tab Alert).

Routing/soglie delle notifiche (alerts_enabled, chat_id/name, min_alert_severity,
cooldown, dedup, format_rich) erano configurabili solo via DB. Ora il tab "Alert"
le espone. Gia' enforced live in telegram_alerts_service: pura esposizione +
validazione. Il service (settings_service.load_telegram_config_row /
save_telegram_alert_settings) e' gia' completo: nessuna modifica al backend.

Il saver dedicato read-merge preserva le credenziali di sessione Telegram.
"""
from __future__ import annotations

import os
import sys

import pytest

from services.settings_service import SettingsService

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_mini_gui_integration import _install_mini_gui_fakes, FakeSettingsService  # noqa: E402

pytestmark = pytest.mark.integration


class _InMemoryTelegramDb:
    """DB in-memory che rispetta il contratto usato dal service reale."""

    def __init__(self):
        self.row = {}

    def get_telegram_settings(self):
        return dict(self.row)

    def save_telegram_settings(self, row):
        self.row = dict(row or {})


class _CapturingService(FakeSettingsService):
    _row = {
        "alerts_enabled": True,
        "alerts_chat_id": "999",
        "alerts_chat_name": "Ops",
        "min_alert_severity": "ERROR",
        "alert_cooldown_sec": 120,
        "alert_dedup_enabled": False,
        "alert_format_rich": False,
    }

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.saved = None
        self.save_calls = 0

    def load_telegram_config_row(self):
        return dict(type(self)._row)

    def save_telegram_alert_settings(self, **kwargs):
        self.save_calls += 1
        self.saved = dict(kwargs)


class _ServiceSaveRaises(_CapturingService):
    def save_telegram_alert_settings(self, **kwargs):
        raise RuntimeError("write error simulato")


class _ServiceNoAlert(FakeSettingsService):
    """Nessun metodo save_telegram_alert_settings (servizio non compatibile)."""


class _RealBackedService(FakeSettingsService):
    """Delega load/save al SettingsService REALE (+ DB in-memory): prova il
    contratto chiavi/coercizione end-to-end attraverso il codice di produzione."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.real = SettingsService(_InMemoryTelegramDb())

    def load_telegram_config_row(self):
        return self.real.load_telegram_config_row()

    def save_telegram_alert_settings(self, **kwargs):
        self.real.save_telegram_alert_settings(**kwargs)


def _make(monkeypatch, cls):
    mg = _install_mini_gui_fakes(monkeypatch, settings_service=cls)
    return mg.MiniPickfairGUI(test_mode=True)


def test_gui_loads_alert_settings(monkeypatch):
    app = _make(monkeypatch, _CapturingService)
    try:
        assert bool(app.alert_enabled_var.get()) is True
        assert app.alert_chat_id_var.get() == "999"
        assert app.alert_chat_name_var.get() == "Ops"
        assert app.alert_min_severity_var.get() == "ERROR"
        assert app.alert_cooldown_var.get() == "120"
        assert bool(app.alert_dedup_var.get()) is False
        assert bool(app.alert_format_rich_var.get()) is False
    finally:
        app.destroy()


def test_gui_saves_alert_settings(monkeypatch):
    app = _make(monkeypatch, _CapturingService)
    try:
        app.alert_enabled_var.set(True)
        app.alert_chat_id_var.set("555")
        app.alert_chat_name_var.set("Room")
        app.alert_min_severity_var.set("HIGH")
        app.alert_cooldown_var.set("90")
        app.alert_dedup_var.set(True)
        app.alert_format_rich_var.set(True)
        app._save_alert_settings()
        assert app.settings_service.save_calls == 1
        assert app.settings_service.saved == {
            "alerts_enabled": True,
            "alerts_chat_id": "555",
            "alerts_chat_name": "Room",
            "min_alert_severity": "HIGH",
            "alert_cooldown_sec": 90,
            "alert_dedup_enabled": True,
            "alert_format_rich": True,
        }
    finally:
        app.destroy()


def test_gui_save_blocks_invalid_severity(monkeypatch):
    app = _make(monkeypatch, _CapturingService)
    try:
        app.alert_enabled_var.set(False)
        app.alert_chat_id_var.set("1")
        app.alert_min_severity_var.set("BOGUS")
        app._save_alert_settings()
        assert app.settings_service.saved is None
    finally:
        app.destroy()


@pytest.mark.parametrize("bad", ["-5", "abc", "", "0.5", "1.9", "inf"])
def test_gui_save_blocks_invalid_cooldown(monkeypatch, bad):
    app = _make(monkeypatch, _CapturingService)
    try:
        app.alert_enabled_var.set(False)
        app.alert_chat_id_var.set("1")
        app.alert_min_severity_var.set("WARNING")
        app.alert_cooldown_var.set(bad)
        app._save_alert_settings()
        assert app.settings_service.saved is None, bad
    finally:
        app.destroy()


def test_gui_save_blocks_enabled_without_chat(monkeypatch):
    # Alert attivi senza chat_id => il service farebbe no-op silenzioso: blocco.
    app = _make(monkeypatch, _CapturingService)
    try:
        app.alert_enabled_var.set(True)
        app.alert_chat_id_var.set("   ")
        app.alert_min_severity_var.set("WARNING")
        app.alert_cooldown_var.set("300")
        app._save_alert_settings()
        assert app.settings_service.saved is None
    finally:
        app.destroy()


def test_gui_save_disabled_allows_empty_chat(monkeypatch):
    # Alert DISATTIVI: chat_id vuoto e' consentito (nessun vincolo).
    app = _make(monkeypatch, _CapturingService)
    try:
        app.alert_enabled_var.set(False)
        app.alert_chat_id_var.set("")
        app.alert_min_severity_var.set("WARNING")
        app.alert_cooldown_var.set("300")
        app._save_alert_settings()
        assert app.settings_service.saved is not None
        assert app.settings_service.saved["alerts_enabled"] is False
    finally:
        app.destroy()


def test_gui_save_without_service_methods_no_crash(monkeypatch):
    app = _make(monkeypatch, _ServiceNoAlert)
    try:
        assert not hasattr(app.settings_service, "save_telegram_alert_settings")
        app._save_alert_settings()  # non deve sollevare
    finally:
        app.destroy()


def test_gui_save_error_no_crash(monkeypatch):
    app = _make(monkeypatch, _ServiceSaveRaises)
    try:
        app.alert_enabled_var.set(True)
        app.alert_chat_id_var.set("1")
        app.alert_min_severity_var.set("WARNING")
        app.alert_cooldown_var.set("300")
        app._save_alert_settings()  # gestito, nessun crash
    finally:
        app.destroy()


def test_gui_save_round_trips_through_real_service(monkeypatch):
    # Contratto chiavi/coercizione GUI<->service REALE: save handler -> load reale.
    app = _make(monkeypatch, _RealBackedService)
    try:
        real = app.settings_service.real
        app.alert_enabled_var.set(True)
        app.alert_chat_id_var.set("555")
        app.alert_chat_name_var.set("Room")
        app.alert_min_severity_var.set("HIGH")
        app.alert_cooldown_var.set("90")
        app.alert_dedup_var.set(False)
        app.alert_format_rich_var.set(False)
        app._save_alert_settings()
        row = real.load_telegram_config_row()
        assert row["alerts_enabled"] is True
        assert str(row["alerts_chat_id"]) == "555"
        assert row["alerts_chat_name"] == "Room"
        assert row["min_alert_severity"] == "HIGH"
        assert row["alert_cooldown_sec"] == 90
        assert row["alert_dedup_enabled"] is False
        assert row["alert_format_rich"] is False
    finally:
        app.destroy()
