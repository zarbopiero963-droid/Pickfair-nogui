"""T3 regressions: Betfair LIVE credentials, incomplete configuration (offline).

Row: PF-I04Q-02. Real Database/SettingsService/BetfairService/BetfairClient;
the HTTP session is the only fake boundary (it counts authentications).
"""
from __future__ import annotations

from dataclasses import replace

import pytest

from core.system_state import BetfairConfig
from database import Database
from services.betfair_service import BetfairService
from services.settings_service import SettingsService
from tests.fixtures.betfair_tls_synthetic import SYNTHETIC_CERT, SYNTHETIC_KEY


class _CountingSession:
    def __init__(self):
        self.calls = []

    def post(self, url, **kwargs):
        from types import SimpleNamespace
        self.calls.append(url)
        return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                               json=lambda: {"loginStatus": "SUCCESS", "sessionToken": "demo-session"})

    def close(self):
        pass


@pytest.fixture
def live_ready(tmp_path, monkeypatch):
    monkeypatch.setenv("PICKFAIR_SECRET_KEY", "21" * 32)
    cert = tmp_path / "cert.crt"
    key = tmp_path / "key.key"
    cert.write_text(SYNTHETIC_CERT)
    key.write_text(SYNTHETIC_KEY)
    key.chmod(0o600)
    db = Database(str(tmp_path / "creds.db"))
    settings = SettingsService(db)
    cfg = BetfairConfig(username="demo-user", certificate=str(cert), private_key=str(key))
    settings.save_betfair_config(replace(cfg, app_key_live="demo-live"), password="demo-password")

    import services.betfair_service as module
    from betfair_client import BetfairClient
    http = _CountingSession()
    built = []

    def build(**kwargs):
        built.append(kwargs["app_key"])
        return BetfairClient(**kwargs, session=http)

    monkeypatch.setattr(module, "BetfairClient", build)
    svc = BetfairService(settings)
    monkeypatch.setattr(svc, "_start_session_keepalive", lambda: None)
    yield db, settings, svc, http, built
    db.close_all_connections()


def test_pf_i04q_02_complete_credentials_control_authenticates_once(live_ready):
    """ID: PF-I04Q-02 (control: the fixture is able to authenticate, so the
    zero-call oracles below are not vacuous)."""
    _db, _settings, svc, http, built = live_ready
    assert svc.connect(simulation_mode=False)["connected"]
    assert built == ["demo-live"] and len(http.calls) == 1


@pytest.mark.parametrize("missing,reason", [
    ("username", "Configurazione Betfair incompleta"),
    ("password", "Password Betfair mancante"),
])
def test_pf_i04q_02_incomplete_credential_explicit_error_zero_authentications(live_ready, missing, reason):
    """ID: PF-I04Q-02

    Oracle: credenziale incompleta -> errore esplicito, ZERO autenticazioni
    (no client built, no POST on the transport, no live client).
    """
    db, _settings, svc, http, built = live_ready
    db.save_settings({missing: ""})
    with pytest.raises(RuntimeError, match=reason):
        svc.connect(simulation_mode=False)
    assert svc.last_error == reason
    assert built == [] and http.calls == []
    assert svc.get_live_client() is None
