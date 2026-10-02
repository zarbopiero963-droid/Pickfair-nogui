"""Current-head review evidence: real settings/client, HTTP boundary only."""
from dataclasses import replace
import inspect
from pathlib import Path

import pytest
from requests.exceptions import ConnectionError, HTTPError, Timeout

from tests.acceptance.test_issue461_pr04_quater import setup, wire_client, NoNetworkSession
from betfair_client import BetfairClient


@pytest.mark.parametrize("key", ["e", "SESSION"])
def test_short_app_key_masks_every_raw_echo(setup, key):
    _, _, cfg = setup
    client = BetfairClient(username=cfg.username, app_key=key,
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=NoNetworkSession())
    raw = f"auth_{key}_failed; key={key}"
    masked = client._redact_error_text(raw)
    assert f"auth_{key}_failed" not in masked
    assert f"key={key}" not in masked


@pytest.mark.parametrize("error,code", [
    (Timeout("demo-live-secret"), "LOGIN_TIMEOUT"),
    (ConnectionError("demo-live-secret"), "LOGIN_NETWORK_ERROR"),
    (HTTPError("demo-live-secret"), "LOGIN_HTTP_ERROR"),
])
def test_real_login_error_contract_and_redaction(setup, monkeypatch, caplog, error, code):
    _, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="demo-live-secret"), password="demo-password")
    service, http, _ = wire_client(settings, monkeypatch)
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(http, "post", fail)
    with pytest.raises(RuntimeError, match=code) as caught:
        service.connect(simulation_mode=False)
    assert caught.value.__suppress_context__
    assert code in service.last_error
    assert not service.connected and service.get_live_client() is None
    for value in (str(caught.value), service.last_error, caplog.text):
        assert "demo-live-secret" not in value


def test_settings_wrapper_uses_real_modified_implementation(setup):
    from services.setting_service import SettingsService as BaseSettings
    from services.settings_service import SettingsService
    _, settings, _ = setup
    assert isinstance(settings, SettingsService)
    assert BaseSettings in SettingsService.__mro__
    assert settings.save_betfair_config.__func__ is BaseSettings.save_betfair_config
    assert Path(inspect.getsourcefile(settings.save_betfair_config)).parts[-2:] == ("services", "setting_service.py")


def test_service_preserves_codes_with_short_key_and_password(setup, monkeypatch):
    _, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="SESSION"), password="e")
    service, http, _ = wire_client(settings, monkeypatch)
    def fail(*args, **kwargs):
        raise ConnectionError("SESSION_EXPIRED; key=SESSION; password=e")
    monkeypatch.setattr(http, "post", fail)
    with pytest.raises(RuntimeError, match="LOGIN_NETWORK_ERROR") as caught:
        service.connect(simulation_mode=False)
    # Only the locally generated login code is authoritative; provider detail
    # that happens to resemble a code is still fully redacted.
    assert "key=SESSION" not in str(caught.value)
    assert "password=e" not in str(caught.value)


@pytest.mark.parametrize("key,echo", [("-x", "appKey-x"), ("abc123", "auth_abc123_failed")])
def test_concatenated_short_key_never_reaches_client_diagnostics(setup, key, echo):
    _, _, cfg = setup
    class FailingTransport(NoNetworkSession):
        def post(self, url, **kwargs):
            raise ConnectionError(echo)
    client = BetfairClient(username=cfg.username, app_key=key,
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=FailingTransport())
    with pytest.raises(RuntimeError, match="LOGIN_NETWORK_ERROR") as caught:
        client.login("demo-password")
    assert key not in str(caught.value)
    assert key not in str(client.io_snapshot())
    assert "LOGIN_NETWORK_ERROR" in client.io_snapshot()["last_error"]


@pytest.mark.parametrize("password,echo", [("abc123", "auth_abc123_failed"), ("e", "auth_e_failed")])
def test_concatenated_password_never_reaches_service_or_client_diagnostics(setup, monkeypatch, caplog, password, echo):
    _, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="demo-live-secret"), password=password)
    service, http, _ = wire_client(settings, monkeypatch)
    clients = []
    import services.betfair_service as module
    original = module.BetfairClient
    def capture(**kwargs):
        client = original(**kwargs)
        clients.append(client)
        return client
    monkeypatch.setattr(module, "BetfairClient", capture)
    def fail(*args, **kwargs):
        raise ConnectionError(echo)
    monkeypatch.setattr(http, "post", fail)
    with pytest.raises(RuntimeError, match="LOGIN_NETWORK_ERROR") as caught:
        service.connect(simulation_mode=False)
    assert caught.value.__suppress_context__
    for value in (str(caught.value), service.last_error, caplog.text, str(clients[0].io_snapshot())):
        assert echo not in value
        if len(password) > 1:
            assert password not in value
    assert "LOGIN_NETWORK_ERROR" in clients[0].io_snapshot()["last_error"]


def test_short_key_cannot_hide_real_exchange_session_expiry(setup):
    from types import SimpleNamespace
    _, _, cfg = setup
    class ExpiredTransport(NoNetworkSession):
        def post(self, url, **kwargs):
            return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                                   json=lambda: [{"error": {"errorCode": "INVALID_SESSION"}}])
    client = BetfairClient(username=cfg.username, app_key="SESSION",
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=ExpiredTransport())
    client._set_session_state(session_token="demo-session-token", session_expiry="", connected=True)
    with pytest.raises(RuntimeError, match="^SESSION_EXPIRED$"):
        client.get_account_funds()
    assert not client.connected and not client.session_token
