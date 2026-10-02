"""Current-head review evidence: real settings/client, HTTP boundary only."""
from dataclasses import replace
import inspect
from pathlib import Path

import pytest
from requests.exceptions import ConnectionError, HTTPError, Timeout

from tests.acceptance.test_issue461_pr04_quater import setup, wire_client, NoNetworkSession
from betfair_client import BetfairClient


@pytest.mark.parametrize("key", ["e", "SESSION"])
def test_short_app_key_masks_echo_without_corrupting_error_code(setup, key):
    _, _, cfg = setup
    client = BetfairClient(username=cfg.username, app_key=key,
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=NoNetworkSession())
    assert client._redact_error_text(f"SESSION_EXPIRED; key={key}") == "SESSION_EXPIRED; key=***APP_KEY***"


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
    assert "SESSION_EXPIRED" in str(caught.value)
    assert "key=SESSION" not in str(caught.value)
    assert "password=e" not in str(caught.value)
