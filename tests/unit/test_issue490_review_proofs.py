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


@pytest.mark.parametrize("invalid,code", [("missing", "CERT_FILE_MISSING"), ("permissions", "CERT_PERMISSIONS_UNSAFE"), ("malformed", "CERT_INVALID_FORMAT")])
def test_certificate_diagnostic_survives_credential_overlap(setup, monkeypatch, invalid, code):
    _, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="CERT"), password="demo-password")
    if invalid == "missing":
        Path(cfg.certificate).unlink()
    elif invalid == "permissions":
        import os
        if os.name != "posix":
            pytest.skip("POSIX file permission contract")
        Path(cfg.private_key).chmod(0o666)
    else:
        Path(cfg.certificate).write_text("not PEM")
    service, http, _ = wire_client(settings, monkeypatch)
    with pytest.raises(RuntimeError, match="^" + code):
        service.connect(simulation_mode=False)
    assert service.last_error.startswith(code)
    assert not http.calls and service.get_live_client() is None


def test_failed_legacy_decrypt_does_not_finalize_migration_then_recovers(setup):
    from core.secret_cipher import SecretCipher
    db, settings, _ = setup
    db.save_settings({"app_key": "demo-legacy-recoverable"})
    correct = db._cipher
    raw = db._execute("SELECT value FROM settings WHERE key='app_key'", fetchone=True, commit=False)["value"]
    # Find an actually unreadable wrong-key decode; random nonce never decides
    # whether this regression case exercises the intended failure path.
    for seed in range(1, 256):
        wrong = SecretCipher(bytes([seed]) * 32, key_source="env")
        if wrong.decrypt(raw) == "":
            db._cipher = wrong
            break
    else:
        pytest.fail("No unreadable wrong-key case found")
    assert db.get_settings()["app_key"] == ""
    settings.load_betfair_config()
    assert "app_key_delayed" not in db.get_settings()
    db._cipher = correct
    assert settings.load_betfair_config().app_key_delayed == "demo-legacy-recoverable"


def test_short_session_token_is_redacted_and_masks_do_not_expand(setup):
    _, _, cfg = setup
    client = BetfairClient(username=cfg.username, app_key="APP",
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=NoNetworkSession())
    client._set_session_state(session_token="tiny", session_expiry="", connected=True)
    masked = client._redact_error_text("echo=APP; token=tiny")
    assert "APP" not in masked and "tiny" not in masked
    assert client._redact_error_text(masked) == masked


def test_authoritative_readiness_blocks_empty_live_key_without_affecting_sim(setup, monkeypatch):
    import mini_gui as gui
    db, settings, cfg = setup
    db.save_settings({"app_key": "demo-legacy"})
    settings.save_roserpina_config(replace(settings.load_roserpina_config(), max_daily_loss=10,
                                         max_open_exposure=10, max_drawdown_hard_stop_pct=20))
    monkeypatch.setattr(gui, "Database", lambda: db)
    monkeypatch.setattr(gui, "TelegramTabUI", lambda *args: None)
    app = gui.MiniPickfairGUI(test_mode=True, force_simulation=True)
    app.runtime.simulation_mode = False
    try:
        report = app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        assert report["blockers"] == ["LIVE_APP_KEY_MISSING"]
        assert "LIVE_APP_KEY_MISSING" in report["blockers"]
        assert not app.runtime.get_deploy_gate_status(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)["allowed"]
        sim = app.runtime.evaluate_live_readiness(execution_mode="SIMULATION")
        assert "LIVE_APP_KEY_MISSING" not in sim["blockers"]
        settings.save_betfair_config(replace(cfg, app_key_live="demo-live"))
        report = app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        assert "LIVE_APP_KEY_MISSING" not in report["blockers"]
        assert report["ready"] is True
        db._execute("ALTER TABLE settings RENAME TO unavailable_settings")
        try:
            blocked = app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
            assert blocked["blockers"] == ["LIVE_APP_KEY_UNAVAILABLE"]
            assert not blocked["ready"]
        finally:
            db._execute("ALTER TABLE unavailable_settings RENAME TO settings")
        assert app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)["ready"]
    finally:
        app.destroy()


def test_migration_preserves_ciphertext_even_if_wrong_key_decodes_utf8(setup):
    from core.secret_cipher import SecretCipher
    db, settings, _ = setup
    db.save_settings({"app_key": "x"})
    correct = db._cipher
    raw = db._execute("SELECT value FROM settings WHERE key='app_key'", fetchone=True, commit=False)["value"]
    for seed in range(1, 256):
        wrong = SecretCipher(bytes([seed]) * 32, key_source="env")
        if wrong.decrypt(raw) not in ("", "x"):
            db._cipher = wrong
            break
    else:
        pytest.fail("No nonempty wrong-key decode found")
    settings.load_betfair_config()
    migrated = db._execute("SELECT value FROM settings WHERE key='app_key_delayed'", fetchone=True, commit=False)["value"]
    assert migrated == raw
    db._cipher = correct
    assert settings.load_betfair_config().app_key_delayed == "x"


def test_legacy_writer_updates_delayed_after_migration_preserving_live_and_password(setup):
    db, settings, cfg = setup
    db.save_settings({"app_key": "demo-legacy-old"})
    migrated = settings.load_betfair_config()
    settings.save_betfair_config(replace(cfg, app_key_delayed=migrated.app_key_delayed,
                                       app_key_live="demo-live-kept"), password="demo-password-kept")
    db.save_credentials(username=cfg.username, app_key="demo-legacy-new",
                        certificate=cfg.certificate, private_key=cfg.private_key)
    loaded = settings.load_betfair_config()
    assert loaded.app_key_delayed == "demo-legacy-new"
    assert loaded.app_key_live == "demo-live-kept"
    assert settings.load_password() == "demo-password-kept"


def test_redacted_login_failure_is_written_to_windows_cp1252_log(setup, monkeypatch):
    import io
    import logging
    import sys
    import services.betfair_service as module
    _, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="APP"), password="demo-password")
    service, http, _ = wire_client(settings, monkeypatch)
    def fail(*args, **kwargs):
        raise ConnectionError("auth_APP_failed")
    monkeypatch.setattr(http, "post", fail)
    errors = []
    class StrictHandler(logging.StreamHandler):
        def handleError(self, record):
            errors.append(sys.exc_info()[0])
    buffer = io.BytesIO()
    stream = io.TextIOWrapper(buffer, encoding="cp1252", errors="strict")
    handler = StrictHandler(stream)
    module.logger.addHandler(handler)
    try:
        with pytest.raises(RuntimeError, match="LOGIN_NETWORK_ERROR"):
            service.connect(simulation_mode=False)
        stream.flush()
        assert not errors
        logged = buffer.getvalue().decode("cp1252")
        assert "LOGIN_NETWORK_ERROR" in logged
        assert "auth_APP_failed" not in logged and "APP" not in logged
    finally:
        module.logger.removeHandler(handler)
        handler.close()
        stream.close()


@pytest.fixture
def readiness_app(setup, monkeypatch):
    import mini_gui as gui
    db, settings, _ = setup
    settings.save_roserpina_config(replace(settings.load_roserpina_config(), max_daily_loss=10,
                                         max_open_exposure=10, max_drawdown_hard_stop_pct=20))
    monkeypatch.setattr(gui, "Database", lambda: db)
    monkeypatch.setattr(gui, "TelegramTabUI", lambda *args: None)
    app = gui.MiniPickfairGUI(test_mode=True, force_simulation=True)
    app.runtime.simulation_mode = False
    try:
        yield app
    finally:
        app.destroy()


def test_live_preflight_does_not_migrate_legacy_settings(setup, readiness_app):
    import sqlite3
    db, settings, _ = setup
    # Reproduce an old physical DB after bootstrap, before any credentials
    # loader; preflight alone must not create canonical credential rows.
    db._execute("DELETE FROM settings WHERE key IN ('app_key_delayed','app_key_live')")
    db.save_settings({"app_key": "demo-legacy-unmigrated"})
    def snapshot():
        with sqlite3.connect(db.db_path) as conn:
            return dict(conn.execute("SELECT key,value FROM settings"))
    before = snapshot()
    for _ in range(2):
        report = readiness_app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        assert report["blockers"] == ["LIVE_APP_KEY_MISSING"]
        status = readiness_app.runtime.get_deploy_gate_status(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        assert not status["allowed"]
    assert snapshot() == before
    # Migration is still available through the explicit credentials loader.
    assert settings.load_betfair_config().app_key_delayed == "demo-legacy-unmigrated"


@pytest.mark.parametrize("unavailable,code", [(False, "LIVE_APP_KEY_MISSING"), (True, "LIVE_APP_KEY_UNAVAILABLE")])
def test_live_key_blockers_have_actionable_registry_and_headless_remedies(setup, readiness_app, unavailable, code):
    from config_registry import readiness_report
    from headless_main import HeadlessApp
    db, _, _ = setup
    if unavailable:
        db._execute("ALTER TABLE settings RENAME TO unavailable_settings")
    try:
        report = readiness_report(readiness_app.runtime, execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        item = next(item for item in report["items"] if item.blocker == code)
        assert not item.ok and "non catalogato" not in item.remedy.lower()
        assert "Live" in item.remedy if not unavailable else "DB" in item.remedy
        status = readiness_app.runtime.get_deploy_gate_status(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
        text, exit_code = object.__new__(HeadlessApp)._format_preflight_report(status, "LIVE", True, True)
        assert exit_code == 2 and code in text
        assert "non catalogato" not in text.lower()
        assert item.remedy in text
    finally:
        if unavailable:
            db._execute("ALTER TABLE unavailable_settings RENAME TO settings")
