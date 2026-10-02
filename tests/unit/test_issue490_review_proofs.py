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


@pytest.mark.parametrize("invalid,code", [("missing", "CERT_FILE_MISSING"), ("permissions", "CERT_PERMISSIONS_UNSAFE"), ("malformed", "CERT_INVALID_FORMAT"), ("future", "CERT_NOT_YET_VALID")])
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
    elif invalid == "future":
        from tests.fixtures.betfair_tls_synthetic import SYNTHETIC_FUTURE_CERT
        Path(cfg.certificate).write_text(SYNTHETIC_FUTURE_CERT)
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


@pytest.mark.parametrize("failure", ["wrong-master-key", "corrupt-ciphertext", "unsupported-ciphertext"])
def test_live_key_decode_failure_directs_recovery_without_overwriting_ciphertext(setup, readiness_app, failure, caplog):
    import sqlite3
    from core.secret_cipher import SecretCipher
    from config_registry import readiness_report
    from headless_main import HeadlessApp
    db, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="demo-live-recoverable"))
    raw = db._execute("SELECT value FROM settings WHERE key='app_key_live'", fetchone=True, commit=False)["value"]
    assert raw.startswith("enc:v1:")
    # Prove the actual read contract, not the plaintext integration fixtures.
    assert settings.get_all_settings()["app_key_live"] == "demo-live-recoverable"
    correct_cipher = db._cipher
    if failure == "wrong-master-key":
        for seed in range(1, 256):
            wrong = SecretCipher(bytes([seed]) * 32, key_source="env")
            if wrong.decrypt(raw) == "":
                db._cipher = wrong
                break
        else:
            pytest.fail("No unreadable wrong-key case found")
    else:
        broken = "enc:v1:broken" if failure == "corrupt-ciphertext" else "enc:v2:unsupported"
        db._execute("UPDATE settings SET value=? WHERE key='app_key_live'", (broken,))
    def snapshot():
        with sqlite3.connect(db.db_path) as conn:
            return dict(conn.execute("SELECT key,value FROM settings"))
    before = snapshot()
    try:
        for _ in range(2):
            report = readiness_report(readiness_app.runtime, execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
            assert report["blockers"] == ["LIVE_APP_KEY_UNAVAILABLE"]
            assert report["details"]["betfair_credentials"] == {"live_key_present": False, "readable": False}
            presence = next(item for item in report["items"] if item.key == "betfair_live_key_present")
            assert not presence.ok and presence.blocker == "LIVE_APP_KEY_UNAVAILABLE"
            assert "master key" in presence.remedy
            item = next(item for item in report["items"] if item.blocker == "LIVE_APP_KEY_UNAVAILABLE")
            assert "master key" in item.remedy
            status = readiness_app.runtime.get_deploy_gate_status(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)
            text, exit_code = object.__new__(HeadlessApp)._format_preflight_report(status, "LIVE", True, True)
            assert exit_code == 2 and item.remedy in text
            assert "Imposta e salva la App Key Live" not in text
            assert "demo-live-recoverable" not in text + str(report) + caplog.text
        assert snapshot() == before
        sim = readiness_app.runtime.evaluate_live_readiness(execution_mode="SIMULATION")
        assert "LIVE_APP_KEY_UNAVAILABLE" not in sim["blockers"]
    finally:
        db._cipher = correct_cipher
        db._execute("UPDATE settings SET value=? WHERE key='app_key_live'", (raw,))
    assert readiness_app.runtime.evaluate_live_readiness(execution_mode="LIVE", live_enabled=True, live_readiness_ok=True)["ready"]


@pytest.mark.parametrize("field", ["app_key_delayed", "app_key_live", "app_key"])
@pytest.mark.parametrize("failure", ["wrong-master-key", "corrupt-ciphertext", "unsupported-ciphertext"])
def test_load_save_preserves_unreadable_app_key_until_replaced(setup, field, failure):
    from core.secret_cipher import SecretCipher
    db, settings, cfg = setup
    db.save_settings({"username": cfg.username, "certificate": cfg.certificate,
                      "private_key": cfg.private_key, field: "demo-recoverable"})
    original = db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"]
    correct_cipher = db._cipher
    if failure == "wrong-master-key":
        for seed in range(1, 256):
            wrong = SecretCipher(bytes([seed]) * 32, key_source="env")
            if wrong.decrypt(original) == "":
                db._cipher = wrong
                break
        else:
            pytest.fail("No unreadable wrong-key case found")
        broken = original
    else:
        broken = "enc:v1:broken" if failure == "corrupt-ciphertext" else "enc:v2:unsupported"
        db._execute("UPDATE settings SET value=? WHERE key=?", (broken, field))
    try:
        loaded = settings.load_betfair_config()
        # The operator can reselect readable certificate paths even when the
        # old master key also made stored paths unreadable.
        loaded = replace(loaded, certificate=cfg.certificate, private_key=cfg.private_key)
        settings.save_betfair_config(loaded, password="")
        stored = db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"]
        assert stored == broken
        canonical = "app_key_live" if field == "app_key_live" else "app_key_delayed"
        if field == "app_key":
            assert db._execute("SELECT value FROM settings WHERE key=?", (canonical,), fetchone=True, commit=False) is None
        else:
            assert getattr(settings.load_betfair_config(), canonical) == ""
        # Restoration is possible because an unrelated save kept the original bytes.
        db._cipher = correct_cipher
        db._execute("UPDATE settings SET value=? WHERE key=?", (original, field))
        assert getattr(settings.load_betfair_config(), canonical) == "demo-recoverable"
        db._execute("UPDATE settings SET value=? WHERE key=?", (broken, canonical))
        replacement = replace(cfg, **{canonical: "demo-explicit-replacement"})
        settings.save_betfair_config(replacement)
        assert getattr(settings.load_betfair_config(), canonical) == "demo-explicit-replacement"
    finally:
        db._cipher = correct_cipher


def test_registry_legacy_report_is_read_only_and_bootstrap_still_migrates(setup):
    import sqlite3
    from config_registry import ConfigRegistry
    db, settings, cfg = setup
    db.save_settings({"username": cfg.username, "app_key": "demo-legacy",
                      "certificate": cfg.certificate, "private_key": cfg.private_key})
    def snapshot():
        with sqlite3.connect(db.db_path) as conn:
            return dict(conn.execute("SELECT key,value FROM settings"))
    before = snapshot()
    entries = {e.key: e for e in ConfigRegistry(settings).entries()}
    assert snapshot() == before
    assert entries["betfair.app_key_delayed"].valid
    assert not entries["betfair.app_key_live"].valid
    conn = db._get_connection()
    writes = {sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE}
    conn.set_authorizer(lambda action, *args: sqlite3.SQLITE_DENY if action in writes else sqlite3.SQLITE_OK)
    try:
        entries = {e.key: e for e in ConfigRegistry(settings).entries()}
        assert entries["betfair.username"].valid
        assert entries["betfair.app_key_delayed"].valid
        assert snapshot() == before
    finally:
        conn.set_authorizer(None)
    assert settings.load_betfair_config().app_key_delayed == "demo-legacy"
    assert "app_key_delayed" in snapshot() and snapshot()["app_key_live"] == ""


def test_readable_app_keys_can_still_be_explicitly_cleared(setup):
    db, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_delayed="demo-delayed", app_key_live="demo-live"))
    settings.save_betfair_config(cfg)
    assert settings.load_betfair_config().app_key_delayed == ""
    assert settings.load_betfair_config().app_key_live == ""


@pytest.mark.parametrize("field", ["app_key_delayed", "app_key_live", "app_key"])
@pytest.mark.parametrize("change", ["restore", "replace"])
def test_stale_unreadable_form_does_not_clear_recovered_or_replaced_key(setup, field, change):
    db, settings, cfg = setup
    db.save_settings({"username": cfg.username, "certificate": cfg.certificate,
                      "private_key": cfg.private_key, field: "demo-original"})
    original = db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"]
    db._execute("UPDATE settings SET value=? WHERE key=?", ("enc:v1:broken", field))
    stale = settings.load_betfair_config()
    canonical = "app_key_live" if field == "app_key_live" else "app_key_delayed"
    if change == "restore":
        db._execute("UPDATE settings SET value=? WHERE key=?", (original, field))
        expected = "demo-original"
    else:
        db.save_settings({field: "demo-concurrent-replacement"})
        expected = "demo-concurrent-replacement"
    assert getattr(settings.load_betfair_config(), canonical) == expected
    before = db._execute("SELECT value FROM settings WHERE key=?", (canonical,), fetchone=True, commit=False)["value"]
    settings.save_betfair_config(stale)
    assert db._execute("SELECT value FROM settings WHERE key=?", (canonical,), fetchone=True, commit=False)["value"] == before
    assert getattr(settings.load_betfair_config(), canonical) == expected
    # An explicit replacement still wins even from that old form.
    settings.save_betfair_config(replace(stale, **{canonical: "demo-user-replacement"}))
    assert getattr(settings.load_betfair_config(), canonical) == "demo-user-replacement"


@pytest.mark.parametrize("field", ["app_key_delayed", "app_key_live"])
@pytest.mark.parametrize("broken", ["enc:v1:broken", "enc:v2:unsupported"])
def test_registry_entries_direct_unreadable_key_to_recovery(setup, field, broken):
    from config_registry import ConfigRegistry
    db, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_delayed="demo-delayed", app_key_live="demo-live"))
    original = db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"]
    db._execute("UPDATE settings SET value=? WHERE key=?", (broken, field))
    entry = next(e for e in ConfigRegistry(settings).entries() if e.key == "betfair." + field)
    assert not entry.valid and entry.value == "(errore lettura)"
    assert "master key" in entry.remedy
    assert "Imposta" not in entry.remedy
    assert db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"] == broken
    db._execute("UPDATE settings SET value=? WHERE key=?", (original, field))
    assert next(e for e in ConfigRegistry(settings).entries() if e.key == "betfair." + field).valid


@pytest.mark.parametrize("validity", ["expired", "future"])
def test_invalid_dates_matching_certificate_save_is_rejected_without_changes(setup, tmp_path, validity):
    import ssl
    from tests.fixtures.betfair_tls_synthetic import SYNTHETIC_EXPIRED_CERT, SYNTHETIC_FUTURE_CERT
    db, settings, cfg = setup
    settings.save_betfair_config(replace(cfg, app_key_live="demo-kept"), password="demo-kept-password")
    before = {
        row['key']: row['value'] for row in db._execute("SELECT key,value FROM settings", fetch=True, commit=False)
    }
    expired = tmp_path / "expired.crt"
    expired.write_text(SYNTHETIC_EXPIRED_CERT if validity == "expired" else SYNTHETIC_FUTURE_CERT)
    # This is a real parseable matching pair, not a mock TLS validator.
    ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).load_cert_chain(str(expired), cfg.private_key)
    with pytest.raises(ValueError, match="scaduto" if validity == "expired" else "non ancora valido"):
        settings.save_betfair_config(replace(cfg, certificate=str(expired), app_key_live="demo-replacement"), password="demo-new")
    assert {row['key']: row['value'] for row in db._execute("SELECT key,value FROM settings", fetch=True, commit=False)} == before
    settings.save_betfair_config(replace(cfg, app_key_live="demo-valid-after-error"))
    assert settings.load_betfair_config().app_key_live == "demo-valid-after-error"


@pytest.mark.parametrize("field", ["app_key_delayed", "app_key_live"])
def test_load_status_snapshot_remains_consistent_with_concurrent_recovery(setup, monkeypatch, field):
    import threading
    db, settings, cfg = setup
    settings.save_betfair_config(cfg)
    db._execute("UPDATE settings SET value=? WHERE key=?", ("enc:v1:broken", field))
    original_getter = db.get_settings
    done = threading.Event()
    failures = []
    workers = []
    def recover():
        try:
            db.save_settings({field: "demo-recovered-between-reads"})
        except Exception as exc:
            failures.append(type(exc).__name__)
        finally:
            done.set()
    def captured_read():
        data = original_getter()
        if not workers:
            worker = threading.Thread(target=recover)
            workers.append(worker)
            worker.start()
            # Without a load transaction the writer completes here, so the
            # next status read disagrees with the captured empty value.
            done.wait(0.1)
        return data
    monkeypatch.setattr(db, "get_settings", captured_read)
    loaded = settings.load_betfair_config()
    workers[0].join(timeout=3)
    assert not workers[0].is_alive() and not failures
    assert getattr(loaded, field) == ""
    assert field in loaded.app_keys_unreadable
    settings.save_betfair_config(loaded)
    assert getattr(settings.load_betfair_config(), field) == "demo-recovered-between-reads"
