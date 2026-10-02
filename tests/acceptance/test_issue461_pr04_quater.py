"""DEC-426-P28–P32: real settings/SQLite/client/GUI, synthetic TLS only.

The HTTP session is the external boundary. No exchange or Telegram I/O.
"""
from dataclasses import replace
import os
import sqlite3

import pytest

from core.system_state import BetfairConfig
from database import Database
from services.settings_service import SettingsService
from services.betfair_service import BetfairService


from tests.fixtures.betfair_tls_synthetic import SYNTHETIC_CERT, SYNTHETIC_KEY, SYNTHETIC_OTHER_KEY


@pytest.fixture
def setup(tmp_path, monkeypatch):
    monkeypatch.setenv("PICKFAIR_SECRET_KEY", "21" * 32)
    cert = tmp_path / "certificato con spazi.crt"
    key = tmp_path / "chiave con spazi.key"
    cert.write_text(SYNTHETIC_CERT)
    key.write_text(SYNTHETIC_KEY)
    key.chmod(0o600)
    db = Database(str(tmp_path / "credentials.db"))
    settings = SettingsService(db)
    cfg = BetfairConfig(username="demo-user", certificate=str(cert), private_key=str(key))
    yield db, settings, cfg
    db.close_all_connections()


def test_blank_password_preserves_saved_password(setup):
    db, settings, cfg = setup
    db.save_password("demo-saved-password")
    settings.save_betfair_config(cfg, password="")
    assert settings.load_password() == "demo-saved-password"


def test_legacy_migrates_only_to_delayed_and_survives_restart(setup):
    db, settings, cfg = setup
    # Physical pre-migration schema, not the compatibility writer, which now
    # updates the canonical Delayed row explicitly.
    db.save_settings({"username": cfg.username, "app_key": "demo-legacy",
                      "certificate": cfg.certificate, "private_key": cfg.private_key})
    db.save_password("demo-password")
    migrated = settings.load_betfair_config()
    assert migrated.app_key_delayed == "demo-legacy"
    assert migrated.app_key_live == ""
    assert db.get_settings()["app_key_delayed"] == "demo-legacy"
    assert settings.load_password() == "demo-password"
    db.close_all_connections()
    assert settings.load_betfair_config() == migrated


def test_explicit_empty_delayed_never_resurrects_legacy(setup):
    db, settings, _ = setup
    db.save_settings({"app_key": "demo-legacy", "app_key_delayed": "", "app_key_live": "demo-live"})
    cfg = settings.load_betfair_config()
    assert cfg.app_key_delayed == ""
    assert cfg.app_key_live == "demo-live"


def test_two_keys_encrypted_and_password_replaced_atomically(setup):
    db, settings, cfg = setup
    cfg = replace(cfg, app_key_delayed="demo-delayed", app_key_live="demo-live")
    settings.save_betfair_config(cfg, password="demo-new-password")
    assert settings.load_betfair_config().app_key_live == "demo-live"
    assert settings.load_betfair_config().app_key_delayed == "demo-delayed"
    assert settings.load_password() == "demo-new-password"
    with sqlite3.connect(db.db_path) as conn:
        raw = dict(conn.execute("SELECT key,value FROM settings"))
    for name in ("app_key_delayed", "app_key_live", "password"):
        assert raw[name].startswith("enc:")
        assert "demo-" not in raw[name]
    assert "demo-live" not in repr(cfg)
    assert "demo-delayed" not in repr(cfg)


@pytest.mark.parametrize("invalid", ["absent", "directory", "malformed-cert", "malformed-key", "mismatch"])
def test_invalid_files_block_without_partial_save_then_recover(setup, tmp_path, invalid):
    db, settings, cfg = setup
    db.save_settings({"username": "before", "password": "before-password"})
    before = db.get_settings()
    if invalid == "absent":
        bad = replace(cfg, certificate=str(tmp_path / "missing.crt"))
    elif invalid == "directory":
        bad = replace(cfg, private_key=str(tmp_path))
    elif invalid == "malformed-cert":
        f = tmp_path / "broken.crt"; f.write_text("not a certificate")
        bad = replace(cfg, certificate=str(f))
    elif invalid == "malformed-key":
        f = tmp_path / "broken.key"; f.write_text("not a private key")
        f.chmod(0o600)
        bad = replace(cfg, private_key=str(f))
    else:
        f = tmp_path / "other.key"; f.write_text(SYNTHETIC_OTHER_KEY)
        f.chmod(0o600)
        bad = replace(cfg, private_key=str(f))
    expected = "file assente" if invalid in ("absent", "directory") else "formato PEM"
    with pytest.raises(ValueError, match=expected):
        settings.save_betfair_config(bad, password="new-password")
    assert db.get_settings() == before
    settings.save_betfair_config(cfg, password="new-password")
    assert settings.load_betfair_config().certificate == cfg.certificate
    assert settings.load_password() == "new-password"


def test_db_failure_rolls_back_password_and_every_credential(setup, monkeypatch):
    db, settings, cfg = setup
    db.save_settings({"username": "before", "password": "before-password"})
    before = db.get_settings()
    original = db._set_setting
    def fail_password(name, value):
        if name == "password":
            raise sqlite3.OperationalError("synthetic write failure")
        original(name, value)
    monkeypatch.setattr(db, "_set_setting", fail_password)
    with pytest.raises(sqlite3.OperationalError):
        settings.save_betfair_config(cfg, password="replacement")
    assert db.get_settings() == before


class NoNetworkSession:
    def __init__(self):
        self.calls = []
    def post(self, url, **kwargs):
        from types import SimpleNamespace
        self.calls.append((url, kwargs))
        return SimpleNamespace(status_code=200, raise_for_status=lambda: None, json=lambda: {"loginStatus": "SUCCESS", "sessionToken": "demo-session"})
    def close(self):
        pass


def wire_client(settings, monkeypatch):
    import services.betfair_service as module
    from betfair_client import BetfairClient
    http = NoNetworkSession()
    keys = []
    def build(**kwargs):
        keys.append(kwargs["app_key"])
        return BetfairClient(**kwargs, session=http)
    monkeypatch.setattr(module, "BetfairClient", build)
    svc = BetfairService(settings)
    # Scheduler only: real login/validation/client remain in the call path.
    monkeypatch.setattr(svc, "_start_session_keepalive", lambda: None)
    return svc, http, keys


def test_live_rejects_legacy_delayed_before_client_then_recovers(setup, monkeypatch):
    db, settings, cfg = setup
    db.save_credentials(username=cfg.username, app_key="demo-delayed", certificate=cfg.certificate, private_key=cfg.private_key)
    db.save_password("demo-password")
    svc, http, keys = wire_client(settings, monkeypatch)
    with pytest.raises(RuntimeError, match="Live"):
        svc.connect(simulation_mode=False)
    assert keys == [] and http.calls == [] and svc.get_live_client() is None
    cfg = settings.load_betfair_config()
    settings.save_betfair_config(replace(cfg, app_key_live="demo-live"))
    assert svc.connect(simulation_mode=False)["connected"]
    assert keys == ["demo-live"]
    assert http.calls[0][1]["headers"]["X-Application"] == "demo-live"
    assert svc.get_client() is svc.get_live_client()


def test_simulation_stays_offline_without_credentials(setup, monkeypatch):
    _, settings, _ = setup
    svc, http, keys = wire_client(settings, monkeypatch)
    assert svc.connect(simulation_mode=True)["simulated"]
    assert keys == [] and http.calls == []
    assert svc.get_live_client() is None
    assert svc.get_client() is svc.get_simulation_broker()
    svc.disconnect()


def test_keys_are_redacted_by_shared_sanitizers(setup):
    from core.redaction import is_sensitive_key
    from observability.sanitizers import sanitize_dict
    for name in ("app_key_delayed", "app_key_live", "appKeyDelayed", "appKeyLive"):
        assert is_sensitive_key(name)
        assert "demo-sentinel" not in str(sanitize_dict({name: "demo-sentinel"}))


def test_live_key_redacted_from_client_exception_and_diagnostics(setup, caplog):
    from betfair_client import BetfairClient
    from requests.exceptions import ConnectionError
    _, _, cfg = setup
    class FailingTransport(NoNetworkSession):
        def post(self, url, **kwargs):
            raise ConnectionError("synthetic transport echoed demo-live-secret")
    client = BetfairClient(username=cfg.username, app_key="demo-live-secret",
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=FailingTransport())
    with pytest.raises(RuntimeError) as error:
        client.login(password="demo-password")
    assert "demo-live-secret" not in str(error.value)
    assert "demo-live-secret" not in str(client.io_snapshot())
    assert "demo-live-secret" not in caplog.text


def test_app_key_redaction_preserves_overlapping_session_token_redaction(setup):
    from betfair_client import BetfairClient
    _, _, cfg = setup
    client = BetfairClient(username=cfg.username, app_key="demo-live",
                           cert_pem=cfg.certificate, key_pem=cfg.private_key,
                           session=NoNetworkSession())
    client.session_token = "demo-live-session-secret"
    assert client._redact_error_text("demo-live-session-secret / demo-live") == "***SESSION_TOKEN*** / ***APP_KEY***"


def test_registry_requires_live_key_without_requiring_delayed(setup):
    from config_registry import ConfigRegistry
    db, settings, _ = setup
    db.save_settings({"app_key_delayed": "demo-delayed", "app_key_live": ""})
    entries = {e.key: e for e in ConfigRegistry(settings).entries()}
    assert not entries["betfair.app_key_live"].valid
    assert entries["betfair.app_key_live"].required_for_live
    assert not entries["betfair.app_key_delayed"].required_for_live
    assert "demo-delayed" not in str(entries)


@pytest.mark.parametrize("real_widgets", [False, True])
def test_gui_save_reopen_file_pickers_and_live_boundary(setup, monkeypatch, real_widgets):
    """Whole GUI bootstrap, actual SQLite/settings/client; stub dialogs/HTTP only."""
    if real_widgets and not os.environ.get("DISPLAY"):
        pytest.skip("BLOCKED_ENV: Tk real widgets require an X11 display")
    import mini_gui as gui
    db, settings, cfg = setup
    db.save_credentials(username=cfg.username, app_key="demo-delayed", certificate=cfg.certificate, private_key=cfg.private_key)
    db.save_password("demo-saved-password")
    monkeypatch.setattr(gui, "Database", lambda: db)
    shown = []
    monkeypatch.setattr(gui.messagebox, "showinfo", lambda title, text: shown.append((title, text)))
    monkeypatch.setattr(gui.messagebox, "showerror", lambda title, text: shown.append((title, text)))
    if not real_widgets:
        # The unrelated Telegram tab has no headless widgets.
        monkeypatch.setattr(gui, "TelegramTabUI", lambda *args: None)
    app = gui.MiniPickfairGUI(test_mode=not real_widgets, force_simulation=True)
    if not real_widgets:
        monkeypatch.setattr(app, "_safe_show_error", lambda title, text: shown.append((title, text)))
    save = app.btn_save_bf.invoke if real_widgets else app._save_betfair_settings
    try:
        assert app.bf_app_key_delayed_var.get() == "demo-delayed"
        assert app.bf_app_key_live_var.get() == ""
        assert app.bf_password_var.get() == ""
        if real_widgets:
            app.tabs.set("Impostazioni")
            app.update()
            for button in (app.btn_browse_bf_cert, app.btn_browse_bf_key, app.btn_save_bf):
                assert button.winfo_ismapped() and button.winfo_width() > 0
                assert button.winfo_rootx() + button.winfo_width() <= app.winfo_rootx() + app.winfo_width()
            def descendants(widget):
                for child in widget.winfo_children():
                    yield child
                    yield from descendants(child)
            masked = [w for w in descendants(app.tab_settings)
                      if isinstance(w, gui.ctk.CTkEntry) and w.cget("show") == "*"]
            assert len(masked) == 3
        selections = iter(["", cfg.certificate, cfg.private_key])
        dialogs = []
        def select(**kwargs):
            dialogs.append(kwargs)
            return next(selections)
        monkeypatch.setattr(gui.filedialog, "askopenfilename", select)
        app._browse_betfair_file(False)
        assert app.bf_cert_var.get() == cfg.certificate  # cancel preserves path
        if real_widgets:
            app.btn_browse_bf_cert.invoke()
            app.btn_browse_bf_key.invoke()
        else:
            app._browse_betfair_file(False)
            app._browse_betfair_file(True)
        assert len(dialogs) == 3 and all(d["parent"] is app for d in dialogs)
        before = db.get_settings()
        app.bf_cert_var.set(cfg.certificate + ".missing")
        save()
        assert db.get_settings() == before
        assert "file assente" in shown[-1][1]
        app.bf_cert_var.set(cfg.certificate)
        app.bf_app_key_live_var.set("demo-live")
        save()
        assert settings.load_password() == "demo-saved-password"
        assert settings.load_betfair_config().app_key_live == "demo-live"
        app.bf_password_var.set("demo-replacement")
        save()
        assert settings.load_password() == "demo-replacement"
        assert app.bf_password_var.get() == ""
        app.bf_app_key_live_var.set("")
        app._load_initial_settings()
        assert app.bf_app_key_live_var.get() == "demo-live"
        svc, http, keys = wire_client(settings, monkeypatch)
        assert svc.connect(simulation_mode=False)["connected"]
        assert keys == ["demo-live"]
        assert http.calls[0][1]["headers"]["X-Application"] == "demo-live"
        raw_keys = {field: db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"]
                    for field in ("app_key_delayed", "app_key_live")}
        broken_keys = {"app_key_delayed": "enc:v2:unsupported", "app_key_live": "enc:v1:broken"}
        for field, broken in broken_keys.items():
            db._execute("UPDATE settings SET value=? WHERE key=?", (broken, field))
        app._load_initial_settings()
        assert app.bf_app_key_delayed_var.get() == app.bf_app_key_live_var.get() == ""
        save()  # Unrelated GUI save cannot erase the recoverable ciphertext.
        for field, broken in broken_keys.items():
            assert db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"] == broken
            db._execute("UPDATE settings SET value=? WHERE key=?", (raw_keys[field], field))
        save()  # The still-empty form cannot erase keys recovered by another writer.
        for field in broken_keys:
            assert db._execute("SELECT value FROM settings WHERE key=?", (field,), fetchone=True, commit=False)["value"] == raw_keys[field]
        app._load_initial_settings()
        assert app.bf_app_key_live_var.get() == "demo-live"
        screenshot = os.environ.get("PICKFAIR_TEST_SCREENSHOT")
        if real_widgets and screenshot:
            from PIL import ImageGrab
            app.update()
            ImageGrab.grab(bbox=(app.winfo_rootx(), app.winfo_rooty(),
                                app.winfo_rootx() + app.winfo_width(),
                                app.winfo_rooty() + app.winfo_height())).save(screenshot)
    finally:
        app._on_close()
