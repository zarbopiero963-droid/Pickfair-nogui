import os, sys
import pytest
WT = "/tmp/claude-0/-home-user/72973979-7da3-59f6-82b8-954d718e6349/scratchpad/wt_cap505"
sys.path.insert(0, os.path.join(WT, "tests", "integration"))
from test_mini_gui_integration import _install_mini_gui_fakes  # noqa
from database import Database
from services.settings_service import SettingsService as RealSS


def _app(monkeypatch, tmp_path, svc_cls=RealSS, preset=None):
    mini_gui = _install_mini_gui_fakes(monkeypatch, settings_service=svc_cls)
    path = str(tmp_path / "gui.db")
    if preset:
        Database(path).save_settings(preset)
    monkeypatch.setattr(mini_gui, "Database", lambda *a, **k: Database(path))
    errors = []
    app = mini_gui.MiniPickfairGUI(test_mode=True)
    app._safe_show_error = lambda t, m: errors.append((t, m))
    app._safe_show_info = lambda *a: None
    return app, path, errors


def test_full_gui_cycle_blank_deactivates_cap(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={
        "roserpina.max_order_exposure": "1.0", "roserpina.max_daily_loss": "50"})
    try:
        assert app.rs_max_order_exposure_var.get() == "1.0"
        app.rs_max_order_exposure_var.set("")
        app.rs_max_daily_loss_var.set("")
        app._save_roserpina_settings()
        assert errors == []
        cfg = RealSS(Database(path)).load_roserpina_config()
        assert cfg.max_order_exposure is None
        assert cfg.max_daily_loss == 50.0          # preserve kept
        assert app.rs_max_order_exposure_var.get() == ""   # refreshed
        assert app.rs_max_daily_loss_var.get() == "50.0"
    finally:
        app.destroy()


def test_untouched_cap_survives_save_of_other_field(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={"roserpina.max_market_exposure": "2.5"})
    try:
        app.rs_commission_var.set("5.0")
        app._save_roserpina_settings()
        assert errors == []
        assert RealSS(Database(path)).load_roserpina_config().max_market_exposure == 2.5
    finally:
        app.destroy()


def test_invalid_persisted_cap_blocks_save_not_silently_cleared(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={"roserpina.max_order_exposure": "dieci"})
    try:
        assert app.rs_max_order_exposure_var.get() == "nan"
        app._save_roserpina_settings()
        assert errors, "save must fail-closed"
        assert Database(path).get_settings()["roserpina.max_order_exposure"] == "dieci"
    finally:
        app.destroy()


class FlakyLoadSS(RealSS):
    calls = 0
    def load_roserpina_config(self):
        FlakyLoadSS.calls += 1
        if FlakyLoadSS.calls == 1:
            raise RuntimeError("database is locked")
        return super().load_roserpina_config()


def test_gui_initial_load_failure_then_save_wipes_caps(monkeypatch, tmp_path):
    FlakyLoadSS.calls = 0
    app, path, errors = _app(monkeypatch, tmp_path, svc_cls=FlakyLoadSS, preset={
        "roserpina.max_order_exposure": "1.0", "roserpina.max_drawdown_abs": "20",
        "roserpina.max_daily_loss": "50"})
    try:
        assert app.rs_max_order_exposure_var.get() == ""  # load swallowed
        app._save_roserpina_settings()                     # user saves (any edit)
        cfg = RealSS(Database(path)).load_roserpina_config()
        print("errors", errors, "order", cfg.max_order_exposure, "dd", cfg.max_drawdown_abs, "daily", cfg.max_daily_loss)
        # documents the regression: before 3369482 the caps survived (skip-key)
        assert cfg.max_order_exposure is None and cfg.max_drawdown_abs is None
        assert cfg.max_daily_loss == 50.0
    finally:
        app.destroy()
