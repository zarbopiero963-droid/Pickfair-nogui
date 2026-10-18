"""PF-CAP-10b (rettifica owner 18/10/2026) — ciclo GUI reale sui quattro cap monetari opzionali.

GUI test_mode con SettingsService e Database REALI: campo vuoto salvato => cap disattivato e campo
riallineato; gli hard-stop di perdita conservano "vuoto preserva"; un valore persistito illeggibile
blocca il salvataggio; caricamento iniziale fallito => salvataggio bloccato con errore esplicito,
nessun limite dell'utente sovrascritto (riesame indipendente F1)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_mini_gui_integration import _install_mini_gui_fakes  # noqa: E402

from database import Database  # noqa: E402
from services.settings_service import SettingsService as RealSettingsService  # noqa: E402


def _app(monkeypatch, tmp_path, svc_cls=RealSettingsService, preset=None):
    mini_gui = _install_mini_gui_fakes(monkeypatch, settings_service=svc_cls)
    path = str(tmp_path / "gui.db")
    if preset:
        Database(path).save_settings(preset)
    monkeypatch.setattr(mini_gui, "Database", lambda *a, **k: Database(path))
    errors = []
    app = mini_gui.MiniPickfairGUI(test_mode=True)
    app._safe_show_error = lambda title, msg: errors.append((title, msg))
    app._safe_show_info = lambda *a, **k: None
    return app, path, errors


def _load(path):
    return RealSettingsService(Database(path)).load_roserpina_config()


def test_blank_cap_field_saved_from_the_gui_deactivates_the_persisted_cap(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={
        "roserpina.max_order_exposure": "1.0", "roserpina.max_daily_loss": "50"})
    try:
        assert app.rs_max_order_exposure_var.get() == "1.0"
        app.rs_max_order_exposure_var.set("")
        app.rs_max_daily_loss_var.set("")
        app._save_roserpina_settings()
        assert errors == []
        cfg = _load(path)
        assert cfg.max_order_exposure is None                 # disattivato
        assert cfg.max_daily_loss == 50.0                     # hard-stop di perdita: vuoto preserva
        assert app.rs_max_order_exposure_var.get() == ""
        assert app.rs_max_daily_loss_var.get() == "50.0"
    finally:
        app.destroy()


def test_untouched_cap_survives_a_save_of_another_field(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={"roserpina.max_market_exposure": "2.5"})
    try:
        app.rs_commission_var.set("5.0")
        app._save_roserpina_settings()
        assert errors == []
        assert _load(path).max_market_exposure == 2.5
    finally:
        app.destroy()


def test_unreadable_persisted_cap_blocks_the_save_and_is_not_cleared(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset={"roserpina.max_order_exposure": "dieci"})
    try:
        app._save_roserpina_settings()
        assert errors, "valore illeggibile: il salvataggio deve fallire in modo esplicito"
        assert Database(path).get_settings()["roserpina.max_order_exposure"] == "dieci"
    finally:
        app.destroy()


class _FlakyLoadSettingsService(RealSettingsService):
    calls = 0

    def load_roserpina_config(self):
        type(self).calls += 1
        if type(self).calls == 1:
            raise RuntimeError("database is locked")
        return super().load_roserpina_config()


def test_failed_initial_load_blocks_the_save_and_keeps_every_user_limit(monkeypatch, tmp_path):
    _FlakyLoadSettingsService.calls = 0
    preset = {"roserpina.max_order_exposure": "1.0", "roserpina.max_drawdown_abs": "20",
              "roserpina.max_daily_loss": "50", "roserpina.max_single_bet_pct": "12.0"}
    app, path, errors = _app(monkeypatch, tmp_path, svc_cls=_FlakyLoadSettingsService, preset=preset)
    try:
        assert app.rs_max_order_exposure_var.get() == ""        # caricamento fallito: campi non affidabili
        app._save_roserpina_settings()
        assert errors and "non caricata" in errors[-1][1], errors
        cfg = _load(path)
        assert (cfg.max_order_exposure, cfg.max_drawdown_abs, cfg.max_daily_loss) == (1.0, 20.0, 50.0)
        assert cfg.max_single_bet_pct == 12.0                   # nessun valore iniziale GUI scritto sopra
    finally:
        app.destroy()
