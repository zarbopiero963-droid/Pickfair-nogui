"""PFC-008 (M019, "+F10 loader/GUI") — ciclo GUI reale sulle percentuali di rischio obbligatorie.

GUI test_mode con SettingsService e Database REALI: una chiave mai salvata si mostra vuota (nessun valore di
codice) e la configurazione guidata elenca i campi mancanti e porta alla tab Roserpina, una volta sola; il
salvataggio con un campo vuoto o fuori da (0, 100] viene annullato con errore esplicito senza scrivere nulla;
un salvataggio valido viene persistito, riletto al riavvio e non fa piu' comparire alcuna richiesta."""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_mini_gui_optional_caps_save import _app, _load  # noqa: E402

PCT_VARS = {
    "rs_target_var": "target_profit_cycle_pct",
    "rs_max_single_var": "max_single_bet_pct",
    "rs_max_total_var": "max_total_exposure_pct",
    "rs_max_event_var": "max_event_exposure_pct",
}
VALIDI = {"roserpina.target_profit_cycle_pct": "3", "roserpina.max_single_bet_pct": "18",
          "roserpina.max_total_exposure_pct": "35", "roserpina.max_event_exposure_pct": "18"}


LABELS = ["Target Profit Cycle %", "Max Single Bet %", "Max Total Exposure %", "Max Event Exposure %"]


def test_prima_installazione_campi_vuoti_avviso_e_apertura_guidata(monkeypatch, tmp_path):
    app, _path, _errors = _app(monkeypatch, tmp_path)
    try:
        assert {v: getattr(app, v).get() for v in PCT_VARS} == dict.fromkeys(PCT_VARS, "")
        assert app.rs_allow_recovery_var.get() is False
        assert app._risk_pct_missing == LABELS
        assert all(label in app.rs_pct_notice_var.get() for label in LABELS)
        assert "Nuovi ingressi bloccati" in app.rs_pct_notice_var.get()
        assert app._roserpina_setup_opened is True
    finally:
        app.destroy()


def test_configurazione_parziale_elenca_solo_i_mancanti(monkeypatch, tmp_path):
    app, _path, _errors = _app(monkeypatch, tmp_path, preset={
        "roserpina.target_profit_cycle_pct": "3", "roserpina.max_single_bet_pct": "18",
        "roserpina.max_total_exposure_pct": "nan"})
    try:
        assert app._risk_pct_missing == ["Max Total Exposure %", "Max Event Exposure %"]
        assert "Target Profit Cycle %" not in app.rs_pct_notice_var.get()
        assert app.rs_target_var.get() == "3.0" and app.rs_max_event_var.get() == ""
        assert app._roserpina_setup_opened is True
    finally:
        app.destroy()


def test_configurazione_valida_nessuna_richiesta(monkeypatch, tmp_path):
    app, _path, _errors = _app(monkeypatch, tmp_path, preset=dict(VALIDI))
    try:
        assert app._risk_pct_missing == [] and app.rs_pct_notice_var.get() == ""
        assert app._roserpina_setup_opened is False
    finally:
        app.destroy()


def test_salvataggio_valido_poi_riavvio_senza_nuova_richiesta(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path)
    try:
        for var, valore in zip(PCT_VARS, ("4", "12", "40", "9")):
            getattr(app, var).set(valore)
        app._save_roserpina_settings()
        assert errors == []
        assert app._risk_pct_missing == [] and app.rs_pct_notice_var.get() == ""
    finally:
        app.destroy()
    riavvio, _path2, _errors2 = _app(monkeypatch, tmp_path)          # stesso database: riavvio
    try:
        assert [getattr(riavvio, v).get() for v in PCT_VARS] == ["4.0", "12.0", "40.0", "9.0"]
        assert riavvio._risk_pct_missing == [] and riavvio._roserpina_setup_opened is False
    finally:
        riavvio.destroy()


@pytest.mark.parametrize("valore,messaggio", [
    ("", "obbligatorio"), ("abc", "non numerico"), ("nan", "maggiore di 0"), ("inf", "maggiore di 0"),
    ("0", "maggiore di 0"), ("-5", "maggiore di 0"), ("100.5", "al massimo 100"),
])
def test_salvataggio_annullato_su_percentuale_non_valida(monkeypatch, tmp_path, valore, messaggio):
    app, path, errors = _app(monkeypatch, tmp_path, preset=dict(VALIDI))
    try:
        app.rs_max_total_var.set(valore)
        app._save_roserpina_settings()
        assert len(errors) == 1 and "Max Total Exposure %" in errors[0][1] and messaggio in errors[0][1]
        assert _load(path).max_total_exposure_pct == 35.0          # nulla sovrascritto
    finally:
        app.destroy()


def test_salvataggio_valido_con_virgola(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path, preset=dict(VALIDI))
    try:
        app.rs_max_total_var.set("40,5")
        app._save_roserpina_settings()
        assert errors == []
        assert _load(path).max_total_exposure_pct == 40.5
    finally:
        app.destroy()


def test_db_nuovo_compilato_dall_utente_poi_salvato(monkeypatch, tmp_path):
    app, path, errors = _app(monkeypatch, tmp_path)
    try:
        for var, valore in zip(PCT_VARS, ("4", "12", "40", "9")):
            getattr(app, var).set(valore)
        app._save_roserpina_settings()
        assert errors == []
        cfg = _load(path)
        assert [getattr(cfg, k) for k in PCT_VARS.values()] == [4.0, 12.0, 40.0, 9.0]
        assert cfg.allow_recovery is False
    finally:
        app.destroy()
