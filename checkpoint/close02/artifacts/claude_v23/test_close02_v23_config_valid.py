"""close02 v23 — CL25-CFG-VALID-001 e CL24-CFG-CONFLICT-NORM-001 (configurazione Roserpina).

CL25. Fonte L4 VALID (#461/6085510337, verbatim): «Validare dominio del campo prima di qualunque salvataggio/uso;
rifiutare input malformati senza mutare l'ultimo stato valido». RED su v22: NaN, inf, negativi e testo venivano
salvati sopra l'ultimo valore valido, e il loader accettava i NEGATIVI come validi (es. commission_pct=-1 gonfia il
netto del P/L; liquidity_multiplier=-1 spegne la guardia). Dominio usato: solo cio' che e' malformato per ogni
chiave numerica (non numerico, non finito, negativo); nessun intervallo inventato.

CL24. Il rilevatore di conflitti (CL02-RSR-RACE-001) confrontava i valori come testo: un valore persistito come
"5" e ricaricato come 5.0 era scambiato per una modifica concorrente e ogni salvataggio falliva."""
import os

import pytest

from core import validators
from database import Database
from services.setting_service import SettingsService

BASE = {"roserpina.max_single_bet_pct": "5.0", "roserpina.max_total_exposure_pct": "20.0",
        "roserpina.max_event_exposure_pct": "10.0", "roserpina.max_stake_abs": "50.0",
        "roserpina.commission_pct": "4.5", "roserpina.max_recovery_tables": "1", "roserpina.allow_recovery": "0",
        "roserpina.max_open_exposure": "10.0", "roserpina.max_drawdown_abs": "10.0"}


def _svc(tmp_path, extra=None):
    db = Database(str(tmp_path / "c.sqlite"))
    db.save_settings(dict(BASE, **(extra or {})))
    return db, SettingsService(db)


@pytest.mark.parametrize("attr", ["max_single_bet_pct", "commission_pct", "min_stake", "max_stake_abs",
                                  "max_open_exposure", "max_drawdown_abs", "liquidity_multiplier", "table_count",
                                  "max_daily_loss", "min_price", "max_win"])
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), -1.0, "abc"])
def test_salvataggio_malformato_rifiutato_ultimo_stato_intatto(tmp_path, attr, bad):
    db, ss = _svc(tmp_path, {"roserpina.max_daily_loss": "20.0"})
    before = dict(db.get_settings())
    cfg = ss.load_roserpina_config()
    setattr(cfg, attr, bad)
    with pytest.raises(ValueError, match="CONFIG_INVALID"):
        ss.save_roserpina_config(cfg)
    assert dict(db.get_settings()) == before  # ne' valore ne' versione cambiati


@pytest.mark.parametrize("key", ["roserpina.commission_pct", "roserpina.min_stake", "roserpina.max_stake_abs",
                                 "roserpina.liquidity_multiplier", "roserpina.table_count",
                                 "roserpina.max_single_bet_pct", "roserpina.max_daily_loss",
                                 "roserpina.max_open_exposure", "roserpina.max_drawdown_abs"])
def test_negativo_su_disco_e_corrotto_e_blocca(tmp_path, key):
    _db, ss = _svc(tmp_path, {key: "-1"})
    cfg = ss.load_roserpina_config()
    corrotto = key in (cfg.corrupt_keys or ())
    nan_opz = isinstance(getattr(cfg, key.split(".", 1)[1]), float) and getattr(cfg, key.split(".", 1)[1]) != \
        getattr(cfg, key.split(".", 1)[1])
    assert corrotto or nan_opz, (key, cfg.corrupt_keys, getattr(cfg, key.split(".", 1)[1]))
    assert corrotto  # osservabile come corruzione: la corsia d'ingresso blocca con config_corrotta


def test_zero_resta_valido(tmp_path):
    db, ss = _svc(tmp_path)
    cfg = ss.load_roserpina_config()
    cfg.max_recovery_tables = 0
    ss.save_roserpina_config(cfg)
    assert db.get_settings()["roserpina.max_recovery_tables"] == "0"
    assert "roserpina.max_recovery_tables" not in (ss.load_roserpina_config().corrupt_keys or ())


def test_salvataggio_valido_versione_incrementata(tmp_path):
    db, ss = _svc(tmp_path)
    cfg = ss.load_roserpina_config()
    cfg.max_single_bet_pct = 7.5
    ss.save_roserpina_config(cfg)
    s = db.get_settings()
    assert s["roserpina.max_single_bet_pct"] == "7.5" and s["roserpina.config_version"] == "1"


@pytest.mark.parametrize("stored", ["5", "5.00", " 5.0 "])
def test_valore_non_canonico_non_e_un_conflitto(tmp_path, stored):
    db, ss = _svc(tmp_path, {"roserpina.max_single_bet_pct": stored})
    cfg = ss.load_roserpina_config()
    cfg.max_single_bet_pct = 7.5
    ss.save_roserpina_config(cfg)
    assert db.get_settings()["roserpina.max_single_bet_pct"] == "7.5"


def test_conflitto_reale_ancora_rilevato(tmp_path):
    db, ss = _svc(tmp_path)
    cfg = ss.load_roserpina_config()
    db.save_settings({"roserpina.max_single_bet_pct": "6.0"})  # altra scrittura nel frattempo
    cfg.max_single_bet_pct = 7.5
    with pytest.raises(RuntimeError, match="CONFIG_CONFLICT"):
        ss.save_roserpina_config(cfg)
    assert db.get_settings()["roserpina.max_single_bet_pct"] == "6.0"
