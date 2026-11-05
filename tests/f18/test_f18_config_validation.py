"""F-18 — un valore di configurazione illeggibile o fuori dominio non diventa mai un default silenzioso.

Contratto (scheda F-18, #586):
- al SALVATAGGIO: valore non finito, non numerico, bool al posto di un numero o fuori dominio => salvataggio rifiutato
  per intero, configurazione precedente intatta;
- al CARICAMENTO (DB legacy o corrotto): valore presente ma invalido => mai il default di codice in silenzio; la chiave
  e' registrata in ``RoserpinaConfig.invalid_settings`` e ogni nuova entrata e' negata
  (``configurazione_non_valida:<chiave>``), senza crash;
- assente/None/"" => default di codice per i parametri che ne hanno uno, None = non attivo per i cap opzionali:
  nessun cap monetario attivo di default (decisione owner 18/10/2026);
- microstake: 0,10 EUR e' uno stake minimo valido (decisione owner 02/11/2026), mai alzato.
"""
from __future__ import annotations

import math

import pytest

from core import validators
from core.money_management import RoserpinaMoneyManagement
from database import Database
from services.setting_service import InvalidSettingsError, SettingsService

pytestmark = [pytest.mark.unit, pytest.mark.safety]

REQUIRED = {"roserpina.target_profit_cycle_pct": 3.0, "roserpina.max_single_bet_pct": 18.0,
            "roserpina.max_total_exposure_pct": 35.0, "roserpina.max_event_exposure_pct": 18.0}


@pytest.fixture()
def svc(tmp_path):
    return SettingsService(Database(str(tmp_path / "f18.db")))


def _raw_write(svc, data):
    """Scrittura diretta nel DB, senza validazione: simula un DB legacy o corrotto."""
    svc.db.save_settings(data)


# ── salvataggio ──────────────────────────────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("key,bad", [
    ("roserpina.commission_pct", "nan"), ("roserpina.commission_pct", float("inf")), ("roserpina.commission_pct", -5),
    ("roserpina.commission_pct", 150), ("roserpina.commission_pct", "abc"), ("roserpina.commission_pct", True),
    ("roserpina.max_single_bet_pct", 150), ("roserpina.max_single_bet_pct", -5), ("roserpina.max_single_bet_pct", 0),
    ("roserpina.expansion_multiplier", float("nan")), ("roserpina.defense_multiplier", -1),
    ("roserpina.min_stake", 0), ("roserpina.min_stake", -0.1), ("roserpina.min_price", 1.0),
    ("roserpina.table_count", 0), ("roserpina.table_count", 2.5), ("roserpina.table_count", "abc"),
    ("roserpina.max_recovery_tables", -1), ("roserpina.max_daily_loss", -5), ("roserpina.max_daily_loss", "inf"),
    ("roserpina.max_stake_abs", float("inf")), ("roserpina.max_win", "abc"), ("roserpina.profit_epsilon", -1),
    ("roserpina.auto_green_delay_sec", float("nan")), ("roserpina.anti_duplication_enabled", "maybe"),
    ("roserpina.liquidity_guard_enabled", 2), ("roserpina.risk_profile", "WILD"),
])
def test_invalid_value_is_rejected_at_save_and_previous_config_stays_intact(svc, key, bad):
    svc.save_settings({**REQUIRED, "roserpina.commission_pct": 4.5, "roserpina.table_count": 5})
    before = svc.get_all_settings()
    with pytest.raises(InvalidSettingsError) as exc:
        svc.save_settings({**REQUIRED, key: bad})
    assert key in str(exc.value)
    assert svc.get_all_settings() == before, "salvataggio parziale con un valore invalido"


@pytest.mark.parametrize("key,good", [
    ("roserpina.min_stake", 0.10),                       # microstake: riferimento owner, mai alzato
    ("roserpina.min_stake", "0,10"),                     # virgola decimale
    ("roserpina.commission_pct", 4.5), ("roserpina.commission_pct", 0),
    ("roserpina.max_daily_loss", None), ("roserpina.max_daily_loss", ""),   # None/"" = non attivo
    ("roserpina.max_stake_abs", ""), ("roserpina.max_win", None),
    ("roserpina.max_recovery_chase_abs", 0),            # <= 0 finito = opt-out documentato del chase
    ("roserpina.table_count", 1), ("roserpina.max_recovery_tables", 0),
    ("roserpina.anti_duplication_enabled", True), ("roserpina.anti_duplication_enabled", "0"),
    ("roserpina.book_warning", 105.0), ("roserpina.book_block", 110.0), ("roserpina.min_price", 1.01),
])
def test_valid_value_is_saved(svc, key, good):
    svc.save_settings({**REQUIRED, key: good})
    assert key in svc.get_all_settings()


def test_microstake_0_10_reaches_the_loaded_config_unchanged(svc):
    svc.save_settings({**REQUIRED, "roserpina.min_stake": 0.10})
    cfg = svc.load_roserpina_config()
    assert cfg.min_stake == pytest.approx(0.10) and not cfg.invalid_settings


def test_incoherent_book_thresholds_are_rejected(svc):
    with pytest.raises(InvalidSettingsError, match="book_block"):
        svc.save_settings({**REQUIRED, "roserpina.book_warning": 110.0, "roserpina.book_block": 105.0})


def test_round_trip_of_a_valid_config_object_is_still_accepted(svc):
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    svc.save_roserpina_config(cfg)                       # il salvataggio della tab Roserpina non deve rompersi
    again = svc.load_roserpina_config()
    assert again.commission_pct == cfg.commission_pct and not again.invalid_settings


def test_no_monetary_cap_is_active_by_default(svc):
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    for name in ("max_daily_loss", "max_open_exposure", "max_order_exposure", "max_market_exposure",
                 "max_event_exposure_abs", "max_drawdown_abs", "max_session_loss", "max_exposure_stop",
                 "max_recovery_chase_abs", "max_stake_abs", "max_win"):
        assert getattr(cfg, name) is None, f"{name}: cap monetario attivo di default"


# ── caricamento di un DB legacy/corrotto ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("key,bad,attr", [
    ("roserpina.commission_pct", "abc", "commission_pct"),
    ("roserpina.commission_pct", "inf", "commission_pct"),
    ("roserpina.expansion_multiplier", "nan", "expansion_multiplier"),
    ("roserpina.min_stake", "-1", "min_stake"),
    ("roserpina.profit_epsilon", "abc", "profit_epsilon"),
    ("roserpina.max_stake_pct", "150", "max_stake_pct"),
    ("roserpina.auto_green_delay_sec", "inf", "auto_green_delay_sec"),
])
def test_corrupt_float_is_never_a_silent_default(svc, key, bad, attr):
    _raw_write(svc, {**REQUIRED, key: bad})
    cfg = svc.load_roserpina_config()
    got = getattr(cfg, attr)
    assert isinstance(got, float) and math.isnan(got), f"{key}={bad!r} caricato come {got!r}"
    assert key in cfg.invalid_settings


@pytest.mark.parametrize("key,bad", [("roserpina.table_count", "abc"), ("roserpina.table_count", "0"),
                                     ("roserpina.max_recovery_tables", "2.5"),
                                     ("roserpina.anti_duplication_enabled", "maybe"),
                                     ("roserpina.liquidity_guard_enabled", "2"),
                                     ("roserpina.risk_profile", "WILD")])
def test_corrupt_int_bool_or_enum_is_registered_invalid(svc, key, bad):
    _raw_write(svc, {**REQUIRED, key: bad})
    cfg = svc.load_roserpina_config()
    assert key in cfg.invalid_settings


@pytest.mark.parametrize("key,safe_attr,safe", [("roserpina.anti_duplication_enabled", "anti_duplication_enabled", True),
                                                ("roserpina.liquidity_guard_enabled", "liquidity_guard_enabled", True),
                                                ("roserpina.allow_recovery", "allow_recovery", False),
                                                ("roserpina.liquidity_warning_only", "liquidity_warning_only", False)])
def test_corrupt_safety_bool_loads_its_protective_value(svc, key, safe_attr, safe):
    _raw_write(svc, {**REQUIRED, key: "maybe"})
    assert getattr(svc.load_roserpina_config(), safe_attr) is safe


def test_invalid_config_blocks_every_new_entry_with_an_explicit_reason(svc):
    _raw_write(svc, {**REQUIRED, "roserpina.commission_pct": "abc"})
    cfg = svc.load_roserpina_config()
    assert validators.risk_pcts_reason(cfg) == "configurazione_non_valida:roserpina.commission_pct"
    decision = RoserpinaMoneyManagement(cfg).calculate(
        signal={"price": 2.0, "side": "BACK", "selection_id": 1, "market_id": "1.1"},
        bankroll_current=100.0, equity_peak=100.0, current_total_exposure=0.0, event_current_exposure=0.0,
        table=None) if hasattr(RoserpinaMoneyManagement(cfg), "calculate") else None
    if decision is not None:
        assert decision.approved is False and decision.reason.startswith("configurazione_non_valida")


def test_valid_config_has_no_invalid_settings_and_passes_the_gate(svc):
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    assert cfg.invalid_settings == () and validators.risk_pcts_reason(cfg) == ""


def test_missing_keys_keep_code_defaults_and_are_not_invalid(svc):
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    assert cfg.commission_pct == 4.5 and cfg.table_count == 5 and cfg.anti_duplication_enabled is True


def test_loading_a_corrupt_db_never_crashes(svc):
    _raw_write(svc, {k: "garbage" for k in ("roserpina.commission_pct", "roserpina.table_count",
                                            "roserpina.min_price", "roserpina.risk_profile",
                                            "roserpina.book_block", "roserpina.allow_recovery")})
    cfg = svc.load_roserpina_config()
    assert len(cfg.invalid_settings) >= 6


def test_gui_save_path_save_roserpina_config_is_validated_too(svc):
    # la tab Roserpina della GUI salva con save_roserpina_config: anche quel percorso non deve scrivere valori invalidi
    svc.save_settings(REQUIRED)
    cfg = svc.load_roserpina_config()
    before = svc.get_all_settings()
    cfg.commission_pct = -5.0
    with pytest.raises(InvalidSettingsError, match="commission_pct"):
        svc.save_roserpina_config(cfg)
    assert svc.get_all_settings() == before
