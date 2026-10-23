"""PF-ROSERPINA-41 (mandato CLOSURE-03, punto 4) — test MIRATI per i parametri Roserpina disponibili al HEAD.

Per ogni parametro: (a) persistenza reale (SettingsService + SQLite su disco) e rilettura dopo un «riavvio»
(nuove istanze sullo stesso file); (b) valore non valido: comportamento del loader documentato; (c) attivazione
reale dell'effetto per i parametri del money management che la campagna non aveva messo alla prova.
I parametri che hanno gia' test mirati di attivazione nella suite sono mappati nella scheda (docs).
RSR-38..41 non esistono al HEAD (solo PR #505): BLOCKED, nessun test qui.
"""
import math

import pytest

from core.money_management import RoserpinaMoneyManagement as MoneyManagement
from core.risk_desk import RiskDesk
from core.system_state import DeskMode, RiskProfile, RoserpinaConfig
from tests.risk_config_fixtures import legacy_test_config
from core.table_manager import TableManager
from database import Database
from services.setting_service import SettingsService

# (chiave, valore non di default, default del loader). PFC-008 (M019): le quattro percentuali di rischio non hanno
# default (assenti => None, illeggibili => NaN: tests/unit/test_pfc008_risk_pct_required.py); recovery assente => spento.
PARAMS = [
    ("target_profit_cycle_pct", 4.5, None), ("max_single_bet_pct", 12.0, None), ("max_total_exposure_pct", 40.0, None),
    ("max_event_exposure_pct", 9.0, None), ("auto_reset_drawdown_pct", 11.0, 15.0), ("defense_drawdown_pct", 6.0, 7.5),
    ("lockdown_drawdown_pct", 25.0, 20.0), ("expansion_profit_pct", 4.0, 5.0), ("expansion_multiplier", 1.25, 1.10),
    ("defense_multiplier", 0.6, 0.80), ("table_count", 3, 5), ("max_recovery_tables", 1, 0), ("commission_pct", 4.5, 4.5),
    ("min_stake", 2.0, None), ("max_stake_abs", 50.0, None), ("max_daily_loss", 25.0, None),
    ("max_open_exposure", 80.0, None), ("max_session_loss", 30.0, None), ("max_exposure_stop", 90.0, None),
    ("max_drawdown_hard_stop_pct", 12.0, None), ("book_warning", 104.0, None), ("book_block", 108.0, None),
    ("liquidity_multiplier", 4.0, None), ("min_liquidity_absolute", 75.0, None), ("min_price", 1.3, None),
    ("max_win", 500.0, None), ("max_stake_pct", 20.0, None), ("profit_epsilon", 0.2, None),
    ("auto_green_delay_sec", 4.0, None), ("risk_profile", "AGGRESSIVE", "BALANCED"), ("allow_recovery", True, False),
    ("anti_duplication_enabled", False, True), ("liquidity_guard_enabled", False, None),
    ("liquidity_warning_only", False, None), ("max_win_warning_only", False, True),
    ("auto_green_delay_enabled", True, False), ("profit_epsilon_enabled", False, True),
]


def _svc(path):
    return SettingsService(Database(str(path)))


def _value(cfg, key):
    v = getattr(cfg, key)
    return v.value if isinstance(v, RiskProfile) else v


@pytest.mark.parametrize("key,value,_default", PARAMS, ids=[p[0] for p in PARAMS])
def test_rsr_persisted_and_reloaded_after_restart(tmp_path, key, value, _default):
    db = tmp_path / "rsr.db"
    _svc(db).save_settings({f"roserpina.{key}": value})
    reloaded = _svc(db).load_roserpina_config()                     # nuove istanze = riavvio
    got = _value(reloaded, key)
    assert got == pytest.approx(value) if isinstance(value, float) else got == value, (key, got)


NUMERIC_WITH_DEFAULT = [p for p in PARAMS if isinstance(p[1], float) and p[2] is not None]


@pytest.mark.parametrize("key,_v,default", NUMERIC_WITH_DEFAULT, ids=[p[0] for p in NUMERIC_WITH_DEFAULT])
def test_rsr_unreadable_numeric_value_falls_back_to_code_default(tmp_path, key, _v, default):
    """Comportamento ATTUALE del loader (documentato nella scheda): un valore illeggibile torna al default di codice.
    I range ammessi (Q1/Q2) sono decisioni owner aperte: qui non si inventa una policy."""
    db = tmp_path / "rsr.db"
    _svc(db).save_settings({f"roserpina.{key}": "abc"})
    assert _value(_svc(db).load_roserpina_config(), key) == pytest.approx(default)


HARD_STOPS = ["max_daily_loss", "max_open_exposure", "max_session_loss", "max_exposure_stop",
              "max_drawdown_hard_stop_pct",
              # Rettifica owner 18-19/10/2026: tetti monetari opzionali, stessa semantica degli hard stop.
              "max_stake_abs", "max_win"]


@pytest.mark.parametrize("key", HARD_STOPS)
def test_rsr_hard_stop_unreadable_is_never_silently_disabled(tmp_path, key):
    """Hard stop illeggibile: non deve diventare «non configurato» (None) in silenzio."""
    db = tmp_path / "rsr.db"
    _svc(db).save_settings({f"roserpina.{key}": "abc"})
    v = getattr(_svc(db).load_roserpina_config(), key)
    assert v is not None, f"{key} illeggibile caricato come non configurato (fail-open)"
    assert not (isinstance(v, float) and math.isfinite(v) and v > 0), f"{key} illeggibile caricato come valido: {v}"


def test_rsr_unknown_risk_profile_falls_back_to_balanced(tmp_path):
    db = tmp_path / "rsr.db"
    _svc(db).save_settings({"roserpina.risk_profile": "YOLO"})
    assert _svc(db).load_roserpina_config().risk_profile is RiskProfile.BALANCED


# ------------------------------------------------------------------ attivazione nel money management
SIG = {"price": 3.0, "side": "BACK", "bet_type": "BACK"}


def _mm(**over):
    cfg = legacy_test_config()
    cfg.max_single_bet_pct = cfg.max_total_exposure_pct = cfg.max_event_exposure_pct = 100.0
    cfg.min_stake = 0.01
    for k, v in over.items():
        setattr(cfg, k, v)
    return MoneyManagement(cfg)


def _calc(mm, bankroll=100.0, peak=100.0, **sig):
    return mm.calculate(signal=dict(SIG, **sig), bankroll_current=bankroll, cap_base_cash=bankroll, equity_peak=peak,
                        current_total_exposure=0.0, event_current_exposure=0.0, table=None)


def test_rsr01_target_profit_sets_base_stake():
    # stake = bankroll * target% / (quota - 1): 100 * 4% / 2 = 2.0 (profilo BALANCED, desk NORMAL)
    assert _calc(_mm(target_profit_cycle_pct=4.0)).recommended_stake == pytest.approx(2.0)


@pytest.mark.parametrize("profile,mult", [("CONSERVATIVE", 0.8), ("BALANCED", 1.0), ("AGGRESSIVE", 1.2)])
def test_rsr30_risk_profile_multiplier(profile, mult):
    d = _calc(_mm(target_profit_cycle_pct=4.0, risk_profile=RiskProfile(profile)))
    assert d.recommended_stake == pytest.approx(2.0 * mult)


def test_rsr06_rsr10_defense_mode_scales_stake_by_defense_multiplier():
    mm = _mm(target_profit_cycle_pct=4.0, defense_drawdown_pct=7.5, lockdown_drawdown_pct=20.0, defense_multiplier=0.5)
    d = _calc(mm, bankroll=90.0, peak=100.0)                          # drawdown 10%: DEFENSE
    assert d.desk_mode is DeskMode.DEFENSE
    assert d.recommended_stake == pytest.approx(90.0 * 0.04 / 2.0 * 0.5)
    assert _calc(mm, bankroll=93.0, peak=100.0).desk_mode is DeskMode.NORMAL   # 7% < 7.5%


def test_rsr07_lockdown_drawdown_blocks_entries():
    d = _calc(_mm(lockdown_drawdown_pct=20.0), bankroll=80.0, peak=100.0)
    assert d.approved is False and d.reason == "desk_lockdown"


def test_rsr16_daily_loss_puts_desk_in_lockdown():
    d = _calc(_mm(max_daily_loss=10.0), daily_loss_snapshot=10.0)
    assert d.approved is False and d.desk_mode is DeskMode.LOCKDOWN


def test_rsr08_rsr09_expansion_in_money_management_unit():
    """A livello di money management l'espansione funziona se bankroll > picco passato."""
    mm = _mm(target_profit_cycle_pct=4.0, expansion_profit_pct=5.0, expansion_multiplier=1.25)
    d = _calc(mm, bankroll=110.0, peak=100.0)
    assert d.desk_mode is DeskMode.EXPANSION
    assert d.recommended_stake == pytest.approx(110.0 * 0.04 / 2.0 * 1.25)


def test_rsr08_rsr09_expansion_reachable_through_runtime_cycle_baseline_F032():
    """F032 corretto (decisione owner CONTINUOUS-DELIVERY-04): la crescita si misura sulla baseline persistente del
    ciclo, non sul picco che segue il bankroll. Prove complete in tests/f026/test_f032_rsr12_units.py e F032_* integrazione."""
    desk = RiskDesk()
    desk.sync_bankroll(100.0)
    desk.sync_bankroll(110.0)
    mm = _mm(target_profit_cycle_pct=4.0, expansion_profit_pct=5.0, expansion_multiplier=1.25)
    d = mm.calculate(signal=dict(SIG, expansion_growth_snapshot=10.0), bankroll_current=desk.bankroll_current, cap_base_cash=desk.bankroll_current,
                     equity_peak=desk.equity_peak, current_total_exposure=0.0, event_current_exposure=0.0, table=None)
    assert d.desk_mode is DeskMode.EXPANSION


def test_rsr31_recovery_not_allowed_rejects_recovery_table():
    tm = TableManager(table_count=1)
    tm.activate(table_id=1, event_key="E0", exposure=1.0, market_id="1.0", selection_id=1)
    tm.release(1, pnl=-2.0)
    d = _mm(allow_recovery=False).calculate(signal=dict(SIG), bankroll_current=100.0, cap_base_cash=100.0, equity_peak=100.0,
                                             current_total_exposure=0.0, event_current_exposure=0.0,
                                             table=tm.get_table(1))
    assert d.approved is False and d.reason == "recovery_non_consentito"


def test_rsr31_recovery_stake_includes_table_loss():
    tm = TableManager(table_count=1)
    tm.activate(table_id=1, event_key="E0", exposure=1.0, market_id="1.0", selection_id=1)
    tm.release(1, pnl=-2.0)
    d = _mm(target_profit_cycle_pct=4.0).calculate(signal=dict(SIG), bankroll_current=100.0, cap_base_cash=100.0, equity_peak=100.0,
                                                    current_total_exposure=0.0, event_current_exposure=0.0,
                                                    table=tm.get_table(1))
    assert d.recommended_stake == pytest.approx((4.0 + 2.0) / 2.0)   # (target 4 + perdita 2) / (3 - 1)
