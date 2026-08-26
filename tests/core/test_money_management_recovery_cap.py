"""#320-D1 — Cap anti-martingala assoluto sul recovery-chase.

`RoserpinaMoneyManagement._calculate_base_stake` calcola lo stake come
`(target_profit + chase) / (price - 1)`, dove `chase = max(0, table_loss)` e'
la porzione che INSEGUE la perdita accumulata sul tavolo. Senza limite, una
perdita grande gonfia lo stake successivo in modo martingala-like (chase
illimitato). Questa PR aggiunge un cap ASSOLUTO in euro, `max_recovery_chase_abs`
su `RoserpinaConfig`, OPT-IN e DEFAULT OFF:

- `None` (default) / `<= 0` (finito)      => cap DISARMATO / opt-out esplicito:
  comportamento storico invariato (`chase = max(0, table_loss)`).
- `> 0` (finito)                           => cap ARMATO: `chase` limitato al
  minimo tra la perdita e il tetto € (`chase = min(max(0, table_loss), cap)`).
- non-finito (NaN/inf, config corrotta)    => FAIL-CLOSED: `chase = 0` (massima
  protezione anti-martingala; un cap corrotto NON ripristina il chase illimitato
  — sarebbe fail-open sulla protezione, rilievo GPT-5.6 Sol #450).

Il cap tocca SOLO la componente di recovery-chase del base_stake calcolato;
NON tocca lo stake fisso da segnale (istruzione esplicita del tipster, non un
inseguimento) e NON allenta nessun cap esistente (single/total/event bet).

BLOCK: sul codice pre-fix i test red-first (cap che morde una perdita grande,
sia diretto sia end-to-end via `calculate`) falliscono — lo stake resta al
valore martingala non limitato. Col cap armato passano. I test di guardia
verificano che il cap NON morda sotto-soglia e che i percorsi OFF (default,
zero, negativo, NaN) restino IDENTICI al comportamento storico.
"""

from __future__ import annotations

import math

import pytest

from core.money_management import RoserpinaMoneyManagement
from core.system_state import RoserpinaConfig


# ---------------------------------------------------------------------------
# HELPERS
# ---------------------------------------------------------------------------
def _mm(**overrides) -> RoserpinaMoneyManagement:
    cfg = RoserpinaConfig(
        target_profit_cycle_pct=3.0,
        max_single_bet_pct=25.0,
        max_total_exposure_pct=50.0,
        max_event_exposure_pct=25.0,
        **overrides,
    )
    return RoserpinaMoneyManagement(cfg)


def _base(mm: RoserpinaMoneyManagement, *, loss: float, price: float = 2.0, br: float = 1000.0) -> float:
    # price=2.0 => (price-1)=1.0 => stake == to_recover (aritmetica pulita).
    # br=1000, target_profit_cycle_pct=3.0 => target_profit = 30.0.
    return mm._calculate_base_stake(price=price, bankroll_current=br, table_loss=loss)


def _calc(mm: RoserpinaMoneyManagement, *, loss: float, price: float = 2.0, br: float = 1000.0):
    return mm.calculate(
        signal={"price": price},
        bankroll_current=br,
        equity_peak=br,  # niente drawdown => desk NORMAL (moltiplicatore 1.0)
        current_total_exposure=0.0,
        event_current_exposure=0.0,
        table={"table_id": 1, "loss_amount": loss, "in_recovery": loss > 0.0},
    )


# ---------------------------------------------------------------------------
# BLOCK (red-first): il cap armato DEVE limitare il chase
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_bounds_large_loss_direct():
    """Cap armato a 20€: perdita 100 => chase limitato a 20, to_recover=50,
    stake=50. Pre-fix (chase illimitato) darebbe 130 => questo test FALLISCE
    sul vecchio codice (BLOCK del bug martingala)."""
    mm = _mm(max_recovery_chase_abs=20.0)
    # target_profit=30 ; chase=min(100,20)=20 ; to_recover=50 ; /1.0 => 50
    assert _base(mm, loss=100.0) == pytest.approx(50.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_flows_through_calculate():
    """End-to-end: lo stake capato deve arrivare in `recommended_stake`
    (non solo nel metodo privato). Cap 20€, perdita 100, bankroll 1000,
    price 2.0 => 50 (i cap single/total/event a 250/500/250 non mordono).
    Pre-fix darebbe 130 => BLOCK end-to-end del wiring."""
    mm = _mm(max_recovery_chase_abs=20.0)
    decision = _calc(mm, loss=100.0)
    assert decision.approved is True
    assert decision.recommended_stake == pytest.approx(50.0)


# ---------------------------------------------------------------------------
# GUARD: il cap NON deve mordere sotto-soglia
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_does_not_bite_small_loss():
    """Perdita (5) sotto il cap (20): chase invariato => to_recover=35,
    stake=35. Il cap non deve ridurre chi e' gia' sotto soglia."""
    mm = _mm(max_recovery_chase_abs=20.0)
    assert _base(mm, loss=5.0) == pytest.approx(35.0)


# ---------------------------------------------------------------------------
# GUARD: percorsi OFF => comportamento storico IDENTICO (chase illimitato)
# ---------------------------------------------------------------------------
@pytest.mark.core
@pytest.mark.safety
def test_recovery_uncapped_by_default():
    """Default (`max_recovery_chase_abs` assente/None): cap DISARMATO =>
    to_recover=130, stake=130 (comportamento storico). Guardia contro un
    default accidentalmente armato."""
    mm = _mm()
    assert getattr(mm.config, "max_recovery_chase_abs", None) is None
    assert _base(mm, loss=100.0) == pytest.approx(130.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_zero_is_off():
    """Cap == 0 => DISARMATO (non azzera il chase): storico 130."""
    mm = _mm(max_recovery_chase_abs=0.0)
    assert _base(mm, loss=100.0) == pytest.approx(130.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_negative_is_off():
    """Cap < 0 => DISARMATO: storico 130."""
    mm = _mm(max_recovery_chase_abs=-5.0)
    assert _base(mm, loss=100.0) == pytest.approx(130.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_nan_is_failclosed():
    """Cap ARMATO ma NON-finito (NaN da config corrotta) => FAIL-CLOSED:
    massima protezione anti-martingala, `chase = 0` (NON si ripristina il
    chase illimitato su config corrotta — sarebbe fail-open sulla protezione
    money-management, rilievo GPT-5.6 Sol #450). to_recover = target_profit
    (30) + 0 => stake 30. Distinto da None/<=0 (opt-out esplicito => storico).
    Nessun blocco dell'ordine: stake resta positivo e finito (no DoS)."""
    mm = _mm(max_recovery_chase_abs=float("nan"))
    result = _base(mm, loss=100.0)
    assert math.isfinite(result)
    assert result == pytest.approx(30.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_chase_cap_inf_is_failclosed():
    """Cap ARMATO ma infinito (config assurda) => FAIL-CLOSED come il NaN:
    `chase = 0` (un cap non-finito non e' un 'nessun limite', e' corrotto).
    stake = target_profit (30)."""
    mm = _mm(max_recovery_chase_abs=float("inf"))
    result = _base(mm, loss=100.0)
    assert math.isfinite(result)
    assert result == pytest.approx(30.0)


@pytest.mark.core
@pytest.mark.safety
def test_recovery_cap_off_matches_historical_through_calculate():
    """End-to-end di controllo: col cap OFF (default) la decisione resta
    storica (approvata, stake 130)."""
    mm = _mm()
    decision = _calc(mm, loss=100.0)
    assert decision.approved is True
    assert decision.recommended_stake == pytest.approx(130.0)


# ---------------------------------------------------------------------------
# PERSISTENZA (SettingsService reale + DB in-memory): load/save del cap.
# Copre i rami aggiunti in services/setting_service.py.
# ---------------------------------------------------------------------------
class _InMemoryDB:
    """DB minimale compatibile con SettingsService (get/save settings, merge)."""

    def __init__(self, initial=None):
        self._settings = dict(initial or {})

    def get_settings(self):
        return dict(self._settings)

    def save_settings(self, payload):
        self._settings.update(dict(payload or {}))


@pytest.mark.core
@pytest.mark.safety
def test_persist_recovery_cap_absent_loads_none():
    from services.setting_service import SettingsService

    cfg = SettingsService(_InMemoryDB()).load_roserpina_config()
    assert cfg.max_recovery_chase_abs is None  # default OFF


@pytest.mark.core
@pytest.mark.safety
def test_persist_recovery_cap_value_round_trip():
    from services.setting_service import SettingsService

    db = _InMemoryDB({"roserpina.max_recovery_chase_abs": 20.0})
    cfg = SettingsService(db).load_roserpina_config()
    assert cfg.max_recovery_chase_abs == pytest.approx(20.0)

    # Save di un nuovo valore lo scrive nel DB e ricarica identico.
    cfg.max_recovery_chase_abs = 15.0
    SettingsService(db).save_roserpina_config(cfg)
    assert db.get_settings()["roserpina.max_recovery_chase_abs"] == pytest.approx(15.0)
    assert SettingsService(db).load_roserpina_config().max_recovery_chase_abs == pytest.approx(15.0)


@pytest.mark.core
@pytest.mark.safety
def test_persist_recovery_cap_none_preserves_stored():
    """Semantica preserve (come gli altri hard-stop opzionali): un save con
    campo None NON azzera un cap gia' persistito."""
    from services.setting_service import SettingsService

    db = _InMemoryDB({"roserpina.max_recovery_chase_abs": 42.0})
    cfg = SettingsService(db).load_roserpina_config()
    assert cfg.max_recovery_chase_abs == pytest.approx(42.0)

    cfg.max_recovery_chase_abs = None  # campo "vuoto" dalla GUI/console
    SettingsService(db).save_roserpina_config(cfg)
    # PRESERVATO, non azzerato.
    assert SettingsService(db).load_roserpina_config().max_recovery_chase_abs == pytest.approx(42.0)
