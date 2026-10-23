"""PFC-008 (M019): configurazione di rischio ESPLICITA per le fixture dei test.

Le quattro percentuali obbligatorie non hanno piu' un default di codice (ne' nel loader ne' nella dataclass
`RoserpinaConfig`). Le fixture che vogliono un Money Management operativo le dichiarano qui, in chiaro, con i
valori che il codice usava prima (3/18/35/18): sono una scelta del test, non un default del prodotto.
"""
from __future__ import annotations

from core.system_state import RoserpinaConfig

EXPLICIT_RISK_PCT = {
    "target_profit_cycle_pct": 3.0,
    "max_single_bet_pct": 18.0,
    "max_total_exposure_pct": 35.0,
    "max_event_exposure_pct": 18.0,
}

# Recovery come la usavano le fixture prima di PFC-008 (default di codice True / 2 tavoli), ora dichiarata.
LEGACY_TEST_RECOVERY = {"allow_recovery": True, "max_recovery_tables": 2}

# Stesse percentuali come chiavi persistite (SettingsService / Database).
EXPLICIT_RISK_SETTINGS = {f"roserpina.{k}": v for k, v in EXPLICIT_RISK_PCT.items()}


def with_explicit_risk_pct(cfg: RoserpinaConfig, **over) -> RoserpinaConfig:
    """Imposta su `cfg` le percentuali esplicite (piu' eventuali override) e lo restituisce."""
    for key, value in {**EXPLICIT_RISK_PCT, **over}.items():
        setattr(cfg, key, value)
    return cfg


def explicit_risk_config(**over) -> RoserpinaConfig:
    """`RoserpinaConfig` con le quattro percentuali dichiarate esplicitamente (override ammessi)."""
    return with_explicit_risk_pct(RoserpinaConfig(), **over)


def legacy_test_config(**over) -> RoserpinaConfig:
    """Percentuali e recovery esplicite, con i valori che le fixture ricevevano prima come default."""
    return explicit_risk_config(**{**LEGACY_TEST_RECOVERY, **over})


def seed_verified_cash(risk_desk, available: float, committed: float = 0.0) -> None:
    """Lettura del conto verificata nel RiskDesk (disponibile + impegnato della stessa lettura)."""
    risk_desk.sync_bankroll(available)
    risk_desk.sync_cash_equity(available + committed, available)
