"""
G5 PR 1/5 — batch-delete costanti morte di trading_config.

Test guard: asserisce l'ASSENZA delle 4 costanti rimosse (duplicati morti /
inerti, lette da nessun modulo di esecuzione). Falliscono sul vecchio codice
(dove le costanti esistevano) e bloccano una re-introduzione zombie.

Motivazioni (evidenza file:riga nella mappatura Phase 0):
- DEFAULT_COMMISSION      -> commissione reale = commission_pct (config/DB) +
                             BETFAIR_ITALY_COMMISSION_PCT enforced.
- SIM_INITIAL_BALANCE     -> bilancio sim reale = sim_cfg['starting_balance']
                             (default 1000.0), non 10000.0.
- SESSION_TIMEOUT_MIN     -> timeout gestito da keepAlive + SESSION_EXPIRED in
                             betfair_client, non da questa costante.
- MAX_SPREAD_TICKS        -> gate spread gia' esistente a ratio in
                             core/safety_layer.validate_selection_prices.
"""

import trading_config
import config_registry


REMOVED_CONSTANTS = (
    "DEFAULT_COMMISSION",
    "SIM_INITIAL_BALANCE",
    "SESSION_TIMEOUT_MIN",
    "MAX_SPREAD_TICKS",
)


def test_dead_constants_are_absent():
    """Le 4 costanti morte non devono piu' esistere in trading_config."""
    for name in REMOVED_CONSTANTS:
        assert not hasattr(trading_config, name), (
            f"trading_config.{name} e' un duplicato morto: non deve essere "
            f"re-introdotto (usa la fonte reale documentata)."
        )


def test_enforce_candidates_are_preserved():
    """Le costanti da enforce-first nelle PR G5 successive restano presenti."""
    for name in ("MAX_WIN", "MAX_STAKE_PCT", "AUTO_GREEN_DELAY_SEC", "PROFIT_EPSILON"):
        assert hasattr(trading_config, name), (
            f"trading_config.{name} e' un enforce-candidate: non va rimosso qui."
        )


def test_commission_invariant_source_intact():
    """La fonte reale della commissione resta intatta dopo la rimozione."""
    assert hasattr(trading_config, "BETFAIR_ITALY_COMMISSION_PCT")
    assert hasattr(trading_config, "enforce_betfair_italy_commission_pct")
    assert trading_config.BETFAIR_ITALY_COMMISSION_PCT == 4.5


def test_config_registry_has_no_spread_ticks_entry():
    """Il display registry non deve piu' referenziare MAX_SPREAD_TICKS.

    Evita un getattr(trading_config, 'MAX_SPREAD_TICKS') su un nome rimosso
    (che tornerebbe None -> voce 'Spread massimo (tick)' morta nella console).
    """
    import inspect

    src = inspect.getsource(config_registry.ConfigRegistry._trading_entries)
    assert "MAX_SPREAD_TICKS" not in src, (
        "config_registry._trading_entries non deve piu' elencare MAX_SPREAD_TICKS."
    )
    # Le voci ancora enforce-candidate restano mostrate.
    assert "MAX_WIN" in src
    assert "MAX_STAKE_PCT" in src
