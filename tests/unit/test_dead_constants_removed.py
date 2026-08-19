"""
G5 PR 1/5 — batch-delete costanti morte di trading_config.

Test guard su due livelli:

1. ASSENZA dei nomi rimossi in trading_config (falliscono sul vecchio codice
   dove esistevano => BLOCK anti-reintroduzione zombie).
2. PROVA GLOBALE (richiesta dai reviewer GPT-5.6 Sol / Fugu Ultra / Fable 5):
   nessun modulo .py del repo referenzia le costanti rimosse, cosi' un
   `from trading_config import <NAME>` residuo in un modulo non coperto dai
   test non puo' sfuggire e causare ImportError a runtime.

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

import pathlib
import re

import trading_config
from config_registry import ConfigRegistry


REMOVED_CONSTANTS = (
    "DEFAULT_COMMISSION",
    "SIM_INITIAL_BALANCE",
    "SESSION_TIMEOUT_MIN",
    "MAX_SPREAD_TICKS",
)

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SKIP_DIRS = {
    ".git",
    "build",
    "dist",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    ".eggs",
    ".mypy_cache",
    ".pytest_cache",
}
# Whole-word matcher per ciascun nome rimosso (evita substring accidentali).
_REFERENCE_RE = re.compile(r"\b(" + "|".join(REMOVED_CONSTANTS) + r")\b")


def test_dead_constants_are_absent():
    """Le 4 costanti morte non devono piu' esistere in trading_config."""
    present = [name for name in REMOVED_CONSTANTS if hasattr(trading_config, name)]
    assert not present, (
        f"Costanti morte ancora presenti in trading_config: {present}. "
        f"Sono duplicati morti: non re-introdurle (usa la fonte reale documentata)."
    )


def test_enforce_candidates_are_preserved():
    """Le costanti da enforce-first nelle PR G5 successive restano presenti."""
    missing = [
        name
        for name in ("MAX_WIN", "MAX_STAKE_PCT", "AUTO_GREEN_DELAY_SEC", "PROFIT_EPSILON")
        if not hasattr(trading_config, name)
    ]
    assert not missing, f"Enforce-candidate rimossi per errore: {missing}"


def test_commission_invariant_source_intact():
    """La fonte reale della commissione resta intatta dopo la rimozione."""
    assert hasattr(trading_config, "BETFAIR_ITALY_COMMISSION_PCT")
    assert hasattr(trading_config, "enforce_betfair_italy_commission_pct")
    assert trading_config.BETFAIR_ITALY_COMMISSION_PCT == 4.5


def test_registry_trading_entries_have_no_spread_ticks():
    """Behavior-level: il registry non espone piu' l'entry MAX_SPREAD_TICKS.

    Costruisce il ConfigRegistry reale e ispeziona le ConfigEntry restituite
    (non il sorgente): robusto a formattazione/commenti e ai pyc-only, come
    chiesto da Codacy/Greptile.
    """
    reg = ConfigRegistry(settings_service=None)
    keys = {entry.key for entry in reg._trading_entries()}
    assert "trading.MAX_SPREAD_TICKS" not in keys, (
        "Il registry non deve piu' produrre l'entry 'trading.MAX_SPREAD_TICKS'."
    )
    # Le entry ancora enforce-candidate restano esposte.
    assert "trading.MAX_WIN" in keys
    assert "trading.MAX_STAKE_PCT" in keys


def test_removed_constants_not_referenced_anywhere():
    """Prova globale: nessun modulo .py del repo referenzia i nomi rimossi.

    Encoda in CI il grep di Phase 0 (nessun consumer runtime) cosi' un import
    residuo non coperto dai test verrebbe intercettato qui. Questo file guard e'
    escluso (contiene i nomi di proposito).
    """
    self_path = pathlib.Path(__file__).resolve()
    offenders: dict[str, list[str]] = {}
    for py in _REPO_ROOT.rglob("*.py"):
        if py.resolve() == self_path:
            continue
        if _SKIP_DIRS & set(py.relative_to(_REPO_ROOT).parts):
            continue
        text = py.read_text(encoding="utf-8", errors="ignore")
        hits = sorted(set(_REFERENCE_RE.findall(text)))
        if hits:
            offenders[str(py.relative_to(_REPO_ROOT))] = hits
    assert not offenders, (
        f"Costanti rimosse ancora referenziate (rischio ImportError runtime): {offenders}"
    )
