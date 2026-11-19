"""Identita' finanziaria degli stati di rischio (PR-04a, PF-OWNER-PR04A-SCOPE-MM029-GO-20261116).

Decisione owner PF-OWNER-PR04A-LIVE-GLOBAL-SIM-LOCAL-20261116: la perdita giornaliera e gli stati collegati si
calcolano per identita' finanziaria completa ``provider_id + environment_id + account_id + capital_scope``; le
decisioni finanziarie non usano P&L, baseline o saldi di un altro ambito.

- ``provider_id``: oggi solo ``betfair`` (nessun adapter SX Bet in questa PR);
- ``environment_id``: ``SIM`` o ``LIVE``; qualsiasi altro valore non e' verificabile;
- ``account_id``: ``sim`` per la SIM; per il LIVE l'hash dello username Betfair (mai in chiaro), vuoto se ignoto;
- ``capital_scope``: la valuta del capitale (``EUR``); EUR e USDC non si mescolano mai.

Un ambito LIVE senza conto identificato (o con ambiente sconosciuto) non e' verificabile: il suo superamento non
puo' essere isolato e sale all'Emergency Stop globale (fail-closed).
"""
from __future__ import annotations

PROVIDER_BETFAIR = "betfair"
CAPITAL_EUR = "EUR"
ENV_SIM = "SIM"
ENV_LIVE = "LIVE"
_SEP = "|"


def scope_key(provider_id: str, environment_id: str, account_id: str, capital_scope: str) -> str:
    parts = [str(p or "").strip().replace(_SEP, "_") for p in (provider_id, environment_id, account_id,
                                                               capital_scope)]
    return _SEP.join(parts)


def parts(key: str) -> tuple[str, str, str, str]:
    raw = str(key or "").split(_SEP)
    if len(raw) != 4:
        return ("", "", "", "")
    return (raw[0], raw[1], raw[2], raw[3])


def is_verified(key: str) -> bool:
    """Ambito isolabile: provider, ambiente SIM/LIVE, conto e valuta tutti noti."""
    provider, env, account, capital = parts(key)
    return bool(provider and account and capital) and env in (ENV_SIM, ENV_LIVE)


def is_sim(key: str) -> bool:
    """SIM verificata: il suo superamento resta locale (blocca solo i suoi ingressi)."""
    return is_verified(key) and parts(key)[1] == ENV_SIM


def escalates_globally(key: str) -> bool:
    """LIVE (o ambito non verificabile): il superamento giornaliero produce l'Emergency Stop globale."""
    return not is_sim(key)


# Ambito delle perdite di origine non verificabile (riserva legacy senza ambito): mai isolabile => escalation globale.
UNVERIFIED_SCOPE = "betfair|UNVERIFIED||EUR"


def runtime_scope(*, simulated: bool, live_account: str) -> str:
    if simulated:
        return scope_key(PROVIDER_BETFAIR, ENV_SIM, "sim", CAPITAL_EUR)
    return scope_key(PROVIDER_BETFAIR, ENV_LIVE, live_account, CAPITAL_EUR)
