"""Limiti di perdita owner del runtime (#461 PR28-b, PKG-P24-B).

F4: la perdita realizzata del giorno sopravvive al riavvio. Si salva la
perdita INTRADAY (non `realized_pnl`, che riparte da 0 a ogni avvio); stesso
giorno UTC => si ricostruisce prima di qualunque nuovo rischio; nuovo giorno
=> zero; stato illeggibile o non affidabile => breach (fail-closed).
Persistenza in core/daily_loss_store.py.

P37: stop perdita di sessione (`max_session_loss`, euro, dall'ultimo start()).
E' un limite di rischio owner configurabile, senza default inventato, NON un
parametro del cashout (#426, decisione 06/10): blocca le nuove entrate e non
tocca uscite/cashout.
"""
from __future__ import annotations

from typing import Any, Optional

from core import validators
from core.daily_loss_store import (  # noqa: F401  (API usata dal runtime)
    DAILY_LOSS_STATE_KEY,
    daily_loss_record,
    persist_daily_loss,
    persist_failed,
    restore_daily_loss,
    restored_daily_loss,
)


def shift_day_baseline(state: dict, shift: float) -> None:
    """Il reset del ciclo recovery azzera `realized_pnl` del desk: spostare le
    baseline dello stesso importo lascia invariata la perdita del giorno."""
    for key in ("realized_pnl_day_baseline", "realized_pnl"):
        if state.get(key) is not None:
            state[key] = float(state[key]) - float(shift)


def session_loss_reason(limit_raw: Any, baseline: Optional[float], realized_pnl: float) -> str:
    """Motivo del blocco per la perdita di sessione, oppure "".

    Limite assente => nessun limite; illeggibile o <= 0 => blocca;
    perdita dall'ultimo start() che raggiunge il limite (con tolleranza) => blocca."""
    if limit_raw is None:
        return ""
    limit = validators.finite_number(limit_raw)
    if limit is None or limit <= 0.0:
        return "max_session_loss_non_valido"
    loss = 0.0 if baseline is None else max(0.0, float(baseline) - float(realized_pnl))
    if validators.reaches_limit(loss, limit):
        return f"session_loss_breached:loss={loss:.2f}:limit={limit}"
    return ""
