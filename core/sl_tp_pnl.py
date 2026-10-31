"""F-01 (#586, CSH-01/CSH-10/NF-06) — P&L liquidabile di una posizione per il monitor SL/TP.

Decisione owner #426 c.6112703848: lo SL/TP si confronta con la perdita/il profitto NETTO LIQUIDABILE, commissioni
incluse; c.6141450045 Q7: Decimal. Il valore e' il green-up del cashout a quel prezzo, calcolato con la STESSA
funzione del cashout (``dutching.dynamic_cashout_single``): stake reale della gamba, prezzo al lato di chiusura del
book (BACK chiude in LAY, LAY chiude in BACK), commissione solo sul netto positivo. Valori non validi => ``None``
(il monitor non decide nulla: fail-closed).
"""
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

CENT = Decimal("0.01")


def to_decimal(value: Any) -> Optional[Decimal]:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(f):
        return None
    try:
        return Decimal(str(f))
    except InvalidOperation:
        return None


def closing_price(runner: dict, side: str) -> Optional[Decimal]:
    """Prezzo a cui la posizione si chiude ORA: BACK => miglior availableToLay, LAY => miglior availableToBack.
    Mai ``lastPriceTraded`` (prezzo storico, non eseguibile)."""
    ex = (runner or {}).get("ex") or {}
    ladder = ex.get("availableToLay") if str(side).upper() == "BACK" else ex.get("availableToBack")
    if not ladder or not isinstance(ladder[0], dict):
        return None
    price = to_decimal(ladder[0].get("price"))
    return price if price is not None and price > Decimal("1.01") else None


def position_stake(side: str, meta: dict, exposure: Any) -> Optional[Decimal]:
    """Stake reale della gamba: ``meta['stake']`` se presente; altrimenti BACK = esposizione, LAY = liability/(p-1)."""
    stake = to_decimal((meta or {}).get("stake"))
    if stake is not None and stake > 0:
        return stake
    exp = to_decimal(exposure)
    price = to_decimal((meta or {}).get("price"))
    if exp is None or exp <= 0:
        return None
    if str(side).upper() == "BACK":
        return exp
    if price is None or price <= Decimal("1"):
        return None
    return (exp / (price - Decimal("1"))).quantize(CENT)


def liquidable_pnl(side: str, stake: Any, entry_price: Any, close_price: Any, commission_pct: Any) -> Optional[Decimal]:
    """Netto liquidabile (2 decimali) chiudendo ora la posizione; ``None`` se un input non e' valido."""
    side_u = str(side or "").upper()
    s, entry, close, comm = to_decimal(stake), to_decimal(entry_price), to_decimal(close_price), to_decimal(commission_pct)
    if side_u not in ("BACK", "LAY") or None in (s, entry, close, comm):
        return None
    if s <= 0 or entry <= Decimal("1.01") or close <= Decimal("1.01") or comm < 0:
        return None
    from dutching import (
        dynamic_cashout_single,  # import tardivo: core non dipende da dutching all'import
    )
    calc = dynamic_cashout_single(matched_stake=float(s), matched_price=float(entry), current_price=float(close),
                                  commission=float(comm), side=side_u)
    return to_decimal(calc.get("net_profit")).quantize(CENT)
