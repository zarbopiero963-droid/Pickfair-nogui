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
from typing import Any, Optional, Tuple

CENT = Decimal("0.01")
MIN_PRICE = Decimal("1.01")   # quota minima Betfair: VALIDA (Sol su 6d232f4: scartarla saltava lo SL di un LAY in perdita)


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


def closing_quote(runner: dict, side: str, stake: Any, entry_price: Any) -> Optional[Tuple[Decimal, bool]]:
    """Prezzo a cui la posizione si chiude ORA e se il book basta a chiuderla: ``(prezzo, coperto)``.

    BACK chiude in LAY (``availableToLay``), LAY chiude in BACK (``availableToBack``); mai ``lastPriceTraded``.
    Sol su 6d232f4: il miglior prezzo da solo ignorava la profondita' (BACK 10@2 chiuso a 1.60 con 0.01 disponibili
    sembrava +2.39). Si scorrono i livelli finche' la size cumulata copre l'hedge necessario a quel prezzo
    (``stake * ingresso / prezzo``): il prezzo e' quello del livello piu' profondo usato (stima prudente). Book
    insufficiente => ``(prezzo dell'ultimo livello valido, False)``; nessun livello valido => ``None``."""
    s, entry = to_decimal(stake), to_decimal(entry_price)
    if s is None or s <= 0 or entry is None or entry < MIN_PRICE:
        return None
    ex = (runner or {}).get("ex") or {}
    ladder = ex.get("availableToLay") if str(side).upper() == "BACK" else ex.get("availableToBack")
    if not isinstance(ladder, list):
        return None
    covered_size, last_price = Decimal("0"), None
    for level in ladder:
        price = to_decimal(level.get("price")) if isinstance(level, dict) else None
        size = to_decimal(level.get("size")) if isinstance(level, dict) else None
        if price is None or price < MIN_PRICE or size is None or size < 0:
            break                               # livello illeggibile: la profondita' oltre non e' dimostrabile
        covered_size, last_price = covered_size + size, price
        if covered_size >= (s * entry / price).quantize(CENT):
            return price, True
    return (last_price, False) if last_price is not None else None


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
    """Netto liquidabile (2 decimali) chiudendo ora la posizione; ``None`` se un input non e' valido.

    Stessa formula e stessi arrotondamenti del cashout (``dutching.dynamic_cashout_single``: hedge = stake*ingresso/
    chiusura, green-up = media dei due esiti, commissione di policy sul netto positivo), ma VALIDA anche alla quota
    minima 1.01 che la funzione del cashout rifiuta (Sol su 6d232f4: un LAY in forte perdita chiuso a 1.01 deve far
    scattare lo stop-loss, non sparire dal monitor)."""
    side_u = str(side or "").upper()
    s, entry, close, comm = to_decimal(stake), to_decimal(entry_price), to_decimal(close_price), to_decimal(commission_pct)
    if side_u not in ("BACK", "LAY") or None in (s, entry, close, comm):
        return None
    if s <= 0 or entry < MIN_PRICE or close < MIN_PRICE or comm < 0:
        return None
    from dutching import (  # import tardivo: core non dipende da dutching all'import
        _apply_commission,
        _resolve_policy_commission_pct,
        _round_step,
    )
    one = Decimal("1")
    hedge = _round_step(s * entry / close)
    if side_u == "LAY":
        if_win, if_lose = hedge * (close - one) - s * (entry - one), s - hedge
    else:
        if_win, if_lose = s * (entry - one) - hedge * (close - one), hedge - s
    green = _round_step((_round_step(if_win) + _round_step(if_lose)) / Decimal("2"))
    return _round_step(_apply_commission(green, _resolve_policy_commission_pct(comm))).quantize(CENT)
