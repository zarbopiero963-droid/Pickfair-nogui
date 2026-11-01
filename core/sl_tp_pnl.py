"""F-01 (#586, CSH-01/CSH-10/NF-06) — P&L liquidabile di una posizione per il monitor SL/TP.

Decisione owner #426 c.6112703848: lo SL/TP si confronta con la perdita/il profitto NETTO LIQUIDABILE, commissioni
incluse; c.6141450045 Q7: Decimal. Il valore e' il green-up del cashout, con la STESSA formula di
``dutching.dynamic_cashout_single`` estesa a piu' livelli del book: stake reale della gamba, hedge al lato di chiusura
(BACK chiude in LAY, LAY chiude in BACK) consumato livello per livello al proprio prezzo, commissione solo sul netto
positivo. Valori non validi => ``None``
(il monitor non decide nulla: fail-closed).
"""
from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any, List, Optional, Tuple

CENT = Decimal("0.01")
MIN_PRICE = Decimal("1.01")   # quota minima Betfair: VALIDA (Sol su 6d232f4: scartarla saltava lo SL di un LAY in perdita)

Leg = Tuple[Decimal, Decimal]               # (prezzo, size) di una gamba di hedge


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


def closing_legs(runner: dict, side: str, stake: Any, entry_price: Any) -> Optional[Tuple[List[Leg], bool]]:
    """Gambe di hedge con cui la posizione si chiude ORA sul book e se il book basta: ``(gambe, coperto)``.

    BACK chiude in LAY (``availableToLay``), LAY chiude in BACK (``availableToBack``); mai ``lastPriceTraded``.
    Il green-up richiede ``somma(hedge_i * prezzo_i) = stake * ingresso``: si consuma la scala livello per livello,
    ognuno al PROPRIO prezzo e fino alla propria size (Codex P2 e Sol su abfd2ed: applicare il prezzo del livello
    piu' profondo a tutta la posizione dichiarava coperto un book che non lo era e invertiva il P&L, con uno SL
    spurio). Book insufficiente => il residuo non coperto e' valutato all'ultimo livello valido e ``coperto=False``
    (vale solo per lo SL protettivo); nessun livello valido => ``None``."""
    from dutching import _round_step  # import tardivo: core non dipende da dutching all'import
    s, entry = to_decimal(stake), to_decimal(entry_price)
    if s is None or s <= 0 or entry is None or entry < MIN_PRICE:
        return None
    ex = (runner or {}).get("ex") or {}
    ladder = ex.get("availableToLay") if str(side).upper() == "BACK" else ex.get("availableToBack")
    if not isinstance(ladder, list):
        return None
    legs: List[Leg] = []
    residual, last_price = s * entry, None      # nozionale ancora da coprire, ultimo livello valido
    for level in ladder:
        price = to_decimal(level.get("price")) if isinstance(level, dict) else None
        size = to_decimal(level.get("size")) if isinstance(level, dict) else None
        if price is None or price < MIN_PRICE or size is None or size < 0:
            break                               # livello illeggibile: la profondita' oltre non e' dimostrabile
        last_price, need = price, residual / price
        if size >= need:
            legs.append((price, _round_step(need)))
            return legs, True
        if size > 0:
            legs.append((price, size))
            residual -= size * price            # ogni livello copre size * il PROPRIO prezzo
    if last_price is None:
        return None
    legs.append((last_price, _round_step(residual / last_price)))   # residuo non coperto: stima per lo SL
    return legs, False


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
    """Netto liquidabile chiudendo ora l'intera posizione a un solo prezzo; ``None`` se un input non e' valido.

    Stessa formula e stessi arrotondamenti del cashout (``dutching.dynamic_cashout_single``: hedge = stake*ingresso/
    chiusura), ma VALIDA anche alla quota minima 1.01 che la funzione del cashout rifiuta (Sol su 6d232f4)."""
    close = to_decimal(close_price)
    s, entry = to_decimal(stake), to_decimal(entry_price)
    if close is None or close < MIN_PRICE or s is None or entry is None:
        return None
    from dutching import _round_step  # import tardivo
    return liquidable_pnl_legs(side, s, entry, [(close, _round_step(s * entry / close))], commission_pct)


def liquidable_pnl_legs(side: str, stake: Any, entry_price: Any, legs: Any, commission_pct: Any,
                        guaranteed: bool = False) -> Optional[Decimal]:
    """Netto liquidabile (2 decimali) chiudendo la posizione con le gambe di hedge ``[(prezzo, size), ...]``.

    Esiti della posizione + hedge (LAY chiuso in BACK, BACK chiuso in LAY), green-up = media dei due esiti
    arrotondati, commissione di policy sul netto positivo: con una sola gamba coincide con
    ``dutching.dynamic_cashout_single``. ``guaranteed=True`` (per il TP) restituisce invece il netto dell'esito
    PEGGIORE: con le size arrotondate gli esiti possono divergere (Sol su f1de646: LAY 10@1.60 + 2@1000, ultima gamba
    0.00 => esiti +4/0, la media +1.91 non e' un profitto garantito). Input non valido => ``None`` (fail-closed)."""
    side_u = str(side or "").upper()
    s, entry, comm = to_decimal(stake), to_decimal(entry_price), to_decimal(commission_pct)
    if side_u not in ("BACK", "LAY") or None in (s, entry, comm) or not isinstance(legs, list) or not legs:
        return None
    if s <= 0 or entry < MIN_PRICE or comm < 0:
        return None
    one = Decimal("1")
    hedge_total, hedge_win = Decimal("0"), Decimal("0")   # somma size, somma size*(prezzo-1)
    for leg in legs:
        if not isinstance(leg, (tuple, list)) or len(leg) != 2:
            return None
        price, size = to_decimal(leg[0]), to_decimal(leg[1])
        if price is None or size is None or price < MIN_PRICE or size < 0:
            return None
        hedge_total += size
        hedge_win += size * (price - one)
    from dutching import (  # import tardivo: core non dipende da dutching all'import
        _apply_commission,
        _resolve_policy_commission_pct,
        _round_step,
    )
    if side_u == "LAY":
        if_win, if_lose = hedge_win - s * (entry - one), s - hedge_total
    else:
        if_win, if_lose = s * (entry - one) - hedge_win, hedge_total - s
    if_win, if_lose = _round_step(if_win), _round_step(if_lose)
    green = min(if_win, if_lose) if guaranteed else _round_step((if_win + if_lose) / Decimal("2"))
    return _round_step(_apply_commission(green, _resolve_policy_commission_pct(comm))).quantize(CENT)
