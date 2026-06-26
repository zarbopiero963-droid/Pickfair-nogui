"""Cashout resolver (Fase 2.1-B, design A3).

Ricostruisce le posizioni aperte del bot dagli **ordini correnti live** (sorgente
A3: ``betfair_service.list_current_orders``) e costruisce i payload
``REQ_EXECUTE_CASHOUT`` per chiuderle (green-up).

Funzioni **PURE** (nessun I/O): il runtime fornisce gli ordini correnti e i
prezzi di mercato; qui si fa solo aggregazione delle posizioni nette + la
matematica del green-up. Così il cuore di A3 è testabile senza broker e senza
toccare file forbidden.

Gap noto SIM/LIVE: il prezzo d'ingresso usa ``averagePriceMatched`` (presente
nei row LIVE di Betfair); in SIM quel campo non c'è e si ripiega su
``priceSize.price`` (prezzo limite d'ingresso) come proxy.

Approssimazione del netting: per una selezione con fill su entrambi i lati, la
posizione netta usa il prezzo medio pesato del **lato dominante** (i fill del
lato opposto riducono solo la size netta). Per il caso comune del copy-mirror
(posizione a lato singolo) il prezzo è esatto.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Tuple

from dutching import dynamic_cashout_single

logger = logging.getLogger(__name__)

_VALID_SIDES = {"BACK", "LAY"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _row_matched_price(order: Dict[str, Any]) -> float:
    """Prezzo medio abbinato del row: averagePriceMatched (LIVE) o, se mancante o
    non valido (None/""/0.0/<=1), priceSize.price (SIM, proxy d'ingresso). Il
    check su >1 evita di tenere uno 0.0 presente e scartare un ordine valido."""
    price = _to_float(order.get("averagePriceMatched"))
    if price <= 1.0:
        price = _to_float((order.get("priceSize") or {}).get("price"))
    return price


def _aggregate_orders(current_orders: Any) -> Dict[Tuple[str, int], Dict[str, float]]:
    """Somma per ``(market_id, selection_id)`` le size abbinate e i nozionali per
    lato (BACK/LAY), filtrando i row invalidi. Estratto da
    ``reconstruct_open_positions`` per separare parsing/filtraggio dal netting."""
    groups: Dict[Tuple[str, int], Dict[str, float]] = {}
    for order in current_orders or []:
        if not isinstance(order, dict):
            continue
        market_id = str(order.get("marketId") or order.get("market_id") or "").strip()
        selection_raw = order.get("selectionId", order.get("selection_id"))
        side = str(order.get("side") or "").upper().strip()
        if not market_id or selection_raw in (None, "") or side not in _VALID_SIDES:
            continue
        matched = _to_float(order.get("sizeMatched"))
        if matched <= 0.0:
            continue
        price = _row_matched_price(order)
        if price <= 1.0:
            continue

        key = (market_id, int(_to_float(selection_raw)))
        grp = groups.setdefault(
            key, {"back_size": 0.0, "back_notional": 0.0, "lay_size": 0.0, "lay_notional": 0.0}
        )
        if side == "BACK":
            grp["back_size"] += matched
            grp["back_notional"] += matched * price
        else:
            grp["lay_size"] += matched
            grp["lay_notional"] += matched * price
    return groups


def reconstruct_open_positions(current_orders: Any) -> List[Dict[str, Any]]:
    """Aggrega gli ordini correnti in posizioni NETTE per ``(market_id, selection_id)``.

    Ritorna una lista di ``{market_id, selection_id, side, stake, price}`` dove
    ``side`` è il lato netto (BACK se il matched BACK supera il LAY, viceversa),
    ``stake`` = ``|back_matched - lay_matched|`` e ``price`` = prezzo medio pesato
    del lato dominante. Selezioni piatte (netto ~0), senza matched o con prezzo
    non valido vengono escluse.
    """
    positions: List[Dict[str, Any]] = []
    for (market_id, selection_id), grp in _aggregate_orders(current_orders).items():
        net = grp["back_size"] - grp["lay_size"]
        if abs(net) <= 1e-9:
            continue  # perfettamente coperta => nessuna esposizione da chiudere
        if net > 0:
            side = "BACK"
            stake = net
            price = grp["back_notional"] / grp["back_size"] if grp["back_size"] > 0 else 0.0
        else:
            side = "LAY"
            stake = -net
            price = grp["lay_notional"] / grp["lay_size"] if grp["lay_size"] > 0 else 0.0
        if stake <= 0.0 or price <= 1.0:
            continue
        positions.append({
            "market_id": market_id,
            "selection_id": selection_id,
            "side": side,
            "stake": stake,
            "price": price,
        })
    return positions


def build_cashout_request(
    position: Dict[str, Any],
    *,
    current_price: Any,
    commission: float = 4.5,
    source: str = "TELEGRAM",
) -> Optional[Dict[str, Any]]:
    """Costruisce il payload ``REQ_EXECUTE_CASHOUT`` per chiudere ``position``.

    ``current_price`` è la quota live al **lato di chiusura** (LAY per una
    posizione BACK, BACK per una LAY): il chiamante la ricava dal book. Il
    payload porta il lato OPPOSTO all'originale (``side_to_place``) da piazzare.
    Ritorna ``None`` se la posizione non è chiudibile (stake/prezzi non validi).
    """
    side = str(position.get("side") or "").upper()
    stake = _to_float(position.get("stake"))
    entry = _to_float(position.get("price"))
    cur = _to_float(current_price)
    if side not in _VALID_SIDES or stake <= 0.0 or entry <= 1.0 or cur <= 1.0:
        return None

    try:
        calc = dynamic_cashout_single(
            matched_stake=stake,
            matched_price=entry,
            current_price=cur,
            commission=commission,
            side=side,
        )
    except Exception as exc:
        # Math non calcolabile (es. violazione policy commissione): fail-closed,
        # si salta questa posizione invece di far crashare il routing. Loggato
        # (non mascherato) per diagnosi.
        logger.warning("[cashout_resolver] green-up non calcolabile, posizione saltata: %s", exc)
        return None
    if not isinstance(calc, dict):
        return None
    cashout_stake = _to_float(calc.get("cashout_stake"))
    side_to_place = str(calc.get("side_to_place") or "").upper()
    if cashout_stake <= 0.0 or side_to_place not in _VALID_SIDES:
        return None

    # round PRIMA del check: uno stake positivo ma sub-cent arrotonda a 0.0, che
    # l'executor rifiuterebbe — meglio saltare la posizione qui (fail-closed).
    rounded_stake = round(cashout_stake, 2)
    if rounded_stake <= 0.0:
        return None

    return {
        "market_id": str(position.get("market_id") or ""),
        "selection_id": int(_to_float(position.get("selection_id"))),
        "side": side_to_place,
        "stake": rounded_stake,
        "price": cur,
        "green_up": round(_to_float(calc.get("green_up")), 2),
        "original_pos": {"side": side, "stake": stake, "price": entry},
        "source": source,
    }
