"""Liquidita' ESEGUIBILE di un ordine sul book reale (PF-ALL-F030, #383).

Lato ladder Betfair-standard, deciso dall'owner (#383): un BACK consuma ``availableToBack``, un LAY consuma
``availableToLay``. Un livello conta solo se eseguibile alla quota dell'ordine: BACK se quota del livello >= quota
dell'ordine, LAY se <=. Le ``size`` del book sono backer-stake, omogenee con lo stake dell'ordine.
Funzioni pure: nessun I/O, nessuno stato. ``None`` = osservazione ignota (mai zero inventato).
"""
import math
from typing import Any, Optional


def liquidity_crosses(side: Any, order_price: float, book_price: float) -> bool:
    if str(side).upper() == "BACK":
        return book_price >= order_price
    return book_price <= order_price


def sum_executable_liquidity(ladder: Any, side: Any, order_price: float) -> float:
    total = 0.0
    if not isinstance(ladder, (list, tuple)):
        # ladder malformata (rilievo Codex #512): nessuna liquidita' dimostrabile, mai un'eccezione
        return total
    for level in ladder:
        try:
            lvl_price = float((level or {}).get("price", 0.0) or 0.0)
            lvl_size = float((level or {}).get("size", 0.0) or 0.0)
        except (TypeError, ValueError, AttributeError):
            continue
        if lvl_price <= 0.0 or lvl_size <= 0.0 or not (math.isfinite(lvl_price) and math.isfinite(lvl_size)):
            continue
        if liquidity_crosses(side, order_price, lvl_price):
            total += lvl_size
    return total


def executable_liquidity(book: Any, selection_id: Any, side: Any, price: Any) -> Optional[float]:
    """Liquidita' eseguibile per selezione/lato/quota; None se book, selezione o ladder non disponibili."""
    if not isinstance(book, dict):
        return None
    runners = book.get("runners")
    if not isinstance(runners, list):
        return None
    try:
        target = int(selection_id)
        order_price = float(price)
    except (TypeError, ValueError):
        return None
    ladder_key = "availableToBack" if str(side).upper() == "BACK" else "availableToLay"
    for runner in runners:
        if not isinstance(runner, dict):
            continue
        sid = runner.get("selectionId")
        try:
            if sid is None or int(sid) != target:
                continue
        except (TypeError, ValueError):
            continue
        ex = runner.get("ex")
        ladder = ex.get(ladder_key) if isinstance(ex, dict) else None
        if not isinstance(ladder, (list, tuple)):
            # assente o malformata (rilievo Codex #512): dato ignoto => il RiskGate nega (fail-closed)
            return None
        for level in ladder:
            # un valore non finito (Infinity/NaN) renderebbe la somma infinita: dato ignoto
            if isinstance(level, dict):
                try:
                    p, s = float(level.get("price", 0.0) or 0.0), float(level.get("size", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
                if not (math.isfinite(p) and math.isfinite(s)):
                    return None
        return sum_executable_liquidity(ladder, side, order_price)
    return None


def single_entry_liquidity(book: Any, selection_id: Any, side: Any, price: Any) -> tuple:
    """Per un ingresso singolo: (liquidita', motivo). Mercato non OPEN o selezione non ACTIVE => dato non
    attendibile per un ingresso (None, motivo); il RiskGate nega un dato richiesto e assente."""
    if not isinstance(book, dict) or not isinstance(book.get("runners"), list):
        return None, "LIQUIDITY_BOOK_UNAVAILABLE"
    # Stato esplicito obbligatorio (rilievo Codex r4312719713 su #512): una risposta senza stato e' incompleta,
    # non un mercato OPEN / una selezione ACTIVE.
    if str(book.get("status") or "").upper() != "OPEN":
        return None, "LIQUIDITY_MARKET_NOT_OPEN"
    for runner in book["runners"]:
        try:
            if isinstance(runner, dict) and int(runner.get("selectionId")) == int(selection_id):
                if str(runner.get("status") or "").upper() != "ACTIVE":
                    return None, "LIQUIDITY_RUNNER_NOT_ACTIVE"
        except (TypeError, ValueError):
            continue
    value = executable_liquidity(book, selection_id, side, price)
    return value, ("" if value is not None else "LIQUIDITY_LADDER_UNAVAILABLE")
