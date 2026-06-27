"""Best-price DIRECT — estrattore puro (Fase 2.2 / B6.1).

Funzione PURA che, dato un market book live + selection_id + side + master_price,
ricava il best price **difensivo** per il percorso copy DIRECT, **fail-closed**:

- **Difensivo**: per un BACK usa solo `availableToBack` (mai il lato LAY), per un
  LAY solo `availableToLay`. Non attraversa lo spread → niente "aggressione"
  involontaria del lato sbagliato (a differenza del resolver `aggressive_best_price`).
- **Tolleranza**: il best price deve stare entro `max_deviation_pct` dal
  master_price; oltre → fallback al master (price drift sospetto / book stale).
- **Fail-closed**: qualunque anomalia (book assente, mercato non OPEN, runner
  mancante/inattivo, lato senza liquidità, prezzo non valido) → fallback al
  master_price, **mai** un prezzo inventato.

Zero dipendenze da broker/bus/runtime: solo dati in input → risultato. Il wiring
sul percorso DIRECT (dietro flag default-OFF) è B6.2; la gestione degli ordini
non abbinati (TTL/cancel) è B6.3.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

#: ``price`` usato dal master (fallback) — il best price live non è applicato.
SOURCE_FALLBACK_MASTER = "FALLBACK_MASTER"
#: ``price`` preso dal book live entro tolleranza.
SOURCE_LIVE_BOOK = "LIVE_BOOK_DIRECT"


def resolve_direct_best_price(
    *,
    market_book: Any,
    selection_id: Any,
    side: Any,
    master_price: Any,
    max_deviation_pct: float = 2.0,
) -> Dict[str, Any]:
    """Risolve il best price difensivo dal book per il DIRECT (fail-closed).

    Ritorna sempre un dict con:
    - ``price``: il prezzo da usare (best live se ok, altrimenti master);
    - ``source``: ``LIVE_BOOK_DIRECT`` o ``FALLBACK_MASTER``;
    - ``reason``: motivo (``ok`` | ``no_book`` | ``market_not_open`` |
      ``runner_missing`` | ``runner_not_active`` | ``no_side_liquidity`` |
      ``invalid_book_price`` | ``out_of_tolerance`` | ``invalid_master_price`` |
      ``invalid_side`` | ``invalid_selection_id``);
    - ``within_tolerance``: bool;
    - ``best_price``: il best price live osservato (``None`` se non ricavabile);
    - ``deviation_pct``: deviazione % best vs master (``None`` se non calcolabile).
    """
    master = _to_float(master_price)
    side_u = str(side or "").strip().upper()

    def fallback(reason: str, *, best: Optional[float] = None,
                 deviation: Optional[float] = None) -> Dict[str, Any]:
        return {
            "price": master,
            "source": SOURCE_FALLBACK_MASTER,
            "reason": reason,
            "within_tolerance": False,
            "best_price": best,
            "deviation_pct": deviation,
        }

    if master <= 1.0:
        return fallback("invalid_master_price")
    if side_u not in ("BACK", "LAY"):
        return fallback("invalid_side")
    if not isinstance(market_book, dict) or not market_book:
        return fallback("no_book")

    status = str(market_book.get("status") or "").strip().upper()
    if status and status != "OPEN":
        return fallback("market_not_open")   # SUSPENDED / CLOSED / INACTIVE

    sid = _to_int(selection_id)
    if sid is None:
        return fallback("invalid_selection_id")

    runner = _find_runner(market_book.get("runners") or [], sid)
    if runner is None:
        return fallback("runner_missing")

    runner_status = str(runner.get("status") or "").strip().upper()
    if runner_status and runner_status != "ACTIVE":
        return fallback("runner_not_active")

    ex = runner.get("ex") or {}
    # Difensivo: SOLO il lato richiesto, mai cross-spread.
    levels = ex.get("availableToBack") if side_u == "BACK" else ex.get("availableToLay")
    levels = levels if isinstance(levels, list) else []
    if not levels:
        return fallback("no_side_liquidity")

    best = _to_float((levels[0] or {}).get("price"))
    if best <= 1.0:
        return fallback("invalid_book_price")

    deviation_pct = abs(best - master) / master * 100.0
    if deviation_pct > float(max_deviation_pct):
        return fallback("out_of_tolerance", best=best, deviation=deviation_pct)

    return {
        "price": best,
        "source": SOURCE_LIVE_BOOK,
        "reason": "ok",
        "within_tolerance": True,
        "best_price": best,
        "deviation_pct": deviation_pct,
    }


def _find_runner(runners: List[Any], selection_id: int) -> Optional[Dict[str, Any]]:
    for runner in runners:
        if not isinstance(runner, dict):
            continue
        rid = _to_int(runner.get("selectionId") if runner.get("selectionId") is not None
                      else runner.get("selection_id"))
        if rid is not None and rid == selection_id:
            return runner
    return None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):   # i bool non sono selection_id validi
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
