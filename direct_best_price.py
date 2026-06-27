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

import math
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

    if not math.isfinite(master) or master <= 1.0:
        return fallback("invalid_master_price")
    if side_u not in ("BACK", "LAY"):
        return fallback("invalid_side")
    # `_to_float` mappa i parse-failure a 0.0, indistinguibile da una tolleranza
    # 0.0 legittima: per la tolleranza serve un parser che distingua il fallimento
    # (None / stringa non numerica ⇒ invalid_tolerance, mai 0.0 silenzioso).
    tol = _to_float_or_none(max_deviation_pct)
    if tol is None or not math.isfinite(tol) or tol < 0.0:
        return fallback("invalid_tolerance")

    # Estrazione book→best del SOLO lato richiesto (difensiva, fail-closed).
    best, reason = _best_side_price(market_book, selection_id, side_u)
    if best is None:
        return fallback(reason)

    deviation_pct = abs(best - master) / master * 100.0
    if deviation_pct > tol:
        return fallback("out_of_tolerance", best=best, deviation=deviation_pct)

    return {
        "price": best,
        "source": SOURCE_LIVE_BOOK,
        "reason": "ok",
        "within_tolerance": True,
        "best_price": best,
        "deviation_pct": deviation_pct,
    }


def _best_side_price(market_book: Any, selection_id: Any, side_u: str):
    """Best price del SOLO lato richiesto, fail-closed.

    Ritorna ``(best_price, "ok")`` se ricavabile, altrimenti ``(None, reason)``.
    Difensivo: ``BACK`` → ``availableToBack``, ``LAY`` → ``availableToLay``;
    mai cross-spread. Qualunque schema malformato (tipi inattesi) ⇒
    ``(None, "malformed_book")`` — fail-closed, mai un'eccezione propagata.
    """
    try:
        runner, reason = _active_runner(market_book, selection_id)
        if runner is None:
            return None, reason
        return _best_level_price(runner, side_u)
    except Exception:  # noqa: BLE001 - schema inatteso => fail-closed al master
        return None, "malformed_book"


def _active_runner(market_book: Any, selection_id: Any):
    """Runner ATTIVO su un mercato esplicitamente OPEN, o ``(None, reason)``."""
    if not isinstance(market_book, dict) or not market_book:
        return None, "no_book"
    # Strict fail-closed: il mercato deve essere ESPLICITAMENTE OPEN (uno status
    # assente non è confermabile → fallback). Status top-level o, nel formato
    # streaming, sotto marketDefinition.
    if _market_status(market_book) != "OPEN":
        return None, "market_not_open"
    sid = _to_int(selection_id)
    if sid is None:
        return None, "invalid_selection_id"
    runners = market_book.get("runners")
    runner = _find_runner(runners if isinstance(runners, list) else [], sid)
    if runner is None:
        return None, "runner_missing"
    # Strict fail-closed: il runner dev'essere ESPLICITAMENTE ACTIVE — uno status
    # assente non è confermabile (book parziale/stale) ⇒ fallback. Coerente con
    # il market status, che pretende un OPEN esplicito.
    runner_status = str(runner.get("status") or "").strip().upper()
    if runner_status != "ACTIVE":
        return None, "runner_not_active"
    return runner, "ok"


def _market_status(market_book: Dict[str, Any]) -> str:
    status = str(market_book.get("status") or "").strip().upper()
    if not status:
        market_definition = market_book.get("marketDefinition")
        if isinstance(market_definition, dict):
            status = str(market_definition.get("status") or "").strip().upper()
    return status


def _best_level_price(runner: Dict[str, Any], side_u: str):
    """Best price del SOLO lato richiesto: finito, > 1.0, con liquidità (size>0)."""
    ex = runner.get("ex")
    ex = ex if isinstance(ex, dict) else {}
    raw_levels = ex.get("availableToBack") if side_u == "BACK" else ex.get("availableToLay")
    levels = raw_levels if isinstance(raw_levels, list) else []
    if not levels:
        return None, "no_side_liquidity"
    top = levels[0] if isinstance(levels[0], dict) else {}
    best = _to_float(top.get("price"))
    if not math.isfinite(best) or best <= 1.0:
        return None, "invalid_book_price"
    size = _to_float(top.get("size"))
    if not math.isfinite(size) or size <= 0.0:
        return None, "no_side_liquidity"   # livello presente ma senza liquidità eseguibile
    return best, "ok"


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


def _to_float_or_none(value: Any) -> Optional[float]:
    """Come ``_to_float`` ma ``None`` sul parse-failure (distingue 0.0 valido)."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> Optional[int]:
    if isinstance(value, bool):   # i bool non sono selection_id validi
        return None
    if isinstance(value, float):
        # un selection_id frazionario (es. 55.9) è malformato: `int()` lo
        # troncherebbe a 55 → match spurio. Accetta solo float interi finiti.
        if not math.isfinite(value) or value != int(value):
            return None
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
