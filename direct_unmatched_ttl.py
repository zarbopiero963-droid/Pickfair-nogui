"""TTL ordini DIRECT non abbinati — detector PURO (Fase 2.2 / B6.3.1).

Funzione PURA che, dato l'elenco degli ordini correnti + l'istante attuale + un
TTL, individua gli ordini DIRECT del bot **completamente non abbinati** la cui
età supera il TTL, da cancellare (no retry; la cancellazione vera + persist +
notifica è il wiring B6.3.2).

Definizione conservativa di "non abbinato": ``sizeMatched == 0`` **e**
``sizeRemaining > 0`` (ordine resting mai eseguito). La cancellazione del
**residuo** di un ordine parzialmente abbinato (``sizeMatched > 0``) è un
non-goal di B6.3.1.

**Fail-closed**: qualunque anomalia (TTL non valido, riga malformata, età non
determinabile per ``placedDate`` assente/illeggibile) ⇒ l'ordine **non** viene
selezionato. Non si cancella mai "a vuoto" o su dati ambigui.

Zero dipendenze da broker/bus/runtime: solo dati in input → lista da cancellare.
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def select_expired_unmatched(
    *,
    current_orders: Any,
    now_epoch: Any,
    ttl_seconds: Any,
    scope_market_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, str]]:
    """Ordini DIRECT non abbinati scaduti da cancellare (fail-closed).

    Ritorna una lista di ``{"market_id": ..., "bet_id": ...}`` per gli ordini
    ``sizeMatched == 0`` e ``sizeRemaining > 0`` la cui età
    (``now_epoch - placed_epoch``) supera ``ttl_seconds``. Lista vuota se il TTL
    non è valido o nessun ordine qualifica. Deduplica per ``bet_id``.
    """
    now = _to_float(now_epoch)
    ttl = _to_float(ttl_seconds)
    # Fail-closed: TTL non finito/<=0 o now non finito ⇒ niente cancellazioni.
    if ttl is None or now is None or not math.isfinite(ttl) or ttl <= 0.0:
        return []
    if not math.isfinite(now):
        return []

    scope = None
    if scope_market_ids is not None:
        scope = {str(m).strip() for m in scope_market_ids if str(m).strip()}

    rows = current_orders if isinstance(current_orders, list) else []
    selected: List[Dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        picked = _expired_unmatched_row(row, now=now, ttl=ttl, scope=scope)
        if picked is None:
            continue
        bet_id = picked["bet_id"]
        if bet_id in seen:
            continue
        seen.add(bet_id)
        selected.append(picked)
    return selected


def _expired_unmatched_row(
    row: Any, *, now: float, ttl: float, scope: Optional[set]
) -> Optional[Dict[str, str]]:
    """``{"market_id","bet_id"}`` se la riga è un ordine non abbinato scaduto."""
    if not isinstance(row, dict):
        return None
    market_id = str(row.get("marketId") or row.get("market_id") or "").strip()
    bet_id = str(row.get("betId") or row.get("bet_id") or "").strip()
    if not market_id or not bet_id:
        return None
    if scope is not None and market_id not in scope:
        return None

    matched = _to_float(row.get("sizeMatched"))
    remaining = _to_float(row.get("sizeRemaining"))
    # Non abbinato (conservativo): nulla di matched, qualcosa di resting.
    if matched is None or remaining is None:
        return None
    if matched != 0.0 or remaining <= 0.0:
        return None

    placed = _placed_epoch(row)
    if placed is None:
        return None  # età non determinabile ⇒ fail-closed (non cancellare)
    if (now - placed) <= ttl:
        return None
    return {"market_id": market_id, "bet_id": bet_id}


def _placed_epoch(row: Dict[str, Any]) -> Optional[float]:
    """Epoch del piazzamento da ``placed_epoch`` (numerico) o ``placedDate`` ISO."""
    raw_epoch = row.get("placed_epoch")
    if raw_epoch is not None:
        val = _to_float(raw_epoch)
        return val if (val is not None and math.isfinite(val)) else None
    raw_date = row.get("placedDate") or row.get("placed_date")
    if not isinstance(raw_date, str) or not raw_date.strip():
        return None
    text = raw_date.strip()
    # Betfair usa lo suffisso Z (UTC); fromisoformat lo accetta solo come +00:00.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
