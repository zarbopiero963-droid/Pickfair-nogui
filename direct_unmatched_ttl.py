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
    params = _validated_params(now_epoch, ttl_seconds, scope_market_ids)
    if params is None:
        return []  # TTL/now non utilizzabili ⇒ fail-closed, niente cancellazioni.
    now, ttl, scope = params

    rows = current_orders if isinstance(current_orders, list) else []
    selected: List[Dict[str, str]] = []
    seen: set[str] = set()
    for row in rows:
        picked = _expired_unmatched_row(row, now=now, ttl=ttl, scope=scope)
        if picked is None or picked["bet_id"] in seen:
            continue
        seen.add(picked["bet_id"])
        selected.append(picked)
    return selected


def _validated_params(
    now_epoch: Any, ttl_seconds: Any, scope_market_ids: Optional[Iterable[str]]
) -> Optional[tuple]:
    """``(now, ttl, scope)`` validati, o ``None`` se TTL/now non sono usabili.

    Fail-closed: TTL non finito o ``<= 0`` e ``now`` non finito ⇒ ``None`` ⇒ il
    chiamante non cancella nulla. ``scope`` è un set di market_id o ``None``.
    """
    now = _to_float(now_epoch)
    ttl = _to_float(ttl_seconds)
    if (now is None or ttl is None
            or not math.isfinite(now) or not math.isfinite(ttl) or ttl <= 0.0):
        return None
    scope = None
    if scope_market_ids is not None:
        scope = {str(m).strip() for m in scope_market_ids if str(m).strip()}
    return now, ttl, scope


def _expired_unmatched_row(
    row: Any, *, now: float, ttl: float, scope: Optional[set]
) -> Optional[Dict[str, str]]:
    """``{"market_id","bet_id"}`` se la riga è un ordine non abbinato scaduto.

    Decomposto in predicati per chiarezza: identità in scope → completamente
    non abbinato → età oltre TTL. Ogni gate è fail-closed.
    """
    ids = _order_ids(row, scope=scope)
    if ids is None:
        return None
    if not _is_fully_unmatched(row):
        return None
    placed = _placed_epoch(row)
    if placed is None or (now - placed) <= ttl:
        # età non determinabile o non oltre TTL ⇒ non cancellare.
        return None
    market_id, bet_id = ids
    return {"market_id": market_id, "bet_id": bet_id}


def _order_ids(row: Any, *, scope: Optional[set]) -> Optional[tuple]:
    """``(market_id, bet_id)`` validi e in scope, altrimenti ``None``."""
    if not isinstance(row, dict):
        return None
    market_id = str(row.get("marketId") or row.get("market_id") or "").strip()
    bet_id = str(row.get("betId") or row.get("bet_id") or "").strip()
    if not market_id or not bet_id:
        return None
    if scope is not None and market_id not in scope:
        return None
    return market_id, bet_id


def _is_fully_unmatched(row: Dict[str, Any]) -> bool:
    """``True`` se l'ordine è resting mai eseguito (``sizeMatched==0`` e
    ``sizeRemaining>0``). Dati malformati ⇒ ``False`` (fail-closed)."""
    matched = _to_float(row.get("sizeMatched"))
    remaining = _to_float(row.get("sizeRemaining"))
    if matched is None or remaining is None:
        return False
    return matched == 0.0 and remaining > 0.0


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
