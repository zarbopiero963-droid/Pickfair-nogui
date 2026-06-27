"""TTL ordini DIRECT non abbinati — detector PURO (Fase 2.2 / B6.3.1).

Funzione PURA che, dato l'elenco degli ordini correnti + l'istante attuale + un
TTL + l'**allowlist dei bet_id DIRECT**, individua gli ordini DIRECT del bot
**completamente non abbinati** la cui età supera il TTL, da cancellare (no retry;
la cancellazione vera + persist + notifica è il wiring B6.3.2).

- **Scope DIRECT obbligatorio**: solo i ``bet_id`` presenti in ``direct_bet_ids``
  sono candidati. In una lista ordini **mista** (cashout/green-up/copy), senza
  allowlist nulla viene selezionato → il detector non cancella mai ordini fuori
  dalla policy DIRECT.
- **Non abbinato (conservativo)**: ``sizeMatched == 0`` **e** ``sizeRemaining > 0``
  (resting mai eseguito). Il residuo di un parziale (``sizeMatched > 0``) è un
  non-goal.
- **Fail-closed**: TTL/now non validi (incl. ``bool`` e overflow), allowlist
  vuota, riga malformata, size non finite (NaN/inf), età non determinabile
  (``placed_epoch``/``placedDate`` assenti o illeggibili) ⇒ l'ordine **non**
  viene selezionato. Nessuna eccezione propagata, mai un cancel "a vuoto".

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
    direct_bet_ids: Any,
    scope_market_ids: Optional[Iterable[str]] = None,
) -> List[Dict[str, str]]:
    """Ordini DIRECT non abbinati scaduti da cancellare (fail-closed).

    Ritorna ``[{"market_id", "bet_id"}]`` per gli ordini il cui ``bet_id`` è in
    ``direct_bet_ids``, ``sizeMatched == 0`` e ``sizeRemaining > 0``, con età
    (``now_epoch - placed``) oltre ``ttl_seconds``. Deduplica per ``bet_id``.
    Lista vuota se TTL/now non validi o ``direct_bet_ids`` è vuoto/assente.
    """
    params = _validated_params(now_epoch, ttl_seconds, scope_market_ids, direct_bet_ids)
    if params is None:
        return []  # TTL/now non usabili o nessun DIRECT noto ⇒ niente cancel.
    now, ttl, scope, direct = params

    rows = current_orders if isinstance(current_orders, list) else []
    selected: List[Dict[str, str]] = []
    seen: set = set()
    for row in rows:
        picked = _expired_unmatched_row(row, now=now, ttl=ttl, scope=scope, direct=direct)
        if picked is None or picked["bet_id"] in seen:
            continue
        seen.add(picked["bet_id"])
        selected.append(picked)
    return selected


def _validated_params(
    now_epoch: Any,
    ttl_seconds: Any,
    scope_market_ids: Optional[Iterable[str]],
    direct_bet_ids: Any,
) -> Optional[tuple]:
    """``(now, ttl, scope, direct)`` validati, o ``None`` (fail-closed).

    ``None`` se ``now``/``ttl`` non sono numeri finiti usabili (``ttl > 0``) o se
    l'allowlist DIRECT è vuota/assente — senza sapere quali ordini sono DIRECT
    non si cancella nulla.
    """
    now = _to_float(now_epoch)
    ttl = _to_float(ttl_seconds)
    if (now is None or ttl is None
            or not math.isfinite(now) or not math.isfinite(ttl) or ttl <= 0.0):
        return None
    direct = _str_set(direct_bet_ids)
    if not direct:
        return None
    scope = _str_set(scope_market_ids) if scope_market_ids is not None else None
    return now, ttl, scope, direct


def _expired_unmatched_row(
    row: Any, *, now: float, ttl: float, scope: Optional[set], direct: set
) -> Optional[Dict[str, str]]:
    """``{"market_id","bet_id"}`` se la riga è un ordine DIRECT non abbinato scaduto."""
    ids = _order_ids(row, scope=scope, direct=direct)
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


def _order_ids(row: Any, *, scope: Optional[set], direct: set) -> Optional[tuple]:
    """``(market_id, bet_id)`` di un ordine **DIRECT** in scope, altrimenti ``None``."""
    if not isinstance(row, dict):
        return None
    market_id = str(row.get("marketId") or row.get("market_id") or "").strip()
    bet_id = str(row.get("betId") or row.get("bet_id") or "").strip()
    if not market_id or not bet_id:
        return None
    if bet_id not in direct:   # solo ordini DIRECT noti (mai cashout/copy/green-up)
        return None
    if scope is not None and market_id not in scope:
        return None
    return market_id, bet_id


def _is_fully_unmatched(row: Dict[str, Any]) -> bool:
    """``True`` se resting mai eseguito (``sizeMatched==0`` e ``sizeRemaining>0``).

    Dati mancanti o **non finiti** (NaN/inf) ⇒ ``False`` (fail-closed): una size
    malformata non deve mai produrre un target di cancellazione.
    """
    matched = _to_float(row.get("sizeMatched"))
    remaining = _to_float(row.get("sizeRemaining"))
    if matched is None or remaining is None:
        return False
    if not math.isfinite(matched) or not math.isfinite(remaining):
        return False
    return matched == 0.0 and remaining > 0.0


def _placed_epoch(row: Dict[str, Any]) -> Optional[float]:
    """Epoch del piazzamento da ``placed_epoch`` (numerico) o ``placedDate`` ISO.

    Se ``placed_epoch`` è presente ma malformato/non finito si ricade su
    ``placedDate`` invece di mascherare un timestamp valido (età-ignota spuria).
    """
    raw_epoch = row.get("placed_epoch")
    if raw_epoch is not None:
        val = _to_float(raw_epoch)
        if val is not None and math.isfinite(val):
            return val
        # placed_epoch inservibile ⇒ tenta placedDate sulla stessa riga.
    raw_date = row.get("placedDate") or row.get("placed_date")
    if not isinstance(raw_date, str) or not raw_date.strip():
        return None
    text = raw_date.strip()
    # Betfair usa il suffisso Z (UTC); fromisoformat lo accetta solo come +00:00.
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _str_set(values: Any) -> set:
    """Set di stringhe non vuote da un iterabile; ``set()`` se assente/non valido."""
    if not values:
        return set()
    try:
        return {str(v).strip() for v in values if str(v).strip()}
    except TypeError:
        return set()


def _to_float(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        # un bool non è una durata/size/epoch valida: True→1.0 maschererebbe un bug.
        return None
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError (es. int gigante): rispetta il contratto no-raise.
        return None
