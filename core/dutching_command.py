"""Comando interno tipizzato per il Dutching: l'ingresso esplicito della corsia dutching.

La corsia ``REQ_PLACE_DUTCHING -> RiskMiddleware -> CMD_PLACE_DUTCHING -> DutchingController`` (precheck Roserpina,
tavoli, fondi freschi, cap, negoziabilita' del mercato, idempotenza del lotto, gambe come ``CMD_QUICK_BET`` nel choke
dell'engine) non aveva un publisher (headless_main.py: «corsia armata ma dormiente»). Questo modulo e' quel publisher,
pensato per essere riusato dalla Control API: nessun percorso di trading parallelo, nessuna decisione finanziaria.

- **Validazione fail-closed** prima di pubblicare: ``command_id`` (1-64 caratteri ``[A-Za-z0-9._:-]``), mercato,
  ``total_stake`` Decimal finito > 0, almeno due gambe con selezione intera > 0 non duplicata, quota Decimal finita
  > 1.0 e lato BACK/LAY ESPLICITO per ogni gamba (mai inventato). Campi fuori contratto => rifiuto (uno ``stake``
  per gamba non viaggia mai: il controller ricalcola gli stake dal totale).
- **Importi in Decimal**: un float si converte dalla sua forma decimale piu' corta (``core.stake_precision``), mai
  dall'espansione binaria; sul bus viaggiano come testo esatto.
- **Tracciabilita'**: il ``command_id`` attraversa il RiskMiddleware ed e' negli eventi del lotto
  (``DUTCHING_BATCH_APPROVED``; i rifiuti portano il payload). Lo stesso comando ripubblicato entro la finestra della
  middleware e' scartato come duplicato; oltre, l'identita' del lotto del controller lo blocca.
- ``simulation_mode`` lo decide il chiamante dal runtime (metadato: l'engine instrada per la modalita' del runtime).

Esito: ``{"ok", "command_id", "reason"}``; ``reason`` e' un codice (``INVALID_*``, ``UNKNOWN_FIELD``,
``DUPLICATE_SELECTION``, ``BUS_UNAVAILABLE``, ``PUBLISH_FAILED``). ``ok`` True vuol dire PUBBLICATO sulla corsia, non
ordine piazzato: l'esito del lotto arriva dagli eventi del controller.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from core import validators
from core.stake_precision import to_decimal

logger = logging.getLogger(__name__)

REQUEST_TOPIC = "REQ_PLACE_DUTCHING"
SOURCE = "DUTCHING_COMMAND"
_COMMAND_ID_RE = re.compile(r"[A-Za-z0-9._:\-]{1,64}")
_COMMAND_FIELDS = frozenset({"command_id", "market_id", "total_stake", "legs", "event_name", "event_id",
                             "market_name"})
_LEG_FIELDS = frozenset({"selection_id", "price", "side", "runner_name"})


@dataclass(frozen=True)
class DutchingLeg:
    selection_id: int
    price: Decimal
    side: str
    runner_name: str = ""


@dataclass(frozen=True)
class DutchingCommand:
    command_id: str
    market_id: str
    total_stake: Decimal
    legs: Tuple[DutchingLeg, ...]
    event_name: str = ""
    event_id: Optional[str] = None
    market_name: str = ""


def _decimal(value: Any, code: str) -> Decimal:
    if value is None:
        raise ValueError(code)
    try:
        return to_decimal(value)                 # bool, NaN, inf, testo non numerico => ValueError
    except (ValueError, TypeError, ArithmeticError) as exc:
        raise ValueError(code) from exc


def _leg(raw: Any) -> DutchingLeg:
    if not isinstance(raw, dict):
        raise ValueError("INVALID_LEGS")
    if set(raw) - _LEG_FIELDS:
        raise ValueError("UNKNOWN_FIELD")
    try:
        selection_id = validators.order_selection_id(raw.get("selection_id"))
    except ValueError as exc:
        raise ValueError("INVALID_SELECTION_ID") from exc
    price = _decimal(raw.get("price"), "INVALID_PRICE")
    if price <= 1:
        raise ValueError("INVALID_PRICE")
    side = validators.leg_side({"side": raw["side"]}) if "side" in raw else None
    if side is None:
        raise ValueError("INVALID_SIDE")                     # lato mai inventato
    if raw["side"] != side:
        raise ValueError("INVALID_SIDE")                     # gia' canonico: nessuna normalizzazione silenziosa
    runner = raw.get("runner_name", "")
    return DutchingLeg(selection_id, price, side, runner if isinstance(runner, str) else "")


def build_dutching_command(raw: Any) -> DutchingCommand:
    """Comando validato dal dict del chiamante; qualunque violazione => ValueError(<codice>)."""
    if not isinstance(raw, dict):
        raise ValueError("INVALID_COMMAND")
    if set(raw) - _COMMAND_FIELDS:
        raise ValueError("UNKNOWN_FIELD")
    command_id = raw.get("command_id")
    if not isinstance(command_id, str) or not _COMMAND_ID_RE.fullmatch(command_id):
        raise ValueError("INVALID_COMMAND_ID")
    market_id = validators.order_market_id(raw.get("market_id"))     # INVALID_MARKET_ID
    total_stake = _decimal(raw.get("total_stake"), "INVALID_TOTAL_STAKE")
    if total_stake <= 0:
        raise ValueError("INVALID_TOTAL_STAKE")
    legs_raw = raw.get("legs")
    if not isinstance(legs_raw, list) or len(legs_raw) < 2:
        raise ValueError("INVALID_LEGS")
    legs = tuple(_leg(item) for item in legs_raw)
    if len({leg.selection_id for leg in legs}) != len(legs):
        raise ValueError("DUPLICATE_SELECTION")
    event_id = raw.get("event_id")
    return DutchingCommand(
        command_id=command_id, market_id=market_id, total_stake=total_stake, legs=legs,
        event_name=raw.get("event_name") if isinstance(raw.get("event_name"), str) else "",
        event_id=str(event_id) if isinstance(event_id, (str, int)) and not isinstance(event_id, bool) else None,
        market_name=raw.get("market_name") if isinstance(raw.get("market_name"), str) else "",
    )


def to_request_payload(cmd: DutchingCommand, *, simulation_mode: bool) -> Dict[str, Any]:
    """Payload ``REQ_PLACE_DUTCHING`` (shape del RiskMiddleware: gambe in ``results``), importi come testo esatto."""
    return {
        "command_id": cmd.command_id,
        "market_id": cmd.market_id,
        "event_name": cmd.event_name,
        "event_id": cmd.event_id,
        "market_name": cmd.market_name,
        "total_stake": str(cmd.total_stake),
        "results": [{"selectionId": leg.selection_id, "price": str(leg.price), "side": leg.side,
                     "runnerName": leg.runner_name} for leg in cmd.legs],
        "simulation_mode": bool(simulation_mode),
        "source": SOURCE,
    }


def submit_dutching_command(bus: Any, raw: Any, *, simulation_mode: bool) -> Dict[str, Any]:
    """Valida e pubblica il comando sulla corsia dutching. Mai un'eccezione verso il chiamante."""
    command_id = raw.get("command_id") if isinstance(raw, dict) and isinstance(raw.get("command_id"), str) else None
    try:
        cmd = build_dutching_command(raw)
    except ValueError as exc:
        return {"ok": False, "command_id": command_id, "reason": str(exc)}
    if bus is None or not callable(getattr(bus, "publish", None)):
        return {"ok": False, "command_id": cmd.command_id, "reason": "BUS_UNAVAILABLE"}
    try:
        bus.publish(REQUEST_TOPIC, to_request_payload(cmd, simulation_mode=simulation_mode))
    except Exception:
        logger.exception("Comando Dutching %s non pubblicato", cmd.command_id)
        return {"ok": False, "command_id": cmd.command_id, "reason": "PUBLISH_FAILED"}
    logger.info("Comando Dutching %s pubblicato su %s", cmd.command_id, REQUEST_TOPIC)
    return {"ok": True, "command_id": cmd.command_id, "reason": ""}
