"""F-04 (#586) — chiusura del SOLO tavolo che ha raggiunto Stop Loss / Take Profit.

Consuma gli eventi di F-02 (``SL_TRIGGERED`` / ``TP_TRIGGERED``) con un contratto tipizzato e versionato
(``SL_TP_CLOSE_CONTRACT_VERSION``). Questo modulo e' puro (nessun I/O): validazione del payload, verifica
dell'identita' del tavolo, raccolta dei ``bet_id`` della posizione e stato persistente della chiusura. Il cablaggio
(claim persistente, instradamento nel percorso cashout con tutti i gate) e' in ``RuntimeController``.

Regole (fail-closed: ogni dubbio => nessun ordine, motivo esplicito):
- la posizione si identifica per tavolo + ``customer_ref`` + mercato + selezione + lato + ``bet_id`` dei suoi
  ordini, MAI per sola partita: gli altri tavoli sullo stesso evento restano intatti;
- un ordine del tavolo in stato incerto (INFLIGHT, AMBIGUOUS, inviato senza ``bet_id``) => niente chiusura:
  prima la riconciliazione;
- una chiusura per posizione (``risk.sl_tp_close.<customer_ref>``), rivendicata in modo persistente PRIMA di
  instradare: un riavvio o un secondo scatto (SL e TP, o lo stesso evento ripetuto) non producono un secondo
  cashout. Si ritenta solo da ``NOT_EXECUTED`` (nessuna richiesta d'hedge pubblicata).
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core import validators

SL_TP_CLOSE_CONTRACT_VERSION = 1

SL_TRIGGERED = "SL_TRIGGERED"
TP_TRIGGERED = "TP_TRIGGERED"
TRIGGER_REASONS = {SL_TRIGGERED: "STOP_LOSS_REACHED", TP_TRIGGERED: "TAKE_PROFIT_REACHED"}
TRIGGER_SOURCE = "AUTO_MONITOR"

# Esiti pubblicati sul bus (contratto in sola lettura per Control API / MCP).
SL_TP_CLOSE_ROUTED = "SL_TP_CLOSE_ROUTED"            # richiesta d'hedge pubblicata nella catena cashout
SL_TP_CLOSE_NOT_EXECUTED = "SL_TP_CLOSE_NOT_EXECUTED"  # nessun hedge pubblicato (gate, mercato sospeso, ...)
SL_TP_CLOSE_REJECTED = "SL_TP_CLOSE_REJECTED"        # evento o identita' non verificabili: nessun ordine
SL_TP_CLOSE_SKIPPED = "SL_TP_CLOSE_SKIPPED"          # chiusura gia' rivendicata per questa posizione
SL_TP_CLOSE_UNCERTAIN = "SL_TP_CLOSE_UNCERTAIN"      # esito dell'instradamento ignoto: riconciliazione, mai reinvio

# Stati persistiti.
STATE_CLAIMED = "CLAIMED"
STATE_ROUTED = "ROUTED"
STATE_NOT_EXECUTED = "NOT_EXECUTED"
STATE_UNCERTAIN = "UNCERTAIN"
REROUTABLE_STATES = frozenset({STATE_NOT_EXECUTED})

CLOSE_KEY_PREFIX = "risk.sl_tp_close."

# Righe `orders` senza bet: certamente nessuna scommessa sul mercato (le altre senza bet_id sono incerte).
_NO_BET_STATUSES = frozenset({"FAILED", "DENIED", "DUPLICATE_BLOCKED"})
_UNCERTAIN_STATUSES = frozenset({"INFLIGHT", "AMBIGUOUS"})


@dataclass(frozen=True)
class SlTpTrigger:
    topic: str
    reason: str
    table_id: int
    market_id: str
    selection_id: str
    side: str
    customer_ref: str
    pnl: float

    @property
    def event_id(self) -> str:
        return f"sltp:v{SL_TP_CLOSE_CONTRACT_VERSION}:{self.customer_ref}:{self.reason}"

    @property
    def close_key(self) -> str:
        return CLOSE_KEY_PREFIX + self.customer_ref

    def as_payload(self) -> Dict[str, Any]:
        return {"contract_version": SL_TP_CLOSE_CONTRACT_VERSION, "event_id": self.event_id,
                "trigger": self.topic, "reason": self.reason, "table_id": self.table_id,
                "market_id": self.market_id, "selection_id": self.selection_id, "side": self.side,
                "customer_ref": self.customer_ref}


def parse_trigger(topic: str, payload: Any) -> Tuple[Optional[SlTpTrigger], str]:
    """Evento F-02 v1 => ``SlTpTrigger``; ``(None, motivo)`` se un campo manca o non e' coerente."""
    if topic not in TRIGGER_REASONS:
        return None, "TOPIC_UNKNOWN"
    if not isinstance(payload, dict):
        return None, "PAYLOAD_NOT_DICT"
    if payload.get("reason") != TRIGGER_REASONS[topic]:
        return None, "REASON_MISMATCH"
    if payload.get("source") != TRIGGER_SOURCE:
        return None, "SOURCE_INVALID"
    table_id = payload.get("table_id")
    if not isinstance(table_id, int) or isinstance(table_id, bool) or table_id <= 0:
        return None, "TABLE_ID_INVALID"
    try:
        market_id = validators.order_market_id(payload.get("market_id"))
        selection_id = validators.order_selection_id(payload.get("selection_id"))
    except ValueError:
        return None, "MARKET_SELECTION_INVALID"
    side = payload.get("side")
    if side not in ("BACK", "LAY"):
        return None, "SIDE_INVALID"
    ref = payload.get("customer_ref")
    if not isinstance(ref, str) or not ref.strip():
        return None, "CUSTOMER_REF_MISSING"
    pnl = payload.get("pnl")
    if not isinstance(pnl, (int, float)) or isinstance(pnl, bool) or not math.isfinite(float(pnl)):
        return None, "PNL_INVALID"
    return SlTpTrigger(topic=topic, reason=TRIGGER_REASONS[topic], table_id=table_id, market_id=str(market_id),
                       selection_id=str(selection_id), side=side, customer_ref=ref.strip(), pnl=float(pnl)), ""


def table_identity_reason(trigger: SlTpTrigger, table: Any) -> str:
    """'' se il tavolo attivo e' quello dell'evento (stesso intento, mercato, selezione e lato), altrimenti il
    motivo. Un evento arrivato dopo che il tavolo e' stato liberato o riassegnato non chiude niente."""
    if table is None:
        return "TABLE_NOT_FOUND"
    meta = getattr(table, "meta", None)
    meta = meta if isinstance(meta, dict) else {}
    if str(meta.get("customer_ref") or "") != trigger.customer_ref:
        return "TABLE_REF_MISMATCH"
    if str(getattr(table, "market_id", "") or "") != trigger.market_id:
        return "TABLE_MARKET_MISMATCH"
    if str(getattr(table, "selection_id", "") or "") != trigger.selection_id:
        return "TABLE_SELECTION_MISMATCH"
    if str(meta.get("bet_type") or "").upper() != trigger.side:
        return "TABLE_SIDE_MISMATCH"
    return ""


def position_bet_ids(trigger: SlTpTrigger, rows: Iterable[Dict[str, Any]]) -> Tuple[Optional[List[str]], str]:
    """``bet_id`` degli ordini dell'intento del tavolo; ``(None, motivo)`` se l'identita' non e' certa."""
    bet_ids: List[str] = []
    for row in rows or []:
        status = str(row.get("status") or "").upper()
        bet_id = str(row.get("bet_id") or "").strip()
        if status in _UNCERTAIN_STATUSES:
            return None, f"ORDER_STATE_UNCERTAIN:{status}"
        if not bet_id:
            if status in _NO_BET_STATUSES:
                continue
            return None, f"ORDER_STATE_UNCERTAIN:{status or 'UNKNOWN'}:NO_BET_ID"
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        if str(payload.get("market_id") or "") != trigger.market_id \
                or str(payload.get("selection_id") or "") != trigger.selection_id \
                or str(payload.get("bet_type") or "").upper() != trigger.side:
            return None, "ORDER_IDENTITY_MISMATCH"
        if bet_id not in bet_ids:
            bet_ids.append(bet_id)
    if not bet_ids:
        return None, "NO_PLACED_ORDERS"
    return bet_ids, ""


def state_record(trigger: SlTpTrigger, state: str, *, bet_ids: List[str], reason: str = "",
                 at: float = 0.0, request_ids: Iterable[str] = ()) -> str:
    """Record persistito della chiusura. ``cashout_request_ids``: le richieste d'hedge pubblicate per questa chiusura,
    lo stesso id che il CashoutExecutor scrive nella riga `orders` dell'hedge (legame tavolo -> hedge verificabile)."""
    return json.dumps({**trigger.as_payload(), "state": state, "bet_ids": list(bet_ids), "detail": reason,
                       "at": at, "cashout_request_ids": list(request_ids)}, sort_keys=True)


def read_state(raw: Any) -> Optional[str]:
    """Stato persistito o ``None`` se illeggibile (il chiamante lo tratta come non ritentabile)."""
    if not isinstance(raw, str) or not raw:
        return None
    try:
        data = json.loads(raw)
    except ValueError:
        return None
    state = data.get("state") if isinstance(data, dict) else None
    return state if isinstance(state, str) and state else None


def route_outcome(box: Dict[str, Any]) -> Tuple[str, str]:
    """Esito dell'instradamento => ``(stato, dettaglio)``. Senza esito registrato => UNCERTAIN (mai reinvio)."""
    if "rejected" in box:
        return STATE_NOT_EXECUTED, f"GATE_REJECTED:{box['rejected']}"
    if "error" in box:
        return STATE_UNCERTAIN, f"ROUTE_ERROR:{box['error']}"
    result = box.get("result")
    if not isinstance(result, dict):
        return STATE_UNCERTAIN, "ROUTE_RESULT_MISSING"
    published = result.get("published")
    if not isinstance(published, int) or isinstance(published, bool) or published < 0:
        return STATE_UNCERTAIN, "ROUTE_RESULT_INVALID"
    if published > 0:
        return STATE_ROUTED, ""
    return STATE_NOT_EXECUTED, str(result.get("reason") or "NO_HEDGE_PUBLISHED")
