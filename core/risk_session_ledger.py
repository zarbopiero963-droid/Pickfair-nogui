"""Registro durevole append-only della sessione di rischio (owner M139, G1 + G2, PR #638).

Una sola catena di record nella tabella esistente ``audit_events`` (append-only: solo INSERT, mai UPDATE/DELETE),
riconoscibili da ``type == RECORD_TYPE``. Ogni record porta l'hash del precedente e il proprio (SHA-256 della forma
canonica): la catena si verifica alla lettura e una catena rotta o un record illeggibile rendono il registro
NON VERIFICABILE (il chiamante resta fail-closed). Ogni scrittura e' verificata rileggendo il record dal database.

Tipi di record (``kind``):
  - ``SESSION_CLOSED``  G2: sessione di rischio chiusa (id, apertura/chiusura, P&L di sessione per ambito, conti,
                        limite) - archivio senza eliminazione automatica;
  - ``LATE_SETTLEMENT`` G2: P&L tardivo di una sessione archiviata, una volta per chiave di settlement;
  - ``RESET_STARTED`` / ``RESET_COMPLETED`` / ``RESET_FAILED``  G1: reset confermato dall'operatore, con recupero
                        all'avvio di un reset iniziato e mai concluso.
"""
from __future__ import annotations

import hashlib
import json
import logging
import threading
from typing import Any, Optional

logger = logging.getLogger(__name__)

RECORD_TYPE = "PF_RISK_SESSION_LEDGER"
GENESIS = "0" * 64
KINDS = ("SESSION_CLOSED", "LATE_SETTLEMENT", "RESET_STARTED", "RESET_COMPLETED", "RESET_FAILED")


# M144 (CX-F3): una sola scrittura alla volta. `append` legge la coda della catena, scrive e rilegge sotto questo lock:
# due scrittori concorrenti non possono mai produrre due record con lo stesso `prev` (catena rotta per sempre). Lock
# FOGLIA: chi lo tiene non prende nessun altro lock (solo I/O sul database), quindi non entra in nessun ciclo d'attesa.
# Fra processi diversi la serializzazione la da' l'autorita' del runtime (#536: un solo processo scrive il database).
LOCK = threading.RLock()


class LedgerUnverifiable(Exception):
    """Registro illeggibile o catena di hash rotta: nessuna conclusione e' ammessa (fail-closed)."""


def _canonical(body: dict) -> str:
    return json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)


def _digest(body: dict) -> str:
    return hashlib.sha256(_canonical(body).encode("utf-8")).hexdigest()


def _rows(db: Any) -> list:
    if not hasattr(db, "_execute"):
        raise LedgerUnverifiable("database senza accesso al registro")
    try:
        rows = db._execute("SELECT id, event_json FROM audit_events WHERE event_json LIKE ? ORDER BY id ASC",
                           (f"%{RECORD_TYPE}%",), fetch=True, commit=False) or []
    except Exception as exc:
        raise LedgerUnverifiable(f"registro non leggibile: {exc}") from exc
    return [dict(r) for r in rows]


def load(db: Any) -> list:
    """Tutti i record della catena, in ordine, verificati (hash e collegamento). Solleva LedgerUnverifiable."""
    out, prev = [], GENESIS
    for row in _rows(db):
        try:
            event = json.loads(row.get("event_json") or "")
        except Exception as exc:
            raise LedgerUnverifiable(f"record {row.get('id')} illeggibile") from exc
        if not isinstance(event, dict) or event.get("type") != RECORD_TYPE:
            continue                                   # un altro evento che contiene la stringa: non e' del registro
        body = {k: v for k, v in event.items() if k != "hash"}
        if event.get("kind") not in KINDS or event.get("prev") != prev or event.get("hash") != _digest(body):
            raise LedgerUnverifiable(f"catena del registro rotta al record {row.get('id')}")
        out.append(event)
        prev = event["hash"]
    return out


def append(db: Any, kind: str, payload: dict) -> Optional[dict]:
    """Aggiunge un record in coda alla catena e lo RILEGGE: ritorna il record scritto, None se la scrittura o la
    rilettura falliscono (il chiamante non deve considerare avvenuta l'azione). Mai raise (eccetto le interruzioni del
    processo, ``BaseException``). Serializzato da ``LOCK`` (M144, CX-F3)."""
    if kind not in KINDS:
        return None
    with LOCK:
        try:
            chain = load(db)
            prev = chain[-1]["hash"] if chain else GENESIS
            body = {"type": RECORD_TYPE, "kind": kind, "seq": len(chain) + 1, "prev": prev, "payload": dict(payload)}
            event = dict(body, hash=_digest(body))
            result = db.insert_audit_event(event) if hasattr(db, "insert_audit_event") else None
            if result is False:
                return None
            after = load(db)
            if not after or after[-1].get("hash") != event["hash"]:
                logger.critical("registro della sessione di rischio: record %s non riletto dopo la scrittura", kind)
                return None
            return event
        except Exception:
            logger.exception("registro della sessione di rischio: scrittura %s fallita", kind)
            return None


def archive_index(chain: list) -> dict:
    """G2: sessioni chiuse archiviate {id: {"record": ..., "delta": P&L aggiornato coi tardivi, "late_keys": set}}."""
    index: dict = {}
    for event in chain:
        payload = event.get("payload") or {}
        if event.get("kind") == "SESSION_CLOSED":
            sid = str(payload.get("id") or "")
            if sid and sid not in index:
                delta = payload.get("delta") or {}
                # M144 (CX-F1): i tardivi gia' compresi nel P&L della sessione chiusa (entrati mentre era nella finestra)
                # sono elencati nel record: un LATE_SETTLEMENT con la stessa chiave non si somma una seconda volta
                index[sid] = {"record": dict(payload), "late_keys": {str(k) for k in (payload.get("late_keys") or [])},
                              "delta": {"desk": float(delta.get("desk") or 0.0),
                                        "scopes": {str(k): float(v) for k, v in (delta.get("scopes") or {}).items()},
                                        "unscoped": float(delta.get("unscoped") or 0.0)}}
        elif event.get("kind") == "LATE_SETTLEMENT":
            sid, key = str(payload.get("session_id") or ""), str(payload.get("settlement_key") or "")
            entry = index.get(sid)
            if entry is None or not key or key in entry["late_keys"]:
                continue                               # idempotenza: una chiave conta una volta sola
            add = payload.get("delta") or {}
            entry["late_keys"].add(key)
            entry["delta"]["desk"] += float(add.get("desk") or 0.0)
            entry["delta"]["unscoped"] += float(add.get("unscoped") or 0.0)
            for k, v in (add.get("scopes") or {}).items():
                entry["delta"]["scopes"][str(k)] = entry["delta"]["scopes"].get(str(k), 0.0) + float(v)
    return index


def reset_status(chain: list) -> dict:
    """G1: {operation_id: ultimo stato (RESET_STARTED/COMPLETED/FAILED) e record}."""
    out: dict = {}
    for event in chain:
        if str(event.get("kind") or "").startswith("RESET_"):
            payload = event.get("payload") or {}
            op = str(payload.get("operation_id") or "")
            if op:
                out[op] = {"kind": event["kind"], "payload": dict(payload)}
    return out
