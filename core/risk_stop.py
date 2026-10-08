"""RISK_STOP (#461 PR28-c, DEC-426-P37/P38), distinto da EMERGENCY.

Contratto (docs/mcp_operational_contract.md): STOP/RISK_STOP blocca nuovi
ordini, cancella gli unmatched pertinenti e tenta il cashout delle posizioni
pertinenti via authority Pickfair; PAUSE blocca solo i nuovi ordini;
EMERGENCY resta la barriera massima; RESUME solo dopo ricontrollo completo.

- Blocca le nuove entrate (segnali, auto-next, resume), NON il cashout.
- Cancel + cashout con un CASHOUT_ALL interno al CashoutRouter: solo ordini
  del bot (gli ordini manuali non si toccano), gate propri del cashout, esiti
  bloccati/incerti dichiarati dal router (mai chiusura presunta).
- Persistito: un riavvio non lo azzera; lo toglie solo reset_risk_stop()
  dopo il ricontrollo completo. Stato illeggibile => resta attivo.
- Nessun importo fisso: le soglie (`max_session_loss`, `max_exposure_stop`)
  sono limiti owner configurabili senza default (#426, decisione 06/10).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from core import daily_loss_store, validators
from core.atomic_io import atomic_write_text

logger = logging.getLogger(__name__)

RISK_STOP_STATE_KEY = "risk_stop_state"
_TRIGGERS = ("exposure_stop_reached", "session_loss_breached")


def exposure_stop_reason(limit_raw: Any, total_exposure: Any) -> str:
    """Limite assente => ""; illeggibile o <= 0 => blocca; esposizione
    complessiva (ordine in corso incluso) che raggiunge il limite => stop."""
    if limit_raw is None:
        return ""
    limit = validators.finite_number(limit_raw)
    if limit is None or limit <= 0.0:
        return "max_exposure_stop_non_valido"
    exposure = validators.finite_number(total_exposure)
    if exposure is None:
        return "esposizione_non_valida"
    if validators.reaches_limit(exposure, limit):
        return f"exposure_stop_reached:exposure={exposure:.2f}:limit={limit}"
    return ""


def is_stop_trigger(reason: str) -> bool:
    """Solo un limite raggiunto fa scattare il RISK_STOP; un limite
    configurato male blocca le entrate ma non chiude le posizioni."""
    return str(reason or "").startswith(_TRIGGERS)


def cashout_all_signal(reason: str) -> dict:
    return {"signal_type": "CASHOUT_ALL", "source": "RISK_STOP", "reason": str(reason)}


def _marker_path(db: Any) -> str:
    """Marker accanto al db, stesso schema di PR28-b (core/daily_loss_store)."""
    path = daily_loss_store._marker_path(db)
    return path[: -len(".daily_loss_pending.json")] + ".risk_stop_pending.json" if path else ""


_UNREADABLE = object()


def _record(raw: Any) -> Any:
    """Copia salvata -> {active, reason, seq} | None (assente) | _UNREADABLE."""
    if raw is None or raw == "":
        return None
    try:
        data = json.loads(raw)
        reason, seq = data["reason"], data.get("seq")
        active = data.get("active", bool(reason))
        if not isinstance(reason, str) or active is not bool(reason) or (  # incoerente => illeggibile
                seq is not None and (type(seq) is not int or seq < 0)):
            raise ValueError("record non valido")
        return {"active": active, "reason": reason, "seq": seq}
    except Exception:
        return _UNREADABLE


def _copies(db: Any) -> tuple:
    try:
        settings = db.get_settings() if hasattr(db, "get_settings") else {}
        from_db = _record(settings.get(RISK_STOP_STATE_KEY)) if isinstance(settings, dict) else None
    except Exception:
        from_db = _UNREADABLE
    try:
        marker, from_marker = _marker_path(db), None
        if marker and os.path.exists(marker):
            with open(marker, encoding="utf-8") as fh:
                from_marker = _record(fh.read())
    except Exception:
        from_marker = _UNREADABLE
    return from_db, from_marker


def _reconcile(a: Any, b: Any) -> str:
    """Vince la copia verificabile con seq maggiore; seq uguale/ignota o copia
    illeggibile => risultato conservativo (stop attivo)."""
    readable = [r for r in (a, b) if isinstance(r, dict)]
    if len(readable) == 2 and None not in (a["seq"], b["seq"]) and a["seq"] != b["seq"]:
        readable = [max(readable, key=lambda r: r["seq"])]
    active = [r for r in readable if r["active"]]
    if active:
        return active[0]["reason"] or "risk_stop"
    return "risk_stop_state_illeggibile" if _UNREADABLE in (a, b) else ""


def _version_unknown(copies: tuple) -> bool:
    """True se almeno una copia esiste ma la sua versione non e' confrontabile."""
    return any(
        r is _UNREADABLE or (isinstance(r, dict) and r["seq"] is None)
        for r in copies
    )


def persist_risk_stop(db: Any, reason: str) -> bool:
    """Persiste una transizione RISK_STOP senza permettere regressioni di versione.

    Se durante l'ATTIVAZIONE una copia e' illeggibile/ha seq ignota, la nuova
    attivazione viene scritta con seq=None: _reconcile la tratta come barriera
    conservativa e quindi non puo' perdere contro un vecchio reset con seq
    numerica quando lo store torna leggibile.

    Un RESET usa invece la prossima seq numerica conosciuta, ma viene considerato
    riuscito soltanto se una rilettura di db+marker riconcilia davvero a stato
    inattivo. Se un marker attivo a versione ignota impedisce la prova, si tenta
    di sostituire anche il marker con la stessa versione di reset; se non si
    riesce, il reset fallisce e il runtime resta bloccato.
    """
    if not hasattr(db, "save_settings"):
        return False

    before = _copies(db)
    seqs = [r["seq"] for r in before if isinstance(r, dict) and r["seq"] is not None]
    active = bool(reason)
    seq = None if active and _version_unknown(before) else max(seqs, default=0) + 1
    payload = json.dumps({
        "active": active,
        "reason": str(reason or ""),
        "seq": seq,
        "at": datetime.utcnow().isoformat(),
    })
    marker = _marker_path(db)

    db_written = False
    try:
        db.save_settings({RISK_STOP_STATE_KEY: payload})
        db_written = True
    except Exception:
        logger.critical("risk-stop: stato non salvato su db, uso il marker", exc_info=True)

    # Un'attivazione e' fail-closed per costruzione: se almeno una copia e'
    # stata scritta, una seq ignota resta conservativa al restore.
    if active and db_written:
        return True

    # Per un reset non basta che una write abbia risposto OK: occorre provare
    # che l'insieme db+marker non contenga ancora una copia attiva/ignota che
    # vincerebbe al restart.
    if not active and db_written and _reconcile(*_copies(db)) == "":
        return True

    marker_written = False
    try:
        if marker:
            atomic_write_text(marker, payload)
            marker_written = True
    except Exception:
        logger.critical("risk-stop: marker non scrivibile", exc_info=True)

    if active:
        if marker_written:
            return True
        if not db_written:
            logger.critical("risk-stop: ne' db ne' marker scrivibili (KNOWN_LIMITATION)")
        return db_written

    if not (db_written or marker_written):
        return False

    # Reset riuscito soltanto se la rilettura dimostra che nessuna copia
    # conservativa/attiva puo' riaprire il RISK_STOP al boot.
    return _reconcile(*_copies(db)) == ""


def restore_risk_stop(db: Any) -> str:
    """Motivo del RISK_STOP da db + marker ("" se non attivo), vedi `_reconcile`."""
    reason = _reconcile(*_copies(db))
    if reason == "risk_stop_state_illeggibile":
        logger.error("risk-stop: stato salvato illeggibile, fail-closed")
    return reason


def reset_blocker(rc: Any, at_start: bool) -> str:
    """Ricontrollo completo prima di riaprire ("" = pulito; chiamato sotto il lock).

    start() non riapre MAI da solo: i tavoli sono in memoria (esposizione 0 dopo
    un riavvio) e la perdita di sessione riparte. Serve un ``reset_risk_stop()``
    esplicito (fail-closed); intanto start() ritenta il CASHOUT_ALL.
    """
    reason = str(getattr(rc, "_risk_stop_reason", "") or "")
    if not reason:
        return ""
    if at_start:
        return "risk_stop_reset_esplicito_richiesto"
    if rc._daily_loss_entry_blocked():
        return "emergency_stop_active"
    if rc._monitor_daily_loss_breach(source="RISK_STOP_RESET").get("breached"):
        return "daily_loss_breached"
    return rc._limits_block_reason() or rc._exposure_stop(False)
