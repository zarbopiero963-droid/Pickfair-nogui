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
from datetime import datetime
from typing import Any

from core import validators

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


def persist_risk_stop(db: Any, reason: str) -> bool:
    """Salva lo stato ("" = non attivo). Mai raise; False se non salvato."""
    if not hasattr(db, "save_settings"):
        return False
    try:
        db.save_settings({RISK_STOP_STATE_KEY: json.dumps(
            {"reason": str(reason or ""), "at": datetime.utcnow().isoformat()})})
        return True
    except Exception:
        logger.critical("risk-stop: stato non salvato", exc_info=True)
        return False


def restore_risk_stop(db: Any) -> str:
    """Motivo del RISK_STOP salvato ("" se non attivo); illeggibile => attivo."""
    try:
        settings = db.get_settings() if hasattr(db, "get_settings") else {}
        if not isinstance(settings, dict):
            return ""
        raw = settings.get(RISK_STOP_STATE_KEY)
        if raw is None or raw == "":
            return ""
        return str(json.loads(raw)["reason"] or "")
    except Exception:
        logger.exception("risk-stop: stato salvato illeggibile, fail-closed")
        return "risk_stop_state_illeggibile"
