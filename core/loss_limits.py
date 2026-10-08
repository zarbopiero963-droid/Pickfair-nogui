"""Limiti di perdita owner del runtime (#461 PR28-b, PKG-P24-B).

F4: la perdita realizzata del giorno sopravvive al riavvio. Si salva la
perdita INTRADAY (non `realized_pnl`, che riparte da 0 a ogni avvio); stesso
giorno UTC => si ricostruisce prima di qualunque nuovo rischio; nuovo giorno
=> zero; stato illeggibile => breach (fail-closed).

P37: stop perdita di sessione (`max_session_loss`, euro, dall'ultimo start()).
E' un limite di rischio owner configurabile, senza default inventato, NON un
parametro del cashout (#426, decisione 06/10): blocca le nuove entrate e non
tocca uscite/cashout.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from typing import Any, Optional

from core import validators
from core.atomic_io import atomic_write_text

logger = logging.getLogger(__name__)

DAILY_LOSS_STATE_KEY = "daily_loss_state"
# Ultimo salvataggio fallito: il runtime rifiuta nuovo rischio finche' un
# salvataggio non riesce (un riavvio ripristinerebbe un valore vecchio).
PERSIST_FAILED: dict = {"persist_failed": True}
_FIELDS = ("day_utc", "intraday_realized_pnl", "breached", "breached_at")


def daily_loss_record(state: dict) -> dict:
    """Parte persistita dello stato del monitor daily-loss."""
    return {k: state.get(k) for k in _FIELDS}


def encode_daily_loss(state: dict) -> str:
    return json.dumps(daily_loss_record(state))


def restored_daily_loss(raw: Any, *, today_utc: str, realized_pnl: float) -> Optional[dict]:
    """Campi da applicare allo stato del monitor per ricostruire il giorno.

    `None` => niente da ripristinare (assente o giorno diverso). Solleva
    ValueError se lo stato salvato e' illeggibile: il chiamante va in breach."""
    if not raw:
        return None
    try:
        record = json.loads(raw)
        day = date.fromisoformat(record["day_utc"])  # None/testo/numero => errore
        intraday = validators.finite_number(record["intraday_realized_pnl"])
    except Exception as exc:
        raise ValueError("daily_loss_state illeggibile") from exc
    if intraday is None:
        raise ValueError("intraday_realized_pnl non valido")
    today = date.fromisoformat(today_utc)
    if day > today:
        raise ValueError("daily_loss_state di un giorno futuro (orologio indietro)")
    if day < today:
        return None
    breached = record.get("breached") is True
    fields = {
        "realized_pnl_day_baseline": float(realized_pnl) - intraday,
        "realized_pnl": float(realized_pnl),
        "intraday_realized_pnl": intraday,
        "daily_loss_amount": max(0.0, -intraday),
        "breached": breached,
        "breached_at": str(record.get("breached_at") or "") if breached else "",
    }
    if breached:
        fields.update(last_status="DAILY_LOSS_BREACHED",
                      reason="daily_loss_breach_persistent_until_day_rollover")
    return fields


def _marker_path(db: Any) -> str:
    """File accanto al db: registra lo stato quando il db non si scrive."""
    path = str(getattr(db, "db_path", "") or "")
    return "" if path in ("", ":memory:") else path + ".daily_loss_pending.json"


def persist_daily_loss(db: Any, state: dict, last: Optional[dict]) -> Optional[dict]:
    """Salva lo stato se cambiato; ritorna l'ultimo record scritto, oppure
    PERSIST_FAILED se il salvataggio fallisce (mai raise). In quel caso lo
    stato va nel file marker, letto al riavvio prima del db."""
    record = daily_loss_record(state)
    if record == last or not hasattr(db, "save_settings"):
        return last
    marker = _marker_path(db)
    try:
        db.save_settings({DAILY_LOSS_STATE_KEY: encode_daily_loss(state)})
    except Exception:
        logger.exception("daily-loss: persist failed")
        try:
            if marker:
                atomic_write_text(marker, encode_daily_loss(state))
        except Exception:
            logger.critical("daily-loss: neanche il marker e' scrivibile", exc_info=True)
        return PERSIST_FAILED
    try:
        if marker and os.path.exists(marker):
            os.remove(marker)
    except OSError:
        logger.exception("daily-loss: marker non rimosso")
    return record


def restore_daily_loss(db: Any, *, today_utc: str, realized_pnl: float) -> tuple:
    """(campi per ricostruire il giorno, ultimo record salvato).

    Marker di un salvataggio fallito (di oggi) => si riparte da lui e si resta
    in PERSIST_FAILED finche' un salvataggio non riesce. Lettura fallita o
    stato illeggibile => breach fail-closed fino al nuovo giorno."""
    marker = _marker_path(db)
    try:
        if marker and os.path.exists(marker):
            with open(marker, encoding="utf-8") as fh:
                fields = restored_daily_loss(fh.read(), today_utc=today_utc, realized_pnl=realized_pnl)
            if fields is not None:
                return fields, PERSIST_FAILED
        settings = db.get_settings() if hasattr(db, "get_settings") else {}
        fields = restored_daily_loss((settings or {}).get(DAILY_LOSS_STATE_KEY),
                                     today_utc=today_utc, realized_pnl=realized_pnl)
    except Exception:
        logger.exception("daily-loss: stato persistito illeggibile, fail-closed")
        return {"breached": True, "breached_at": datetime.utcnow().isoformat(),
                "last_status": "DAILY_LOSS_BREACHED", "reason": "daily_loss_state_unreadable"}, None
    return fields or {}, None


def shift_day_baseline(state: dict, shift: float) -> None:
    """Il reset del ciclo recovery azzera `realized_pnl` del desk: spostare le
    baseline dello stesso importo lascia invariata la perdita del giorno."""
    for key in ("realized_pnl_day_baseline", "realized_pnl"):
        if state.get(key) is not None:
            state[key] = float(state[key]) - float(shift)


def session_loss_reason(limit_raw: Any, baseline: Optional[float], realized_pnl: float) -> str:
    """Motivo del blocco per la perdita di sessione, oppure "".

    Limite assente => nessun limite; illeggibile o <= 0 => blocca;
    perdita dall'ultimo start() che raggiunge il limite (con tolleranza) => blocca."""
    if limit_raw is None:
        return ""
    limit = validators.finite_number(limit_raw)
    if limit is None or limit <= 0.0:
        return "max_session_loss_non_valido"
    loss = 0.0 if baseline is None else max(0.0, float(baseline) - float(realized_pnl))
    if validators.reaches_limit(loss, limit):
        return f"session_loss_breached:loss={loss:.2f}:limit={limit}"
    return ""
