"""Persistenza durevole della perdita giornaliera (F4, #461 PR28-b).

Ogni record (db e marker `<db>.daily_loss_pending.json`, scritto quando il db
non si salva) porta il giorno UTC e un numero di sequenza monotono `seq`.
Al riavvio si leggono ENTRAMBI: vince la `seq` piu' alta; a `seq` uguale o
ignota vince il record piu' conservativo (perdita maggiore, breach se uno dei
due lo e'); un breach del giorno non si annulla mai. Un marker rimasto
(rimozione fallita, crash tra salvataggio e rimozione) ha `seq` minore del db
e non puo' ridurre la perdita.
Stato illeggibile, giorno non valido o futuro => breach fino al nuovo giorno.
KNOWN_LIMITATION (dichiarata): se NE' db NE' marker sono scrivibili il blocco
resta in RAM per la sessione; un riavvio in quella condizione non puo'
ricostruire le perdite non salvate.
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
SEQ_KEY = "persist_seq"
# Esito di persist: record scritto con la sua `seq`, oppure {"persist_failed":
# True, "seq": n}: nuovo rischio rifiutato finche' un salvataggio non riesce.
PERSIST_FAILED_KEY = "persist_failed"
_FIELDS = ("day_utc", "intraday_realized_pnl", "breached", "breached_at")


def daily_loss_record(state: dict) -> dict:
    return {k: state.get(k) for k in _FIELDS}


def _seq(value: Any) -> Optional[int]:
    return value if type(value) is int and value >= 0 else None


def restored_daily_loss(raw: Any, *, today_utc: str, realized_pnl: float) -> Optional[dict]:
    """Campi del giorno (con `seq`, None se ignota); None se assente o di un
    giorno passato; ValueError se illeggibile, non finito o futuro."""
    if not raw:
        return None
    try:
        record = json.loads(raw)
        day = date.fromisoformat(record["day_utc"])
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
    return _fields(intraday, record.get("breached") is True, record.get("breached_at"),
                   realized_pnl, _seq(record.get("seq")))


def _fields(intraday: float, breached: bool, breached_at: Any, realized_pnl: float,
            seq: Optional[int]) -> dict:
    fields = {
        "realized_pnl_day_baseline": float(realized_pnl) - intraday,
        "realized_pnl": float(realized_pnl),
        "intraday_realized_pnl": intraday,
        "daily_loss_amount": max(0.0, -intraday),
        "breached": breached,
        "breached_at": str(breached_at or "") if breached else "",
        SEQ_KEY: seq,
    }
    if breached:
        fields.update(last_status="DAILY_LOSS_BREACHED",
                      reason="daily_loss_breach_persistent_until_day_rollover")
    return fields


def reconcile(a: Optional[dict], b: Optional[dict], realized_pnl: float) -> dict:
    """`seq` piu' alta, altrimenti la perdita maggiore; breach se uno dei due."""
    if not a or not b:
        return a or b or {}
    sa, sb = a[SEQ_KEY], b[SEQ_KEY]
    breached = [f for f in (a, b) if f["breached"]]  # breach del giorno: mai annullato
    seqs = [s for s in (sa, sb) if s is not None]
    if sa is not None and sb is not None and sa != sb:
        intraday = (a if sa > sb else b)["intraday_realized_pnl"]
    else:
        intraday = min(a["intraday_realized_pnl"], b["intraday_realized_pnl"])
    return _fields(intraday, bool(breached), breached[0]["breached_at"] if breached else "",
                   realized_pnl, max(seqs) if seqs else None)


def _marker_path(db: Any) -> str:
    path = getattr(db, "db_path", None)
    if not isinstance(path, str) or path in ("", ":memory:"):
        return ""
    return path + ".daily_loss_pending.json"


def persist_failed(last: Any) -> bool:
    return isinstance(last, dict) and last.get(PERSIST_FAILED_KEY) is True


def persist_daily_loss(db: Any, state: dict, last: Optional[dict]) -> Optional[dict]:
    """Salva se cambiato con `seq` successiva (anche il marker la consuma);
    ritorna il record scritto (con `seq`) o l'esito fallito. Mai raise."""
    record = daily_loss_record(state)
    last_seq = _seq((last or {}).get("seq"))
    if not hasattr(db, "save_settings") or (
            not persist_failed(last) and last is not None and daily_loss_record(last) == record):
        return last
    seq = max(last_seq or 0, _seq(state.get(SEQ_KEY)) or 0) + 1
    payload = json.dumps(dict(record, seq=seq))
    marker = _marker_path(db)
    try:
        db.save_settings({DAILY_LOSS_STATE_KEY: payload})
    except Exception:
        logger.exception("daily-loss: persist failed")
        try:
            if marker:
                atomic_write_text(marker, payload)
        except Exception:
            logger.critical("daily-loss: neanche il marker e' scrivibile (KNOWN_LIMITATION)", exc_info=True)
        return {PERSIST_FAILED_KEY: True, "seq": seq}
    try:
        if marker and os.path.exists(marker):
            os.remove(marker)
    except OSError:
        logger.warning("daily-loss: marker non rimosso (innocuo: seq minore)", exc_info=True)
    return dict(record, seq=seq)


def restore_daily_loss(db: Any, *, today_utc: str, realized_pnl: float) -> dict:
    """Campi per ricostruire il giorno da db e marker (vedi `reconcile`)."""
    marker = _marker_path(db)
    try:
        settings = db.get_settings() if hasattr(db, "get_settings") else {}
        from_db = restored_daily_loss((settings or {}).get(DAILY_LOSS_STATE_KEY),
                                      today_utc=today_utc, realized_pnl=realized_pnl)
        from_marker = None
        if marker and os.path.exists(marker):
            with open(marker, encoding="utf-8") as fh:
                from_marker = restored_daily_loss(fh.read(), today_utc=today_utc, realized_pnl=realized_pnl)
    except Exception:
        logger.exception("daily-loss: stato persistito illeggibile, fail-closed")
        return {"breached": True, "breached_at": datetime.utcnow().isoformat(),
                "last_status": "DAILY_LOSS_BREACHED", "reason": "daily_loss_state_unreadable"}
    return reconcile(from_db, from_marker, realized_pnl)
