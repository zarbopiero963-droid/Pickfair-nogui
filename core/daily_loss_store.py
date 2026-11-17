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

PR-04a (PF-OWNER-PR04A-LIVE-GLOBAL-SIM-LOCAL-20261116): formato v2 per ambito
finanziario (`core/financial_scope.py`): ``{"version": 2, "scopes": {ambito:
{giorno, intraday, breach}}, "legacy": {...}|null, "seq": n}``. Un record v1
(senza ambito) non e' attribuibile a nessun conto: diventa QUARANTENA
``legacy``, applicata in modo conservativo (solo perdite, breach compreso) a
ogni ambito fino al cambio di giorno; mai attribuita in automatico a un conto.
Fra db e marker: `seq` piu' alta per ambito, altrimenti la perdita maggiore;
breach se uno dei due lo e'; nessun ambito di uno dei due si perde.
"""
from __future__ import annotations

import json
import logging
import math
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
    breached = [f for f in (a, b) if f["breached"]]
    seqs = [s for s in (sa, sb) if s is not None]
    if sa is not None and sb is not None and sa != sb:
        intraday = (a if sa > sb else b)["intraday_realized_pnl"]
    else:
        intraday = min(a["intraday_realized_pnl"], b["intraday_realized_pnl"])
    return _fields(intraday, bool(breached), breached[0]["breached_at"] if breached else "",
                   realized_pnl, max(seqs) if seqs else None)


def _marker_path(db: Any) -> str:
    try:
        path = os.fspath(getattr(db, "db_path", None))  # str o os.PathLike
    except TypeError:
        return ""
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
        _save_verified(db, payload)
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


# ----------------------------------------------------------------------------------------------- PR-04a, formato v2
BOOK_VERSION = 2


def _parse_fields(record: Any, *, today: date) -> Optional[tuple]:
    """(intraday, breached, breached_at) del giorno; None se di un giorno passato; ValueError se illeggibile."""
    if not isinstance(record, dict):
        raise ValueError("record di perdita giornaliera non valido")
    try:
        day = date.fromisoformat(record["day_utc"])
        intraday = validators.finite_number(record["intraday_realized_pnl"])
    except Exception as exc:
        raise ValueError("daily_loss_state illeggibile") from exc
    if intraday is None:
        raise ValueError("intraday_realized_pnl non valido")
    if day > today:
        raise ValueError("daily_loss_state di un giorno futuro (orologio indietro)")
    if day < today:
        return None
    breached = record.get("breached") is True
    return (float(intraday), breached, str(record.get("breached_at") or "") if breached else "")


def parse_book(raw: Any, *, today_utc: str) -> Optional[dict]:
    """Libro del giorno ``{"scopes": {k: fields}, "legacy": fields|None, "seq": int|None}``; None se assente.
    Record v1 => sola quarantena ``legacy``. ValueError se illeggibile (=> breach per ogni ambito)."""
    if not raw:
        return None
    try:
        record = json.loads(raw)
    except Exception as exc:
        raise ValueError("daily_loss_state illeggibile") from exc
    if not isinstance(record, dict):
        raise ValueError("daily_loss_state illeggibile")
    today = date.fromisoformat(today_utc)
    seq = _seq(record.get("seq"))
    if "scopes" not in record and "version" not in record:
        return {"scopes": {}, "legacy": _parse_fields(record, today=today), "seq": seq}
    if record.get("version") != BOOK_VERSION or not isinstance(record.get("scopes"), dict):
        raise ValueError("daily_loss_state: versione o ambiti non validi")
    scopes = {}
    for key, sub in record["scopes"].items():
        fields = _parse_fields(sub, today=today)
        if fields is not None:
            scopes[str(key)] = fields
    legacy_raw = record.get("legacy")
    legacy = None if legacy_raw is None else _parse_fields(legacy_raw, today=today)
    return {"scopes": scopes, "legacy": legacy, "seq": seq, "settled_keys": _parse_settled(record, today=today),
            "risk_session": parse_risk_session(record.get("risk_session"))}


def _finite(value: Any) -> float:
    number = float(value)
    if type(value) is bool or not math.isfinite(number):
        raise ValueError("valore non finito")
    return number


def _parse_delta(raw: Any) -> dict:
    if not isinstance(raw, dict) or not isinstance(raw.get("scopes"), dict):
        raise ValueError("delta della sessione di rischio non valido")
    return {"desk": _finite(raw.get("desk")), "unscoped": _finite(raw.get("unscoped")),
            "scopes": {str(k): _finite(v) for k, v in raw["scopes"].items()}}


def parse_risk_session(raw: Any) -> Optional[dict]:
    """F1 (owner M134): sessione di rischio salvata nel libro (sopravvive al cambio di giornata e ai riavvii).
    ``{"id", "opened_at", "rev", "delta": {"desk", "scopes", "unscoped"}, "closed": [{"id", "delta", "closed_at"}],
    "archive_pending": [{"session_id", "settlement_key", "delta"}]}`` (G2: P&L tardivi non ancora nel registro).
    Assente => None; illeggibile => ValueError (libro illeggibile: fail-closed)."""
    if raw is None:
        return None
    try:
        if not isinstance(raw, dict) or not str(raw.get("id") or "") or type(raw.get("rev")) is not int:
            raise ValueError("sessione di rischio non valida")
        closed = []
        for item in raw.get("closed") or []:
            if not isinstance(item, dict) or not str(item.get("id") or ""):
                raise ValueError("sessione di rischio chiusa non valida")
            limit = item.get("limit")
            entry = {"id": str(item["id"]), "delta": _parse_delta(item.get("delta")),
                     "closed_at": str(item.get("closed_at") or "")}
            if "opened_at" in item or "limit" in item or "accounts" in item:   # G2: conto, limiti, istanti
                entry.update(opened_at=str(item.get("opened_at") or ""),
                             limit=None if limit is None else _finite(limit),
                             accounts=[str(a) for a in (item.get("accounts") or [])])
            if "late_keys" in item:      # M144 (CX-F1): tardivi gia' compresi nel P&L della sessione chiusa
                if not isinstance(item["late_keys"], list):
                    raise ValueError("tardivi della sessione chiusa non validi")
                entry["late_keys"] = [str(k) for k in item["late_keys"]]
            if "rev" in item:            # M144: revisione alla chiusura (un ripristino riparte sopra)
                if type(item["rev"]) is not int:
                    raise ValueError("revisione della sessione chiusa non valida")
                entry["rev"] = int(item["rev"])
            if item.get("retired") is True:   # M144 (G1e): id di un reset annullato, mai riusato ne' riaperto
                entry["retired"] = True
                entry["merged_into"] = str(item.get("merged_into") or "")
            closed.append(entry)
        pending = []
        for item in raw.get("archive_pending") or []:      # G2: P&L tardivi da scrivere nell'archivio durevole
            if not isinstance(item, dict) or not str(item.get("session_id") or "") or not str(
                    item.get("settlement_key") or ""):
                raise ValueError("P&L tardivo in attesa non valido")
            pending.append({"session_id": str(item["session_id"]), "settlement_key": str(item["settlement_key"]),
                            "delta": _parse_delta(item.get("delta"))})
        out = {"id": str(raw["id"]), "opened_at": str(raw.get("opened_at") or ""), "rev": int(raw["rev"]),
               "delta": _parse_delta(raw.get("delta")), "closed": closed, "archive_pending": pending}
        if raw.get("unverifiable") is True:   # M144: perdita della sessione non verificabile (libro illeggibile)
            out["unverifiable"] = True
        return out
    except ValueError:
        raise
    except Exception as exc:
        raise ValueError("sessione di rischio illeggibile") from exc


def _parse_settled(record: dict, *, today: date) -> set:
    """Chiavi dei settlement gia' realizzati OGGI, salvate nella stessa scrittura dello stato giornaliero."""
    raw = record.get("settled")
    if raw is None:
        return set()
    if not isinstance(raw, dict) or not isinstance(raw.get("keys"), list):
        raise ValueError("daily_loss_state: chiavi dei settlement non valide")
    try:
        day = date.fromisoformat(raw["day_utc"])
    except Exception as exc:
        raise ValueError("daily_loss_state: giorno delle chiavi non valido") from exc
    if day != today:
        return set()
    return {str(k) for k in raw["keys"]}


def _merge_fields(a: Optional[tuple], b: Optional[tuple], newer: Optional[str]) -> Optional[tuple]:
    if a is None or b is None:
        return a or b
    if newer == "a":
        intraday = a[0]
    elif newer == "b":
        intraday = b[0]
    else:
        intraday = min(a[0], b[0])
    breached = [f for f in (a, b) if f[1]]
    return (intraday, bool(breached), breached[0][2] if breached else "")


def reconcile_books(a: Optional[dict], b: Optional[dict]) -> Optional[dict]:
    """Per ambito: `seq` piu' alta, altrimenti la perdita maggiore; breach se uno dei due; unione degli ambiti."""
    if not a or not b:
        return a or b
    sa, sb = a.get("seq"), b.get("seq")
    newer = ("a" if sa > sb else "b") if (sa is not None and sb is not None and sa != sb) else None
    keys = set(a["scopes"]) | set(b["scopes"])
    seqs = [s for s in (sa, sb) if s is not None]
    return {"scopes": {k: _merge_fields(a["scopes"].get(k), b["scopes"].get(k), newer) for k in keys},
            "legacy": _merge_fields(a.get("legacy"), b.get("legacy"), newer),
            "seq": max(seqs) if seqs else None,
            "settled_keys": set(a.get("settled_keys") or ()) | set(b.get("settled_keys") or ()),
            "risk_session": _newer_risk_session(a.get("risk_session"), b.get("risk_session"), sa, sb)}


def _newer_risk_session(a: Optional[dict], b: Optional[dict], seq_a: Optional[int] = None,
                        seq_b: Optional[int] = None) -> Optional[dict]:
    """F1 + M144 (CX-E1, G-D): sessione di rischio fra db e marker di riserva. La revisione conta SOLO fra copie della
    STESSA sessione: sessioni diverse si ordinano con la `seq` del libro (monotona per scrittura: un marker rimasto ha
    sempre la `seq` piu' bassa del salvataggio che l'ha superato) o, senza `seq`, con la catena delle chiusure (la
    sessione che compare fra le chiuse dell'altra e' la piu' vecchia). Ordine non determinabile => ValueError (libro
    illeggibile, fail-closed). Le sessioni chiuse e i tardivi in attesa delle due copie si UNISCONO: nessuna sessione
    chiusa e nessun P&L tardivo spariscono per la scelta della copia."""
    if not a or not b:
        return a or b
    if a["id"] == b["id"]:
        if a["rev"] != b["rev"]:
            winner, other = (a, b) if a["rev"] > b["rev"] else (b, a)
        else:      # stessa revisione: la perdita di sessione maggiore (fail-closed)
            winner, other = (a, b) if a["delta"]["desk"] <= b["delta"]["desk"] else (b, a)
    elif seq_a is not None and seq_b is not None and seq_a != seq_b:
        winner, other = (a, b) if seq_a > seq_b else (b, a)
    else:
        closed_a = {str(c.get("id")) for c in a.get("closed") or []}
        closed_b = {str(c.get("id")) for c in b.get("closed") or []}
        if a["id"] in closed_b and b["id"] not in closed_a:
            winner, other = b, a
        elif b["id"] in closed_a and a["id"] not in closed_b:
            winner, other = a, b
        else:
            raise ValueError("sessione di rischio: copie non ordinabili")
    out = dict(winner)
    known = {str(c.get("id")) for c in winner.get("closed") or []} | {str(winner["id"])}
    # le chiuse note solo all'altra copia sono le piu' vecchie (gia' uscite dalla finestra): in testa, cosi' la finestra
    # operativa le scarta se sono archiviate (`_trim_closed_window`), senza mai oscurare l'archivio piu' aggiornato
    out["closed"] = [dict(c) for c in other.get("closed") or [] if str(c.get("id")) not in known] + list(
        winner.get("closed") or [])
    seen = {(str(p.get("session_id")), str(p.get("settlement_key"))) for p in winner.get("archive_pending") or []}
    out["archive_pending"] = list(winner.get("archive_pending") or []) + [
        dict(p) for p in other.get("archive_pending") or []
        if (str(p.get("session_id")), str(p.get("settlement_key"))) not in seen]
    return out


def unreadable_book() -> dict:
    """Stato illeggibile: quarantena in breach per ogni ambito fino al nuovo giorno (fail-closed)."""
    return {"scopes": {}, "legacy": (0.0, True, datetime.utcnow().isoformat()), "seq": None,
            "reason": "daily_loss_state_unreadable", "risk_session_unreadable": True}


def restore_daily_loss_book(db: Any, *, today_utc: str) -> dict:
    marker = _marker_path(db)
    try:
        settings = db.get_settings() if hasattr(db, "get_settings") else {}
        from_db = parse_book((settings or {}).get(DAILY_LOSS_STATE_KEY), today_utc=today_utc)
        from_marker = None
        if marker and os.path.exists(marker):
            with open(marker, encoding="utf-8") as fh:
                from_marker = parse_book(fh.read(), today_utc=today_utc)
        # M144: copie della sessione di rischio non ordinabili => ValueError => libro illeggibile (mai un avvio rotto)
        merged = reconcile_books(from_db, from_marker)
    except Exception:
        logger.exception("daily-loss: stato persistito illeggibile, fail-closed per ogni ambito")
        return unreadable_book()
    return merged or {"scopes": {}, "legacy": None, "seq": None}


def _save_verified(db: Any, payload: str) -> None:
    """M130 (matrice D, ledger «falso»): `save_settings` non ha un contratto di ritorno. Il salvataggio vale solo se il
    libro RILETTO coincide con quello scritto; altrimenti solleva (=> ripiego sul marker, stato non salvato)."""
    db.save_settings({DAILY_LOSS_STATE_KEY: payload})
    readback = db.get_settings() if hasattr(db, "get_settings") else None
    if not isinstance(readback, dict) or readback.get(DAILY_LOSS_STATE_KEY) != payload:
        raise OSError("libro giornaliero non riletto dopo il salvataggio")


def book_record(scopes: dict, legacy: Optional[dict], settled: Optional[dict] = None,
                risk_session: Optional[dict] = None) -> dict:
    # Ogni ambito si scrive, anche a zero (Codex su 38979e59): omettere uno zero farebbe risorgere, alla
    # riconciliazione con un marker rimasto, un profitto piu' vecchio dello stesso ambito.
    return {"version": BOOK_VERSION,
            "scopes": {str(k): daily_loss_record(v) for k, v in sorted((scopes or {}).items())},
            "legacy": daily_loss_record(legacy) if legacy else None,
            # mandato owner P2 (PF-OWNER-PR638-BETFAIR-SIM-E2E-GATE-20261116): chiavi dei settlement realizzati oggi,
            # nella STESSA scrittura dello stato: un crash prima della marcatura non fa contare due volte il P&L
            "settled": ({"day_utc": settled["day_utc"], "keys": sorted(settled["keys"])} if settled else None),
            # F1 (owner M134): sessione di rischio nella STESSA scrittura delle chiavi dei settlement (exactly-once)
            "risk_session": risk_session}


def _strip_seq(record: Any) -> Any:
    return {k: v for k, v in record.items() if k != "seq"} if isinstance(record, dict) else record


def persist_daily_loss_book(db: Any, scopes: dict, legacy: Optional[dict], last: Optional[dict],
                            restored_seq: Optional[int] = None, settled: Optional[dict] = None,
                            risk_session: Optional[dict] = None) -> Optional[dict]:
    """Come `persist_daily_loss`, per il libro v2 di tutti gli ambiti; la `seq` riparte sopra quella ripristinata
    (un marker rimasto non puo' mai superare un salvataggio successivo). Mai raise."""
    record = book_record(scopes, legacy, settled, risk_session)
    last_seq = _seq((last or {}).get("seq"))
    if not hasattr(db, "save_settings") or (
            not persist_failed(last) and last is not None and _strip_seq(last) == record):
        return last
    seq = max(last_seq or 0, _seq(restored_seq) or 0) + 1
    payload = json.dumps(dict(record, seq=seq))
    marker = _marker_path(db)
    try:
        _save_verified(db, payload)
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


def scope_fields(fields: Optional[tuple], legacy: Optional[tuple], realized_pnl: float,
                 seq: Optional[int]) -> dict:
    """Campi di un ambito dal libro: la quarantena legacy vale solo come perdita (mai come profitto) e col suo
    breach; si prende la perdita maggiore fra ambito e quarantena (mai la somma: possono coprire gli stessi
    settlement)."""
    intraday, breached, at = fields if fields is not None else (0.0, False, "")
    if legacy is not None:
        intraday = min(intraday, min(0.0, legacy[0]))
        if legacy[1] and not breached:
            breached, at = True, legacy[2]
    return _fields(intraday, breached, at, realized_pnl, seq)
