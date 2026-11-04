"""F-11 (#586, DD-03/DD-04) — ricostruzione del picco dell'equity dallo storico Betfair: SOLO ANTEPRIMA.

Decisioni owner: opzione A (#426 c.6145958246 / c.6146717266) e mandato del 04/11/2026: lettura non distruttiva,
ricostruzione verificabile con evidenze e provenienza, anteprima, rifiuto di dati incompleti o incoerenti, nessun reset
arbitrario, PAUSED finché il picco non è dimostrabile, **nessuna scrittura sul database operativo** senza una procedura
specificamente approvata (questo strumento non ne ha: produce solo il valore proposto per la ratifica).

Fonti:
- estratto conto Betfair (`getAccountStatement`, righe con ``refId``, ``itemDate``, ``amount``, ``balance``), esportato
  in sola lettura dal canale autorizzato in un file JSON;
- DB di Pickfair aperto in SOLA LETTURA (``mode=ro``): solo l'istante del primo ordine, per sapere da quando serve lo
  storico.

Regole (fail-closed, ogni violazione => REJECTED con il motivo; l'installazione resta PAUSED):
1. righe ben formate: ``refId`` unico, ``itemDate`` ISO-8601 con fuso, ``amount`` e ``balance`` decimali finiti;
2. catena dei saldi coerente al centesimo in ordine cronologico: ``balance[i] == balance[i-1] + amount[i]``
   (una riga mancante o alterata rompe la catena);
3. copertura: la prima riga dell'estratto precede il primo ordine di Pickfair (il saldo di partenza è noto) e l'ultima
   riga non è anteriore all'ultimo ordine;
4. il saldo dell'estratto è l'equity di CASSA regolata (disponibile + impegnato a posizioni chiuse): il suo massimo è
   il picco di cassa. Il picco del solo disponibile NON è ricostruibile dallo storico (l'impegnato passato non è noto):
   si propone lo stesso valore come LIMITE SUPERIORE PRUDENTE (un picco più alto rende il drawdown più severo, mai più
   permissivo), dichiarato nel report.

Uscita: report JSON con stato (``RECONSTRUCTED_PENDING_RATIFICATION`` o ``REJECTED``), evidenze (periodo, righe,
SHA-256 dei file d'ingresso, esito della catena) e il valore proposto per ``risk.equity_peaks_state``. Mai una scrittura.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

STATE_OK = "RECONSTRUCTED_PENDING_RATIFICATION"
STATE_REJECTED = "REJECTED"
CENT = Decimal("0.01")


class ReconstructionError(ValueError):
    """Dato incompleto o incoerente: il picco non è dimostrabile."""


def _dec(value: Any, field: str, ref: str) -> Decimal:
    if value is None or isinstance(value, bool):
        raise ReconstructionError(f"FIELD_INVALID:{field}:{ref}")
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ReconstructionError(f"FIELD_INVALID:{field}:{ref}") from exc
    if not out.is_finite():
        raise ReconstructionError(f"FIELD_INVALID:{field}:{ref}")
    return out


def _when(value: Any, ref: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ReconstructionError(f"FIELD_INVALID:itemDate:{ref}")
    try:
        out = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReconstructionError(f"FIELD_INVALID:itemDate:{ref}") from exc
    if out.tzinfo is None:
        raise ReconstructionError(f"FIELD_INVALID:itemDate:{ref}")          # mai un fuso inventato
    return out.astimezone(timezone.utc)


def _rows(statement: Any) -> List[Tuple[datetime, str, Decimal, Decimal]]:
    items = statement.get("accountStatement") if isinstance(statement, dict) else statement
    if not isinstance(items, list) or not items:
        raise ReconstructionError("STATEMENT_EMPTY")
    rows, seen = [], set()
    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            raise ReconstructionError(f"ROW_MALFORMED:{idx}")
        ref = item.get("refId")
        if not isinstance(ref, (str, int)) or isinstance(ref, bool) or str(ref) == "":
            raise ReconstructionError(f"FIELD_INVALID:refId:{idx}")
        ref = str(ref)
        if ref in seen:
            raise ReconstructionError(f"DUPLICATE_REF:{ref}")
        seen.add(ref)
        rows.append((_when(item.get("itemDate"), ref), ref, _dec(item.get("amount"), "amount", ref),
                     _dec(item.get("balance"), "balance", ref)))
    rows.sort(key=lambda r: (r[0], r[1]))
    return rows


def reconstruct(statement: Any, *, first_order_at: Optional[datetime], last_order_at: Optional[datetime]) -> Dict[str, Any]:
    """Anteprima della ricostruzione. Mai un'eccezione verso il chiamante: un dato non valido => REJECTED."""
    try:
        rows = _rows(statement)
        for prev, cur in zip(rows, rows[1:]):
            if (prev[3] + cur[2]).quantize(CENT) != cur[3].quantize(CENT):
                raise ReconstructionError(f"BALANCE_CHAIN_BROKEN:{cur[1]}")
        if first_order_at is None or last_order_at is None:
            raise ReconstructionError("PICKFAIR_HISTORY_UNKNOWN")
        if rows[0][0] >= first_order_at:
            raise ReconstructionError("STATEMENT_STARTS_AFTER_FIRST_ORDER")
        if rows[-1][0] < last_order_at:
            raise ReconstructionError("STATEMENT_ENDS_BEFORE_LAST_ORDER")
        opening = rows[0][3] - rows[0][2]                      # saldo prima della prima riga
        peak = max([opening] + [r[3] for r in rows])
        if peak <= 0:
            raise ReconstructionError("PEAK_NOT_POSITIVE")
    except ReconstructionError as exc:
        return {"state": STATE_REJECTED, "reason": str(exc), "proposed_value": None}
    peak_s = str(peak.quantize(CENT))
    return {
        "state": STATE_OK, "reason": "",
        "evidence": {"rows": len(rows), "first_item_at": rows[0][0].isoformat(), "last_item_at": rows[-1][0].isoformat(),
                     "opening_balance": str(opening.quantize(CENT)), "balance_chain": "OK",
                     "first_order_at": first_order_at.isoformat(), "last_order_at": last_order_at.isoformat()},
        "cash_equity_peak": peak_s,
        "equity_peak": peak_s,
        "equity_peak_basis": "CONSERVATIVE_UPPER_BOUND_FROM_CASH_EQUITY",
        # Valore proposto per `risk.equity_peaks_state`: da scrivere SOLO con una procedura approvata dall'owner.
        "proposed_value": {"equity_peak": float(peak_s), "cash_equity_peak": float(peak_s),
                           "source": "RECONSTRUCTED_BETFAIR_STATEMENT"},
    }


def pickfair_order_window(db_path: str) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Primo e ultimo ``created_at`` della tabella ``orders``, letti in SOLA LETTURA (``mode=ro``)."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        row = conn.execute("SELECT MIN(created_at), MAX(created_at) FROM orders").fetchone()
    finally:
        conn.close()
    if not row or row[0] is None:
        return None, None
    return (datetime.fromtimestamp(float(row[0]), tz=timezone.utc),
            datetime.fromtimestamp(float(row[1]), tz=timezone.utc))


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def preview(statement_path: str, db_path: str) -> Dict[str, Any]:
    """Anteprima completa con provenienza (hash dei file); nessuna scrittura, DB aperto in sola lettura."""
    with open(statement_path, "r", encoding="utf-8") as fh:
        statement = json.load(fh)
    first, last = pickfair_order_window(db_path)
    report = reconstruct(statement, first_order_at=first, last_order_at=last)
    report["provenance"] = {"statement_sha256": _sha256(statement_path), "db_sha256": _sha256(db_path),
                            "generated_at": datetime.now(timezone.utc).isoformat(), "writes": "NONE"}
    return report


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Anteprima della ricostruzione del picco (F-11): nessuna scrittura.")
    ap.add_argument("--statement", required=True, help="JSON di getAccountStatement esportato in sola lettura")
    ap.add_argument("--db", required=True, help="DB di Pickfair (aperto in sola lettura)")
    ap.add_argument("--out", required=True)
    args = ap.parse_args(argv)
    report = preview(args.statement, args.db)
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
    print(report["state"], report.get("reason") or "")
    return 0 if report["state"] == STATE_OK else 1
