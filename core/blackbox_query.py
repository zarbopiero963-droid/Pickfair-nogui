"""PF-BLACKBOX-01 (BB-1): lettura della scatola nera — unica API usata da GUI, CLI, Control API e MCP.

Sola lettura sul registro `core.blackbox` (nessuna seconda copia dei dati). Contratto con il lato Control API/MCP di
Claude C (`pickfair-mcp-` `docs/design/BLACKBOX_READ_CONTRACT.md` 1.13.0, docs/roadmap/pf_blackbox_01.md §4-bis):

- `status()`, `sessions()`, `query(filters, after_seq, limit)`, `get(event_id, chain_depth)`;
- `request_hook(record) -> bool`: un record per ogni richiesta HTTP (ATTEMPT prima della lettura, RESULT dopo
  l'invio; solo RESULT per una richiesta rifiutata), mai uno per evento letto. False = la Control API risponde 503;
- vista **esterna** (Control API/MCP): `order_ref` (customerOrderRef) non esposto; un campo non registrato vale `null`
  ed e' elencato in `not_recorded`;
- lacune dichiarate (`BLACKBOX_GAP`) restituite come tali in `gaps_in_range`, mai ricostruite.
"""

from __future__ import annotations

import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from core import blackbox as bb

MAX_LIMIT = 500
DEFAULT_LIMIT = 100
MAX_CHAIN_DEPTH = 20
FILTERS = ("from_ts", "to_ts", "session_id", "table_id", "market_id", "selection_id", "order_id", "bet_id", "strategy",
           "type", "category", "source", "severity", "phase", "outcome", "correlation_id", "causation_id")
_UUID_FIELDS = ("correlation_id", "causation_id")


normalize_event_id = bb.normalize_uuid          # una sola regola per scrittura e lettura
EXTERNAL_HIDDEN = ("order_ref",)
_EQ = {f: f for f in FILTERS if f not in ("from_ts", "to_ts")}
RATE_PER_S = 20.0
BURST = 40.0


class QueryError(ValueError):
    """Richiesta di lettura non valida (filtro sconosciuto, limite, cursore)."""


_HIDDEN_KEYS = frozenset({"order_ref", "customer_ref", "customerorderref", "customer_order_ref"})


def _strip_hidden(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _strip_hidden(v) for k, v in value.items()
                if not (isinstance(k, str) and k.replace("-", "_").lower() in _HIDDEN_KEYS)}
    if isinstance(value, list):
        return [_strip_hidden(v) for v in value]
    return value


def _row_to_event(row, external: bool) -> Dict[str, Any]:
    ev: Dict[str, Any] = {"seq": row["seq"], "hash": row["hash"]}
    for f in bb.RECORD_FIELDS:
        v = row[f]
        if f in bb._JSON_FIELDS and v is not None:
            v = json.loads(v)
        ev[f] = v
    if external:
        for f in EXTERNAL_HIDDEN:
            ev.pop(f, None)
        for f in bb._JSON_FIELDS:                       # a ogni profondita', in data e values_*: mai il riferimento
            ev[f] = _strip_hidden(ev.get(f))           # cliente nella vista esterna (analisi preventiva BB-1)
    ev["not_recorded"] = sorted(k for k, v in ev.items() if v is None and k not in ("seq", "hash"))
    return ev


class BlackBoxReader:
    """Lettura del registro aperto da `box` (stessa connessione, stessa serializzazione)."""

    def __init__(self, box: bb.BlackBox, *, clock: Callable[[], float] = time.monotonic):
        self.box = box.open()
        self._clock = clock
        self._lock = threading.Lock()
        self._tokens, self._last = BURST, clock()
        self._coalesced = 0

    def _rows(self, sql: str, args: Tuple = ()) -> List[Any]:
        with self.box._lock:
            return self.box._conn.execute(sql, args).fetchall()

    def status(self) -> Dict[str, Any]:
        h = self.box.health()
        last = self._rows("SELECT MAX(seq) AS s, COUNT(*) AS n FROM events")[0]
        return {"ok": not h["degraded"], "degraded": h["degraded"], "as_of_seq": last["s"] or 0,
                "count": last["n"], "session_id": h["session_id"], "buffered": h["buffered"],
                "dropped_non_critical": h["dropped_non_critical"],
                "not_reported": [] if h["last_error"] is None else ["last_error_detail"]}

    def sessions(self) -> List[Dict[str, Any]]:
        rows = self._rows("SELECT session_id, MIN(seq) AS first_seq, MAX(seq) AS last_seq, MIN(ts_utc) AS started, "
                          "MAX(ts_utc) AS last_ts, MAX(pid) AS pid, "
                          "SUM(CASE WHEN type='SHUTDOWN' THEN 1 ELSE 0 END) AS shutdowns "
                          "FROM events GROUP BY session_id ORDER BY first_seq")
        return [{"session_id": r["session_id"], "first_seq": r["first_seq"], "last_seq": r["last_seq"],
                 "started": r["started"], "last_ts": r["last_ts"], "pid": r["pid"],
                 "clean_shutdown": bool(r["shutdowns"])} for r in rows]

    def query(self, filters: Optional[Dict[str, Any]] = None, after_seq: int = 0, limit: int = DEFAULT_LIMIT, *,
              external: bool = True) -> Dict[str, Any]:
        filters = dict(filters or {})
        unknown = set(filters) - set(FILTERS) - ({"order_ref"} if not external else set())
        if unknown:
            raise QueryError(f"filtri non ammessi: {sorted(unknown)}")
        if not isinstance(after_seq, int) or isinstance(after_seq, bool) or after_seq < 0:
            raise QueryError("after_seq non valido")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= MAX_LIMIT:
            raise QueryError(f"limit fuori da 1..{MAX_LIMIT}")
        where, args = ["seq > ?"], [after_seq]
        for k, v in filters.items():
            if k == "from_ts":
                where.append("ts_utc >= ?"); args.append(str(v))
            elif k == "to_ts":
                where.append("ts_utc <= ?"); args.append(str(v))
            elif k in _UUID_FIELDS:
                where.append(f"{k} = ?"); args.append(normalize_event_id(v))
            else:
                where.append(f"{k} = ?"); args.append(str(v))
        as_of = self._rows("SELECT COALESCE(MAX(seq), 0) AS s FROM events")[0]["s"]
        rows = self._rows(f"SELECT * FROM events WHERE {' AND '.join(where)} AND seq <= ? ORDER BY seq LIMIT ?",
                          (*args, as_of, limit + 1))
        more = len(rows) > limit
        rows = rows[:limit]
        records = [_row_to_event(r, external) for r in rows]
        lo = filters.get("from_ts")
        hi = filters.get("to_ts")
        gap_rows = self._rows("SELECT * FROM events WHERE type = 'BLACKBOX_GAP' AND seq <= ? ORDER BY seq", (as_of,))
        gaps = []
        for g in gap_rows:
            d = json.loads(g["data"]) if g["data"] else {}
            if (hi is None or str(d.get("from_ts", "")) <= str(hi)) and (lo is None or str(d.get("to_ts", "")) >= str(lo)):
                gaps.append({"seq": g["seq"], "from_ts": d.get("from_ts"), "to_ts": d.get("to_ts"),
                             "outcome": g["outcome"], "reason_code": g["reason_code"]})
        return {"records": records, "next_after_seq": records[-1]["seq"] if (more and records) else 0,
                "gaps_in_range": gaps, "as_of_seq": as_of}

    def get(self, event_id: str, chain_depth: int = 0, *, external: bool = True) -> Optional[Dict[str, Any]]:
        if not isinstance(chain_depth, int) or isinstance(chain_depth, bool) or not 0 <= chain_depth <= MAX_CHAIN_DEPTH:
            raise QueryError(f"chain_depth fuori da 0..{MAX_CHAIN_DEPTH}")
        rows = self._rows("SELECT * FROM events WHERE event_id = ?", (normalize_event_id(event_id),))
        if not rows:
            return None
        ev = _row_to_event(rows[0], external)
        chain, cause = [], ev.get("causation_id")
        while cause and len(chain) < chain_depth:
            r = self._rows("SELECT * FROM events WHERE event_id = ?", (str(cause),))
            if not r:
                chain.append({"event_id": cause, "missing": True})
                break
            c = _row_to_event(r[0], external)
            chain.append(c)
            cause = c.get("causation_id")
        ev["causation_chain"] = chain
        return ev

    # ------------------------------------------------------------------ hook delle richieste (Control API / MCP)
    def request_hook(self, record: Dict[str, Any]) -> bool:
        """Registra una richiesta della Control API. Mai ricorsivo (non legge), aggregato oltre 20/s (raffica 40).
        False = registrazione non riuscita: la Control API deve rispondere 503 `BLACKBOX_UNAVAILABLE`."""
        try:
            phase = str(record.get("phase") or "")
            if phase not in ("ATTEMPT", "RESULT"):
                return False
            with self._lock:
                now = self._clock()
                self._tokens = min(BURST, self._tokens + (now - self._last) * RATE_PER_S)
                self._last = now
                if self._tokens < 1.0:
                    self._coalesced += 1
                    return True                  # aggregata: contata nel prossimo record scritto
                self._tokens = max(0.0, self._tokens - 1.0)
                coalesced, self._coalesced = self._coalesced, 0
            data = {k: record.get(k) for k in ("route", "params", "status", "code", "request_id", "duration_ms")}
            data["coalesced_before"] = int(record.get("coalesced_before") or 0) + coalesced
            self.box.record("CONTROL_API_REQUEST", critical=True, source="CONTROL_API", component="control_api",
                            phase=phase, outcome=("VERIFIED" if phase == "RESULT" else "PENDING"),
                            correlation_id=record.get("request_id"), data=data)
            return True
        except Exception:
            return False
