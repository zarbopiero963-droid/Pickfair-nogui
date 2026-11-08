"""Implementazione FINTA della scatola nera (PF-BLACKBOX-01), solo per i test dell'oracolo di completezza.

Segue il contratto di `docs/roadmap/pf_blackbox_01.md` (branch `pf-gov-persistent-roadmap-01` @ `cb2f04bb`):
§3 campi del record, §4 `record(..., critical=...)`, §4-bis `status()` / `sessions()` / `query()` / `get()`.
Non e' l'implementazione di BB-1 (Claude B): quando BB-1 sara' in PR, i test passeranno a `core.blackbox` /
`core.blackbox_query` e questo modulo restera' solo come doppio per i casi di guasto.
"""
from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

PHASES = frozenset({"ATTEMPT", "RESULT", "OBSERVATION"})
OUTCOMES = frozenset({"VERIFIED", "PENDING", "UNKNOWN", "REJECTED", "FAILED", "NOT_APPLICABLE"})
SOURCES = frozenset({"USER", "SYSTEM", "MCP", "CONTROL_API", "STRATEGY", "EXCHANGE", "TELEGRAM", "CLI"})
REF_FIELDS = ("table_id", "market_id", "selection_id", "order_ref", "bet_id", "strategy")
GENESIS = "0" * 64


class BlackBoxUnavailable(RuntimeError):
    """Scrittura critica non durevole: il chiamante finanziario NON deve inviare (BLACKBOX_UNAVAILABLE)."""


def canonical(record: Dict[str, Any]) -> str:
    body = {k: v for k, v in record.items() if k not in ("hash",)}
    return json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def chain_hash(prev_hash: str, record: Dict[str, Any]) -> str:
    return hashlib.sha256((prev_hash + canonical(record)).encode("utf-8")).hexdigest()


class FakeBlackBox:
    def __init__(self, session_id: Optional[str] = None, pid: int = 1) -> None:
        self._lock = threading.Lock()
        self.events: List[Dict[str, Any]] = []
        self.session_id = session_id or uuid.uuid4().hex
        self.pid = pid
        self._session_seq = 0
        self.disk_full = False          # simula ENOSPC: nessuna scrittura durevole
        self.degraded = False

    # ------------------------------------------------------------------ scrittura (§4)
    def record(self, type: str, *, source: str, phase: str = "OBSERVATION", outcome: str = "NOT_APPLICABLE",
               critical: bool = False, causation_id: Optional[str] = None, correlation_id: Optional[str] = None,
               reason_code: str = "", reason: str = "", state_before: Any = None, state_after: Any = None,
               values_before: Any = None, values_after: Any = None, data: Any = None,
               **refs: Any) -> Dict[str, Any]:
        if phase not in PHASES or outcome not in OUTCOMES or source not in SOURCES:
            raise ValueError(f"record non conforme: phase={phase} outcome={outcome} source={source}")
        unknown = set(refs) - set(REF_FIELDS)
        if unknown:
            raise ValueError(f"riferimenti non previsti: {sorted(unknown)}")
        with self._lock:
            if self.disk_full:
                if not self.degraded:
                    self.degraded = True
                if critical:
                    raise BlackBoxUnavailable(f"BLACKBOX_UNAVAILABLE:{type}")
                return {}
            self._session_seq += 1
            prev = self.events[-1]["hash"] if self.events else GENESIS
            rec = {"seq": len(self.events) + 1, "event_id": uuid.uuid4().hex,
                   "ts_utc": datetime.now(timezone.utc).isoformat(timespec="microseconds"),
                   "mono": time.monotonic(), "session_id": self.session_id, "session_seq": self._session_seq,
                   "pid": self.pid, "source": source, "type": type, "phase": phase, "outcome": outcome,
                   "critical": bool(critical), "causation_id": causation_id, "correlation_id": correlation_id,
                   "reason_code": reason_code, "reason": reason, "state_before": state_before,
                   "state_after": state_after, "values_before": values_before, "values_after": values_after,
                   "data": data, **{k: refs.get(k) for k in REF_FIELDS}, "prev_hash": prev}
            rec["hash"] = chain_hash(prev, rec)
            self.events.append(rec)
            return dict(rec)

    def recover_disk(self) -> None:
        with self._lock:
            self.disk_full = False
        if self.degraded:
            self.degraded = False
            self.record("BLACKBOX_RECOVERED", source="SYSTEM")

    # ------------------------------------------------------------------ lettura (§4-bis)
    def status(self) -> Dict[str, Any]:
        return {"healthy": not self.degraded, "last_seq": self.events[-1]["seq"] if self.events else 0,
                "degraded": self.degraded, "gaps": []}

    def sessions(self) -> List[str]:
        return sorted({e["session_id"] for e in self.events})

    def query(self, filters: Optional[Dict[str, Any]] = None, after_seq: int = 0,
              limit: int = 1000) -> Tuple[List[Dict[str, Any]], int, list, int]:
        filters = filters or {}
        rows = [dict(e) for e in self.events if e["seq"] > after_seq
                and all(e.get(k) == v for k, v in filters.items())][:limit]
        nxt = rows[-1]["seq"] if rows else after_seq
        return rows, nxt, [], (self.events[-1]["seq"] if self.events else 0)

    def get(self, event_id: str, chain_depth: int = 10) -> List[Dict[str, Any]]:
        by_id = {e["event_id"]: e for e in self.events}
        out, cur = [], by_id.get(event_id)
        while cur is not None and len(out) <= chain_depth:
            out.append(dict(cur))
            cur = by_id.get(cur.get("causation_id") or "")
        return out

    def all(self) -> List[Dict[str, Any]]:
        return [dict(e) for e in self.events]


def records_of(source: Any) -> List[Dict[str, Any]]:
    """Tutti i record via il contratto di lettura (paginando `query`), mai accedendo allo store."""
    out: List[Dict[str, Any]] = []
    after = 0
    while True:
        rows, nxt, _gaps, as_of = source.query({}, after_seq=after, limit=500)
        out.extend(rows)
        if not rows or nxt >= as_of:
            return out
        after = nxt


def iter_types(records: Iterable[Dict[str, Any]], *types: str) -> List[Dict[str, Any]]:
    return [r for r in records if r.get("type") in types]
