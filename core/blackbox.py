"""PF-BLACKBOX-01 (BB-1): scatola nera universale di Pickfair — nucleo di scrittura.

Un solo registro cronologico permanente (`blackbox.sqlite`), append-only, per tutto il programma. Unifica
(docs/roadmap/pf_blackbox_01.md §1-bis) il vocabolario e la redazione di `core.event_journal` e la semantica di
sessione/sequenza/catene di `core.session_recorder`; non e' un terzo sistema accanto a quelli.

Garanzie:
- **Append-only**: trigger SQLite che vietano UPDATE e DELETE sulla tabella `events`.
- **Sequenza globale persistente** (`seq`, PK) assegnata dentro una transazione `BEGIN IMMEDIATE`: corretta anche con
  piu' thread e piu' processi sullo stesso file; `session_seq` per sessione.
- **Catena di hash**: `hash = sha256(prev_hash + JSON canonico del record)`; `verify()` rileva record mancanti,
  alterati o riordinati.
- **Durevole**: WAL + `synchronous=FULL`, una transazione per evento.
- **Redazione in scrittura**: chiavi sensibili (`core.redaction.is_sensitive_key`) e segreti registrati
  (`core.event_log.redact_secrets`); mai un token nel registro.
- **Due modalita'** nella stessa API (§2): `critical=False` best-effort, non solleva mai (semantica del recorder);
  `critical=True` scrittura sincrona e durevole, un fallimento solleva `BlackBoxUnavailable` (il chiamante finanziario
  NON invia). `buffer_on_failure=True` (solo uscite protettive gia' decise) mette l'evento nel buffer critico in
  memoria: allo scarico viene scritto con `data.buffered=true`, poi una `BLACKBOX_GAP` con l'intervallo e i ref, poi
  `BLACKBOX_RECOVERED`.
- **Richiesta != conferma**: `phase` (ATTEMPT/RESULT/OBSERVATION) e `outcome` (VERIFIED/PENDING/UNKNOWN/REJECTED/
  FAILED/NOT_APPLICABLE) sono campi del record, mai dedotti.

Retention e rotazione: fuori da BB-1 (nessuna cancellazione e' possibile: i trigger la vietano). Arriveranno con una
regola che non tocca mai eventi di ordini non riconciliati.
"""

from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

from core import event_journal, event_log, redaction

SCHEMA_VERSION = 1

SOURCES = ("USER", "SYSTEM", "MCP", "CONTROL_API", "STRATEGY", "EXCHANGE", "TELEGRAM", "CLI")
PHASES = ("ATTEMPT", "RESULT", "OBSERVATION")
OUTCOMES = ("VERIFIED", "PENDING", "UNKNOWN", "REJECTED", "FAILED", "NOT_APPLICABLE")
SEVERITIES = event_journal.LEVELS

# Campi di primo livello del record (oltre a seq/hash). L'ordine e' parte della forma canonica dell'hash.
RECORD_FIELDS = (
    "event_id", "ts_utc", "mono", "session_id", "session_seq", "pid", "source", "component", "category", "type",
    "severity", "phase", "outcome", "state_before", "state_after", "values_before", "values_after", "reason_code",
    "reason", "correlation_id", "causation_id", "table_id", "market_id", "selection_id", "order_id", "order_ref",
    "bet_id", "strategy", "data",
)
_JSON_FIELDS = ("values_before", "values_after", "data")
_INDEXED = ("ts_utc", "session_id", "type", "category", "table_id", "market_id", "order_id", "order_ref", "bet_id",
            "correlation_id")
GENESIS_HASH = "0" * 64
ENV_PATH = "PICKFAIR_BLACKBOX_PATH"
MAX_CRITICAL_BUFFER = 1000
REDACTED = "[REDACTED]"


class BlackBoxUnavailable(RuntimeError):
    """La scrittura durevole di un evento critico non e' riuscita: nessuna nuova azione finanziaria."""

    reason_code = "BLACKBOX_UNAVAILABLE"


def default_path() -> str:
    """`PICKFAIR_BLACKBOX_PATH` se valorizzata, altrimenti `<cartella dati>/blackbox/blackbox.sqlite`."""
    explicit = (os.environ.get(ENV_PATH) or "").strip()
    if explicit:
        return explicit
    import percorsi
    return percorsi.dentro("blackbox", "blackbox.sqlite")


def normalize_uuid(value: Any) -> Any:
    """Forma unica degli identificativi uuid nel registro: 32 hex minuscoli. La forma con trattini (contratto MCP
    1.14.0) e quella compatta sono lo STESSO id, in scrittura e in lettura: mai due righe o due risultati diversi."""
    if value is None:
        return None
    s = str(value).strip()
    compact = s.replace("-", "").lower()
    return compact if len(compact) == 32 and all(c in "0123456789abcdef" for c in compact) else s


def utc_iso(ts: float) -> str:
    """ISO-8601 UTC al microsecondo, con `Z`."""
    return _dt.datetime.fromtimestamp(float(ts), tz=_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _redact(value: Any, key: str = "") -> Any:
    """Redazione ricorsiva: valore intero per chiavi sensibili, segreti registrati e token nelle stringhe."""
    if key and redaction.is_sensitive_key(key):
        return REDACTED
    if isinstance(value, str):
        return event_log.redact_secrets(value)
    if isinstance(value, dict):
        return {(event_log.redact_secrets(k) if isinstance(k, str) else str(k)): _redact(v, str(k))
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in value]
    if isinstance(value, float) and value != value:          # NaN non e' JSON canonico
        return None
    return value


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=str)


def compute_hash(prev_hash: str, seq: int, record: Dict[str, Any]) -> str:
    body = canonical_json({"seq": int(seq), **{f: record.get(f) for f in RECORD_FIELDS}})
    return hashlib.sha256((prev_hash + body).encode("utf-8")).hexdigest()


class BlackBox:
    """Registro unico. Thread-safe; piu' processi sullo stesso file sono serializzati da `BEGIN IMMEDIATE`."""

    def __init__(self, path: Optional[str] = None, *, session_id: Optional[str] = None,
                 clock: Callable[[], float] = time.time, mono: Callable[[], float] = time.monotonic,
                 pid: Optional[int] = None, connect: Callable[..., sqlite3.Connection] = sqlite3.connect):
        self.path = str(path or default_path())
        self.session_id = session_id or f"s_{int(clock())}_{uuid.uuid4().hex[:8]}"
        self._clock, self._mono, self._connect = clock, mono, connect
        self.pid = int(os.getpid() if pid is None else pid)
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._session_seq = 0
        self._buffer: List[Dict[str, Any]] = []        # buffer critico (uscite protettive durante un guasto)
        self._degraded_since: Optional[float] = None
        self.dropped = 0                               # eventi non critici persi (mai silenziosi: GAP al ripristino)
        self.last_error: Optional[str] = None

    # ------------------------------------------------------------------ apertura e schema
    def open(self) -> "BlackBox":
        with self._lock:
            if self._conn is not None:
                return self
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            conn = self._connect(self.path, isolation_level=None, check_same_thread=False, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=FULL")
            cols = ", ".join(f"{f} {'REAL' if f == 'mono' else 'INTEGER' if f in ('session_seq', 'pid') else 'TEXT'}"
                             for f in RECORD_FIELDS)
            conn.execute(f"CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, {cols}, "
                         "prev_hash TEXT NOT NULL, hash TEXT NOT NULL)")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_events_event_id ON events(event_id)")
            for f in _INDEXED:
                conn.execute(f"CREATE INDEX IF NOT EXISTS ix_events_{f} ON events({f})")
            conn.execute("CREATE TRIGGER IF NOT EXISTS events_no_update BEFORE UPDATE ON events "
                         "BEGIN SELECT RAISE(ABORT, 'blackbox: append-only'); END")
            conn.execute("CREATE TRIGGER IF NOT EXISTS events_no_delete BEFORE DELETE ON events "
                         "BEGIN SELECT RAISE(ABORT, 'blackbox: append-only'); END")
            conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
            conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('schema_version', ?)", (str(SCHEMA_VERSION),))
            self._conn = conn
            return self

    def close(self) -> None:
        with self._lock:
            if self._conn is not None:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    # ------------------------------------------------------------------ scrittura
    def _build(self, event_type: str, kw: Dict[str, Any]) -> Dict[str, Any]:
        if event_type not in event_journal.EVENT_TYPES:
            raise ValueError(f"blackbox: tipo evento sconosciuto: {event_type!r}")
        source = str(kw.pop("source", "SYSTEM")).upper()
        if source not in SOURCES:
            raise ValueError(f"blackbox: origine non ammessa: {source!r}")
        phase = kw.pop("phase", None)
        if phase is not None and phase not in PHASES:
            raise ValueError(f"blackbox: phase non ammessa: {phase!r}")
        outcome = kw.pop("outcome", None)
        if outcome is not None and outcome not in OUTCOMES:
            raise ValueError(f"blackbox: outcome non ammesso: {outcome!r}")
        unknown = set(kw) - set(RECORD_FIELDS)
        if unknown:
            raise ValueError(f"blackbox: campi sconosciuti: {sorted(unknown)}")
        now = float(self._clock())
        rec: Dict[str, Any] = {f: None for f in RECORD_FIELDS}
        rec.update({k: v for k, v in kw.items()})
        rec.update(
            event_id=normalize_uuid(kw.get("event_id") or uuid.uuid4().hex), ts_utc=kw.get("ts_utc") or utc_iso(now),
            mono=float(self._mono()), session_id=self.session_id, pid=self.pid, source=source,
            category=event_journal.category_of(event_type), type=event_type,
            severity=event_journal.normalize_level(kw.get("severity")), phase=phase, outcome=outcome,
        )
        for f in ("correlation_id", "causation_id"):
            rec[f] = normalize_uuid(rec[f])
        for f in _JSON_FIELDS:
            rec[f] = _redact(rec[f]) if rec[f] is not None else None
        for f in RECORD_FIELDS:
            if f not in _JSON_FIELDS and isinstance(rec[f], str):
                rec[f] = event_log.redact_secrets(rec[f])
            if f in ("table_id", "market_id", "selection_id", "order_id", "order_ref", "bet_id") and rec[f] is not None:
                rec[f] = str(rec[f])
        return rec

    def _insert(self, rec: Dict[str, Any]) -> int:
        conn = self._conn
        if conn is None:
            raise sqlite3.OperationalError("blackbox non aperta")
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute("SELECT seq, hash FROM events ORDER BY seq DESC LIMIT 1").fetchone()
            seq = (row["seq"] + 1) if row else 1
            prev = row["hash"] if row else GENESIS_HASH
            self._session_seq += 1
            rec["session_seq"] = self._session_seq
            h = compute_hash(prev, seq, rec)
            values = [canonical_json(rec[f]) if f in _JSON_FIELDS and rec[f] is not None else rec[f]
                      for f in RECORD_FIELDS]
            conn.execute(f"INSERT INTO events (seq, {', '.join(RECORD_FIELDS)}, prev_hash, hash) "
                         f"VALUES (?, {', '.join('?' for _ in RECORD_FIELDS)}, ?, ?)", [seq, *values, prev, h])
            conn.execute("COMMIT")
            return seq
        except BaseException:
            self._session_seq -= 1
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def record(self, event_type: str, *, critical: bool = False, buffer_on_failure: bool = False,
               **fields: Any) -> Optional[str]:
        """Registra un evento. Ritorna `event_id`; `None` solo per un evento non critico non scritto.

        `critical=True`: scrittura durevole o `BlackBoxUnavailable` (il chiamante non esegue l'azione). Con
        `buffer_on_failure=True` (solo uscite protettive) un guasto mette l'evento nel buffer critico e ritorna il
        suo id: verra' scritto al ripristino, coperto da una `BLACKBOX_GAP`."""
        rec = self._build(event_type, dict(fields))      # errori di forma: sempre sollevati (bug del chiamante)
        with self._lock:
            try:
                if self._buffer or self._degraded_since is not None:
                    self._flush_after_outage()
                self._insert(rec)
                return rec["event_id"]
            except (sqlite3.Error, OSError) as exc:
                self._enter_degraded(exc)
                if critical and buffer_on_failure and len(self._buffer) < MAX_CRITICAL_BUFFER:
                    self._buffer.append(rec)
                    return rec["event_id"]
                if critical:
                    raise BlackBoxUnavailable(f"{type(exc).__name__}: {exc}") from exc
                self.dropped += 1
                return None

    def _enter_degraded(self, exc: BaseException) -> None:
        self.last_error = f"{type(exc).__name__}: {exc}"
        if self._degraded_since is None:
            self._degraded_since = float(self._clock())

    def _flush_after_outage(self) -> None:
        """Al ripristino: eventi bufferizzati (ora originale, `buffered=true`), poi GAP con intervallo e ref, poi
        RECOVERED. Se fallisce, solleva: il buffer resta intatto."""
        start = self._degraded_since if self._degraded_since is not None else float(self._clock())
        pending = list(self._buffer)
        for rec in pending:
            rec = dict(rec)
            rec["data"] = {**(rec.get("data") or {}), "buffered": True}
            self._insert(rec)
            self._buffer.pop(0)
        refs = sorted({str(r["order_ref"]) for r in pending if r.get("order_ref")})
        gap = self._build("BLACKBOX_GAP", dict(
            source="SYSTEM", component="blackbox", severity="ERROR", phase="OBSERVATION",
            outcome="VERIFIED", reason_code="BLACKBOX_OUTAGE",
            reason=self.last_error, data={"from_ts": utc_iso(start), "to_ts": utc_iso(self._clock()), "refs": refs,
                                          "buffered": len(pending), "dropped_non_critical": self.dropped}))
        self._insert(gap)
        self._insert(self._build("BLACKBOX_RECOVERED", dict(source="SYSTEM", component="blackbox",
                                                            phase="OBSERVATION", outcome="VERIFIED")))
        self._degraded_since, self.dropped = None, 0

    # ------------------------------------------------------------------ sessione
    def start_session(self, **data: Any) -> str:
        """Apre la sessione: se la precedente non ha chiuso con SHUTDOWN registra UNCLEAN_TERMINATION e una GAP
        UNKNOWN sull'intervallo, poi PROCESS_START. Evento critico: senza registro il processo non parte."""
        self.open()
        with self._lock:
            last = self._conn.execute(
                "SELECT session_id, type, ts_utc FROM events ORDER BY seq DESC LIMIT 1").fetchone()
        if last is not None and last["type"] not in ("SHUTDOWN", "UNCLEAN_TERMINATION"):
            self.record("UNCLEAN_TERMINATION", critical=True, source="SYSTEM", component="blackbox",
                        severity="ERROR", phase="OBSERVATION", outcome="VERIFIED",
                        reason_code="NO_SHUTDOWN_RECORDED", data={"previous_session": last["session_id"],
                                                                  "last_event_type": last["type"]})
            self.record("BLACKBOX_GAP", critical=True, source="SYSTEM", component="blackbox", severity="ERROR",
                        phase="OBSERVATION", outcome="UNKNOWN", reason_code="UNCLEAN_TERMINATION",
                        data={"from_ts": last["ts_utc"], "to_ts": utc_iso(self._clock()), "refs": []})
        return self.record("PROCESS_START", critical=True, source="SYSTEM", component="process",
                           phase="OBSERVATION", outcome="VERIFIED", data={"pid": self.pid, **data})

    def end_session(self, reason: str = "STOP", **data: Any) -> Optional[str]:
        return self.record("SHUTDOWN", source="SYSTEM", component="process", phase="OBSERVATION",
                           outcome="VERIFIED", reason_code=str(reason), data=data or None)

    # ------------------------------------------------------------------ stato e verifica
    def health(self) -> Dict[str, Any]:
        return {"path": self.path, "session_id": self.session_id, "open": self._conn is not None,
                "degraded": self._degraded_since is not None, "buffered": len(self._buffer),
                "dropped_non_critical": self.dropped, "last_error": self.last_error}

    def verify(self) -> Dict[str, Any]:
        """Ricalcola sequenza e catena di hash su tutto il registro (sola lettura)."""
        self.open()
        problems: List[Dict[str, Any]] = []
        prev, expected, last = GENESIS_HASH, 1, 0
        with self._lock:
            rows = self._conn.execute("SELECT * FROM events ORDER BY seq").fetchall()
        for row in rows:
            rec = {f: row[f] for f in RECORD_FIELDS}
            for f in _JSON_FIELDS:
                if rec[f] is not None:
                    try:
                        rec[f] = json.loads(rec[f])
                    except ValueError:
                        problems.append({"seq": row["seq"], "problem": "JSON_CORRUPT", "field": f})
            if row["seq"] != expected:
                problems.append({"seq": row["seq"], "problem": "SEQ_GAP", "expected": expected})
            if row["prev_hash"] != prev:
                problems.append({"seq": row["seq"], "problem": "CHAIN_BROKEN"})
            if compute_hash(row["prev_hash"], row["seq"], rec) != row["hash"]:
                problems.append({"seq": row["seq"], "problem": "HASH_MISMATCH"})
            prev, expected, last = row["hash"], row["seq"] + 1, row["seq"]
        return {"ok": not problems, "last_seq": last, "count": len(rows), "problems": problems}

