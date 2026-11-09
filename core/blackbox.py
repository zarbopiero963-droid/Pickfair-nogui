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
  (`core.text_redaction.redact_secrets`, riesportato da `core.event_log`); mai un token nel registro.
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
import errno
import stat
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

# vocabolario e redazione dai moduli SENZA dipendenze: `core.event_journal`/`core.event_log` trascinerebbero
# `core.config_store` (Bridge XTrader) nel grafo vivo (tests/percorsi/test_via_dal_bridge.py)
from core import event_vocabulary as event_journal, redaction, text_redaction

SCHEMA_VERSION = 1

SOURCES = ("USER", "SYSTEM", "MCP", "CONTROL_API", "STRATEGY", "EXCHANGE", "TELEGRAM", "CLI")
PHASES = ("ATTEMPT", "RESULT", "OBSERVATION")
OUTCOMES = ("VERIFIED", "PENDING", "UNKNOWN", "REJECTED", "FAILED", "NOT_APPLICABLE")
SEVERITIES = event_journal.LEVELS

# Campi di primo livello del record (oltre a seq/hash). L'ordine e' parte della forma canonica dell'hash.
RECORD_FIELDS = (
    "event_id", "ts_utc", "mono", "session_id", "session_seq", "pid", "source", "component", "category", "type",
    "severity", "phase", "outcome", "state_before", "state_after", "values_before", "values_after", "reason_code",
    "reason", "correlation_id", "causation_id", "table_id", "market_id", "selection_id", "order_id", "order_alias",
    "order_ref", "bet_id", "exchange_ts", "strategy", "data",
)
_JSON_FIELDS = ("values_before", "values_after", "data")
_INDEXED = ("ts_utc", "session_id", "type", "category", "table_id", "market_id", "order_id", "order_alias", "order_ref",
            "bet_id", "correlation_id")
# RFC 3339 canonico del registro (decisione owner D2): UTC, microsecondi, `Z`. Mai ora locale.
_TS_CANONICAL = re.compile(r"\A\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z\Z")


class RuntimeAlreadyActive(RuntimeError):
    """Un secondo runtime finanziario sullo stesso registro: rifiutato (decisione owner D3)."""


def check_utc_ts(value: Any, field: str) -> str:
    """`YYYY-MM-DDTHH:MM:SS.ffffffZ` valido (data e ora reali), altrimenti ValueError: mai un orario ambiguo."""
    s = str(value)
    if not _TS_CANONICAL.fullmatch(s):
        raise ValueError(f"blackbox: {field} non nel formato UTC canonico YYYY-MM-DDTHH:MM:SS.ffffffZ: {s!r}")
    try:
        _dt.datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        raise ValueError(f"blackbox: {field} non e' una data/ora valida: {s!r}") from None
    return s
GENESIS_HASH = "0" * 64
# separazione di dominio della chiave del registro (decisione owner PF-BB1-R2-FINAL-CYCLE-10-20261109): la chiave degli
# alias firma solo gli alias; le impronte degli errori usano una chiave derivata con questo contesto esplicito
FINGERPRINT_KEY_CONTEXT = b"pickfair-blackbox/v1/error-fingerprint-key"
# errno di un lock gia' tenuto da un altro processo: flock (EWOULDBLOCK/EAGAIN), msvcrt.LK_NBLCK (EACCES, EDEADLOCK)
_LOCK_BUSY_ERRNOS = frozenset(e for e in (getattr(errno, n, None) for n in ("EWOULDBLOCK", "EAGAIN", "EACCES",
                                                                              "EDEADLK", "EDEADLOCK")) if e is not None)
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


def _event_epoch(rec: Dict[str, Any], fallback: float) -> float:
    """Istante (epoch) di un record: il suo `ts_utc` (anche se fornito dal chiamante, es. l'ATTEMPT di un emergency
    stop), mai successivo a ``fallback``; `ts_utc` illeggibile => ``fallback``."""
    try:
        ts = _dt.datetime.strptime(str(rec.get("ts_utc")), "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=_dt.timezone.utc)
        return min(float(fallback), ts.timestamp())
    except (TypeError, ValueError):
        return float(fallback)


def private_file(path: str) -> None:
    """R2/permessi (decisioni owner PF-BB1-SOL-R2-NEEDS-MANUAL-20261109 e PF-BB1-R2-FINAL-CYCLE-10-20261109): file del
    registro leggibili solo dal proprietario su POSIX (0600). Crea il file se manca e restringe i permessi di un file
    gia' esistente con `fchmod` sul descrittore aperto con `O_NOFOLLOW`: un symlink al posto del file viene RIFIUTATO
    (OSError), mai seguito, e la sua destinazione non viene toccata; lo stesso per un hardlink o un file non
    regolare (ciclo 11 #617), rifiutati prima di `fchmod`. Su Windows `chmod` non gestisce le ACL: il file
    eredita quelle della cartella dati (verifica delle ACL NOT_RUN in questo ambiente)."""
    if os.name != "posix":
        return
    if os.path.islink(path):
        raise OSError(errno.ELOOP, "file del registro sostituito da un symlink: rifiutato", path)
    fd = os.open(path, os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0), 0o600)
    try:
        st = os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink > 1:   # ciclo 11: hardlink/FIFO mai chmod-ati ne' usati
            raise OSError(errno.ELOOP, "file del registro non privato (hardlink o non regolare): rifiutato", path)
        os.fchmod(fd, 0o600)
    finally:
        os.close(fd)


def utc_iso(ts: float) -> str:
    """ISO-8601 UTC al microsecondo, con `Z`."""
    return _dt.datetime.fromtimestamp(float(ts), tz=_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _redact(value: Any, key: str = "") -> Any:
    """Redazione ricorsiva: valore intero per chiavi sensibili, segreti registrati e token nelle stringhe."""
    if key and redaction.is_sensitive_key(key):
        return REDACTED
    if isinstance(value, str):
        return text_redaction.redact_secrets(value)
    if isinstance(value, dict):
        # Sol #617 (761a52f2): anche una chiave non stringa diventa testo redatto, mai un segreto in chiaro
        return {text_redaction.redact_secrets(k if isinstance(k, str) else str(k)): _redact(v, str(k))
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in value]
    if isinstance(value, float) and value != value:          # NaN non e' JSON canonico
        return None
    if value is None or isinstance(value, (bool, int, float)):
        return value
    # Sol #617: tipi non JSON (bytes, oggetti...) diventerebbero testo solo DOPO la redazione (`default=str` di
    # `canonical_json`), lasciando in chiaro un segreto registrato. Si convertono qui e si redigono.
    if isinstance(value, (bytes, bytearray)):
        value = bytes(value).decode("utf-8", errors="replace")
    return text_redaction.redact_secrets(str(value))


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
        self._outage_flushed: Optional[List[Dict[str, Any]]] = None   # uscite gia' scaricate nel guasto corrente
        self.last_error: Optional[str] = None
        self._alias_key: Optional[bytes] = None
        self._writer_lock = None                       # file lock del runtime (un solo scrittore per registro)

    # ------------------------------------------------------------------ apertura e schema
    def open(self) -> "BlackBox":
        with self._lock:
            if self._conn is not None:
                return self
            parent = os.path.dirname(self.path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            if self.path != ":memory:":
                private_file(self.path)        # SQLite crea -wal/-shm con gli stessi permessi del database
                for side in ("-wal", "-shm"):
                    if os.path.exists(self.path + side):
                        private_file(self.path + side)
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
            # chiave dell'alias esterno di order_id (D1): casuale, per registro, persistita => alias stabile ai riavvii
            conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES ('order_alias_key', ?)", (secrets.token_hex(32),))
            self._alias_key = bytes.fromhex(conn.execute(
                "SELECT value FROM meta WHERE key = 'order_alias_key'").fetchone()["value"])
            self._conn = conn
            return self

    def order_alias(self, order_id: Any) -> Optional[str]:
        """Alias esterno stabile di un order_id del ledger (D1): `ord_` + HMAC-SHA256 con la chiave del registro.
        Non enumerabile (l'id progressivo non si ricava dall'alias), uguale per lo stesso ordine dopo un riavvio."""
        if order_id is None:
            return None
        self.open()
        return "ord_" + hmac.new(self._alias_key, str(order_id).encode("utf-8"), hashlib.sha256).hexdigest()[:32]

    def fingerprint(self, text: str) -> str:
        """R2 (Claude D su #617, decisione owner PF-BB1-R2-FINAL-CYCLE-10-20261109): impronta di un testo NON registrato
        (messaggio di eccezione, errore). HMAC-SHA256 con una chiave DERIVATA, separata per dominio da quella degli
        alias degli ordini: k_fp = HMAC-SHA256(order_alias_key, FINGERPRINT_KEY_CONTEXT). Stabile nello stesso
        registro, diversa fra registri con chiavi diverse, inutile per un controllo a dizionario su un export senza la
        chiave. NON protegge da chi legge il file del registro, che contiene anche la chiave. Registro non aperto
        => ''."""
        if not text or self._alias_key is None:
            return ""
        key = hmac.new(self._alias_key, FINGERPRINT_KEY_CONTEXT, hashlib.sha256).digest()
        return hmac.new(key, str(text).encode("utf-8", "replace"), hashlib.sha256).hexdigest()[:32]

    def acquire_runtime(self) -> None:
        """Un solo runtime finanziario per registro (D3): lock esclusivo del file `<registro>.runtime.lock`, tenuto
        fino a `close`. Un secondo runtime riceve RuntimeAlreadyActive con motivo esplicito. I lettori (Control
        API/MCP) non lo chiamano mai. Non certifica il fencing finanziario del motore (#536, separato)."""
        if self._writer_lock is not None:
            return
        private_file(self.path + ".runtime.lock")
        fh = open(self.path + ".runtime.lock", "a+")
        try:
            if os.name == "nt":
                import msvcrt
                # Grok #617 (374ce396): il byte bloccato deve esistere. Un file appena creato e' vuoto: si scrive un
                # byte PRIMA del lock (chi tiene gia' il lock lo ha scritto prima di prenderlo, quindi non si tocca
                # mai un file bloccato); dopo il lock il file non si tocca piu'.
                if os.path.getsize(fh.name) == 0:
                    fh.write("\n")
                    fh.flush()
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            fh.close()
            # Grok #617: solo "gia' bloccato" e' un altro runtime; ogni altro errore e' un registro non disponibile
            # (l'avvio resta rifiutato in entrambi i casi, ma con il motivo giusto).
            if exc.errno in _LOCK_BUSY_ERRNOS:
                raise RuntimeAlreadyActive(
                    f"RUNTIME_ALREADY_ACTIVE: un altro runtime Pickfair scrive gia' {self.path}") from exc
            raise BlackBoxUnavailable(f"RUNTIME_LOCK_FAILED: {type(exc).__name__}: {exc}") from exc
        # il file di lock non si tocca piu' dopo il lock (su Windows il lock e' una regione del file, rilievo Grok
        # #617): il proprietario va in un file separato, solo informativo
        try:
            private_file(self.path + ".runtime.owner")
            with open(self.path + ".runtime.owner", "w") as owner:
                owner.write(f"pid={self.pid} session={self.session_id}\n")
        except OSError:
            pass
        self._writer_lock = fh

    def is_runtime(self) -> bool:
        """Vero se questa istanza e' il runtime che scrive il registro (tiene il lock di D3)."""
        return self._writer_lock is not None

    def close(self) -> None:
        with self._lock:
            if self._writer_lock is not None:
                try:
                    self._writer_lock.close()          # chiudere il file rilascia il lock (POSIX e Windows)
                finally:
                    self._writer_lock = None
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
        if "order_alias" in kw:
            raise ValueError("blackbox: order_alias e' calcolato dal registro, mai fornito dal chiamante")
        for f in ("ts_utc", "exchange_ts"):            # D2: un orario fornito deve essere UTC canonico
            if kw.get(f) is not None:
                check_utc_ts(kw[f], f)
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
        rec["order_alias"] = self.order_alias(rec["order_id"])
        for f in _JSON_FIELDS:
            rec[f] = _redact(rec[f]) if rec[f] is not None else None
        for f in RECORD_FIELDS:
            if f not in _JSON_FIELDS and rec[f] is not None and not isinstance(rec[f], (str, bool, int, float)):
                # Sol #617 (761a52f2): bytes o oggetti in un campo testuale (es. `reason`) finivano come BLOB non
                # redatto. Diventano testo e passano dalla redazione come ogni stringa.
                value = rec[f]
                rec[f] = bytes(value).decode("utf-8", errors="replace") if isinstance(value, (bytes, bytearray)) \
                    else str(value)
            if f not in _JSON_FIELDS and isinstance(rec[f], str):
                rec[f] = text_redaction.redact_secrets(rec[f])
            if f in ("table_id", "market_id", "selection_id", "order_id", "order_ref", "bet_id") and rec[f] is not None:
                rec[f] = str(rec[f])
        return rec

    def _insert(self, rec: Dict[str, Any]) -> int:
        conn = self._conn
        if conn is None:
            raise sqlite3.OperationalError("blackbox non aperta")
        saved_session_seq = self._session_seq      # ripristino esatto, mai un decremento alla cieca (Codex #617)
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
            self._session_seq = saved_session_seq
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
        attempted_at = float(self._clock())               # istante dell'evento, PRIMA di qualunque attesa del lock
        rec = self._build(event_type, dict(fields))      # errori di forma: sempre sollevati (bug del chiamante)
        with self._lock:
            try:
                if self._buffer or self._degraded_since is not None:
                    self._flush_after_outage()
                self._insert(rec)
                return rec["event_id"]
            except (sqlite3.Error, OSError) as exc:
                self._enter_degraded(exc, since=_event_epoch(rec, attempted_at))
                if critical and buffer_on_failure and len(self._buffer) < MAX_CRITICAL_BUFFER:
                    self._buffer.append(rec)
                    return rec["event_id"]
                if critical:
                    raise BlackBoxUnavailable(f"{type(exc).__name__}: {exc}") from exc
                self.dropped += 1
                return None

    def _enter_degraded(self, exc: BaseException, *, since: Optional[float] = None) -> None:
        """Sol #617 (761a52f2): la finestra della GAP parte dall'istante dell'evento non scritto, non dalla rilevazione
        dell'errore (che puo' arrivare dopo l'attesa del lock SQLite, fino a 30 s): una query sull'intervallo
        dell'evento perso trova sempre la lacuna."""
        self.last_error = f"{type(exc).__name__}: {exc}"
        start = float(self._clock()) if since is None else float(since)
        if self._degraded_since is None or start < self._degraded_since:
            self._degraded_since = start

    def _flush_after_outage(self) -> None:
        """Al ripristino: eventi bufferizzati (ora originale, `buffered=true`), poi GAP con intervallo e ref, poi
        RECOVERED. Se fallisce, solleva: il buffer resta intatto."""
        start = self._degraded_since if self._degraded_since is not None else float(self._clock())
        # ref e conteggi CUMULATIVI del guasto: uno scarico riuscito a meta' (prima uscita scritta, seconda fallita)
        # non li perde, finche' GAP e RECOVERED non sono scritti (rilievo Codex #617)
        if self._outage_flushed is None:
            self._outage_flushed = []
        flushed = self._outage_flushed
        for rec in list(self._buffer):
            rec = dict(rec)
            rec["data"] = {**(rec.get("data") or {}), "buffered": True}
            self._insert(rec)
            self._buffer.pop(0)
            flushed.append(rec)
        pending = list(flushed)
        refs = sorted({str(r["order_ref"]) for r in pending if r.get("order_ref")})
        gap = self._build("BLACKBOX_GAP", dict(
            source="SYSTEM", component="blackbox", severity="ERROR", phase="OBSERVATION",
            # UNKNOWN anche senza eventi persi: nella finestra il registro non era disponibile, quindi cosa sia
            # successo non e' verificato (stessa semantica della GAP di UNCLEAN_TERMINATION; contratto Claude C, I2)
            outcome="UNKNOWN", reason_code="BLACKBOX_OUTAGE",
            reason=self.last_error, data={"from_ts": utc_iso(start), "to_ts": utc_iso(self._clock()), "refs": refs,
                                          "buffered": len(pending), "dropped_non_critical": self.dropped}))
        self._insert(gap)
        self._insert(self._build("BLACKBOX_RECOVERED", dict(source="SYSTEM", component="blackbox",
                                                            phase="OBSERVATION", outcome="VERIFIED")))
        self._degraded_since, self.dropped = None, 0
        self._outage_flushed = None

    # ------------------------------------------------------------------ sessione
    def start_session(self, **data: Any) -> str:
        """Apre la sessione: se la precedente non ha chiuso con SHUTDOWN registra UNCLEAN_TERMINATION e una GAP
        UNKNOWN sull'intervallo, poi PROCESS_START. Evento critico: senza registro il processo non parte."""
        self.open()
        self.acquire_runtime()                         # D3: prima di leggere lo stato della sessione precedente
        # solo gli eventi del CICLO DI VITA del runtime decidono se la sessione precedente e' terminata: un lettore
        # (Control API/MCP) scrive eventi propri dopo lo SHUTDOWN e non e' un runtime (rilievo Codex #617, D3)
        with self._lock:
            life = self._conn.execute(
                "SELECT seq, session_id, type, ts_utc, data FROM events"
                " WHERE type IN ('PROCESS_START', 'SHUTDOWN', 'UNCLEAN_TERMINATION') ORDER BY seq DESC LIMIT 1").fetchone()
            last = None
            gap_from = None
            if life is not None and life["type"] == "PROCESS_START":
                last = self._conn.execute(
                    "SELECT session_id, type, ts_utc FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1",
                    (life["session_id"],)).fetchone()
            elif life is not None and life["type"] == "UNCLEAN_TERMINATION":
                # recupero riprendibile (rilievo Sol #617): UNCLEAN_TERMINATION scritto ma GAP mancante (guasto fra i
                # due record) => la GAP UNKNOWN si scrive ora, l'intervallo non resta mai senza lacuna dichiarata
                has_gap = self._conn.execute(
                    "SELECT 1 FROM events WHERE seq > ? AND type = 'BLACKBOX_GAP' AND reason_code = 'UNCLEAN_TERMINATION'"
                    " LIMIT 1", (life["seq"],)).fetchone()
                if has_gap is None:
                    try:
                        gap_from = (json.loads(life["data"]) if life["data"] else {}).get("from_ts")
                    except ValueError:
                        gap_from = None
                    gap_from = gap_from or life["ts_utc"]
        if last is not None:
            self.record("UNCLEAN_TERMINATION", critical=True, source="SYSTEM", component="blackbox",
                        severity="ERROR", phase="OBSERVATION", outcome="VERIFIED",
                        reason_code="NO_SHUTDOWN_RECORDED", data={"previous_session": last["session_id"],
                                                                  "last_event_type": last["type"],
                                                                  "from_ts": last["ts_utc"]})
            gap_from = last["ts_utc"]
        if gap_from is not None:
            self.record("BLACKBOX_GAP", critical=True, source="SYSTEM", component="blackbox", severity="ERROR",
                        phase="OBSERVATION", outcome="UNKNOWN", reason_code="UNCLEAN_TERMINATION",
                        data={"from_ts": gap_from, "to_ts": utc_iso(self._clock()), "refs": []})
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

