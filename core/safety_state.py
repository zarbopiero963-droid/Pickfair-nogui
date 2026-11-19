"""M153 (owner, opzione B) — SafetyStateOwner: UNICO proprietario dello stato di sicurezza del runtime.

Causa comune dei P1 di #638 (CX-M1, CX-M2, C02, C12, CX-N1, L1/L2, K2): uno stesso stato di sicurezza scritto da molti
punti sotto cinque lock diversi. Qui:

* un solo `RLock` FOGLIA (`lock`), che sostituisce `_safety_state_lock` e `_live_accounts_lock` del runtime: dentro, solo
  I/O su db e file; mai rete, `bus.publish` o altri lock (in modalita' test il tracciatore fallisce su ogni acquisizione
  di un altro lock tracciato mentre questo e' tenuto);
* ogni scrittura dello stato in memoria passa da una TRANSIZIONE DICHIARATA (`apply`), che elenca i campi che puo'
  toccare: un campo fuori dalla transizione e' un errore di programmazione (ValueError), mai una scrittura silenziosa;
* il giornale di sicurezza `<db>.safety_pending.json` e' un'ISTANTANEA completa dello stato non durevole, con la chiave
  `journal_seq`, letta, scritta e rimossa SOLO qui sotto `lock` (CX-M1 e C02: nessuna lettura-modifica-scrittura
  concorrente, nessun `os.remove` dopo una riscrittura dell'ES). Un giornale illeggibile non si riscrive mai (M152,
  CX-L1). Il formato su disco resta quello di M151/M152 (chiavi in piano): un giornale precedente si legge com'e' e la
  prima scrittura successiva lo riscrive per intero con `journal_seq`, senza perdere nessuna chiave;
* le chiavi di sicurezza sul db si scrivono solo da qui (`save`, `durable_save`), e i marker dello stop di rischio solo
  tramite `persist_risk_stop`;
* `seq` cresce a ogni transizione; `es_seq` a ogni dichiarazione dell'Emergency Stop (P7: una rimozione del marker
  dell'ES dal settlement si scrive solo se nessun ES e' stato dichiarato dopo l'apertura del settlement);
* P8: `initialized` falso durante la ricostruzione all'avvio => ingressi, reset e sweep rifiutati.

La guardia AST `tests/safety/test_m153_safety_state_owner.py` fallisce su ogni scrittura di questi campi, chiavi, marker o
del giornale fuori da questo modulo."""
from __future__ import annotations

import json
import logging
import os
import threading
from typing import Any, Callable, Dict, Iterable, Optional

import core.atomic_io as atomic_io

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------------------------- stato posseduto
FIELDS = frozenset({
    "_emergency_stopped", "_emergency_stopped_at", "_emergency_reason", "_settlement_source_before_emergency",
    "_risk_stop_reason", "_live_verify_only", "_hedge_guard", "_session_loss_pending_global",
    "_daily_loss_pending_stop", "_emergency_live_accounts", "_live_breaches", "_pending_live_bindings",
    "_emergency_reconcile_pending", "_live_binding_failed", "_live_exposure_record",
})

_ES = frozenset({"_emergency_stopped", "_emergency_stopped_at", "_emergency_reason",
                 "_settlement_source_before_emergency"})
TRANSITIONS: Dict[str, frozenset] = {
    "init": FIELDS,                                                       # costruzione del runtime
    "declare_es": _ES | {"_live_verify_only"},
    "reset_es": _ES,
    "restore_es": _ES | {"_emergency_reconcile_pending"},                 # ricostruzione all'avvio / recupero
    "set_risk_stop": frozenset({"_risk_stop_reason"}),
    "reset_risk_stop": frozenset({"_risk_stop_reason", "_session_loss_pending_global"}),
    "set_verify_only": frozenset({"_live_verify_only", "_hedge_guard"}),
    "registry": frozenset({"_emergency_live_accounts", "_live_breaches", "_live_binding_failed"}),
    "defer_binding": frozenset({"_pending_live_bindings"}),
    "session_barrier": frozenset({"_session_loss_pending_global"}),
    "daily_pending_stop": frozenset({"_daily_loss_pending_stop"}),
    "reconcile_pending": frozenset({"_emergency_reconcile_pending"}),
    "exposure": frozenset({"_live_exposure_record"}),
}
_ES_DECLARING = frozenset({"declare_es"})

# chiavi del db che solo il proprietario scrive
DB_KEYS = frozenset({
    "emergency_stopped", "emergency_stopped_at", "emergency_reason", "emergency_settlement_source",
    "emergency_hedge_guard", "emergency_live_verify_only", "emergency_live_accounts",
    "emergency_live_pending_bindings", "emergency_unresolved_exposure", "risk_stop_state",
    "betfair_authorized_identity", "session_loss_stop_pending",
})

JOURNAL_SUFFIX = ".safety_pending.json"
JOURNAL_SEQ_KEY = "journal_seq"


# ------------------------------------------------------------------------------------------------- ordine dei lock
LOCK_ORDER_CHECK = False          # True in modalita' test (tests/conftest.py): violazioni registrate in VIOLATIONS
VIOLATIONS: list = []             # il test che le produce fallisce (fixture di tests/conftest.py)


_held = threading.local()


def _stack() -> list:
    st = getattr(_held, "stack", None)
    if st is None:
        st = _held.stack = []
    return st


class TrackedLock:
    """Lock (o RLock) con nome. In modalita' test registra i lock tenuti dal thread e registra una violazione se un lock
    tracciato qualunque viene acquisito mentre il thread tiene il lock FOGLIA del proprietario (deadlock possibile).
    Fuori dai test il controllo costa una lettura di flag."""

    def __init__(self, name: str, inner: Any = None, leaf: bool = False):
        self.name, self.leaf = name, leaf
        self._inner = inner if inner is not None else threading.RLock()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        if LOCK_ORDER_CHECK:
            st = _stack()
            if any(lk.leaf and lk is not self for lk in st):
                # registrata, mai sollevata: non cambia il flusso del codice sotto test (gestori `except Exception`)
                VIOLATIONS.append(f"lock '{self.name}' acquisito tenendo il lock foglia del proprietario "
                                  f"(stack: {[lk.name for lk in st]})")
        got = self._inner.acquire(blocking, timeout) if timeout != -1 else self._inner.acquire(blocking)
        if got and LOCK_ORDER_CHECK:
            _stack().append(self)
        return got

    def release(self) -> None:
        if LOCK_ORDER_CHECK:
            st = _stack()
            for i in range(len(st) - 1, -1, -1):
                if st[i] is self:
                    del st[i]
                    break
        self._inner.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False

    def locked(self) -> bool:
        probe = getattr(self._inner, "locked", None)
        return bool(probe()) if callable(probe) else False


def tracked(rc: Any, attr: str, factory: Callable[[], Any], name: str) -> TrackedLock:
    """Lock tracciato dell'istanza (creato una volta, anche su istanze costruite con `__new__`)."""
    current = rc.__dict__.get(attr)
    if isinstance(current, TrackedLock):
        return current
    return rc.__dict__.setdefault(attr, TrackedLock(name, factory()))


# ------------------------------------------------------------------------------------------------- proprietario
class SafetyStateOwner:
    def __init__(self, initialized: bool = True):
        self.lock = TrackedLock("safety_state_owner", threading.RLock(), leaf=True)
        self.seq = 0
        self.es_seq = 0
        self.initialized = bool(initialized)

    # -- creazione / aggancio
    @staticmethod
    def of(rc: Any) -> "SafetyStateOwner":
        owner = rc.__dict__.get("_safety_owner")
        if not isinstance(owner, SafetyStateOwner):
            owner = rc.__dict__.setdefault("_safety_owner", SafetyStateOwner(initialized=True))
        # un solo dominio di sincronizzazione: i nomi storici dei lock dello stato di sicurezza sono lo STESSO lock
        rc.__dict__["_safety_state_lock"] = owner.lock
        rc.__dict__["_live_accounts_lock"] = owner.lock
        return owner

    @staticmethod
    def install(rc: Any) -> "SafetyStateOwner":
        """P8: proprietario NON inizializzato (ricostruzione all'avvio in corso): blocco finche' `mark_initialized`."""
        owner = SafetyStateOwner(initialized=False)
        rc.__dict__["_safety_owner"] = owner
        return SafetyStateOwner.of(rc)

    def mark_initialized(self) -> None:
        with self.lock:
            self.initialized = True

    # -- transizioni
    def apply(self, rc: Any, transition: str, **fields: Any) -> int:
        allowed = TRANSITIONS.get(transition)
        if allowed is None:
            raise ValueError(f"transizione non dichiarata: {transition}")
        extra = set(fields) - set(allowed)
        if extra:
            raise ValueError(f"campi {sorted(extra)} fuori dalla transizione {transition}")
        with self.lock:
            for name, value in fields.items():
                rc.__dict__[name] = value
            self.seq += 1
            if transition in _ES_DECLARING and fields.get("_emergency_stopped"):
                self.es_seq += 1
            return self.seq

    def update_dict(self, rc: Any, transition: str, field: str, fn: Callable[[dict], Optional[dict]]) -> dict:
        """Lettura-modifica-scrittura di un campo dizionario su una COPIA, sotto il lock; `fn` riceve la copia e la
        modifica (o ne restituisce una nuova). Mai una mutazione in posto dello stato posseduto fuori da qui."""
        with self.lock:
            current = rc.__dict__.get(field)
            copy = {k: (list(v) if isinstance(v, list) else dict(v) if isinstance(v, dict) else v)
                    for k, v in (current or {}).items()} if isinstance(current, dict) else {}
            out = fn(copy)
            new = copy if out is None else out
            self.apply(rc, transition, **{field: new})
            return new

    # -- db
    def save(self, rc: Any, data: dict) -> Any:
        with self.lock:
            return rc.db.save_settings(dict(data))

    def durable_save(self, rc: Any, data: dict, drop: Optional[Callable[[list], bool]] = None) -> str:
        """Salva e RILEGGE; '' se ogni chiave e' sul db identica, altrimenti l'errore. Una scrittura durevole delle chiavi
        ne toglie le copie dal giornale. Mai raise."""
        with self.lock:
            try:
                rc.db.save_settings(dict(data))
                readback = rc.db.get_settings() if hasattr(rc.db, "get_settings") else None
                if not isinstance(readback, dict):
                    raise OSError("rilettura non disponibile")
                diverse = sorted(k for k, v in data.items() if str(readback.get(k) or "") != str(v or ""))
                if diverse:
                    raise OSError(f"chiavi non rilette: {diverse}")
            except Exception as exc:
                return str(exc) or exc.__class__.__name__
            dropped = drop(list(data)) if drop is not None else self.journal_drop(rc, list(data))
            return "" if dropped else "giornale di sicurezza non ripulito"

    def persist_risk_stop(self, rc: Any, reason: str, **kw: Any) -> Any:
        from core import risk_stop as stop_rules
        with self.lock:
            return stop_rules.persist_risk_stop(rc.db, reason, **kw)

    # -- giornale (istantanea con seq)
    @staticmethod
    def journal_path(rc: Any) -> str:
        try:
            path = os.fspath(getattr(rc.db, "db_path", None))
        except TypeError:
            return ""
        if not isinstance(path, str) or path in ("", ":memory:"):
            return ""
        return path + JOURNAL_SUFFIX

    def journal_read(self, rc: Any) -> Optional[dict]:
        """Stato del giornale ({} se assente, senza `journal_seq`); None se presente ma illeggibile."""
        path = SafetyStateOwner.journal_path(rc)
        if not path or not os.path.exists(path):
            return {}
        with self.lock:
            try:
                with open(path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
            except Exception:
                logger.exception("M153: giornale di sicurezza illeggibile")
                return None
            if not isinstance(data, dict):
                return None
            return {k: v for k, v in data.items() if k != JOURNAL_SEQ_KEY}

    def _journal_write(self, path: str, state: dict) -> None:
        self.seq += 1
        atomic_io.atomic_write_text(path, json.dumps(dict(state, **{JOURNAL_SEQ_KEY: str(self.seq)}), sort_keys=True))

    def journal_merge(self, rc: Any, payload: dict, reader: Optional[Callable[[], Optional[dict]]] = None) -> bool:
        """Unisce `payload` allo stato del giornale e scrive l'istantanea, sotto il lock (lettura e scrittura nella stessa
        sezione critica). `reader` = lettura del chiamante (stesso confine di prima). Giornale illeggibile (anche al
        secondo tentativo) => MAI riscritto (M152, CX-L1), False. Mai raise."""
        path = SafetyStateOwner.journal_path(rc)
        if not path:
            return False
        read = reader or (lambda: self.journal_read(rc))
        with self.lock:
            try:
                current = read()
                if current is None:
                    current = read()                           # una lettura transitoria si ritenta
                if current is None:
                    logger.critical("M152: giornale di sicurezza illeggibile: nessuna riscrittura (stato solo in "
                                    "memoria)")
                    return False
                current.update({k: str(v or "") for k, v in payload.items()})
                self._journal_write(path, current)
                logger.critical("M151: stato di sicurezza non durevole sul db: scritto nel giornale %s", path)
                return True
            except Exception:
                logger.exception("M151: giornale di sicurezza non scrivibile (stato solo in memoria)")
                return False

    def journal_drop(self, rc: Any, keys: Iterable[str],
                     reader: Optional[Callable[[], Optional[dict]]] = None) -> bool:
        """Toglie `keys` dal giornale sotto il lock (rimozione del file se vuoto). Illeggibile => resta, False."""
        path = SafetyStateOwner.journal_path(rc)
        if not path or not os.path.exists(path):
            return True
        read = reader or (lambda: self.journal_read(rc))
        with self.lock:
            try:
                current = read()
                if current is None:
                    return False                          # illeggibile: resta (fail-closed all'avvio)
                drop = set(keys)
                remaining = {k: v for k, v in current.items() if k not in drop}
                if remaining == current:
                    return True
                if remaining:
                    self._journal_write(path, remaining)
                else:
                    os.remove(path)
                return True
            except Exception:
                logger.exception("M151: giornale di sicurezza non ripulito")
                return False
