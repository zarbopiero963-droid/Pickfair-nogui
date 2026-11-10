"""PF-BLACKBOX-01 (BB-1): la scatola nera del PROCESSO.

Installata dall'entrypoint (`headless_main.main`) PRIMA di costruire qualunque componente operativo: il primo evento
del processo e' `PROCESS_START`. Un componente registra con `record(...)`, che non fa nulla se la scatola nera non e'
installata (test, strumenti) e non solleva mai per un evento non critico.

Cattura dei crash:
- `sys.excepthook` / `threading.excepthook`: `UNCAUGHT_EXCEPTION` / `TASK_EXCEPTION` (tipo, messaggio e traceback
  redatti e troncati), poi l'hook precedente;
- `faulthandler` su `<cartella della scatola nera>/fault.log` per i crash nativi (segfault, abort): al riavvio
  l'ultima parte del file entra in `UNCLEAN_TERMINATION` e il file viene ruotato;
- crash senza traccia (kill -9, perdita di corrente): `UNCLEAN_TERMINATION` + `BLACKBOX_GAP` UNKNOWN al riavvio
  (`BlackBox.start_session`).
"""

from __future__ import annotations

import errno
import faulthandler
import os
import sys
import threading
import traceback
from typing import Any, Optional

from core import blackbox as bb

_box: Optional[bb.BlackBox] = None
_fault_file = None
_prev_excepthook = None
_prev_threading_hook = None
_fault_path = ""                                 # percorso di fault.log (il file e' aperto da descrittore)
_keep_fault_file = False                         # fault.log contiene un dump non ancora consumato: non rimuoverlo
MAX_TRACE_CHARS = 8000
FAULT_TAIL_BYTES = 4096


def get() -> Optional[bb.BlackBox]:
    return _box


def record(event_type: str, **fields: Any) -> Optional[str]:
    """Registra sulla scatola nera del processo, se installata. Gli eventi `critical=True` sollevano come in
    `BlackBox.record`; quelli non critici non sollevano mai."""
    box = _box
    if box is None:
        if fields.get("critical"):
            raise bb.BlackBoxUnavailable("scatola nera non installata")
        return None
    return box.record(event_type, **fields)


def safe_text(text: Any, limit: int, *, tail: bool = False) -> str:
    """Testo redatto PRIMA di essere troncato: un segreto a cavallo del limite non lascia un frammento non piu'
    riconoscibile dalla redazione (rilievo Codex #617)."""
    from core import text_redaction
    clean = text_redaction.redact_secrets(str(text))
    return clean[-limit:] if tail else clean[:limit]


MAX_TRACE_FRAMES = 40


def exception_reason(exc: Any) -> str:
    """R2 (decisione owner PF-BB1-SOL-R2-NEEDS-MANUAL-20261109): il `reason` persistito di un'eccezione e' solo il suo
    tipo. Il testo del messaggio e' arbitrario (puo' contenere credenziali mai registrate) e non si bonifica con
    garanzie sufficienti: non viene registrato."""
    t = type(exc) if isinstance(exc, BaseException) else None
    return f"{t.__module__}.{t.__qualname__}" if t is not None else "unknown"


def exception_diagnostics(exc_type, exc, tb) -> dict:
    """R2: diagnostica STRUTTURATA di un'eccezione per la ricostruzione del crash, senza testo arbitrario: tipo,
    catena delle cause, frame (file, riga, funzione; mai il sorgente ne' le variabili) e un'impronta del messaggio
    (lunghezza + HMAC con la chiave del registro) per correlare due occorrenze senza conservarne il contenuto."""
    out = {"exception_type": getattr(exc_type, "__qualname__", str(exc_type)),
           "exception_module": getattr(exc_type, "__module__", ""), "message_recorded": False}
    try:
        msg = str(exc)
    except Exception:
        msg = ""
    out["message_len"] = len(msg)
    out["message_fingerprint"] = _fingerprint(msg)
    frames = [{"file": f.filename, "line": f.lineno, "function": f.name}
              for f in traceback.extract_tb(tb)][-MAX_TRACE_FRAMES:] if tb is not None else []
    out["frames"] = frames
    chain, cur, seen = [], exc, set()
    while isinstance(cur, BaseException) and id(cur) not in seen and len(chain) < 5:
        seen.add(id(cur))
        nxt = cur.__cause__ or cur.__context__
        if nxt is not None:
            chain.append(exception_reason(nxt))
        cur = nxt
    out["cause_chain"] = chain
    return out


def _fingerprint(text: str) -> str:
    """Impronta HMAC con la chiave del registro installato (`BlackBox.fingerprint`); senza registro => ''."""
    box = _box
    try:
        return box.fingerprint(text) if box is not None and text else ""
    except Exception:
        return ""


# Codici tecnici fissi (non testo di eccezione) che restano leggibili nei campi d'errore: la classificazione serve
# all'audit e non contiene dati (ciclo 11 #617).
KNOWN_ERROR_CODES = frozenset({"db_save_settings_unavailable"})


def _is_error_key(key: Any) -> bool:
    k = str(key).lower()
    return k in ("error", "errors", "exception", "message") or k.endswith(("_error", "_errors"))


# Dentro un contenitore d'errore (es. `cancel_errors[]`) restano in chiaro solo questi campi strutturali, che
# classificano l'errore senza testo libero; ogni altra stringa diventa un'impronta (allowlist, fail-closed).
_ERROR_STRUCT_KEYS = frozenset({"stage", "market_id", "bet_id", "code", "error_code", "classification", "outcome",
                                "status", "ok", "order_count"})


def _scrub_error_value(v: Any) -> Any:
    """Tutto cio' che sta sotto una chiave d'errore e' testo arbitrario: ogni stringa (anche in liste e dizionari
    annidati) diventa lunghezza + impronta, tranne la stringa vuota, i codici tecnici fissi e i campi strutturali
    in allowlist dei dizionari d'errore."""
    if isinstance(v, str):
        if not v or v in KNOWN_ERROR_CODES:
            return v
        return {"redacted": True, "len": len(v), "fingerprint": _fingerprint(v)}
    if isinstance(v, dict):
        return {k: (x if str(k).lower() in _ERROR_STRUCT_KEYS and not isinstance(x, (dict, list, tuple))
                    else _scrub_error_value(x)) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_scrub_error_value(x) for x in v]
    return v


def scrub_error_texts(value: Any) -> Any:
    """R2: nei risultati strutturati (es. `emergency_stop()`), i campi d'errore (`error`, `errors`, `exception`,
    `message` e ogni `*_error`/`*_errors`, es. `persist_error`) contengono testo di eccezioni o di API arbitrario:
    diventano un'impronta (lunghezza + HMAC con la chiave del registro), a ogni profondita'. Il resto (id, stati,
    esiti, conteggi) resta per l'audit."""
    if isinstance(value, dict):
        return {k: (_scrub_error_value(v) if _is_error_key(k) else scrub_error_texts(v)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [scrub_error_texts(v) for v in value]
    return value


def _excepthook(exc_type, exc, tb):
    try:
        record("UNCAUGHT_EXCEPTION", source="SYSTEM", component="process", severity="CRITICAL",
               phase="OBSERVATION", outcome="VERIFIED", reason_code=getattr(exc_type, "__name__", "Exception"),
               reason=exception_reason(exc), data=exception_diagnostics(exc_type, exc, tb))
    except Exception:
        pass
    (_prev_excepthook or sys.__excepthook__)(exc_type, exc, tb)


def _threading_hook(args):
    try:
        record("TASK_EXCEPTION", source="SYSTEM", component=f"thread:{getattr(args.thread, 'name', '?')}",
               severity="CRITICAL", phase="OBSERVATION", outcome="VERIFIED",
               reason_code=getattr(args.exc_type, "__name__", "Exception"), reason=exception_reason(args.exc_value),
               data=exception_diagnostics(args.exc_type, args.exc_value, args.exc_traceback))
    except Exception:
        pass
    if _prev_threading_hook is not None:
        _prev_threading_hook(args)


def _drop_if_not_private(path: str) -> None:
    """Ciclo 11 #617 (Codex P2): un symlink o un hardlink al posto di un file del dump nativo viene RIMOSSO (solo il
    nome nella cartella della scatola nera: il bersaglio resta identico), mai letto, accodato o ruotato."""
    try:
        st = os.lstat(path)
    except OSError:
        return
    import stat as _stat
    if _stat.S_ISLNK(st.st_mode) or not _stat.S_ISREG(st.st_mode) or st.st_nlink > 1:
        try:
            os.unlink(path)
        except OSError:
            pass


def _is_private(path: str) -> bool:
    """File regolare con un solo nome, non symlink (lstat: nessun link seguito)."""
    import stat as _stat
    try:
        st = os.lstat(path)
    except OSError:
        return False
    return _stat.S_ISREG(st.st_mode) and st.st_nlink == 1


def _open_private(path: str, flags: int) -> int:
    """Apre un file del dump SOLO tramite `blackbox.open_private` (ciclo 12): file regolare con un solo nome, mai un
    link seguito (nemmeno senza `O_NOFOLLOW`, es. Windows: controllo prima e confronto dopo l'apertura), nessun
    blocco su un FIFO, creazione solo esclusiva. Altrimenti OSError, fail-closed."""
    return bb.open_private(path, flags)


def _read_private(path: str) -> bytes:
    fd = _open_private(path, os.O_RDONLY)
    with os.fdopen(fd, "rb") as fh:
        return fh.read()


def _append_private(path: str, data: bytes) -> None:
    fd = _open_private(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT)
    with os.fdopen(fd, "ab") as fh:
        fh.write(data)


def _previous_fault_dump(fault_path: str) -> Optional[str]:
    """Ultima parte dei dump nativi di sessioni precedenti: `fault.log.pending` (in attesa di import) e `fault.log`.
    La LETTURA non dipende dallo spostamento: se `fault.log` non si puo' accodare a `.pending`, viene letto dov'e'.
    Niente viene consumato qui: lo fa `_consume_fault_dump`, solo dopo un CRASH_RECOVERY durevole (rilievi Codex e
    Sol #617). Symlink, hardlink e file non regolari non vengono mai seguiti (ciclo 11)."""
    pending = fault_path + ".pending"
    for p in (fault_path, pending, fault_path + ".1"):
        _drop_if_not_private(p)
    try:                                         # best effort: mettere da parte prima del nuovo faulthandler
        if os.path.exists(fault_path) and os.path.getsize(fault_path) > 0:
            data = _read_private(fault_path)
            _append_private(pending, data)
            os.remove(fault_path)
    except OSError:
        pass                                     # resta in fault.log: lo si legge li', e non lo si tronca mai
    parts = []
    for p in (pending, fault_path):
        try:
            data = _read_private(p)
        except OSError:
            continue
        if data:
            parts.append(data[-FAULT_TAIL_BYTES:].decode("utf-8", errors="replace"))
    return "\n".join(parts) or None


def _runtime_fault_dump(box: bb.BlackBox, fault_path: str) -> Optional[str]:
    """Dump nativi precedenti, SOLO per l'istanza che tiene il lock del runtime (ciclo 14 #617): chi non ha il lock
    (un lettore, un avvio non ammesso) non legge, non sposta e non cancella i file diagnostici di un altro runtime."""
    if not box.is_runtime():
        return None
    return _previous_fault_dump(fault_path)


def _consume_fault_dump(fault_path: str) -> bool:
    """Archivia in `fault.log.1` i dump importati. False se qualcosa resta da consumare (verra' reimportato al
    prossimo avvio: meglio un import doppio che un dump perso)."""
    pending, archive = fault_path + ".pending", fault_path + ".1"
    ok = True
    for p in (pending, archive):
        _drop_if_not_private(p)
    try:
        if os.path.lexists(pending):
            if not _is_private(pending):
                raise OSError(errno.ELOOP, "dump in attesa non privato: non ruotato", pending)
            os.replace(pending, archive)
    except OSError:
        ok = False
    try:
        if os.path.exists(fault_path) and os.path.getsize(fault_path) > 0:
            _append_private(archive, _read_private(fault_path))
            os.remove(fault_path)
    except OSError:
        ok = False
    return ok


# R2 (decisione owner PF-BB1-SOL-R2-NEEDS-MANUAL-20261109): argv persistito con ALLOWLIST. Restano in chiaro solo i
# flag operativi di headless_main, senza valore, e il NOME dei flag noti con valore (il valore e' sempre redatto).
# Ogni altro argomento (posizionale, flag sconosciuto, valore) diventa `[REDACTED]`: un segreto non registrato non
# finisce mai nel registro persistente.
OPERATIONAL_FLAGS = frozenset({"--live", "--live-enabled", "--live-disabled", "--sim", "--simulation", "--preflight",
                               "--emergency-stop", "--telegram-login", "--kill-switch-on", "--kill-switch-off",
                               # forme posizionali accettate da `HeadlessApp._parse_args` (Claude D su #617)
                               "live", "sim", "simulation"})
VALUE_FLAGS = frozenset({"--password", "--api-id", "--token", "--api-key"})


def safe_argv(argv) -> list:
    """Argomenti della riga di comando per PROCESS_START, con allowlist (R2): flag operativi noti in chiaro, flag noti
    con valore come `--flag=[REDACTED]` (anche col valore nell'argomento successivo), tutto il resto `[REDACTED]`."""
    out, expect_value = [], False
    for a in list(argv):
        a = str(a)
        low = a.strip().lower()
        name = low.partition("=")[0]
        if low in OPERATIONAL_FLAGS:
            out.append(low)                              # flag operativo: mai il valore di un flag precedente
            expect_value = False
            continue
        if name in VALUE_FLAGS:
            out.append(f"{name}={bb.REDACTED}")
            expect_value = "=" not in low
            continue
        if expect_value:
            expect_value = False                         # valore del flag noto precedente: gia' rappresentato
            continue
        out.append(bb.REDACTED)                          # posizionale o flag sconosciuto: mai in chiaro
    return out


def install(path: Optional[str] = None, **process_data: Any) -> bb.BlackBox:
    """Apre la scatola nera, registra PROCESS_START (critico: senza registro il processo non parte) e installa gli
    hook dei crash. Idempotente."""
    global _box, _fault_file, _prev_excepthook, _prev_threading_hook
    if _box is not None:
        return _box
    box = bb.BlackBox(path).open()
    fault_path = os.path.join(box.dir_path, "fault.log")   # ciclo 13: nella cartella ancorata, mai per nome
    try:
        box.start_session(argv=safe_argv(sys.argv[1:]), **process_data)
    except BaseException:
        box.close()                     # rifiuto (es. RUNTIME_ALREADY_ACTIVE) o guasto: nessuna risorsa lasciata aperta
        raise                           # e nessun dump toccato: restano dove sono per il prossimo avvio
    # Ciclo 14 (Sol su 8bfb96d3): i dump si toccano solo DOPO il lock del runtime. Prima, un avvio concorrente poi
    # rifiutato spostava o cancellava il `fault.log` gia' aperto dal runtime attivo, che perdeva i dump successivi.
    dump = _runtime_fault_dump(box, fault_path)
    keep_fault = False
    if dump:
        try:
            box.record("CRASH_RECOVERY", critical=True, source="SYSTEM", component="faulthandler", severity="ERROR",
                       phase="OBSERVATION", outcome="VERIFIED", reason_code="NATIVE_CRASH_DUMP",
                       data={"tail": dump})
            keep_fault = not _consume_fault_dump(fault_path)    # solo dopo la scrittura durevole
        except bb.BlackBoxUnavailable:
            # Sol #617 (97b4c8fc): un guasto accertato del registro su un evento critico non lascia partire il
            # processo con un registro degradato. Il dump resta in attesa (`fault.log.pending`) per il prossimo avvio.
            box.close()
            raise
    try:
        # append, mai troncatura: un dump non ancora consumato resta leggibile (rilievo Sol #617)
        _drop_if_not_private(fault_path)
        bb.private_file(fault_path)
        # ciclo 11: aperto senza seguire link (race fra controllo e apertura), solo file regolare con un solo nome
        _fault_file = os.fdopen(_open_private(fault_path, os.O_WRONLY | os.O_APPEND | os.O_CREAT), "a")
        globals()["_fault_path"] = fault_path
        faulthandler.enable(file=_fault_file, all_threads=True)
    except OSError:
        _fault_file = None
    _prev_excepthook, sys.excepthook = sys.excepthook, _excepthook
    _prev_threading_hook, threading.excepthook = threading.excepthook, _threading_hook
    _box = box
    globals()["_keep_fault_file"] = keep_fault or (os.path.exists(fault_path) and os.path.getsize(fault_path) > 0)
    return box


def shutdown(reason: str = "STOP", **data: Any) -> None:
    """Registra SHUTDOWN, chiude il registro e ripristina gli hook. Idempotente."""
    global _box, _fault_file
    box = _box
    if box is None:
        return
    try:
        box.end_session(reason, **data)
    finally:
        try:
            faulthandler.disable()
        except Exception:
            pass
        if _fault_file is not None:
            try:
                _fault_file.close()
                if not _keep_fault_file:           # chiusura pulita: nessun dump da conservare
                    os.remove(_fault_path)
            except OSError:
                pass
            _fault_file = None
        if sys.excepthook is _excepthook:
            sys.excepthook = _prev_excepthook or sys.__excepthook__
        if threading.excepthook is _threading_hook:
            threading.excepthook = _prev_threading_hook or threading.__excepthook__
        box.close()
        _box = None


# ---------------------------------------------------------------------------------------------------------------------
# BB-1G (decisione owner PF-OWNER-BB1G-FULL-GO-20261110): doppio gate fail-closed.
#
# Gate A (avvio): nessun runtime operativo parte senza una scatola nera installata, che tiene il lock del runtime (D3)
# e accetta una scrittura durevole. Lo chiama `RuntimeController.start`/`resume`: qualunque entrypoint (GUI, `main.py`,
# headless, CLI, Control API) passa da li', quindi un launcher nuovo non puo' aggirarlo.
#
# Gate B (operazioni finanziarie): lo chiamano i punti d'invio REALI (`BetfairClient` e `SimulationBroker`), non i
# chiamanti, quindi copre ogni percorso (ingressi, Dutching, Roserpina, Microstake, cashout, recovery, thread tardivi).
# Contratto gia' ratificato in BB-1 (`docs/blackbox.md`, «Evento critico» / «Uscita protettiva»):
# - ingresso (nuovo ordine, replace): evento critico scritto in modo DUREVOLE prima dell'invio, altrimenti rifiuto
#   pre-invio (`GateRefused`, codice `BLACKBOX_UNAVAILABLE`). Un ingresso non si bufferizza mai;
# - uscita protettiva (cancel, riduzione, hedge del cashout dentro `protective_exit()`): evento critico con
#   `buffer_on_failure=True`, cosi' un guasto temporaneo del registro non blocca ne' ritarda la riduzione del rischio
#   (PF-EXEC-03); serve comunque una scatola nera installata;
# - scatola nera assente (mai installata, gia' chiusa allo shutdown, `atexit`, thread sopravvissuti): tutto rifiutato,
#   tranne le cancellazioni dentro `emergency_exit()`, cioe' il solo `--emergency-stop` CLI (D3 #617, gia' ratificato:
#   cancel-all prima di aprire il registro, esito nel log CRITICAL).
# Il contesto e' per thread: un lavoro protettivo spostato in un altro thread vale come ingresso (fail-closed).
# ---------------------------------------------------------------------------------------------------------------------
import contextlib
import logging

_gate_log = logging.getLogger("pickfair.blackbox.gate")
GATE_REFUSED_CODE = bb.BlackBoxUnavailable.reason_code       # "BLACKBOX_UNAVAILABLE"
_gate_ctx = threading.local()


class GateRefused(bb.BlackBoxUnavailable):
    """Operazione rifiutata dal gate PRIMA di qualunque effetto (nessun avvio, nessun invio). `str(exc)` e' il codice
    `BLACKBOX_UNAVAILABLE`: i classificatori dei punti d'invio lo trattano come rifiuto pre-invio certo, mai come
    esito ignoto (AMBIGUOUS)."""

    def __init__(self, detail: str = "") -> None:
        super().__init__(GATE_REFUSED_CODE)
        self.detail = str(detail)


def _active_runtime_box() -> Optional[bb.BlackBox]:
    box = _box
    if box is None:
        return None
    try:
        return box if box.is_runtime() else None
    except Exception:            # noqa: BLE001 - stato illeggibile => nessuna protezione
        return None


def require_runtime(component: str) -> bb.BlackBox:
    """Gate A. Scatola nera installata, runtime (lock D3) e scrittura durevole dell'avvio, altrimenti `GateRefused`."""
    box = _active_runtime_box()
    if box is None:
        raise GateRefused(f"{component}: scatola nera non installata o non runtime")
    try:
        box.record("START", critical=True, source="SYSTEM", component=str(component), phase="ATTEMPT",
                   outcome="PENDING", reason_code="BLACKBOX_GATE_A")
    except bb.BlackBoxUnavailable as exc:
        raise GateRefused(f"{component}: avvio non registrabile") from exc
    return box


@contextlib.contextmanager
def protective_exit(reason: str):
    """Marca come uscita protettiva gli invii fatti in questo thread dentro il blocco (hedge del cashout, chiusure)."""
    prev = getattr(_gate_ctx, "protective", None)
    _gate_ctx.protective = str(reason or "PROTECTIVE_EXIT")
    try:
        yield
    finally:
        _gate_ctx.protective = prev


@contextlib.contextmanager
def emergency_exit():
    """D3 (#617): il solo `--emergency-stop` CLI cancella prima di aprire il registro. Dentro il blocco, e senza
    scatola nera, passano SOLO le cancellazioni; ogni altro invio resta rifiutato."""
    prev = getattr(_gate_ctx, "emergency", False)
    _gate_ctx.emergency = True
    try:
        yield
    finally:
        _gate_ctx.emergency = prev


@contextlib.contextmanager
def batch_gated():
    prev = getattr(_gate_ctx, "batch", False)
    _gate_ctx.batch = True
    try:
        yield
    finally:
        _gate_ctx.batch = prev


GATE_OPERATIONS = frozenset({"ORDER_PLACE_REQUEST", "ORDER_CANCEL_REQUEST", "ORDER_REPLACE_REQUEST"})
_GATE_ID_FIELDS = ("market_id", "selection_id", "bet_id", "order_ref", "table_id")


def financial_gate(operation: str, *, component: str, cancel: bool = False, **fields: Any) -> Optional[str]:
    """Gate B. Da chiamare nel punto d'invio, dopo la validazione degli argomenti e prima di qualunque effetto.

    Ritorna l'`event_id` dell'ATTEMPT registrato (None solo per la cancellazione d'emergenza CLI senza registro o per
    una gamba di un batch gia' passato dal gate); altrimenti solleva `GateRefused` e il chiamante NON invia."""
    if operation not in GATE_OPERATIONS:
        raise GateRefused(f"operazione non ammessa dal gate: {operation!r}")
    if getattr(_gate_ctx, "batch", False):
        return None                          # gamba di un batch: il batch e' gia' passato (e registrato) dal gate
    protective = bool(cancel) or bool(getattr(_gate_ctx, "protective", None))
    box = _active_runtime_box()
    if box is None:
        if cancel and getattr(_gate_ctx, "emergency", False):
            _gate_log.critical("EMERGENCY STOP: %s su %s senza scatola nera (D3, registrazione dopo la cancellazione)",
                               operation, fields.get("market_id"))
            return None
        _gate_log.critical("Gate B: %s rifiutato, scatola nera non disponibile (%s)", operation, component)
        raise GateRefused(f"{component}: {operation} senza scatola nera")
    ids = {k: fields.pop(k) for k in _GATE_ID_FIELDS if fields.get(k) not in (None, "")}
    data = {"protective": protective, "intent": str(getattr(_gate_ctx, "protective", None) or
                                                    ("CANCEL" if cancel else "ENTRY"))}
    data.update({k: v for k, v in fields.items() if v is not None})
    try:
        return box.record(operation, critical=True, buffer_on_failure=protective, source="SYSTEM",
                          component=str(component), phase="ATTEMPT", outcome="PENDING",
                          reason_code="BLACKBOX_GATE_B", data=data, **ids)
    except bb.BlackBoxUnavailable as exc:
        _gate_log.critical("Gate B: %s rifiutato, registrazione durevole fallita (%s)", operation, component)
        raise GateRefused(f"{component}: {operation} non registrabile") from exc
    except (ValueError, TypeError) as exc:   # forma del record non valida: mai un invio non registrato
        raise GateRefused(f"{component}: {operation} con record non valido") from exc
