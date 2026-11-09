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


def _trace(exc_type, exc, tb) -> str:
    return safe_text("".join(traceback.format_exception(exc_type, exc, tb)), MAX_TRACE_CHARS, tail=True)


def _excepthook(exc_type, exc, tb):
    try:
        record("UNCAUGHT_EXCEPTION", source="SYSTEM", component="process", severity="CRITICAL",
               phase="OBSERVATION", outcome="VERIFIED", reason_code=getattr(exc_type, "__name__", "Exception"),
               reason=safe_text(exc, 500), data={"traceback": _trace(exc_type, exc, tb)})
    except Exception:
        pass
    (_prev_excepthook or sys.__excepthook__)(exc_type, exc, tb)


def _threading_hook(args):
    try:
        record("TASK_EXCEPTION", source="SYSTEM", component=f"thread:{getattr(args.thread, 'name', '?')}",
               severity="CRITICAL", phase="OBSERVATION", outcome="VERIFIED",
               reason_code=getattr(args.exc_type, "__name__", "Exception"), reason=safe_text(args.exc_value, 500),
               data={"traceback": _trace(args.exc_type, args.exc_value, args.exc_traceback)})
    except Exception:
        pass
    if _prev_threading_hook is not None:
        _prev_threading_hook(args)


def _previous_fault_dump(fault_path: str) -> Optional[str]:
    """Ultima parte del dump nativo di una sessione precedente. Il file NON viene consumato qui: lo e' solo dopo che
    il CRASH_RECOVERY e' scritto in modo durevole (`_consume_fault_dump`). Prima viene messo da parte in
    `fault.log.pending`, che il nuovo `faulthandler` non sovrascrive e che resta in attesa di un import riuscito
    (rilievo Codex #617)."""
    pending = fault_path + ".pending"
    try:
        if os.path.exists(fault_path) and os.path.getsize(fault_path) > 0:
            if os.path.exists(pending):          # un dump precedente ancora da importare: si accoda, mai si perde
                with open(fault_path, "rb") as src, open(pending, "ab") as dst:
                    dst.write(src.read())
                os.remove(fault_path)
            else:
                os.replace(fault_path, pending)
        if not os.path.exists(pending) or os.path.getsize(pending) <= 0:
            return None
        with open(pending, "rb") as fh:
            fh.seek(max(0, os.path.getsize(pending) - FAULT_TAIL_BYTES))
            return fh.read().decode("utf-8", errors="replace")
    except OSError:
        return None


def _consume_fault_dump(fault_path: str) -> None:
    try:
        os.replace(fault_path + ".pending", fault_path + ".1")
    except OSError:
        pass


def safe_argv(argv) -> list:
    """Argomenti della riga di comando senza segreti: un flag sensibile (`--password`, `--token`, `--api-key`...)
    viene registrato come `--flag=[REDACTED]`, anche quando il valore e' l'argomento successivo."""
    from core import redaction
    out, skip = [], False
    for a in list(argv):
        if skip:
            skip = False
            continue
        a = str(a)
        if a.startswith("-"):
            name, sep, _value = a.lstrip("-").partition("=")
            if redaction.is_sensitive_key(name.replace("-", "_")) or name.lower() in ("password", "token", "pin"):
                out.append(f"--{name}={bb.REDACTED}")
                skip = not sep
                continue
        out.append(a)
    return out


def install(path: Optional[str] = None, **process_data: Any) -> bb.BlackBox:
    """Apre la scatola nera, registra PROCESS_START (critico: senza registro il processo non parte) e installa gli
    hook dei crash. Idempotente."""
    global _box, _fault_file, _prev_excepthook, _prev_threading_hook
    if _box is not None:
        return _box
    box = bb.BlackBox(path).open()
    fault_path = os.path.join(os.path.dirname(box.path) or ".", "fault.log")
    dump = _previous_fault_dump(fault_path)
    try:
        box.start_session(argv=safe_argv(sys.argv[1:]), **process_data)
    except BaseException:
        box.close()                     # rifiuto (es. RUNTIME_ALREADY_ACTIVE) o guasto: nessuna risorsa lasciata aperta
        raise
    if dump:
        try:
            box.record("CRASH_RECOVERY", critical=True, source="SYSTEM", component="faulthandler", severity="ERROR",
                       phase="OBSERVATION", outcome="VERIFIED", reason_code="NATIVE_CRASH_DUMP",
                       data={"tail": dump})
            _consume_fault_dump(fault_path)      # solo dopo la scrittura durevole
        except bb.BlackBoxUnavailable:
            pass                                 # il dump resta in fault.log.pending: nuovo tentativo al prossimo avvio
    try:
        _fault_file = open(fault_path, "w")
        faulthandler.enable(file=_fault_file, all_threads=True)
    except OSError:
        _fault_file = None
    _prev_excepthook, sys.excepthook = sys.excepthook, _excepthook
    _prev_threading_hook, threading.excepthook = threading.excepthook, _threading_hook
    _box = box
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
                os.remove(_fault_file.name)        # chiusura pulita: nessun dump da conservare
            except OSError:
                pass
            _fault_file = None
        if sys.excepthook is _excepthook:
            sys.excepthook = _prev_excepthook or sys.__excepthook__
        if threading.excepthook is _threading_hook:
            threading.excepthook = _prev_threading_hook or threading.__excepthook__
        box.close()
        _box = None
