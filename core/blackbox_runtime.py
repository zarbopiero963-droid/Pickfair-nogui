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


def _trace(exc_type, exc, tb) -> str:
    return "".join(traceback.format_exception(exc_type, exc, tb))[-MAX_TRACE_CHARS:]


def _excepthook(exc_type, exc, tb):
    try:
        record("UNCAUGHT_EXCEPTION", source="SYSTEM", component="process", severity="CRITICAL",
               phase="OBSERVATION", outcome="VERIFIED", reason_code=getattr(exc_type, "__name__", "Exception"),
               reason=str(exc)[:500], data={"traceback": _trace(exc_type, exc, tb)})
    except Exception:
        pass
    (_prev_excepthook or sys.__excepthook__)(exc_type, exc, tb)


def _threading_hook(args):
    try:
        record("TASK_EXCEPTION", source="SYSTEM", component=f"thread:{getattr(args.thread, 'name', '?')}",
               severity="CRITICAL", phase="OBSERVATION", outcome="VERIFIED",
               reason_code=getattr(args.exc_type, "__name__", "Exception"), reason=str(args.exc_value)[:500],
               data={"traceback": _trace(args.exc_type, args.exc_value, args.exc_traceback)})
    except Exception:
        pass
    if _prev_threading_hook is not None:
        _prev_threading_hook(args)


def _previous_fault_dump(fault_path: str) -> Optional[str]:
    """Ultima parte del dump nativo della sessione precedente (se c'e'), poi rotazione del file."""
    try:
        if os.path.getsize(fault_path) <= 0:
            return None
        with open(fault_path, "rb") as fh:
            fh.seek(max(0, os.path.getsize(fault_path) - FAULT_TAIL_BYTES))
            tail = fh.read().decode("utf-8", errors="replace")
        os.replace(fault_path, fault_path + ".1")
        return tail
    except OSError:
        return None


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
        box.record("CRASH_RECOVERY", source="SYSTEM", component="faulthandler", severity="ERROR",
                   phase="OBSERVATION", outcome="VERIFIED", reason_code="NATIVE_CRASH_DUMP", data={"tail": dump})
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
