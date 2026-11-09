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


def scrub_error_texts(value: Any) -> Any:
    """R2: nei risultati strutturati (es. `emergency_stop()`), i campi `error`/`errors` contengono testo di eccezioni
    o di API arbitrario: diventano un'impronta (lunghezza + HMAC con la chiave del registro), il resto resta."""
    if isinstance(value, dict):
        out = {}
        for k, v in value.items():
            if str(k).lower() in ("error", "errors", "exception", "message") and isinstance(v, str):
                out[k] = {"redacted": True, "len": len(v), "fingerprint": _fingerprint(v)}
            else:
                out[k] = scrub_error_texts(v)
        return out
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


def _previous_fault_dump(fault_path: str) -> Optional[str]:
    """Ultima parte dei dump nativi di sessioni precedenti: `fault.log.pending` (in attesa di import) e `fault.log`.
    La LETTURA non dipende dallo spostamento: se `fault.log` non si puo' accodare a `.pending`, viene letto dov'e'.
    Niente viene consumato qui: lo fa `_consume_fault_dump`, solo dopo un CRASH_RECOVERY durevole (rilievi Codex e
    Sol #617)."""
    pending = fault_path + ".pending"
    try:                                         # best effort: mettere da parte prima del nuovo faulthandler
        if os.path.exists(fault_path) and os.path.getsize(fault_path) > 0:
            if os.path.exists(pending):
                with open(fault_path, "rb") as src, open(pending, "ab") as dst:
                    dst.write(src.read())
                os.remove(fault_path)
            else:
                os.replace(fault_path, pending)
    except OSError:
        pass                                     # resta in fault.log: lo si legge li', e non lo si tronca mai
    parts = []
    for p in (pending, fault_path):
        try:
            size = os.path.getsize(p) if os.path.exists(p) else 0
            if size > 0:
                with open(p, "rb") as fh:
                    fh.seek(max(0, size - FAULT_TAIL_BYTES))
                    parts.append(fh.read().decode("utf-8", errors="replace"))
        except OSError:
            continue
    return "\n".join(parts) or None


def _consume_fault_dump(fault_path: str) -> bool:
    """Archivia in `fault.log.1` i dump importati. False se qualcosa resta da consumare (verra' reimportato al
    prossimo avvio: meglio un import doppio che un dump perso)."""
    pending, archive = fault_path + ".pending", fault_path + ".1"
    ok = True
    try:
        if os.path.exists(pending):
            os.replace(pending, archive)
    except OSError:
        ok = False
    try:
        if os.path.exists(fault_path) and os.path.getsize(fault_path) > 0:
            with open(fault_path, "rb") as src, open(archive, "ab") as dst:
                dst.write(src.read())
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
    fault_path = os.path.join(os.path.dirname(box.path) or ".", "fault.log")
    dump = _previous_fault_dump(fault_path)
    try:
        box.start_session(argv=safe_argv(sys.argv[1:]), **process_data)
    except BaseException:
        box.close()                     # rifiuto (es. RUNTIME_ALREADY_ACTIVE) o guasto: nessuna risorsa lasciata aperta
        raise
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
        bb.private_file(fault_path)
        _fault_file = open(fault_path, "a")
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
                    os.remove(_fault_file.name)
            except OSError:
                pass
            _fault_file = None
        if sys.excepthook is _excepthook:
            sys.excepthook = _prev_excepthook or sys.__excepthook__
        if threading.excepthook is _threading_hook:
            threading.excepthook = _prev_threading_hook or threading.__excepthook__
        box.close()
        _box = None
