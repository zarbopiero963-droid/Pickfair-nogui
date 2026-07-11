"""Single-instance lock (#311-1.1): impedisce la DOPPIA istanza del bridge.

Due processi bridge avrebbero `SignalTracker`/`DailyLimiter`/`SignalQueue` separati in
memoria (i lock `_write_lock`/`_queue_lock` sono intra-processo): entrambi potrebbero
scrivere il CSV → **doppia scommessa possibile**. Questo modulo fornisce un lock di
istanza a livello di sistema operativo, acquisito PRIMA di costruire la GUI.

- **Windows** (target principale): mutex **named** via ctypes (`CreateMutexW`). Scelto
  rispetto al lockfile perché il kernel lo **rilascia da solo alla terminazione del
  processo** (anche su crash/kill): nessun lock orfano che blocchi il riavvio dopo un
  blackout. Namespace ``Local\\`` (sessione desktop corrente) e non ``Global\\``: copre
  il caso reale (doppio avvio sullo stesso desktop) senza richiedere il privilegio
  `SeCreateGlobalPrivilege` in ambienti terminal-server.
- **POSIX** (dev/CI, nessun utente finale): lockfile con `flock` esclusivo non bloccante;
  anche `flock` si rilascia da solo alla terminazione del processo. Permette di testare
  la logica reale offline.

Semantica FAIL-OPEN/FAIL-CLOSED (review #346):

- il **rifiuto** (ritorno ``None``) avviene SOLO sul segnale **certo** «lock già
  posseduto da un'altra istanza» (`ERROR_ALREADY_EXISTS` / `EWOULDBLOCK`);
- qualsiasi **errore imprevisto** del sistema nella creazione del lock → **fail-open
  consapevole**: warning nei log e handle no-op, l'avvio procede senza protezione. Un
  bridge inavviabile per un guasto raro è peggio del caso limite.

Modulo foglia: nessun import dal resto del package (usabile ovunque, testabile puro).
"""

import atexit
import errno as _errno
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Nome del mutex Windows / del file di lock POSIX: identifica QUESTA applicazione.
DEFAULT_NAME = "XTraderBridge"

_ERROR_ALREADY_EXISTS = 183          # winerror: il mutex esisteva già → altra istanza attiva


class InstanceLockHandle:
    """Handle opaco del lock acquisito; da passare a `release`. `kind` è "mutex"
    (Windows: `resource` = tupla ``(kernel32, handle)`` così il release usa la STESSA
    kernel32 con restype/argtypes configurati), "flock" (POSIX: `resource` = fd) o
    "noop" (fail-open)."""

    def __init__(self, kind: str, resource, path: str = ""):
        self.kind = kind
        self.resource = resource
        self.path = path
        self.released = False


def acquire(name: str = DEFAULT_NAME, lock_dir: str = ""):
    """Prova ad acquisire il lock di istanza. Ritorna un `InstanceLockHandle` se QUESTO
    processo è l'unica istanza, ``None`` se un'altra istanza lo possiede già.

    `lock_dir` (solo POSIX): cartella del lockfile — tipicamente la cartella dati
    dell'app (`config_store.config_dir()`), passata dal chiamante per tenere questo
    modulo foglia. Vuota → cartella temporanea di sistema.

    Errori IMPREVISTI (non «già posseduto») → fail-open: warning + handle no-op (vedi
    docstring del modulo)."""
    try:
        if sys.platform == "win32":
            return _acquire_windows(name)
        return _acquire_posix(name, lock_dir)
    except Exception:   # noqa: BLE001 — backstop fail-open per errori NON gestiti nei rami
        logger.warning("Single-instance lock non disponibile (errore imprevisto): "
                       "avvio consentito senza protezione doppia-istanza.", exc_info=True)
        return InstanceLockHandle("noop", None)


def _acquire_windows(name: str, kernel32=None, get_last_error=None):
    """CreateMutexW: se il mutex esisteva già, un'altra istanza è attiva → chiudi
    l'handle duplicato e rifiuta (None).

    `kernel32`/`get_last_error` sono INIETTABILI per i test (mock eseguibili anche su
    POSIX, review Fable #346). Il default costruisce la kernel32 reale con:
    - ``use_last_error=True`` + ``ctypes.get_last_error()``: il LastError va catturato
      dal meccanismo FFI della chiamata — ``windll.kernel32.GetLastError()`` sarebbe
      INAFFIDABILE (può essere azzerato/sovrascritto dagli interni di ctypes) e un
      `ERROR_ALREADY_EXISTS` perso = seconda istanza ammessa (Fable #346);
    - ``restype``/``argtypes`` ESPLICITI: il restype di default (`c_int`, 32 bit)
      TRONCHEREBBE l'HANDLE su Windows 64-bit, corrompendo `CloseHandle` e il test
      ``if not handle`` (Fable #346)."""
    if kernel32 is None:
        import ctypes
        from ctypes import wintypes
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.restype = wintypes.HANDLE
        kernel32.CreateMutexW.argtypes = (wintypes.LPVOID, wintypes.BOOL, wintypes.LPCWSTR)
        kernel32.CloseHandle.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = (wintypes.HANDLE,)
        get_last_error = ctypes.get_last_error
    handle = kernel32.CreateMutexW(None, False, f"Local\\{name}")
    already = (get_last_error() == _ERROR_ALREADY_EXISTS)   # letto SUBITO dopo la chiamata
    if not handle:
        # Creazione fallita (raro): fail-open, vedi docstring del modulo.
        logger.warning("CreateMutexW fallita (LastError=%s): avvio consentito senza "
                       "protezione doppia-istanza.", get_last_error())
        return InstanceLockHandle("noop", None)
    if already:
        kernel32.CloseHandle(handle)
        return None
    lock = InstanceLockHandle("mutex", (kernel32, handle))
    atexit.register(release, lock)     # backstop: il kernel rilascia comunque alla terminazione
    return lock


def _acquire_posix(name: str, lock_dir: str):
    """Lockfile + `flock` esclusivo non bloccante. Un secondo `flock` sullo stesso file
    (anche dallo stesso processo, su un altro fd) fallisce con EWOULDBLOCK → None.

    SOLO `EWOULDBLOCK`/`EAGAIN` significa «lock posseduto» → rifiuto certo. Qualsiasi
    ALTRA `OSError` (filesystem/permessi) è un errore imprevisto → fail-open con warning
    (Sourcery #346): un guasto FS non deve né ammettere silenziosamente la doppia
    istanza né — peggio per l'operatività — rendere il bridge inavviabile."""
    import fcntl
    import tempfile
    directory = lock_dir or tempfile.gettempdir()
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, f"{name}.lock")
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        os.close(fd)
        if exc.errno in (_errno.EWOULDBLOCK, _errno.EAGAIN):
            return None                   # lock POSSEDUTO da un'altra istanza: rifiuto certo
        logger.warning("flock fallita per errore imprevisto (%s): avvio consentito senza "
                       "protezione doppia-istanza.", exc)
        return InstanceLockHandle("noop", None)
    try:                                     # PID per diagnosi manuale (best-effort)
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode("ascii"))
    except OSError:
        pass
    lock = InstanceLockHandle("flock", fd, path)
    atexit.register(release, lock)     # backstop: flock cade comunque alla terminazione
    return lock


def release(lock) -> None:
    """Rilascia il lock (idempotente; `None`/handle no-op = no-op). Il file di lock POSIX
    NON viene cancellato: rimuoverlo sarebbe raceabile con un'istanza in avvio — è il
    `flock`, non l'esistenza del file, a fare da lock."""
    if lock is None or getattr(lock, "released", True):
        return
    lock.released = True
    try:
        if lock.kind == "mutex":
            kernel32, handle = lock.resource   # la STESSA kernel32 configurata in acquire
            kernel32.CloseHandle(handle)
        elif lock.kind == "flock":
            import fcntl
            fcntl.flock(lock.resource, fcntl.LOCK_UN)
            os.close(lock.resource)
    except Exception:   # noqa: BLE001 — best-effort: alla terminazione rilascia comunque il SO
        logger.debug("Rilascio del single-instance lock fallito (best-effort).", exc_info=True)
