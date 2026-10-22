"""#536 PF-RUNTIME-LOCK-01 — autorita' esclusiva del runtime Pickfair su un database.

Un solo runtime attivo per database (decisione owner ratificata il 22/10/2026). Tre strati, tutti obbligatori e
fail-closed; nessuno e' disattivabile da configurazione:

1. **Lock del sistema operativo** su ``<realpath(db)>.runtime.lock`` (``fcntl.flock`` su POSIX, ``msvcrt.locking``
   sul byte 0 su Windows), preso da `acquire_os_lock` e tenuto finche' il runtime non e' fermo.
2. **Identita' del file di lock**: all'acquisizione si memorizzano ``(st_dev, st_ino)`` del descrittore bloccato e si
   scrive nel file il record del proprietario (pid, host, owner_id). A ogni verifica si confrontano con lo
   ``stat`` del percorso: file cancellato (``nlink == 0`` o percorso assente) o sostituito (inode diverso) =>
   autorita' persa. Un ``flock`` su un inode scollegato non protegge piu' nulla, e qui viene riconosciuto.
3. **Lease persistente nel database** (tabella ``runtime_lease``, riga singola) con ``fencing_token`` monotono:
   acquisita in ``BEGIN IMMEDIATE`` (`Database.acquire_runtime_lease`), rinnovata dall'heartbeat
   (`Database.renew_runtime_lease`, che rifiuta una lease scaduta o di un altro proprietario), e vincolata al
   ``Database`` con `bind_runtime_fence`: ogni scrittura (transazione o autocommit) verifica DENTRO la stessa
   transazione che la lease sia ancora di questo proprietario con questo token. Un runtime superato non puo'
   piu' scrivere stato finanziario, qualunque cosa creda di essere.

Prima di ogni invio di ordine, cashout, auto-next, reset di ciclo e sweep F028 il runtime chiama `verify`: lock
e lease si ricontrollano sul momento (TOCTOU ridotto al tempo fra la verifica e la chiamata all'exchange, che non
puo' essere fenced: vedi `RuntimeController.after_order_send_authority_check`). Persa l'autorita', il runtime va in
LOCKDOWN senza cancel-all (gli ordini sull'exchange possono essere del nuovo proprietario) e la lease viene
scaduta, cosi' il proprietario legittimo puo' subentrare dopo la riconciliazione.

Finestre e limiti dichiarati (non un falso senso di sicurezza):
- fra `verify` e la chiamata all'exchange un ordine puo' partire con autorita' appena persa: il ricontrollo
  dopo l'invio lo segnala (`RUNTIME_AUTHORITY_LOST_AFTER_SEND`) e lo lascia alla riconciliazione del nuovo
  proprietario (ordine sull'exchange con ``customer_ref`` del runtime superato); la riga ``orders`` di quel
  runtime e' rifiutata dal fence;
- ``st_ino`` su Windows e' l'indice del file NTFS: la sostituzione del file e' riconosciuta se l'indice cambia;
  comportamento non eseguito qui (MANUAL_ONLY);
- lo stesso host per tutti i runtime: il confronto dei pid e l'orologio condiviso (``time.time``) lo assumono;
  un database su filesystem di rete non e' supportato.
"""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Any, Callable, Optional

from database import RuntimeFenceViolation

logger = logging.getLogger(__name__)

DEFAULT_LEASE_TTL_SEC = 30.0
DEFAULT_HEARTBEAT_SEC = 10.0


class RuntimeAuthorityLost(RuntimeError):
    """L'autorita' esclusiva non e' dimostrabile. ``permanent`` False = verifica impossibile sul momento (DB non
    leggibile): l'operazione e' rifiutata ma il runtime non e' ancora dichiarato superato."""

    def __init__(self, reason: str, *, permanent: bool = True):
        super().__init__(f"runtime_authority_lost:{reason}")
        self.reason = str(reason)
        self.permanent = bool(permanent)


def _holder_alive(pid: Any, host: Any, *, my_host: str) -> Optional[bool]:
    """True/False solo quando dimostrabile (stesso host, pid valido); None altrimenti (mai si scavalca)."""
    try:
        pid = int(pid or 0)
    except (TypeError, ValueError):
        return None
    if pid <= 0 or str(host or "") != my_host or os.name == "nt":
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return None
    return True


class RuntimeAuthority:
    def __init__(self, db: Any, db_path: str, *, owner_id: Optional[str] = None,
                 lease_ttl_sec: float = DEFAULT_LEASE_TTL_SEC, heartbeat_sec: float = DEFAULT_HEARTBEAT_SEC,
                 on_lost: Optional[Callable[[str], None]] = None, clock: Callable[[], float] = time.time,
                 pid: Optional[int] = None, host: Optional[str] = None):
        self.db = db
        self.db_path = str(db_path)
        self.lock_path = os.path.normcase(os.path.realpath(os.path.abspath(self.db_path))) + ".runtime.lock"
        self.owner_id = str(owner_id or uuid.uuid4().hex)
        self.lease_ttl_sec = float(lease_ttl_sec)
        self.heartbeat_sec = float(heartbeat_sec)
        if not (self.lease_ttl_sec > 0) or not (0 < self.heartbeat_sec < self.lease_ttl_sec):
            raise ValueError("RuntimeAuthority: heartbeat deve essere > 0 e < ttl della lease")
        self._on_lost = on_lost
        self._clock = clock
        self.pid = int(pid if pid is not None else os.getpid())
        self.host = str(host if host is not None else socket.gethostname())
        self.lock_fh = None
        self.lock_identity = ""
        self._lock_stat: Optional[tuple] = None
        self.token: Optional[int] = None
        self.lease_expires_at: float = 0.0
        self.lost_reason = ""
        self.released = False
        self._lock = threading.RLock()
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()

    # ------------------------------------------------------------------ strato 1 e 2: lock OS e identita'
    def acquire_os_lock(self) -> str:
        with self._lock:
            if self.lock_fh is not None:
                return ""
            try:
                handle = open(self.lock_path, "a+")
            except OSError as exc:
                return f"runtime_lock_unavailable:{exc.__class__.__name__}"
            try:
                if os.name == "nt":
                    import msvcrt
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                handle.close()
                return "runtime_already_active_on_database"
            try:
                st = os.fstat(handle.fileno())
                self._lock_stat = (st.st_dev, st.st_ino)
                self.lock_identity = f"{st.st_dev}:{st.st_ino}"
                handle.seek(0)
                handle.truncate()
                handle.write(json.dumps({"owner_id": self.owner_id, "pid": self.pid, "host": self.host,
                                         "acquired_at": self._clock()}) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            except OSError:
                logger.exception("Lock di istanza: record del proprietario non scritto")
            self.lock_fh = handle
            return ""

    def lock_identity_problem(self) -> str:
        """'' se il file di lock bloccato e' ancora quello sul percorso; altrimenti il motivo."""
        handle = self.lock_fh
        if handle is None or self._lock_stat is None:
            return "lock_not_held"
        try:
            held = os.fstat(handle.fileno())
        except OSError:
            return "lock_handle_invalid"
        try:
            current = os.stat(self.lock_path)
        except FileNotFoundError:
            return "lock_file_deleted"
        except OSError:
            return "lock_path_unreadable"
        if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
            return "lock_file_replaced"          # sul percorso c'e' un altro file: il flock tenuto non vale
        if getattr(held, "st_nlink", 1) == 0:
            return "lock_file_deleted"
        return ""

    # ------------------------------------------------------------------ strato 3: lease e fencing
    def acquire_lease(self) -> str:
        with self._lock:
            if self.lost_reason:
                # Codex su fc8f136: un'autorita' persa non apre una nuova epoca senza lo STOP che smonta i servizi
                return f"runtime_authority_lost:{self.lost_reason}:stop_required"
            if self.released:
                return "runtime_authority_released"
            if self.lock_fh is None:
                return "runtime_lock_not_held"
        problem = self.lock_identity_problem()
        if problem:
            return problem
        try:                                            # I/O sul DB fuori da self._lock (Grok su 125aa05)
            outcome = self.db.acquire_runtime_lease(
                self.owner_id, ttl_sec=self.lease_ttl_sec, now=self._clock(), pid=self.pid, host=self.host,
                lock_identity=self.lock_identity, clock=self._clock,
                holder_alive=lambda pid, host: _holder_alive(pid, host, my_host=self.host))
        except Exception as exc:
            logger.exception("Lease del runtime non acquisibile")
            return f"runtime_lease_unavailable:{exc.__class__.__name__}"
        if not outcome.get("acquired"):
            holder = outcome.get("holder") or {}
            return (f"runtime_lease_held_by:{holder.get('owner_id')}:pid={holder.get('pid')}"
                    f":expires_in={max(0.0, float(holder.get('expires_at') or 0.0) - self._clock()):.1f}s")
        with self._lock:
            stale = int(outcome["fencing_token"]) if (self.lost_reason or self.released) else None
            if stale is None:
                self.token = int(outcome["fencing_token"])
                self.lease_expires_at = float(outcome["expires_at"])
                self.db.bind_runtime_fence(self.owner_id, self.token, on_violation=self._on_fence_violation)
        if stale is not None:                           # perdita o rilascio concorrenti: la lease non vale
            self._expire_lease_quietly(stale)
            return "runtime_authority_lost_during_acquire"
        return ""

    def _expire_lease_quietly(self, token: int) -> None:
        try:
            self.db.release_runtime_lease(self.owner_id, int(token), now=self._clock())
        except Exception:
            logger.warning("Lease non riportata a scaduta (token %s)", token)

    def _on_fence_violation(self, reason: str) -> None:
        self.mark_lost(str(reason or "fence_violation"))

    def verify(self, context: str = "") -> None:
        """Lock OS tenuto, file di lock identico, lease ancora nostra e non scaduta. Altrimenti
        `RuntimeAuthorityLost`; le perdite dimostrate sono definitive (`mark_lost`)."""
        if self.released:
            raise RuntimeAuthorityLost("released")
        if self.lost_reason:
            raise RuntimeAuthorityLost(self.lost_reason)
        if self.lock_fh is None or self.token is None:
            # mai armata (lock o lease non ancora presi): rifiuto, ma non e' una perdita da rendere definitiva
            raise RuntimeAuthorityLost("lock_not_held" if self.lock_fh is None else "lease_not_acquired")
        problem = self.lock_identity_problem()
        if problem:
            self.mark_lost(problem)
            raise RuntimeAuthorityLost(problem)
        try:
            row = self.db.read_runtime_lease()
        except Exception:
            logger.exception("Lease del runtime non leggibile (%s): operazione rifiutata", context)
            raise RuntimeAuthorityLost("lease_unreadable", permanent=False)
        if row is None or str(row.get("owner_id")) != self.owner_id or int(row.get("fencing_token") or -1) != self.token:
            self.mark_lost("lease_superseded")
            raise RuntimeAuthorityLost("lease_superseded")
        if float(row.get("expires_at") or 0.0) < self._clock():
            self.mark_lost("lease_expired")
            raise RuntimeAuthorityLost("lease_expired")

    def heartbeat_once(self) -> bool:
        """Rinnovo della lease. Nessun I/O sul database sotto ``self._lock`` (Codex su fc8f136): chi tiene il write
        lock del DB e segnala una violazione del fence deve poter marcare la perdita senza attendere l'heartbeat."""
        with self._lock:
            if self.released or self.lost_reason:
                return False
            token, expires = self.token, self.lease_expires_at
        problem = self.lock_identity_problem()
        if problem:
            self.mark_lost(problem)
            return False
        now = self._clock()
        try:
            renewed = self.db.renew_runtime_lease(self.owner_id, int(token), ttl_sec=self.lease_ttl_sec, now=now,
                                                  clock=self._clock)
        except Exception:
            logger.exception("Heartbeat della lease fallito (database non disponibile)")
            if now + self.heartbeat_sec >= expires:
                # non si riuscirebbe piu' a rinnovare prima della scadenza: autorita' non dimostrabile
                self.mark_lost("lease_unrenewable")
                return False
            return True
        if renewed is None:
            self.mark_lost("lease_superseded")
            return False
        with self._lock:
            undo = bool(self.released or self.lost_reason)
            if not undo:
                self.lease_expires_at = float(renewed)
        if undo:
            # Grok su 125aa05: perdita o STOP concorrenti al rinnovo => la lease rinnovata torna scaduta, cosi' il
            # proprietario legittimo subentra senza attendere il TTL.
            self._expire_lease_quietly(int(token))
            return False
        return True

    def start_heartbeat(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop = threading.Event()
            stop = self._stop
            self._thread = threading.Thread(target=self._heartbeat_loop, args=(stop,), name="runtime-authority-heartbeat",
                                            daemon=True)
            self._thread.start()

    def _heartbeat_loop(self, stop: threading.Event) -> None:
        while not stop.wait(self.heartbeat_sec):
            try:
                if not self.heartbeat_once():
                    return
            except Exception:
                logger.exception("Heartbeat della lease: errore inatteso")

    def stop_heartbeat(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=5.0)
        self._thread = None

    # ------------------------------------------------------------------ perdita e rilascio
    def mark_lost(self, reason: str) -> None:
        """Autorita' persa in modo definitivo: lease scaduta subito (solo se ancora nostra), heartbeat fermato,
        callback una sola volta. Il fence resta vincolato: ogni scrittura successiva viene rifiutata."""
        with self._lock:
            if self.lost_reason or self.released:
                return
            self.lost_reason = str(reason or "authority_lost")
            logger.critical("Autorita' del runtime persa: %s", self.lost_reason)
            self._stop.set()
            token = self.token
        if token is not None:                           # I/O sul DB fuori da self._lock (nessun lock-order inverso)
            try:
                self.db.release_runtime_lease(self.owner_id, int(token), now=self._clock())
            except Exception:
                logger.warning("Lease non rilasciabile dopo la perdita dell'autorita' (%s)", self.lost_reason)
        callback = self._on_lost
        if callable(callback):
            try:
                callback(self.lost_reason)
            except Exception:
                logger.exception("Callback di perdita dell'autorita' fallita")

    def release(self) -> bool:
        """Rilascio ordinato a runtime fermo: heartbeat, lease, fence, lock. False (nulla rilasciato, heartbeat
        ripreso) se la lease non si puo' scadere: lo STOP non si dichiara pulito con una lease ancora viva."""
        with self._lock:
            if self.released:
                return True
            token = self.token if not self.lost_reason else None
            self._stop.set()
        self.stop_heartbeat()
        if token is not None:                           # I/O sul DB fuori da self._lock
            try:
                self.db.release_runtime_lease(self.owner_id, int(token), now=self._clock())
            except Exception:
                logger.exception("Lease del runtime non rilasciata allo STOP: autorita' trattenuta")
                self.start_heartbeat()                  # Codex su 125aa05: si continua a dimostrarla
                return False
        with self._lock:
            self.released = True
        try:
            if getattr(self.db, "runtime_fence", None) and (self.db.runtime_fence() or {}).get("owner_id") == self.owner_id:
                self.db.unbind_runtime_fence()
        except Exception:
            logger.exception("Fence del runtime non sciolto allo STOP")
        with self._lock:
            handle, self.lock_fh = self.lock_fh, None
            self._lock_stat = None
            if handle is not None:
                try:
                    handle.close()                   # chiudere il descrittore rilascia flock/msvcrt
                except OSError:
                    logger.exception("Lock di istanza: chiusura fallita")
        return True


__all__ = ["RuntimeAuthority", "RuntimeAuthorityLost", "RuntimeFenceViolation",
           "DEFAULT_LEASE_TTL_SEC", "DEFAULT_HEARTBEAT_SEC"]
