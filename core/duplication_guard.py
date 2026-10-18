from __future__ import annotations

import time
from datetime import datetime
from threading import RLock
from typing import Any, Dict, List


class DuplicationGuard:
    """
    Duplication Guard PRO

    - check + register atomico
    - TTL automatico su clock MONOTONICO (time.monotonic): immune ai salti
      del wall-clock (correzioni NTP, sleep/resume del VPS, set manuale).
      Un salto del wall-clock NON deve far scadere prematuramente una
      chiave di dedup -> altrimenti un duplicato passerebbe = doppia bet.
      Il timestamp leggibile in _registered_at resta wall-clock (audit).
    - evita memory leak
    - supporta strategie diverse
    """

    def __init__(self, ttl_seconds: int = 120):
        self._lock = RLock()

        # key -> timestamp
        self._active: Dict[str, float] = {}

        # metadata debug
        self._registered_at: Dict[str, str] = {}

        # chiavi "appuntate": trattenute da un reset (esito incerto, PF-ALL-F024) o
        # acquisite dalla ricostruzione F018 di una posizione viva CON tavolo.
        # Rappresentano una posizione aperta, non un segnale recente: esenti dalla
        # scadenza TTL (rilievo Codex #512 ciclo 9, r4307328207); liberate solo da
        # release() (chiusura della posizione) o clear(). Il seed di avvio senza
        # tavolo (`register_startup_order`) NON e' appuntato.
        self._pinned: set = set()

        self.ttl = int(ttl_seconds)

    # =========================================================
    # KEY BUILD
    # =========================================================
    def build_event_key(self, payload: Dict[str, Any]) -> str:
        payload = dict(payload or {})

        market_id = str(
            payload.get("market_id")
            or payload.get("marketId")
            or ""
        ).strip()

        selection_id = str(
            payload.get("selection_id")
            or payload.get("selectionId")
            or ""
        ).strip()

        bet_type = str(
            payload.get("bet_type")
            or payload.get("side")
            or "BACK"
        ).upper().strip()

        strategy = str(
            payload.get("strategy")
            or payload.get("source")
            or "default"
        ).lower().strip()

        return f"{market_id}:{selection_id}:{bet_type}:{strategy}"

    # =========================================================
    # ATOMIC CHECK + REGISTER
    # =========================================================
    def acquire(self, event_key: str, *, pin: bool = False) -> bool:
        """
        True → puoi eseguire ordine
        False → duplicato

        ``pin=True``: la chiave rappresenta una posizione viva (seed al riavvio) e
        non scade per TTL; vale anche se la chiave era gia' attiva.
        """
        key = str(event_key or "").strip()
        if not key:
            return False

        now = time.monotonic()

        with self._lock:
            self._cleanup_locked(now)

            if key in self._active:
                if pin:
                    self._pinned.add(key)
                return False

            self._active[key] = now
            self._registered_at[key] = datetime.utcnow().isoformat()
            if pin:
                self._pinned.add(key)

            return True


    def register_startup_order(self, payload: Dict[str, Any]) -> bool:
        """
        Seed della guardia da ordini già vivi su exchange al restart.
        True se registrato, False se payload insufficiente o già presente.
        """
        order = dict(payload or {})
        event_key = str(order.get("event_key") or "").strip()
        if not event_key:
            event_key = self.build_event_key(order)
        # Seed di avvio (ordine trovato sull'exchange, senza tavolo): NON appuntato. Scade con il TTL
        # come ogni chiave (contratto documentato): l'ordine vivo e' gestito dalla reconciliation e,
        # se ha un tavolo, dal controllo di occupazione dell'evento in `_on_signal_received`; un seed
        # eterno bloccherebbe per sempre la selezione di un ordine poi annullato o scaduto.
        return self.acquire(event_key)

    # =========================================================
    # TWO-PHASE INTERFACE (is_duplicate / register)
    # Kept for monitoring / audit use-cases only.
    # WARNING: using is_duplicate() + register() as a pair is NOT atomic
    # and is subject to TOCTOU races under concurrent signal handling.
    # Use acquire() for all order-gate decisions instead.
    # =========================================================
    def is_duplicate(self, event_key: str) -> bool:
        """Read-only check: True if the key is already active.

        Does NOT register the key.

        WARNING: non-atomic when paired with register() – two threads can
        both see False here before either registers.  Use acquire() for
        order-gate decisions.
        """
        key = str(event_key or "").strip()
        if not key:
            return False

        now = time.monotonic()
        with self._lock:
            self._cleanup_locked(now)
            return key in self._active

    def register(self, event_key: str) -> None:
        """Register a key as active without atomically checking first.

        Idempotent: registering an already-active key refreshes its timestamp.

        WARNING: non-atomic when preceded by is_duplicate() – use acquire()
        for order-gate decisions.
        """
        key = str(event_key or "").strip()
        if not key:
            return

        now = time.monotonic()
        with self._lock:
            self._active[key] = now
            self._registered_at[key] = datetime.utcnow().isoformat()

    # =========================================================
    # RELEASE
    # =========================================================
    def release(self, event_key: str) -> None:
        key = str(event_key or "").strip()
        if not key:
            return

        with self._lock:
            self._active.pop(key, None)
            self._registered_at.pop(key, None)
            self._pinned.discard(key)

    def unpin(self, event_key: str) -> None:
        """La posizione viva rappresentata dalla chiave e' stata chiusa ma la chiave resta (es. F028:
        ordine mai abbinato con stato finale confermato): torna alla scadenza NORMALE per TTL,
        conteggiata da adesso. Senza questo una chiave fissata al riavvio bloccherebbe per sempre
        i segnali validi successivi sullo stesso evento (riesame indipendente ciclo 9 #512)."""
        key = str(event_key or "").strip()
        if not key:
            return
        with self._lock:
            if key in self._pinned:
                self._pinned.discard(key)
                if key in self._active:
                    self._active[key] = time.monotonic()

    # =========================================================
    # CLEANUP TTL
    # =========================================================
    def _cleanup_locked(self, now: float):
        if not self._active:
            return

        expired = [
            k for k, ts in self._active.items()
            if (now - ts) > self.ttl and k not in self._pinned
        ]

        for k in expired:
            self._active.pop(k, None)
            self._registered_at.pop(k, None)

    # =========================================================
    # SNAPSHOT
    # =========================================================
    def snapshot(self) -> Dict[str, Any]:
        now = time.monotonic()

        with self._lock:
            self._cleanup_locked(now)

            keys: List[Dict[str, str]] = []

            for key in sorted(self._active):
                keys.append({
                    "event_key": key,
                    "registered_at": self._registered_at.get(key, ""),
                })

            return {
                "active_count": len(self._active),
                "active_keys": keys,
            }

    # =========================================================
    # CLEAR
    # =========================================================
    def clear(self, keep_keys=None):
        """Svuota le chiavi. ``keep_keys``: chiavi di ordini con esito incerto
        (AMBIGUOUS, PF-ALL-F024) che restano acquisite."""
        keep = {str(k or "").strip() for k in (keep_keys or ())} - {""}
        with self._lock:
            if not keep:
                self._active.clear()
                self._registered_at.clear()
                self._pinned.clear()
                return
            for key in list(self._active):
                if key not in keep:
                    self._active.pop(key, None)
            for key in list(self._registered_at):
                if key not in keep:
                    self._registered_at.pop(key, None)
            # le chiavi trattenute sono posizioni aperte/incerte: non scadono per TTL (ciclo 9)
            self._pinned = {k for k in keep if k in self._active}
