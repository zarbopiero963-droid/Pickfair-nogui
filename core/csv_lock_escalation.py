"""Escalation visibile su CSV-lock persistente (audit #105 / roadmap #153 — voce H2).

Quando la scrittura del CSV operativo fallisce ripetutamente perché XTrader tiene il file
lockato a lungo, oltre al log + retry timer serve un segnale **visibile**: dopo N fallimenti
consecutivi si ESCALA (stato «CSV bloccato»), e al primo successo si notifica il recovery.

Questa è SOLO logica di stato/decisione (un contatore con soglia): **NON** tocca la scrittura,
la coda o il rollback dei guardrail — quindi non introduce alcun rischio di doppia scrittura
né bypassa il retry esistente. La GUI (`App`) applica il risultato (log/indicatore).

Thread-safe: i metodi sono serializzati da un lock interno, perché i fallimenti di scrittura
arrivano da flussi diversi (listener Telegram, timer di scadenza, conferme XTrader).
"""

import threading

# Numero di fallimenti consecutivi dopo i quali si rende VISIBILE il blocco. Default basso:
# poche scritture fallite di fila indicano già che XTrader tiene il file aperto.
DEFAULT_THRESHOLD = 3


class CsvLockEscalation:
    """Contatore dei fallimenti consecutivi di scrittura CSV con soglia di escalation.

    - `record_failure()` → True **solo** quando si entra in escalation (soglia raggiunta la
      prima volta): il chiamante logga/mostra lo stato una sola volta;
    - `record_success()` → True se si ERA in escalation: il chiamante notifica il recovery
      una sola volta. In ogni caso azzera il contatore.
    """

    __slots__ = ("_threshold", "_lock", "_failures", "_escalated")

    def __init__(self, threshold=DEFAULT_THRESHOLD):
        self._threshold = self._valid_threshold(threshold)
        self._lock = threading.Lock()
        self._failures = 0
        self._escalated = False

    @staticmethod
    def _valid_threshold(value):
        """Soglia valida: intero >= 1. Bool/non-numerico/<1 → `DEFAULT_THRESHOLD` (fail-safe)."""
        if isinstance(value, bool):
            return DEFAULT_THRESHOLD
        try:
            t = int(value)
        except (TypeError, ValueError):
            return DEFAULT_THRESHOLD
        return t if t >= 1 else DEFAULT_THRESHOLD

    @property
    def threshold(self) -> int:
        return self._threshold

    @property
    def count(self) -> int:
        with self._lock:
            return self._failures

    @property
    def escalated(self) -> bool:
        with self._lock:
            return self._escalated

    def record_failure(self) -> bool:
        """Registra un fallimento di scrittura CSV. Ritorna True SOLO quando si entra in
        escalation (raggiunta la soglia per la prima volta); False altrimenti."""
        with self._lock:
            self._failures += 1
            if not self._escalated and self._failures >= self._threshold:
                self._escalated = True
                return True
            return False

    def record_success(self) -> bool:
        """Scrittura riuscita: azzera il contatore. Ritorna True se ERA in escalation (così il
        chiamante notifica il recovery una sola volta); False altrimenti."""
        with self._lock:
            was = self._escalated
            self._failures = 0
            self._escalated = False
            return was

    def reset(self) -> None:
        """Azzera contatore e stato SENZA segnalare un recovery. Da chiamare ai confini di
        sessione (START/STOP): i fallimenti di una sessione non devono "colare" nella
        successiva e far scattare una falsa escalation (review Codex #156)."""
        with self._lock:
            self._failures = 0
            self._escalated = False

    def text(self, path=None) -> str:
        """Messaggio di escalation visibile («CSV bloccato»), con il numero di tentativi."""
        msg = (f"🔒 CSV BLOCCATO: {self.count} scritture fallite di fila "
               f"(XTrader tiene il file aperto?). Retry automatico in corso: "
               f"chiudi o sblocca il CSV in XTrader.")
        if path:
            msg += f" File: {path}"
        return msg

    @staticmethod
    def recovery_text() -> str:
        """Messaggio di recupero, dopo che una scrittura torna a riuscire."""
        return "✅ CSV sbloccato: scritture riprese."
