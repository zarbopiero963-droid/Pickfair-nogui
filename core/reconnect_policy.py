"""Politica di riconnessione del listener Telegram (logica pura, testabile in CI).

Il listener gira in un thread (`app._run_bot`). Se la connessione cade, vogliamo
**riprovare** con un ritardo crescente (backoff) finché il bridge è in esecuzione,
SENZA:
- ritentare dopo uno STOP manuale (è una chiusura voluta);
- ritentare all'infinito su un errore **permanente** (es. token invalido), che non
  si risolve da solo e farebbe girare a vuoto.

Qui sta solo la **decisione** (quanto aspettare, se ritentare): nessun widget, nessun
asyncio, nessuna dipendenza da `python-telegram-bot`. Così è interamente testabile.
La parte di I/O (ricostruire l'updater, attendere) vive nella vista sottile `app`.
"""

# Backoff esponenziale, in secondi: 2, 4, 8, 16, 32, … limitato a un tetto.
DEFAULT_BASE_DELAY = 2.0
DEFAULT_MAX_DELAY = 60.0

# Errori considerati **transitori** (rete/timeout): vale la pena riprovare. La
# classificazione preferisce `isinstance` sulle CLASSI REALI di `telegram.error` (vedi
# `is_transient_error`); questi NOMI restano come **fallback** per gli ambienti dove
# `python-telegram-bot` non è importabile (es. CI headless). Tutto ciò che NON è
# transitorio è trattato come NON recuperabile (token invalido, bug, auth…) → niente
# retry: meglio fermarsi e mostrare l'errore che ciclare a vuoto.
TRANSIENT_ERROR_NAMES = frozenset({
    "NetworkError",      # telegram.error.NetworkError (base dei problemi di rete)
    "TimedOut",          # telegram.error.TimedOut
    "RetryAfter",        # flood control: riprovare più tardi
})

# Cache dei tipi transitori REALI di telegram.error, risolti al primo uso.
# None = non ancora risolto; tupla (anche vuota) = risolto. Vuota → telegram non
# importabile, si ricade sul match per nome.
_TRANSIENT_TYPES_CACHE = None


def _real_transient_types():
    """Tupla delle classi transitorie reali di `telegram.error`
    (`NetworkError`/`TimedOut`/`RetryAfter`), risolta e cache-ata al primo uso.

    Tupla **vuota** se `telegram` non è importabile (es. CI/test headless dove la
    dipendenza non è installata): in tal caso `is_transient_error` ricade sul match
    per nome. L'import è lazy proprio per non richiedere `python-telegram-bot` al
    semplice import del modulo (così la logica resta testabile senza la dipendenza)."""
    global _TRANSIENT_TYPES_CACHE
    if _TRANSIENT_TYPES_CACHE is None:
        try:
            from telegram.error import NetworkError, RetryAfter, TimedOut
            _TRANSIENT_TYPES_CACHE = (NetworkError, TimedOut, RetryAfter)
        except Exception:   # noqa: BLE001 — telegram assente/incompleto → fallback per nome
            _TRANSIENT_TYPES_CACHE = ()
    return _TRANSIENT_TYPES_CACHE


def backoff_delay(attempt: int, base: float = DEFAULT_BASE_DELAY,
                  cap: float = DEFAULT_MAX_DELAY) -> float:
    """Ritardo (secondi) prima del tentativo di riconnessione `attempt` (1-based).

    `attempt=1 → base`, poi raddoppia ad ogni tentativo, limitato a `cap`. Valori
    `attempt < 1` sono trattati come 1 (mai un ritardo negativo o nullo)."""
    if attempt < 1:
        attempt = 1
    # Cappa l'ESPONENTE prima di elevare: dopo molte ore di tentativi `attempt`
    # diventa grande e `2 ** (attempt-1)` causerebbe OverflowError (uccidendo il
    # supervisor). Una volta raggiunto il cap il delay vi resta comunque, quindi
    # un esponente limitato (2**30 · base ≫ cap) non cambia il risultato.
    exponent = min(attempt - 1, 30)
    delay = base * (2 ** exponent)
    return float(min(delay, cap))


def effective_delay(attempt: int, retry_after=None,
                    base: float = DEFAULT_BASE_DELAY,
                    cap: float = DEFAULT_MAX_DELAY) -> float:
    """Ritardo effettivo (secondi) prima della riconnessione `attempt`, combinando il
    backoff esponenziale con l'eventuale `retry_after` di Telegram (flood control).

    Se l'eccezione porta un `retry_after` (es. `telegram.error.RetryAfter`) **più
    lungo** del backoff locale, si attende QUELLO: il server ha chiesto esplicitamente
    di non riprovare prima di quel tempo, quindi riconnettersi prima sarebbe inutile e
    peggiorerebbe il flood control. Un `retry_after` assente, non numerico o non
    maggiore del backoff non ha effetto (resta il backoff).

    I `bool` sono rifiutati prima del confronto (come altrove nel progetto): `True`
    non è un `retry_after` valido, solo un intero accidentale da `getattr`."""
    delay = backoff_delay(attempt, base, cap)
    if isinstance(retry_after, bool):
        return delay
    if isinstance(retry_after, (int, float)) and retry_after > delay:
        return float(retry_after)
    return delay


def is_transient_error(exc: BaseException) -> bool:
    """`True` se l'eccezione è un errore transitorio noto (rete/timeout) per cui ha
    senso riconnettersi. Un `TimedOut(NetworkError)` è transitorio mentre
    `InvalidToken`/`ValueError` no.

    Classificazione (audit C6): quando `python-telegram-bot` è importabile si usa
    `isinstance` sulle CLASSI REALI di `telegram.error` — preciso. Il vecchio match per
    **nome di classe** sull'MRO trattava come transitoria QUALSIASI eccezione che
    condividesse solo il nome (es. un `NetworkError` permanente di un'altra libreria o
    una sottoclasse omonima): un falso positivo che mandava il supervisor in un loop di
    reconnect infinito ("running ma sordo"). Con `isinstance` un omonimo NON-telegram non
    è istanza del tipo reale → trattato come NON transitorio.

    Fallback: se `telegram` non è importabile (es. CI/test headless) si ricade sul match
    per nome di prima, così la logica resta testabile senza la dipendenza."""
    types = _real_transient_types()
    if types:
        return isinstance(exc, types)
    # Fallback (telegram non importabile): match per nome lungo l'MRO come in precedenza.
    names = {cls.__name__ for cls in type(exc).__mro__}
    return bool(names & TRANSIENT_ERROR_NAMES)


def should_reconnect(running: bool, exc: BaseException) -> bool:
    """Decisione finale del supervisor: riconnettersi solo se il bridge è ancora in
    esecuzione (uno STOP manuale azzera `running` → nessun retry) **e** l'errore è
    transitorio. Un errore permanente o una chiusura voluta non vanno ritentati."""
    return bool(running) and is_transient_error(exc)
