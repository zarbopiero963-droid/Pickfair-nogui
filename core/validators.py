"""Helper condivisi di validazione/sanitizzazione input (fonte UNICA, anti-drift).

Raccoglie regole prima **duplicate** in più moduli safety-critical, così una
correzione non rischia di applicarsi a una copia e dimenticarne un'altra
(audit #105 / #133 item 6, parte "validatori"):

- `require_positive_int` / `require_finite_now`: validazione difensiva di
  parametri numerici e timestamp — un valore malformato (bool da JSON, NaN/inf,
  ``<= 0``, non intero) deve fallire con `ValueError`, non rendere un limite
  inefficace o sempre bloccante. Usati da `safety_guard.DailyLimiter` e
  `signal_dedupe.SignalTracker`.
- `WIN_RESERVED` + `safe_filename_core`: nucleo comune della sanitizzazione del
  nome file (Windows). I chiamanti (`custom_parser`, `profile_store`) applicano
  poi il PROPRIO fallback, volutamente diverso (vedi sotto), quindi qui resta solo
  la parte condivisa.

Modulo **puro**: nessuna dipendenza da GUI/CSV/Telegram, testabile headless.
"""

import math

# Nomi device riservati di Windows: un file con questo nome-base (anche con
# estensione) non è creabile. Match ESATTO, case-insensitive (es. "con" sì,
# "console" no). Fonte unica condivisa.
WIN_RESERVED = frozenset(
    {"con", "prn", "aux", "nul"}
    | {f"com{i}" for i in range(1, 10)}
    | {f"lpt{i}" for i in range(1, 10)}
)


def require_positive_int(value, name: str) -> int:
    """`value` come int finito e > 0, altrimenti `ValueError`.

    Rifiuta esplicitamente `bool` (``True``/``False`` da JSON verrebbero coerciti a
    1/0: es. `max_per_day=True` capperebbe l'app a 1 segnale/giorno invece di essere
    trattato come config malformata) e `NaN`/`inf`/`<= 0`/non-interi (renderebbero il
    limite inefficace o sempre bloccante)."""
    if isinstance(value, bool):
        raise ValueError(f"{name} non valido: {value!r}")
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        # OverflowError (#318 L1-1): un int troppo grande per un float (es. 10**400) — da config
        # corrotta/manomessa — NON è sottoclasse di ValueError, quindi senza catturarlo esplicito
        # `float()` propagherebbe e farebbe crashare l'handler/START. Trattato come malformato
        # (ValueError controllato), coerente con message_freshness/csv_writer.
        raise ValueError(f"{name} non valido: {value!r}") from None
    if not math.isfinite(f) or f <= 0 or f != int(f):
        raise ValueError(f"{name} deve essere un intero > 0 (ricevuto {value!r})")
    return int(f)


def require_finite_now(now) -> float:
    """`now` (epoch) come float finito, altrimenti `ValueError`.

    Rifiuta `bool` (``True``/``False`` non sono timestamp) e `NaN`/`inf`, che
    falserebbero finestra di deduplica, conteggio al minuto e reset giornaliero."""
    if isinstance(now, bool):
        raise ValueError(f"now non valido: {now!r}")
    try:
        f = float(now)
    except (TypeError, ValueError, OverflowError):
        # OverflowError (#318 L1-1): un int troppo grande per un float (es. 10**400) non è
        # sottoclasse di ValueError → senza catturarlo `float()` crasherebbe il chiamante
        # (DailyLimiter/SignalTracker). Trattato come `now` malformato (ValueError controllato).
        raise ValueError(f"now non valido: {now!r}") from None
    if not math.isfinite(f):
        raise ValueError(f"now deve essere finito (ricevuto {now!r})")
    return f


def safe_filename_core(name: str) -> str:
    """Nucleo condiviso della sanitizzazione di un nome file (Windows).

    Tiene solo alfanumerici, ``-``, ``_`` e spazi (poi spazi → ``_``); evita path
    traversal e caratteri non validi; prefissa con ``_`` i NOMI DEVICE RISERVATI
    (``con``/``nul``/``com1``…). Ritorna la stringa pulita, **eventualmente vuota**:
    il fallback su vuoto è LASCIATO al chiamante, perché diverge per dominio —
    `custom_parser` usa un default (``"parser"``), `profile_store` rifiuta il nome
    vuoto. Per questo i due `_safe_filename` restano funzioni separate, ma il nucleo
    è unico (anti-drift).

    Annotato `name: str` per il contratto, ma `str(name)` resta come rete difensiva:
    un chiamante che passi un non-stringa per errore non deve far crashare l'I/O."""
    cleaned = "".join(c for c in str(name).strip() if c.isalnum() or c in " -_")
    cleaned = "_".join(cleaned.split())
    if cleaned.casefold() in WIN_RESERVED:
        cleaned = "_" + cleaned
    return cleaned
