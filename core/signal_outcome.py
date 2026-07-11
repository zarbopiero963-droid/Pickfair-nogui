"""Mappatura PURA degli esiti guardrail → presentazione.

Estratto da `App._after_non_write` / `_process` (#136 item 1, refactor incrementale
di `app.py`): la traduzione di una decisione `live_guard` (DRY_RUN / DUPLICATE /
RATE_LIMITED / DAILY_LIMITED) e di una scrittura CSV riuscita nel testo di log, nel
contatore della dashboard e nell'eventuale «ultimo segnale» è qui, testabile headless.

`App` applica il risultato via GUI (`_bump` / `_log` / `_set_last`): qui dentro NON si
tocca tkinter e non si scrive nulla.
"""

from __future__ import annotations

from dataclasses import dataclass

from . import confirmation_reader, live_guard


def _attivi_label(n_active) -> str:
    """Pluralizzazione delle righe attive: «attivo» se `n_active == 1`, altrimenti
    «attivi». Fonte UNICA, così la dicitura non diverge tra log e test."""
    return "attivo" if n_active == 1 else "attivi"


@dataclass(frozen=True)
class NonWriteOutcome:
    """Descrizione di presentazione per un esito che NON scrive il CSV.

    - `counter`: nome del contatore della dashboard da incrementare (`_bump`);
    - `log`: riga di log da mostrare (`_log`);
    - `last_signal`: testo «ultimo segnale» (`_set_last`), oppure `None` se l'esito
      non aggiorna l'ultimo segnale (solo DRY_RUN lo fa);
    - `last_color`: colore per «ultimo segnale» (significativo solo se `last_signal`).
    """

    counter: str
    log: str
    last_signal: str | None = None
    last_color: str | None = None


@dataclass(frozen=True)
class WriteOutcome:
    """Descrizione di presentazione per una scrittura CSV RIUSCITA.

    - `last_signal`: testo «ultimo segnale» (bianco) da `_set_last`;
    - `signal_log`: riga di log del segnale scritto (con la sorgente del parser);
    - `csv_log`: riga di log di conferma aggiornamento CSV (pluralizzazione
      «attivo»/«attivi» secondo il numero di righe attive).
    """

    last_signal: str
    signal_log: str
    csv_log: str


def describe_non_write(decision, row):
    """Ritorna la `NonWriteOutcome` per `decision`, o `None` se non è un esito
    non-WRITE noto (il chiamante non fa nulla, come prima dell'estrazione).

    `row` è la riga CSV parsata: si leggono solo `EventName`/`SelectionName`/`Price`
    (campi mancanti → stringa vuota), nessuna mutazione."""
    ev = row.get("EventName", "")
    sel = row.get("SelectionName", "")
    price = row.get("Price", "")
    if decision == live_guard.DRY_RUN:
        return NonWriteOutcome(
            counter="dry_run",
            log=f"🧪 DRY_RUN: segnale riconosciuto ma CSV NON scritto (simulazione): "
                f"{ev} | {sel}",
            last_signal=f"🧪 DRY_RUN — {ev}  |  {sel}  q.{price}",
            last_color="#ffb74d",
        )
    if decision == live_guard.DUPLICATE:
        return NonWriteOutcome(
            counter="duplicate",
            log=f"♻️ Duplicato ignorato (nessuna doppia scommessa): {ev} | {sel}",
        )
    if decision == live_guard.RATE_LIMITED:
        return NonWriteOutcome(
            counter="limited",
            log="🚦 Limite al minuto raggiunto: segnale ignorato.",
        )
    if decision == live_guard.DAILY_LIMITED:
        return NonWriteOutcome(
            counter="limited",
            log="🚦 Limite giornaliero raggiunto: segnale ignorato.",
        )
    return None


def describe_write(row, source, n_active):
    """Presentazione di una scrittura CSV riuscita per `row` (riga parsata),
    `source` (sorgente del parser) e `n_active` (righe attive nel CSV dopo la
    scrittura). Pura: legge solo `EventName`/`SelectionName`/`Price` (mancanti →
    stringa vuota), nessuna mutazione."""
    ev = row.get("EventName", "")
    sel = row.get("SelectionName", "")
    price = row.get("Price", "")
    return WriteOutcome(
        last_signal=f"🏆 {ev}  |  {sel}  |  q.{price}",
        signal_log=f"📱 Segnale ({source}): {ev}  |  {sel}  q.{price}",
        csv_log=f"✅ CSV aggiornato ({n_active} {_attivi_label(n_active)}) "
                f"→ XTrader può piazzare",
    )


def confirmation_removed_log(status):
    """Log della rimozione dal CSV dopo una conferma XTrader **terminale**:
    CONFIRMED → «confermato (CONFIRMED)», REJECTED → «rifiutato (REJECTED)».
    Ritorna `None` per qualsiasi altro status (incluso ignoto): il chiamante non logga."""
    if status == confirmation_reader.CONFIRMED:
        esito = "confermato (CONFIRMED)"
    elif status == confirmation_reader.REJECTED:
        esito = "rifiutato (REJECTED)"
    else:
        return None
    return f"✅ XTrader: segnale {esito} → rimosso dal CSV"


def confirmation_ignored_log(status):
    """Log per una notifica XTrader che NON rimuove nulla:
    - UNKNOWN: associata a un segnale ma esito non riconoscibile;
    - UNMATCHED: non associata ad alcun segnale attivo.
    Ritorna `None` per qualsiasi altro status (terminali CONFIRMED/REJECTED o ignoti):
    il chiamante non logga."""
    if status == confirmation_reader.UNKNOWN:
        return "ℹ️ Notifica XTrader associata a un segnale ma esito non chiaro: ignorata."
    if status == confirmation_reader.UNMATCHED:
        return "ℹ️ Notifica XTrader non associata ad alcun segnale attivo: ignorata."
    return None
