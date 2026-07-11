"""PR-21: aggancio dei guardrail al flusso live (decisione pura, testabile).

`app._process` produce una riga piazzabile (via `signal_router`) e poi deve
decidere **se scriverla davvero** nel CSV operativo. Questa decisione qui è pura:
combina i guardrail già esistenti (PR-15/PR-19) in un unico esito, così la GUI
resta sottile e la logica è testabile headless.

Ordine (fail-safe, anti-doppia-scommessa):
1. **dedup + limite/minuto** (`SignalTracker.register`): un duplicato o una raffica
   non devono scrivere;
2. **limite/giorno** (`DailyLimiter.allow`): tetto giornaliero;
3. **DRY_RUN** (`safety_guard.is_dry_run`): in simulazione non si scrive il CSV
   operativo.

Esiti:
- `WRITE`     → scrivi la riga;
- `DRY_RUN`   → riconosci il segnale ma NON scrivere (simulazione);
- `DUPLICATE` → stesso messaggio già visto nella finestra;
- `RATE_LIMITED` → troppi segnali nell'ultimo minuto;
- `DAILY_LIMITED` → raggiunto il tetto giornaliero.

Solo `WRITE` autorizza la scrittura: ogni altro esito la sopprime.
"""

from . import safety_guard, signal_dedupe

WRITE = "WRITE"
DRY_RUN = "DRY_RUN"
DUPLICATE = "DUPLICATE"
RATE_LIMITED = "RATE_LIMITED"
DAILY_LIMITED = "DAILY_LIMITED"


def evaluate(cfg, tracker, daily, text, *, now=None, dedup_key=None) -> str:
    """Decide l'esito del percorso di scrittura per `text` (vedi modulo).

    `tracker`: `signal_dedupe.SignalTracker` (dedup + limite/minuto); obbligatorio.
    `daily`: `safety_guard.DailyLimiter` o None (None = nessun limite giornaliero).
    `dedup_key` (#192): chiave di deduplica PER-RIGA per il multi-output (vedi
    `signal_dedupe.row_dedup_key`); assente → dedup sull'hash del messaggio (single-row).
    Effetti: un segnale accettato consuma una slot nel tracker e (se presente) nel
    `daily`; un duplicato/rate-limited NON consuma la slot giornaliera."""
    reg = tracker.register(text, now=now, key=dedup_key)
    if reg.status == signal_dedupe.DUPLICATE:
        return DUPLICATE
    if reg.status == signal_dedupe.RATE_LIMITED:
        return RATE_LIMITED
    if daily is not None and not daily.allow(now=now):
        return DAILY_LIMITED
    if safety_guard.is_dry_run(cfg):
        return DRY_RUN
    return WRITE
