"""Path di stato runtime e costruzione pura dei guardrail del percorso di scrittura.

Estratto da `App._init_guards` (#136 item 1, refactor incrementale di `app.py`):
la costruzione di `SignalTracker`, `DailyLimiter` e `SignalQueue` — con i relativi
fallback fail-safe — è qui, in un modulo puro e testabile headless.

Vincoli di questo modulo:
- NON scrive né legge stato su disco (il load/save resta in `App`);
- NON tocca la GUI/tkinter e NON logga: gli avvisi di fallback sono restituiti in
  `GuardSet.warnings`, così il chiamante li logga e la logica di fallback resta
  verificabile senza ambiente GUI.

Così un `max_per_day` o un `clear_delay` invalido in config produce gli stessi
default sicuri di prima, ma ora il comportamento è coperto da test reali.
"""

import os

from . import safety_guard, signal_dedupe, signal_queue
from .config_store import DEFAULTS

# Nomi file dello stato persistito, accanto al config (AppData). Fonte UNICA:
# usati sia per costruire i path sia, indirettamente, dai test.
DEDUPE_STATE_FILE = "dedupe_state.json"
DAILY_STATE_FILE = "daily_state.json"
EVENT_JOURNAL_FILE = "event_journal.jsonl"
BETFAIR_DB_FILE = "betfair_dictionary.db"
BETFAIR_AUTOSYNC_STATE_FILE = "betfair_autosync_state.json"


def betfair_db_path(config_dir_path: str) -> str:
    """Path del dizionario Betfair locale (SQLite), accanto al config (AppData):
    sport/eventi/mercati/selezioni sincronizzati restano SOLO sul PC (issue #86).
    Nessun cloud, nessun export/import."""
    return os.path.join(config_dir_path, BETFAIR_DB_FILE)


def betfair_autosync_state_path(config_dir_path: str) -> str:
    """Path dello stato auto-sync (ultima run giorno+ora): persiste la guardia
    'una volta al giorno/orario' attraverso i riavvii (issue #86 PR-P8)."""
    return os.path.join(config_dir_path, BETFAIR_AUTOSYNC_STATE_FILE)


def event_journal_path(config_dir_path: str) -> str:
    """Path del ledger eventi append-only (issue #110 voce 20), accanto al config
    (AppData): lo storico strutturato di «cosa ha fatto» sopravvive ai riavvii."""
    return os.path.join(config_dir_path, EVENT_JOURNAL_FILE)


def dedupe_state_path(config_dir_path: str) -> str:
    """Path dello stato anti-duplicato, accanto al config (AppData): i duplicati
    recenti restano riconosciuti dopo un riavvio."""
    return os.path.join(config_dir_path, DEDUPE_STATE_FILE)


def daily_state_path(config_dir_path: str) -> str:
    """Path del conteggio giornaliero persistito: stop/start nello stesso giorno
    (UTC) NON deve azzerare il tetto (altrimenti il limite/giorno è aggirabile)."""
    return os.path.join(config_dir_path, DAILY_STATE_FILE)


class GuardSet:
    """Guardrail del percorso di scrittura costruiti dalla config + avvisi.

    `warnings` è la lista (in italiano) dei messaggi di fallback fail-safe che il
    chiamante DEVE loggare: tenerli qui rende la logica testabile senza GUI.
    """

    __slots__ = ("tracker", "daily", "queue", "queue_timeout", "mode", "warnings")

    def __init__(self, tracker, daily, queue, queue_timeout, mode, warnings):
        self.tracker = tracker
        self.daily = daily
        self.queue = queue
        self.queue_timeout = queue_timeout
        self.mode = mode
        self.warnings = warnings


def build_guards(cfg: dict) -> GuardSet:
    """Costruisce i guardrail dalla config con gli stessi fallback fail-safe di
    `App._init_guards`:

    - `max_per_day` invalido → `DailyLimiter()` di default + avviso;
    - `clear_delay`/timeout invalido per la coda → `SignalQueue` senza
      `default_timeout` (usa il proprio default) + avviso.

    Il tetto righe attive (`max_active_signals`, #136 p5) e la modalità coda
    (`queue_mode`) sono normalizzati dalle rispettive funzioni di dominio.
    Non carica stato da disco: il caricamento di dedupe/daily resta in `App`.
    """
    warnings: list[str] = []

    tracker = signal_dedupe.SignalTracker()

    try:
        daily = safety_guard.DailyLimiter(
            max_per_day=cfg.get("max_per_day", safety_guard.DEFAULT_MAX_PER_DAY))
    except ValueError:
        daily = safety_guard.DailyLimiter()
        warnings.append(
            f"⚠️ max_per_day non valido in config: uso {safety_guard.DEFAULT_MAX_PER_DAY}.")

    mode = signal_queue.normalize_mode(cfg.get("queue_mode"))
    delay = signal_queue.timeout_from_config(cfg)
    max_active = cfg.get("max_active_signals", DEFAULTS["max_active_signals"])
    try:
        queue = signal_queue.SignalQueue(
            mode=mode, default_timeout=delay, max_active=max_active)
    except ValueError:
        queue = signal_queue.SignalQueue(mode=mode, max_active=max_active)
        warnings.append("⚠️ clear_delay non valido per la coda: uso il default.")

    return GuardSet(
        tracker=tracker,
        daily=daily,
        queue=queue,
        queue_timeout=queue.default_timeout,
        mode=mode,
        warnings=warnings,
    )
