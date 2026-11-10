"""Vocabolario degli eventi (categorie, tipi, livelli) condiviso dal journal e dalla scatola nera.

Modulo SENZA dipendenze dal progetto, estratto da `core.event_journal` (che lo riesporta con gli stessi nomi: API
invariata). La scatola nera (PF-BLACKBOX-01) sta nel percorso vivo di Pickfair e non deve trascinare
`core.event_journal` -> `core.event_log` -> `core.config_store` (Bridge XTrader) nel grafo degli import vivi.
"""

from __future__ import annotations

# Vocabolario degli eventi, raggruppato per CATEGORIA (Recorder fase A).
#
# Perche' a categorie e non un unico insieme piatto: con ~120 tipi i due consumatori
# che li enumerano diventerebbero inservibili — l'help di `--type` in `journal_view`
# e la tendina della scheda «Diario». La categoria da' un secondo asse di filtro
# ("mostrami tutto quello che riguarda gli ORDINI") senza dover conoscere i nomi.
#
# `LEGACY` sono gli 11 tipi dell'epoca bridge/CSV: restano validi e invariati, cosi'
# i ledger gia' scritti si rileggono e i punti di emissione esistenti non cambiano.
EVENT_CATEGORIES = {
    # Epoca bridge/CSV (#110 voce 20 / G2). NON rinominare: sono sui ledger esistenti.
    "LEGACY": frozenset({
        "START",
        "STOP",
        "SIGNAL_RECEIVED",
        "SIGNAL_PARSED",
        "SIGNAL_VALIDATED",
        "CSV_WRITTEN",
        "CSV_CLEARED",
        "XTRADER_CONFIRMED",
        "XTRADER_REJECTED",
        "RECONNECT",
        "CRASH_RECOVERY_CSV_CLEARED",
    }),
    # Ciclo di vita del processo: apre e chiude la storia di una sessione.
    "SESSION": frozenset({
        "SESSION_START",
        "SESSION_END",
        "SESSION_CHECKPOINT",
        "UNCAUGHT_EXCEPTION",
        "SHUTDOWN",
        "CRASH_RECOVERY",
        # PF-BLACKBOX-01 (BB-1): ciclo di vita del processo nella scatola nera
        "PROCESS_START",
        "UNCLEAN_TERMINATION",
        "COMPONENT_INIT",
    }),
    # Configurazione. `CONFIG_CHANGE` porta SEMPRE da dove arriva la modifica
    # (`source`: EXE_GUI / TELEGRAM_BOT / CONFIG_FILE / ENV / DEFAULT): e' la domanda
    # «questa impostazione l'ho messa dall'app o dal bot?», oggi non ricostruibile.
    "CONFIG": frozenset({
        "CONFIG_CHANGE",
        "CONFIG_LOAD",
        "CONFIG_VALIDATION_FAIL",
        "MODE_CHANGE",
        "PROFILE_SWITCH",
        "PARSER_CHANGE",
        "SOURCE_CHAT_ADD",
        "SOURCE_CHAT_REMOVE",
        "ALLOWLIST_CHANGE",
    }),
    # Interfaccia. `UI_ERROR_SHOWN` porta il testo ESATTO mostrato all'utente, cosi'
    # «mi e' uscito un errore ma non ricordo quale» smette di essere una domanda aperta.
    "UI": frozenset({
        "UI_CLICK",
        "UI_DOUBLE_CLICK",
        "UI_SCROLL",
        "UI_TAB_CHANGE",
        "UI_WINDOW_OPEN",
        "UI_WINDOW_CLOSE",
        "UI_INPUT_COMMIT",
        "UI_TOGGLE",
        "UI_DIALOG_SHOWN",
        "UI_DIALOG_DISMISSED",
        "UI_ERROR_SHOWN",
        "UI_ACTION_BLOCKED",
    }),
    # Telegram in ingresso e in uscita. `TG_MESSAGE_DROPPED` porta il MOTIVO: la
    # allow-list e' fail-closed, quindi «il bot non fa niente» ha quasi sempre questa
    # causa, e oggi non lascia traccia consultabile.
    "TELEGRAM": frozenset({
        "TG_CONNECT",
        "TG_DISCONNECT",
        "TG_UPDATE_RECEIVED",
        "TG_MESSAGE_IN",
        "TG_MESSAGE_DROPPED",
        "TG_MESSAGE_EDIT",
        "TG_COMMAND",
        "TG_CALLBACK_QUERY",
        "TG_SEND_ATTEMPT",
        "TG_SEND_OK",
        "TG_SEND_FAIL",
        "TG_RATE_LIMITED",
    }),
    # Dal testo del messaggio al mercato Betfair risolto.
    "SIGNAL": frozenset({
        "PARSE_ATTEMPT",
        "PARSE_OK",
        "PARSE_FAIL",
        "SIGNAL_CREATED",
        "SIGNAL_DEDUPED",
        "SIGNAL_STALE",
        "SIGNAL_QUEUED",
        "SIGNAL_DEQUEUED",
        "SIGNAL_ROUTED",
        "SIGNAL_REJECTED",
        "MAPPING_HIT",
        "MAPPING_MISS",
        "MARKET_RESOLVED",
        "MARKET_RESOLUTION_FAIL",
    }),
    # Betfair: sessione, chiamate, stato del mercato, rete.
    "BETFAIR": frozenset({
        "BF_LOGIN_ATTEMPT",
        "BF_LOGIN_OK",
        "BF_LOGIN_FAIL",
        "BF_KEEPALIVE",
        "BF_SESSION_EXPIRED",
        "API_REQUEST",
        "API_RESPONSE",
        "API_ERROR",
        "API_THROTTLED",
        "MARKET_STATE_CHANGE",
        "PRICE_SNAPSHOT",
        "PROXY_STATE",
        "NETWORK_DOWN",
        "NETWORK_UP",
    }),
    # Rischio e money management. `RISK_GATE_EVAL` porta i NUMERI di ogni regola, non
    # solo l'esito: e' cio' che permette di dire PERCHE' una giocata legittima e' stata
    # negata, invece di constatare che lo e' stata.
    "RISK": frozenset({
        "BANKROLL_READ",
        "STAKE_COMPUTED",
        # PF-BLACKBOX-01: riserve di fondi
        "FUNDS_RESERVED",
        "FUNDS_RELEASED",
        "RISK_GATE_EVAL",
        "RISK_GATE_DENY",
        "EXPOSURE_UPDATE",
        "CIRCUIT_BREAKER_OPEN",
        "CIRCUIT_BREAKER_CLOSE",
        "SAFE_MODE_ENTER",
        "SAFE_MODE_EXIT",
        "LIVE_GUARD_BLOCK",
        "EMERGENCY_STOP",
        "DUPLICATION_GUARD_HIT",
    }),
    # Ordini: il percorso dei soldi. `RECONCILIATION_DIFF` e' la prova regina di un
    # incidente — dice che stato locale e Betfair non erano d'accordo, quando e su cosa.
    "ORDER": frozenset({
        "ORDER_INTENT",
        "ORDER_PLACE_REQUEST",
        "ORDER_PLACE_RESPONSE",
        "ORDER_MATCHED",
        "ORDER_UNMATCHED",
        "ORDER_CANCEL_REQUEST",
        "ORDER_CANCEL_RESPONSE",
        "ORDER_REPLACE_REQUEST",
        "ORDER_REPLACE_RESPONSE",
        "ORDER_LAPSED",
        "ORDER_VOIDED",
        "ORDER_EXPIRED_TTL",
        "CASHOUT_REQUEST",
        "CASHOUT_COMPUTED",
        "CASHOUT_EXECUTED",
        "CASHOUT_RESIDUAL",
        "POSITION_UPDATE",
        "PNL_UPDATE",
        "RECONCILIATION_RUN",
        # PF-BLACKBOX-01: esiti per ordine e catena di protezione (oracolo di completezza di Claude D)
        "RECONCILIATION_RESOLVED",
        "SL_TRIGGERED",
        "TP_TRIGGERED",
        "TRAILING_TRIGGERED",
        "SL_TP_CLOSE_ROUTED",
        "SL_TP_CLOSE_NOT_EXECUTED",
        "SL_TP_CLOSE_UNCERTAIN",
        "SL_TP_CLOSE_REJECTED",
        "SL_TP_CLOSE_SKIPPED",
        "PROTECTION_WAITING",
        "PROTECTION_RESUMED",
        "PROTECTION_LATCH_RELEASED",
        "RECONCILIATION_DIFF",
        "SETTLEMENT",
    }),
    # Sistema. `CLOCK_SKEW` e' registrato perche' un wall-clock che salta (NTP,
    # sospensione del portatile) falsa OGNI latenza calcolata su `ts`, in silenzio.
    "SYSTEM": frozenset({
        "THREAD_START",
        "THREAD_STOP",
        "TASK_EXCEPTION",
        "RETRY",
        "TIMEOUT",
        "LATENCY_SPIKE",
        "DB_WRITE_FAIL",
        "DB_LOCK_CONTENTION",
        "CLOCK_SKEW",
        "DISK_SPACE_LOW",
        "INSTANCE_LOCK_CONFLICT",
    }),
    # PF-BLACKBOX-01: stato del registro stesso (guasti, lacune dichiarate, ripristino)
    "BLACKBOX": frozenset({
        "BLACKBOX_DEGRADED",
        "BLACKBOX_RECOVERED",
        "BLACKBOX_GAP",
        "BLACKBOX_UNAVAILABLE",
        "BLACKBOX_CORRUPT_TAIL",
    }),
    # PF-BLACKBOX-01: richieste ricevute dalle interfacce di controllo (Control API / MCP)
    "CONTROL": frozenset({
        "CONTROL_API_REQUEST",
    }),
}

# Vocabolario piatto. Fail-closed: un tipo non in elenco e' rifiutato da `make_event`.
EVENT_TYPES = frozenset().union(*EVENT_CATEGORIES.values())

# Tipo -> categoria. Costruito una volta sola; l'ULTIMA categoria vince in caso di
# tipo duplicato. Un duplicato e' un errore di scrittura del catalogo, non una
# condizione di runtime: NON si solleva qui (un import che esplode rende il modulo
# inservibile per un refuso), lo copre `test_event_journal_catalogo` che verifica la
# disgiunzione delle categorie.
_CATEGORY_BY_TYPE = {t: cat for cat, types in EVENT_CATEGORIES.items() for t in types}

# Livelli ammessi per il campo `lvl`. Sovrainsieme di `event_log.LEVELS`: il ledger
# distingue anche TRACE/DEBUG/CRITICAL, che il log testuale per l'utente non usa.
LEVELS = ("TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
DEFAULT_LEVEL = "INFO"


def category_of(event_type):
    """Categoria di un tipo evento (`"ORDER"`, `"TELEGRAM"`, ...), `None` se ignoto.

    Non solleva su un tipo sconosciuto: serve anche a classificare eventi LETTI da un
    ledger scritto da una versione futura/diversa, dove un tipo non piu' in catalogo
    non deve rompere la lettura."""
    return _CATEGORY_BY_TYPE.get(event_type)


def types_of_category(category) -> frozenset:
    """Tipi di una categoria (insieme vuoto se la categoria non esiste)."""
    return EVENT_CATEGORIES.get(str(category or "").upper(), frozenset())


def normalize_level(level) -> str:
    """Livello normalizzato a uno di `LEVELS`; ignoto/mancante -> `DEFAULT_LEVEL`.

    Asimmetria VOLUTA rispetto al tipo evento, che invece e' fail-closed: un tipo
    sbagliato e' un refuso che non deve entrare nel ledger, mentre un livello
    sbagliato e' solo un'etichetta — rifiutare l'evento perderebbe il FATTO per un
    problema di metadato. Si degrada a INFO e si scrive."""
    lvl = str(level or "").strip().upper()
    return lvl if lvl in LEVELS else DEFAULT_LEVEL
