"""Caricamento/salvataggio configurazione (funzioni pure, nessuna GUI).

PR-04: la configurazione vive in una cartella utente persistente
(`%APPDATA%\\XTraderBridge\\config.json` su Windows), così sopravvive a
spostamenti/aggiornamenti/reinstallazioni dell'EXE. Alla prima esecuzione, se
esiste un vecchio `config.json` accanto all'eseguibile, viene migrato.
Un file corrotto viene messo da parte (`.bak`) e si riparte dai default.
"""

import copy
import json
import logging
import math
import os
import shutil
import sys

from . import (atomic_io, autostart, bridge_mode, confirmation_reader, csv_writer,
               dizionario, language_select, recognition, safety_guard, token_store)

APP_DIR_NAME = "XTraderBridge"
CONFIG_VERSION = 1

# Prefisso/suffisso del file temporaneo della scrittura atomica del config: fonte
# unica così i test si allineano senza duplicare la stringa (finding Sourcery).
TMP_PREFIX = ".config_"
TMP_SUFFIX = ".tmp"

# Marker SOLO-IN-RAM (mai su disco) che `load_config` mette nella config quando il file
# era corrotto ed è stato messo da parte come `.bak` (issue #199). Segnala a `save_config`
# che un `bot_token` vuoto è il RESIDUO della corruzione (sentinel perso), non un clear
# voluto: il ramo CLEAR allora PRESERVA il token nel keyring invece di cancellarlo.
POST_CORRUPTION_KEY = "_post_corruption"

# Marker SOLO-IN-RAM (mai su disco) che `load_config` mette quando il sentinel dice "keyring"
# ma il token NON è stato reidratato perché il keyring era ILLEGGIBILE al load (outage transitorio,
# #140). In quel caso il `bot_token` resta "" pur potendo esistere ancora una credenziale nel
# keyring: un `bot_token` vuoto al save successivo NON è un clear voluto, ma il RESIDUO di un load
# incompleto. `save_config` lo legge e, nel ramo CLEAR, PRESERVA il token invece di cancellarlo
# (un delete distruggerebbe un token che l'utente non ha mai chiesto di rimuovere).
TOKEN_LOAD_INCOMPLETE_KEY = "_token_load_incomplete"

# Tutti i marker SOLO-IN-RAM: flag interni che `load_config` può mettere nella config viva ma che
# NON devono MAI finire su disco né, soprattutto, in un PROFILO (Codex #256). `profile_store` li
# rimuove insieme ai segreti: caricati su uno stato diverso falserebbero la logica clear/preserve
# del token (es. preservare/resuscitare un token che lo stato corrente teneva cancellato).
RAM_ONLY_KEYS = (POST_CORRUPTION_KEY, TOKEN_LOAD_INCOMPLETE_KEY)

# ── Contratto a STATI ESPLICITI di save_config (#255 line-647) ─────────────────────────────
# Prima `save_config` tornava `(config, ok: bool)` e `ok=False` ACCORPAVA cause diverse: disco
# fallito (permessi/spazio), token non persistibile per un outage del keyring (niente scritto,
# riprova), config su disco corrotto su un save parziale. La GUI mostrava un unico messaggio
# "FALLITO su disco / controlla permessi" anche quando il disco era a posto (es. keyring giù),
# fuorviando l'utente. Ora ogni esito ha uno `status` esplicito così la GUI può dare il messaggio
# GIUSTO. I valori sono stringhe stabili (usabili anche in test/log).
SAVE_OK = "ok"                       # scritto su disco con successo
SAVE_DISK_ERROR = "disk_error"       # scrittura atomica fallita (OSError: permessi/spazio/IO)
SAVE_TOKEN_DEFERRED = "token_deferred"   # bot token non persistibile ora (keyring giù/illeggibile/
                                         # non scrivibile): NIENTE scritto su disco, riprova
SAVE_CONFIG_CORRUPT = "config_corrupt"   # save parziale abortito: config su disco corrotto e
                                         # puntatore keyring non in memoria → non si sovrascrive

# Messaggi utente per ciascuno stato NON-ok (single source of truth: GUI e test pescano da qui,
# niente stringhe divergenti per-call-site). Niente segreti nel testo (mai il token).
_SAVE_STATUS_MESSAGES = {
    SAVE_DISK_ERROR: ("Salvataggio FALLITO su disco: le impostazioni sono attive solo in "
                      "memoria. Controlla permessi/spazio del percorso di configurazione."),
    SAVE_TOKEN_DEFERRED: ("Salvataggio RIMANDATO: il bot token non è memorizzabile ora "
                          "(keyring di sistema non disponibile/illeggibile). NIENTE è stato "
                          "scritto su disco; le impostazioni restano attive in memoria. "
                          "Riprova quando il keyring è di nuovo disponibile."),
    SAVE_CONFIG_CORRUPT: ("Salvataggio ANNULLATO: il config.json su disco è corrotto/illeggibile "
                          "e il puntatore al keyring del token non è in memoria. Non si "
                          "sovrascrive per non orfanare il token. Ripristina o rimuovi il "
                          "config.json corrotto e riprova."),
}


def save_status_message(status) -> str:
    """Messaggio utente per uno `status` di `save_config` (vuoto per `SAVE_OK`/sconosciuto).

    Fonte UNICA dei testi degli esiti NON riusciti, così ogni call-site GUI mostra lo stesso
    messaggio coerente con la causa reale (disco vs keyring vs config corrotto) — niente più
    "FALLITO su disco" quando il problema è il keyring (#255 line-647)."""
    return _SAVE_STATUS_MESSAGES.get(status, "")


class SaveResult(tuple):
    """Esito di `save_config`. È RETRO-COMPATIBILE con la vecchia tupla `(config, ok)` — quindi
    `saved, ok = save_config(...)` continua a funzionare invariato — ma porta anche `.status`
    (uno dei `SAVE_*`) per messaggi GUI specifici della causa (#255 line-647). Modellata come
    `os.stat_result`/`urlsplit`: una sottoclasse di `tuple` con campi nominati in più. (Niente
    `__slots__`: una sottoclasse di `tuple` non lo supporta non-vuoto; `_status` vive nel
    `__dict__` d'istanza, irrilevante per la retro-compatibilità di unpacking/indexing.)"""

    def __new__(cls, config, ok, status):
        self = super().__new__(cls, (config, ok))
        self._status = status
        return self

    @property
    def config(self):
        """La config tenuta in memoria (elemento 0 della tupla)."""
        return self[0]

    @property
    def ok(self) -> bool:
        """True se scritto su disco (elemento 1 della tupla); equivalente al vecchio `ok`."""
        return self[1]

    @property
    def status(self) -> str:
        """Esito granulare: uno dei `SAVE_*`."""
        return self._status

# Logger di modulo: un salvataggio/migrazione config fallito NON deve restare
# silenzioso (prima era `except: pass`). Resta comunque best-effort — l'app non
# crasha e prosegue dai default — ma l'errore diventa visibile per la diagnosi.
logger = logging.getLogger(__name__)

DEFAULTS = {
    "config_version":   CONFIG_VERSION,
    "bot_token":        "",
    "chat_id":          "",
    "csv_path":         r"C:\XTrader\segnali.csv",
    "clear_delay":      90,
    "provider":         "TelegramBot",
    # Tema UI (#288 Delta 1): "dark" / "light". Default sicuro "dark" (tema storico
    # dell'app): una config vecchia senza il campo, o un valore malformato, eredita lo scuro.
    "theme":            "dark",
    # Lingua del CSV XTrader/Betting Toolkit (#342, fondazione multilingua #343): "IT"/"EN"/"ES".
    # Governa il SEPARATORE DECIMALE scritto nel CSV (IT/ES = virgola, EN = punto — il supporto
    # XTrader ha confermato che la versione ITA attuale legge le quote/points con la virgola).
    # Default sicuro "IT" (target principale). Senza campo GUI in questa slice: si imposta via
    # config.json; il selettore lingua all'avvio arriva con #343.
    "csv_language":     "IT",
    # Lingua dell'APPLICAZIONE (#343 slice «selettore lingua»): "IT"/"EN"/"ES" scelta
    # dall'utente al primo avvio, oppure "" = mai scelta → il selettore ricompare.
    # Fail-closed: un valore malformato torna "" (NON default IT: la scelta è
    # dell'utente). Alla scelta viene allineata anche `csv_language`.
    "app_language":     "",
    # Modalità di riconoscimento XTrader: ID_ONLY / NAME_ONLY / BOTH.
    # Default NAME_ONLY: oggi il bridge non ricava gli ID dal messaggio Telegram.
    "recognition_mode": "NAME_ONLY",
    # NB: la quota obbligatoria sì/no NON è più una chiave globale: la governa la
    # casella «Obblig.» sulla riga Price di ogni Parser Personalizzato (per-parser).
    # Parser Personalizzato attivo (nome; "" = usa il parser hardcoded). CP-07.
    "active_parser":    "",
    # Override per chat sorgente: {chat_id: nome_parser}. Vuoto = usa active_parser.
    "parser_by_chat":   {},
    # PR-2 (router multi-parser): {chat_id: [nome_parser, ...]} — PIÙ parser per chat,
    # valutati in ordine; scattano TUTTI quelli le cui condizioni combaciano. Vuoto = usa
    # `parser_by_chat` (singolo) o `active_parser`. Retro-compat: assente → nessun multi.
    "parser_list_by_chat": {},
    # Chat sorgente multiple (PR-12): lista di {name, chat_id, enabled, provider,
    # mode PRE/LIVE}. Vuoto = setup mono-chat classico (chat_id + provider globali).
    "source_chats":     [],
    # Conferme XTrader (PR-17): chat SEPARATA dalle sorgenti su cui XTrader notifica
    # l'esito; timeout senza conferma; keyword (vuote = usa i default del modulo).
    "xtrader_notification_chat_id": "",
    "confirmation_timeout":         120,
    "confirmation_keywords":        [],
    "rejection_keywords":           [],
    # Guardrail di sicurezza (PR-19): in DRY_RUN (simulazione) il CSV operativo non
    # viene scritto. Default sicuro True: una config vecchia senza il campo eredita
    # la simulazione, così un aggiornamento non genera scommesse reali per sbaglio.
    "dry_run":                      True,
    # Modalità nominata del bridge (#311 §3.1): SIMULAZIONE / COLLAUDO / REALE.
    # Stato DERIVATO e coerente con `dry_run` (che resta l'unica fonte del percorso
    # di scrittura): la coercion la ri-deriva sempre via `bridge_mode.mode_from_cfg`
    # (config incoerente → vince dry_run, fail-closed).
    "bridge_mode":                  "SIMULAZIONE",
    # Tetto di segnali nuovi accettati in un giorno (UTC), complementare al
    # limite/minuto di signal_dedupe (PR-15).
    "max_per_day":                  200,
    # Coda dei segnali attivi (PR-22): OVERWRITE_LAST (default sicuro: un solo segnale
    # attivo alla volta) / APPEND_ACTIVE / QUEUE_UNTIL_CONFIRMED. Le ultime due
    # producono PIÙ righe attive nel CSV (più scommesse simultanee).
    "queue_mode":                   "OVERWRITE_LAST",
    # Tetto di righe/segnali attivi simultanei nelle modalità coda multi-riga (#136 p5):
    # un nuovo segnale oltre il tetto viene BLOCCATO (ritentabile). Default basso (2) per
    # limitare le scommesse simultanee; ininfluente in OVERWRITE_LAST (sempre 1 riga).
    "max_active_signals":           2,
    # Anti-segnale-stantio (PR reconnect): un messaggio Telegram più vecchio di questi
    # secondi viene SCARTATO all'arrivo (probabile recupero dopo una disconnessione,
    # quando la connessione torna e arretrati vengono rifetchati). 0 = filtro disattivo.
    "max_signal_age":               120,
    # Avvio automatico del listener all'apertura dell'app. Default sicuro False: il
    # bridge parte solo con START manuale. Se True parte da solo SOLO se token e chat
    # sono configurati; in modalità REALE chiede comunque conferma prima di avviare.
    "auto_start_listener":          False,
    # Privacy dei log (audit #105 P1): se False (default), il TESTO del messaggio
    # Telegram NON viene loggato in chiaro — solo hash + lunghezza + prima riga
    # troncata (vedi `log_privacy`). True logga il payload completo (opt-in di debug
    # consapevole). Default OFF = privacy on; coerce via `as_bool_optin` (ALLOWLIST
    # fail-closed: solo un "sì" esplicito riconosciuto attiva, refusi/sconosciuti → OFF).
    "debug_message_payload":        False,
    # Auto Sync del dizionario Betfair (issue #86 PR-P8). Default OFF: l'auto-sync
    # parte solo se l'utente la attiva esplicitamente. `betfair_auto_sync_hour` è
    # l'ora locale (HH, 0-23) in cui scatta una volta al giorno; default 23.
    # `betfair_sync_sports` è la lista degli sport da sincronizzare.
    "betfair_auto_sync":            False,
    "betfair_auto_sync_hour":       23,
    "betfair_sync_sports":          ["Calcio", "Tennis", "Basket", "Rugby Union"],
}


VALID_THEMES = ("dark", "light")


def normalize_theme(value) -> str:
    """Normalizza il tema UI (#288 Delta 1) a ``"dark"``/``"light"`` (case-insensitive,
    spazi ai bordi ignorati). Qualsiasi valore mancante, non-stringa o non riconosciuto →
    default sicuro ``"dark"`` (il tema storico dell'app): una config vecchia o un
    `config.json` editato a mano con un valore sporco non lascia mai l'UI in uno stato
    indefinito. Unica fonte di verità, usata sia in `load_config` sia dal toggle nell'app."""
    if isinstance(value, str) and value.strip().lower() in VALID_THEMES:
        return value.strip().lower()
    return "dark"


def as_bool(value) -> bool:
    """Coercizione robusta a bool, condivisa dai vari moduli (config da JSON o stringhe
    truthy/falsey). `bool`→sé; numeri→`!= 0`; stringa→False solo se vuota o in
    ``{"0","false","no","off"}`` (case-insensitive). Unica fonte per evitare versioni
    divergenti dello stesso helper."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() not in ("", "0", "false", "no", "off")


# Token ESPLICITAMENTE "acceso" per i flag opt-in (privacy/sicurezza). È un'ALLOWLIST,
# non una denylist: per un opt-in tutto ciò che non è un "sì" riconosciuto deve restare
# SPENTO (fail-closed). Così un valore mancante/ambiguo o un refuso editato a mano
# (`"flase"`, `"disabled"`, `"null"`) NON attiva per sbaglio il comportamento.
_OPTIN_TRUE = frozenset({"1", "true", "yes", "on", "y", "t"})


def as_bool_optin(value) -> bool:
    """Coercizione **allowlist, fail-closed** per i flag **opt-in** con default OFF
    (es. `debug_message_payload`): True SOLO per un valore esplicitamente acceso.

    - `bool` → sé; numero **finito** → `!= 0` (NaN/±Infinity da un `config.json`
      corrotto/editato a mano NON sono un "sì" esplicito → False, #258/#259 C8);
    - stringa → True solo se (normalizzata) è in ``_OPTIN_TRUE`` (`1/true/yes/on/y/t`);
    - QUALSIASI altro valore (None/`null`/vuoto, `"0"/"false"/"off"`, ma anche stringhe
      non riconosciute come `"flase"/"disabled"`) → **False**.

    Differenza voluta da `as_bool` (denylist, fail-OPEN sulle stringhe non riconosciute):
    un opt-in di privacy NON deve accendersi per un refuso o un valore sconosciuto
    (finding Codex P1). Fonte UNICA: evita che la regola fail-closed diverga tra
    `_migrate`, il settings controller e il runtime (finding Sourcery)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        # Un int non può essere NaN/inf: basta `!= 0`. NON passare da `math.isfinite`,
        # che su int fuori range float (es. 10**400, ammesso dal JSON) solleva
        # OverflowError trasformando un config corrotto in un crash al load
        # (Codex P2 su #299) invece del fail-closed documentato.
        return value != 0
    if isinstance(value, float):
        # Stessa guardia di `autostart.is_enabled`: un float non finito non è un
        # "true" esplicito (fail-closed).
        return math.isfinite(value) and value != 0
    return str(value).strip().lower() in _OPTIN_TRUE


def config_dir() -> str:
    """Cartella utente per i dati dell'app.

    Windows: ``%APPDATA%\\XTraderBridge``. Altrove (dev/CI/Linux/macOS):
    ``$XDG_CONFIG_HOME/XTraderBridge`` o ``~/.config/XTraderBridge``.
    """
    base = (
        os.environ.get("APPDATA")
        or os.environ.get("XDG_CONFIG_HOME")
        or os.path.join(os.path.expanduser("~"), ".config")
    )
    return os.path.join(base, APP_DIR_NAME)


def config_path() -> str:
    return os.path.join(config_dir(), "config.json")


def legacy_config_path() -> str:
    """Vecchia posizione del config: accanto all'EXE/script.

    Nell'EXE PyInstaller (`--onefile`) `__file__` punta al bundle temporaneo,
    quindi quando l'app è "frozen" usiamo la cartella di `sys.executable` (dove
    sta davvero `XTrader-Signal-Bridge.exe`), così la migrazione trova il
    vecchio `config.json` accanto all'eseguibile.
    """
    if getattr(sys, "frozen", False):
        base = os.path.dirname(os.path.abspath(sys.executable))
    else:
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "config.json")


# Posizione persistente (nuova) e posizione legacy (accanto all'EXE/script).
CONFIG_FILE = config_path()
LEGACY_CONFIG_FILE = legacy_config_path()


def csv_path_problem(path) -> str:
    """Diagnostica PURA del `csv_path` da fare a START (#184 low-csvpath-validate): ritorna una
    stringa col problema (messaggio per l'utente) oppure `""` se il path è plausibilmente usabile.

    Controlla (senza creare nulla né aprire il file — l'I/O reale e gli errori di lock/permessi
    restano a `csv_writer.init_csv`): path non vuoto, la **cartella padre esiste** ed è una
    directory, e il path non è esso stesso una cartella. Serve a dare un errore CHIARO e azionabile
    a START (es. default `C:\\XTrader\\segnali.csv` con la cartella mancante) invece di un generico
    `FileNotFoundError` dall'inizializzazione del CSV."""
    p = str(path or "").strip()
    if not p:
        return "Percorso CSV vuoto: imposta un file .csv valido."
    if os.path.isdir(p):
        return f"Il percorso CSV punta a una cartella, non a un file: {p}"
    d = os.path.dirname(p) or "."
    if not os.path.exists(d):
        return f"La cartella del CSV non esiste: {d}. Crea la cartella o correggi il percorso CSV."
    if not os.path.isdir(d):
        return f"Il percorso del CSV non è dentro una cartella valida: {d}"
    return ""


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(os.path.abspath(path))
    if d:
        os.makedirs(d, exist_ok=True)


def migrate_legacy_config(new_path: str = CONFIG_FILE,
                          legacy_path: str = LEGACY_CONFIG_FILE) -> bool:
    """Se il config nuovo non esiste ma c'è quello vecchio, lo copia.

    Non distruttivo: il file legacy viene lasciato dov'è. Ritorna True se ha
    effettivamente migrato qualcosa.
    """
    try:
        if (not os.path.exists(new_path)
                and os.path.abspath(legacy_path) != os.path.abspath(new_path)
                and os.path.exists(legacy_path)):
            _ensure_dir(new_path)
            # Copia ATOMICA (audit L3): file temporaneo nella stessa cartella + flush/fsync +
            # `os.replace`, come `save_config`. `shutil.copyfile` non è atomico: un'interruzione
            # a metà (crash/blackout) lascerebbe un `config.json` troncato nella posizione nuova
            # alla prima esecuzione, costringendo a ripartire dai default. Copia BINARIA byte-per-byte
            # (modalità "wb"): il legacy può non essere UTF-8 valido, non va re-decodificato.
            def _copy_legacy(f):
                with open(legacy_path, "rb") as src:
                    shutil.copyfileobj(src, f)
            atomic_io.atomic_write(new_path, _copy_legacy, prefix=TMP_PREFIX, suffix=TMP_SUFFIX,
                                   mode="wb", encoding=None)
            return True
    except Exception as exc:   # noqa: BLE001 — best-effort, ma ora loggato (non silenzioso)
        # exc_info=True: l'except è ampio, il traceback aiuta a capire la causa.
        logger.warning("Migrazione config legacy fallita (%s -> %s): %s",
                       legacy_path, new_path, exc, exc_info=True)
    return False


# Chiavi-lista che accettano anche una STRINGA singola come formato valido (keyword
# conferma/rifiuto XTrader): vanno normalizzate, non azzerate (finding Codex P2).
_KEYWORD_KEYS = ("confirmation_keywords", "rejection_keywords")

# Chiavi STRINGA note per cui spazi/newline ai bordi non sono mai significativi e
# falserebbero il confronto a valle: vanno strippate in `_migrate` (issue #184 M1). In
# particolare un `chat_id`/notifiche con whitespace o newline (config editata a mano,
# copia-incolla da Telegram) renderebbe "sordo" il filtro chat — fail-closed: nessuna bet
# sbagliata, ma il bridge SMETTE di ascoltare quella chat — e un `recognition_mode`/
# `queue_mode`/`active_parser`/`provider` con padding non matcherebbe il valore atteso.
# Esclusi di proposito: `bot_token` (segreto, gestito da `token_store`/keyring, fuori scope)
# e `csv_path` (path: la validazione è un finding separato).
_STRIP_STR_KEYS = frozenset({
    "chat_id", "xtrader_notification_chat_id", "provider",
    "recognition_mode", "queue_mode", "active_parser",
})


def _coerce_int(value, default: int) -> int:
    """int robusto per la migrazione config (audit C5): accetta int/float e stringhe
    numeriche (`"90"`); su un valore non interpretabile torna al `default` SICURO invece
    di propagare un tipo sbagliato ai consumer. `bool` è sottoclasse di `int` ma NON è un
    numero qui (`True` da JSON non deve diventare `1` di timeout) → torna al default."""
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        # Solo float FINITI e INTERI: un `max_signal_age: 0.5` NON deve troncare a 0
        # (un valore <= 0 disattiva il filtro anti-stale → backlog vecchio passa,
        # finding Codex P2); `inf`/`nan` da un JSON editato a mano → default sicuro.
        if math.isfinite(value) and value.is_integer():
            return int(value)
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _migrate(cfg: dict) -> dict:
    """Coercizione difensiva dei tipi noti (audit C5).

    `cfg.update(data)` sovrappone tipi arbitrari da un file vecchio o editato a mano
    (es. `"90"` stringa dove serve un intero, una stringa dove serve una lista): senza
    questo passo la sicurezza dipenderebbe dal fatto che OGNI consumer sia difensivo.
    Qui riportiamo ogni chiave NOTA (presente nei `DEFAULTS`) al tipo del suo default;
    un valore non interpretabile torna al default **sicuro** (es. `dry_run` resta `True`
    → simulazione). Le chiavi sconosciute/future NON vengono toccate (forward-compat):
    `config_version` con skew su disco viene preservato come intero.

    NB: l'ordine dei controlli mette `bool` prima di `int` perché `bool` è sottoclasse
    di `int` in Python.
    """
    for key, default in DEFAULTS.items():
        if isinstance(default, bool):
            # Le bool di SICUREZZA hanno semantica per-chiave OPPOSTA e fail-closed che
            # un coercitore generico fail-OPEN come `as_bool` falserebbe (finding Codex P1 /
            # CodeRabbit): `dry_run` (default True) deve restare in simulazione su valore
            # sporco/vuoto, `auto_start_listener` (default False) NON deve auto-avviarsi su
            # un valore non esplicitamente truthy. Deleghiamo alla funzione CANONICA di
            # ciascuna (single source of truth: stessi insiemi truthy/falsey dei consumer),
            # così il valore migrato coincide esattamente con la decisione di sicurezza.
            # Una eventuale bool futura senza semantica dedicata ricade su `as_bool`.
            if key == "dry_run":
                cfg[key] = safety_guard.is_dry_run(cfg)
            elif key == "auto_start_listener":
                cfg[key] = autostart.is_enabled(cfg)
            elif key == "debug_message_payload":
                # Privacy fail-closed (helper unico): solo un truthy ESPLICITO attiva il log
                # completo; None/`null`/vuoto → False (il payload resta redatto di default).
                cfg[key] = as_bool_optin(cfg.get(key))
            elif key == "betfair_auto_sync":
                # Auto-sync Betfair = opt-in fail-closed (issue #86 PR-P8): un valore
                # sporco/typo (`"flase"`, `"disabled"`) NON deve attivare il ciclo
                # automatico login→sync→logout. Solo un truthy esplicito accende.
                cfg[key] = as_bool_optin(cfg.get(key))
            else:
                cfg[key] = as_bool(cfg.get(key, default))
        elif isinstance(default, int):
            cfg[key] = _coerce_int(cfg.get(key), default)
            # Il tetto di righe attive deve essere >= 1 (un 0/negativo da config editata a
            # mano disattiverebbe il limite di sicurezza): in tal caso torna al default (#136 p5).
            if key == "max_active_signals" and cfg[key] < 1:
                cfg[key] = default
        elif isinstance(default, str):
            val = cfg.get(key, default)
            val = val if isinstance(val, str) else (default if val is None else str(val))
            # M1 (#184): normalizza i campi stringa noti togliendo spazi/newline ai bordi, così
            # un `chat_id` con padding non rende "sordo" il filtro chat e i valori-modalità
            # combaciano. Solo l'allowlist `_STRIP_STR_KEYS`; gli altri str restano invariati.
            if key in _STRIP_STR_KEYS:
                val = val.strip()
            if key == "theme":
                # Tema UI: solo "dark"/"light" (case-insensitive); mancante/malformato →
                # default sicuro "dark" (#288 Delta 1). Unico punto: `normalize_theme`.
                val = normalize_theme(val)
            elif key == "csv_language":
                # Lingua CSV (#342): solo IT/EN/ES (case-insensitive); mancante/malformata →
                # default sicuro "IT". Unica fonte: `csv_writer.normalize_csv_language`.
                val = csv_writer.normalize_csv_language(val)
            elif key == "app_language":
                # Lingua APP (#343): IT/EN/ES o "" (mai scelta). Fail-closed: valore
                # sporco → "" e il selettore all'avvio ricompare — mai un default IT
                # silenzioso spacciato per scelta. Fonte unica: `language_select`.
                val = language_select.normalize_app_language(val)
            elif key == "bridge_mode":
                # Modalità nominata (#311 §3.1): SEMPRE ri-derivata dalla coppia salvata
                # via `mode_from_cfg` — `dry_run` (già coerciuto sopra, viene prima nei
                # DEFAULTS) è autoritativo: un `bridge_mode` sporco/incoerente non può
                # accendere la scrittura (fail-closed) né declassare un reale confermato.
                val = bridge_mode.mode_from_cfg(cfg)
            cfg[key] = val
        elif isinstance(default, list):
            if key in _KEYWORD_KEYS:
                # Una STRINGA singola è un formato SUPPORTATO per le keyword di conferma/
                # rifiuto (config scritta a mano: `confirmation_reader.normalize_keywords` la
                # avvolge come singola keyword, e il settings controller gestisce la CSV-string).
                # Azzerarla a `[]` farebbe ricadere `app._handle_confirmation` sui default del
                # modulo, ignorando i custom XTrader words → segnale chiuso solo a timeout
                # (finding Codex P2). La normalizziamo alla lista canonica; vuoto/tipo inatteso
                # → `[]` (= usa le keyword di default del modulo).
                cfg[key] = confirmation_reader.normalize_keywords(cfg.get(key)) or []
            elif not isinstance(cfg.get(key), list):
                cfg[key] = copy.deepcopy(default)
        elif isinstance(default, dict):
            if not isinstance(cfg.get(key), dict):
                cfg[key] = copy.deepcopy(default)
    return cfg


def _default_recognition_mode() -> str:
    """Default `recognition_mode` per una config **NUOVA** (#311-2.3, gate su #311-2.2).

    `BOTH` («ID se univoci, altrimenti nomi») **solo** se il dizionario Betfair è **pienamente
    validato** (`dizionario.is_validated()`); altrimenti `NAME_ONLY`. Motivo (blocker Fugu Ultra su
    #374): con `BOTH` il bridge accetta un segnale sugli ID risolti dal dizionario; finché il
    dizionario non è validato da un export reale, un match errato scriverebbe ID sbagliati → in
    modalità REALE una scommessa sul mercato/selezione errato. Il gate lega quindi 2.3 a 2.2 in
    automatico: **oggi** il dizionario ha righe «Generato da schema» → ritorna `NAME_ONLY`
    (comportamento invariato); diventa `BOTH` **da solo** quando #2.2 promuove tutte le righe a
    «Export XTrader». Fail-safe: qualunque problema → `NAME_ONLY` (`recognition.DEFAULT_MODE`)."""
    try:
        return recognition.BOTH if dizionario.is_validated() else recognition.NAME_ONLY
    except Exception:   # noqa: BLE001 — qualunque errore nel gate → NAME_ONLY (fail-closed, A10)
        # Esplicito `NAME_ONLY`, NON `recognition.DEFAULT_MODE` (review Fugu Ultra su #375): il
        # fail-safe non deve dipendere dal valore di DEFAULT_MODE — se un domani divergesse verso
        # `BOTH`, l'`except` fail-aprirebbe l'invariante A10. Entrambi i rami tornano NAME_ONLY.
        return recognition.NAME_ONLY


def load_config(path: str = CONFIG_FILE) -> dict:
    """Ritorna i default, sovrascritti dal file se presente e leggibile.

    Se il file esiste ma è corrotto (JSON non valido), ne fa un backup `.bak`
    e riparte dai default, così una config rotta non blocca l'avvio. Prima di
    restituire, `_migrate` coerce i tipi noti (audit C5) così un file vecchio/editato
    a mano non immette tipi sbagliati nei consumer.

    #311-2.3 (gate su #311-2.2): SOLO per una config **NUOVA** (nessun file preesistente) il
    `recognition_mode` di default è deciso da `_default_recognition_mode()` (BOTH se il dizionario
    è validato, altrimenti NAME_ONLY). Config esistenti (valore esplicito o legacy) e config
    corrotte NON vengono toccate: restano sul loro valore / sul `NAME_ONLY` dei DEFAULTS (fail-safe).
    """
    # deepcopy: i default contengono valori mutabili nested (es. parser_by_chat
    # {}); una copia shallow li condividerebbe con DEFAULTS e una mutazione
    # in-place della config restituita corromperebbe i load successivi.
    cfg = copy.deepcopy(DEFAULTS)
    corrupted = False
    file_existed = os.path.exists(path)
    # Config NUOVA (nessun file): applica il gate #311-2.3 al default recognition_mode. Prima del
    # blocco di load così un file esistente (che sotto fa `cfg.update(data)`) sovrascrive comunque
    # col proprio valore esplicito; una config corrotta (file_existed=True) resta sul NAME_ONLY dei
    # DEFAULTS (conservativo). `_migrate` più sotto ri-valida comunque il valore.
    if not file_existed:
        cfg["recognition_mode"] = _default_recognition_mode()
    if file_existed:
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            if isinstance(data, dict):
                cfg.update(data)
            else:
                _backup_corrupted(path)
                corrupted = True
        except (json.JSONDecodeError, ValueError, OSError):
            _backup_corrupted(path)
            corrupted = True
    # `config_version` è già garantito dai DEFAULTS; se il file ne porta uno
    # (futuro schema v2+) viene preservato così non perdiamo lo skew su disco.
    cfg = _migrate(cfg)
    # Token storage sicuro (audit #105 P1): reidrata il token dal keyring SOLO se il
    # sentinel di stato dice "keyring" e la chiave su disco è vuota (CodeRabbit). Gating
    # sul sentinel (non sul solo `bot_token == ""`): così un token CANCELLATO — sentinel
    # "none", anche se un vecchio valore è rimasto orfano nel keyring per un delete fallito
    # — NON viene fatto risorgere. Una config vecchia con token in chiaro (nessun sentinel
    # o "plaintext") si usa com'è e viene migrata nel keyring al prossimo salvataggio.
    if cfg.get("bot_token_storage") == "keyring" and not str(cfg.get("bot_token") or ""):
        # `load_token_status` distingue "assente" da "lettura fallita" (#140, introdotto in PR-08a):
        # - read_ok + valore → reidrata il token;
        # - read_ok + None → keyring leggibile e GENUINAMENTE vuoto: il campo vuoto è uno stato reale
        #   (eventuale clear al save successivo è legittimo) → nessun marker;
        # - NON read_ok → keyring ILLEGGIBILE ora (outage): il token reale potrebbe esistere ancora ma
        #   non l'abbiamo letto. Si marca `_token_load_incomplete` così `save_config` NON tratta il
        #   `bot_token` vuoto come un clear voluto (evita di cancellare un token mai rimosso dall'utente).
        stored, read_ok = token_store.load_token_status()
        if read_ok:
            if stored:
                cfg["bot_token"] = stored
        else:
            cfg[TOKEN_LOAD_INCOMPLETE_KEY] = True
    # Marker SOLO-IN-RAM (issue #199): se il file era corrotto, il sentinel del token è andato
    # perso col `.bak` e un eventuale `bot_token=""` al save successivo NON è un clear voluto.
    # `save_config` lo legge e, nel ramo CLEAR, preserva il token keyring invece di cancellarlo.
    if corrupted:
        cfg[POST_CORRUPTION_KEY] = True
    # Lingua CSV (#342): allinea il writer alla config APPENA caricata, così le scritture
    # successive usano il separatore decimale giusto fin dallo startup (nessun altro wiring).
    csv_writer.set_csv_language(cfg.get("csv_language"))
    return cfg


def save_config(cfg: dict, path: str = CONFIG_FILE):
    """Salva la configurazione su file (best-effort) in modo **atomico**.

    Ritorna un ``SaveResult``, RETRO-COMPATIBILE con la vecchia tupla ``(config_salvata, ok)``
    (l'unpacking ``saved, ok = save_config(...)`` resta invariato), che porta anche ``.status``:
    - ``config_salvata`` (elem. 0): la config tenuta IN MEMORIA (con `config_version` e, se
      presente, il `bot_token` per il runtime), sempre restituita così il chiamante può tenerla
      anche se il disco fallisce;
    - ``ok`` (elem. 1): ``True`` se la scrittura su disco è riuscita, ``False`` altrimenti — così
      la GUI non può più segnalare "salvato" quando in realtà non lo è (finding A1);
    - ``status``: esito GRANULARE (#255 line-647) che DISAMBIGUA un ``ok=False`` prima accorpato:
      ``SAVE_OK`` (scritto), ``SAVE_DISK_ERROR`` (scrittura atomica fallita: permessi/spazio),
      ``SAVE_TOKEN_DEFERRED`` (token non persistibile per un outage del keyring: niente scritto,
      riprova), ``SAVE_CONFIG_CORRUPT`` (save parziale abortito: config su disco corrotto). La GUI
      usa ``save_status_message(status)`` per il messaggio giusto (disco vs keyring vs corrotto).

    **Token storage sicuro (audit #105 P1).** Il `bot_token` è una credenziale: se è
    presente e c'è un keyring di sistema, viene salvato lì (`token_store`) e su disco la
    chiave resta **vuota** (niente segreto in chiaro nel `config.json`). Se il keyring non
    è disponibile si RIPIEGA sul comportamento storico (token in chiaro) con un avviso. La
    config restituita in memoria mantiene comunque il token, così il runtime (`_start`) non
    cambia. La chiave `bot_token` **assente** (save parziale) NON tocca il keyring; solo la
    chiave **presente e vuota** è un clear esplicito. Il keyring viene aggiornato **prima**
    della scrittura su disco e, se il disco fallisce, si esegue il **rollback** del keyring:
    così un crash a metà non perde il token e un disco fallito non lascia keyring e disco
    incoerenti. Un sentinel `bot_token_storage` (`keyring`/`plaintext`/`none`) registra dove
    sta il token e `load_config` reidrata solo se vale "keyring".

    **Post-corruzione (issue #199).** Se `load_config` ha appena messo da parte un config
    corrotto (`.bak`), il `bot_token` torna vuoto e il sentinel è perso: un `bot_token=""` al
    save successivo è il RESIDUO della corruzione, non un clear voluto. `load_config` marca
    questo stato in RAM (`POST_CORRUPTION_KEY`, mai su disco); in quel caso il ramo CLEAR
    **PRESERVA** il token nel keyring (sentinel "keyring", reidratazione) invece di cancellarlo,
    così una corruzione recuperabile non distrugge la credenziale. Il marker è consumato dal
    primo save: un clear DELIBERATO a config integro cancella di nuovo, come prima.

    Scrittura **atomica** (tempfile nella stessa cartella + `flush`+`fsync` + `os.replace`,
    lo stesso schema di `csv_writer`/`signal_dedupe`/`profile_store`): un'interruzione a
    metà (crash, blackout, disco pieno) NON lascia un `config.json` troncato — o resta
    quello vecchio intatto, o c'è quello nuovo completo. Niente più reset ai default per
    una scrittura interrotta."""
    # deepcopy (audit C7): la `dict(cfg)` shallow condivideva i nested mutabili (liste/dict
    # come source_chats, parser_by_chat) tra lo snapshot restituito e il `cfg` del chiamante.
    # Il chiamante fa `self._config = saved`: con l'aliasing una mutazione successiva di uno
    # avrebbe alterato silenziosamente l'altro. Con la copia profonda i due sono indipendenti.
    in_memory = copy.deepcopy(cfg)
    in_memory.setdefault("config_version", CONFIG_VERSION)
    # Lingua CSV (#342): il chiamante tiene SEMPRE `in_memory` come config viva
    # (`self._config = saved`, anche su disco fallito) → il writer si allinea QUI, così un
    # salvataggio/caricamento profilo che cambia `csv_language` ha effetto senza riavvio.
    # SOLO se la chiave è presente in QUESTO save (Fable #344): un save PARZIALE senza
    # `csv_language` non dice nulla sulla lingua — sincronizzare `None` la resetterebbe
    # silenziosamente al default IT, corrompendo i decimali per un utente EN.
    # Il valore CANONICO ritornato viene riscritto in `in_memory` PRIMA della copia
    # `to_save` (CodeRabbit #344): così config su disco, `saved` restituita al chiamante e
    # lingua attiva del writer non divergono mai (niente "francese" persistito verbatim
    # mentre il writer usa IT).
    if "csv_language" in in_memory:
        in_memory["csv_language"] = csv_writer.set_csv_language(in_memory.get("csv_language"))
    # Copia separata per il DISCO: il token sicuro non va scritto in chiaro (vedi sotto),
    # ma `in_memory` lo conserva per il runtime.
    to_save = copy.deepcopy(in_memory)
    # Marker post-corruzione (issue #199): SOLO-IN-RAM. Lo si CONSUMA qui — rimosso sia da
    # `to_save` (mai su disco) sia da `in_memory` (così un save riuscito risana lo stato e i
    # save successivi tornano normali). Il suo valore guida solo il ramo CLEAR qui sotto.
    post_corruption = bool(in_memory.pop(POST_CORRUPTION_KEY, False))
    to_save.pop(POST_CORRUPTION_KEY, None)
    # `bot_token` PRESENTE in questo save? Determina la modalità (set/clear/partial) e va valutato
    # PRIMA di consumare il marker load-incompleto.
    token_present = "bot_token" in in_memory
    # Marker load-incompleto (#140): SOLO-IN-RAM. Segnala che un `bot_token` vuoto deriva da un load
    # in cui il keyring era illeggibile (token NON reidratato), non da un clear voluto → il ramo CLEAR
    # PRESERVA invece di cancellare. Lo si CONSUMA (pop da `in_memory`) solo quando questo save
    # include la chiave `bot_token` (set/clear): allora il ramo del token gira e decide l'esito. Su un
    # save PARZIALE (chiave assente) il ramo del token NON gira: NON si deve rimuovere il marker da
    # `in_memory` (lascerebbe il bridge senza la guardia per un clear successivo, CodeRabbit) — lo si
    # LEGGE soltanto. In ogni caso il marker è sempre tolto da `to_save` (mai su disco né nei profili).
    if token_present:
        load_incomplete = bool(in_memory.pop(TOKEN_LOAD_INCOMPLETE_KEY, False))
    else:
        load_incomplete = bool(in_memory.get(TOKEN_LOAD_INCOMPLETE_KEY, False))
    to_save.pop(TOKEN_LOAD_INCOMPLETE_KEY, None)

    # ── Routing del bot_token (audit #105 P1) ─────────────────────────────────────
    # Casi DISTINTI: chiave `bot_token` ASSENTE = save PARZIALE → keyring e sentinel NON
    # toccati (un save senza la chiave non cancella la credenziale migrata). Presente non
    # vuota = set; presente e vuota = clear.
    #
    # Ordine KEYRING-FIRST con ROLLBACK (Codex P2): il keyring viene aggiornato PRIMA della
    # scrittura su disco e, se il disco fallisce, si ripristina lo stato precedente. Così
    # (a) un crash tra disco e keyring non perde il token — il keyring ha già il valore
    # quando il disco dice "keyring"; (b) un disco fallito non lascia keyring e disco
    # incoerenti. Il sentinel `bot_token_storage` (`keyring`/`plaintext`/`none`, CodeRabbit)
    # disambigua `bot_token == ""`: `load_config` reidrata SOLO se vale "keyring".
    token = str(in_memory.get("bot_token") or "")
    prior_sentinel = str(in_memory.get("bot_token_storage") or "")
    keyring_changed = False        # se True, su disco-fallito va eseguito il rollback
    prior_keyring = None           # valore del keyring PRIMA della modifica (per il rollback)
    token_not_persisted = False    # il token NON è stato persistito → il save va segnalato non riuscito
    # Dopo una corruzione recuperata (#199) il sentinel `bot_token_storage` è andato perso col file:
    # lo stato di storage precedente è SCONOSCIUTO. NON va trattato come "prima installazione
    # plaintext" (Codex P2) — il keyring potrebbe ancora contenere la credenziale migrata e un
    # fallback in chiaro la declasserebbe silenziosamente. Si protegge come il caso "keyring":
    # ogni ramo che altrimenti scriverebbe il token in chiaro lo DIFFERISCE invece (mai plaintext).
    prior_is_keyring_or_unknown = prior_sentinel == "keyring" or post_corruption
    if token_present:
        if token:
            if token_store.available():
                # Snapshot del valore PRECEDENTE distinguendo "assente" da "lettura fallita"
                # (#140): serve come base per il rollback su disco-fallito.
                prior_keyring, prior_read_ok = token_store.load_token_status()
                if not prior_read_ok and prior_is_keyring_or_unknown:
                    # Pre-lettura del keyring FALLITA con stato precedente "keyring" — o SCONOSCIUTO
                    # post-corruzione (Codex): non conosciamo il valore migrato, quindi un rollback
                    # dopo disco-fallito non potrebbe ripristinarlo. NON sovrascrivere il keyring (un
                    # save fallito cambierebbe la credenziale in modo IRREVERSIBILE) e NON esporre il
                    # segreto in chiaro. Si DIFFERISCE l'aggiornamento del token e il save è segnalato
                    # come NON riuscito (il token eventualmente già nel keyring resta valido).
                    to_save["bot_token"] = ""
                    to_save["bot_token_storage"] = "keyring"
                    token_not_persisted = True
                    logger.warning("Keyring illeggibile ora: il bot token NON è stato aggiornato "
                                   "(impossibile garantire un rollback sicuro né esporlo in chiaro). "
                                   "Riprova quando il keyring è stabile; il token memorizzato resta valido.")
                elif not prior_read_ok:
                    # Pre-lettura fallita, stato precedente NON "keyring" e NESSUNA corruzione recente
                    # (install in chiaro / prima installazione): NON c'è credenziale migrata nel keyring
                    # da proteggere e il token è già/sta per essere su disco in chiaro. Azzerarlo
                    # CANCELLEREBBE il token in chiaro esistente — una GUI re-invia SEMPRE il campo
                    # token, quindi anche cambiare un'altra impostazione durante questo guasto
                    # transitorio perderebbe la credenziale (Codex P2). Si PRESERVA il comportamento
                    # storico: token in chiaro su disco, sentinel "plaintext", save RIUSCITO.
                    to_save["bot_token_storage"] = "plaintext"
                    logger.warning("Keyring illeggibile ora: il bot token resta in chiaro in %s "
                                   "(nessuna credenziale migrata da proteggere).", path)
                elif token_store.save_token(token):
                    keyring_changed = True
                    to_save["bot_token"] = ""                # niente segreto in chiaro su disco
                    to_save["bot_token_storage"] = "keyring"
                elif prior_is_keyring_or_unknown:
                    # `available()` True ma set fallito (raro) e lo stato precedente era "keyring" —
                    # o SCONOSCIUTO post-corruzione (#140 Codex): NON declassare a plaintext —
                    # scriverebbe in chiaro un segreto forse prima protetto. Preserva "keyring" (il
                    # valore eventualmente già memorizzato resta valido), NON aggiornare il token ora
                    # e segnala il save come NON riuscito: il token NUOVO non è persistito, la GUI non
                    # deve mostrare "salvato".
                    to_save["bot_token"] = ""
                    to_save["bot_token_storage"] = "keyring"
                    token_not_persisted = True
                    logger.warning("Keyring non scrivibile ora: il bot token NON è stato aggiornato "
                                   "(per non esporlo in chiaro nel config). Riprova; il token già "
                                   "memorizzato nel keyring resta valido.")
                else:
                    # `available()` True ma set fallito e nessuno stato keyring precedente:
                    # fallback storico al token in chiaro (prima installazione/plaintext).
                    to_save["bot_token_storage"] = "plaintext"
                    logger.warning("Keyring non scrivibile: il bot token resta in chiaro in %s.", path)
            elif prior_is_keyring_or_unknown:
                # Stato precedente "keyring" — o SCONOSCIUTO post-corruzione — + keyring NON
                # disponibile ORA = outage TRANSIENTE (Codex P2). NON declassare a plaintext:
                # esporrebbe il segreto su disco per un guasto temporaneo e declasserebbe una
                # credenziale forse keyring-backed. Preserva "keyring" e NON scrivere il token in
                # chiaro; il keyring conserva (eventualmente ancora) il valore. Un token NUOVO non
                # può essere salvato ora → save DIFFERITO e segnalato NON riuscito (Codex/CodeRabbit):
                # la GUI non deve mostrare "salvato" mentre il token nuovo è fuori sia da keyring sia da disco.
                to_save["bot_token"] = ""
                to_save["bot_token_storage"] = "keyring"
                token_not_persisted = True
                logger.warning("Keyring non disponibile: il bot token NON è stato aggiornato ora "
                               "(per non esporlo in chiaro nel config). Riprova quando il keyring "
                               "è disponibile; il token già memorizzato resta valido.")
            else:
                # Nessun backend keyring e nessuno stato keyring precedente (prima installazione):
                # token in chiaro su disco (comportamento storico).
                to_save["bot_token_storage"] = "plaintext"
                logger.warning("Keyring non disponibile: il bot token resta in chiaro in %s. "
                               "Installa un backend keyring per cifrarlo.", path)
        elif post_corruption or load_incomplete:
            # `bot_token` vuoto che NON è un clear voluto, ma il RESIDUO di un load in cui il token
            # reale non è stato reidratato. Due cause:
            # - #199 POST-CORRUZIONE: config illeggibile → backup `.bak` → default con `bot_token=""`
            #   e sentinel perso;
            # - #140 LOAD-INCOMPLETO: sentinel "keyring" ma keyring ILLEGGIBILE al load (outage), quindi
            #   `bot_token` è rimasto "" pur potendo esistere ancora una credenziale nel keyring.
            # In questi casi cancellare ora il token keyring distruggerebbe una credenziale VALIDA che
            # l'utente non ha mai chiesto di rimuovere. Si decide il sentinel PER-CASO sulla lettura del
            # keyring (CodeRabbit): "keyring" SOLO quando c'è davvero un token da reidratare o il keyring
            # è ancora illeggibile; se il keyring è leggibile e GENUINAMENTE vuoto si usa "none" per non
            # lasciare un puntatore di reidratazione STANTIO (che resusciterebbe un eventuale orphan).
            stored, read_ok = token_store.load_token_status()
            to_save["bot_token"] = ""
            if stored:
                # Keyring leggibile e col valore → reidrata: il load è ora completo, il token reale
                # torna in `in_memory` per il runtime e i save successivi sono normali. Si PRESERVA;
                # il marker è consumato (load risolto).
                in_memory["bot_token"] = stored
                to_save["bot_token_storage"] = "keyring"
                logger.warning("Bot token NON cancellato: il campo vuoto deriva da un load incompleto "
                               "(config corrotto o keyring illeggibile al caricamento), non da un clear "
                               "voluto. Il token nel keyring è stato PRESERVATO; per cancellarlo davvero "
                               "rifallo a stato integro col keyring leggibile.")
            elif not read_ok:
                # Keyring ANCORA illeggibile → il load resta incompleto: RE-MANTIENI il marker (RAM)
                # così la protezione "non cancellare" sopravvive al prossimo save (stesso principio del
                # mirror sentinel di PR-08a). Senza, un secondo save col keyring tornato leggibile
                # tratterebbe il campo vuoto come clear reale e cancellerebbe il token.
                in_memory[TOKEN_LOAD_INCOMPLETE_KEY] = True
                to_save["bot_token_storage"] = "keyring"
                logger.warning("Bot token NON cancellato: keyring ancora illeggibile, impossibile "
                               "confermare un clear. Protezione mantenuta; riprova quando il keyring "
                               "è leggibile (il token memorizzato resta valido).")
            else:
                # read_ok + None → keyring leggibile e GENUINAMENTE vuoto: NON c'è alcun token da
                # preservare. Sentinel "none" (niente reidratazione stantia che resusciterebbe un
                # orphan — coerente col gating sentinel di `load_config`); il marker resta consumato.
                to_save["bot_token_storage"] = "none"
        elif token_store.available():
            # Clear (chiave presente e vuota) col keyring LEGGIBILE e load COMPLETO (nessun marker
            # post-corruzione/load-incompleto): l'ambiguità "clear vs miss transiente" non c'è (se ci
            # fosse un token, al load sarebbe stato reidratato e il campo non sarebbe vuoto). Clear REALE.
            stored = token_store.load_token()
            if stored is not None:
                # C'è ancora un token nel keyring → rimuovilo.
                prior_keyring = stored
                if token_store.delete_token():
                    keyring_changed = True
                elif not keyring_changed:
                    # delete fallito: il sentinel "none" evita che il bridge riusi il token
                    # (niente clear "finto"), ma il segreto resta orfano nel keyring → avviso.
                    logger.warning("Impossibile rimuovere il bot token dal keyring: potrebbe "
                                   "restare memorizzato. Rimuovilo dal Credential Manager di sistema.")
            to_save["bot_token_storage"] = "none"   # keyring leggibile e svuotato → niente reidratazione
        else:
            # Clear/empty col keyring NON leggibile: non posso né confermare un clear né
            # cancellare. Se lo stato era "keyring", PRESERVALO (un token ancora memorizzato
            # non va perso quando il keyring torna — MISS TRANSIENTE), ma se l'utente INTENDEVA
            # cancellarlo non è stato possibile ora → avviso (Codex P2). Altrimenti è "none".
            if prior_sentinel == "keyring":
                to_save["bot_token_storage"] = "keyring"
                logger.warning("Keyring non disponibile: impossibile rimuovere/azzerare il bot "
                               "token ora. Se volevi cancellarlo, riprova quando il keyring è "
                               "disponibile; il token memorizzato resta valido nel frattempo.")
            else:
                to_save["bot_token_storage"] = "none"
    else:
        # Chiave `bot_token` ASSENTE → save PARZIALE: preserva i campi del token già su disco
        # (token + sentinel), così un save di sole altre impostazioni non perde il puntatore al
        # keyring e `load_config` continua a reidratare (Codex P2). Il keyring non viene toccato.
        reread_failed = False
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
            except (OSError, json.JSONDecodeError, ValueError):
                existing = None
            if isinstance(existing, dict):
                for _k in ("bot_token", "bot_token_storage"):
                    if _k in existing and _k not in to_save:
                        to_save[_k] = existing[_k]
            else:
                # File presente ma ILLEGGIBILE (JSON corrotto / non-dict / IO error): non si
                # possono recuperare i campi del token dal disco.
                reread_failed = True
        # M3 (#184): se il re-read di un FILE config esistente fallisce (JSON corrotto / non-dict
        # / IO error) e il puntatore NON è già in memoria (`bot_token_storage` in `to_save`, es.
        # da `self._config`), si FAIL-CLOSED: NON si scrive. Scrivere ora cancellerebbe il sentinel
        # → token ORFANO nel keyring. Ma NON si tenta nemmeno di "recuperare" il sentinel dal
        # keyring: il keyring da solo è AMBIGUO — un valore rimasto dopo un clear con `delete`
        # fallito (sentinel `none`, ora perso col file corrotto) verrebbe RESUSCITATO come token
        # attivo (Codex P1). Si re-linka SOLO se c'è evidenza IN MEMORIA (sentinel in `to_save`,
        # ramo sopra); altrimenti si aborta lasciando intatti disco (corrotto) e keyring, così
        # `load_config` può fare il backup `.bak` e l'utente reinserisce il token in sicurezza.
        # Il gate `isfile` evita di abortire quando il path NON è un file (es. una directory):
        # in quel caso si lascia proseguire e fallire la `os.replace` con l'errore atteso.
        if reread_failed and os.path.isfile(path) and "bot_token_storage" not in to_save:
            logger.warning("Salvataggio parziale annullato: config su disco corrotto/illeggibile e "
                           "puntatore keyring del bot token non disponibile in memoria. Non si "
                           "sovrascrive (eviterebbe di orfanare il token, ma il keyring da solo è "
                           "ambiguo e potrebbe resuscitare un token già cancellato). Ripristina o "
                           "rimuovi il config.json corrotto e riprova.")
            return SaveResult(in_memory, False, SAVE_CONFIG_CORRUPT)
    if token_not_persisted:
        # Il token NUOVO non è persistibile ora (keyring illeggibile / non scrivibile / non
        # disponibile, #140). Per evitare un esito AMBIGUO non si scrive NULLA su disco: i
        # chiamanti interpretano `ok=False` come "niente persistito su disco — impostazioni
        # attive solo in memoria" (es. `_save_config` lo logga così). Se scrivessimo le altre
        # impostazioni e poi tornassimo `ok=False`, un cambio non-token (es. `dry_run`, auto-sync)
        # sopravvivrebbe al riavvio pur essendo segnalato come fallito (Codex P2). Si aborta in
        # modo ATOMICO: la config su disco resta intatta (sentinel "keyring" già presente → il
        # vecchio token reidrata), le modifiche restano in memoria, l'utente riprova quando il
        # keyring è stabile. Nessun `keyring_changed` qui (i rami differiti non toccano il keyring),
        # quindi non serve rollback.
        #
        # MIRROR del sentinel fail-safe in memoria (Codex/CodeRabbit, post-#199): i rami differiti
        # hanno impostato `to_save["bot_token_storage"] = "keyring"`. Il chiamante fa
        # `self._config = saved` ANCHE su `ok=False`, e `POST_CORRUPTION_KEY` è già stato consumato:
        # senza questo mirror un secondo save (keyring ancora giù) vedrebbe `prior_sentinel` vuoto e
        # ripiegherebbe sul plaintext, scrivendo il token in chiaro. Riportando "keyring" in
        # `in_memory`, la protezione "stato keyring/ignoto → differisci" sopravvive ai retry.
        if "bot_token_storage" in to_save:
            in_memory["bot_token_storage"] = to_save["bot_token_storage"]
        logger.warning("Salvataggio NON eseguito su disco: il bot token non è persistibile ora "
                       "(keyring non disponibile/illeggibile). Per non dare un esito ambiguo non è "
                       "stata scritta alcuna impostazione; riprova quando il keyring è stabile.")
        return SaveResult(in_memory, False, SAVE_TOKEN_DEFERRED)
    try:
        _ensure_dir(path)
        # Scrittura atomica condivisa (tmp + flush/fsync + os.replace, cleanup su errore).
        atomic_io.atomic_write_json(path, to_save, prefix=TMP_PREFIX, suffix=TMP_SUFFIX,
                                    indent=2)
    except OSError as exc:
        # Disco fallito → ROLLBACK del keyring allo stato precedente, così keyring e disco
        # restano coerenti (la credenziale non cambia se la config non è stata salvata).
        if keyring_changed:
            # `keyring_changed` è True dopo una modifica keyring RIUSCITA: `save_token` nel ramo SET
            # (token nuovo) oppure `delete_token` nel ramo CLEAR (rimozione). In entrambi i casi
            # `prior_keyring` è lo stato da ripristinare — il valore precedente nel SET, o il token
            # appena cancellato nel CLEAR. Entrambi i rami partono da una lettura keyring RIUSCITA
            # (SET: `prior_read_ok`; CLEAR: `load_token()` non-None), quindi `prior_keyring` è o un
            # valore reale (da ri-salvare) o `None` perché GENUINAMENTE assente (da cancellare): mai
            # il caso ambiguo "cancella su lettura fallita" (quel ramo non tocca il keyring).
            try:
                if prior_keyring is not None:
                    rolled_back = token_store.save_token(prior_keyring)
                else:
                    rolled_back = token_store.delete_token()
            except Exception:   # noqa: BLE001 — rollback best-effort
                rolled_back = False
            # `save_token`/`delete_token` segnalano il fallimento con False (non sollevano):
            # se il rollback non riesce (keyring diventato indisponibile), keyring e disco
            # restano incoerenti → log ESPLICITO perché richiede attenzione (Codex P2).
            if not rolled_back:
                logger.error("Rollback del keyring NON riuscito dopo config non salvata (%s): la "
                             "credenziale potrebbe essere incoerente col config su disco. "
                             "Verifica il Credential Manager di sistema.", path)
        logger.error("Salvataggio config fallito (%s): %s", path, exc, exc_info=True)
        return SaveResult(in_memory, False, SAVE_DISK_ERROR)
    # Disco OK → mantieni la config IN MEMORIA coerente col disco per il sentinel (Codex P2):
    # il chiamante fa `self._config = saved`, quindi un sentinel stantio rifarebbe scrivere uno
    # stato sbagliato al save successivo. `bot_token` in memoria resta il token reale (runtime).
    if "bot_token_storage" in to_save:
        in_memory["bot_token_storage"] = to_save["bot_token_storage"]
    if token and to_save.get("bot_token_storage") == "keyring":
        logger.info("Bot token salvato nel keyring di sistema (non in chiaro nel config).")
    # Si arriva qui solo con `token_not_persisted` False: i rami che NON persistono il token
    # (keyring illeggibile/non scrivibile/non disponibile, #140) abortiscono PRIMA della scrittura
    # su disco e tornano `False` (vedi sopra), così `ok=False` significa uniformemente "niente su
    # disco". Disco scritto con successo → `ok=True`.
    return SaveResult(in_memory, True, SAVE_OK)


def _backup_corrupted(path: str) -> None:
    """Sposta una config illeggibile in `<path>.bak` (best-effort).

    Usa os.replace così sovrascrive un eventuale `.bak` preesistente in modo
    atomico e cross-platform (su Windows shutil.move fallirebbe se esiste già).
    """
    try:
        os.replace(path, path + ".bak")
    except OSError as exc:
        # Best-effort, ma non più SILENZIOSO (audit #105 P2): se anche il backup fallisce
        # (permessi/lock), config corrotta + backup mancato = perdita di evidenza e l'utente
        # non capisce perché è tornato ai default. Si logga path + tipo errore (nessun
        # contenuto della config → niente leak). exc_info per il traceback in diagnosi.
        logger.warning("Backup della config corrotta fallito (%s → %s): %s",
                       path, path + ".bak", exc, exc_info=True)
