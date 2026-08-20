"""Event journal append-only (issue #110 voce 20 / G2): ledger transazionale degli
eventi safety-critical del bridge.

Serve a rispondere a «cosa aveva fatto?» dopo un crash/riavvio in modo affidabile:
ogni passo rilevante (START/STOP, segnale ricevuto/parsato/validato, CSV scritto/
svuotato, conferma/rifiuto XTrader, riconnessione, recovery del CSV all'avvio) può
essere registrato come un EVENTO con id univoco e timestamp. A differenza del log
testuale (`event_log`, pensato per l'utente), questo è un ledger **strutturato** e
**append-only**, pensato per ricostruzione/forense e per future integrazioni.

Proprietà:
- **Append-only JSONL**: una riga = un evento JSON (`{id, ts, type, cat, data}`, piu'
  i campi opzionali del Recorder `sess/seq/mono/lvl/corr/root`); l'ordine
  d'inserimento è preservato e lo storico sopravvive a chiusura/riavvio.
- **Atomicità della singola riga**: `write` + `flush` + `os.fsync` per ogni evento.
- **Fail-safe in lettura**: una riga finale TRONCATA da un crash a metà append non
  rompe il replay — `read_events` salta le righe malformate.
- **Redazione**: nessun token Telegram in chiaro (riusa `event_log.redact_secrets`),
  applicata sia ricorsivamente ai valori sia alla riga serializzata (difesa-in-profondità).
- **Fail-closed sul tipo**: un `event_type` non in `EVENT_TYPES` solleva `ValueError`
  (un refuso non finisce silenziosamente nel ledger).
- **Vocabolario a categorie** (`EVENT_CATEGORIES`): il catalogo e' raggruppato per
  dominio (SESSION/CONFIG/UI/TELEGRAM/SIGNAL/BETFAIR/RISK/ORDER/SYSTEM + LEGACY), cosi'
  si puo' filtrare per categoria senza conoscere i nomi dei singoli tipi.
- **Modulo puro**: nessuna dipendenza da GUI/Telegram/CSV runtime → testabile headless.

NB: l'AGGANCIO al runtime (chiamare `append_event` da `app._process`/`_process_confirmation`/
`_run_bot`/`_clear_stale_csv`/`_expire_tick`) è in `app.py` (#230), best-effort e mai
bloccante; questo modulo resta puro e testabile headless.
"""

import json
import os
import time
import uuid

from . import atomic_io, event_log, validators

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


def _redact(value):
    """Redazione RICORSIVA dei token nei valori stringa (dict/list inclusi), così un
    token finito per errore nel payload non viene mai scritto in chiaro."""
    if isinstance(value, str):
        return event_log.redact_secrets(value)
    if isinstance(value, dict):
        # Redatte anche le CHIAVI stringa: un token usato come chiave non deve restare
        # in chiaro né nell'evento ritornato né nella riga persistita (review Codex).
        return {(event_log.redact_secrets(k) if isinstance(k, str) else k): _redact(v)
                for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in value]
    return value


def make_event(event_type, data=None, *, now=None, event_id=None,
               sess=None, seq=None, mono=None, lvl=None, corr=None, root=None) -> dict:
    """Costruisce (senza scrivere) un evento normalizzato.

    Forma minima (invariata dal 2023): `{id, ts, type, cat, data}`. I campi del
    Recorder (fase A) sono **opzionali** e compaiono solo se passati, cosi' un
    chiamante storico produce una riga identica a prima piu' il solo `cat`, che e'
    derivato dal tipo e non richiede nulla a nessuno:

    - `sess` — id della sessione, per separare due avvii nello stesso file;
    - `seq`  — progressivo di sessione. **Un buco nella numerazione dice che si e'
      perso un evento**, cosa altrimenti invisibile: senza, un ledger amputato da un
      disco pieno sembra semplicemente un ledger piu' corto;
    - `mono` — orologio MONOTONO. `ts` e' wall-clock e puo' saltare all'indietro (NTP,
      sospensione del portatile): calcolare latenze su `ts` produce durate negative o
      gonfiate senza che nulla lo segnali. `mono` e' l'unico affidabile per le durate,
      `ts` resta per collocare l'evento nel tempo reale;
    - `lvl`  — livello (`LEVELS`), normalizzato: un livello ignoto degrada a INFO e
      l'evento si scrive comunque (vedi `normalize_level`);
    - `corr` — id dell'evento che ha CAUSATO questo;
    - `root` — id del capostipite della catena (tipicamente il messaggio Telegram):
      filtrando per `root` si ottiene la storia intera di una giocata, dal messaggio
      al P&L, e da un ordine strano si risale in un passo a cosa l'ha prodotto.

    Regole invariate: `event_type` deve essere in `EVENT_TYPES`, altrimenti
    `ValueError` (fail-closed); `now` e `mono` sono validati finiti
    (`validators.require_finite_now`) — un timestamp NaN/inf non entra nel ledger;
    `event_id` e' iniettabile per i test; `data` e' copiato e **redatto**.

    `seq=0` e' un valore legittimo (primo evento della sessione): la presenza si
    verifica con `is not None`, non con la verita' del valore."""
    if event_type not in EVENT_TYPES:
        raise ValueError(f"event type sconosciuto: {event_type!r}")
    ts = time.time() if now is None else validators.require_finite_now(now)
    eid = uuid.uuid4().hex if event_id is None else str(event_id)
    payload = _redact(dict(data or {}))
    # Ordine di inserimento = ordine sulla riga JSON: prima l'identita' dell'evento,
    # poi i metadati, `data` per ultimo (puo' essere lungo e finirebbe per nascondere
    # il resto quando si legge il .jsonl a occhio).
    event = {"id": eid, "ts": float(ts), "type": event_type,
             "cat": category_of(event_type)}
    if sess is not None:
        event["sess"] = str(sess)
    if seq is not None:
        event["seq"] = int(seq)
    if mono is not None:
        event["mono"] = validators.require_finite_now(mono)
    if lvl is not None:
        event["lvl"] = normalize_level(lvl)
    if corr is not None:
        event["corr"] = str(corr)
    if root is not None:
        event["root"] = str(root)
    event["data"] = payload
    return event


def _ends_without_newline(path: str) -> bool:
    """`True` se il file esiste, è non vuoto e NON termina con `\\n` (cioè l'ultima
    riga è troncata, es. da un crash a metà append)."""
    try:
        if os.path.getsize(path) == 0:
            return False
        with open(path, "rb") as f:
            f.seek(-1, os.SEEK_END)
            return f.read(1) != b"\n"
    except OSError:
        return False


def _append_line(path: str, line: str) -> None:
    """Appende UNA riga al file (creando la cartella se serve) con `flush`+`fsync`.

    Se l'ultima riga esistente è TRONCATA (nessun `\\n` finale, es. crash a metà
    append), antepone un `\\n` separatore: così la riga troncata resta isolata sulla
    sua riga (verrà saltata da `read_events`) e il NUOVO evento finisce su una riga
    pulita — senza questo, l'append si concatenerebbe alla riga parziale e anche il
    nuovo evento andrebbe perso (review Codex P1).

    Separatore + riga + `\\n` vengono scritti in UN SOLO `f.write` (issue #184 M6): due
    `write` separati prima del `flush`/`fsync` potevano lasciare, su un crash a metà, solo il
    separatore senza l'evento (evento perso). La singola write elimina QUESTA finestra a
    livello di PROCESSO — o tutta la riga o niente, mai "separatore sì, evento no". NON è
    atomicità a livello di disco: un crash durante il trasferimento kernel→disco può comunque
    lasciare una riga finale parziale (dipende da filesystem/hardware), ma quella coda troncata
    è già gestita — `read_events` la salta e il prossimo append vi antepone un separatore
    (precisazione review Sourcery)."""
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    prefix = "\n" if _ends_without_newline(path) else ""
    with open(path, "a", encoding="utf-8") as f:
        f.write(prefix + line + "\n")        # M6: separatore+riga+newline in una sola write
        f.flush()
        os.fsync(f.fileno())


def append_event(path: str, event_type, data=None, *, now=None, event_id=None,
                 sess=None, seq=None, mono=None, lvl=None, corr=None, root=None) -> dict:
    """Costruisce l'evento (tipo validato, payload redatto) e lo APPENDE come una
    riga JSON al ledger `path`. Ritorna l'evento scritto.

    La serializzazione è su una sola riga (`json.dumps` con `\\n` escapato → niente
    righe spezzate da contenuti multilinea); la riga è ri-redatta come difesa finale
    (mai token in chiaro). Solleva `ValueError` su tipo/timestamp non validi; gli
    errori di I/O propagano (il chiamante runtime li gestirà best-effort, come per
    `event_log`)."""
    event = make_event(event_type, data, now=now, event_id=event_id,
                       sess=sess, seq=seq, mono=mono, lvl=lvl, corr=corr, root=root)
    line = event_log.redact_secrets(json.dumps(event, ensure_ascii=False))
    _append_line(path, line)
    return event


def read_events(path: str) -> list:
    """Legge il ledger come lista di eventi (dict), nell'ordine d'inserimento.

    Tollerante e fail-safe: file assente → `[]`; righe vuote ignorate; una riga
    **malformata** (es. l'ultima troncata da un crash a metà append) viene **saltata**
    senza crashare, così il resto dello storico resta leggibile.

    `errors="replace"` (review Codex): un crash a metà di un carattere NON-ASCII (le
    scritture usano `ensure_ascii=False`, quindi accenti & co. finiscono come byte UTF-8)
    lascerebbe una coda di byte UTF-8 INVALIDA; con la decodifica stretta `readlines()`
    solleverebbe `UnicodeDecodeError` PRIMA del filtro per-riga, facendo fallire il replay
    anche degli eventi validi precedenti. Con `replace` i byte rotti diventano `�` su QUELLA
    riga (che resta JSON malformato → saltata), mentre le righe valide si decodificano."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            raw_lines = f.readlines()
    except OSError:
        return []
    events = []
    for raw in raw_lines:
        text = raw.strip()
        if not text:
            continue
        try:
            obj = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            continue   # riga troncata/malformata: salta (append-only fail-safe)
        if isinstance(obj, dict):
            events.append(obj)
    return events


def clear(path: str) -> bool:
    """Svuota il ledger in modo ATOMICO (file vuoto), via `atomic_io.atomic_write_text`.
    Utile per manutenzione/retention senza lasciare un file a metà. `True` se riuscito,
    `False` su errore di I/O (best-effort, non solleva)."""
    try:
        atomic_io.atomic_write_text(path, "", prefix=".journal_", suffix=".tmp")
        return True
    except OSError:
        return False


def prune_events(path: str, keep: int) -> int:
    """Mantiene solo gli ULTIMI `keep` eventi del ledger, riscrivendolo in modo ATOMICO
    (tmp + `os.replace`). Retention: senza, il `.jsonl` crescerebbe all'infinito (#230).

    Best-effort, **non solleva mai**: ritorna quanti eventi ha rimosso (`0` se non c'era
    nulla da potare o su errore di I/O). `keep<=0` è un **no-op** (guardia: non svuota il
    ledger per errore — per svuotarlo c'è `clear`). Le righe tenute sono ri-redatte come in
    scrittura (mai token in chiaro)."""
    if not keep or keep <= 0:
        return 0
    events = read_events(path)
    if len(events) <= keep:
        return 0
    kept = events[len(events) - keep:]
    try:
        payload = "".join(
            event_log.redact_secrets(json.dumps(e, ensure_ascii=False)) + "\n" for e in kept)
        atomic_io.atomic_write_text(path, payload, prefix=".journal_", suffix=".tmp")
    except (OSError, ValueError):
        # Best-effort: oltre agli errori di I/O (OSError) cattura anche `UnicodeEncodeError`
        # (⊂ ValueError) — un evento storico con un carattere non codificabile (es. surrogato
        # spaiato letto da una riga corrotta) NON deve far esplodere la potatura allo startup
        # (Codex P2 #233). La retention è una pulizia: meglio saltarla che crashare il boot.
        return 0
    return len(events) - keep
