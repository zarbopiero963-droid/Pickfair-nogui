"""PR-12: gestione di più chat sorgente (multi-chat), logica pura e testabile.

Una **sorgente** è una chat/canale Telegram da cui il bridge accetta segnali, con
una sua identità:

    {"name": str, "chat_id": str, "enabled": bool, "provider": str, "mode": str}

- `mode` ∈ {PRE, LIVE}: pre-match o live. Determina il `Provider` di default
  (PRE → ``TG_PRE``, LIVE → ``TG_LIVE``) se non se ne imposta uno esplicito.
- `provider` esplicito (se valorizzato) ha la precedenza sul default della modalità.
- `enabled=False`: la sorgente è ignorata (non processata).

Questo modulo è **puro**: non tocca GUI, Telegram, CSV o config su disco. Espone
risoluzione (provider/sorgente per chat) e validazione (chat_id duplicato =
errore bloccante; nome duplicato = avviso). Il wiring nel listener/router live è
un passo successivo (come `parser_manager` CP-07 ha preceduto CP-09), così questa
parte resta interamente testabile headless e a rischio zero per il CSV.
"""

import logging
import math
import threading

_LOG = logging.getLogger(__name__)

MODES = ("PRE", "LIVE")
DEFAULT_MODE = "PRE"
# Provider di default per modalità: DERIVATO da MODES (PRE → "TG_PRE", LIVE →
# "TG_LIVE"), così aggiungere/cambiare una modalità non può desincronizzare la
# mappa (fonte unica = MODES).
_MODE_PROVIDER = {m: "TG_" + m for m in MODES}


def is_valid_mode(mode) -> bool:
    """True se `mode` (case/spazi-insensibile) è una modalità ammessa. Fonte unica
    usata sia dalla normalizzazione runtime sia dalla validazione, per non divergere."""
    return str(mode or "").strip().upper() in MODES


# Stringhe che significano "sì" o "no" ESPLICITI per `enabled` (il vocabolario "sì"
# è lo stesso di `autostart.is_enabled`, italiano incluso; test di parità in
# `test_source_manager`). Tutto ciò che non è in nessuno dei due è MALFORMATO:
# coercito a False (fail-closed) e segnalato a log da `_normalize_source`.
_ENABLED_TRUE = ("true", "1", "yes", "on", "si", "sì")
_ENABLED_FALSE = ("", "0", "false", "no", "off")


def as_enabled_bool(value) -> bool:
    """Coercizione FAIL-CLOSED a bool per `enabled` (C7 #259): abilita solo un "sì"
    esplicito — bool True, numeri FINITI non-zero, stringhe in `_ENABLED_TRUE` (il
    contratto numerico è identico a `autostart.is_enabled`/`as_bool_optin`: anche
    `2` o `-1` sono un numero esplicitamente non-zero, mai un typo). Prima era
    denylist-based (qualunque stringa non vuota fuori da `0/false/no/off` diventava
    True): un typo («flase», «disabled») o un NaN/inf da config corrotta RIABILITAVANO
    una sorgente che l'operatore credeva spenta → chat di nuovo ascoltata. Il default
    True per chiave ASSENTE resta al chiamante (`raw.get("enabled", True)`)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        # Un int non può essere NaN/inf: basta `!= 0` (niente conversione a float,
        # che su int fuori range solleverebbe OverflowError — lezione di #299).
        return value != 0
    if isinstance(value, float):
        return math.isfinite(value) and value != 0
    if isinstance(value, str):
        return value.strip().lower() in _ENABLED_TRUE
    return False


def _is_recognized_off(value) -> bool:
    """True se `value` è un "no" ESPLICITO (bool False, zero finito, stringa della
    denylist storica): serve a distinguere una disattivazione VOLUTA da un valore
    MALFORMATO coercito a False, che va invece segnalato a log (review Fable #309:
    il flip fail-closed non deve essere silenzioso per l'operatore)."""
    if isinstance(value, bool):
        return value is False
    if isinstance(value, int):
        return value == 0
    if isinstance(value, float):
        return math.isfinite(value) and value == 0
    if isinstance(value, str):
        return value.strip().lower() in _ENABLED_FALSE
    return False


# Alias retro-compatibile del vecchio nome privato (riferito da codice/test esterni).
_as_bool = as_enabled_bool


def _is_malformed_enabled(raw_enabled) -> bool:
    """True se `enabled` viene coercito a False senza essere un "no" ESPLICITO
    (fail-closed C7 #259). Predicato unico (nitpick CodeRabbit/Fable #309) condiviso
    tra `_normalize_source` (warning logger) e `malformed_enabled_warnings` (avvisi
    GUI), così log di modulo e log eventi non possono divergere."""
    return not as_enabled_bool(raw_enabled) and not _is_recognized_off(raw_enabled)

# Lunghezza massima di un valore mostrato nel messaggio di log ("..." inclusi).
_SHOWN_MAX = 60
# Coppie (chat_id, hash del valore) già segnalate a log: `source_chats` può girare in
# hot path (una normalizzazione per messaggio), e una config corrotta non deve
# riempire il log con lo stesso warning a ogni evento (review GLM/GPT #309). La chiave
# usa l'hash del valore COMPLETO (dimensione fissa: niente valori giganti in memoria)
# e non il testo troncato: due valori distinti con lo stesso prefisso loggano entrambi
# (review GLM/GPT round 3). Cap assoluto sotto: il set non cresce oltre `_WARNED_CAP`
# nemmeno con garbage variabile in un processo long-running (review Fable round 3).
_WARNED_ENABLED = set()
# Oltre il cap i warning NUOVI sono soppressi fino al riavvio/reset: config patologica
# (centinaia di valori malformati distinti) = flood; il fail-closed resta comunque
# attivo, il dedup tocca solo la visibilità.
_WARNED_CAP = 256
# check-then-add atomico: senza lock due thread (handler concorrenti) potrebbero
# superare entrambi il `not in` e loggare doppio (review Fugu/Fable round 3).
_WARNED_LOCK = threading.Lock()


def _reset_warnings() -> None:
    """Svuota il dedup dei warning: per i test e per un eventuale futuro hook di
    reload config (così una chat corretta e poi ri-corrotta torna a essere segnalata)."""
    with _WARNED_LOCK:
        _WARNED_ENABLED.clear()


def _safe_log_value(value, max_len=_SHOWN_MAX) -> str:
    """Rappresentazione SICURA di un valore da interpolare in un messaggio di log:
    `ascii()` escapa sia i non-ASCII (niente UnicodeEncodeError su handler cp1252
    Windows legacy, review GPT/Fable round 3) sia i caratteri di controllo (niente
    newline iniettate nel log); il troncamento evita righe giganti/leak lunghi."""
    shown = ascii(value)
    if len(shown) > max_len:
        shown = shown[:max_len - 3] + "..."
    return shown


def normalize_mode(mode) -> str:
    """Normalizza la modalità a PRE/LIVE; valore mancante/ignoto → DEFAULT_MODE
    (coercizione difensiva a runtime; la validazione invece RIFIUTA un valore
    ignoto, vedi `validate_sources`)."""
    m = str(mode or "").strip().upper()
    return m if is_valid_mode(m) else DEFAULT_MODE


def _normalize_source(raw: dict) -> dict:
    """Porta una sorgente grezza alla forma canonica (tipi e default coerenti).

    `enabled` è True di default (una sorgente appena aggiunta è attiva); `chat_id`
    e `provider` sono rifilati; `mode` normalizzato a PRE/LIVE. Un `enabled`
    MALFORMATO (né sì né no espliciti) viene coercito a False (fail-closed, C7 #259)
    e SEGNALATO a log: la sorgente smette di essere ascoltata e l'operatore deve
    poterlo vedere, non scoprirlo dall'assenza di segnali (review Fable/Sourcery)."""
    raw_enabled = raw.get("enabled", True)
    enabled = as_enabled_bool(raw_enabled)
    if _is_malformed_enabled(raw_enabled):
        # Solo chat_id + valore incriminato, entrambi resi ASCII-safe ed escapati da
        # `_safe_log_value` (niente UnicodeEncodeError cp1252, niente newline iniettate)
        # e TRONCATI (niente righe giganti né leak lunghi): mai altri campi della
        # config nel log. Freccia ASCII: handler Windows non-UTF8 (review GPT).
        chat = str(raw.get("chat_id", "") or "").strip()
        # Chiave di dedup su HASH dei valori COMPLETI: dimensione fissa in memoria
        # (nessun chat_id/valore gigante trattenuto) e nessuna collisione di prefisso
        # tra valori distinti che condividono i primi 57 caratteri.
        key = (hash(chat), hash(ascii(raw_enabled)))
        with _WARNED_LOCK:                       # una volta per chat+valore: no spam
            warn = key not in _WARNED_ENABLED and len(_WARNED_ENABLED) < _WARNED_CAP
            if warn:
                _WARNED_ENABLED.add(key)
        if warn:
            _LOG.warning(
                "source_chats: enabled=%s non riconosciuto per chat_id=%s -> sorgente "
                "DISABILITATA (fail-closed, C7 #259). Valori ammessi: %s / %s.",
                _safe_log_value(raw_enabled), _safe_log_value(chat),
                "/".join(_ENABLED_TRUE), "/".join(v or "''" for v in _ENABLED_FALSE))
    return {
        "name": str(raw.get("name", "") or "").strip(),
        "chat_id": str(raw.get("chat_id", "") or "").strip(),
        "enabled": enabled,
        "provider": str(raw.get("provider", "") or "").strip(),
        "mode": normalize_mode(raw.get("mode", "")),
    }


def source_chats(cfg: dict) -> list:
    """Elenco normalizzato delle sorgenti in config (le voci non-dict sono
    ignorate). Ritorna una COPIA: mutarla non altera la config."""
    out = []
    for raw in cfg.get("source_chats", []) or []:
        if isinstance(raw, dict):
            out.append(_normalize_source(raw))
    return out


def enabled_sources(cfg: dict) -> list:
    """Solo le sorgenti attive (`enabled=True`)."""
    return [s for s in source_chats(cfg) if s["enabled"]]


def enabled_chat_ids(cfg: dict) -> set:
    """Insieme dei `chat_id` delle sorgenti attive (esclusi quelli vuoti)."""
    return {s["chat_id"] for s in enabled_sources(cfg) if s["chat_id"]}


def source_for_chat(cfg: dict, chat_id: str):
    """La sorgente ATTIVA che gestisce `chat_id`, oppure None (chat non
    configurata o sorgente disattivata → ignorata)."""
    chat = str(chat_id or "").strip()
    if not chat:
        return None
    for s in enabled_sources(cfg):
        if s["chat_id"] == chat:
            return s
    return None


def provider_for_chat(cfg: dict, chat_id: str, default: str = "") -> str:
    """Provider da usare per `chat_id`:

    - provider esplicito della sorgente, se valorizzato;
    - altrimenti il default della modalità (PRE → ``TG_PRE``, LIVE → ``TG_LIVE``);
    - se non esiste una sorgente attiva per quella chat → `default` (il provider
      globale di config, per retro-compatibilità con il setup mono-chat)."""
    src = source_for_chat(cfg, chat_id)
    if src is None:
        return default
    if src["provider"]:
        return src["provider"]
    return _MODE_PROVIDER.get(src["mode"], default)


def validate_sources(raw_sources) -> list:
    """Errori **bloccanti** sulle sorgenti: `chat_id` mancante, `chat_id`
    duplicato (ogni chat una sola sorgente, altrimenti il provider sarebbe
    ambiguo), modalità non valida. Lista vuota = sorgenti valide."""
    errors = []
    seen_ids = set()
    for i, raw in enumerate(raw_sources or []):
        where = f"sorgente #{i + 1}"
        if not isinstance(raw, dict):
            errors.append(f"{where}: non è un oggetto.")
            continue
        chat = str(raw.get("chat_id", "") or "").strip()
        if not chat:
            errors.append(f"{where}: chat_id mancante.")
        elif chat in seen_ids:
            errors.append(
                f"{where}: chat_id duplicato {chat!r} (ogni chat una sola sorgente).")
        else:
            seen_ids.add(chat)
        raw_mode = str(raw.get("mode", "") or "").strip()
        # La validazione RIFIUTA una modalità ignota (a differenza di
        # normalize_mode che la coercizza a default): qui vogliamo avvisare l'utente.
        if raw_mode and not is_valid_mode(raw_mode):
            errors.append(
                f"{where}: modalità non valida {raw_mode!r}; ammesse {', '.join(MODES)}.")
    return errors


def malformed_enabled_warnings(raw_sources) -> list:
    """Avvisi **non bloccanti** per la GUI/event log: sorgenti con `enabled` MALFORMATO
    (né sì né no espliciti), che il normalizzatore coercizza a DISATTIVATA (fail-closed,
    C7 #259). Il warning del logger Python di `_normalize_source` non è visibile
    nell'app windowed Windows (Codex P2 #309): `_start` mostra QUESTI messaggi nel log
    eventi, così l'operatore non scopre il flip dall'assenza di segnali. Valori
    sanificati/troncati come nel log di modulo: mai altri campi della config."""
    warnings = []
    for raw in raw_sources or []:
        if not isinstance(raw, dict):
            continue
        raw_enabled = raw.get("enabled", True)
        if _is_malformed_enabled(raw_enabled):
            chat = str(raw.get("chat_id", "") or "").strip()
            warnings.append(
                f"Sorgente chat_id={_safe_log_value(chat)}: enabled="
                f"{_safe_log_value(raw_enabled)} non riconosciuto -> considerata "
                f"DISATTIVATA (fail-closed). Usa true/false (o si/no, on/off, 1/0).")
    return warnings


def duplicate_name_warnings(raw_sources) -> list:
    """Avvisi **non bloccanti**: nomi di sorgente duplicati (confondono l'utente,
    ma non compromettono il routing, che usa il `chat_id`)."""
    counts = {}
    for raw in raw_sources or []:
        if not isinstance(raw, dict):
            continue
        name = str(raw.get("name", "") or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    return [
        f"Nome sorgente duplicato {name!r} ({n} volte): rinominane uno per distinguerle."
        for name, n in counts.items() if n > 1
    ]
