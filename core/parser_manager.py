"""CP-07: gestione del Parser Personalizzato attivo.

Decide QUALE parser usare, leggendo la config:
- `active_parser`: nome del parser attivo di default ("" = usa il parser hardcoded);
- `parser_by_chat`: override per chat sorgente `{chat_id: nome}` (forward-compatibile
  col multi-chat; con la singola chat attuale funziona comunque).

Funzioni pure su `dict` di config + caricamento dei parser salvati (CP-01). NON
scrive il CSV e NON tocca il runtime/GUI: la sostituzione del parser hardcoded
nel flusso live è CP-09.
"""

from . import custom_parser


def active_parser_name(cfg: dict) -> str:
    return str((cfg or {}).get("active_parser", "") or "").strip()


def parser_by_chat(cfg: dict) -> dict:
    value = (cfg or {}).get("parser_by_chat", {})
    if not isinstance(value, dict):
        return {}
    # Chiavi normalizzate a str ALLA FONTE: i chat_id arrivano come stringhe nel live e
    # tutti i consumatori (is_chat_allowed/allowed_chats, _chat_approved_for_custom,
    # resolve_parser_name) confrontano `str(chat)`. Una chiave non-stringa (es. int da
    # config editata a mano) darebbe lookup incoerenti: la chat verrebbe ammessa ma il
    # parser per-chat non trovato (Codex P2). Normalizzare qui allinea tutti i percorsi.
    return {str(k): v for k, v in value.items()}


def parser_list_by_chat(cfg: dict) -> dict:
    """PR-2 (router multi-parser): mappa `{chat_id: [nome, ...]}` di PIÙ parser per una
    chat, valutati in ordine (first-to-last). Chiavi normalizzate a str; valori = liste di
    nomi non vuoti, **deduplicati preservando l'ordine**. Voci non-lista o liste che
    restano vuote dopo la pulizia sono ignorate (fail-safe su config manomessa a mano).

    Chiave di config separata da `parser_by_chat` (che resta il singolo, retro-compat):
    così i file salvati prima della feature caricano invariati e i lettori "single"
    esistenti non cambiano comportamento."""
    value = (cfg or {}).get("parser_list_by_chat", {})
    if not isinstance(value, dict):
        return {}
    out = {}
    for k, v in value.items():
        if not isinstance(v, list):
            continue
        seen = set()
        ordered = []
        for n in v:
            name = str(n or "").strip()
            if name and name not in seen:
                seen.add(name)
                ordered.append(name)
        if ordered:
            out[str(k)] = ordered
    return out


def resolve_parser_names(cfg: dict, chat_id: str = "") -> list:
    """PR-2: LISTA ORDINATA dei parser da usare per `chat_id`. Precedenza:

    1. lista esplicita `parser_list_by_chat[chat]` (multi-parser) — in ordine;
    2. altrimenti il singolo override `parser_by_chat[chat]`;
    3. altrimenti l'attivo globale `active_parser`.

    `[]` = nessun parser configurato (→ chat non processata). Deduplicata preservando
    l'ordine. È la fonte unica del routing multi-parser: `resolve_parser_name` (singolo)
    ne ritorna il primo per i chiamanti legacy."""
    chat_key = str(chat_id or "").strip()
    if chat_key:
        multi = parser_list_by_chat(cfg).get(chat_key)
        if multi:
            return list(multi)
        override = str(parser_by_chat(cfg).get(chat_key, "") or "").strip()
        if override:
            return [override]
    name = active_parser_name(cfg)
    return [name] if name else []


def resolve_parser_name(cfg: dict, chat_id: str = "") -> str:
    """Nome del parser PRIMARIO per `chat_id`: il PRIMO di `resolve_parser_names`
    (retro-compat coi chiamanti single-parser). "" = nessuno.

    Onora anche la lista multi (`parser_list_by_chat`): così `should_process` e le viste
    che chiedono "c'è un parser?" restano corrette anche per una chat configurata SOLO
    con la lista multi (senza `parser_by_chat`/globale)."""
    names = resolve_parser_names(cfg, chat_id)
    return names[0] if names else ""


def set_active(cfg: dict, name: str) -> dict:
    """Ritorna una COPIA della config con il parser attivo impostato."""
    out = dict(cfg or {})
    out["active_parser"] = str(name or "").strip()
    out["parser_by_chat"] = parser_by_chat(out)  # copia: non condividere la mappa
    return out


def set_for_chat(cfg: dict, chat_id: str, name: str) -> dict:
    """Ritorna una copia della config con l'override SINGOLO per la chat impostato
    (nome vuoto → rimuove l'override).

    PR-2 (Codex #391): imposta il singolo `parser_by_chat[chat]` **e** RIMUOVE una eventuale
    LISTA multi-parser stantia per la stessa chat (`parser_list_by_chat[chat]`). Senza,
    riportando una chat a un solo parser (o azzerandola) via questo percorso legacy, la lista
    vecchia — che ha PRECEDENZA in `resolve_parser_names` — continuerebbe a vincere, lasciando
    il routing sui parser vecchi. Il singolo è la nuova verità → la lista va tolta."""
    out = dict(cfg or {})
    mapping = parser_by_chat(out)
    list_mapping = parser_list_by_chat(out)
    chat_key = str(chat_id or "").strip()
    clean = str(name or "").strip()
    if chat_key:
        if clean:
            mapping[chat_key] = clean
        else:
            mapping.pop(chat_key, None)
        list_mapping.pop(chat_key, None)   # la lista multi stantia NON deve più vincere
    out["parser_by_chat"] = mapping
    _set_or_clear(out, "parser_list_by_chat", list_mapping)
    return out


def set_list_for_chat(cfg: dict, chat_id: str, names) -> dict:
    """PR-2: ritorna una copia della config con la LISTA di parser per la chat impostata
    (`parser_list_by_chat[chat]`). Lista vuota/None → rimuove la voce.

    Deduplica preservando l'ordine. Mantiene inoltre `parser_by_chat[chat]` sincronizzato
    col PRIMO nome (o lo rimuove se la lista è vuota): così le viste e i lettori "single"
    (config_summary, chat-approval legacy) restano coerenti e la chat resta approvata
    dallo stesso percorso di sempre, senza indebolire il filtro chat. Scrive ENTRAMBE le mappe
    direttamente (non via `set_for_chat`, che rimuoverebbe la lista appena impostata)."""
    out = dict(cfg or {})
    mapping = parser_list_by_chat(out)
    by_chat = parser_by_chat(out)
    chat_key = str(chat_id or "").strip()
    clean = []
    seen = set()
    for n in (names or []):
        nm = str(n or "").strip()
        if nm and nm not in seen:
            seen.add(nm)
            clean.append(nm)
    if chat_key:
        if clean:
            mapping[chat_key] = clean
            by_chat[chat_key] = clean[0]       # sync del singolo al primo
        else:
            mapping.pop(chat_key, None)
            by_chat.pop(chat_key, None)
    out["parser_by_chat"] = by_chat
    _set_or_clear(out, "parser_list_by_chat", mapping)
    return out


def _set_or_clear(container: dict, key: str, value: dict) -> None:
    """Scrive `value` sotto `key` se non vuoto, altrimenti rimuove la chiave (config pulita)."""
    if value:
        container[key] = value
    else:
        container.pop(key, None)


def available_parser_names(dir_path: str = None) -> list:
    """Nomi dei parser salvati (dal campo `name`), per i menu di selezione.
    I file illeggibili vengono saltati."""
    import os
    names = []
    for path in custom_parser.list_parser_files(dir_path):
        try:
            defn = custom_parser.load_parser(path)
        except (OSError, ValueError):
            continue
        # Elenca solo i nomi che load_active() saprebbe ricaricare: il parser
        # dev'essere valido E il suo nome deve ri-mappare proprio a questo file
        # (un file rinominato a mano, es. Wrong.json con name "Shown", offrirebbe
        # altrimenti un nome che porta a un fallback silenzioso).
        if not custom_parser.is_valid(defn):
            continue
        if os.path.abspath(custom_parser.parser_path(defn.name, dir_path)) != os.path.abspath(path):
            continue
        names.append(defn.name)
    return sorted(names)


def _load_by_name(name: str, dir_path: str = None):
    """Carica UN parser per nome con la logica **fail-closed** del live (CP-09):
    - `""`/file mancante/corrotto → None;
    - il file caricato deve essere PROPRIO il parser richiesto: nomi diversi che si
      sanitizzano allo stesso file (es. "A/B" → AB.json) non devono caricare il parser
      sbagliato;
    - la definizione dev'essere valida (un file editato a mano con, es., BetType duplicato,
      non deve raggiungere il CSV)."""
    if not name:
        return None
    path = custom_parser.parser_path(name, dir_path)
    try:
        defn = custom_parser.load_parser(path)
    except (OSError, ValueError):
        return None
    if defn.name != name or not custom_parser.is_valid(defn):
        return None
    return defn


def load_active(cfg: dict, chat_id: str = "", dir_path: str = None):
    """Carica la definizione del parser PRIMARIO per `chat_id`, oppure None se
    nessun parser è selezionato o il file non esiste. Un file corrotto → None
    (fail-safe). Per il multi-parser usa `load_active_list`."""
    return _load_by_name(resolve_parser_name(cfg, chat_id), dir_path)


def load_active_list(cfg: dict, chat_id: str = "", dir_path: str = None) -> list:
    """PR-2: carica TUTTE le definizioni per `chat_id`, in ordine (`resolve_parser_names`),
    saltando i nomi mancanti/corrotti/invalidi (stessa logica fail-closed di `load_active`).
    Niente duplicati per nome. `[]` se nessun parser è configurato o caricabile.

    Un nome non caricabile viene SALTATO (non blocca gli altri parser della chat): così un
    file rimosso a mano non ferma gli altri segnali, coerente col fail-safe di `load_active`."""
    out = []
    seen = set()
    for name in resolve_parser_names(cfg, chat_id):
        if name in seen:
            continue
        seen.add(name)
        defn = _load_by_name(name, dir_path)
        if defn is not None:
            out.append(defn)
    return out
