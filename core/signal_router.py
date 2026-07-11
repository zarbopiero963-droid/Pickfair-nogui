"""CP-09: instradamento del segnale al parser giusto (logica testabile).

Decide la riga CSV da scrivere per un messaggio Telegram:

- se per la chat è attivo un **Parser Personalizzato** (CP-07), è lui a parsare:
  se produce una riga piazzabile la si scrive, altrimenti il segnale è scartato.
  Un custom attivo è **autoritativo**;
- se nessun custom è attivo, il messaggio è **ignorato**: il parser automatico
  P.Bet (`parse_message`) è DISATTIVATO nel percorso live (CP-09b). Per processare
  una chat serve un Parser Personalizzato attivo (globale o per-chat). Il codice di
  `parse_message`/`build_csv_row` resta nel repo ma non è più nel flusso live.

Funzione pura (nessuna GUI/scrittura): ritorna la riga e lo stato; è `app` a
scrivere il CSV. Così l'instradamento è interamente testabile.
"""

from dataclasses import dataclass, field

from . import (
    custom_parser_engine,
    custom_pipeline,
    market_mapping_store,
    name_mapping_store,
    parser_manager,
    recognition,
    source_manager,
    validator,
)

CUSTOM = "custom"
HARDCODED = "hardcoded"

# Nessun Parser Personalizzato attivo: il parser automatico P.Bet è disattivato
# (CP-09b), quindi il messaggio è ignorato (nessuna riga scritta).
NO_PARSER = "no_parser"

# Gate di contenuto: il custom è piazzabile ma non ha estratto nulla dal messaggio
# (parser a soli valori fissi su testo arbitrario) → scartato, niente scrittura.
NO_CONTENT_MATCH = "NO_CONTENT_MATCH"


def _disabled_source_ids(cfg: dict) -> set:
    """Chat di sorgenti `source_chats` **disattivate** (deny-list): disattivare una
    sorgente deve fermarla DAVVERO, anche se la stessa chat ha un override
    `parser_by_chat` o coincide con `chat_id` (PR-24, finding Codex)."""
    return {s["chat_id"] for s in source_manager.source_chats(cfg)
            if not s["enabled"] and s["chat_id"]}


def _chat_approved_for_custom(cfg: dict, chat: str) -> bool:
    """Una chat è approvata per il parsing custom se è quella CONFIGURATA
    (`chat_id`), ha una voce esplicita in `parser_by_chat`, o è una **sorgente
    multi-chat ATTIVA** (`source_chats`, PR-24): così un `active_parser` GLOBALE
    funziona anche per le sorgenti, senza far scommettere chat non autorizzate.
    Una sorgente **disattivata** non è mai approvata, nemmeno con un override."""
    # `.strip()` sul chat runtime per SIMMETRIA con gli altri comparatori (#184 M2, Codex P2):
    # `is_chat_allowed`/`resolve_parser_name`/`is_notification_chat` normalizzano già il chat,
    # ma il gate live `should_process` chiama ANCHE questa funzione — se restasse grezza, un
    # chat con padding sarebbe ammesso da `is_chat_allowed` ma poi scartato qui
    # (IGNORE_NOT_RELEVANT), lasciando il fix M2 monco. Lo strip rende il gate coerente; resta
    # fail-closed (un id diverso non è approvato) e rafforza anche la deny-list sorgenti.
    chat = str(chat or "").strip()
    if chat in _disabled_source_ids(cfg):
        return False
    if chat and chat in parser_manager.parser_by_chat(cfg):
        return True
    # PR-2: una chat con una LISTA multi-parser (`parser_list_by_chat`) è approvata come lo
    # è una con override singolo. `set_list_for_chat` sincronizza già `parser_by_chat`, ma
    # questo copre anche una config editata a mano con la sola lista (robustezza; non
    # indebolisce il filtro: approva solo chat con un parser ESPLICITO configurato).
    if chat and chat in parser_manager.parser_list_by_chat(cfg):
        return True
    if chat and chat in set(map(str, source_manager.enabled_chat_ids(cfg))):
        return True
    configured = str(cfg.get("chat_id", "") or "").strip()
    return bool(configured) and chat == configured


def has_chat_filter(cfg: dict) -> bool:
    """True se la config definisce ALMENO un criterio di ammissione chat: `chat_id`,
    una voce `parser_by_chat`, o una `source_chats` (anche **disattivata**).

    Unica fonte di verità della condizione "ammetti tutte": `is_chat_allowed` la usa
    per il ramo legacy e `app._start` per il fail-fast d'avvio, così le due non
    possono divergere (finding Sourcery). Quando ritorna False il bridge accetterebbe
    segnali da **qualsiasi** chat → `app._start` annulla l'avvio."""
    configured = str(cfg.get("chat_id", "") or "").strip()
    per_chat = parser_manager.parser_by_chat(cfg)
    per_chat_list = parser_manager.parser_list_by_chat(cfg)   # PR-2: lista multi-parser
    has_sources = bool(source_manager.source_chats(cfg))
    return bool(configured or per_chat or per_chat_list or has_sources)


def allowed_chats(cfg: dict) -> set:
    """Insieme ESPLICITO dei `chat_id` che il listener processerà: unione di
    `chat_id` configurato, chiavi `parser_by_chat` e sorgenti `source_chats`
    **attive**, MENO le sorgenti **disattivate** (deny-list). È il modello "ascolta
    solo queste chat, mai tutte": la GUI può mostrarlo all'utente ("ascolto queste N
    chat") e il listener processa esattamente questo insieme.

    Fonte unica della allowlist: `is_chat_allowed` la riusa, così filtro live e
    visualizzazione non possono divergere. ATTENZIONE: un set **vuoto** NON significa
    "ammetti tutte". Quando non c'è alcun criterio (`not has_chat_filter`) il
    comportamento legacy sarebbe "ammetti tutte" (vedi `is_chat_allowed`), ma è
    bloccato dal fail-fast d'avvio; distinguere i due casi con `has_chat_filter`."""
    configured = str(cfg.get("chat_id", "") or "").strip()
    # Chiavi parser_by_chat già normalizzate a str dalla fonte (parser_manager),
    # coerenti col confronto str(chat) di is_chat_allowed e coi lookup del custom.
    allowed = set(parser_manager.parser_by_chat(cfg).keys())
    allowed |= set(parser_manager.parser_list_by_chat(cfg).keys())   # PR-2: chat multi-parser
    allowed |= set(map(str, source_manager.enabled_chat_ids(cfg)))   # solo le attive
    if configured:
        allowed.add(configured)
    # Una sorgente DISATTIVATA è deny-list: vince su parser_by_chat/chat_id.
    allowed -= _disabled_source_ids(cfg)
    return allowed


def is_chat_allowed(cfg: dict, chat: str) -> bool:
    """Chat che il bridge può processare nel live: quella CONFIGURATA (`chat_id`),
    le chiavi `parser_by_chat` e le **sorgenti multi-chat ATTIVE** (`source_chats`
    con `enabled=True`, PR-24). Una sorgente disattivata NON è ammessa.

    Comportamento legacy "tutte ammesse" SOLO se NULLA è configurato (`not
    has_chat_filter`): `chat_id` vuoto, `parser_by_chat` vuota e **nessuna**
    `source_chats` (anche disattivata). Così disattivare tutte le sorgenti **blocca
    tutte** le chat, non riapre il gate. Gatea sia il percorso custom sia l'hardcoded:
    nessuna scrittura per chat non autorizzate. L'allowlist esplicita è calcolata da
    `allowed_chats` (fonte unica)."""
    if not has_chat_filter(cfg):
        return True
    # `.strip()` sul chat runtime per SIMMETRIA con l'allowlist (#184 M2): `allowed_chats`
    # strippa l'ID configurato (e M1 normalizza `chat_id` già a monte), mentre qui il chat in
    # ingresso era confrontato grezzo. Senza, un chat con whitespace (es. da una sorgente che
    # lo formatta con padding) farebbe fallire un match logicamente valido → segnale scartato
    # (fail-closed, non un bypass). Stesso confronto simmetrico di `is_notification_chat`.
    return str(chat or "").strip() in allowed_chats(cfg)


def is_notification_chat(cfg: dict, chat: str) -> bool:
    """`True` se `chat` è la chat-notifiche XTrader configurata
    (`xtrader_notification_chat_id`): una chat SEPARATA dalle sorgenti che porta gli
    ESITI (conferma/rifiuto), non segnali. Confronto stringa-vs-stringa; vuoto → mai.

    Letta dalla config VIVA dal listener (audit C8): cambiarla a runtime ha effetto
    SUBITO, coerentemente col live-reload del routing — niente più snapshot a START che
    ignorava la modifica (conferme mis-classificate). Fonte unica, così GUI/listener non
    la reimplementano."""
    notif = str((cfg or {}).get("xtrader_notification_chat_id", "") or "").strip()
    # `.strip()` su ENTRAMBI i lati: l'ID configurato e il chat runtime vanno confrontati
    # in modo simmetrico, altrimenti un eventuale whitespace (config scritta a mano) farebbe
    # fallire un match logicamente valido (Sourcery).
    return bool(notif) and str(chat or "").strip() == notif


def listened_chats(cfg: dict) -> list:
    """Vista LEGGIBILE delle chat che il listener processerà, per la GUI (B1).

    Per ogni `chat_id` in `allowed_chats(cfg)` ritorna `{"chat_id", "name"}`: il nome è
    quello della `source_chats` corrispondente (se presente), altrimenti "" (chat da
    `chat_id`/`parser_by_chat` senza voce sorgente → solo l'ID). Ordinata per nome
    (case-insensitive), poi per ID; le chat senza nome vanno in fondo. Solo
    presentazione: nessuna decisione di routing qui, l'allowlist resta `allowed_chats`."""
    names = {}
    for src in source_manager.source_chats(cfg):
        cid = str(src.get("chat_id", "") or "").strip()
        nm = str(src.get("name", "") or "").strip()
        if cid and nm:
            names[cid] = nm
    rows = [{"chat_id": cid, "name": names.get(cid, "")} for cid in allowed_chats(cfg)]
    # Prima le chat con nome (False ordina prima di True), per nome case-insensitive,
    # poi per chat_id; quelle senza nome finiscono in fondo, ordinate per ID.
    return sorted(rows, key=lambda r: (r["name"] == "", r["name"].lower(), r["chat_id"]))


def active_custom_parser(cfg: dict, chat: str, parsers_dir: str = None):
    """Parser custom PRIMARIO per `chat`, oppure None se la chat non è approvata o
    nessun parser è attivo. Usato dal prefiltro live e dai chiamanti single-parser;
    per il routing multi-parser (PR-2) usa `active_custom_parsers`."""
    if not _chat_approved_for_custom(cfg, chat):
        return None
    return parser_manager.load_active(cfg, chat, parsers_dir)


def active_custom_parsers(cfg: dict, chat: str, parsers_dir: str = None) -> list:
    """PR-2 (router multi-parser): LISTA ORDINATA dei parser custom per `chat`, oppure `[]`
    se la chat non è approvata o nessun parser è configurato/caricabile. È la fonte del
    routing multi-parser di `resolve_row`; la stessa gate di approvazione chat del singolo
    (`_chat_approved_for_custom`) resta invariata (nessun indebolimento del filtro chat)."""
    if not _chat_approved_for_custom(cfg, chat):
        return []
    return parser_manager.load_active_list(cfg, chat, parsers_dir)


def has_active_parser_config(cfg: dict) -> bool:
    """True se è configurato almeno un Parser Personalizzato: l'attivo globale
    (`active_parser`) oppure un override per-chat non vuoto (`parser_by_chat`).

    Con il parser automatico P.Bet disattivato (CP-09b), senza alcun parser
    configurato il bridge non processerebbe NESSUN segnale: `app._start` lo usa per
    avvisare l'operatore (un listener "connesso" che ignora tutto in silenzio sarebbe
    fuorviante, Codex P2)."""
    if parser_manager.active_parser_name(cfg):
        return True
    if any(str(v or "").strip() for v in parser_manager.parser_by_chat(cfg).values()):
        return True
    # PR-2: anche una sola chat con lista multi-parser conta come "parser configurato".
    return bool(parser_manager.parser_list_by_chat(cfg))


def should_process(cfg: dict, chat: str, text: str, parsers_dir: str = None) -> bool:
    """Decide se un messaggio live va instradato (PR-11). Logica pura e testabile,
    estratta dal listener Telegram. Con il parser automatico P.Bet disattivato
    (CP-09b) una chat viene processata SOLO se:

    - è ammessa (`is_chat_allowed`) → altrimenti mai (non si indebolisce il filtro chat);
    - è approvata per il custom **e** ha un parser **configurato** (nome non vuoto,
      globale o per-chat).

    Si guarda il NOME configurato (`resolve_parser_name`), non il caricamento: se il
    file è mancante/invalido la chat viene comunque processata, così `resolve_row`
    gira e LOGGA il fallimento (NO_PARSER) invece di far sparire i segnali in silenzio
    (Codex P2). Senza alcun parser configurato non c'è nulla da processare. Una chat
    ammessa ma senza parser custom NON viene più processata: prima la gestiva il parser
    hardcoded col prefiltro marker `P.Bet.`/📊, ora rimosso dal percorso live. (`text`
    non è più usato: la decisione non dipende dal contenuto.)"""
    if not is_chat_allowed(cfg, chat):
        return False
    if not _chat_approved_for_custom(cfg, chat):
        return False
    return bool(parser_manager.resolve_parser_name(cfg, chat))


@dataclass
class RouteResult:
    """Esito dell'instradamento. `row` è valorizzata SOLO se piazzabile.

    `rows` (#192) contiene TUTTE le righe piazzabili quando il parser è multi-riga
    (MultiMarket/MultiSelection). Per il single-row resta `None` e si usa `row` (retro-
    compatibile: i chiamanti esistenti che leggono `row`/`placeable` continuano a funzionare;
    `row` riflette comunque la prima riga). Usare `all_rows()` per iterare in modo uniforme."""

    row: dict = None
    status: str = validator.VALID
    source: str = HARDCODED                       # custom | hardcoded
    detail: object = None                         # motivo dello scarto
    missing_required: list = field(default_factory=list)
    rows: list = None                             # multi-row (#192); None → single via `row`

    @property
    def placeable(self) -> bool:
        return bool(self.all_rows())

    def all_rows(self) -> list:
        """Tutte le righe piazzabili: `rows` se valorizzato (multi), altrimenti `[row]` se
        presente, altrimenti `[]` (#192, retro-compatibile)."""
        if self.rows is not None:
            return [r for r in self.rows if r]
        return [self.row] if self.row is not None else []


def _row_key(row: dict):
    """Chiave di identità di una riga CSV per la deduplica multi-parser (PR-2): due righe
    con gli stessi campi/valori sono la STESSA scommessa → una sola volta. Ordinata sui
    campi così l'ordine di inserimento nel dict non conta."""
    return tuple(sorted((str(k), str(v)) for k, v in row.items()))


def _resolve_one(defn, text: str, *, cfg: dict, chat: str, provider: str, id_resolver):
    """Applica UN parser al messaggio e dice se ha «scattato» (PR-2). Ritorna un dict:
    `{"fired": bool, "rows": list, "status": str, "detail": obj, "missing": list,
      "multi": bool}`.

    `fired=True` solo se il parser ha prodotto almeno una riga piazzabile **e** ha superato
    il **gate di contenuto** `matches_message` (che include le condizioni di gate PR-1):
    così un parser scatta solo sui messaggi pertinenti. Altrimenti `fired=False` con la
    diagnostica del perché (nessuna riga). La logica per-parser è identica al percorso
    single di prima — qui è solo estratta per poterla applicare a più parser."""
    # Modalità di riconoscimento DEL PARSER (per-parser); se assente (file legacy) eredita
    # la globale `recognition_mode` — comportamento invariato (Codex P1).
    mode = recognition.normalize_mode(
        defn.mode or cfg.get("recognition_mode", recognition.DEFAULT_MODE))
    # Quota obbligatoria governata dalla riga Price del parser (`Obblig.`).
    require_price = defn.price_required()
    # Profili di mappatura nomi/mercati del parser (None se non selezionati → invariato).
    name_mapping_profiles = (
        name_mapping_store.entries_for_profiles(cfg, defn.name_mapping_profiles)
        if defn.name_mapping_profiles else None)
    market_mapping_profiles = (
        market_mapping_store.entries_for_profiles(cfg, defn.market_mapping_profiles)
        if defn.market_mapping_profiles else None)
    # #192: pipeline multi-riga (un parser single-row → esattamente 1 elemento).
    results = custom_pipeline.build_validated_rows(
        defn, text, provider=provider, mode=mode, require_price=require_price,
        name_mapping_profiles=name_mapping_profiles,
        market_mapping_profiles=market_mapping_profiles,
        id_resolver=id_resolver)
    placeable = [r.row for r in results if r.placeable]
    # Gate di contenuto (incl. condizioni PR-1): il parser deve aver estratto qualcosa DA
    # QUESTO messaggio, altrimenti non scatta (niente bet spurio su testo arbitrario). Valutato
    # PRIMA della diagnostica di validazione (CodeRabbit #391): se le condizioni/contenuto NON
    # combaciano, il motivo è NO_CONTENT_MATCH — non un errore di validazione (campi mancanti) su
    # un messaggio che il parser non doveva neppure gestire. Una msg che il parser DOVEVA gestire
    # ma è malformata passa comunque il gate (recognition estratto) → resta la diagnostica utile.
    matched = custom_parser_engine.matches_message(defn, text, mode)
    if not matched:
        return {"fired": False, "rows": [], "status": NO_CONTENT_MATCH,
                "detail": "no_content_match", "missing": [], "multi": defn.is_multi_row()}
    if not placeable:
        first = results[0]
        return {"fired": False, "rows": [], "status": first.status,
                "detail": first.detail, "missing": list(first.missing_required),
                "multi": defn.is_multi_row()}
    # PR-24: su una chat che è sorgente attiva, il provider della sorgente VINCE sul Provider
    # fisso del parser, per TUTTE le righe.
    if source_manager.source_for_chat(cfg, chat) is not None:
        placeable = [{**row, "Provider": provider} for row in placeable]
    return {"fired": True, "rows": placeable, "status": validator.VALID,
            "detail": None, "missing": [], "multi": defn.is_multi_row()}


def resolve_row(text: str, cfg: dict, *, chat_id: str = None, parsers_dir: str = None,
                id_resolver=None) -> RouteResult:
    """Sceglie il parser e ritorna la riga da scrivere (o `row=None` se scartata).

    `chat_id` è la chat di ORIGINE del messaggio (dal live): se passato ha la
    precedenza sul `chat_id` di config, così l'override `parser_by_chat` funziona
    anche in setup multi-chat dove il singolo `chat_id` non è impostato.

    `id_resolver` (opzionale, PR-P12): se fornito (es. `DictionaryResolver` sul dizionario
    Betfair locale), il pipeline prova a riempire EventId/MarketId/SelectionId dalla catena
    del dizionario per lo sport del parser — **additivo e fail-open**: se non trova un match
    univoco la riga resta a nomi (fallback nomi), senza bloccare il segnale."""
    chat = str((chat_id if chat_id is not None else cfg.get("chat_id", "")) or "")
    # Provider PER-CHAT (PR-24): per una sorgente multi-chat attiva usa il suo provider
    # (esplicito, o derivato dalla modalità PRE→TG_PRE / LIVE→TG_LIVE); altrimenti il
    # provider globale di config (retro-compatibilità mono-chat).
    provider = source_manager.provider_for_chat(
        cfg, chat, default=str(cfg.get("provider", "") or ""))

    # PR-2 (router multi-parser): la chat può avere PIÙ parser (in ordine). Ciascuno è
    # applicato al messaggio; scattano TUTTI quelli le cui condizioni/estrazione combaciano
    # (scelta del proprietario: «tutti quelli che combaciano»). Un solo parser configurato →
    # il loop ha un elemento e il comportamento è IDENTICO a prima (retro-compatibile).
    parsers = active_custom_parsers(cfg, chat, parsers_dir)
    if parsers:
        outcomes = [
            _resolve_one(defn, text, cfg=cfg, chat=chat, provider=provider,
                         id_resolver=id_resolver)
            for defn in parsers
        ]
        fired = [o for o in outcomes if o["fired"]]
        if fired:
            # Unisci le righe di TUTTI i parser che hanno matchato, in ordine di priorità,
            # deduplicando le righe IDENTICHE: due parser che producono la STESSA riga = una
            # sola scommessa (no doppia scommessa accidentale); righe DIVERSE = bet diversi
            # voluti dal proprietario. La deduplica per-riga a valle (#192/#239) resta il gate
            # anti-duplicato nel commit.
            combined = []
            seen = set()
            for outcome in fired:
                for row in outcome["rows"]:
                    key = _row_key(row)
                    if key not in seen:
                        seen.add(key)
                        combined.append(row)
            # UN solo parser single-row con UNA riga → percorso legacy (`rows=None`):
            # comportamento e deduplica-per-hash IDENTICI a prima della feature. Appena c'è
            # più di un parser che scatta, un parser multi-riga, o più righe, si passa la
            # provenienza multi (`rows`) così il commit usa la deduplica PER-RIGA (Codex
            # #239/#192): senza, un secondo bet generato dallo stesso messaggio non verrebbe
            # riconosciuto e si rischierebbe una doppia scommessa.
            if len(fired) == 1 and not fired[0]["multi"] and len(combined) == 1:
                return RouteResult(combined[0], validator.VALID, CUSTOM)
            return RouteResult(combined[0], validator.VALID, CUSTOM, rows=combined)
        # Nessun parser ha matchato: riporta la diagnostica del PRIMO esito. Con un solo
        # parser configurato è ESATTAMENTE la diagnostica di prima (retro-compatibile).
        first = outcomes[0]
        return RouteResult(None, first["status"], CUSTOM, first["detail"], list(first["missing"]))

    # Nessun Parser Personalizzato caricato: il parser automatico P.Bet è DISATTIVATO
    # (CP-09b), quindi il messaggio è ignorato (riga non piazzabile, nessuna scrittura).
    # Distinguo i due casi per l'operatore (Codex P2): nessun parser configurato vs un
    # parser configurato ma con file mancante/invalido (selezione stantia) — quest'ultimo
    # va segnalato col nome, così non sparisce in silenzio.
    configured = parser_manager.resolve_parser_name(cfg, chat)
    detail = f"parser_non_caricabile:{configured}" if configured else "no_active_parser"
    return RouteResult(None, NO_PARSER, NO_PARSER, detail)
