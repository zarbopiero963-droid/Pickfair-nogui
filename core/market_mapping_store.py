"""Dizionario di mappatura mercati: frase del provider → Mercato/Selezione XTrader.

Alcuni provider (canali Telegram) scrivono il mercato **a parole** ("0,5 HT") dentro il
messaggio. Questo modulo tiene **profili** di regole che leggono il mercato **da una
posizione precisa** del messaggio — tra i delimitatori ``Inizia dopo``/``Finisce prima``,
come una regola del Parser Personalizzato — e lo traducono nel Mercato/Selezione canonici
del **Catalogo XTrader**. I valori di mercato/selezione **non** sono testo libero: vanno
scelti dal Catalogo (vedi GUI), così ciò che finisce nel CSV è sempre canonico.

Perché a delimitatori e non "frase su tutto il messaggio": molti provider mettono in testa
un **banner/menu** con più mercati (es. ``P.Bet. 30/0,5HT/1,5HT/1 ASIATICO``); cercare la
frase nell'intero testo darebbe falsi match/ambiguità. Leggendo SOLO il campo delimitato
(es. fra «Quota» e «Prematch») si prende il mercato vero del segnale e si ignora il banner.

Modello dati (config, chiave ``market_mappings``)::

    cfg["market_mappings"] = {
        "<nome profilo>": [
            {"start_after": "Quota",      # "Inizia dopo": delimitatore sinistro
             "end_before": "Prematch",    # "Finisce prima": delimitatore destro ("" = fine riga)
             "phrase": "0,5 HT",          # testo mercato da riconoscere nel campo estratto
             "market_type": "FIRST_HALF_GOALS_05",
             "market_name": "1º tempo - Totale goal 0,5",
             "selection_name": "Over 0,5 goal"},
            ...
        ],
        ...
    }

Logica PURA su un ``dict`` di config: nessuna GUI, nessun I/O — la persistenza è del
chiamante (``config_store.save_config``), come ``name_mapping_store``/``provider_store``.
Le funzioni di modifica ritornano una COPIA della config, non mutano l'originale.

Sicurezza (safety-critical: un mercato sbagliato = scommessa sbagliata). Decisioni
del proprietario, vedi ``docs/audit/mercati_mapping_design.md``:
- **D2 fail-closed sull'ambiguità**: se più frasi combaciano e indicano mercati
  **diversi**, ``resolve_market`` ritorna stato ``"ambiguous"`` → il chiamante NON
  scrive nulla (niente mercato "a caso");
- **D3 match sul campo estratto**: il mercato si legge SOLO tra i delimitatori
  ``Inizia dopo``/``Finisce prima`` (non su tutto il messaggio), poi il testo mercato si
  confronta case-insensitive e su **confini di token** (no falsi positivi tipo "over" dentro
  "overflow"). Una voce **senza delimitatori** è **preservata** in config ma **non
  applicata** (``resolve_market`` la salta, fail-closed): la modalità "frase su tutto il
  messaggio" è rimossa, ma le voci vecchie non vengono cancellate (no perdita dati);
- nessun match → stato ``"none"`` (il chiamante decide il fallback, vedi precedenza D1
  nel runtime). ``resolve_market`` non inventa mai un mercato.

NB: la **precedenza D1** ("il dizionario vince" sulla regola-colonna) è una scelta del
**runtime** (``custom_pipeline``), non di questo store: qui si risolve solo la frase.
"""

import re
from collections import namedtuple

from . import dizionario
from .custom_parser_engine import extract_between

# Chiave di config che ospita i profili di mappatura mercati.
_STORE_KEY = "market_mappings"

# Esito della risoluzione di un mercato da una frase.
#   status: "ok"        → match univoco; `market` = {market_type, market_name, selection_name}
#           "ambiguous" → più frasi combaciano con mercati DIVERSI (fail-closed, D2); market=None
#           "none"      → nessuna frase combacia; market=None
MarketResolution = namedtuple("MarketResolution", ["status", "market"])


def _normalize_text(s) -> str:
    """Testo normalizzato per il confronto: spazi collassati + casefold (case-insensitive)."""
    return re.sub(r"\s+", " ", str(s or "")).strip().casefold()


def _store(cfg: dict) -> dict:
    """Sezione ``market_mappings`` della config (dict vuoto se assente/malformata)."""
    raw = (cfg or {}).get(_STORE_KEY, {})
    return raw if isinstance(raw, dict) else {}


def _norm_profile_name(name) -> str:
    """Nome profilo normalizzato per il confronto: stringa ripulita (strip)."""
    return str(name or "").strip()


def _find_store_key(store: dict, name: str):
    """Chiave REALE in ``store`` che corrisponde a ``name`` una volta normalizzata, o
    ``None``. Serve a ritrovare profili salvati con spazi attorno al nome (``config.json``
    legacy/editato a mano), che ``profile_names`` mostra già ripuliti: senza, lookup/CRUD
    mancherebbero il profilo o creerebbero un doppione, disabilitando in silenzio la
    mappatura per quel profilo (CodeRabbit). Compatibilità con vecchie config preservata."""
    target = _norm_profile_name(name)
    if not target:
        return None
    for k in store:
        if _norm_profile_name(k) == target:
            return k
    return None


def _clean_entry(entry) -> dict:
    """Normalizza una voce in ``{start_after, end_before, phrase, market_type, market_name,
    selection_name}`` (stringhe ripulite), o ``None`` se inutile.

    Una voce è valida se ha **testo mercato** (``phrase``), **market_name** e
    **selection_name**: senza, non può formare un mercato. I delimitatori ``start_after``/
    ``end_before`` sono **facoltativi a livello dati**: così una config vecchia senza
    delimitatori NON viene cancellata al load/save (niente perdita dati, CodeRabbit). È
    ``resolve_market`` a **non applicare** una voce senza delimitatori — la **salta**
    (fail-closed) — invece di eliminarla. Dei delimitatori si tolgono **solo spazi/tab** ai
    bordi (come ``_delim_pattern`` del Parser), **preservando i newline** (es. ``"\\nMercato:"``
    resta ancorato a inizio riga, Codex). ``market_type`` può essere vuoto (lo ricava
    ``_canonical_market`` dal catalogo)."""
    if not isinstance(entry, dict):
        return None
    start_after = str(entry.get("start_after", "") or "").strip(" \t")
    end_before = str(entry.get("end_before", "") or "").strip(" \t")
    phrase = str(entry.get("phrase", "") or "").strip()
    market_type = str(entry.get("market_type", "") or "").strip()
    market_name = str(entry.get("market_name", "") or "").strip()
    selection_name = str(entry.get("selection_name", "") or "").strip()
    if not phrase or not market_name or not selection_name:
        return None
    return {"start_after": start_after, "end_before": end_before, "phrase": phrase,
            "market_type": market_type, "market_name": market_name,
            "selection_name": selection_name}


def profile_names(cfg: dict) -> list:
    """Nomi dei profili mercati salvati, ordinati (case-insensitive). Per la GUI."""
    names = [str(k).strip() for k in _store(cfg).keys() if str(k).strip()]
    return sorted(names, key=str.casefold)


def get_entries(cfg: dict, name: str) -> list:
    """Voci (ripulite) di un profilo, nell'ordine salvato. Profilo assente → ``[]``.
    Le voci vuote/incomplete vengono filtrate, così il resolver non itera su rumore."""
    store = _store(cfg)
    key = _find_store_key(store, name)
    rows = store.get(key, []) if key is not None else []
    if not isinstance(rows, (list, tuple)):
        return []
    out = []
    for e in rows:
        ce = _clean_entry(e)
        if ce is not None:
            out.append(ce)
    return out


def entries_for_profiles(cfg: dict, names) -> list:
    """Lista di liste-di-voci per i profili indicati (ordine preservato): è la forma
    attesa da ``resolve_market``. Un profilo mancante contribuisce con ``[]``."""
    return [get_entries(cfg, n) for n in (names or []) if str(n or "").strip()]


def set_entries(cfg: dict, name: str, entries) -> dict:
    """Copia di ``cfg`` con il profilo ``name`` impostato/sostituito da ``entries``
    (ripulite). Nome vuoto → config invariata. Crea il profilo se non esiste."""
    out = dict(cfg or {})
    nm = _norm_profile_name(name)
    if not nm:
        return out
    store = dict(_store(out))
    existing = _find_store_key(store, nm)
    if existing is not None and existing != nm:
        store.pop(existing)   # migra una chiave legacy con spazi al nome normalizzato (no doppioni)
    store[nm] = [ce for ce in (_clean_entry(e) for e in (entries or [])) if ce is not None]
    out[_STORE_KEY] = store
    return out


def add_profile(cfg: dict, name: str) -> dict:
    """Copia di ``cfg`` con un profilo vuoto ``name`` (no-op se esiste già o nome
    vuoto): la creazione non deve mai cancellare le voci di un profilo omonimo."""
    out = dict(cfg or {})
    nm = _norm_profile_name(name)
    store = dict(_store(out))
    if nm and _find_store_key(store, nm) is None:
        store[nm] = []
    out[_STORE_KEY] = store
    return out


def delete_profile(cfg: dict, name: str) -> dict:
    """Copia di ``cfg`` senza il profilo ``name`` (idempotente)."""
    out = dict(cfg or {})
    nm = _norm_profile_name(name)
    store = {k: v for k, v in _store(out).items() if _norm_profile_name(k) != nm}
    out[_STORE_KEY] = store
    return out


def rename_profile(cfg: dict, old: str, new: str) -> dict:
    """Copia di ``cfg`` con il profilo ``old`` rinominato ``new`` (conserva le voci).
    No-op se ``old`` non esiste, ``new`` è vuoto, o ``new`` esiste già (non si
    sovrascrive in silenzio un altro profilo)."""
    out = dict(cfg or {})
    o = _norm_profile_name(old)
    n = _norm_profile_name(new)
    store = dict(_store(out))
    old_key = _find_store_key(store, o)
    new_key = _find_store_key(store, n)
    if o == n or old_key is None or not n or new_key is not None:
        return out
    store[n] = store.pop(old_key)
    out[_STORE_KEY] = store
    return out


def _canonical_market(market_name: str, selection_name: str, rows=None):
    """Risolve ``(market_name, selection_name)`` del config nella tupla **canonica** del
    Catalogo XTrader ``{market_type, market_name, selection_name}``, o ``None`` se la
    coppia non è valida.

    Validazione + canonicalizzazione safety-critical (design §5.3): il match col catalogo è
    case/spazio-insensitive (``dizionario.normalize``), ma ciò che si ritorna — e che il
    runtime scriverà nel CSV — sono **sempre i valori canonici del catalogo** (MarketType,
    MarketName, SelectionName), non quelli grezzi del config: così una config editata a
    mano con case/spazi diversi (o un ``market_type`` stantio) non produce mai una tupla che
    XTrader non riconosce. Mercato **fisso** + selezione **non dinamica** (Codex). ``rows``
    inietta un catalogo nei test; di default usa quello reale."""
    mn = str(market_name or "").strip()
    sn = str(selection_name or "").strip()
    if not mn or not sn:
        return None
    nmn = dizionario.normalize(mn)
    nsn = dizionario.normalize(sn)
    canon_market = next((m for m in dizionario.market_names(rows=rows, fixed_only=True)
                         if dizionario.normalize(m) == nmn), None)
    if canon_market is None:
        return None
    for s in dizionario.selections_for_market(canon_market, rows):
        if s.get("dynamic") or not s.get("SelectionName"):
            continue
        if dizionario.normalize(s["SelectionName"]) == nsn:
            mtype = dizionario.market_type_for_name(canon_market, rows) or ""
            return {"market_type": mtype, "market_name": canon_market,
                    "selection_name": s["SelectionName"]}
    return None


def _phrase_in_text(phrase: str, text_norm: str) -> bool:
    """``True`` se ``phrase`` compare in ``text_norm`` (già normalizzato) come
    sottostringa su **confini di token**. I lookaround escludono dai confini sia i
    caratteri di parola (``\\w``) sia ``/`` e ``-``: così "over" non combacia dentro
    "overflow" **e** una frase corta come "x" non combacia dentro codici tipo "1/x" o
    "1-x" (HT/FT), evitando falsi positivi che imposterebbero il mercato sbagliato
    (Codex). Funziona comunque con frasi che finiscono con cifre/punteggiatura
    (es. "over 2.5" seguito da spazio o "!")."""
    p = _normalize_text(phrase)
    if not p:
        return False
    return re.search(r"(?<![\w/-])" + re.escape(p) + r"(?![\w/-])", text_norm) is not None


def resolve_market(text: str, profiles, rows=None) -> MarketResolution:
    """Risolve il mercato canonico XTrader dal mercato scritto dal provider nel ``text``.

    Per ogni voce il mercato si legge **da una posizione precisa** del messaggio: si estrae
    il testo tra i delimitatori ``start_after``/``end_before`` (stesso motore del Parser,
    ``extract_between``) e si verifica che il **testo mercato** (``phrase``) compaia in
    quel campo estratto (case-insensitive, a confini di token). Così un banner/menu altrove
    nel messaggio non crea falsi match (es. ``30/0,5HT/1,5HT/1`` non viene letto se il
    mercato vero sta tra «Quota» e «Prematch»). La coppia Mercato/Selezione dev'essere
    **coerente col Catalogo XTrader** (``_canonical_market``, §5.3): una voce incoerente
    (config a mano/bug) è ignorata, mai scritta. ``rows`` inietta un catalogo nei test.
    Poi (D2):

    - 0 match → ``MarketResolution("none", None)``;
    - match che indicano **lo stesso** ``(market_type, market_name, selection_name)``
      → ``MarketResolution("ok", {...})``;
    - match che indicano mercati **diversi** → ``MarketResolution("ambiguous", None)``
      (fail-closed: il chiamante non scrive nulla, niente mercato a caso).
    """
    if not str(text or "").strip():
        return MarketResolution("none", None)
    found = []
    for entries in (profiles or []):
        for e in entries:
            sa = str(e.get("start_after", "") or "")
            eb = str(e.get("end_before", "") or "")
            ph = str(e.get("phrase", "") or "").strip()
            # Difesa ANCHE sul percorso runtime (i profili possono arrivare grezzi, non solo
            # ripuliti da _clean_entry): una voce senza testo mercato o senza alcun
            # delimitatore è ignorata qui (non applicata, fail-closed), così non può MAI
            # combaciare su tutto il messaggio (Sourcery/CodeRabbit). "Vuoto" = solo spazi/tab
            # ai bordi, come `_delim_pattern`: un delimitatore di soli newline conta.
            if not ph or (not sa.strip(" \t") and not eb.strip(" \t")):
                continue
            # Leggi il mercato SOLO dalla posizione delimitata (niente scansione dell'intero
            # messaggio): i delimitatori RAW vanno a extract_between, che preserva i newline
            # (ancoraggio a inizio riga, Codex); poi il testo mercato si confronta sul campo
            # normalizzato. I delimitatori sono case-sensitive come nel Parser.
            region = extract_between(text, sa, eb)
            if not region:
                continue
            if not _phrase_in_text(e.get("phrase", ""), _normalize_text(region)):
                continue
            # Risolvi nella tupla CANONICA del catalogo (type+nomi esatti, ignorando i
            # valori grezzi del config): una coppia incoerente → None → IGNORATA, mai
            # scritta; una coppia valida ma non-canonica (case/spazi) → valori canonici,
            # così XTrader riconosce sempre la tupla (design §5.3, Codex).
            canon = _canonical_market(e.get("market_name", ""), e.get("selection_name", ""), rows)
            if canon is None:
                continue
            found.append((canon["market_type"], canon["market_name"], canon["selection_name"]))
    if not found:
        return MarketResolution("none", None)
    if len(set(found)) > 1:
        return MarketResolution("ambiguous", None)
    mt, mn, sn = found[0]
    return MarketResolution("ok", {"market_type": mt, "market_name": mn,
                                   "selection_name": sn})
