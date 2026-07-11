"""Dizionario di mappatura nomi squadra: alias del provider → nome Betfair/XTrader.

Un *provider* (canale Telegram) può scrivere le squadre con nomi diversi da quelli
che XTrader/Betfair si aspettano nell'``EventName`` (es. "Liverpool" vs "Liverpool",
"Inter" vs "Internazionale", abbreviazioni, lingue diverse). Questo modulo tiene
**profili di mappatura** definiti dall'utente e li applica all'``EventName`` prima
della scrittura, così l'evento combacia col name-matching di XTrader.

Modello dati (config, chiave ``name_mappings``)::

    cfg["name_mappings"] = {
        "<nome profilo>": [
            {"country": "Inghilterra", "betfair": "Liverpool", "provider": "Liverpool FC",
             "sport": "Calcio", "entity_type": "team"},
            ...
        ],
        ...
    }

Entrambe le colonne sono **campo libero** (le riempie l'utente): ``betfair`` è il
nome canonico XTrader/Betfair (anche l'output della mappatura), ``provider`` è
l'alias usato nei messaggi del canale. ``country`` è solo organizzativo (opz.).
``sport`` (PR-P10) ed ``entity_type`` (PR-P10 / #178 §2) sono filtri opzionali per
riga (vedi `ENTITY_TYPES`): vuoti = agnostici, così le config salvate prima di questi
campi restano valide.

Logica PURA su un ``dict`` di config: nessuna GUI, nessun I/O — la persistenza è
del chiamante (``config_store.save_config``), come per ``provider_store``. Le
funzioni di modifica ritornano una COPIA della config, non mutano l'originale.

Regole di sicurezza (safety-critical: un evento sbagliato = scommessa sbagliata):
- lookup **normalizzato** (case/spazi-insensibile), come il dizionario XTrader;
- **fail-closed**: un nome non risolvibile NON viene tradotto a caso → il chiamante
  ottiene ``None`` e scarta il segnale (nessuna riga CSV);
- multi-profilo: i profili selezionati si applicano nell'ordine dato e vince la
  **prima** corrispondenza (deterministico in caso di conflitto fra profili).
"""

import re

import logging

from . import sports
from .dizionario import compose_event_name, normalize

_LOG = logging.getLogger(__name__)

# Coppie (campo, valore troncato) già segnalate a log: il resolver gira in hot path
# (una risoluzione per messaggio) e una riga malformata non deve riempire il log con
# lo stesso warning a ogni evento (stesso pattern anti-flooding di `source_manager`).
_WARNED_MALFORMED = set()
_WARNED_CAP = 256


def _reset_warnings() -> None:
    """Svuota il dedup dei warning (per i test)."""
    _WARNED_MALFORMED.clear()


def _warn_malformed(field: str, value) -> None:
    """Segnala UNA volta (per campo+valore) una riga di mappatura scartata perché
    `sport`/`entity_type` non è riconosciuto (fail-closed, audit #259 B4)."""
    shown = ascii(value)
    if len(shown) > 60:
        shown = shown[:57] + "..."
    key = (field, hash(shown))
    if key in _WARNED_MALFORMED or len(_WARNED_MALFORMED) >= _WARNED_CAP:
        return
    _WARNED_MALFORMED.add(key)
    _LOG.warning(
        "name_mappings: %s=%s non riconosciuto -> riga di mappatura IGNORATA "
        "(fail-closed, #259 B4): correggi il valore per riattivarla.", field, shown)

# Chiave di config che ospita i profili di mappatura.
_STORE_KEY = "name_mappings"

# Tassonomia del tipo di entità mappata (issue #86 PR-P10 / #178 §2). Una riga può
# dichiarare COSA mappa, così un alias di "competition" non scavalca un nome squadra e
# si possono esprimere anche player/competition (non solo team/event). "" = agnostico
# (vale per ogni tipo), retro-compatibile con le righe salvate prima di questo campo.
ENTITY_TYPES = ("participant", "team", "player", "competition", "market", "selection")

# Tipi "partecipante" usati per risolvere un nome di squadra/giocatore nell'EventName.
# Il flusso live (custom_pipeline) restringe la mappatura dell'EventName a QUESTI tipi (più
# le righe agnostiche), così una riga di tipo competition/market/selection con un alias
# che collide NON traduce un partecipante dell'evento (issue #178 §2, Codex P1).
PARTICIPANT_ENTITY_TYPES = ("participant", "team", "player")


def normalize_entity_type(value) -> str:
    """Normalizza un tipo di entità a uno di ``ENTITY_TYPES`` (case-insensitive) oppure
    ``""`` (agnostico) se vuoto/ignoto. Fail-safe: un valore non riconosciuto NON sceglie
    un tipo a caso, diventa agnostico."""
    v = str(value or "").strip().casefold()
    return v if v in ENTITY_TYPES else ""


def _entity_filter(want_entity):
    """Normalizza ``want_entity`` (str, iterabile di str, o falsy) nell'insieme dei tipi
    AMMESSI (frozenset) oppure ``None`` = nessun filtro. I valori ignoti sono scartati;
    un insieme che si svuota → ``None`` (nessun filtro, non un filtro che blocca tutto)."""
    if not want_entity:
        return None
    if isinstance(want_entity, str):
        v = normalize_entity_type(want_entity)
        return frozenset({v}) if v else None
    allowed = {normalize_entity_type(x) for x in want_entity}
    allowed.discard("")
    return frozenset(allowed) if allowed else None


def _store(cfg: dict) -> dict:
    """Sezione ``name_mappings`` della config (dict vuoto se assente/malformata)."""
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
    mappatura nomi per quel profilo (audit L1, come ``market_mapping_store``). Compatibilità
    con vecchie config preservata."""
    target = _norm_profile_name(name)
    if not target:
        return None
    for k in store:
        if _norm_profile_name(k) == target:
            return k
    return None


def _malformed_fields(entry: dict) -> list:
    """Coppie ``(campo, valore_grezzo)`` NON riconosciute di una riga di mappatura:
    ``sport`` non in ``sports.SPORTS`` o ``entity_type`` non in ``ENTITY_TYPES``
    (vuoto = agnostico intenzionale, NON malformato). Predicato unico (audit #259 B4)
    condiviso tra `_clean_entry` (scarto fail-closed + log) e
    `malformed_entry_warnings` (avvisi GUI), così i due non possono divergere."""
    out = []
    raw_sport = str(entry.get("sport", "") or "").strip()
    if raw_sport and not sports.normalize_sport(raw_sport):
        out.append(("sport", raw_sport))
    raw_entity = str(entry.get("entity_type", "") or "").strip()
    if raw_entity and not normalize_entity_type(raw_entity):
        out.append(("entity_type", raw_entity))
    return out


def _clean_entry(entry) -> dict:
    """Normalizza una riga di mappatura in
    ``{country, betfair, provider, sport, entity_type}`` (stringhe ripulite), oppure
    ``None`` se la riga è vuota/non valida. Una riga senza né ``betfair`` né ``provider``
    è inutile (non mappa nulla) e viene scartata.

    ``sport`` (PR-P10) restringe la riga a uno sport (``sports.SPORTS``); **vuoto** →
    ``""`` = **agnostico** (vale per tutti gli sport, retro-compatibile con le righe
    salvate prima di P10). ``entity_type`` (PR-P10 / #178 §2) restringe la riga a un tipo
    di entità (``ENTITY_TYPES``); **vuoto** → ``""`` = agnostico.

    Un valore NON vuoto ma **non riconosciuto** (typo: ``"Calc1o"``) è invece FAIL-CLOSED
    (audit #259 B4, decisione proprietario): la riga viene **scartata** con warning, NON
    allargata ad agnostica — un typo non deve far applicare una mappatura pensata per uno
    sport/tipo a tutti gli altri (EventName sbagliato nel CSV). Le righe agnostiche
    INTENZIONALI (campo vuoto) restano valide e agnostiche."""
    if not isinstance(entry, dict):
        return None
    country = str(entry.get("country", "") or "").strip()
    betfair = str(entry.get("betfair", "") or "").strip()
    provider = str(entry.get("provider", "") or "").strip()
    if not betfair and not provider:
        return None
    bad = _malformed_fields(entry)
    if bad:
        for field, raw in bad:
            _warn_malformed(field, raw)
        return None
    return {"country": country, "betfair": betfair, "provider": provider,
            "sport": sports.normalize_sport(entry.get("sport")) or "",
            "entity_type": normalize_entity_type(entry.get("entity_type"))}


def profile_names(cfg: dict) -> list:
    """Nomi dei profili di mappatura salvati, ordinati (case-insensitive). Per le
    tendine/checkbox della GUI."""
    names = [str(k).strip() for k in _store(cfg).keys() if str(k).strip()]
    return sorted(names, key=str.casefold)


def get_entries(cfg: dict, name: str) -> list:
    """Righe (ripulite) di un profilo, nell'ordine salvato. Profilo assente → ``[]``.
    Le righe vuote vengono filtrate, così il resolver non itera su rumore."""
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


def malformed_entry_warnings(cfg: dict) -> list:
    """Avvisi **non bloccanti** per la GUI/event log (audit #259 B4): righe di
    mappatura con ``sport``/``entity_type`` non riconosciuto, che il resolver SCARTA
    (fail-closed). Il warning del logger Python di `_clean_entry` non è visibile
    nell'app windowed (stesso principio di `source_manager.malformed_enabled_warnings`,
    Codex P2 #309): `_start` mostra QUESTI messaggi nel log eventi, così l'operatore
    scopre subito la riga disattivata invece che dal nome non tradotto."""
    warnings = []
    for profile, rows in _store(cfg).items():
        if not isinstance(rows, (list, tuple)):
            continue
        for entry in rows:
            if not isinstance(entry, dict):
                continue
            betfair = str(entry.get("betfair", "") or "").strip()
            provider = str(entry.get("provider", "") or "").strip()
            if not betfair and not provider:
                continue                      # riga vuota: scartata comunque, senza avviso
            bad = _malformed_fields(entry)
            if bad:
                dove = ", ".join(f"{f}={v!r}" for f, v in bad)
                riga = betfair or provider
                warnings.append(
                    f"Mappatura nomi «{_norm_profile_name(profile)}», riga «{riga}»: "
                    f"{dove} non riconosciuto -> riga IGNORATA (fail-closed). "
                    f"Correggi il valore per riattivarla.")
    return warnings


def entries_for_profiles(cfg: dict, names) -> list:
    """Lista di liste-di-righe per i profili indicati (ordine preservato): è la
    forma attesa da `resolve_team`/`resolve_event_name`. Un profilo mancante
    contribuisce con ``[]`` (nessun match da lì → fail-closed a valle)."""
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
    vuoto): la creazione non deve mai cancellare le righe di un profilo omonimo."""
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
    """Copia di ``cfg`` con il profilo ``old`` rinominato ``new`` (conserva le righe).
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


def _entity_eligible(entry, allowed) -> bool:
    """Una riga è eleggibile se il suo ``entity_type`` è fra quelli ``allowed`` (insieme
    dei tipi richiesti) oppure se è **agnostica** (``entity_type`` vuoto, vale per ogni
    tipo). ``allowed`` ``None`` → nessun filtro (tutte eleggibili)."""
    if allowed is None:
        return True
    et = str(entry.get("entity_type", "") or "")
    return et in allowed or et == ""


def _scoped_entry_groups(entries, want_sport, want_entity=None):
    """Righe eleggibili per lo scope richiesto (sport + tipo di entità) **raggruppate per
    tier di priorità**, dal più specifico all'agnostico, dando priorità ai match esatti su
    ENTRAMBE le dimensioni (PR-P10, CodeRabbit + Codex).

    Ritorna una **lista di gruppi** (ogni gruppo = righe con lo stesso rango, nell'ordine
    salvato), coi gruppi ordinati dal tier più specifico a quello più agnostico:

    - si scartano le righe di un ALTRO ``entity_type`` (le agnostiche restano) e di un
      ALTRO sport (le agnostiche restano);
    - le rimanenti si raggruppano per rango: PRIMA il **tipo esatto** sull'agnostico, e a
      parità PRIMA lo sport esatto sull'agnostico; a parità di rango l'ordine salvato è
      preservato (sort stabile). ``want_sport``/``want_entity`` assenti → quella dimensione
      non influenza il rango.

    Il **tipo** è la dimensione PRIMARIA (Codex): un override tipizzato (`entity_type`
    valorizzato) vince anche su una riga legacy **sport-specifica ma senza tipo** salvata
    prima. Senza filtro tipo (`allowed is None`) il tipo non influenza il rango, quindi lo
    scoping per sport resta identico al comportamento legacy.

    Il chiamante (`resolve_team`) esaurisce un tier — **alias E canonico** — prima di
    scendere al successivo: così un alias **agnostico** non scavalca un canonico
    **esatto-sport** dello stesso nome (Codex P2 #174), e una riga agnostica salvata PRIMA
    non scavalca un override esatto salvato dopo (la GUI fa solo append). Senza alcun filtro
    c'è **un solo gruppo** nell'ordine salvato (comportamento legacy invariato)."""
    want = want_sport or ""
    allowed = _entity_filter(want_entity)
    pool = [e for e in entries
            if _entity_eligible(e, allowed)
            and (not want or str(e.get("sport", "") or "") in (want, ""))]
    if not want and allowed is None:
        return [pool] if pool else []         # nessun filtro → un solo gruppo (ordine salvato, legacy)

    def _rank(e):
        entity_rank = 0 if (allowed is None
                            or str(e.get("entity_type", "") or "") in allowed) else 1
        sport_rank = 0 if (not want or str(e.get("sport", "") or "") == want) else 1
        return (entity_rank, sport_rank)      # tipo PRIMARIO, poi sport (Codex)

    groups = {}
    for e in sorted(pool, key=_rank):         # sort STABILE: ordine salvato a parità di rango
        groups.setdefault(_rank(e), []).append(e)
    return [groups[rank] for rank in sorted(groups)]   # tier dal più specifico all'agnostico


def resolve_team(team: str, profiles, sport=None, entity_type=None) -> str:
    """Traduce un nome squadra grezzo nel nome Betfair/XTrader, o ``None`` se ignoto.

    ``profiles`` è una lista di liste-di-righe (vedi `entries_for_profiles`), nell'
    ordine di selezione. Strategia (deterministica, fail-closed): **il primo profilo
    vince**. Per ogni profilo, nell'ordine, si prova prima l'alias e poi il canonico,
    e solo se nessuno dei due combacia si passa al profilo successivo:

    1. **alias provider**: riga del profilo il cui ``provider`` combacia (normalizzato)
       → ritorna il suo ``betfair``;
    2. **nome canonico**: altrimenti riga del profilo il cui ``betfair`` combacia (il
       provider ha già mandato il nome canonico, o la riga non ha alias);
    3. nessun match in TUTTI i profili → ``None`` (non si indovina mai un nome squadra).

    ``sport`` (PR-P10): se valorizzato (uno fra ``sports.SPORTS``), si considerano SOLO
    le righe di quello sport o **agnostiche** (sport vuoto), con **priorità allo sport
    esatto** sulle agnostiche (vedi `_scoped_entry_groups`): un override per-sport non
    viene mai scavalcato da una riga agnostica salvata prima. Le righe taggate per un altro
    sport sono saltate. Sport assente/ignoto → nessun filtro (comportamento legacy).

    ``entity_type`` (PR-P10 / #178 §2): un singolo tipo (``ENTITY_TYPES``) **oppure un
    insieme** di tipi ammessi (es. ``PARTICIPANT_ENTITY_TYPES``). Si considerano SOLO le
    righe di quei tipi o agnostiche, saltando quelle di un altro tipo (così l'alias di una
    "competition" non traduce un nome squadra), con **priorità al tipo esatto** sulle
    agnostiche. Assente/ignoto → nessun filtro. È additivo allo scoping per sport.

    L'esaurire alias+canonico di un profilo prima del successivo evita che l'alias di
    un profilo più in basso scavalchi il canonico di uno più in alto (Codex)."""
    nt = normalize(team)
    if not nt:
        return None
    want = sports.normalize_sport(sport)
    for entries in profiles:
        # Si esaurisce un TIER di priorità (alias, poi canonico) PRIMA di scendere al tier
        # più agnostico: così un alias agnostico non scavalca un canonico esatto-sport dello
        # stesso nome (Codex P2 #174). Dentro il tier resta alias→canonico (l'alias del
        # provider ha precedenza sul nome canonico).
        for group in _scoped_entry_groups(entries, want, entity_type):
            for e in group:
                alias = e.get("provider", "")
                betfair = e.get("betfair", "")
                if alias and betfair and normalize(alias) == nt:
                    return betfair
            for e in group:
                betfair = e.get("betfair", "")
                if betfair and normalize(betfair) == nt:
                    return betfair
    return None


def split_event(event_name: str, separator: str):
    """Divide un ``EventName`` ("Casa <sep> Trasferta") in ``(casa, trasferta)``,
    o ``None`` se non si riesce a separarlo in due nomi non vuoti.

    Il separatore è **testo libero** configurato dall'utente (es. "v", "vs", "-",
    "/"). Si preferisce **sempre** il delimitatore **con spazi attorno**
    (``\\s+<sep>\\s+``): così non si spezza su punteggiatura/lettere interne al nome
    (es. "Paris Saint-Germain - Lyon" → "Paris Saint-Germain" / "Lyon", non sulla
    prima "-"). Solo per separatori **simbolici** ("-"/"/"), se la forma con spazi non
    c'è, si ripiega sulla forma **compatta** (``\\s*<sep>\\s*``, es. "Liverpool/Leeds");
    per i separatori **alfabetici** ("v"/"vs") non c'è fallback compatto, altrimenti
    "v" senza spazi spezzerebbe "Liverpool". Solo la prima occorrenza separa
    (``maxsplit=1``)."""
    name = str(event_name or "").strip()
    sep = str(separator or "").strip()
    if not name or not sep:
        return None
    esc = re.escape(sep)
    parts = re.compile(r"\s+" + esc + r"\s+", re.IGNORECASE).split(name, maxsplit=1)
    if len(parts) != 2 and not sep.isalpha():
        parts = re.compile(r"\s*" + esc + r"\s*").split(name, maxsplit=1)
    if len(parts) != 2:
        return None
    home, away = parts[0].strip(), parts[1].strip()
    if not home or not away:
        return None
    return home, away


def resolve_event_name(event_name: str, separator: str, profiles, sport=None,
                       entity_type=None) -> str:
    """Traduce un ``EventName`` provider in ``EventName`` Betfair/XTrader, o ``None``.

    Divide su ``separator``, mappa casa e trasferta coi ``profiles`` e ricompone nel
    formato XTrader "Casa - Trasferta" (`dizionario.compose_event_name`). Ritorna
    ``None`` (fail-closed: niente riga CSV) se non si riesce a dividere **o** se una
    delle due squadre non è mappabile.

    ``sport`` (PR-P10) ed ``entity_type`` (#178 §2) sono inoltrati a `resolve_team` per
    restringere la mappatura alle righe di quello sport/tipo o agnostiche. Le squadre di un
    evento sono partecipanti: il chiamante può passare ``entity_type`` per usare solo le
    righe pertinenti (default ``None`` = nessun filtro, comportamento legacy)."""
    split = split_event(event_name, separator)
    if split is None:
        return None
    home, away = split
    h = resolve_team(home, profiles, sport=sport, entity_type=entity_type)
    a = resolve_team(away, profiles, sport=sport, entity_type=entity_type)
    if not h or not a:
        return None
    return compose_event_name(h, a)
