"""Loader del dizionario XTrader (PR-07).

Il dizionario (`data/dizionario_xtrader.csv`) è il "traduttore" tra gli alias dei
segnali Telegram e i valori esatti che XTrader si aspetta (MarketType, MarketName,
SelectionName, Handicap, BetType). Basato sui dati reali forniti dal team XTrader.

PR-07 fornisce solo caricamento e validazione strutturale; il lookup vero e
proprio (alias → riga) e l'integrazione in `build_csv_row` sono PR-08.
"""

import csv
import functools
import os
import re
import sys


def resource_path(relative: str) -> str:
    """Percorso assoluto di un asset **read-only impacchettato** (es. `data/…`), corretto in
    TUTTE le forme di distribuzione: run da **sorgente**, EXE **PyInstaller** ed EXE **Nuitka**.

    L'ordine dei rami è deliberato (Nuitka PRIMA di `sys.frozen`):

    - **Nuitka** (`--standalone`/`--onefile`): si rileva col modo RACCOMANDATO dai doc ufficiali
      Nuitka, cioè l'attributo di modulo `__compiled__` — NON `sys.frozen`, che Nuitka *non*
      imposta di proposito (Nuitka User Manual: «Nuitka does *not* set sys.frozen unlike other
      tools because it usually triggers inferior code»). I dati inclusi con
      `--include-data-dir=./data=data` stanno relativi alla cartella del programma (standalone)
      o vengono spacchettati nella dir temporanea ACCANTO ai moduli (onefile): in entrambi i
      casi `__file__` (genitore del package) li risolve, IDENTICO al sorgente. Il gating
      esplicito su `__compiled__` è difesa-in-profondità: se un giorno qualcosa impostasse
      `sys.frozen` su un binario Nuitka, NON si deve cadere nel ramo PyInstaller
      (`_MEIPASS`/`dirname(executable)`) — in onefile `dirname(executable)` punta accanto
      all'EXE reale, dove i dati spacchettati NON sono → dizionario non trovato → CSV senza
      lookup alias.
    - **PyInstaller** (`--onefile`/`--onedir`): imposta `sys.frozen=True` e `sys._MEIPASS`
      (la cartella del bundle); `--add-data "src;dest"` finisce in `<_MEIPASS>/dest`. Qui
      `__file__` punta nel bundle temporaneo, quindi si DEVE usare `_MEIPASS` (fallback
      difensivo a `dirname(sys.executable)` per freezer che settano `frozen` senza `_MEIPASS`).
    - **Sorgente**: base = genitore del package, via `__file__`."""
    if "__compiled__" in globals():                    # Nuitka: rilevamento raccomandato
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    elif getattr(sys, "frozen", False):                # PyInstaller
        base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(sys.executable)))
    else:                                              # sorgente
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, relative)


def _data_dir() -> str:
    """Cartella `data/` degli asset impacchettati, via `resource_path` (source/PyInstaller/
    Nuitka-aware)."""
    return resource_path("data")


DIZIONARIO_PATH = os.path.join(_data_dir(), "dizionario_xtrader.csv")

EXPECTED_COLUMNS = [
    "Sport", "Periodo", "MarketAliasTelegram", "SelectionAliasTelegram",
    "MarketType_XTrader", "MarketName_XTrader", "SelectionRole",
    "SelectionName_XTrader", "Linea", "Handicap", "BetType_XTrader", "Lingua",
    "SelezioneDinamica", "MetodoConsigliato", "Stato", "Fonte",
    "EsempioEventName", "EsempioEventId", "EsempioMarketId",
    "EsempioSelectionId", "Note",
]


# WHITELIST (fail-CLOSED, review Fable 5/GPT-5.5 su #375) dei valori `Fonte` che marcano una riga
# VALIDATA contro un export XTrader reale (#311-2.2). Confronto `strip().casefold()`. È volutamente
# una whitelist, NON una blacklist del solo «Generato da schema»: una riga con `Fonte` **vuota,
# assente, con typo o con un'etichetta futura non prevista** NON conta come validata → il dizionario
# resta NON validato → il gate #311-2.3 NON apre `BOTH`. Meglio non aprire `BOTH` che aprirlo su una
# riga di provenienza ignota (il rischio è una scommessa su mercato/selezione errato in REALE).
VALIDATED_FONTI = frozenset({"export xtrader"})


def is_validated(rows=None) -> bool:
    """True se il dizionario è **pienamente validato** contro un export XTrader reale (#311-2.2):
    ha almeno una riga e **OGNI** riga ha `Fonte` nella whitelist `VALIDATED_FONTI` (fail-closed).

    Gate anti-doppia-scommessa per #311-2.3: il default `BOTH` (che fa accettare un segnale sugli ID
    risolti dal dizionario) va abilitato SOLO quando ci si può fidare di quegli ID; se anche una sola
    riga non è validata (provenienza non whitelistata), un match errato scriverebbe `MarketId`/
    `SelectionId` sbagliati → in modalità REALE una scommessa sul mercato/selezione errato (blocker
    Fugu Ultra su #374; whitelist per il blocker Fable 5 su #375).

    Fail-safe: dizionario assente/illeggibile/vuoto **o riga malformata** (es. non-dict, dove
    `.get` solleverebbe `AttributeError`) → **False** (non si dichiara «validato» ciò che non si
    può leggere o interpretare). L'intero corpo è dentro il `try`, non solo il load, così un
    chiamante diretto che passa `rows` sporchi resta fail-closed come il percorso dal file (review
    Fable 5 su #375). `rows` iniettabile per i test; assente → carica dal file."""
    try:
        rows = load_dizionario() if rows is None else rows
        if not rows:
            return False
        return all(str(r.get("Fonte", "")).strip().casefold() in VALIDATED_FONTI for r in rows)
    except Exception:   # noqa: BLE001 — assente/header rotto/riga non-dict → non validato (fail-safe)
        return False


def load_dizionario(path: str = DIZIONARIO_PATH) -> list:
    """Carica il dizionario come lista di dict (una per riga).

    Valida l'header contro ``EXPECTED_COLUMNS`` (audit C4): una colonna **rinominata o
    mancante** farebbe fallire in SILENZIO ogni mapping mercato (un `.get(col, "")` darebbe
    `""` → nessun match → segnali scartati senza spiegazione) oppure crashare con `KeyError`
    al primo `row[col]`. Il dizionario è la sorgente di verità di mercati/selezioni: un
    header sbagliato rende il bridge inutilizzabile, quindi va detto con un errore CHIARO al
    load invece di degradare di nascosto. Colonne EXTRA sono tollerate (forward-compat); solo
    quelle attese mancanti sono fatali."""
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        missing = [c for c in EXPECTED_COLUMNS if c not in header]
        if missing:
            raise ValueError(
                f"Dizionario XTrader con header non valido in {path}: colonne attese mancanti "
                f"{missing}. Ripristina/correggi l'intestazione del CSV (attese: {EXPECTED_COLUMNS}).")
        return list(reader)


def normalize(s: str) -> str:
    """Normalizzazione per il lookup (case/space-insensitive): minuscolo, trim e
    collasso degli spazi interni (es. "Over  0.5  HT" -> "over 0.5 ht").

    Funzione pubblica: fonte unica della normalizzazione, riusata anche dalle
    value-map (CP-03) per evitare implementazioni divergenti."""
    return " ".join(str(s).strip().lower().split())


_norm = normalize  # alias interno storico


def alias_key(market_alias: str, selection_alias: str) -> tuple:
    """Chiave normalizzata (case/space-insensitive) usata per il lookup (PR-08)."""
    return (_norm(market_alias), _norm(selection_alias))


def duplicate_alias_pairs(rows: list) -> list:
    """Coppie (MarketAliasTelegram, SelectionAliasTelegram) duplicate: devono
    essere zero, altrimenti il lookup sarebbe ambiguo. Le righe con alias vuoti
    vengono ignorate (non sono lookabili e non devono generare falsi duplicati)."""
    seen, dups = set(), []
    for row in rows:
        ma = str(row.get("MarketAliasTelegram", "")).strip()
        sa = str(row.get("SelectionAliasTelegram", "")).strip()
        if not ma or not sa:
            continue
        k = alias_key(ma, sa)
        if k in seen:
            dups.append(k)
        else:
            seen.add(k)
    return dups


def assert_no_duplicate_aliases(rows: list) -> None:
    """Solleva ``ValueError`` se ci sono coppie (MarketAlias, SelectionAlias) **duplicate**.

    Il lookup alias→riga sarebbe AMBIGUO: ogni consumer (indice o dict) terrebbe in silenzio
    l'ULTIMA riga → mercato/selezione SBAGLIATI = scommessa sbagliata. Guardia **condivisa**
    chiamata da TUTTI i percorsi che costruiscono un indice alias — `mapping._index` (legacy)
    **e** `value_maps.dizionario_value_maps` (live, custom parser) — così entrambi falliscono
    chiusi e rumorosi su un dizionario editato/corrotto (audit B3 / Codex). Il dizionario
    shippato ha 0 duplicati, quindi la produzione non cambia."""
    dups = duplicate_alias_pairs(rows)
    if dups:
        raise ValueError(
            "Dizionario alias ambiguo: coppie (MarketAlias, SelectionAlias) duplicate, "
            "lookup non deterministico → "
            + ", ".join(str(k) for k in sorted(set(map(str, dups))))
            + ". Correggi il CSV del dizionario (una sola riga per coppia).")


def market_types(rows: list) -> set:
    """Insieme dei ``MarketType_XTrader`` presenti nelle righe.

    Usa ``.get(...)`` come i fratelli del modulo (``market_catalog``, ``selections_for_market``):
    una riga **senza** la colonna (dizionario non validato, o un dict parziale passato dai test)
    degrada a valore assente invece di sollevare ``KeyError`` (#184 M9). I valori vuoti sono
    **esclusi**: non sono MarketType reali (coerente con ``market_catalog``, che salta un ``mt``
    vuoto). I valori sono strippati come negli altri lettori del dizionario."""
    out = set()
    for row in rows:
        mt = (row.get("MarketType_XTrader") or "").strip()
        if mt:
            out.add(mt)
    return out


# ── Catalogo per le tendine della GUI (A1) ──────────────────────────────────
# Funzioni PURE costruite sul dizionario reale: popolano i menù a tendina del
# Parser Personalizzato (scelta del VALORE FISSO della colonna) senza che la GUI
# conosca il formato del CSV/del dizionario. Mercati e selezioni sono restituiti
# nell'ordine del file: stabile e prevedibile per l'utente.

_PLACEHOLDER_RE = re.compile(r"\{[A-Z_]+\}")
# Valori della colonna SelezioneDinamica che marcano una riga come dinamica.
_DYNAMIC_FLAG = frozenset({"sì", "si", "yes", "true", "1"})


@functools.lru_cache(maxsize=1)
def _cached_rows() -> tuple:
    """Dizionario reale caricato UNA volta (il catalogo non cambia a runtime).
    `lru_cache` gestisce la cache, evitando stato mutabile a livello di modulo."""
    return tuple(load_dizionario())


def _rows(rows=None):
    """Righe del dizionario: quelle passate (mantiene le funzioni pure/testabili
    senza toccare il disco), oppure il file reale tenuto in cache."""
    return rows if rows is not None else _cached_rows()


def market_catalog(rows=None) -> list:
    """Elenco ordinato e SENZA duplicati dei mercati nell'ordine di prima comparsa:
    ``[{"MarketType": ..., "MarketName": ..., "dynamic": bool}, …]``. Popola la
    tendina dei mercati (l'utente sceglie il MarketName; il MarketType è accoppiato).

    ``dynamic=True`` marca un mercato il cui **MarketName contiene un placeholder**
    squadra (es. handicap TEAM_A_1 ``"{HOME_TEAM} +1"``): quel nome NON è un valore
    fisso sicuro: va completato con Home/Away (`fill_placeholders`) prima di finire
    nel CSV, altrimenti resterebbe un ``{HOME_TEAM}`` non risolto (Codex P2)."""
    seen, out = set(), []
    for r in _rows(rows):
        mt = (r.get("MarketType_XTrader") or "").strip()
        mn = (r.get("MarketName_XTrader") or "").strip()
        if not mt or mt in seen:
            continue
        seen.add(mt)
        out.append({"MarketType": mt, "MarketName": mn, "dynamic": has_placeholder(mn)})
    return out


def market_is_dynamic(market, rows=None) -> bool:
    """True se il MarketName del mercato (cercato per MarketType **o** MarketName)
    contiene un placeholder squadra → il nome va completato, non usato come fisso."""
    key = _norm(market)
    if not key:
        return False
    for m in market_catalog(rows):
        if _norm(m["MarketType"]) == key or _norm(m["MarketName"]) == key:
            return bool(m["dynamic"])
    return False


def market_names(rows=None, fixed_only=False) -> list:
    """I MarketName per una tendina.

    Con ``fixed_only=True`` esclude i mercati **dinamici** (MarketName con placeholder
    squadra, es. handicap ``"{HOME_TEAM} +1"``), lasciando solo nomi usabili come
    valore **fisso** — utile per una tendina che salva il nome così com'è. Con il
    default (``fixed_only=False``) li include **tutti**: i mercati handicap sono
    scommesse legittime, ma il loro nome va completato con Home/Away
    (`fill_placeholders`), non persistito grezzo — il flag `dynamic` lo segnala in
    `market_catalog()`, e `has_placeholder` impedisce a un nome non risolto di finire
    nel CSV a valle (Codex P2)."""
    return [m["MarketName"] for m in market_catalog(rows)
            if not (fixed_only and m["dynamic"])]


def market_name_for_type(market_type, rows=None):
    """MarketName accoppiato a un MarketType, o None se non esiste."""
    key = (market_type or "").strip()
    for m in market_catalog(rows):
        if m["MarketType"] == key:
            return m["MarketName"]
    return None


def market_type_for_name(market_name, rows=None):
    """MarketType accoppiato a un MarketName (case/space-insensitive), o None."""
    key = _norm(market_name)
    for m in market_catalog(rows):
        if _norm(m["MarketName"]) == key:
            return m["MarketType"]
    return None


def selections_for_market(market, rows=None) -> list:
    """Selezioni di un mercato (match per MarketType **o** MarketName,
    case/space-insensitive), nell'ordine del dizionario. Ogni voce è un dict con i
    campi utili a tendina e riga CSV. ``dynamic=True`` marca una selezione che
    contiene un placeholder squadra ({HOME_TEAM}/{AWAY_TEAM}): quel valore va
    completato dal parser (Home/Away), non scelto come fisso."""
    key = _norm(market)
    if not key:
        return []
    out = []
    for r in _rows(rows):
        mt = _norm(r.get("MarketType_XTrader"))
        mn = _norm(r.get("MarketName_XTrader"))
        if key not in (mt, mn):
            continue
        name = (r.get("SelectionName_XTrader") or "").strip()
        market_name = (r.get("MarketName_XTrader") or "").strip()
        # `dynamic`: la riga richiede Home/Away per essere risolta. Vero se il
        # placeholder è nella selezione **o** nel MarketName (es. handicap TEAM_A_1
        # "{HOME_TEAM} +1" con selezione statica "Pareggio") **o** se il dizionario
        # marca la riga come dinamica (SelezioneDinamica = "Sì"). Così un chiamante
        # che usa il catalogo per i valori "fissi" non lascia placeholder irrisolti
        # (Codex P2).
        selez_dyn = (r.get("SelezioneDinamica") or "").strip().lower() in _DYNAMIC_FLAG
        out.append({
            "SelectionName":          name,
            "SelectionRole":          (r.get("SelectionRole") or "").strip(),
            "MarketType":             (r.get("MarketType_XTrader") or "").strip(),
            "MarketName":             market_name,
            "MarketAliasTelegram":    (r.get("MarketAliasTelegram") or "").strip(),
            "SelectionAliasTelegram": (r.get("SelectionAliasTelegram") or "").strip(),
            "Linea":                  (r.get("Linea") or "").strip(),
            "Handicap":               ((r.get("Handicap") or "").strip() or "0"),
            "BetType":                (r.get("BetType_XTrader") or "").strip(),
            "SelezioneDinamica":      selez_dyn,
            "dynamic":                has_placeholder(name) or has_placeholder(market_name) or selez_dyn,
        })
    return out


def has_placeholder(value) -> bool:
    """True se nel valore restano placeholder non risolti (es. ``{HOME_TEAM}``)."""
    return bool(_PLACEHOLDER_RE.search(str(value)))


def compose_event_name(home, away) -> str:
    """Compone l'EventName nel formato del dizionario/XTrader: ``"Casa - Trasferta"``
    (separatore " - ", come negli esempi reali). Se manca una squadra ritorna
    l'altra (o stringa vuota), senza separatori penzolanti."""
    h = str(home or "").strip()
    a = str(away or "").strip()
    if h and a:
        return f"{h} - {a}"
    return h or a


def fill_placeholders(value, home="", away="") -> str:
    """Sostituisce i placeholder dinamici di un valore del dizionario:
    ``{HOME_TEAM}``/``{AWAY_TEAM}`` con le squadre e ``{EVENT_NAME}`` con l'EventName
    composto (solo se entrambe le squadre sono note). I placeholder privi di valore
    restano invariati: il chiamante può rilevarli con `has_placeholder` e scartare
    una selezione non completabile."""
    out = str(value)
    h = str(home or "").strip()
    a = str(away or "").strip()
    if h:
        out = out.replace("{HOME_TEAM}", h)
    if a:
        out = out.replace("{AWAY_TEAM}", a)
    if h and a:
        out = out.replace("{EVENT_NAME}", compose_event_name(h, a))
    return out
