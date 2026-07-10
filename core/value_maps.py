"""CP-03: value-map del Parser Personalizzato.

Una *value-map* traduce un valore grezzo estratto da un messaggio (un alias, es.
"BACK", "OVER 2.5", "GG") nel valore esatto che XTrader si aspetta (es. "PUNTA",
"Over 2,5 gol", "Sì"). Le regole (`FieldRule`, CP-01) indicano quale value-map
usare nel campo `value_map`; il motore (CP-02) la applica dopo l'estrazione.

Fonti delle value-map:
- **built-in**: `bettype` (BACK/LAY e sinonimi → PUNTA/BANCA), safety-critical.
- **dizionario**: `data/dizionario_xtrader.csv` (CP-07/08) esposto come mappe
  per-colonna selezionabili da menu a tendina nel costruttore (CP-06).

Regole di sicurezza:
- lookup **normalizzato** (case/spazi-insensibile), come il dizionario.
- alias **ambiguo** (stesso alias → valori diversi) viene **scartato** dalla
  mappa: meglio "Non pronto" che indovinare un valore sbagliato.
- valore non mappato o mappa sconosciuta → stringa vuota: un obbligatorio resta
  "Non pronto" e non si scrive una riga CSV errata. NON tradurre mai a caso un
  lato di scommessa (PUNTA/BANCA) o una selezione.
"""

from . import dizionario, mapping

# Normalizzazione del lookup: usa quella shorthand di `mapping` (minuscolo,
# spazi, virgola→punto, suffisso FT rimosso). È un superset sicuro della
# normalizzazione del dizionario, così le grafie comuni dei messaggi Telegram
# ("OVER 2,5", "OVER 2.5 FT") combaciano con le mappe; per gli alias interni del
# dizionario (con underscore) non cambia nulla.
_norm = mapping.normalize_shorthand


# ── built-in: BetType (lato scommessa) ─────────────────────────────────────
# PUNTA = back, BANCA = lay. Solo alias NON ambigui: niente monolettera come
# "B"/"P" (es. "B" potrebbe stare per Back→PUNTA ma anche Banca→BANCA, e
# invertire il lato sarebbe catastrofico). Tutto il resto NON è mappato
# (→ vuoto → "Non pronto"): non si indovina mai il lato della scommessa.
_BETTYPE = {
    "back": "PUNTA", "punta": "PUNTA",
    "lay": "BANCA", "banca": "BANCA",
}

_BUILTIN = {
    "bettype": dict(_BETTYPE),
}


def _is_placeholder(value: str) -> bool:
    """Valore con placeholder dinamico non sostituito, es. "{HOME_TEAM}".

    #318 L2-2: usa `or`, non `and`. Un placeholder PARZIALE/troncato («{HOME_TEAM» senza `}`,
    o «HOME_TEAM}») deve comunque essere escluso dalla value-map: i valori betting reali non
    contengono MAI graffe, quindi la presenza di UNA sola graffa è già segno di placeholder
    non sostituito (fail-closed: mai un placeholder rotto usato come valore reale)."""
    v = value or ""
    return "{" in v or "}" in v


def value_map_from_pairs(pairs) -> dict:
    """Costruisce una value-map normalizzata da coppie (alias, valore).

    - alias/valore vuoti → ignorati;
    - alias **ambiguo** (stesso alias normalizzato → valori diversi) → rimosso
      (non si indovina). Ritorna `{alias_normalizzato: valore}`.
    """
    acc = {}
    ambiguous = set()
    for alias, value in pairs:
        a = _norm(alias)
        v = str(value).strip() if value is not None else ""
        if a == "" or v == "":
            continue
        if a in acc and acc[a] != v:
            ambiguous.add(a)
        else:
            acc.setdefault(a, v)
    for a in ambiguous:
        acc.pop(a, None)
    return acc


# Colonne del dizionario esposte come value-map per-campo (alias → valore XTrader).
_DIZIONARIO_MAPS = {
    "markettype":    ("MarketAliasTelegram", "MarketType_XTrader"),
    "marketname":    ("MarketAliasTelegram", "MarketName_XTrader"),
    "selectionname": ("SelectionAliasTelegram", "SelectionName_XTrader"),
}


def _shorthand_rows(rows) -> list:
    """Per ogni forma breve Telegram (`mapping.SYNONYMS`, es. "gg", "over 2.5",
    "1") trova la riga corrispondente del dizionario. Così le value-map
    riconoscono anche gli shorthand dei messaggi, non solo gli alias interni del
    dizionario. Ritorna coppie (shorthand_normalizzato, riga_dizionario)."""
    index = {
        dizionario.alias_key(r.get("MarketAliasTelegram", ""), r.get("SelectionAliasTelegram", "")): r
        for r in rows
    }
    out = []
    for short_key, (market_alias, selection_alias) in mapping.SYNONYMS.items():
        row = index.get(dizionario.alias_key(market_alias, selection_alias))
        if row is not None:
            out.append((short_key, row))
    return out


def dizionario_value_maps(rows=None) -> dict:
    """Costruisce le value-map derivate dal dizionario (una per colonna utile).

    Le mappe sono chiavate sia sugli **alias interni** del dizionario sia sugli
    **shorthand Telegram** (`mapping.SYNONYMS`), così un parser che estrae "GG" o
    "OVER 2.5" da un messaggio trova comunque il valore XTrader.

    `rows` opzionale (lista di dict come da `dizionario.load_dizionario`); se
    assente carica il dizionario ufficiale. Alias ambigui scartati; valori
    placeholder dinamici ("{HOME_TEAM}"...) esclusi (→ "Non pronto" finché non
    sostituiti col match)."""
    if rows is None:
        rows = dizionario.load_dizionario()
    # Stessa guardia fail-closed del path legacy (audit B3/Codex): un dizionario con coppie
    # (MarketAlias, SelectionAlias) duplicate renderebbe ambiguo anche `_shorthand_rows`
    # (last-wins), mappando uno shorthand al mercato/selezione SBAGLIATI sul percorso LIVE.
    dizionario.assert_no_duplicate_aliases(rows)
    shorthand = _shorthand_rows(rows)
    maps = {}
    for name, (alias_col, value_col) in _DIZIONARIO_MAPS.items():
        pairs = []
        for r in rows:
            v = r.get(value_col, "")
            if not _is_placeholder(v):
                pairs.append((r.get(alias_col, ""), v))
        for short_key, row in shorthand:
            v = row.get(value_col, "")
            if not _is_placeholder(v):
                pairs.append((short_key, v))
        maps[name] = value_map_from_pairs(pairs)
    return maps


def registry(include_dizionario: bool = False, rows=None) -> dict:
    """Registro `nome → value-map`. Sempre i built-in; opzionalmente le mappe
    derivate dal dizionario (lettura CSV, quindi off di default per i test/uso
    offline)."""
    reg = {name: dict(m) for name, m in _BUILTIN.items()}
    if include_dizionario:
        reg.update(dizionario_value_maps(rows))
    return reg


def available_value_maps(include_dizionario: bool = False, rows=None) -> list:
    """Nomi delle value-map disponibili (per il menu a tendina del costruttore)."""
    return sorted(registry(include_dizionario=include_dizionario, rows=rows))


def resolve(value: str, map_name: str, reg: dict = None) -> str:
    """Traduce `value` tramite la value-map `map_name`.

    Sicuro per default: mappa sconosciuta, valore vuoto o alias non mappato →
    stringa vuota (→ un campo obbligatorio resta "Non pronto"). Mai pass-through
    di un valore non riconosciuto."""
    if reg is None:
        reg = registry()
    table = reg.get(map_name)
    if not table or not value:
        return ""
    return table.get(_norm(value), "")
