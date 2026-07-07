"""Value-map del Parser Personalizzato (portato: CP-03).

Una *value-map* traduce un valore grezzo estratto da un messaggio (un alias, es.
``"BACK"``, ``"punta"``) nel valore **canonico** atteso a valle (es. ``"BACK"``).
Le regole (``FieldRule``) indicano quale value-map usare nel campo ``value_map``;
il motore la applica dopo l'estrazione.

Adattamento per Pickfair rispetto al bridge:

- il built-in ``bettype`` mappa i sinonimi del lato in **terminologia Betfair
  BACK/LAY** (il bridge usava PUNTA/BANCA di XTrader);
- le value-map derivate dal **dizionario** XTrader (market/selection) **non** sono
  incluse in questo stadio: nasceranno dal catalogo Betfair / mappe Provider
  (stadio successivo). Qui restano solo i built-in e la costruzione da coppie.

Regole di sicurezza (invariate dal bridge):

- lookup **normalizzato** (case/spazi-insensibile);
- alias **ambiguo** (stesso alias → valori diversi) viene **scartato** dalla mappa:
  meglio "Non pronto" che indovinare un valore sbagliato;
- valore non mappato o mappa sconosciuta ⇒ stringa vuota: un obbligatorio resta
  "Non pronto" e non si produce un valore errato. Mai indovinare il lato di una
  scommessa (BACK/LAY) o una selezione.

Modulo **puro**: nessun I/O, nessuna lettura di file.
"""
from __future__ import annotations

import re

_INNER_WS = re.compile(r"\s+")


def _normalize(value) -> str:
    """Normalizza un valore per il lookup: minuscolo, spazi ai bordi rimossi, run di
    spazi interni collassati a uno, virgola decimale → punto (``"OVER 2,5"`` ≡
    ``"over 2.5"``). Superset sicuro delle grafie comuni dei messaggi Telegram.

    NB: la normalizzazione ricca del dizionario XTrader (rimozione suffisso "FT",
    sinonimi) è delegata allo stadio catalogo/Provider; qui basta un canonico
    stabile per i built-in e le mappe costruite da coppie.
    """
    s = str(value or "").strip().lower().replace(",", ".")
    return _INNER_WS.sub(" ", s)


# ── built-in: BetType (lato scommessa) ─────────────────────────────────────
# BACK / LAY (terminologia Betfair). Solo alias NON ambigui: niente monolettera
# come "B"/"P" (es. "B" potrebbe stare per Back ma anche Banca, e invertire il
# lato sarebbe catastrofico). "punta"/"banca" (grafie IT dei canali) sono mappate
# ai termini Betfair. Tutto il resto NON è mappato (→ vuoto → "Non pronto"): non
# si indovina mai il lato della scommessa.
_BETTYPE = {
    "back": "BACK", "punta": "BACK",
    "lay": "LAY", "banca": "LAY",
}

_BUILTIN = {
    "bettype": dict(_BETTYPE),
}


def _is_placeholder(value: str) -> bool:
    """Valore con placeholder dinamico non sostituito, es. ``"{HOME_TEAM}"``."""
    v = value or ""
    return "{" in v and "}" in v


def value_map_from_pairs(pairs) -> dict:
    """Costruisce una value-map normalizzata da coppie ``(alias, valore)``.

    - alias/valore vuoti ⇒ ignorati;
    - valore placeholder dinamico (``"{...}"``) ⇒ ignorato;
    - alias **ambiguo** (stesso alias normalizzato → valori diversi) ⇒ rimosso
      (non si indovina).

    Ritorna ``{alias_normalizzato: valore}``.
    """
    acc: dict = {}
    ambiguous: set = set()
    for alias, value in pairs:
        a = _normalize(alias)
        v = str(value).strip() if value is not None else ""
        if a == "" or v == "" or _is_placeholder(v):
            continue
        if a in acc and acc[a] != v:
            ambiguous.add(a)
        else:
            acc.setdefault(a, v)
    for a in ambiguous:
        acc.pop(a, None)
    return acc


def registry() -> dict:
    """Registro ``nome → value-map``: i built-in (es. ``bettype``).

    Copia difensiva per mappa, così un chiamante non può mutare i built-in
    condivisi.
    """
    return {name: dict(m) for name, m in _BUILTIN.items()}


def available_value_maps() -> list:
    """Nomi delle value-map disponibili (per il menu del costruttore, stadio GUI)."""
    return sorted(registry())


def resolve(value: str, map_name: str, reg: dict = None) -> str:
    """Traduce ``value`` tramite la value-map ``map_name``.

    Sicuro per default: mappa sconosciuta, valore vuoto o alias non mappato ⇒
    stringa vuota (→ un campo obbligatorio resta "Non pronto"). Mai pass-through
    di un valore non riconosciuto.
    """
    if reg is None:
        reg = registry()
    table = reg.get(map_name)
    if not table or not value:
        return ""
    return table.get(_normalize(value), "")
