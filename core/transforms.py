"""CP-05: trasformazioni configurabili del Parser Personalizzato.

Una *trasformazione* deriva un valore da quello estratto, quando il valore da
scrivere non è nel messaggio ma va calcolato. Esempio del proprietario: dal
punteggio "6-0" si ricava la linea Over della somma gol → "Over 6,5".

Le regole (`FieldRule`, CP-01) indicano la trasformazione nel campo `transform`;
il motore (CP-02) la applica DOPO l'estrazione e PRIMA della value-map (CP-03).

Sicurezza (fail-closed): trasformazione sconosciuta o input non interpretabile
→ stringa vuota, così un campo obbligatorio resta "Non pronto" e non si scrive
una riga CSV inventata.
"""

import re

# Punteggio "X-Y" / "X:Y" / "X x Y" (con spazi opzionali).
_SCORE_RE = re.compile(r"^\s*(\d{1,3})\s*[-:x]\s*(\d{1,3})\s*$", re.IGNORECASE)

# Gol per lato oltre cui il punteggio è implausibile per una partita reale: un input
# come "999-999" (ben formato ma assurdo) non deve generare una linea Over inventata (A5).
_MAX_GOALS_PER_SIDE = 30
# Cap anche sulla SOMMA: "30-30" sta nel limite per-lato ma dà un totale assurdo
# ("Over 60,5"). Il pipeline custom richiede solo SelectionName/price/event/bet non vuoti,
# quindi senza questo cap potrebbe scrivere una riga CSV con una linea inventata (Codex).
_MAX_GOALS_TOTAL = 30


def _score_to_over(value: str) -> str:
    """Punteggio → linea Over della somma gol: "6-0" → "Over 6,5", "2-3" → "Over 5,5".

    Input non interpretabile come punteggio → "" (fail-closed). Anche un punteggio ben
    formato ma **implausibile** → "" invece di una linea Over assurda (A5): sia un lato
    oltre `_MAX_GOALS_PER_SIDE` (es. "999-999"), sia una **somma** oltre
    `_MAX_GOALS_TOTAL` (es. "30-30" → totale 60)."""
    m = _SCORE_RE.match(value or "")
    if not m:
        return ""
    home, away = int(m.group(1)), int(m.group(2))
    if home > _MAX_GOALS_PER_SIDE or away > _MAX_GOALS_PER_SIDE:
        return ""
    if home + away > _MAX_GOALS_TOTAL:
        return ""
    return f"Over {home + away},5"


# Registro delle trasformazioni disponibili (per il menu del costruttore, CP-06).
_TRANSFORMS = {
    "score_to_over": _score_to_over,
}


def available_transforms() -> list:
    return sorted(_TRANSFORMS)


def has_transform(name: str) -> bool:
    return name in _TRANSFORMS


def apply(value: str, name: str) -> str:
    """Applica la trasformazione `name` a `value`.

    Sicuro per default: trasformazione sconosciuta → "" (→ "Non pronto" se il
    campo è obbligatorio). Mai propagare un valore non trasformato a caso."""
    fn = _TRANSFORMS.get(name)
    if fn is None:
        return ""
    return fn(value)
