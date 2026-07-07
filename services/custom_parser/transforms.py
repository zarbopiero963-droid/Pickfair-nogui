"""Trasformazioni configurabili del Parser Personalizzato (portato: CP-05).

Una *trasformazione* deriva un valore da quello estratto, quando il valore da
usare non è nel messaggio ma va calcolato. Esempio: dal punteggio ``"6-0"`` si
ricava la linea Over della somma gol → ``"Over 6,5"``.

Le regole (``FieldRule``) indicano la trasformazione nel campo ``transform``; il
motore la applica **dopo** l'estrazione e **prima** della value-map.

Sicurezza (fail-closed): trasformazione sconosciuta o input non interpretabile
⇒ stringa vuota, così un campo obbligatorio resta "Non pronto" e non si produce
un valore inventato sul percorso d'ordine.

Modulo **puro**: nessuna dipendenza esterna.
"""
from __future__ import annotations

import re

# Punteggio "X-Y" / "X:Y" / "X x Y" (con spazi opzionali).
_SCORE_RE = re.compile(r"^\s*(\d{1,3})\s*[-:x]\s*(\d{1,3})\s*$", re.IGNORECASE)

# Gol per lato oltre cui il punteggio è implausibile per una partita reale: un input
# come "999-999" (ben formato ma assurdo) non deve generare una linea Over inventata.
_MAX_GOALS_PER_SIDE = 30
# Cap anche sulla SOMMA: "30-30" sta nel limite per-lato ma dà un totale assurdo
# ("Over 60,5"). Senza questo cap si produrrebbe una linea inventata.
_MAX_GOALS_TOTAL = 30


def _score_to_over(value: str) -> str:
    """Punteggio → linea Over della somma gol: "6-0" → "Over 6,5", "2-3" → "Over 5,5".

    Input non interpretabile come punteggio ⇒ ``""`` (fail-closed). Anche un punteggio
    ben formato ma **implausibile** ⇒ ``""`` invece di una linea Over assurda: sia un
    lato oltre ``_MAX_GOALS_PER_SIDE`` (es. "999-999"), sia una **somma** oltre
    ``_MAX_GOALS_TOTAL`` (es. "30-30" → totale 60).
    """
    m = _SCORE_RE.match(value or "")
    if not m:
        return ""
    home, away = int(m.group(1)), int(m.group(2))
    if home > _MAX_GOALS_PER_SIDE or away > _MAX_GOALS_PER_SIDE:
        return ""
    if home + away > _MAX_GOALS_TOTAL:
        return ""
    return f"Over {home + away},5"


# Registro delle trasformazioni disponibili (per il menu del costruttore, stadio GUI).
_TRANSFORMS = {
    "score_to_over": _score_to_over,
}


def available_transforms() -> list:
    """Nomi delle trasformazioni disponibili (ordinati)."""
    return sorted(_TRANSFORMS)


def has_transform(name: str) -> bool:
    """True se ``name`` è una trasformazione nota."""
    return name in _TRANSFORMS


def apply(value: str, name: str) -> str:
    """Applica la trasformazione ``name`` a ``value``.

    Sicuro per default: trasformazione sconosciuta ⇒ ``""`` (→ "Non pronto" se il
    campo è obbligatorio). Mai propagare un valore non trasformato a caso.
    """
    fn = _TRANSFORMS.get(name)
    if fn is None:
        return ""
    return fn(value)
