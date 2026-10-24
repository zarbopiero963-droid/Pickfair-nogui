"""PF-STAKE-PRECISION-MICROSTAKE-01 — contratto comune della precisione dello stake (decisione owner 24/10/2026).

Ogni ordine si invia con uno stake rappresentabile in centesimi, ottenuto **per difetto** dallo stake calcolato:
mai un importo maggiore di quello calcolato dal Money Management, mai un minimo nuovo, nessuna scelta del percorso
(diretto o Microstake: la decide l'engine sull'importo normalizzato, come prima).

Rappresentazione: `Decimal` costruito dalla forma decimale piu' corta del float (`repr`), mai dal valore binario.
Le sole differenze **rappresentative** del float vengono eliminate prima del troncamento: un risultato come
`0.09999999999999999` (che vale 0,10 in aritmetica esatta) torna 0,10 invece di diventare 0,09. La soglia e'
`REPRESENTATION_EPS` (1e-9 euro), molto sotto il centesimo: una differenza economica reale, anche di un millesimo di
centesimo (1e-5), non viene mai arrotondata verso l'alto.

Esempi del mandato: 0,10599 -> 0,10 · 0,50999 -> 0,50 · 1,96476 -> 1,96 · 2,00000 -> 2,00 · 3,08595 -> 3,08 ·
10,99999 -> 10,99. La funzione e' idempotente: normalizzare uno stake gia' normalizzato non lo cambia.
"""
from __future__ import annotations

import math
from decimal import ROUND_DOWN, ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Any, Dict

CENT = Decimal("0.01")
# Differenze sotto questa soglia sono artefatti binari del float, non importi: 1e-9 EUR.
REPRESENTATION_EPS = Decimal("0.000000001")

REASON_UNCHANGED = "gia_al_centesimo"
REASON_FLOORED = "troncato_al_centesimo"


def to_decimal(value: Any) -> Decimal:
    """`value` come Decimal finito. bool, NaN, inf, testo non numerico => ValueError (fail-closed)."""
    if isinstance(value, bool) or value is None:
        raise ValueError("STAKE_NOT_NUMERIC")
    if isinstance(value, Decimal):
        out = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("STAKE_NOT_FINITE")
        out = Decimal(repr(value))          # forma decimale piu' corta del float, non il valore binario
    else:
        try:
            out = Decimal(str(value).strip())
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("STAKE_NOT_NUMERIC") from exc
    if not out.is_finite():
        raise ValueError("STAKE_NOT_FINITE")
    return out


def normalize_stake(value: Any) -> Decimal:
    """Stake per difetto al centesimo. Negativo o non numerico => ValueError: nessuno stake inventato."""
    raw = to_decimal(value)
    if raw < 0:
        raise ValueError("STAKE_NEGATIVE")
    cleaned = raw.quantize(REPRESENTATION_EPS, rounding=ROUND_HALF_EVEN)
    return cleaned.quantize(CENT, rounding=ROUND_DOWN)


def is_cent_exact(value: Any) -> bool:
    """True se `value` e' gia' rappresentabile in centesimi (a meno degli artefatti binari del float)."""
    try:
        return normalize_stake(value) == to_decimal(value).quantize(REPRESENTATION_EPS, rounding=ROUND_HALF_EVEN)
    except ValueError:
        return False


def normalization_record(calculated: Any, normalized: Decimal) -> Dict[str, Any]:
    """Campi del registro per un ordine: stake calcolato, stake normalizzato e motivo (leggeri, senza I/O)."""
    try:
        changed = to_decimal(calculated).quantize(REPRESENTATION_EPS, rounding=ROUND_HALF_EVEN) != normalized
    except ValueError:
        changed = True
    return {
        "stake_calcolato": str(calculated) if not isinstance(calculated, float) else repr(calculated),
        "stake_normalizzato": str(normalized),
        "motivo_normalizzazione": REASON_FLOORED if changed else REASON_UNCHANGED,
    }
