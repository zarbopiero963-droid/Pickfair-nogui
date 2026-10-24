"""PF-STAKE-PRECISION-MICROSTAKE-01 / -FINAL-01 — contratto comune della precisione dello stake (decisioni owner
24/10/2026).

Ogni ordine si invia con uno stake rappresentabile in centesimi, ottenuto **per difetto** (ROUND_DOWN) dallo stake
economico: mai un importo maggiore, mai un minimo nuovo, nessuna scelta del percorso (diretto o Microstake: la
decide l'engine sull'importo finale, come prima).

**Nessuna tolleranza.** 1,9999999999 e' 1,99: un importo sotto la soglia non viene mai alzato. Gli errori binari del
float si correggono **alla fonte**: il money management calcola lo stake in `Decimal` sugli operandi della formula
(`RoserpinaMoneyManagement._base_stake_decimal`), cosi' 3,333...% non diventa 0,0999... e poi 0,09.

Un float che arriva da fuori si legge dalla sua forma decimale piu' corta (`repr`), mai dal valore binario:
0.1 e' 0,10 (non 0,1000000000000000055...).

Esempi del mandato: 0,10 -> 0,10 · 0,10599 -> 0,10 · 1,96476 -> 1,96 · 3,08595 -> 3,08 · 1,9999999999 -> 1,99 ·
2,00 -> 2,00 · 10,99999 -> 10,99. Idempotente: normalizzare uno stake gia' normalizzato non lo cambia.
"""
from __future__ import annotations

import math
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Dict

CENT = Decimal("0.01")
# Minimo del singolo ordine dell'exchange (Betfair: 2 EUR): sotto, l'engine usa il Microstake nativo. Solo per il
# registro dell'ordine; la soglia di instradamento resta quella dell'engine e della policy Microstake.
EXCHANGE_MIN_ORDER = Decimal("2.00")

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
    try:
        return raw.quantize(CENT, rounding=ROUND_DOWN)
    except InvalidOperation as exc:          # importo oltre la precisione del contesto (es. 1e308)
        raise ValueError("STAKE_NOT_REPRESENTABLE") from exc


def normalization_record(calculated: Any, normalized: Decimal) -> Dict[str, Any]:
    """Campi del registro per un ordine: stake calcolato, stake normalizzato e motivo (leggeri, senza I/O)."""
    try:
        exact = to_decimal(calculated)
        changed = exact != normalized
        shown = format(exact.normalize(), "f") if changed else str(normalized)
    except ValueError:
        changed, shown = True, str(calculated)
    return {
        "stake_calcolato": shown,
        "stake_normalizzato": str(normalized),
        "motivo_normalizzazione": REASON_FLOORED if changed else REASON_UNCHANGED,
    }
