"""F-03 (#586, K-7, CSH-04) — trailing stop sul P&L liquidabile della posizione.

Decisione owner K-7 (#426/#586 c.6504686680): configurabile, OFF di default, P&L liquidabile corretto (stessa funzione
del cashout, F-01), persistito in DB, restart-safe, coerente SIM/LIVE, integrato col cashout (F-04), protetto contro
quote assenti/stale e mercati sospesi (F-23). Scheda F-03: soglia mobile che segue il massimo del P&L liquidabile e non
torna indietro; esempio "profitto 10 -> 15 -> 12 con trailing 20 % => scatta a 12"; precedenza hard-stop > SL >
trailing > TP; picco persistito; un kill fra aggiornamento del picco e persistenza lascia al riavvio un picco <= ultimo
persistito, mai maggiore.

Modulo puro: nessun I/O. Regole (tutte in Decimal):
- ``pct`` in (0, 100] (stesso dominio di ``roserpina.trailing_stop_pct``, F-18); assente/invalido => trailing spento;
- il picco considera solo profitto GARANTITO e liquidabile per intero (il chiamante passa il P&L dell'esito peggiore
  sulle gambe di chiusura, ``covered``): nessun profitto inventato da un book che non chiude tutta la posizione;
- il trailing e' armato solo con picco > 0; soglia = picco x (1 - pct/100), al centesimo per difetto (mai sopra il
  valore esatto: lo scatto non anticipa);
- scatta quando il P&L corrente <= soglia (armato);
- il picco non scende mai: un valore persistito piu' alto di quello in memoria vince.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import ROUND_DOWN, Decimal, InvalidOperation
from typing import Any, Optional

CENT = Decimal("0.01")
HUNDRED = Decimal("100")
TRAILING_TRIGGERED = "TRAILING_TRIGGERED"
TRAILING_REASON = "TRAILING_STOP_REACHED"
PEAK_KEY_PREFIX = "risk.trailing_peak."
STATE_VERSION = 1


def _dec(value: Any) -> Optional[Decimal]:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return out if out.is_finite() else None


def trailing_pct(value: Any) -> Optional[Decimal]:
    """Percentuale valida in (0, 100] o ``None`` (trailing spento)."""
    pct = _dec(value)
    if pct is None or pct <= 0 or pct > HUNDRED:
        return None
    return pct


def threshold(peak: Decimal, pct: Decimal) -> Decimal:
    """Soglia di scatto: picco x (1 - pct/100), al centesimo per difetto."""
    return (peak * (HUNDRED - pct) / HUNDRED).quantize(CENT, rounding=ROUND_DOWN)


@dataclass(frozen=True)
class TrailingDecision:
    peak: Optional[Decimal]          # picco dopo questo tick (None = nessun profitto ancora)
    peak_raised: bool                # il picco e' salito: va persistito PRIMA di usarlo
    armed: bool
    threshold: Optional[Decimal]
    triggered: bool


def evaluate(prev_peak: Any, pnl: Any, pct: Any) -> Optional[TrailingDecision]:
    """Un tick del trailing. ``None`` = nessuna decisione (pct o P&L non validi: fail-closed, picco invariato)."""
    pct_d = trailing_pct(pct)
    pnl_d = _dec(pnl)
    if pct_d is None or pnl_d is None:
        return None
    prev = _dec(prev_peak)
    if prev is not None and prev <= 0:
        prev = None
    peak = prev
    raised = False
    if pnl_d > 0 and (peak is None or pnl_d.quantize(CENT, rounding=ROUND_DOWN) > peak):
        peak = pnl_d.quantize(CENT, rounding=ROUND_DOWN)
        raised = True
    if peak is None:
        return TrailingDecision(None, False, False, None, False)
    limit = threshold(peak, pct_d)
    # Un tick che alza il picco non puo' scattare: pnl >= picco > soglia per ogni pct in (0, 100].
    triggered = pnl_d <= limit
    return TrailingDecision(peak, raised, True, limit, triggered)


def state_record(peak: Decimal, pct: Decimal, at: float) -> str:
    return json.dumps({"v": STATE_VERSION, "peak": str(peak), "pct": str(pct), "at": at}, sort_keys=True)


def read_peak(raw: Any) -> tuple[Optional[Decimal], str]:
    """Picco persistito: ``(picco, "")``; ``(None, "")`` se assente; ``(None, motivo)`` se illeggibile (il chiamante non
    decide: fail-closed)."""
    if raw is None or raw == "":
        return None, ""
    try:
        data = json.loads(raw) if isinstance(raw, str) else None
    except ValueError:
        return None, "TRAILING_STATE_CORRUPT"
    if not isinstance(data, dict) or data.get("v") != STATE_VERSION:
        return None, "TRAILING_STATE_CORRUPT"
    peak = _dec(data.get("peak"))
    if peak is None or peak <= 0:
        return None, "TRAILING_STATE_CORRUPT"
    return peak, ""
