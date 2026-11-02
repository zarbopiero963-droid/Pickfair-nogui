"""F-18 (#586, CFG-01/02/04, NF-16, NF-17/NEW-01): dominio delle chiavi Roserpina, unico per salvataggio e caricamento.

Un valore illeggibile o fuori dominio non diventa mai un default silenzioso:
- al salvataggio (``SettingsService.save_settings``) il salvataggio e' rifiutato per intero;
- al caricamento (DB legacy o corrotto) la chiave e' registrata come invalida e ogni nuova entrata e' negata.

Assente, ``None`` o ``""`` non e' un errore: per un parametro con default di codice vale il default, per un cap
opzionale significa "non attivo". Nessun cap monetario e' attivo di default (decisione owner 18/10/2026). Nessun minimo
monetario applicativo: ``min_stake`` deve solo essere > 0, cosi' 0,10 EUR (microstake, decisione owner 02/11/2026) e'
valido e non viene mai alzato.

I domini descrivono cio' che il runtime sa trattare, non preferenze: percentuali di rischio in (0, 100], soglie di
drawdown/espansione in [0, 100], commissione in
[0, 100), quote nella scala Betfair [1.01, 1000], moltiplicatori > 0, conteggi interi.
"""
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# kind -> (minimo, minimo incluso, massimo, massimo incluso); None = nessun limite da quel lato
_RANGES: Dict[str, Tuple[Optional[float], bool, Optional[float], bool]] = {
    "pct": (0.0, False, 100.0, True),
    "threshold_pct": (0.0, True, 100.0, True),
    "commission": (0.0, True, 100.0, False),
    "positive": (0.0, False, None, False),
    "non_negative": (0.0, True, None, False),
    "price": (1.01, True, 1000.0, True),
    "finite": (None, False, None, False),
}

#: chiave -> (tipo di valore, dominio). ``num`` = numero decimale, ``int`` = intero, ``bool``, ``enum``.
SPECS: Dict[str, Tuple[str, Any]] = {
    # percentuali di rischio obbligatorie (PFC-008) e drawdown hard-stop opzionale
    "roserpina.target_profit_cycle_pct": ("num", "pct"),
    "roserpina.max_single_bet_pct": ("num", "pct"),
    "roserpina.max_total_exposure_pct": ("num", "pct"),
    "roserpina.max_event_exposure_pct": ("num", "pct"),
    "roserpina.max_drawdown_hard_stop_pct": ("num", "pct"),
    # cap monetari opzionali: se presenti devono essere > 0
    "roserpina.max_daily_loss": ("num", "positive"),
    "roserpina.max_open_exposure": ("num", "positive"),
    "roserpina.max_order_exposure": ("num", "positive"),
    "roserpina.max_market_exposure": ("num", "positive"),
    "roserpina.max_event_exposure_abs": ("num", "positive"),
    "roserpina.max_drawdown_abs": ("num", "positive"),
    "roserpina.max_session_loss": ("num", "positive"),
    "roserpina.max_exposure_stop": ("num", "positive"),
    "roserpina.max_stake_abs": ("num", "positive"),
    "roserpina.max_win": ("num", "positive"),
    # cap del chase: <= 0 finito e' l'opt-out documentato (core/system_state.py), non un errore
    "roserpina.max_recovery_chase_abs": ("num", "finite"),
    # parametri con default di codice; le soglie di drawdown/espansione ammettono 0 ("a qualsiasi variazione")
    "roserpina.auto_reset_drawdown_pct": ("num", "threshold_pct"),
    "roserpina.defense_drawdown_pct": ("num", "threshold_pct"),
    "roserpina.lockdown_drawdown_pct": ("num", "threshold_pct"),
    "roserpina.expansion_profit_pct": ("num", "threshold_pct"),
    "roserpina.max_stake_pct": ("num", "pct"),
    "roserpina.expansion_multiplier": ("num", "positive"),
    "roserpina.defense_multiplier": ("num", "positive"),
    "roserpina.liquidity_multiplier": ("num", "positive"),
    "roserpina.commission_pct": ("num", "commission"),
    "roserpina.min_stake": ("num", "positive"),
    "roserpina.min_liquidity_absolute": ("num", "non_negative"),
    "roserpina.min_price": ("num", "price"),
    "roserpina.book_warning": ("num", "positive"),
    "roserpina.book_block": ("num", "positive"),
    "roserpina.profit_epsilon": ("num", "non_negative"),
    "roserpina.auto_green_delay_sec": ("num", "non_negative"),
    "roserpina.table_count": ("int", (1, 50)),
    "roserpina.max_recovery_tables": ("int", (0, 50)),
    "roserpina.allow_recovery": ("bool", None),
    "roserpina.anti_duplication_enabled": ("bool", None),
    "roserpina.liquidity_guard_enabled": ("bool", None),
    "roserpina.liquidity_warning_only": ("bool", None),
    "roserpina.max_win_warning_only": ("bool", None),
    "roserpina.profit_epsilon_enabled": ("bool", None),
    "roserpina.auto_green_delay_enabled": ("bool", None),
    "roserpina.risk_profile": ("enum", ("CONSERVATIVE", "BALANCED", "AGGRESSIVE")),
}

_TRUE = {"1", "true", "yes", "on"}
_FALSE = {"0", "false", "no", "off"}


def is_unset(value: Any) -> bool:
    """Assente, None o stringa vuota: nessun valore scelto (default di codice o cap non attivo)."""
    return value is None or (isinstance(value, str) and value.strip() == "")


def parse_number(value: Any) -> Optional[float]:
    """Numero finito o None. Accetta la virgola decimale ("0,10"); rifiuta bool, NaN, inf e testo."""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if text.count(",") == 1 and "." not in text:
            text = text.replace(",", ".")
        try:
            number = float(text)
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def parse_int(value: Any) -> Optional[int]:
    """Intero o None: un numero con parte decimale (2.5) non e' un intero."""
    number = parse_number(value)
    if number is None or number != int(number):
        return None
    return int(number)


def parse_bool(value: Any) -> Optional[bool]:
    """True/False o None: solo bool, 0/1 e i token espliciti (``maybe`` o 2 non sono un booleano)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        token = value.strip().lower()
        if token in _TRUE:
            return True
        if token in _FALSE:
            return False
    return None


def _in_range(number: float, kind: str) -> bool:
    low, low_inc, high, high_inc = _RANGES[kind]
    if low is not None and (number < low or (number == low and not low_inc)):
        return False
    if high is not None and (number > high or (number == high and not high_inc)):
        return False
    return True


def invalid_reason(key: str, value: Any) -> str:
    """'' se ``value`` e' accettabile per ``key`` (o se e' non impostato), altrimenti il motivo."""
    spec = SPECS.get(key)
    if spec is None or is_unset(value):
        return ""
    vtype, domain = spec
    if vtype == "num":
        number = parse_number(value)
        if number is None:
            return f"{key}: valore non numerico o non finito ({value!r})"
        if not _in_range(number, domain):
            return f"{key}: fuori dominio ({value!r})"
        return ""
    if vtype == "int":
        number = parse_int(value)
        low, high = domain
        if number is None or not low <= number <= high:
            return f"{key}: intero fuori dominio [{low}, {high}] ({value!r})"
        return ""
    if vtype == "bool":
        return "" if parse_bool(value) is not None else f"{key}: booleano non valido ({value!r})"
    if vtype == "enum":
        return "" if str(value).strip().upper() in domain else f"{key}: valore non ammesso ({value!r})"
    return ""


def coherence_reasons(values: Dict[str, Any]) -> List[str]:
    """Incoerenze fra chiavi, sui valori gia' validi singolarmente."""
    out = []
    warning = parse_number(values.get("roserpina.book_warning"))
    block = parse_number(values.get("roserpina.book_block"))
    if warning is not None and block is not None and block < warning:
        out.append(f"roserpina.book_block: inferiore a roserpina.book_warning ({block} < {warning})")
    return out


def validate(values: Dict[str, Any]) -> List[str]:
    """Tutti i motivi di rifiuto per le chiavi note in ``values`` (le altre chiavi non sono di questo dominio)."""
    reasons = [r for r in (invalid_reason(k, v) for k, v in (values or {}).items()) if r]
    return reasons or coherence_reasons(values or {})
