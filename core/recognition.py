"""Modalità di riconoscimento del segnale per XTrader (PR-06).

XTrader può validare un segnale tramite gli ID (`MarketId` + `SelectionId`)
oppure tramite i nomi (`EventName` + `MarketType` + `SelectionName`).
Questa logica decide se una riga CSV ha i campi necessari per la modalità scelta:
serve a **bloccare la scrittura** di righe incomplete (non riconoscibili da
XTrader), evitando segnali ambigui.
"""

ID_ONLY = "ID_ONLY"
NAME_ONLY = "NAME_ONLY"
BOTH = "BOTH"

VALID_MODES = (ID_ONLY, NAME_ONLY, BOTH)
DEFAULT_MODE = NAME_ONLY

# Campi richiesti da ciascun "set" di riconoscimento.
_ID_FIELDS = ("MarketId", "SelectionId")
_NAME_FIELDS = ("EventName", "MarketType", "SelectionName")

# Tutte le colonne legate al riconoscimento (per il builder: sono quelle la cui
# obbligatorietà è guidata dalla Modalità, vs Price/BetType/Provider che no).
RECOGNITION_FIELDS = _ID_FIELDS + _NAME_FIELDS


def recognition_fields_for_mode(mode: str) -> tuple:
    """Campi di riconoscimento RILEVANTI per la modalità (per il content-gate A10):

    - `ID_ONLY`   → solo i campi ID (`MarketId`+`SelectionId`);
    - `NAME_ONLY` → solo i campi nome (`EventName`+`MarketType`+`SelectionName`);
    - `BOTH`      → entrambi i set (basta un set, quindi ognuno è "di segnale").

    Differisce da `RECOGNITION_FIELDS` (insieme totale, mode-agnostico): qui un
    campo ID opzionale NON conta come "contenuto di segnale" per un parser
    `NAME_ONLY`, così un'estrazione ID casuale non fa passare un non-segnale (A10).
    """
    mode = normalize_mode(mode)
    if mode == ID_ONLY:
        return _ID_FIELDS
    if mode == NAME_ONLY:
        return _NAME_FIELDS
    return RECOGNITION_FIELDS


def required_targets(mode: str) -> tuple:
    """Colonne che la Modalità rende obbligatorie nel builder (auto-Obblig.):

    - `ID_ONLY`   → `MarketId`+`SelectionId`;
    - `NAME_ONLY` → `EventName`+`MarketType`+`SelectionName`;
    - `BOTH`      → `()`: basta UN set completo, quindi il builder non forza un set
      preciso (lo decide l'utente; il validatore accetta ID **oppure** nomi).
    """
    mode = normalize_mode(mode)
    if mode == ID_ONLY:
        return _ID_FIELDS
    if mode == NAME_ONLY:
        return _NAME_FIELDS
    return ()


def normalize_mode(mode: str) -> str:
    """Riporta una modalità sconosciuta al default sicuro (NAME_ONLY)."""
    return mode if mode in VALID_MODES else DEFAULT_MODE


def _missing(row: dict, fields) -> list:
    return [f for f in fields if not str(row.get(f, "")).strip()]


def missing_fields(row: dict, mode: str) -> list:
    """Elenco dei campi mancanti perché la riga sia riconoscibile nella modalità.

    - ID_ONLY: servono tutti gli ID.
    - NAME_ONLY: servono tutti i nomi.
    - BOTH: basta che sia completo ALMENO uno dei due set (ID **oppure** nomi);
      se nessuno è completo, riporta i campi nome mancanti come indicazione.
    Lista vuota = riga valida per quella modalità.
    """
    mode = normalize_mode(mode)
    if mode == ID_ONLY:
        return _missing(row, _ID_FIELDS)
    if mode == NAME_ONLY:
        return _missing(row, _NAME_FIELDS)
    # BOTH
    id_missing = _missing(row, _ID_FIELDS)
    name_missing = _missing(row, _NAME_FIELDS)
    if not id_missing or not name_missing:
        return []
    return name_missing


def is_valid(row: dict, mode: str) -> bool:
    return not missing_fields(row, mode)
