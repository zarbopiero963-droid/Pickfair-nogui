"""Validazione del segnale prima della scrittura CSV (PR-10).

Gate esplicito sopra il riconoscimento (PR-06). Oltre ai campi nome/ID richiesti
dalla modalità, verifica due cose safety-critical:

- il **prezzo** (`Price`) è una quota valida, cioè un numero **> 1.0** (una quota
  a 1.00 non dà guadagno e non è piazzabile; sotto 1 è una linea di mercato, non
  una quota). Il parametro `require_price` rende il controllo disattivabile: a
  runtime è guidato dalla riga Price del parser (`CustomParserDef.price_required`),
  non più da una chiave di config globale;
- il **BetType** è `PUNTA`/`BANCA` (un lato sconosciuto pizzerebbe la scommessa
  sbagliata).

Un segnale senza prezzo, con prezzo non valido o con lato sconosciuto NON deve
raggiungere XTrader. Il validatore non modifica la riga: la accetta o la scarta.
`Points` (moltiplicatore stake) NON va normalizzato a "1" e resta vuoto di default,
ma se un parser custom lo valorizza deve essere un numero **positivo**; e i limiti
`MinPrice`/`MaxPrice`, oltre a essere quote valide, non devono essere **incoerenti**
(intervallo invertito o che esclude `Price`).
"""

import re

from . import numbers_re, recognition

VALID = "VALID"

# Quota = decimale "semplice": cifre con al più un separatore (punto o virgola).
# Esclude esponenti ("1e2"), inf/nan, segni, separatori multipli: il contratto
# XTrader documenta quote decimali punto-normalizzate, non notazioni arbitrarie.
# Frammento decimale condiviso (anti-drift, audit L4).
_DECIMAL_PRICE = re.compile(r"^" + numbers_re.DECIMAL + r"$")
INVALID_MISSING_FIELDS = "INVALID_MISSING_FIELDS"   # campi nome/ID per la modalità
INVALID_MISSING_PRICE = "INVALID_MISSING_PRICE"
INVALID_PRICE = "INVALID_PRICE"
INVALID_BETTYPE = "INVALID_BETTYPE"
INVALID_POINTS = "INVALID_POINTS"           # Points valorizzato ma non un numero > 0
INVALID_PRICE_BOUNDS = "INVALID_PRICE_BOUNDS"  # Min/Max incoerenti (invertiti o escludono Price)

_VALID_BETTYPES = ("PUNTA", "BANCA")


def _price_status(value) -> str:
    """VALID se `value` è una quota valida (> 1.0); altrimenti il codice di errore.

    `None` e stringa vuota contano come **prezzo mancante** (non malformato).
    """
    if value is None:
        return INVALID_MISSING_PRICE
    s = str(value).strip()
    if not s:
        return INVALID_MISSING_PRICE
    # Forma decimale stretta: niente esponenti/inf/nan/testo (il parser custom
    # può estrarre testo arbitrario, es. "1e2"/"inf"/"abc").
    if not _DECIMAL_PRICE.match(s):
        return INVALID_PRICE
    price = float(s.replace(",", "."))
    return VALID if price > 1.0 else INVALID_PRICE


def price_status(value) -> str:
    """Pubblico: `VALID` se `value` è una quota valida (> 1.0), altrimenti il codice
    di errore (`INVALID_MISSING_PRICE`/`INVALID_PRICE`). Usato dalla diagnostica per
    attribuire un errore prezzo alla colonna giusta (Price vs Min/MaxPrice)."""
    return _price_status(value)


def bettype_status(value) -> str:
    """Pubblico: `VALID` se `value` è un BetType valido (`PUNTA`/`BANCA`, case-insensitive),
    altrimenti `INVALID_BETTYPE`. Vuoto/sconosciuto = non valido (il lato è obbligatorio).
    Usato dalla diagnostica per segnalare la colonna BetType indipendentemente dagli altri."""
    return VALID if str(value).strip().upper() in _VALID_BETTYPES else INVALID_BETTYPE


def points_status(value) -> str:
    """Pubblico: `VALID` se `value` è un Points valido — vuoto (facoltativo) oppure un numero
    **positivo** (`> 0`) — altrimenti `INVALID_POINTS`. Usato dalla diagnostica per segnalare
    la colonna Points indipendentemente dagli altri errori."""
    s = str(value).strip()
    if not s:
        return VALID
    if not _DECIMAL_PRICE.match(s) or float(s.replace(",", ".")) <= 0.0:
        return INVALID_POINTS
    return VALID


def price_bounds_offenders(row: dict) -> tuple:
    """Colonne dei limiti che rendono l'intervallo Min/Max INCOERENTE (tupla vuota = coerente).

    Presuppone che i valori presenti siano già quote valide (validati a monte), quindi
    `float(...)` è sicuro. Restituisce SOLO i limiti che offendono, così la diagnostica non
    segnala un limite opzionale ASSENTE (Codex):
    - `MinPrice > MaxPrice` → `("MinPrice", "MaxPrice")` (entrambi: la relazione è fra loro);
    - `MinPrice > Price`     → `("MinPrice",)`;
    - `MaxPrice < Price`     → `("MaxPrice",)`.
    """
    def _num(col):
        v = str(row.get(col, "")).strip()
        return float(v.replace(",", ".")) if v else None

    price_v, min_v, max_v = _num("Price"), _num("MinPrice"), _num("MaxPrice")
    if min_v is not None and max_v is not None and min_v > max_v:
        return ("MinPrice", "MaxPrice")
    if price_v is not None:
        if min_v is not None and min_v > price_v:
            return ("MinPrice",)
        if max_v is not None and max_v < price_v:
            return ("MaxPrice",)
    return ()


def validate(row: dict, mode: str, require_price: bool = True):
    """Valuta una riga CSV già costruita.

    Ritorna una tupla `(status, detail)`:
    - status == VALID  → la riga può essere scritta;
    - INVALID_MISSING_FIELDS → `detail` è la lista dei campi nome/ID mancanti;
    - INVALID_BETTYPE        → `detail` è il BetType trovato;
    - INVALID_MISSING_PRICE / INVALID_PRICE → `detail` è il valore Price trovato.
    """
    missing = recognition.missing_fields(row, mode)
    if missing:
        return (INVALID_MISSING_FIELDS, missing)

    bet_type = str(row.get("BetType", "")).strip().upper()
    if bettype_status(bet_type) != VALID:
        return (INVALID_BETTYPE, bet_type)

    # Un `Price` valorizzato va SEMPRE validato; `require_price` governa solo se
    # un `Price` VUOTO è ammesso (flag = "prezzo opzionale", non "non validato").
    price_raw = str(row.get("Price", "")).strip()
    if price_raw:
        status = _price_status(price_raw)
        if status != VALID:
            return (status, price_raw)
    elif require_price:
        return (INVALID_MISSING_PRICE, "")

    # MinPrice/MaxPrice sono opzionali (possono restare vuoti), ma se valorizzati
    # devono essere quote valide: un limite malformato ("abc") o sotto/uguale a
    # 1.0 non deve raggiungere XTrader. (Il percorso hardcoded li lascia vuoti.)
    for col in ("MinPrice", "MaxPrice"):
        v = str(row.get(col, "")).strip()
        if v:
            status = _price_status(v)
            if status != VALID:
                return (status, v)

    # Points (moltiplicatore stake): il percorso hardcoded lo lascia vuoto, ma un
    # parser custom può estrarre testo arbitrario. Se valorizzato deve essere un
    # numero POSITIVO (> 0): "abc"/"-5"/"0"/"inf"/"1e2" non devono raggiungere XTrader.
    points_raw = str(row.get("Points", "")).strip()
    if points_status(points_raw) != VALID:
        return (INVALID_POINTS, points_raw)

    # Coerenza dei limiti di prezzo: oltre a essere singolarmente validi (sopra),
    # Min/Max non devono CONTRADDIRE loro stessi o `Price`. Un intervallo invertito
    # (Min > Max) o che ESCLUDE la quota selezionata (Min > Price, Max < Price) non
    # è usabile da XTrader → fail-closed. Bordi inclusivi ok. `detail` è la tupla dei
    # SOLI limiti che offendono, così la diagnostica non segnala un limite assente.
    offenders = price_bounds_offenders(row)
    if offenders:
        return (INVALID_PRICE_BOUNDS, offenders)

    return (VALID, None)


def is_valid(row: dict, mode: str, require_price: bool = True) -> bool:
    """True se la riga supera la validazione completa."""
    return validate(row, mode, require_price)[0] == VALID
