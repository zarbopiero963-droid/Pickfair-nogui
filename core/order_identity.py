"""Identita' stabile degli intenti d'ordine (PR26-a, #461).

Il TradingEngine rifiuta ogni richiesta senza ``customer_ref``
(``CUSTOMER_REF_REQUIRED``) e su quel ref fa de-dup (in memoria e su DB).
Questo modulo da' ai PRODUTTORI d'ordine un ref:

- **stabile**: la stessa intenzione logica (riconsegna sul bus, restart,
  retry consentito, ricalcolo) produce sempre lo stesso ref, cosi' l'engine
  la riconosce come doppione invece di piazzarla due volte;
- **distinto**: intenti diversi hanno materiale d'identita' diverso e quindi
  ref diversi;
- **conforme a Betfair** (``[A-Za-z0-9-._+*:;~]{1,32}``): prefisso di 1-3
  lettere minuscole, un trattino e 28 cifre esadecimali di SHA-256 = 32
  caratteri. ``BetfairClient._normalize_customer_ref`` lo inoltra invariato.

``derive_customer_ref`` non usa casualita' ne' orologio: il ref dipende solo
dal materiale passato dal produttore, che deve escludere cio' che cambia fra
un tentativo e l'altro della STESSA intenzione (es. lo stake ricalcolato
dalla MM, l'ora di ricezione). ``new_operation_customer_ref`` serve solo dove
ogni invocazione e' per definizione un intento nuovo (un click manuale).
Un ref a monte non conforme viene reso conforme in modo deterministico
(``normalize_upstream_customer_ref``) invece di essere omesso da Betfair.
"""
from __future__ import annotations

import hashlib
import json
import re
import uuid
from typing import Any, Mapping

__all__ = [
    "derive_customer_ref",
    "is_betfair_customer_ref",
    "new_operation_customer_ref",
    "normalize_upstream_customer_ref",
    "resolve_customer_ref",
]

_PREFIX_RE = re.compile(r"[a-z]{1,3}")
_DIGEST_HEX_CHARS = 28  # 3 + 1 + 28 = 32, il massimo ammesso da Betfair
# Stesso vincolo di BetfairClient._normalize_customer_ref (customerRef Betfair).
_BETFAIR_CUSTOMER_REF_RE = re.compile(r"[A-Za-z0-9\-._+*:;~]{1,32}")


def is_betfair_customer_ref(value: Any) -> bool:
    """``True`` se ``value`` e' un customerRef che Betfair accetta cosi' com'e'."""
    return isinstance(value, str) and _BETFAIR_CUSTOMER_REF_RE.fullmatch(value) is not None


def _validate_prefix(prefix: Any) -> None:
    if not isinstance(prefix, str) or _PREFIX_RE.fullmatch(prefix) is None:
        raise ValueError(f"customer_ref prefix non valido: {prefix!r}")


def _json_default(value: Any) -> Any:
    # Insiemi: ordine d'iterazione non deterministico fra processi -> ordinati.
    if isinstance(value, (set, frozenset)):
        return sorted(str(item) for item in value)
    return str(value)


def derive_customer_ref(prefix: str, material: Mapping[str, Any]) -> str:
    """Ref deterministico ``<prefix>-<sha256[:28]>`` del materiale d'identita'.

    Il materiale e' serializzato in JSON canonico (chiavi ordinate), quindi
    l'ordine delle chiavi non cambia il ref. ``prefix`` deve essere di 1-3
    lettere minuscole: un prefisso invalido solleva ``ValueError`` invece di
    produrre un ref che Betfair scarterebbe.
    """
    _validate_prefix(prefix)
    canonical = json.dumps(
        dict(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:_DIGEST_HEX_CHARS]}"


def new_operation_customer_ref(prefix: str) -> str:
    """Ref NUOVO per un'operazione che non ha identita' a monte (es. un click).

    Da usare solo dove ogni invocazione e' per definizione un intento nuovo:
    il ref nasce una volta, all'origine, e i retry a valle riusano il payload
    (quindi lo stesso ref). Stesso formato conforme di ``derive_customer_ref``.
    """
    _validate_prefix(prefix)
    return f"{prefix}-{uuid.uuid4().hex[:_DIGEST_HEX_CHARS]}"


def normalize_upstream_customer_ref(upstream: Any) -> str:
    """Ref a monte reso sicuro per Betfair; ``""`` se assente.

    Un ref conforme passa invariato (solo ``strip``): e' l'identita' scelta dal
    chiamante. Uno NON conforme (oltre 32 caratteri o charset vietato) verrebbe
    omesso in silenzio da ``BetfairClient`` togliendo il de-dup lato Betfair:
    lo si sostituisce con ``up-<hash>`` derivato dal valore a monte, quindi lo
    stesso ref a monte da' sempre lo stesso ref conforme e due ref diversi
    restano diversi.
    """
    ref = str(upstream or "").strip()
    if not ref:
        return ""
    if is_betfair_customer_ref(ref):
        return ref
    return derive_customer_ref("up", {"upstream_customer_ref": ref})


def resolve_customer_ref(upstream: Any, prefix: str, material: Mapping[str, Any]) -> str:
    """Preserva il ``customer_ref`` a monte (reso conforme), altrimenti lo deriva."""
    return normalize_upstream_customer_ref(upstream) or derive_customer_ref(prefix, material)
