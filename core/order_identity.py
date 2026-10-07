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

Nessuna casualita' e nessun orologio: il ref dipende solo dal materiale
passato dal produttore. Il chiamante sceglie il materiale e deve escludere
cio' che cambia fra un tentativo e l'altro della STESSA intenzione (es. lo
stake ricalcolato dalla MM).
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

__all__ = ["derive_customer_ref", "resolve_customer_ref"]

_PREFIX_RE = re.compile(r"[a-z]{1,3}")
_DIGEST_HEX_CHARS = 28  # 3 + 1 + 28 = 32, il massimo ammesso da Betfair


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
    if not isinstance(prefix, str) or _PREFIX_RE.fullmatch(prefix) is None:
        raise ValueError(f"customer_ref prefix non valido: {prefix!r}")
    canonical = json.dumps(
        dict(material),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=_json_default,
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{prefix}-{digest[:_DIGEST_HEX_CHARS]}"


def resolve_customer_ref(upstream: Any, prefix: str, material: Mapping[str, Any]) -> str:
    """Preserva un ``customer_ref`` gia' fornito a monte, altrimenti lo deriva.

    Un ref a monte non vuoto e' l'identita' scelta dal chiamante e passa
    invariato (solo ``strip``): riscriverlo romperebbe il de-dup del chiamante.
    """
    ref = str(upstream or "").strip()
    if ref:
        return ref
    return derive_customer_ref(prefix, material)
