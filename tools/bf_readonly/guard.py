"""Guardia READ-ONLY sul trasporto HTTP del `BetfairClient` (PF-BETFAIR-E2E-01, livello B).

Si inserisce come ``session`` del client: ogni POST passa di qui PRIMA della rete. Ammette solo
- login certificato, keepAlive e logout sugli host identitysso di Betfair;
- metodi JSON-RPC di LETTURA in ``ALLOWED_METHODS`` (allowlist).
Qualunque altro metodo — ``placeOrders``, ``cancelOrders``, ``replaceOrders``, ``updateOrders`` o un metodo sconosciuto —,
un corpo illeggibile o un URL fuori dagli endpoint noti => ``ReadOnlyViolation`` sollevata PRIMA dell'invio: la richiesta
non lascia mai il processo, qualunque sia il codice chiamante. Le violazioni restano registrate in ``violations``
(la campagna le tratta come FAIL anche se il client le avesse inghiottite).
"""
from __future__ import annotations

import json
from typing import Any, List
from urllib.parse import urlparse

ALLOWED_METHODS = frozenset({
    "SportsAPING/v1.0/listEventTypes",
    "SportsAPING/v1.0/listCompetitions",
    "SportsAPING/v1.0/listCountries",
    "SportsAPING/v1.0/listEvents",
    "SportsAPING/v1.0/listMarketTypes",
    "SportsAPING/v1.0/listMarketCatalogue",
    "SportsAPING/v1.0/listMarketBook",
    "SportsAPING/v1.0/listCurrentOrders",
    "SportsAPING/v1.0/listClearedOrders",
    "AccountAPING/v1.0/getAccountFunds",
    "AccountAPING/v1.0/getAccountDetails",
    "AccountAPING/v1.0/getAccountStatement",     # storico conto: input della ricostruzione del picco (F-11)
})

#: Metodi che alterano ordini o fondi: elencati per documentazione e test; la guardia rifiuta comunque tutto cio' che non
#: e' nell'allowlist.
MUTATING_METHODS = frozenset({
    "SportsAPING/v1.0/placeOrders",
    "SportsAPING/v1.0/cancelOrders",
    "SportsAPING/v1.0/replaceOrders",
    "SportsAPING/v1.0/updateOrders",
})

_IDENTITY_HOSTS = frozenset({"identitysso-cert.betfair.it", "identitysso.betfair.it",
                             "identitysso-cert.betfair.com", "identitysso.betfair.com"})
_IDENTITY_PATHS = frozenset({"/api/certlogin", "/api/keepAlive", "/api/logout"})
_JSONRPC_HOSTS = frozenset({"api.betfair.com", "api.betfair.it"})
_JSONRPC_PATHS = frozenset({"/exchange/betting/json-rpc/v1", "/exchange/account/json-rpc/v1"})


class ReadOnlyViolation(RuntimeError):
    """Richiesta non di sola lettura: bloccata prima della rete."""


class ReadOnlyTransport:
    """Avvolge una sessione requests-like e ne lascia passare solo le letture."""

    def __init__(self, inner: Any) -> None:
        if inner is None or not callable(getattr(inner, "post", None)):
            raise TypeError("sessione interna senza post()")
        self._inner = inner
        self.violations: List[str] = []
        self.methods_sent: List[str] = []

    def _refuse(self, reason: str) -> None:
        self.violations.append(reason)
        raise ReadOnlyViolation(reason)

    def post(self, url: str, **kwargs: Any) -> Any:
        parsed = urlparse(str(url or ""))
        host, path = (parsed.hostname or "").lower(), parsed.path
        if parsed.scheme != "https":
            self._refuse(f"SCHEME_NOT_HTTPS:{parsed.scheme}")
        if host in _IDENTITY_HOSTS and path in _IDENTITY_PATHS:
            self.methods_sent.append(f"IDENTITY{path}")
            return self._inner.post(url, **kwargs)
        if host not in _JSONRPC_HOSTS or path not in _JSONRPC_PATHS:
            self._refuse(f"URL_NOT_ALLOWED:{host}{path}")
        try:
            body = json.loads(kwargs.get("data") or "")
        except (TypeError, ValueError):
            self._refuse("BODY_UNREADABLE")
        items = body if isinstance(body, list) else [body]
        if not items:
            self._refuse("BODY_EMPTY")
        for item in items:
            method = item.get("method") if isinstance(item, dict) else None
            if not isinstance(method, str) or method not in ALLOWED_METHODS:
                self._refuse(f"METHOD_NOT_READ_ONLY:{method}")
        self.methods_sent.extend(str(i["method"]) for i in items)
        return self._inner.post(url, **kwargs)

    @property
    def proxies(self) -> Any:
        return getattr(self._inner, "proxies", {})

    @proxies.setter
    def proxies(self, value: Any) -> None:
        # `BetfairClient._configura_proxy` imposta il proxy sulla sessione: va sulla sessione reale.
        self._inner.proxies = value

    def get(self, *_a: Any, **_k: Any) -> Any:
        self._refuse("HTTP_GET_NOT_ALLOWED")

    def __getattr__(self, name: str) -> Any:
        # put/delete/request/send o altro: mai inoltrati alla sessione reale.
        if name in {"put", "patch", "delete", "request", "send"}:
            def _refused(*_a: Any, **_k: Any) -> Any:
                self._refuse(f"HTTP_{name.upper()}_NOT_ALLOWED")
            return _refused
        raise AttributeError(name)
