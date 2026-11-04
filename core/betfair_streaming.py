"""Streaming mercati Betfair (ESA) per il `BetfairClient` di Pickfair.

`services.streaming_feed.StreamingFeed` usa l'interfaccia di betfairlightweight (`client.streaming.create_stream`,
`StreamListener`, `subscribe_to_markets`), ma il `BetfairClient` di Pickfair non la esponeva: in modalita' stream il
feed falliva sempre con ``STREAMING_INTERFACE_UNAVAILABLE`` e ricadeva sul REST. Questo modulo e' il ponte, senza un
protocollo proprietario: riusa il client di streaming di betfairlightweight (dipendenza bloccata del progetto) legato
all'``app_key`` e al ``session_token`` del client di Pickfair.

Differenza voluta rispetto alla libreria: betfairlightweight apre il socket TLS con ``ssl.CERT_NONE`` (nessuna verifica
del certificato del server). Qui lo stream usa un contesto TLS di default: certificato e nome host verificati. Un
socket non verificabile e' una disconnessione, mai dati accettati.

L'host resta quello della libreria (``stream-api.betfair.com``); che sia quello corretto per l'Exchange italiano e'
UNVERIFIED (da provare al livello B read-only, PF-BETFAIR-E2E-01). Lo streaming fornisce solo dati di mercato: nessun
ordine passa di qui, e l'ammissione dei nuovi ingressi non si basa sulla cache dello stream (F-23: lettura REST fresca).
"""
from __future__ import annotations

import socket
import ssl
from typing import Any

from betfairlightweight.endpoints.streaming import Streaming
from betfairlightweight.streaming.betfairstream import BetfairStream
from betfairlightweight.streaming.listener import BaseListener, StreamListener

STREAM_PORT = 443


class VerifiedBetfairStream(BetfairStream):
    """`BetfairStream` con TLS verificato (certificato e nome host)."""

    def _create_socket(self) -> socket.socket:
        context = ssl.create_default_context()           # CERT_REQUIRED + check_hostname
        s = context.wrap_socket(socket.socket(socket.AF_INET, socket.SOCK_STREAM), server_hostname=self.host)
        s.settimeout(self.timeout)
        s.connect((self.host, STREAM_PORT))
        return s


class PickfairStreaming(Streaming):
    """`Streaming` di betfairlightweight che crea stream con TLS verificato."""

    def create_stream(self, unique_id: int = 0, listener: BaseListener = None, timeout: float = 64,
                      buffer_size: int = 1024, host: str = None) -> BetfairStream:
        if not str(getattr(self.client, "app_key", "") or "").strip():
            raise RuntimeError("STREAM_APP_KEY_MISSING")
        reader = getattr(self.client, "_session_token_value", None)      # lettura sotto il lock della sessione
        token = reader() if callable(reader) else getattr(self.client, "session_token", "")
        if not str(token or "").strip():
            raise RuntimeError("STREAM_SESSION_MISSING")            # mai un'autenticazione con token vuoto
        return VerifiedBetfairStream(unique_id, listener if listener else StreamListener(),
                                     app_key=self.client.app_key, session_token=token, timeout=timeout,
                                     buffer_size=buffer_size, host=host)
