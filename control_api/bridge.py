"""Collega le viste di sola lettura del core al server ``pickfair_control_api``."""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from control_api.views import account_view, exposure_view, reconciliation_view, status_view

# Pin esatto del package pickfair_control_api (repository pickfair-mcp-). Un package diverso da
# quello verificato non viene MAI usato: la Control API non parte (CONTRACT_PIN_MISMATCH).
EXPECTED_CONTROL_API_VERSION = "1.2.0"
EXPECTED_SCHEMA_HASH = "bafc77d9623e8af9e233fe71c38f0a34949d73f6ebd0b627b4ce2498c9361528"


class ControlApiPinMismatch(Exception):
    """Il package installato non è quello verificato per questo ponte."""


def verify_pinned_package() -> None:
    import pickfair_control_api

    if getattr(pickfair_control_api, "__version__", None) != EXPECTED_CONTROL_API_VERSION:
        raise ControlApiPinMismatch("VERSION")
    if pickfair_control_api.schema_hash() != EXPECTED_SCHEMA_HASH:
        raise ControlApiPinMismatch("SCHEMA_HASH")


def make_providers(
    runtime: object, *, clock: Callable[[], float] = time.time
) -> dict[str, Callable[[], dict[str, Any]]]:
    return {
        "status": lambda: status_view(runtime, clock=clock),
        "reconciliation_status": lambda: reconciliation_view(runtime, clock=clock),
        "account": lambda: account_view(runtime, clock=clock),
        "exposure": lambda: exposure_view(runtime, clock=clock),
    }


def start_control_api(
    runtime: object, *, port: int, token_path: Path, git_sha: str,
    clock: Callable[[], float] = time.time, allow_ephemeral_port: bool = False,
) -> Any:
    """Avvio in quest'ordine, senza servizi parziali:

    1. pin esatto del package (versione e schema hash): se fallisce, nessun socket e nessun file;
    2. token nuovo SOLO in memoria;
    3. bind del listener su 127.0.0.1: se fallisce, nessun file del token;
    4. pubblicazione atomica del token (0600): se fallisce, il listener viene chiuso;
    5. avvio del servizio.
    """
    verify_pinned_package()
    # Import locale: il package serve solo quando la Control API è richiesta esplicitamente.
    from pickfair_control_api.server import ControlApiServer
    from pickfair_control_api.tokens import generate_token_store

    tokens = generate_token_store()
    server = ControlApiServer(
        make_providers(runtime, clock=clock), port=port, tokens=tokens, git_sha=git_sha,
        allow_ephemeral_port=allow_ephemeral_port,
    )
    try:
        tokens.publish(token_path)
    except BaseException:
        server.stop()
        raise
    server.start()
    return server
