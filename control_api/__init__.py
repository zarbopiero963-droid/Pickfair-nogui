"""Control API v1 di Pickfair — SOLA LETTURA (PF-API-1, preparazione).

Stato: PROVISIONAL, non cablata in ``headless_main`` e non inclusa nel packaging.
Nessuna route di comando: GET /v1/meta, GET /v1/status, GET /v1/reconciliation/status.

Regole (docs/control_api.md):
- in-process, bind solo su 127.0.0.1 con porta esplicita, autenticazione Bearer;
- letture pure dai componenti autorevoli del runtime: nessuna chiamata Betfair, nessuna
  scrittura su DB, nessun publish sul bus, nessuna transizione di stato;
- un dato che il runtime non possiede è ``null`` ed è elencato in ``unavailable``;
- codici e motivi sono token MAIUSCOLI: il testo libero non esce mai.
"""

from control_api.contract import CAPABILITIES, CONTRACT_VERSION, schema_hash
from control_api.server import ControlApiConfigError, ControlApiServer, issue_token_file
from control_api.views import reconciliation_view, status_view

__all__ = [
    "CAPABILITIES",
    "CONTRACT_VERSION",
    "ControlApiConfigError",
    "ControlApiServer",
    "issue_token_file",
    "reconciliation_view",
    "schema_hash",
    "status_view",
]
