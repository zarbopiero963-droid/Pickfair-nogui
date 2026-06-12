"""PR-D programma test hedge-fund grade: policy errori AUTH nella reconciliation.

Gap chiusi (dedup verificato — MATCHED/ghost/ambiguous/audit/batch-lock/
fencing/transient-retry sono GIA' coperti nei 17 file esistenti di
tests/reconciliation/ e tests/invariant/; qui solo signal nuovo):
- errore AUTH (sessione invalida/401/403) durante il fetch -> STOP
  immediato con AUTH_ERROR, MAI retry (martellare Betfair con una
  sessione morta = lockout account)
- precedenza del classificatore: AUTH vince su TRANSIENT/PERMANENT
  anche quando il messaggio contiene marker di entrambe
- esaurimento dei retry transient -> TRANSIENT_ERROR con budget esatto
"""
from __future__ import annotations

import pytest

from core.reconciliation_engine import ReasonCode, ReconcileConfig, ReconciliationEngine
from core.reconciliation_types import ErrorClass, classify_error


class FakeDB:
    @staticmethod
    def persist_decision_log(batch_id, entries):
        return None

    @staticmethod
    def get_pending_sagas():
        return []

    @staticmethod
    def get_reconcile_marker(batch_id):
        return None

    @staticmethod
    def set_reconcile_marker(batch_id, value):
        return None


class FakeBatchManager:
    @staticmethod
    def get_batch(batch_id):
        return {"batch_id": batch_id, "market_id": "1.1", "status": "LIVE"}

    @staticmethod
    def get_batch_legs(batch_id):
        return []

    @staticmethod
    def get_open_batches():
        return []

    @staticmethod
    def update_leg_status(batch_id, leg_index, status, bet_id=None,
                          raw_response=None, error_text=None):
        return None

    @staticmethod
    def recompute_batch_status(batch_id):
        return {"batch_id": batch_id, "status": "LIVE"}

    @staticmethod
    def mark_batch_failed(batch_id, reason=""):
        return None

    @staticmethod
    def release_runtime_artifacts(**kwargs):
        return None


class FakeClient:
    def __init__(self, side_effects):
        self.side_effects = list(side_effects)
        self.calls = 0

    def get_current_orders(self, market_ids=None):
        self.calls += 1
        if not self.side_effects:
            raise AssertionError(
                f"FakeClient esaurito dopo {self.calls} chiamate: "
                "il test ha configurato meno side_effects del previsto"
            )
        effect = self.side_effects.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect


def make_engine(client):
    return ReconciliationEngine(
        db=FakeDB(),
        batch_manager=FakeBatchManager(),
        client_getter=lambda: client,
        config=ReconcileConfig(
            max_transient_retries=2,
            transient_retry_base_delay=0.0,
            transient_retry_max_delay=0.0,
        ),
    )


@pytest.mark.reconciliation
@pytest.mark.parametrize("auth_message", [
    "INVALID_SESSION_INFORMATION",
    "session expired",
    "401 unauthorized",
    "403 forbidden",
    "NOT LOGGED IN",
])
def test_auth_error_stops_immediately_without_retry(auth_message):
    """Sessione morta: un solo tentativo, AUTH_ERROR esplicito, niente
    martellamento (il retry con auth rotta rischia il lockout account).
    Il budget transient era disponibile (2 retry) e NON viene usato."""
    client = FakeClient([
        RuntimeError(auth_message),
        [],  # mai raggiunto: nessun retry deve avvenire
    ])
    eng = make_engine(client)

    orders, failure_reason = eng._fetch_current_orders_by_market("1.1")

    assert orders == []
    assert failure_reason == ReasonCode.AUTH_ERROR
    assert client.calls == 1


@pytest.mark.reconciliation
def test_auth_marker_beats_transient_marker_in_same_message():
    """Messaggio ambiguo con marker di entrambe le classi: AUTH ha la
    precedenza (e' controllato per primo) -> niente retry."""
    client = FakeClient([RuntimeError("connection forbidden by proxy")])
    eng = make_engine(client)

    _orders, failure_reason = eng._fetch_current_orders_by_market("1.1")

    assert failure_reason == ReasonCode.AUTH_ERROR
    assert client.calls == 1


@pytest.mark.reconciliation
def test_transient_retries_exhausted_returns_transient_error():
    """Timeout persistente: il budget retry si esaurisce e l'esito e'
    TRANSIENT_ERROR (mai successo finto), con numero di chiamate esatto."""
    client = FakeClient([
        TimeoutError("timeout"),
        TimeoutError("timeout"),
        TimeoutError("timeout"),
        TimeoutError("timeout"),  # margine: non deve mai servire
    ])
    eng = make_engine(client)

    orders, failure_reason = eng._fetch_current_orders_by_market("1.1")

    assert orders == []
    assert failure_reason == ReasonCode.TRANSIENT_ERROR
    # max_transient_retries=2 -> 1 tentativo + 2 retry = 3 chiamate.
    assert client.calls == 3


@pytest.mark.reconciliation
@pytest.mark.parametrize("message,expected", [
    ("session expired", ErrorClass.AUTH),
    ("ssoid invalid", ErrorClass.AUTH),
    ("HTTP 403", ErrorClass.AUTH),
    # Codici letterali API Betfair: erano classificati TRANSIENT (bug
    # trovato da questa suite) -> la reconciliation ritentava con una
    # sessione morta invece di fermarsi con AUTH_ERROR.
    ("INVALID_SESSION_INFORMATION", ErrorClass.AUTH),
    ("NO_SESSION", ErrorClass.AUTH),
    ("invalid_input", ErrorClass.PERMANENT),
    ("market not found", ErrorClass.PERMANENT),
    ("read timed out", ErrorClass.TRANSIENT),
    ("rate limit exceeded", ErrorClass.TRANSIENT),
])
def test_classify_error_message_markers(message, expected):
    assert classify_error(RuntimeError(message)) == expected


@pytest.mark.reconciliation
def test_classify_error_matches_exception_type_name_too():
    """La classificazione guarda anche il NOME del tipo di eccezione,
    non solo il messaggio."""
    class UnauthorizedError(Exception):
        pass

    class ConnectionResetError2(Exception):
        pass

    assert classify_error(UnauthorizedError("boom")) == ErrorClass.AUTH
    assert classify_error(ConnectionResetError2("x")) == ErrorClass.TRANSIENT
