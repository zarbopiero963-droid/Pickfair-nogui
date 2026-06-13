"""PR-I programma test hedge-fund grade: redazione segreti nelle stringhe d'errore.

La redazione STRUTTURATA esiste gia' ed e' testata (sanitize_value per
chiave su export/snapshot/alert + cifratura at-rest dei campi segreti).
Il gap reale, provato da questa suite, e' nelle STRINGHE d'errore grezze
(fix autorizzato dall'owner su betfair_client.py):

1. login() sollevava "LOGIN_FAILED: {data}" con l'INTERA risposta di
   login di Betfair dentro il messaggio: se la risposta contiene un
   sessionToken finisce in log, last_error e status snapshot.
2. Le eccezioni di rete passano intatte in _record_io(error=...) ->
   io_snapshot()["last_error"] -> get_status()["runtime_io"]: un token
   dentro il testo dell'eccezione arriva dritto all'observability.

Contratto nuovo: il valore del session token corrente e' mascherato
nelle stringhe d'errore registrate/loggate; LOGIN_FAILED riporta solo
loginStatus. La classificazione errori resta intatta (marker testuali
TIMEOUT/NETWORK_ERROR invariati).

Dedup verificato: sanitizer ricorsivo, export json/csv, snapshot DB,
alert Telegram e cifratura at-rest sono GIA' coperti
(test_sanitizers_coverage, test_export_sanitization,
test_secret_cipher_*); qui solo le stringhe d'errore + il contratto
correlation_id-non-redatto.
"""
from __future__ import annotations

import pytest
import requests

from betfair_client import BetfairClient
from observability.sanitizers import sanitize_value

SECRET_TOKEN = "TOK-SUPER-SECRET-1234567890"


class FakeResponse:
    def __init__(self, *, json_data=None):
        self.status_code = 200
        self._json_data = json_data

    @staticmethod
    def raise_for_status():
        return None

    def json(self):
        return self._json_data


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)

    def post(self, url, **kwargs):
        _ = url, kwargs
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _client(session):
    client = BetfairClient(
        username="user",
        app_key="app",
        cert_pem="cert.pem",
        key_pem="key.pem",
        session=session,
        max_retries=0,
    )
    return client


# ---------------------------------------------------------------------------
# BLOCK: il messaggio LOGIN_FAILED non deve contenere la risposta grezza
# ---------------------------------------------------------------------------

@pytest.mark.observability
def test_login_failed_error_never_echoes_raw_login_response():
    """Risposta di login fallita che (per difesa in profondita') contiene
    comunque un sessionToken: il messaggio d'errore NON deve fare eco
    alla risposta grezza — solo il loginStatus diagnostico."""
    session = FakeSession([
        FakeResponse(json_data={
            "loginStatus": "INVALID_APP_KEY",
            "sessionToken": SECRET_TOKEN,
        }),
    ])
    client = _client(session)
    client._cert_tuple = lambda: ("cert.pem", "key.pem")  # bypass FS check

    with pytest.raises(RuntimeError) as excinfo:
        client.login(password="pw-never-logged")

    message = str(excinfo.value)
    assert SECRET_TOKEN not in message, (
        "sessionToken della risposta di login finito nel messaggio d'errore"
    )
    assert "pw-never-logged" not in message
    assert "INVALID_APP_KEY" in message  # il diagnostico utile resta


# ---------------------------------------------------------------------------
# BLOCK: token dentro un'eccezione di rete -> mai in last_error/log/result
# ---------------------------------------------------------------------------

@pytest.mark.observability
def test_session_token_in_network_exception_is_redacted_everywhere(caplog):
    """Eccezione di rete il cui testo contiene il session token corrente:
    il token NON deve comparire in io_snapshot()['last_error'] (che arriva
    a get_status()['runtime_io']), nel dict d'errore ritornato, ne' nei log."""
    session = FakeSession([
        requests.exceptions.ConnectionError(
            f"connection reset during call with session {SECRET_TOKEN}"
        ),
    ])
    client = _client(session)
    client.session_token = SECRET_TOKEN

    with caplog.at_level("WARNING"):
        out = client.place_bet(
            market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
        )

    assert out["ok"] is False
    assert SECRET_TOKEN not in str(out.get("error", "")), (
        "token nel dict d'errore ritornato al motore"
    )
    snapshot = client.io_snapshot()
    assert SECRET_TOKEN not in str(snapshot.get("last_error", "")), (
        "token in io_snapshot: arriverebbe a get_status()['runtime_io']"
    )
    assert SECRET_TOKEN not in caplog.text, "token nei log"


@pytest.mark.observability
def test_redaction_keeps_error_classification_intact():
    """Controllo PASS: la redazione maschera solo il VALORE del token —
    i marker testuali (NETWORK_ERROR/TIMEOUT) e quindi classification e
    order_unknown restano invariati."""
    session = FakeSession([
        requests.exceptions.ConnectionError(f"reset {SECRET_TOKEN}"),
    ])
    client = _client(session)
    client.session_token = SECRET_TOKEN

    out = client.place_bet(
        market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
    )

    assert out["classification"] == "TRANSIENT"
    assert out["order_unknown"] is True
    assert "NETWORK_ERROR" in out["error"]


@pytest.mark.observability
def test_short_or_empty_token_does_not_break_redaction():
    """Controllo PASS: token vuoto o troppo corto (sotto la soglia di
    redazione) non causa sostituzioni spurie ne' eccezioni."""
    session = FakeSession([
        requests.exceptions.ConnectionError("reset by peer"),
    ])
    client = _client(session)
    client.session_token = "TOK"  # corto: mai redatto (eviterebbe falsi match)

    out = client.place_bet(
        market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
    )

    assert out["ok"] is False
    assert "reset by peer" in out["error"]


@pytest.mark.observability
def test_login_network_error_redacts_live_token_in_raised_message():
    """Re-login con un token vivo ancora in mano (Codex/Greptile P2): se
    l'eccezione di rete del login contiene il token corrente, il
    RuntimeError sollevato al chiamante (che puo' loggarlo) e last_error
    devono essere redatti."""
    session = FakeSession([
        requests.exceptions.ConnectionError(f"reset with live {SECRET_TOKEN}"),
    ])
    client = _client(session)
    client._cert_tuple = lambda: ("cert.pem", "key.pem")
    client.session_token = SECRET_TOKEN

    with pytest.raises(RuntimeError) as excinfo:
        client.login(password="pw")

    assert SECRET_TOKEN not in str(excinfo.value)
    assert "LOGIN_NETWORK_ERROR" in str(excinfo.value)
    assert SECRET_TOKEN not in str(client.io_snapshot().get("last_error", ""))


@pytest.mark.observability
def test_login_rotated_token_is_still_redacted_via_snapshot():
    """Stessa race di rotazione, lato login (CodeRabbit round 3): un altro
    thread azzera il token vivo durante il re-login — il token VECCHIO nel
    testo dell'eccezione deve comunque essere redatto (snapshot a inizio
    login)."""
    class _RotatingSession:
        def __init__(self, client_holder):
            self.client_holder = client_holder

        def post(self, url, **kwargs):
            _ = url, kwargs
            self.client_holder["client"].session_token = ""
            raise requests.exceptions.ConnectionError(
                f"reset with stale session {SECRET_TOKEN}"
            )

    holder = {}
    client = _client(_RotatingSession(holder))
    holder["client"] = client
    client._cert_tuple = lambda: ("cert.pem", "key.pem")
    client.session_token = SECRET_TOKEN

    with pytest.raises(RuntimeError) as excinfo:
        client.login(password="pw")

    assert SECRET_TOKEN not in str(excinfo.value)
    assert SECRET_TOKEN not in str(client.io_snapshot().get("last_error", ""))


@pytest.mark.observability
def test_rotated_token_is_still_redacted_via_request_snapshot():
    """Race di rotazione (CodeRabbit): un altro thread azzera/ruota il token
    tra l'invio e la gestione dell'eccezione — il token VECCHIO nel testo
    dell'eccezione deve comunque essere redatto (snapshot per-tentativo)."""
    class _RotatingSession:
        def __init__(self, client_holder):
            self.client_holder = client_holder

        def post(self, url, **kwargs):
            _ = url, kwargs
            # Rotazione concorrente simulata PRIMA dell'eccezione.
            self.client_holder["client"].session_token = ""
            raise requests.exceptions.ConnectionError(
                f"reset with stale session {SECRET_TOKEN}"
            )

    holder = {}
    client = _client(_RotatingSession(holder))
    holder["client"] = client
    client.session_token = SECRET_TOKEN

    out = client.place_bet(
        market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
    )

    assert SECRET_TOKEN not in str(out.get("error", ""))
    assert SECRET_TOKEN not in str(client.io_snapshot().get("last_error", ""))


@pytest.mark.observability
def test_overlapping_tokens_are_redacted_longest_first():
    """Codex round 4: se il token ruotato e quello dello snapshot si
    sovrappongono (uno e' prefisso dell'altro), sostituire prima il corto
    lascerebbe un residuo parziale del lungo (es. ***SESSION_TOKEN***890).
    La redazione deve procedere dal token piu' lungo."""
    short_token = SECRET_TOKEN[:12]
    remnant = SECRET_TOKEN[12:]

    class _RotatingSession:
        def __init__(self, client_holder):
            self.client_holder = client_holder

        def post(self, url, **kwargs):
            _ = url, kwargs
            # Rotazione verso un token che e' PREFISSO di quello vecchio.
            self.client_holder["client"].session_token = short_token
            raise requests.exceptions.ConnectionError(
                f"reset with stale session {SECRET_TOKEN}"
            )

    holder = {}
    client = _client(_RotatingSession(holder))
    holder["client"] = client
    client.session_token = SECRET_TOKEN

    out = client.place_bet(
        market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
    )

    assert SECRET_TOKEN not in str(out.get("error", ""))
    assert remnant not in str(out.get("error", "")), (
        "residuo parziale del token lungo dopo la sostituzione del corto"
    )
    assert remnant not in str(client.io_snapshot().get("last_error", ""))


@pytest.mark.observability
def test_login_error_cause_chain_does_not_render_token():
    """Codex round 4: il messaggio era redatto ma 'raise ... from exc'
    conservava in __cause__ l'eccezione originale col token — chiunque
    renderizzi il traceback (logger.exception) lo avrebbe stampato. La
    catena deve essere soppressa."""
    import traceback

    session = FakeSession([
        requests.exceptions.ConnectionError(f"reset with live {SECRET_TOKEN}"),
    ])
    client = _client(session)
    client._cert_tuple = lambda: ("cert.pem", "key.pem")
    client.session_token = SECRET_TOKEN

    with pytest.raises(RuntimeError) as excinfo:
        client.login(password="pw")

    assert excinfo.value.__cause__ is None
    rendered = "".join(traceback.format_exception(excinfo.value))
    assert SECRET_TOKEN not in rendered, (
        "token grezzo nel traceback renderizzato via catena delle cause"
    )


@pytest.mark.observability
def test_login_timeout_cause_chain_does_not_render_token():
    """Codex round 5: anche l'handler Timeout del login conservava la causa
    grezza via 'from exc' — un token nel testo del timeout sarebbe finito
    nel traceback renderizzato nonostante il messaggio costante."""
    import traceback

    session = FakeSession([
        requests.exceptions.Timeout(f"timed out with session {SECRET_TOKEN}"),
    ])
    client = _client(session)
    client._cert_tuple = lambda: ("cert.pem", "key.pem")
    client.session_token = SECRET_TOKEN

    with pytest.raises(RuntimeError) as excinfo:
        client.login(password="pw")

    assert "LOGIN_TIMEOUT" in str(excinfo.value)
    assert excinfo.value.__cause__ is None
    rendered = "".join(traceback.format_exception(excinfo.value))
    assert SECRET_TOKEN not in rendered


@pytest.mark.observability
def test_api_error_payload_with_reflected_token_is_redacted():
    """Codacy (corpo review, outside diff): il branch API_ERROR sollevava il
    payload d'errore grezzo della risposta Betfair — un token riflesso
    dall'API passerebbe non redatto a log/circuit breaker. I marker di
    sessione (INVALID_SESSION/NO_SESSION) restano riconoscibili."""
    session = FakeSession([
        FakeResponse(json_data=[{
            "error": {"code": -32099, "data": f"rejected for session {SECRET_TOKEN}"},
        }]),
    ])
    client = _client(session)
    client.session_token = SECRET_TOKEN

    out = client.place_bet(
        market_id="1.234", selection_id=1, side="BACK", price=2.0, size=2.0,
    )

    assert out["ok"] is False
    assert "API_ERROR" in str(out.get("error", ""))
    assert SECRET_TOKEN not in str(out.get("error", ""))
    assert SECRET_TOKEN not in str(client.io_snapshot().get("last_error", ""))


# ---------------------------------------------------------------------------
# correlation_id: il sanitizer NON deve redarlo (e' il filo del forensics)
# ---------------------------------------------------------------------------

@pytest.mark.observability
def test_sanitizer_preserves_correlation_id_and_redacts_secrets():
    payload = {
        "correlation_id": "corr-abc-123",
        "password": "super-secret",
        "details": {
            "correlation_id": "corr-abc-123",
            "session_token": SECRET_TOKEN,
        },
    }

    cleaned = sanitize_value(payload)

    assert cleaned["correlation_id"] == "corr-abc-123"
    assert cleaned["details"]["correlation_id"] == "corr-abc-123"
    assert cleaned["password"] != "super-secret"
    assert cleaned["details"]["session_token"] != SECRET_TOKEN
