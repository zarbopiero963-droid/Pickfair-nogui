"""
Tests for the betting-session keepalive (B2 / Fase 1.2).

Verifies:
- _keepalive_tick() calls client.keep_alive() only when LIVE + session valid
- tick skips in simulation, when session invalid, or when client missing
- a generic keep_alive() error is soft (continue + counter), no re-auth
- a SESSION_EXPIRED error routes to handle_session_expiry() and exits the loop
- start/stop lifecycle + idempotent start
- _stop_session_keepalive() is re-entrancy-safe (no self-join deadlock) when
  invoked from within the keepalive thread (the re-auth path)
"""

import threading

import pytest

from services.betfair_service import BetfairService


class _Settings:
    def load_betfair_config(self):
        class Cfg:
            username = "user"
            app_key = "key"
            certificate = "cert"
            private_key = "pk"
        return Cfg()

    def load_password(self):
        return "pw"


class _KAClient:
    def __init__(self, keep_alive_raises=None):
        self.keep_alive_calls = 0
        self._raises = keep_alive_raises

    def keep_alive(self):
        self.keep_alive_calls += 1
        if self._raises:
            raise RuntimeError(self._raises)
        return {"ok": True}

    def logout(self):
        return {"ok": True}


def _make_service():
    return BetfairService(_Settings())


@pytest.mark.unit
def test_tick_calls_keep_alive_when_live_and_valid():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = False
    client = _KAClient()
    svc.client = client

    assert svc._keepalive_tick() is True
    assert client.keep_alive_calls == 1
    status = svc.keepalive_status()
    assert status["last_error"] == ""
    assert status["last_ok_ts"] != ""


@pytest.mark.unit
def test_tick_skips_in_simulation():
    svc = _make_service()
    svc.simulation_mode = True
    client = _KAClient()
    svc.client = client

    assert svc._keepalive_tick() is True
    assert client.keep_alive_calls == 0


@pytest.mark.unit
def test_tick_skips_when_session_invalid():
    svc = _make_service()
    svc.simulation_mode = False
    svc._session_invalid = True
    client = _KAClient()
    svc.client = client

    assert svc._keepalive_tick() is True
    assert client.keep_alive_calls == 0


@pytest.mark.unit
def test_tick_skips_when_no_client_or_no_method():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = None
    assert svc._keepalive_tick() is True

    svc.client = object()  # nessun keep_alive callable
    assert svc._keepalive_tick() is True


@pytest.mark.unit
def test_tick_generic_error_is_soft_and_continues():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="BOOM_NETWORK")

    called = []
    svc.handle_session_expiry = lambda reason="": called.append(reason)

    assert svc._keepalive_tick() is True  # continua il loop
    assert svc.keepalive_status()["failure_count"] == 1
    assert svc.keepalive_status()["last_error"] == "BOOM_NETWORK"
    assert called == []  # nessun re-auth per un errore generico


@pytest.mark.unit
def test_tick_session_expired_routes_reauth_and_exits():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="SESSION_EXPIRED")

    called = []
    svc.handle_session_expiry = lambda reason="": called.append(reason) or {"recovered": False}

    assert svc._keepalive_tick() is False  # esce dal loop
    assert called == ["KEEPALIVE_SESSION_EXPIRED"]
    assert svc.keepalive_status()["failure_count"] == 1


@pytest.mark.unit
def test_is_session_expiry_error_classification():
    assert BetfairService._is_session_expiry_error("SESSION_EXPIRED") is True
    assert BetfairService._is_session_expiry_error("err: INVALID_SESSION") is True
    assert BetfairService._is_session_expiry_error("NO_SESSION token") is True
    assert BetfairService._is_session_expiry_error("TIMEOUT") is False
    assert BetfairService._is_session_expiry_error("") is False


@pytest.mark.unit
def test_start_stop_lifecycle_and_idempotent():
    svc = _make_service()
    svc._keepalive_interval = 60.0  # il thread resta in attesa, non ticka nel test

    assert svc.keepalive_status()["running"] is False
    svc._start_session_keepalive()
    assert svc.keepalive_status()["running"] is True

    first_thread = svc._keepalive_thread
    svc._start_session_keepalive()  # idempotente
    assert svc._keepalive_thread is first_thread

    svc._stop_session_keepalive()
    assert svc.keepalive_status()["running"] is False


@pytest.mark.unit
def test_stop_session_keepalive_no_self_join_deadlock():
    """Il re-auth viene instradato DAL thread di keepalive (handle_session_expiry
    -> _connect_live(force) -> disconnect -> _stop_session_keepalive): lo stop NON
    deve fare self-join, altrimenti il thread si bloccherebbe su se stesso."""
    svc = _make_service()
    result = {}

    def worker():
        svc._keepalive_thread = threading.current_thread()
        svc._keepalive_stop_event = threading.Event()
        svc._stop_session_keepalive()  # chiamato dal "proprio" thread
        result["done"] = True

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=3.0)
    assert result.get("done") is True
    assert not t.is_alive()
    assert svc._keepalive_thread is None


@pytest.mark.unit
def test_running_keepalive_reauth_from_loop_does_not_deadlock():
    """End-to-end: con un thread di keepalive REALE (intervallo minimo) il cui
    keep_alive solleva SESSION_EXPIRED, il re-auth (qui simulato) chiama lo stop
    dal thread stesso: il test deve completare senza deadlock e il thread uscire."""
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="SESSION_EXPIRED")
    svc._keepalive_interval = 0.01

    done = threading.Event()

    def _fake_expiry(reason=""):
        svc._stop_session_keepalive()  # dal thread di keepalive: no self-join
        done.set()
        return {"recovered": False}

    svc.handle_session_expiry = _fake_expiry

    svc._start_session_keepalive()
    assert done.wait(timeout=3.0) is True  # nessun deadlock
    assert svc._keepalive_thread is None
