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
def test_tick_skips_when_no_client():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = None
    assert svc._keepalive_tick() is True


@pytest.mark.unit
def test_tick_generic_error_is_soft_and_continues():
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="BOOM_NETWORK")

    called = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:called.append(reason)

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
    svc.handle_session_expiry = lambda reason="", abort_if=None:called.append(reason) or {"recovered": False}

    assert svc._keepalive_tick() is False  # esce dal loop
    assert called == ["KEEPALIVE_SESSION_EXPIRED"]
    assert svc.keepalive_status()["failure_count"] == 1


@pytest.mark.unit
def test_tick_session_expired_during_shutdown_no_reauth():
    """Greptile/Codex P1: un tick in volo che fallisce con SESSION_EXPIRED mentre
    e' in corso uno shutdown (stop-event settato) NON deve ri-autenticare (niente
    nuova sessione live dopo uno stop intenzionale)."""
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="SESSION_EXPIRED")
    called = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:called.append(reason)

    ev = threading.Event()
    ev.set()
    assert svc._keepalive_tick(ev) is False
    assert called == []  # shutdown in corso: nessun re-auth


@pytest.mark.unit
def test_tick_session_expired_when_client_swapped_no_reauth():
    """Codex P1: se durante il keep_alive il client live corrente cambia
    (disconnect/switch a SIMULATION/re-auth gia' avvenuto), un tick stale NON deve
    ri-autenticare il client vecchio."""
    svc = _make_service()
    svc.simulation_mode = False
    stale = _KAClient(keep_alive_raises="SESSION_EXPIRED")

    def _swap_then_raise():
        svc.client = _KAClient()  # il client corrente non e' piu' quello catturato
        raise RuntimeError("SESSION_EXPIRED")

    stale.keep_alive = _swap_then_raise
    svc.client = stale
    called = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:called.append(reason)

    assert svc._keepalive_tick() is False
    assert called == []  # client cambiato: nessun re-auth


@pytest.mark.unit
def test_spaced_session_error_routes_reauth():
    """Greptile P1: un errore di sessione con SPAZIO ("session expired") deve
    comunque essere classificato come session-expiry e instradare il re-auth."""
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="API_ERROR: SESSION EXPIRED")
    called = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:called.append(reason) or {}

    assert svc._keepalive_tick() is False
    assert called == ["KEEPALIVE_SESSION_EXPIRED"]


@pytest.mark.unit
def test_start_after_stop_restarts_with_new_thread():
    """Codacy: dopo un _stop (come nel ramo re-auth: disconnect azzera il ref del
    thread) un _start crea un NUOVO thread — il restart dopo re-auth funziona,
    non termina permanentemente."""
    svc = _make_service()
    svc._keepalive_interval = 60.0
    svc._start_session_keepalive()
    t1 = svc._keepalive_thread
    assert t1 is not None and t1.is_alive()

    svc._stop_session_keepalive()  # come disconnect()->_stop nel re-auth
    assert svc._keepalive_thread is None

    svc._start_session_keepalive()  # come _connect_live->_start dopo il login
    t2 = svc._keepalive_thread
    assert t2 is not None and t2.is_alive() and t2 is not t1
    svc._stop_session_keepalive()


@pytest.mark.unit
def test_keepalive_tick_stale_generation_is_noop():
    """CodeRabbit Major: un tick con generation stale (worker sopravvissuto a uno
    stop/reconnect) NON tocca metriche ne' instrada re-auth."""
    svc = _make_service()
    svc.simulation_mode = False
    svc.client = _KAClient(keep_alive_raises="SESSION_EXPIRED")
    reauth = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:reauth.append(reason)
    svc._keepalive_generation = 7
    before = svc.keepalive_status()["failure_count"]

    # il worker era stato avviato con generation=5 -> ora e' stale
    assert svc._keepalive_tick(generation=5) is False
    assert reauth == []
    assert svc.keepalive_status()["failure_count"] == before


@pytest.mark.unit
def test_keepalive_tick_goes_stale_during_blocking_call_is_noop():
    """CodeRabbit Major: keep_alive() puo' bloccarsi a lungo (fino al timeout del
    client). Se durante la chiamata uno stop/reconnect bumpa la generation, il
    completamento (successo o fallimento) NON deve sporcare le metriche ne'
    instradare un re-auth sulla NUOVA sessione."""
    svc = _make_service()
    svc.simulation_mode = False
    reauth = []
    svc.handle_session_expiry = lambda reason="", abort_if=None:reauth.append(reason)

    class _StaleDuringCall:
        def keep_alive(self_inner):
            svc._keepalive_generation += 1  # disconnect/reconnect concorrente
            raise RuntimeError("SESSION_EXPIRED")

    svc.client = _StaleDuringCall()
    gen = svc._keepalive_generation
    before = svc.keepalive_status()["failure_count"]

    assert svc._keepalive_tick(generation=gen) is False
    assert reauth == []  # worker stale: nessun re-auth sulla nuova sessione
    assert svc.keepalive_status()["failure_count"] == before  # nessuna metrica stale


@pytest.mark.unit
def test_handle_session_expiry_dedup_when_recovered_while_waiting():
    """Greptile P1 (race): se un altro thread completa il re-auth mentre
    attendiamo _reauth_lock (epoch avanzata, sessione valida), NON ri-autentichiamo
    di nuovo (evita che una recovery distrugga il client fresco dell'altra)."""
    svc = _make_service()
    svc.connected = True
    svc._session_invalid = False
    inner = []
    svc._do_handle_session_expiry = lambda reason: inner.append(reason)

    class _BumpLock:
        def __init__(self, s):
            self._s = s
            self._l = threading.Lock()

        def __enter__(self):
            self._s._reauth_epoch += 1  # simula recovery concorrente durante l'attesa
            return self._l.__enter__()

        def __exit__(self, *a):
            return self._l.__exit__(*a)

    svc._reauth_lock = _BumpLock(svc)

    out = svc.handle_session_expiry("X")
    assert out.get("already_recovered") is True
    assert inner == []  # niente re-auth ridondante


@pytest.mark.unit
def test_handle_session_expiry_aborts_when_stale_guard_true():
    """Codex round-2 P1: il guard stale (abort_if) e' valutato SOTTO _reauth_lock,
    atomico con la decisione di reconnect: se tra il rilevamento dell'expiry e
    l'acquisizione del lock e' avvenuto un disconnect/switch, il re-auth aborta
    invece di ricreare una sessione LIVE."""
    svc = _make_service()
    inner = []
    svc._do_handle_session_expiry = lambda reason: inner.append(reason)

    out = svc.handle_session_expiry("X", abort_if=lambda: True)
    assert out.get("aborted_stale") is True
    assert inner == []  # nessun re-auth: stato stale rilevato sotto lock


@pytest.mark.unit
def test_handle_session_expiry_runs_reauth_when_guard_false():
    svc = _make_service()
    inner = []
    svc._do_handle_session_expiry = lambda reason: inner.append(reason) or {"recovered": True}

    out = svc.handle_session_expiry("X", abort_if=lambda: False)
    assert inner == ["X"]
    assert out == {"recovered": True}


@pytest.mark.unit
def test_handle_session_expiry_runs_reauth_when_not_recovered():
    svc = _make_service()
    svc.connected = True
    svc._session_invalid = False
    inner = []
    svc._do_handle_session_expiry = lambda reason: inner.append(reason) or {"recovered": True}

    out = svc.handle_session_expiry("X")
    assert inner == ["X"]  # nessun recovery concorrente: esegue il re-auth
    assert out == {"recovered": True}


@pytest.mark.unit
def test_is_session_expiry_error_classification():
    assert BetfairService._is_session_expiry_error("SESSION_EXPIRED") is True
    assert BetfairService._is_session_expiry_error("err: INVALID_SESSION") is True
    assert BetfairService._is_session_expiry_error("NO_SESSION token") is True
    assert BetfairService._is_session_expiry_error("SESSION EXPIRED") is True
    assert BetfairService._is_session_expiry_error("api error: invalid session") is True
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

    def _fake_expiry(reason="", abort_if=None):
        svc._stop_session_keepalive()  # dal thread di keepalive: no self-join
        done.set()
        return {"recovered": False}

    svc.handle_session_expiry = _fake_expiry

    svc._start_session_keepalive()
    assert done.wait(timeout=3.0) is True  # nessun deadlock
    assert svc._keepalive_thread is None
