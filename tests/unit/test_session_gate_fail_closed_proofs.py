"""Proof fail-closed del session gate (test_suite_proof_session_gate).

Phase 0 dedup (verificato): il session gate di BetfairService e' GIA' coperto
in modo forte da 17 test in tests/unit/test_session_expiry_recovery.py
(handle_session_expiry marca invalid/blocca, is_live_usable, ensure_connected
raises su live+invalid, re-auth bounded success/fail, max-attempts, no-password
con stringa vuota, get_account_funds recovery, place_order bloccato su invalid,
place_order rileva SESSION_EXPIRED dalla RISPOSTA ok=False, no-false-positive,
stream gate, signal-reject nel RuntimeController). Scrivere quelli sarebbe
duplicazione.

Qui SOLO i gap fail-closed netto-nuovi, caratterizzati sul comportamento REALE
(probe eseguito prima di scrivere):

1. load_password() che SOLLEVA un'eccezione (keystore corrotto/indisponibile):
   deve restare bloccato senza crash e SENZA tentare re-auth — il test esistente
   copre solo il ritorno di stringa vuota "".
2. load_password() che ritorna None (record assente, distinto da ""): trattato
   come password mancante -> bloccato, nessun re-auth.
3. password di soli spazi "   ": caratterizzazione del confine — NON e' trattata
   come mancante, viene passata a _connect_live (re-auth tentato) e su login
   fallito resta comunque bloccato (fail-closed nel risultato).
4. ritorno sim<->live: con sessione invalida si puo' ripiegare in simulazione,
   ma un successivo rientro in LIVE resta RIFIUTATO (il flag invalid persiste,
   non viene azzerato dal passaggio a simulazione).
5. place_order su sessione gia' invalida NON re-invoca handle_session_expiry
   (niente doppio recovery / loop di re-auth) — il test esistente verifica che
   resta bloccato ma non che il recovery non venga richiamato.
6. place_order, ramo ECCEZIONE: se client.place_bet SOLLEVA SESSION_EXPIRED il
   recovery viene invocato e l'eccezione e' ri-sollevata — l'esistente copre
   solo il ramo RISPOSTA (ok=False).

Tutto pure-unit e deterministico, nessuna modifica al codice di produzione
(services/betfair_service.py e' file critico: solo test).
"""
from __future__ import annotations

import pytest

from services.betfair_service import BetfairService

# ---------------------------------------------------------------------------
# Stub coerenti con tests/unit/test_session_expiry_recovery.py
# ---------------------------------------------------------------------------

class _Settings:
    """settings_service con comportamento di load_password() configurabile.

    password_behavior:
      - "raise": load_password() solleva un'eccezione (keystore indisponibile)
      - qualunque altro valore (None, "", "   ", "secret"): viene ritornato
    """

    _RAISE = object()

    def __init__(self, password_behavior):
        self._behavior = password_behavior

    def load_betfair_config(self):
        class Cfg:
            username = "user"
            app_key = "key"
            certificate = "cert"
            private_key = "pk"
        return Cfg()

    def load_password(self):
        if self._behavior == "raise":
            raise RuntimeError("keystore indisponibile")
        return self._behavior


# ---------------------------------------------------------------------------
# 1-2. RE-AUTH FAIL-CLOSED SU PASSWORD NON DISPONIBILE (raise / None)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_reauth_blocked_when_password_load_raises():
    """Se load_password() solleva (keystore corrotto/indisponibile) il recovery
    NON deve crashare ne' fare fail-open: resta bloccato, nessun re-auth."""
    svc = BetfairService(_Settings("raise"))
    svc.connected = True

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    assert result["recovered"] is False
    assert result["reauth_attempted"] is False
    assert svc.is_session_invalid is True
    assert svc.is_live_usable() is False


@pytest.mark.unit
@pytest.mark.guardrail
def test_reauth_blocked_when_password_is_none():
    """load_password() == None (record assente, distinto da "") e' trattato come
    password mancante: bloccato, nessun tentativo di re-auth."""
    svc = BetfairService(_Settings(None))
    svc.connected = True

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    assert result["recovered"] is False
    assert result["reauth_attempted"] is False
    assert svc.is_session_invalid is True


# ---------------------------------------------------------------------------
# 3. CONFINE: password di soli spazi NON e' "mancante" (caratterizzazione)
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_whitespace_password_is_passed_through_then_fails_closed(monkeypatch):
    """Confine documentato: "   " e' truthy, quindi NON e' trattata come
    mancante — viene passata a _connect_live (re-auth TENTATO con il valore
    letterale). Se il login fallisce il servizio resta comunque bloccato
    (fail-closed nel risultato). Il test blocca una regressione che cambiasse
    silenziosamente questo confine."""
    svc = BetfairService(_Settings("   "))
    svc.connected = True

    seen = {}

    def fake_connect_live(password=None, force=False):
        seen["password"] = password
        raise RuntimeError("login rifiutato")

    monkeypatch.setattr(svc, "_connect_live", fake_connect_live)

    result = svc.handle_session_expiry("SESSION_EXPIRED")

    # re-auth TENTATO, con la password whitespace passata letteralmente
    assert result["reauth_attempted"] is True
    assert seen["password"] == "   "
    # ma il login e' fallito -> resta bloccato (fail-closed nel risultato)
    assert result["recovered"] is False
    assert svc.is_session_invalid is True


# ---------------------------------------------------------------------------
# 4. RITORNO SIM<->LIVE: fallback a simulazione ok, rientro LIVE ancora rifiutato
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_sim_fallback_allowed_but_live_reentry_still_refused_while_invalid():
    """Con sessione invalida l'operatore puo' ripiegare in simulazione, ma un
    successivo rientro in LIVE resta RIFIUTATO: il flag invalid persiste e non
    viene azzerato dal passaggio a simulazione (niente live accidentale)."""
    from unittest.mock import MagicMock

    svc = BetfairService(_Settings("secret"))
    svc.simulation_mode = False
    svc._session_invalid = True
    svc._session_invalid_reason = "SESSION_EXPIRED"

    # (a) LIVE rifiutato all'inizio
    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.ensure_connected(simulation_mode=False)

    # (b) fallback a simulazione: consentito (la simulazione ignora la sessione).
    # Pre-impostiamo broker/connected cosi' ensure_connected ritorna senza
    # eseguire un connect reale.
    svc.simulation_mode = True
    svc.simulation_broker = MagicMock()
    svc.connected = True
    assert svc.ensure_connected(simulation_mode=True) is not None

    # (c) rientro in LIVE: ANCORA rifiutato, il flag invalid e' persistito
    assert svc.is_session_invalid is True
    with pytest.raises(RuntimeError, match="LIVE_BLOCKED_SESSION_INVALID"):
        svc.ensure_connected(simulation_mode=False)


# ---------------------------------------------------------------------------
# 5. NO DOPPIO RECOVERY: place_order su invalid non re-invoca handle_session_expiry
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_place_order_already_invalid_does_not_reinvoke_recovery(monkeypatch):
    """place_order() su sessione gia' invalida rifiuta subito SENZA richiamare
    handle_session_expiry: evita un loop di re-auth a ogni ordine bloccato."""
    svc = BetfairService(_Settings("secret"))
    svc._session_invalid = True
    svc._session_invalid_reason = "SESSION_EXPIRED"

    calls = []
    monkeypatch.setattr(
        svc, "handle_session_expiry",
        lambda *a, **k: calls.append((a, k)) or {"recovered": False},
    )

    result = svc.place_order({
        "market_id": "1.123", "selection_id": 456,
        "bet_type": "BACK", "price": 2.0, "stake": 10.0,
    })

    assert calls == [], "place_order non deve richiamare il recovery se gia' invalido"
    assert result["ok"] is False
    assert "LIVE_BLOCKED_SESSION_INVALID" in result["error"]
    assert result.get("session_invalid") is True


# ---------------------------------------------------------------------------
# 6. RAMO ECCEZIONE: place_bet che SOLLEVA SESSION_EXPIRED -> recovery + re-raise
# ---------------------------------------------------------------------------

@pytest.mark.unit
@pytest.mark.guardrail
def test_place_order_exception_path_triggers_recovery_and_reraises():
    """Se client.place_bet SOLLEVA SESSION_EXPIRED (ramo eccezione, distinto
    dalla risposta ok=False gia' coperta), place_order invoca il recovery e
    ri-solleva: il servizio resta bloccato e l'errore non viene inghiottito."""
    class _RaisingClient:
        def place_bet(self, **_kw):
            raise RuntimeError("SESSION_EXPIRED")

    svc = BetfairService(_Settings(""))  # nessuna password: recovery resta bloccato
    svc.connected = True
    svc.simulation_mode = False
    svc.client = _RaisingClient()

    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        svc.place_order({
            "market_id": "1.123", "selection_id": 456,
            "bet_type": "BACK", "price": 2.0, "stake": 10.0,
        })

    # recovery invocato dal ramo eccezione -> servizio bloccato
    assert svc.is_session_invalid is True
    assert svc.is_live_usable() is False
