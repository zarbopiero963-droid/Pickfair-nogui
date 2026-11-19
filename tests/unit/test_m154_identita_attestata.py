"""M154-bis (owner, single-account) — F1 di Claude D: identita' ATTESTATA sul vero `BetfairClient` con HTTP simulato.

`authenticated_identity()` vale lo username INVIATO nell'ultimo login riuscito di questa sessione, e None:
mai loggato, login fallito o in errore, logout, sessione invalida/scaduta, keep-alive fallito, token o `connected` azzerati
dal servizio. `client.username` e' configurazione: cambiarlo dopo il login non cambia l'identita'. Nessun percorso fuori da
`login()` la imposta (guardia AST). Il mutante «identita' nel costruttore» si applica al sorgente fuori da questo file e
deve far fallire `test_f1_mai_loggato_nessuna_identita_anche_con_username` (certificazione, prova 7).
Nessuna rete: la sessione HTTP e' un finto `requests.Session`; il controllo del certificato su disco e' l'unico punto
sostituito (`_cert_tuple`), come negli altri test del client."""
from __future__ import annotations

import ast
import inspect
import textwrap
from datetime import datetime, timedelta, timezone

import pytest
from requests.exceptions import HTTPError, Timeout

import betfair_client
from betfair_client import BetfairClient

pytestmark = [pytest.mark.unit, pytest.mark.safety]
IDENTITA_REALE = True

A = "conto-a@example.invalid"
B = "conto-b@example.invalid"


class _Risposta:
    def __init__(self, data, status=200):
        self._data, self.status_code = data, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self._data


class _Http:
    """Finto requests.Session: risposte in coda per URL (identity / keepAlive / JSON-RPC); registra i corpi inviati."""

    def __init__(self):
        self.code, self.inviati = [], []

    def accoda(self, risposta):
        self.code.append(risposta)

    def post(self, url, headers=None, data=None, cert=None, timeout=None, **kw):
        self.inviati.append((url, data))
        r = self.code.pop(0)
        if isinstance(r, BaseException):
            raise r
        if callable(r):
            return r(url, data)
        return r


def _client(username=A):
    http = _Http()
    c = BetfairClient(username=username, app_key="app", cert_pem="cert.pem", key_pem="key.pem", session=http)
    c._cert_tuple = lambda: ("cert.pem", "key.pem")          # nessun file di certificato nei test (come gli altri)
    return c, http


def _login_ok(c, http, token="TOK-1", expiry=""):
    http.accoda(_Risposta({"loginStatus": "SUCCESS", "sessionToken": token, "sessionExpiryTime": expiry}))
    return c.login("pw")


def test_f1_mai_loggato_nessuna_identita_anche_con_username():
    c, _ = _client()
    assert c.username == A
    assert c.authenticated_identity() is None, "identita' disponibile senza login (mutante: identita' nel costruttore)"


def test_f1_login_riuscito_attesta_lo_username_inviato_e_lo_username_modificato_non_conta():
    c, http = _client()
    _login_ok(c, http)
    assert http.inviati[0][1]["username"] == A
    assert c.authenticated_identity() == A
    c.username = B                                   # configurazione manomessa dopo il login
    assert c.authenticated_identity() == A, "lo username modificato dopo il login ha cambiato l'identita'"


def test_f1_identita_e_lo_username_inviato_non_quello_letto_dopo():
    c, http = _client()

    def risposta(url, data):
        c.username = B                               # cambiato DURANTE la richiesta di login
        return _Risposta({"loginStatus": "SUCCESS", "sessionToken": "TOK"})
    http.accoda(risposta)
    c.login("pw")
    assert c.authenticated_identity() == A


@pytest.mark.parametrize("guasto", ["login_fallito", "timeout", "http", "json_non_valido"])
def test_f1_login_fallito_dopo_un_login_riuscito_nessuna_identita(guasto):
    c, http = _client()
    _login_ok(c, http)
    assert c.authenticated_identity() == A
    c.username = B
    if guasto == "login_fallito":
        http.accoda(_Risposta({"loginStatus": "INVALID_USERNAME_OR_PASSWORD"}))
    elif guasto == "timeout":
        http.accoda(Timeout("timeout"))
    elif guasto == "http":
        http.accoda(_Risposta({}, status=500))
    else:
        http.accoda(_Risposta(None))
    with pytest.raises(Exception):
        c.login("pw")
    assert c.authenticated_identity() is None, f"identita' ancora valida dopo {guasto}"


def test_f1_nuovo_login_riuscito_con_un_altro_username_attesta_il_nuovo():
    c, http = _client()
    _login_ok(c, http)
    c.username = B
    _login_ok(c, http, token="TOK-2")
    assert c.authenticated_identity() == B


def test_f1_logout_azzera():
    c, http = _client()
    _login_ok(c, http)
    c.logout()
    assert c.authenticated_identity() is None


def test_f1_sessione_invalida_dall_api_azzera():
    c, http = _client()
    _login_ok(c, http)
    http.accoda(_Risposta([{"error": {"code": "INVALID_SESSION_INFORMATION"}}]))
    with pytest.raises(RuntimeError, match="SESSION_EXPIRED"):
        c.get_account_funds()
    assert c.authenticated_identity() is None


def test_f1_scadenza_dichiarata_gia_passata_nessuna_identita():
    c, http = _client()
    passata = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    _login_ok(c, http, expiry=passata)
    assert c.authenticated_identity() is None
    futura = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
    _login_ok(c, http, token="TOK-2", expiry=futura)
    assert c.authenticated_identity() == A


@pytest.mark.parametrize("guasto", ["status_fallito", "timeout", "http", "rete", "senza_token"])
def test_f1_keep_alive_fallito_azzera_e_un_keep_alive_successivo_non_la_ripristina(guasto):
    from requests.exceptions import ConnectionError as _ConnErr
    c, http = _client()
    _login_ok(c, http)
    if guasto == "status_fallito":
        http.accoda(_Risposta({"status": "FAIL", "error": "INVALID_SESSION_INFORMATION"}))
    elif guasto == "timeout":
        http.accoda(Timeout("timeout"))
    elif guasto == "http":
        http.accoda(_Risposta({}, status=503))
    elif guasto == "rete":
        http.accoda(_ConnErr("giu'"))
    else:
        c._set_session_state(session_token="")
    with pytest.raises(RuntimeError):
        c.keep_alive()
    assert c.authenticated_identity() is None
    c._set_session_state(session_token="TOK-1", connected=True)
    http.accoda(_Risposta({"status": "SUCCESS"}))
    c.keep_alive()
    assert c.authenticated_identity() is None, "un keep-alive riuscito ha ripristinato un'identita' non piu' provata"


def test_f1_keep_alive_riuscito_conserva():
    c, http = _client()
    _login_ok(c, http)
    http.accoda(_Risposta({"status": "SUCCESS"}))
    c.keep_alive()
    assert c.authenticated_identity() == A


def test_f1_token_o_connected_azzerati_dal_servizio_nessuna_identita():
    c, http = _client()
    _login_ok(c, http)
    c.connected = False                              # come BetfairService.handle_session_expiry
    assert c.authenticated_identity() is None
    c2, http2 = _client()
    _login_ok(c2, http2)
    c2.session_token = ""
    assert c2.authenticated_identity() is None


def test_f1_nessun_percorso_fuori_da_login_imposta_un_identita_non_vuota():
    """Guardia AST: `_authenticated_identity` si assegna solo in `__init__` (vuota) e in `_set_session_state`; la chiamata
    con un valore non costante-vuoto avviene SOLO dentro `login()`."""
    albero = ast.parse(textwrap.dedent(inspect.getsource(BetfairClient)))
    violazioni = []
    for funzione in [n for n in ast.walk(albero) if isinstance(n, ast.FunctionDef)]:
        for nodo in ast.walk(funzione):
            if isinstance(nodo, ast.Attribute) and nodo.attr == "_authenticated_identity" \
                    and isinstance(nodo.ctx, ast.Store):
                if funzione.name not in ("__init__", "_set_session_state"):
                    violazioni.append(f"assegnazione in {funzione.name}")
                if funzione.name == "__init__":
                    padre = next(a for a in ast.walk(funzione) if isinstance(a, ast.Assign) and nodo in a.targets)
                    if not (isinstance(padre.value, ast.Constant) and padre.value.value == ""):
                        violazioni.append("__init__ assegna un'identita' non vuota")
            if isinstance(nodo, ast.Call) and getattr(nodo.func, "attr", "") == "_set_session_state":
                for kw in nodo.keywords:
                    if kw.arg == "authenticated_identity" and not (
                            isinstance(kw.value, ast.Constant) and kw.value.value == "") and funzione.name != "login":
                        violazioni.append(f"identita' non vuota impostata in {funzione.name}")
    assert not violazioni, violazioni


def test_f1_il_modulo_espone_esattamente_l_api_fissata():
    firma = inspect.signature(BetfairClient.authenticated_identity)
    assert list(firma.parameters) == ["self"]
    assert betfair_client.BetfairClient.authenticated_identity.__doc__
