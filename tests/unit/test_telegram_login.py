"""Test del login userbot Telegram reale (#367 follow-up).

`TelegramListener.request_code`/`sign_in` erano STUB: `sign_in` ignorava il
codice ed emetteva un finto "AUTHORIZED". Ora fanno un login Telethon reale
(telefono+codice+2FA) e generano una `session_string`. Il comando headless
`--telegram-login` (`HeadlessApp._telegram_login_flow`) guida il flusso e la
salva. La verifica end-to-end con l'API Telegram reale resta sul VPS.
"""

import sys
from types import SimpleNamespace

from telegram_listener import TelegramListener
from headless_main import HeadlessApp


# Eccezioni fake: il codice sotto test le riconosce per type(exc).__name__,
# quindi il NOME della classe deve combaciare con quelle reali di Telethon.
class SessionPasswordNeededError(Exception):
    pass


class PhoneCodeInvalidError(Exception):
    pass


class PhoneCodeExpiredError(Exception):
    pass


class PasswordHashInvalidError(Exception):
    pass


class FloodWaitError(Exception):
    # Il nome classe DEVE combaciare con quello reale di Telethon: request_code
    # riconosce il FloodWait via type(exc).__name__ ed espone `retry_after`.
    def __init__(self, seconds=None):
        super().__init__(f"A wait of {seconds} seconds is required")
        self.seconds = seconds


class _FakeSession:
    def __init__(self, s):
        self._s = s

    def save(self):
        return self._s


class _FakeClient:
    """Client Telethon fittizio (metodi async) per pilotare il login."""

    def __init__(self, *, behavior="ok", session="SESS", code_hash="HASH", password_ok=True, fail_on=None, flood_seconds=42):
        self.behavior = behavior  # ok | 2fa | invalid_code | expired_code
        self.session = _FakeSession(session)
        self.code_hash = code_hash
        self.password_ok = password_ok
        self.fail_on = fail_on  # None | "connect" | "send_code" | "flood"
        self.flood_seconds = flood_seconds
        self.calls = []
        self._authorized = False

    async def connect(self):
        self.calls.append(("connect",))
        if self.fail_on == "connect":
            raise RuntimeError("connect boom")

    async def disconnect(self):
        self.calls.append(("disconnect",))

    async def send_code_request(self, phone):
        self.calls.append(("send_code_request", phone))
        if self.fail_on == "send_code":
            raise RuntimeError("send_code boom")
        if self.fail_on == "flood":
            raise FloodWaitError(seconds=self.flood_seconds)
        return SimpleNamespace(phone_code_hash=self.code_hash)

    async def is_user_authorized(self):
        return self._authorized

    async def sign_in(self, **kwargs):
        self.calls.append(("sign_in", kwargs))
        if "password" in kwargs:
            if self.password_ok:
                self._authorized = True
                return
            raise PasswordHashInvalidError()
        if self.behavior == "ok":
            self._authorized = True
            return
        if self.behavior == "2fa":
            raise SessionPasswordNeededError()
        if self.behavior == "invalid_code":
            raise PhoneCodeInvalidError()
        if self.behavior == "expired_code":
            raise PhoneCodeExpiredError()


def _listener(fake):
    return TelegramListener(api_id=1, api_hash="h", client_factory=lambda a, b, s: fake)


# ---- TelegramListener: login reale (BLOCK sullo stub) -----------------------


def test_request_code_then_sign_in_returns_session_string():
    # BLOCK: lo stub ritornava {"ok": True} SENZA session_string e senza mai
    # chiamare client.sign_in. Ora il codice viene verificato davvero.
    fake = _FakeClient(behavior="ok", session="NEW-SESSION")
    lis = _listener(fake)

    assert lis.request_code("+3912345")["ok"] is True
    assert ("send_code_request", "+3912345") in fake.calls

    res = lis.sign_in("54321")
    assert res["ok"] is True
    assert res["session_string"] == "NEW-SESSION"
    assert lis.session_string == "NEW-SESSION"
    # Il codice è stato passato a client.sign_in (non ignorato come nello stub).
    assert any(c[0] == "sign_in" and c[1].get("code") == "54321" for c in fake.calls)


def test_sign_in_without_request_code_is_rejected():
    res = _listener(_FakeClient()).sign_in("123")
    assert res == {"ok": False, "error": "request_code_first"}


def test_sign_in_two_factor_flow():
    # sign_in(code) -> serve password; sign_in(code, pwd) -> ok.
    fake = _FakeClient(behavior="2fa", session="SESS-2FA")
    lis = _listener(fake)
    lis.request_code("+39")

    step1 = lis.sign_in("111")
    assert step1["ok"] is False
    assert step1["requires_password"] is True

    step2 = lis.sign_in("111", password_2fa="hunter2")
    assert step2["ok"] is True
    assert step2["session_string"] == "SESS-2FA"
    assert any(c[0] == "sign_in" and "password" in c[1] for c in fake.calls)


def test_sign_in_invalid_password_keeps_requiring_password():
    fake = _FakeClient(behavior="2fa", password_ok=False)
    lis = _listener(fake)
    lis.request_code("+39")
    lis.sign_in("111")  # -> requires_password
    res = lis.sign_in("111", password_2fa="wrong")
    assert res["ok"] is False
    assert res["error"] == "invalid_password"
    assert res["requires_password"] is True


def test_sign_in_invalid_code():
    fake = _FakeClient(behavior="invalid_code")
    lis = _listener(fake)
    lis.request_code("+39")
    res = lis.sign_in("000")
    assert res == {"ok": False, "error": "invalid_code"}


def test_sign_in_sanitizes_pasted_code_at_source():
    # BLOCK (F-1a login hardening): la sanitizzazione a sole cifre è nel listener,
    # così vale per TUTTI i chiamanti (GUI + headless). Incollare l'intero
    # messaggio 777000 "Login code: 54321" deve passare a client.sign_in "54321".
    fake = _FakeClient(behavior="ok", session="S")
    lis = _listener(fake)
    lis.request_code("+39")
    res = lis.sign_in("Login code: 54321")
    assert res["ok"] is True
    assert any(c[0] == "sign_in" and c[1].get("code") == "54321" for c in fake.calls), \
        "sign_in deve ricevere solo le cifre, non il messaggio intero"


def test_request_code_floodwait_returns_retry_after():
    # BLOCK (F-1a): request_code riconosce il FloodWait e ritorna un errore
    # strutturato con `retry_after` (secondi), così GUI e headless sanno dire
    # "attendi N s, non rilanciare" invece di un errore opaco.
    fake = _FakeClient(fail_on="flood")
    lis = _listener(fake)
    res = lis.request_code("+39")
    assert res["ok"] is False
    assert res["retry_after"] == 42
    assert "FloodWait" in res["error"] and "42" in res["error"]


def test_request_code_floodwait_none_seconds_no_literal_none():
    # BLOCK (Fable): se FloodWaitError non ha `seconds`, il messaggio NON deve
    # dire "attendi None secondi"; retry_after resta None (campo strutturato).
    fake = _FakeClient(fail_on="flood", flood_seconds=None)
    res = _listener(fake).request_code("+39")
    assert res["ok"] is False
    assert res["retry_after"] is None
    assert "None" not in res["error"] and "FloodWait" in res["error"]


def test_request_code_missing_phone():
    res = _listener(_FakeClient()).request_code("   ")
    assert res == {"ok": False, "error": "missing_phone"}


def test_sign_in_expired_code_cleans_up():
    # expired_code è terminale: sign_in ritorna l'errore e chiude il login.
    fake = _FakeClient(behavior="expired_code")
    lis = _listener(fake)
    lis.request_code("+39")
    res = lis.sign_in("999")
    assert res == {"ok": False, "error": "expired_code"}
    assert lis._login_client is None


def test_request_code_send_code_failure_cleans_up_client():
    # BLOCK (CodeRabbit Major): se send_code_request fallisce DOPO connect(), il
    # client (già connesso) dev'essere disconnesso da _cleanup_login. Prima del
    # fix il client era assegnato solo a esito positivo -> connessione orfana.
    fake = _FakeClient(fail_on="send_code")
    lis = _listener(fake)
    res = lis.request_code("+39")
    assert res["ok"] is False
    assert ("disconnect",) in fake.calls
    assert lis._login_client is None


def test_stop_cleans_up_abandoned_login():
    # BLOCK (Fugu/CodeRabbit #371): request_code senza sign_in NON deve lasciare
    # il client di login orfano; stop() lo chiude DAVVERO (disconnect + loop
    # chiuso + thread terminato), non solo azzerando i riferimenti.
    fake = _FakeClient(behavior="ok")
    lis = _listener(fake)
    assert lis.request_code("+39")["ok"] is True
    loop = lis._login_loop
    thread = lis._login_thread
    assert lis._login_client is not None
    lis.stop()
    assert lis._login_client is None
    assert ("disconnect",) in fake.calls
    assert loop.is_closed()
    assert not thread.is_alive()


# ---- headless --telegram-login: flusso interattivo --------------------------


def _seq(*values):
    """Callable che ignora l'argomento (prompt) e ritorna i valori in sequenza."""
    it = iter(values)
    return lambda *_a, **_k: next(it)


class _ScriptListener:
    def __init__(self, request_ok=True, sign_in_results=None, request_error="boom"):
        self.request_ok = request_ok
        self.request_error = request_error
        self.sign_in_results = list(sign_in_results or [])
        self.sign_in_calls = []

    def request_code(self, phone):
        self.phone = phone
        return {"ok": True} if self.request_ok else {"ok": False, "error": self.request_error}

    def sign_in(self, code, password_2fa=None):
        self.sign_in_calls.append((code, password_2fa))
        return self.sign_in_results.pop(0)


def test_login_flow_happy_path():
    lis = _ScriptListener(sign_in_results=[{"ok": True, "session_string": "SS"}])
    code, ss = HeadlessApp._telegram_login_flow(
        lis, "1", "h",
        prompt=_seq("+39", "12345"),
        prompt_secret=lambda p: "",
        out=lambda *_: None,
    )
    assert code == 0
    assert ss == "SS"


def test_login_flow_two_factor():
    lis = _ScriptListener(
        sign_in_results=[{"ok": False, "requires_password": True}, {"ok": True, "session_string": "SS2"}]
    )
    code, ss = HeadlessApp._telegram_login_flow(
        lis, "1", "h",
        prompt=_seq("+39", "12345"),
        prompt_secret=_seq("pwd"),
        out=lambda *_: None,
    )
    assert code == 0
    assert ss == "SS2"
    assert lis.sign_in_calls[1] == ("12345", "pwd")


def test_login_flow_missing_api_credentials():
    code, ss = HeadlessApp._telegram_login_flow(
        object(), "", "h",
        prompt=lambda p: "", prompt_secret=lambda p: "", out=lambda *_: None,
    )
    assert code == 2
    assert ss is None


def test_login_flow_request_code_failure():
    lis = _ScriptListener(request_ok=False)
    code, ss = HeadlessApp._telegram_login_flow(
        lis, "1", "h",
        prompt=_seq("+39"), prompt_secret=lambda p: "", out=lambda *_: None,
    )
    assert code == 2
    assert ss is None


def test_sanitize_login_code_extracts_digits():
    # BLOCK (login UX, #371): il messaggio 777000 è "Login code: 12345"; incollarlo
    # tutto dava "codice non valido". Ora si tengono solo le cifre.
    assert HeadlessApp._sanitize_login_code("Login code: 12345") == "12345"
    assert HeadlessApp._sanitize_login_code("  12345  ") == "12345"
    assert HeadlessApp._sanitize_login_code("1 2 3 4 5") == "12345"
    # nessuna cifra: ritorna il testo strip (errore comprensibile, non crash)
    assert HeadlessApp._sanitize_login_code("  abc ") == "abc"
    assert HeadlessApp._sanitize_login_code(None) == ""


def test_login_flow_sanitizes_pasted_code():
    # BLOCK: l'utente incolla l'intero messaggio; sign_in DEVE ricevere solo le
    # cifre. Sul vecchio flow (nessuna sanitizzazione) sign_in riceveva
    # "Login code: 12345" => codice non valido.
    lis = _ScriptListener(sign_in_results=[{"ok": True, "session_string": "SS"}])
    code, ss = HeadlessApp._telegram_login_flow(
        lis, "1", "h",
        prompt=_seq("+39", "Login code: 12345"),
        prompt_secret=_seq(""),
        out=lambda *_: None,
    )
    assert code == 0 and ss == "SS"
    assert lis.sign_in_calls[0] == ("12345", None), "sign_in deve ricevere solo le cifre"


def test_request_code_error_message_floodwait():
    # FloodWait: messaggio esplicito "attesa + NON rilanciare" (evita il loop che
    # invalida i codici). Errore generico: messaggio normale.
    flood = HeadlessApp._request_code_error_message("A wait of 3600 seconds is required")
    assert "NON rilanciare" in flood and "3600" in flood
    assert "Invio codice fallito" in HeadlessApp._request_code_error_message("boom")


def test_login_flow_floodwait_shows_wait_message():
    # BLOCK: request_code fallito per FloodWait => l'utente vede il messaggio di
    # attesa (non un errore opaco), il flow ritorna (2, None) e sign_in NON parte.
    lis = _ScriptListener(request_ok=False, request_error="A wait of 42 seconds is required")
    seen = []
    code, ss = HeadlessApp._telegram_login_flow(
        lis, "1", "h",
        prompt=_seq("+39"), prompt_secret=_seq(""), out=seen.append,
    )
    assert code == 2 and ss is None
    assert lis.sign_in_calls == [], "dopo un FloodWait il sign_in non deve partire"
    assert any("NON rilanciare" in str(m) and "42" in str(m) for m in seen)


def test_signin_error_message_invalid_code_guides_user():
    msg = HeadlessApp._signin_error_message("invalid_code")
    assert "ULTIMO codice" in msg and "solo le cifre" in msg
    assert "Login non riuscito" in HeadlessApp._signin_error_message("not_authorized")


def test_run_telegram_login_settings_read_error_returns_2():
    # BLOCK (CodeRabbit Major): se db.get_telegram_settings() solleva, il comando
    # --telegram-login deve rispettare il contratto 0/2 (return 2) e NON propagare
    # un traceback. Prima del fix la lettura dei settings era fuori da try/except,
    # quindi l'eccezione risaliva e il comando terminava con stacktrace (≠ 2).
    class _RaisingDB:
        def get_telegram_settings(self):
            raise RuntimeError("db boom")

    app = HeadlessApp.__new__(HeadlessApp)
    app.db = _RaisingDB()
    assert app._run_telegram_login() == 2


# ---- --telegram-login: credenziali headless via CLI/env (F-1 di #371) --------

_resolve = HeadlessApp._resolve_telegram_credentials


def test_resolve_creds_api_id_cli_wins_api_hash_from_env():
    # api_id: flag CLI vince su env/DB. api_hash: da env (MAI da CLI).
    api_id, api_hash, ext = _resolve(
        ["--telegram-login", "--api-id", "111"],
        {"TELEGRAM_API_ID": "222", "TELEGRAM_API_HASH": "H2"},
        {"api_id": "999", "api_hash": "DBHASH"},
    )
    assert (api_id, api_hash, ext) == ("111", "H2", True)


def test_resolve_creds_api_hash_never_read_from_cli():
    # SECURITY (GPT/Fugu/Fable): --api-hash sulla CLI NON deve essere letto
    # (esporrebbe il segreto in ps/history). Qui è passato in argv ma va IGNORATO:
    # api_hash arriva dal DB, e non marca from_external.
    _api_id, api_hash, ext = _resolve(
        ["--telegram-login", "--api-hash", "SECRET_ON_CLI"],
        {},
        {"api_id": "55", "api_hash": "DBH"},
    )
    assert api_hash == "DBH"
    assert ext is False


def test_resolve_creds_env_fallback_when_no_cli():
    api_id, api_hash, ext = _resolve(
        ["--telegram-login"],
        {"TELEGRAM_API_ID": "222", "TELEGRAM_API_HASH": "H2"},
        {"api_id": "", "api_hash": ""},
    )
    assert (api_id, api_hash, ext) == ("222", "H2", True)


def test_resolve_creds_db_when_no_external_is_not_flagged():
    api_id, api_hash, ext = _resolve(
        ["--telegram-login"], {}, {"api_id": "55", "api_hash": "DBH"}
    )
    assert (api_id, api_hash, ext) == ("55", "DBH", False)


def test_resolve_creds_flag_does_not_swallow_next_option():
    # BLOCK (CodeRabbit/Fable/Codacy): `--api-id --api-hash` NON deve prendere
    # `--api-hash` come valore di api_id. Il valore è '' => fallback al DB.
    api_id, _h, ext = _resolve(["--api-id", "--api-hash"], {}, {"api_id": "DBID"})
    assert api_id == "DBID"
    assert ext is False
    # forma con '=' funziona
    api_id2, _h2, ext2 = _resolve(["--api-id=333"], {}, {})
    assert (api_id2, ext2) == ("333", True)


def _login_db(settings=None):
    class _DB:
        def __init__(self):
            self.saved = None
            self._settings = dict(settings or {"api_id": "", "api_hash": "", "session_string": ""})

        def get_telegram_settings(self):
            return dict(self._settings)

        def save_telegram_settings(self, payload):
            self.saved = dict(payload)

    return _DB()


def test_run_telegram_login_persists_only_after_success(monkeypatch):
    # BLOCK (Fable/Fugu/GLM): le credenziali CLI/env su un login RIUSCITO vengono
    # salvate nel DB, complete di session_string. api_id da CLI, api_hash da env.
    db = _login_db()
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login", "--api-id", "12345"])
    monkeypatch.setenv("TELEGRAM_API_HASH", "HH")
    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(lambda *a, **k: (0, "SESS")))
    rc = app._run_telegram_login()
    assert rc == 0
    assert db.saved is not None
    assert db.saved.get("api_id") == "12345"
    assert db.saved.get("api_hash") == "HH"
    assert db.saved.get("session_string") == "SESS"


def test_run_telegram_login_does_not_persist_on_failed_login(monkeypatch):
    # BLOCK (persistenza pre-verifica): credenziali CLI/env ERRATE non devono
    # sovrascrivere il DB se il login FALLISCE. Prima del fix la persistenza era
    # pre-login => saved veniva scritto anche a login fallito.
    db = _login_db({"api_id": "OLD", "api_hash": "OLDH", "session_string": "OLDS"})
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    # api_id numerico VALIDO (CodeRabbit): con "BAD" `int()` solleverebbe prima del
    # flow, facendo passare il test per il motivo sbagliato. Con "999" il flow
    # viene davvero raggiunto e ritorna (2, None) = login fallito -> nessun save.
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login", "--api-id", "999"])
    monkeypatch.setenv("TELEGRAM_API_HASH", "BADH")
    flow_called = {"v": False}

    def _flow(*a, **k):
        flow_called["v"] = True
        return (2, None)

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_flow))
    rc = app._run_telegram_login()
    assert rc == 2
    assert flow_called["v"] is True, "il flow di login deve essere davvero eseguito"
    assert db.saved is None, "login fallito NON deve persistere/sovrascrivere le creds"


def test_run_telegram_login_missing_api_id_returns_2(monkeypatch):
    # Fable #2: api_id assente (no CLI/env/DB) => errore chiaro + return 2, senza
    # costruire il listener con api_id=0 né avviare il flow di login.
    db = _login_db({"api_id": "", "api_hash": "", "session_string": ""})
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login"])
    monkeypatch.delenv("TELEGRAM_API_ID", raising=False)
    monkeypatch.setenv("TELEGRAM_API_HASH", "HH")  # hash presente => niente prompt
    flow_called = {"v": False}

    def _flow(*a, **k):
        flow_called["v"] = True
        return (0, "S")

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_flow))
    rc = app._run_telegram_login()
    assert rc == 2
    assert flow_called["v"] is False, "il guard api_id deve precedere il flow"
    assert db.saved is None


def test_run_telegram_login_rejects_nonpositive_api_id(monkeypatch):
    # BLOCK (CodeRabbit): api_id "0" o negativo è config invalida (Telethon
    # fallirebbe dopo con errore opaco). Deve dare return 2 SENZA avviare il flow
    # né persistere. Prima del fix `int(api_id or 0)` accettava 0/-1.
    # Stub definito UNA volta fuori dal loop (niente closure su var di loop —
    # rilievo DeepSource): se il flow parte, fallisce il test.
    def _must_not_run(*a, **k):
        raise AssertionError("il flow non deve partire con api_id non valido")

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_must_not_run))
    monkeypatch.setenv("TELEGRAM_API_HASH", "HH")  # hash presente => niente prompt
    for bad in ("0", "-1", "abc"):
        db = _login_db({"api_id": "OLD", "api_hash": "OLDH", "session_string": "OLDS"})
        app = HeadlessApp.__new__(HeadlessApp)
        app.db = db
        monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login", "--api-id", bad])
        rc = app._run_telegram_login()
        assert rc == 2, f"api_id={bad!r} deve dare exit 2"
        assert db.saved is None, f"api_id={bad!r}: niente persistenza"


def test_run_telegram_login_missing_api_hash_returns_2(monkeypatch):
    # Simmetrico (GLM/Fable): api_id presente ma api_hash assente (env/DB vuoti) e
    # input nascosto vuoto => return 2, senza avviare il flow né persistere.
    db = _login_db({"api_id": "123", "api_hash": "", "session_string": ""})
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login"])
    monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)
    monkeypatch.setattr("getpass.getpass", lambda *a, **k: "")  # getpass vuoto
    flow_called = {"v": False}

    def _flow(*a, **k):
        flow_called["v"] = True
        return (0, "S")

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_flow))
    rc = app._run_telegram_login()
    assert rc == 2
    assert flow_called["v"] is False
    assert db.saved is None


def test_run_telegram_login_flow_exception_returns_2(monkeypatch):
    # BLOCK (CodeRabbit): un'eccezione imprevista nel flusso di login NON deve
    # propagare a main() (che la appiattirebbe a exit 1): il contratto documentato
    # è exit 2 per login fallito. Prima del fix l'eccezione usciva da
    # _run_telegram_login e _run_telegram_login() sollevava invece di ritornare 2.
    db = _login_db({"api_id": "123", "api_hash": "HH", "session_string": ""})
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login"])
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.setenv("TELEGRAM_API_HASH", "HH")

    def _boom(*a, **k):
        raise RuntimeError("rete giù")

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_boom))
    rc = app._run_telegram_login()
    assert rc == 2, "eccezione nel flow => exit 2 (non propagata, non exit 1)"
    assert db.saved is None, "login fallito NON deve persistere"


def test_run_telegram_login_getpass_exception_returns_2(monkeypatch):
    # BLOCK (CodeRabbit): se getpass solleva (es. stdin chiuso su VPS non
    # interattivo) mentre si chiede l'api_hash, il comando deve uscire 2, non
    # propagare l'eccezione (che main() appiattirebbe a exit 1).
    db = _login_db({"api_id": "123", "api_hash": "", "session_string": ""})
    app = HeadlessApp.__new__(HeadlessApp)
    app.db = db
    monkeypatch.setattr(sys, "argv", ["headless_main.py", "--telegram-login"])
    monkeypatch.setenv("TELEGRAM_API_ID", "123")
    monkeypatch.delenv("TELEGRAM_API_HASH", raising=False)

    def _boom(*a, **k):
        raise EOFError("stdin chiuso")

    monkeypatch.setattr("getpass.getpass", _boom)
    flow_called = {"v": False}

    def _flow(*a, **k):
        flow_called["v"] = True
        return (0, "S")

    monkeypatch.setattr(HeadlessApp, "_telegram_login_flow", staticmethod(_flow))
    rc = app._run_telegram_login()
    assert rc == 2, "getpass che solleva => exit 2 (non propagata)"
    assert flow_called["v"] is False, "senza api_hash il flow non parte"
    assert db.saved is None


def test_cli_flags_do_not_break_dispatch(monkeypatch):
    # BLOCK (Fable final review): il comando documentato
    #   python headless_main.py --telegram-login --api-id 123 --api-hash H
    # DEVE dispatchare a _run_telegram_login. Il parser NON è argparse strict
    # (scansione manuale `in sys.argv` che ignora i flag sconosciuti), quindi
    # --api-id/--api-hash non fanno fallire né il dispatch né _parse_args.
    # Se qualcuno introducesse un argparse strict, questo test fallirebbe
    # (regressione del comando sul VPS, non coperta dai test che chiamano
    # _run_telegram_login direttamente).
    app = HeadlessApp.__new__(HeadlessApp)
    app.settings_service = None
    monkeypatch.setattr(
        sys, "argv",
        ["headless_main.py", "--telegram-login", "--api-id", "123", "--api-hash", "H"],
    )
    # dispatch: --telegram-login riconosciuto nonostante i flag extra
    assert app._telegram_login_requested() is True
    assert app._preflight_requested() is False
    # _parse_args ignora i flag sconosciuti e ritorna un dict valido (no raise,
    # no SystemExit da un eventuale argparse strict)
    parsed = app._parse_args()
    assert isinstance(parsed, dict)
    assert "simulation_mode" in parsed and "execution_mode" in parsed
