"""Test del login userbot Telegram reale (#367 follow-up).

`TelegramListener.request_code`/`sign_in` erano STUB: `sign_in` ignorava il
codice ed emetteva un finto "AUTHORIZED". Ora fanno un login Telethon reale
(telefono+codice+2FA) e generano una `session_string`. Il comando headless
`--telegram-login` (`HeadlessApp._telegram_login_flow`) guida il flusso e la
salva. La verifica end-to-end con l'API Telegram reale resta sul VPS.
"""

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


class _FakeSession:
    def __init__(self, s):
        self._s = s

    def save(self):
        return self._s


class _FakeClient:
    """Client Telethon fittizio (metodi async) per pilotare il login."""

    def __init__(self, *, behavior="ok", session="SESS", code_hash="HASH", password_ok=True):
        self.behavior = behavior  # ok | 2fa | invalid_code | expired_code
        self.session = _FakeSession(session)
        self.code_hash = code_hash
        self.password_ok = password_ok
        self.calls = []
        self._authorized = False

    async def connect(self):
        self.calls.append(("connect",))

    async def disconnect(self):
        self.calls.append(("disconnect",))

    async def send_code_request(self, phone):
        self.calls.append(("send_code_request", phone))
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


def test_request_code_missing_phone():
    res = _listener(_FakeClient()).request_code("   ")
    assert res == {"ok": False, "error": "missing_phone"}


def test_stop_cleans_up_abandoned_login():
    # BLOCK (Fugu #371): request_code senza sign_in NON deve lasciare il client
    # di login orfano; stop() lo chiude (disconnect + loop fermato).
    fake = _FakeClient(behavior="ok")
    lis = _listener(fake)
    lis.request_code("+39")
    assert lis._login_client is not None
    lis.stop()
    assert lis._login_client is None
    assert ("disconnect",) in fake.calls


# ---- headless --telegram-login: flusso interattivo --------------------------


def _seq(*values):
    """Callable che ignora l'argomento (prompt) e ritorna i valori in sequenza."""
    it = iter(values)
    return lambda *_a, **_k: next(it)


class _ScriptListener:
    def __init__(self, request_ok=True, sign_in_results=None):
        self.request_ok = request_ok
        self.sign_in_results = list(sign_in_results or [])
        self.sign_in_calls = []

    def request_code(self, phone):
        self.phone = phone
        return {"ok": True} if self.request_ok else {"ok": False, "error": "boom"}

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
