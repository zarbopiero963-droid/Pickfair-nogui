import asyncio
import types

from controllers.telegram_controller import TelegramController


class _Var:
    def __init__(self, value=""):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class _Label:
    def __init__(self):
        self.last = None

    def configure(self, **kwargs):
        self.last = kwargs


class _Tree:
    def __init__(self):
        self._items = {}

    def delete(self, *item_ids):
        for item_id in item_ids:
            self._items.pop(item_id, None)

    def get_children(self):
        return tuple(self._items.keys())

    def insert(self, _parent, _index, iid, values):
        self._items[str(iid)] = values


class _UIQ:
    def post(self, fn, *args, **kwargs):
        fn(*args, **kwargs)


class _Executor:
    def submit(self, _name, fn):
        fn()


class _DB:
    def __init__(self):
        self.saved = None

    def get_telegram_settings(self):
        return {"api_id": "1", "api_hash": "hash", "session_string": "existing"}

    def save_telegram_settings(self, data):
        self.saved = data

    def get_telegram_chats(self):
        return []


class _App:
    def __init__(self):
        self.telegram_listener = types.SimpleNamespace(running=False)
        self.tg_api_id_var = _Var("1")
        self.tg_api_hash_var = _Var("hash")
        self.tg_phone_var = _Var("+1000000000")
        self.tg_code_var = _Var("12345")
        self.tg_2fa_var = _Var("")
        self.tg_auto_bet_var = _Var(False)
        self.tg_confirm_var = _Var(False)
        self.tg_auto_stake_var = _Var("1.0")
        self.tg_status_label = _Label()
        self.tg_available_status = _Label()
        self.tg_available_tree = _Tree()
        self.available_chats_data = []
        self.uiq = _UIQ()
        self.executor = _Executor()
        self.db = _DB()


def _install_fake_telethon(monkeypatch):
    class _Dialog:
        def __init__(self, dialog_id, name):
            self.id = dialog_id
            self.name = name

    class _Client:
        def __init__(self, *_args, **_kwargs):
            self.session = types.SimpleNamespace(save=lambda: "sess")

        async def connect(self):
            return None

        async def is_user_authorized(self):
            return True

        async def disconnect(self):
            return None

        async def send_code_request(self, _phone):
            return None

        async def sign_in(self, **_kwargs):
            return None

        async def get_dialogs(self):
            return [_Dialog(77, "Channel 77")]

    telethon_mod = types.SimpleNamespace(TelegramClient=_Client)
    sessions_mod = types.SimpleNamespace(StringSession=lambda s: s)
    errors_mod = types.SimpleNamespace(SessionPasswordNeededError=RuntimeError)
    monkeypatch.setitem(__import__("sys").modules, "telethon", telethon_mod)
    monkeypatch.setitem(__import__("sys").modules, "telethon.sessions", sessions_mod)
    monkeypatch.setitem(__import__("sys").modules, "telethon.errors", errors_mod)


def _silence_messageboxes(monkeypatch):
    monkeypatch.setattr("controllers.telegram_controller.messagebox.showinfo", lambda *a, **k: None)
    monkeypatch.setattr("controllers.telegram_controller.messagebox.showwarning", lambda *a, **k: None)
    monkeypatch.setattr("controllers.telegram_controller.messagebox.showerror", lambda *a, **k: None)


def _run_with_closed_current_loop(monkeypatch, fn):
    try:
        previous_loop = asyncio.get_event_loop()
    except RuntimeError:
        previous_loop = None

    closed = asyncio.new_event_loop()
    closed.close()
    asyncio.set_event_loop(closed)

    calls = []
    real_run = asyncio.run

    def _tracking_run(coro):
        calls.append("run")
        return real_run(coro)

    monkeypatch.setattr(asyncio, "run", _tracking_run)

    try:
        fn()
    finally:
        if previous_loop is not None and not previous_loop.is_closed():
            asyncio.set_event_loop(previous_loop)
        else:
            asyncio.set_event_loop(asyncio.new_event_loop())

    return calls


def test_send_code_uses_asyncio_run_and_keeps_public_behavior(monkeypatch):
    app = _App()
    controller = TelegramController(app)
    _install_fake_telethon(monkeypatch)
    _silence_messageboxes(monkeypatch)

    calls = _run_with_closed_current_loop(monkeypatch, controller.send_code)

    assert calls == ["run"]
    assert app.db.saved is not None
    assert app.tg_status_label.last == {"text": "Stato: Già autenticato. Nessun codice necessario."}


def test_verify_code_uses_asyncio_run_and_keeps_public_behavior(monkeypatch):
    app = _App()
    controller = TelegramController(app)
    _install_fake_telethon(monkeypatch)
    _silence_messageboxes(monkeypatch)

    calls = _run_with_closed_current_loop(monkeypatch, controller.verify_code)

    assert calls == ["run"]
    assert app.db.saved is not None
    assert app.tg_status_label.last == {"text": "Stato: Autenticato con successo"}


def test_load_dialogs_uses_asyncio_run_and_keeps_public_behavior(monkeypatch):
    app = _App()
    controller = TelegramController(app)
    _install_fake_telethon(monkeypatch)
    _silence_messageboxes(monkeypatch)

    calls = _run_with_closed_current_loop(monkeypatch, controller.load_dialogs)

    assert calls == ["run"]
    assert app.tg_available_status.last == {"text": "1 chat disponibili"}
    assert app.available_chats_data == [{"chat_id": "77", "title": "Channel 77", "dialog_type": "Chat"}]
    assert app.tg_available_tree.get_children() == ("77",)


# ---- F-1a: hardening login GUI (sanitize codice + FloodWait) -----------------


def _install_capturing_telethon(
    monkeypatch, captured, *, authorized=False, flood_seconds=None, signin_flood_seconds=None
):
    class FloodWaitError(Exception):
        # Il nome DEVE combaciare: il controller riconosce il flood via
        # type(exc).__name__ == "FloodWaitError".
        def __init__(self, seconds=None):
            super().__init__(f"A wait of {seconds} seconds is required")
            self.seconds = seconds

    class _Client:
        def __init__(self, *_args, **_kwargs):
            self.session = types.SimpleNamespace(save=lambda: "sess")
            self.disconnected = 0

        async def connect(self):
            return None

        async def is_user_authorized(self):
            return authorized

        async def disconnect(self):
            self.disconnected += 1
            captured["disconnected"] = self.disconnected
            return None

        async def send_code_request(self, _phone):
            if flood_seconds is not None:
                raise FloodWaitError(seconds=flood_seconds)
            return None

        async def sign_in(self, **kwargs):
            captured.update(kwargs)
            if signin_flood_seconds is not None:
                raise FloodWaitError(seconds=signin_flood_seconds)
            return None

    telethon_mod = types.SimpleNamespace(TelegramClient=_Client)
    sessions_mod = types.SimpleNamespace(StringSession=lambda s: s)
    errors_mod = types.SimpleNamespace(SessionPasswordNeededError=RuntimeError)
    monkeypatch.setitem(__import__("sys").modules, "telethon", telethon_mod)
    monkeypatch.setitem(__import__("sys").modules, "telethon.sessions", sessions_mod)
    monkeypatch.setitem(__import__("sys").modules, "telethon.errors", errors_mod)


def test_verify_code_sanitizes_pasted_code(monkeypatch):
    # BLOCK (Greptile P1 GUI bypass): incollare l'intero messaggio 777000 nella
    # GUI deve passare a client.sign_in SOLO le cifre. Prima del fix la GUI usava
    # tg_code_var.get().strip() grezzo => "Login code: 54321" invalido.
    app = _App()
    app.tg_code_var = _Var("Login code: 54321")
    controller = TelegramController(app)
    captured = {}
    _install_capturing_telethon(monkeypatch, captured)
    _silence_messageboxes(monkeypatch)

    _run_with_closed_current_loop(monkeypatch, controller.verify_code)

    assert captured.get("code") == "54321", "la GUI deve inviare solo le cifre"


def test_send_code_floodwait_shows_wait_message(monkeypatch):
    # BLOCK (Greptile P1 GUI FloodWait bypass): un FloodWait nel send_code GUI
    # deve mostrare un messaggio d'attesa chiaro, non un errore opaco.
    app = _App()
    controller = TelegramController(app)
    captured = {}
    _install_capturing_telethon(monkeypatch, captured, authorized=False, flood_seconds=30)
    _silence_messageboxes(monkeypatch)

    _run_with_closed_current_loop(monkeypatch, controller.send_code)

    assert app.tg_status_label.last is not None
    assert "FloodWait" in app.tg_status_label.last["text"]
    assert "30" in app.tg_status_label.last["text"]
    # BLOCK (Fugu/Fable): il client Telethon locale dev'essere disconnesso anche
    # quando send_code_request solleva FloodWait (try/finally in run()): nessun
    # socket orfano. Prima del fix il disconnect era dopo la send => saltato.
    assert captured.get("disconnected", 0) >= 1, "il client va disconnesso anche su FloodWait"


def test_verify_code_floodwait_shows_wait_message(monkeypatch):
    # BLOCK (CodeRabbit): un FloodWait durante sign_in (verifica) deve mostrare
    # un messaggio d'attesa chiaro, non "Verifica fallita: ..." opaco.
    app = _App()
    app.tg_code_var = _Var("12345")
    controller = TelegramController(app)
    captured = {}
    _install_capturing_telethon(monkeypatch, captured, signin_flood_seconds=45)
    _silence_messageboxes(monkeypatch)

    _run_with_closed_current_loop(monkeypatch, controller.verify_code)

    assert app.tg_status_label.last is not None
    assert "FloodWait" in app.tg_status_label.last["text"]
    assert "45" in app.tg_status_label.last["text"]
    # anche qui: disconnect garantito dal try/finally in run().
    assert captured.get("disconnected", 0) >= 1, "il client va disconnesso anche su FloodWait in verify"
