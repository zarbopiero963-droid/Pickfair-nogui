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
        return {}

    def save_telegram_settings(self, data):
        self.saved = data


class _App:
    def __init__(self):
        self.telegram_listener = types.SimpleNamespace(running=False)
        self.tg_api_id_var = _Var("1")
        self.tg_api_hash_var = _Var("hash")
        self.tg_phone_var = _Var("+1000000000")
        self.tg_auto_bet_var = _Var(False)
        self.tg_confirm_var = _Var(False)
        self.tg_auto_stake_var = _Var("1.0")
        self.tg_status_label = _Label()
        self.uiq = _UIQ()
        self.executor = _Executor()
        self.db = _DB()


def _install_fake_telethon(monkeypatch):
    class _Client:
        def __init__(self, *_args, **_kwargs):
            self.session = types.SimpleNamespace(save=lambda: "sess")

        async def connect(self):
            return None

        async def is_user_authorized(self):
            return True

        async def disconnect(self):
            return None

    telethon_mod = types.SimpleNamespace(TelegramClient=_Client)
    sessions_mod = types.SimpleNamespace(StringSession=lambda s: s)
    monkeypatch.setitem(__import__("sys").modules, "telethon", telethon_mod)
    monkeypatch.setitem(__import__("sys").modules, "telethon.sessions", sessions_mod)


def test_send_code_uses_asyncio_run_and_keeps_public_behavior(monkeypatch):
    app = _App()
    controller = TelegramController(app)
    _install_fake_telethon(monkeypatch)

    closed = asyncio.new_event_loop()
    closed.close()
    asyncio.set_event_loop(closed)

    calls = []
    real_run = asyncio.run

    def _tracking_run(coro):
        calls.append("run")
        return real_run(coro)

    monkeypatch.setattr(asyncio, "run", _tracking_run)

    controller.send_code()

    assert calls == ["run"]
    assert app.db.saved is not None
    assert app.tg_status_label.last == {"text": "Stato: Già autenticato. Nessun codice necessario."}

    with asyncio.Runner() as runner:
        assert runner.run(asyncio.sleep(0, result="ok")) == "ok"
