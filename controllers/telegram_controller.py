from __future__ import annotations

"""
TelegramController
Gestisce autenticazione Telegram e caricamento chat.
Non esegue trading e non chiama mai direttamente il client di betting.
"""

import asyncio
import os
from tkinter import messagebox


class TelegramController:
    def __init__(self, app):
        self.app = app

    # =========================
    # DB / SETTINGS HELPERS
    # =========================
    def _db_call(self, method_name, *args, default=None, **kwargs):
        db = getattr(self.app, "db", None)
        method = getattr(db, method_name, None)
        if not callable(method):
            return default
        try:
            return method(*args, **kwargs)
        except Exception:
            return default

    def _get_settings(self):
        settings_service = getattr(self.app, "settings_service", None)
        if settings_service and hasattr(settings_service, "load_telegram_config"):
            try:
                cfg = settings_service.load_telegram_config()
                return {
                    "api_id": getattr(cfg, "api_id", 0),
                    "api_hash": getattr(cfg, "api_hash", ""),
                    "session_string": getattr(cfg, "session_string", ""),
                    "phone_number": getattr(cfg, "phone_number", ""),
                    "enabled": getattr(cfg, "enabled", False),
                    "auto_bet": getattr(cfg, "auto_bet", False),
                    "require_confirmation": getattr(cfg, "require_confirmation", True),
                    "auto_stake": getattr(cfg, "auto_stake", 1.0),
                    "monitored_chat_ids": getattr(cfg, "monitored_chat_ids", []),
                }
            except Exception:
                pass

        return self._db_call("get_telegram_settings", default={}) or {}

    def _save_settings_dict(self, data):
        settings_service = getattr(self.app, "settings_service", None)
        if settings_service and hasattr(settings_service, "save_telegram_config"):
            try:
                from core.system_state import TelegramRuntimeConfig

                cfg = TelegramRuntimeConfig(
                    api_id=int(data.get("api_id") or 0),
                    api_hash=str(data.get("api_hash") or ""),
                    session_string=str(data.get("session_string") or ""),
                    phone_number=str(data.get("phone_number") or ""),
                    enabled=bool(data.get("enabled", False)),
                    auto_bet=bool(data.get("auto_bet", False)),
                    require_confirmation=bool(
                        data.get("require_confirmation", True)
                    ),
                    auto_stake=float(data.get("auto_stake", 1.0) or 1.0),
                    monitored_chat_ids=list(data.get("monitored_chat_ids", []) or []),
                )
                settings_service.save_telegram_config(cfg)
                return True
            except Exception:
                return False

        return self._db_call("save_telegram_settings", data, default=False)

    def _get_session_path(self):
        base = os.getenv("APPDATA") or os.path.expanduser("~")
        session_dir = os.path.join(base, "Pickfair")
        os.makedirs(session_dir, exist_ok=True)
        return os.path.join(session_dir, "telegram_session")

    def _run_async_in_thread(self, coro_factory):
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro_factory())
        finally:
            try:
                loop.close()
            except Exception:
                pass

    def _post_ui(self, fn, *args, **kwargs):
        uiq = getattr(self.app, "uiq", None)
        if uiq:
            try:
                uiq.post(fn, *args, **kwargs)
                return
            except Exception:
                pass
        fn(*args, **kwargs)

    def _set_status_label(self, text):
        label = getattr(self.app, "tg_status_label", None)
        if label is not None:
            try:
                label.configure(text=text)
            except Exception:
                pass

    # =========================
    # SAVE SETTINGS
    # =========================
    def save_settings(self):
        try:
            stake = float(str(self.app.tg_auto_stake_var.get()).replace(",", "."))
        except Exception:
            stake = 1.0
            self.app.tg_auto_stake_var.set("1.0")

        settings = self._get_settings()
        settings.update(
            {
                "api_id": self.app.tg_api_id_var.get(),
                "api_hash": self.app.tg_api_hash_var.get(),
                "session_string": settings.get("session_string", ""),
                "phone_number": self.app.tg_phone_var.get(),
                "enabled": True,
                "auto_bet": bool(self.app.tg_auto_bet_var.get()),
                "require_confirmation": bool(self.app.tg_confirm_var.get()),
                "auto_stake": stake,
            }
        )

        ok = self._save_settings_dict(settings)
        if ok:
            messagebox.showinfo("Salvato", "Impostazioni Telegram salvate.")
        else:
            messagebox.showwarning(
                "DB incompleto",
                "save_telegram_settings non esiste o ha fallito.",
            )

    # =========================
    # AUTH SEND CODE
    # =========================
    def send_code(self):
        listener = getattr(self.app, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            messagebox.showwarning(
                "Attenzione",
                "Ferma il Listener Telegram prima di inviare il codice.",
            )
            return

        api_id = self.app.tg_api_id_var.get()
        api_hash = self.app.tg_api_hash_var.get()
        phone = self.app.tg_phone_var.get()

        if not api_id or not api_hash or not phone:
            messagebox.showwarning("Errore", "Compila API ID, API Hash e Telefono.")
            return

        self._set_status_label("Stato: Invio codice in corso...")

        def task():
            try:
                from telethon import TelegramClient

                async def run():
                    client = TelegramClient(
                        self._get_session_path(),
                        int(api_id),
                        api_hash,
                    )
                    await client.connect()

                    if await client.is_user_authorized():
                        await client.disconnect()
                        return True, "Già autenticato. Nessun codice necessario."

                    await client.send_code_request(phone)
                    await client.disconnect()
                    return False, "Codice inviato. Inseriscilo e clicca Verifica."

                authorized, msg = self._run_async_in_thread(run)

                self._post_ui(
                    self._set_status_label,
                    f"Stato: {msg}",
                )

            except Exception as e:
                self._post_ui(
                    messagebox.showerror,
                    "Errore",
                    f"Impossibile inviare codice: {str(e)}",
                )

        self.app.executor.submit("tg_send_code", task)

    # =========================
    # AUTH VERIFY CODE
    # =========================
    def verify_code(self):
        listener = getattr(self.app, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            messagebox.showwarning(
                "Attenzione",
                "Ferma il Listener Telegram prima di verificare il codice.",
            )
            return

        api_id = self.app.tg_api_id_var.get()
        api_hash = self.app.tg_api_hash_var.get()
        phone = self.app.tg_phone_var.get()
        code = self.app.tg_code_var.get()
        password = self.app.tg_2fa_var.get()

        if not api_id or not api_hash or not phone:
            messagebox.showwarning(
                "Errore",
                "Compila API ID, API Hash e Telefono prima di verificare.",
            )
            return

        if not code:
            messagebox.showwarning("Errore", "Inserisci il codice ricevuto.")
            return

        self._set_status_label("Stato: Verifica in corso...")

        def task():
            try:
                from telethon import TelegramClient
                from telethon.errors import SessionPasswordNeededError

                async def run():
                    client = TelegramClient(
                        self._get_session_path(),
                        int(api_id),
                        api_hash,
                    )
                    await client.connect()

                    try:
                        await client.sign_in(phone, code)
                    except SessionPasswordNeededError:
                        if not password:
                            await client.disconnect()
                            return False, "Password 2FA richiesta. Inseriscila e riprova."
                        await client.sign_in(password=password)

                    session_string = client.session.save()
                    await client.disconnect()
                    return True, session_string

                success, result = self._run_async_in_thread(run)

                if success:
                    settings = self._get_settings()
                    settings.update(
                        {
                            "api_id": api_id,
                            "api_hash": api_hash,
                            "session_string": result,
                            "phone_number": phone,
                            "enabled": True,
                        }
                    )

                    self._save_settings_dict(settings)

                    self._post_ui(
                        self._set_status_label,
                        "Stato: Autenticato con successo",
                    )
                    self._post_ui(
                        messagebox.showinfo,
                        "Successo",
                        "Login Telegram completato. Ora puoi usare il listener.",
                    )
                else:
                    self._post_ui(
                        self._set_status_label,
                        "Stato: Errore Verifica",
                    )
                    self._post_ui(
                        messagebox.showwarning,
                        "Attenzione",
                        result,
                    )

            except Exception as e:
                self._post_ui(
                    self._set_status_label,
                    "Stato: Errore Verifica",
                )
                self._post_ui(
                    messagebox.showerror,
                    "Errore",
                    f"Verifica fallita: {str(e)}",
                )

        self.app.executor.submit("tg_verify_code", task)

    # =========================
    # RESET SESSION
    # =========================
    def reset_session(self):
        listener = getattr(self.app, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            messagebox.showwarning(
                "Attenzione",
                "Ferma il Listener Telegram prima di resettare.",
            )
            return

        if not messagebox.askyesno(
            "Conferma",
            "Vuoi cancellare la sessione Telegram attuale?",
        ):
            return

        base_path = self._get_session_path()
        candidates = [
            base_path,
            base_path + ".session",
            base_path + ".session-journal",
        ]

        for path in candidates:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

        settings = self._get_settings()
        settings.update({"session_string": "", "enabled": False})

        self._save_settings_dict(settings)

        self._set_status_label("Stato: Sessione resettata.")
        messagebox.showinfo(
            "OK",
            "Sessione rimossa. Reinserisci i dati e richiedi un nuovo codice.",
        )

    # =========================
    # LOAD DIALOGS
    # =========================
    def load_dialogs(self):
        listener = getattr(self.app, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            messagebox.showwarning(
                "Attenzione",
                "Ferma il Listener Telegram prima di caricare le chat.",
            )
            return

        settings = self._get_settings()
        if not settings.get("api_id") or not settings.get("api_hash"):
            messagebox.showwarning("Errore", "Configura Telegram prima.")
            return

        status_label = getattr(self.app, "tg_available_status", None)
        if status_label is not None:
            try:
                status_label.configure(text="Caricamento...")
            except Exception:
                pass

        def task():
            try:
                from telethon import TelegramClient

                async def run():
                    client = TelegramClient(
                        self._get_session_path(),
                        int(settings["api_id"]),
                        settings["api_hash"],
                    )
                    await client.connect()

                    if not await client.is_user_authorized():
                        await client.disconnect()
                        return None

                    dialogs = await client.get_dialogs()
                    await client.disconnect()
                    return dialogs

                dialogs = self._run_async_in_thread(run)

                if dialogs is None:
                    self._post_ui(
                        status_label.configure,
                        text="Non autenticato",
                    )
                    self._post_ui(
                        messagebox.showwarning,
                        "Attenzione",
                        "Non autenticato. Effettua il login.",
                    )
                    return

                def update_ui():
                    self.app.tg_available_tree.delete(
                        *self.app.tg_available_tree.get_children()
                    )

                    monitored_ids = {
                        int(chat["chat_id"])
                        for chat in self._db_call(
                            "get_telegram_chats", default=[]
                        ) or []
                        if str(chat.get("chat_id", "")).strip()
                    }

                    self.app.available_chats_data = []

                    for d in dialogs:
                        if d.id in monitored_ids:
                            continue

                        if getattr(d, "is_user", False):
                            dialog_type = "User"
                        elif getattr(d, "is_group", False):
                            dialog_type = "Group"
                        elif getattr(d, "is_channel", False):
                            dialog_type = "Channel"
                        else:
                            dialog_type = "Chat"

                        safe_name = d.name or str(d.id)

                        row = {
                            "chat_id": int(d.id),
                            "type": dialog_type,
                            "name": safe_name,
                        }
                        self.app.available_chats_data.append(row)

                        self.app.tg_available_tree.insert(
                            "",
                            "end",
                            iid=str(d.id),
                            values=("", dialog_type, safe_name),
                        )

                    count = len(self.app.tg_available_tree.get_children())
                    self.app.tg_available_status.configure(
                        text=f"{count} chat disponibili"
                    )

                self._post_ui(update_ui)

            except Exception as e:
                if status_label is not None:
                    self._post_ui(
                        status_label.configure,
                        text="Errore caricamento",
                    )
                self._post_ui(
                    messagebox.showerror,
                    "Errore",
                    str(e),
                )

        self.app.executor.submit("tg_load_dialogs", task)

    # =========================
    # OPTIONAL HELPERS
    # =========================
    def start_listener(self):
        start_fn = getattr(self.app, "_start_telegram_listener", None)
        if callable(start_fn):
            return start_fn()

    def stop_listener(self):
        stop_fn = getattr(self.app, "_stop_telegram_listener", None)
        if callable(stop_fn):
            return stop_fn()