from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox
from typing import Callable

try:
    import customtkinter as ctk
except Exception:  # pragma: no cover
    class _FallbackCTk(tk.Tk):
        pass

    class _FallbackFrame(tk.Frame):
        def __init__(self, master=None, fg_color=None, **kwargs):
            super().__init__(master, **kwargs)

    class _FallbackLabel(tk.Label):
        def __init__(
            self,
            master=None,
            text="",
            textvariable=None,
            font=None,
            anchor=None,
            wraplength=None,
            width=None,
            **kwargs,
        ):
            super().__init__(
                master,
                text=text,
                textvariable=textvariable,
                font=font,
                anchor=anchor,
                wraplength=wraplength,
                width=width,
                **kwargs,
            )

    class _FallbackButton(tk.Button):
        def __init__(self, master=None, text="", command=None, **kwargs):
            super().__init__(master, text=text, command=command, **kwargs)

    class _FallbackEntry(tk.Entry):
        def __init__(self, master=None, textvariable=None, width=None, show=None, **kwargs):
            super().__init__(master, textvariable=textvariable, show=show, **kwargs)

    class _FallbackCheckBox(tk.Checkbutton):
        def __init__(self, master=None, text="", variable=None, **kwargs):
            super().__init__(master, text=text, variable=variable, **kwargs)

    class _FallbackSwitch(tk.Checkbutton):
        def __init__(
            self,
            master=None,
            text="",
            variable=None,
            command=None,
            onvalue=True,
            offvalue=False,
            **kwargs,
        ):
            super().__init__(
                master,
                text=text,
                variable=variable,
                command=command,
                onvalue=onvalue,
                offvalue=offvalue,
                **kwargs,
            )

        def select(self):
            return None

    class _FallbackComboBox(ttk.Combobox):
        def __init__(self, master=None, variable=None, values=None, width=None, **kwargs):
            super().__init__(master, textvariable=variable, values=values or [], width=width, **kwargs)

    class _FallbackText(tk.Text):
        pass

    class _FallbackScrollableFrame(tk.Frame):
        def __init__(self, master=None, **kwargs):
            super().__init__(master, **kwargs)

    class _FallbackTabview(tk.Frame):
        def __init__(self, master=None, **kwargs):
            super().__init__(master, **kwargs)
            self._tabs: dict[str, tk.Frame] = {}

        def add(self, name: str):
            frame = tk.Frame(self)
            self._tabs[name] = frame
            return frame

    class _FallbackModule:
        CTk = _FallbackCTk
        CTkFrame = _FallbackFrame
        CTkLabel = _FallbackLabel
        CTkButton = _FallbackButton
        CTkEntry = _FallbackEntry
        CTkCheckBox = _FallbackCheckBox
        CTkSwitch = _FallbackSwitch
        CTkComboBox = _FallbackComboBox
        CTkTextbox = _FallbackText
        CTkScrollableFrame = _FallbackScrollableFrame
        CTkTabview = _FallbackTabview

        @staticmethod
        def set_appearance_mode(_mode: str):
            return None

        @staticmethod
        def set_default_color_theme(_theme: str):
            return None

    ctk = _FallbackModule()


from database import Database
from core.event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from services.setting_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController

from controllers.telegram_controller import TelegramController

try:
    from telegram_module import TelegramModule
except Exception:  # pragma: no cover
    class TelegramModule:
        pass

try:
    from telegram_tab_ui import TelegramTabUI
except Exception:  # pragma: no cover
    class TelegramTabUI:
        def __init__(self, parent, app):
            _ = app
            frame = ctk.CTkFrame(parent)
            if hasattr(frame, "pack"):
                frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)
            label = ctk.CTkLabel(frame, text="Telegram UI non disponibile")
            if hasattr(label, "pack"):
                label.pack(anchor="w", padx=12, pady=12)

try:
    from theme import COLORS, FONTS
except Exception:  # pragma: no cover
    COLORS = {}
    FONTS = {}


class _HeadlessBoolVar:
    def __init__(self, value: bool = False):
        self._value = bool(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = bool(value)


class _HeadlessStringVar:
    def __init__(self, value: str = ""):
        self._value = str(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = str(value)


class _HeadlessRoot:
    def after(self, _delay, fn=None):
        if callable(fn):
            try:
                fn()
            except Exception:
                pass
        return None

    def destroy(self):
        return None

    def protocol(self, *_args, **_kwargs):
        return None

    def withdraw(self):
        return None

    def title(self, *_args, **_kwargs):
        return None

    def geometry(self, *_args, **_kwargs):
        return None

    def grid_columnconfigure(self, *_args, **_kwargs):
        return None

    def grid_rowconfigure(self, *_args, **_kwargs):
        return None


class _DummyButton:
    def __init__(self, fn):
        self._fn = fn

    def cget(self, name):
        if name == "command":
            return self._fn
        return None


class _DummyTree:
    def __init__(self):
        self.rows = []

    def delete(self, *_args, **_kwargs):
        self.rows = []

    def get_children(self):
        return list(range(len(self.rows)))

    def insert(self, *_args, **kwargs):
        self.rows.append(kwargs.get("values"))


class _DummyLog:
    def __init__(self):
        self.lines = []

    def insert(self, *_args, **kwargs):
        if len(args) >= 2:
            self.lines.append(args[1])
        elif "text" in kwargs:
            self.lines.append(kwargs["text"])

    def see(self, *_args, **_kwargs):
        return None


class SimpleUIQueue:
    def __init__(self, root):
        self.root = root

    def post(self, fn: Callable, *args, **kwargs):
        if hasattr(self.root, "after"):
            self.root.after(0, lambda: fn(*args, **kwargs))
        else:
            fn(*args, **kwargs)


class MiniPickfairGUI(ctk.CTk, TelegramModule):
    def __init__(self, test_mode: bool = False):
        self._test_mode = bool(test_mode)

        # FIX CRITICO: non creare Tk in ambiente headless di test
        if not self._test_mode:
            super().__init__()
            self._headless_root = None
        else:
            self._headless_root = _HeadlessRoot()
            self.tk = None
            self._w = "."

        self.title("Pickfair Mini GUI")
        self.geometry("1420x900")

        try:
            ctk.set_appearance_mode("dark")
            ctk.set_default_color_theme("blue")
        except Exception:
            pass

        if self._test_mode:
            try:
                self.withdraw()
            except Exception:
                pass

        self.simulation_mode = True
        self.telegram_status = "STOPPED"

        self._build_core()
        self._build_vars()
        self._build_ui()
        self._load_initial_settings()
        self._wire_bus()
        self._apply_simulation_mode_to_runtime()

        if not self._test_mode:
            self._start_polling()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

    # =========================================================
    # ROOT / HEADLESS HELPERS
    # =========================================================
    def after(self, delay_ms, fn=None):
        if self._test_mode:
            return self._headless_root.after(delay_ms, fn)
        return super().after(delay_ms, fn)

    def protocol(self, name, func):
        if self._test_mode:
            return self._headless_root.protocol(name, func)
        return super().protocol(name, func)

    def destroy(self):
        if self._test_mode:
            return self._headless_root.destroy()
        return super().destroy()

    def withdraw(self):
        if self._test_mode:
            return self._headless_root.withdraw()
        return super().withdraw()

    def title(self, text):
        if self._test_mode:
            return self._headless_root.title(text)
        return super().title(text)

    def geometry(self, value):
        if self._test_mode:
            return self._headless_root.geometry(value)
        return super().geometry(value)

    def grid_columnconfigure(self, *args, **kwargs):
        if self._test_mode:
            return self._headless_root.grid_columnconfigure(*args, **kwargs)
        return super().grid_columnconfigure(*args, **kwargs)

    def grid_rowconfigure(self, *args, **kwargs):
        if self._test_mode:
            return self._headless_root.grid_rowconfigure(*args, **kwargs)
        return super().grid_rowconfigure(*args, **kwargs)

    # =========================================================
    # CORE BOOTSTRAP
    # =========================================================
    def _build_core(self):
        self.db = Database()
        self.bus = EventBus()
        self.executor = ExecutorManager(max_workers=4, default_timeout=30)
        self.shutdown = ShutdownManager()
        self.uiq = SimpleUIQueue(self)

        self.settings_service = SettingsService(self.db)
        self.betfair_service = BetfairService(self.settings_service)
        self.telegram_service = TelegramService(
            self.settings_service,
            self.db,
            self.bus,
        )

        self.trading_engine = TradingEngine(
            bus=self.bus,
            db=self.db,
            client_getter=self.betfair_service.get_client,
            executor=self.executor,
        )

        self.runtime = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings_service,
            betfair_service=self.betfair_service,
            telegram_service=self.telegram_service,
            trading_engine=self.trading_engine,
            executor=self.executor,
        )

        self.telegram_controller = TelegramController(self)

        self._register_shutdown_hook("telegram_stop", self.telegram_service.stop, priority=10)
        self._register_shutdown_hook("betfair_disconnect", self.betfair_service.disconnect, priority=20)
        self._register_shutdown_hook("db_close", self.db.close_all_connections, priority=30)
        self._register_shutdown_hook("executor_shutdown", self.executor.shutdown, priority=40)

    def _register_shutdown_hook(self, name, fn, priority=100):
        if hasattr(self.shutdown, "register"):
            try:
                self.shutdown.register(name, fn, priority=priority)
                return
            except TypeError:
                try:
                    self.shutdown.register(name, fn)
                    return
                except TypeError:
                    pass

        if hasattr(self.shutdown, "register_shutdown_hook"):
            try:
                self.shutdown.register_shutdown_hook(name, fn, priority=priority)
                return
            except TypeError:
                try:
                    self.shutdown.register_shutdown_hook(fn)
                    return
                except TypeError:
                    pass

    def _apply_simulation_mode_to_runtime(self):
        self.simulation_mode = bool(self.simulation_mode_var.get())

        if hasattr(self.betfair_service, "set_simulation_mode"):
            try:
                self.betfair_service.set_simulation_mode(self.simulation_mode)
            except Exception:
                pass

        if hasattr(self.runtime, "set_simulation_mode"):
            try:
                self.runtime.set_simulation_mode(self.simulation_mode)
            except Exception:
                pass

    # =========================================================
    # VARS
    # =========================================================
    def _make_string_var(self, value: str = ""):
        if self._test_mode:
            return _HeadlessStringVar(value)
        return tk.StringVar(value=value)

    def _make_bool_var(self, value: bool = False):
        if self._test_mode:
            return _HeadlessBoolVar(value)
        return tk.BooleanVar(value=value)

    def _build_vars(self):
        self.bf_username_var = self._make_string_var()
        self.bf_password_var = self._make_string_var()
        self.bf_app_key_var = self._make_string_var()
        self.bf_cert_var = self._make_string_var()
        self.bf_key_var = self._make_string_var()

        self.rs_target_var = self._make_string_var("3.0")
        self.rs_max_single_var = self._make_string_var("18.0")
        self.rs_max_total_var = self._make_string_var("35.0")
        self.rs_max_event_var = self._make_string_var("18.0")
        self.rs_auto_reset_var = self._make_string_var("15.0")
        self.rs_defense_var = self._make_string_var("7.5")
        self.rs_lockdown_var = self._make_string_var("20.0")
        self.rs_expansion_profit_var = self._make_string_var("5.0")
        self.rs_expansion_mult_var = self._make_string_var("1.10")
        self.rs_defense_mult_var = self._make_string_var("0.80")
        self.rs_table_count_var = self._make_string_var("5")
        self.rs_recovery_tables_var = self._make_string_var("2")
        self.rs_commission_var = self._make_string_var("4.5")
        self.rs_min_stake_var = self._make_string_var("0.10")
        self.rs_max_abs_var = self._make_string_var("10000.0")
        self.rs_allow_recovery_var = self._make_bool_var(True)
        self.rs_anti_dup_var = self._make_bool_var(True)
        self.rs_risk_profile_var = self._make_string_var("BALANCED")

        self.simulation_mode_var = self._make_bool_var(True)

        self.status_mode_var = self._make_string_var("STOPPED")
        self.status_betfair_var = self._make_string_var("DISCONNECTED")
        self.status_telegram_var = self._make_string_var("STOPPED")
        self.status_bankroll_var = self._make_string_var("0.00")
        self.status_drawdown_var = self._make_string_var("0.00")
        self.status_exposure_var = self._make_string_var("0.00")
        self.status_tables_var = self._make_string_var("0")
        self.status_last_signal_var = self._make_string_var("-")
        self.status_last_error_var = self._make_string_var("-")
        self.sim_label_var = self._make_string_var("SIMULAZIONE")
        self.status_broker_var = self._make_string_var("SIMULATION")

    # =========================================================
    # UI
    # =========================================================
    def _build_ui(self):
        if self._test_mode:
            self.tabs = object()
            self.tab_dashboard = object()
            self.tab_settings = object()
            self.tab_telegram = object()
            self.tab_roserpina = object()
            self.tab_risk = object()
            self.tab_log = object()

            self._build_topbar()
            self._build_dashboard_tab()
            self._build_settings_tab()
            self._build_telegram_tab()
            self._build_roserpina_tab()
            self._build_risk_tab()
            self._build_log_tab()
            return

        self.grid_columnconfigure(0, weight=1)
        self.grid_rowconfigure(1, weight=1)

        self._build_topbar()

        self.tabs = ctk.CTkTabview(self)
        self.tabs.grid(row=1, column=0, sticky="nsew", padx=12, pady=(0, 12))

        self.tab_dashboard = self.tabs.add("Dashboard")
        self.tab_settings = self.tabs.add("Impostazioni")
        self.tab_telegram = self.tabs.add("Telegram")
        self.tab_roserpina = self.tabs.add("Roserpina")
        self.tab_risk = self.tabs.add("Risk Desk")
        self.tab_log = self.tabs.add("Log")

        self._build_dashboard_tab()
        self._build_settings_tab()
        self._build_telegram_tab()
        self._build_roserpina_tab()
        self._build_risk_tab()
        self._build_log_tab()

    def _build_topbar(self):
        if self._test_mode:
            self.live_sim_switch = object()
            self.live_sim_label = object()
            return

        top = ctk.CTkFrame(self, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=12, pady=12)
        try:
            top.grid_columnconfigure(1, weight=1)
        except Exception:
            pass

        title = ctk.CTkLabel(top, text="Pickfair Mini Control Panel", font=("Segoe UI", 18, "bold"))
        title.grid(row=0, column=0, sticky="w")

        center = ctk.CTkFrame(top, fg_color="transparent")
        center.grid(row=0, column=1, sticky="e")

        ctk.CTkLabel(center, text="Modalità:").pack(side=tk.LEFT, padx=(0, 8))

        self.live_sim_switch = ctk.CTkSwitch(
            center,
            text="SIMULAZIONE",
            variable=self.simulation_mode_var,
            command=self._toggle_simulation_mode,
            onvalue=True,
            offvalue=False,
        )
        self.live_sim_switch.pack(side=tk.LEFT, padx=6)
        if hasattr(self.live_sim_switch, "select"):
            try:
                self.live_sim_switch.select()
            except Exception:
                pass

        self.live_sim_label = ctk.CTkLabel(
            center,
            textvariable=self.sim_label_var,
            font=("Segoe UI", 12, "bold"),
        )
        self.live_sim_label.pack(side=tk.LEFT, padx=10)

    def _build_dashboard_tab(self):
        if self._test_mode:
            self.btn_start = _DummyButton(self._runtime_start)
            self.btn_pause = _DummyButton(self._runtime_pause)
            self.btn_resume = _DummyButton(self._runtime_resume)
            self.btn_stop = _DummyButton(self._runtime_stop)
            self.btn_reset = _DummyButton(self._runtime_reset)
            self.btn_refresh = _DummyButton(self._refresh_runtime_status)
            return

        frame = self.tab_dashboard
        try:
            frame.grid_columnconfigure((0, 1, 2, 3), weight=1)
        except Exception:
            pass

        cards = [
            ("Runtime", self.status_mode_var),
            ("Broker", self.status_broker_var),
            ("Betfair", self.status_betfair_var),
            ("Telegram", self.status_telegram_var),
            ("Bankroll", self.status_bankroll_var),
            ("Drawdown %", self.status_drawdown_var),
            ("Exposure", self.status_exposure_var),
            ("Tavoli Attivi", self.status_tables_var),
            ("Ultimo Segnale", self.status_last_signal_var),
        ]

        for idx, (label, var) in enumerate(cards):
            r = idx // 4
            c = idx % 4
            card = ctk.CTkFrame(frame)
            card.grid(row=r, column=c, sticky="nsew", padx=8, pady=8)
            ctk.CTkLabel(card, text=label, font=("Segoe UI", 12)).pack(anchor="w", padx=12, pady=(10, 4))
            ctk.CTkLabel(card, textvariable=var, font=("Segoe UI", 16, "bold")).pack(anchor="w", padx=12, pady=(0, 10))

        controls = ctk.CTkFrame(frame)
        controls.grid(row=3, column=0, columnspan=4, sticky="ew", padx=8, pady=8)

        self.btn_start = ctk.CTkButton(controls, text="Avvia", command=self._runtime_start)
        self.btn_pause = ctk.CTkButton(controls, text="Pausa", command=self._runtime_pause)
        self.btn_resume = ctk.CTkButton(controls, text="Resume", command=self._runtime_resume)
        self.btn_stop = ctk.CTkButton(controls, text="Stop", command=self._runtime_stop)
        self.btn_reset = ctk.CTkButton(controls, text="Reset Ciclo", command=self._runtime_reset)
        self.btn_refresh = ctk.CTkButton(controls, text="Refresh", command=self._refresh_runtime_status)

        for btn in [self.btn_start, self.btn_pause, self.btn_resume, self.btn_stop, self.btn_reset, self.btn_refresh]:
            btn.pack(side=tk.LEFT, padx=6, pady=10)

        err = ctk.CTkFrame(frame)
        err.grid(row=4, column=0, columnspan=4, sticky="ew", padx=8, pady=8)
        ctk.CTkLabel(err, text="Ultimo Errore", font=("Segoe UI", 12)).pack(anchor="w", padx=12, pady=(10, 4))
        ctk.CTkLabel(err, textvariable=self.status_last_error_var, wraplength=1200).pack(anchor="w", padx=12, pady=(0, 10))

    def _build_settings_tab(self):
        if self._test_mode:
            self.btn_save_betfair = _DummyButton(self._save_betfair_settings)
            return

        frame = self.tab_settings
        box = ctk.CTkFrame(frame)
        box.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        self._labeled_entry(box, "Username Betfair", self.bf_username_var)
        self._labeled_entry(box, "Password Betfair", self.bf_password_var, show="*")
        self._labeled_entry(box, "App Key", self.bf_app_key_var)
        self._labeled_entry(box, "Certificato", self.bf_cert_var, width=700)
        self._labeled_entry(box, "Private Key", self.bf_key_var, width=700)

        btns = ctk.CTkFrame(box, fg_color="transparent")
        btns.pack(fill=tk.X, padx=12, pady=12)
        self.btn_save_betfair = ctk.CTkButton(
            btns,
            text="Salva Impostazioni Betfair",
            command=self._save_betfair_settings,
        )
        self.btn_save_betfair.pack(side=tk.LEFT, padx=6)

    def _build_telegram_tab(self):
        if self._test_mode:
            return
        TelegramTabUI(self.tab_telegram, self)

    def _build_roserpina_tab(self):
        if self._test_mode:
            self.btn_save_roserpina = _DummyButton(self._save_roserpina_settings)
            return

        frame = self.tab_roserpina
        outer = ctk.CTkScrollableFrame(frame)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        self._labeled_entry(outer, "Target Profitto Ciclo %", self.rs_target_var)
        self._labeled_entry(outer, "Max Stake Singola %", self.rs_max_single_var)
        self._labeled_entry(outer, "Max Capitale Esposto %", self.rs_max_total_var)
        self._labeled_entry(outer, "Max Esposizione Evento %", self.rs_max_event_var)
        self._labeled_entry(outer, "Auto Reset Drawdown %", self.rs_auto_reset_var)
        self._labeled_entry(outer, "Defense Drawdown %", self.rs_defense_var)
        self._labeled_entry(outer, "Lockdown Drawdown %", self.rs_lockdown_var)
        self._labeled_entry(outer, "Expansion Profit %", self.rs_expansion_profit_var)
        self._labeled_entry(outer, "Expansion Multiplier", self.rs_expansion_mult_var)
        self._labeled_entry(outer, "Defense Multiplier", self.rs_defense_mult_var)
        self._labeled_entry(outer, "Numero Tavoli", self.rs_table_count_var)
        self._labeled_entry(outer, "Max Recovery Tables", self.rs_recovery_tables_var)
        self._labeled_entry(outer, "Commission %", self.rs_commission_var)
        self._labeled_entry(outer, "Min Stake", self.rs_min_stake_var)
        self._labeled_entry(outer, "Max Stake Assoluto", self.rs_max_abs_var)

        rp = ctk.CTkFrame(outer)
        rp.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(rp, text="Risk Profile", width=220, anchor="w").pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkComboBox(
            rp,
            variable=self.rs_risk_profile_var,
            values=["CONSERVATIVE", "BALANCED", "AGGRESSIVE"],
            width=220,
        ).pack(side=tk.LEFT, padx=8, pady=8)

        cb = ctk.CTkFrame(outer)
        cb.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkCheckBox(cb, text="Allow Recovery", variable=self.rs_allow_recovery_var).pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkCheckBox(cb, text="Anti Duplication Enabled", variable=self.rs_anti_dup_var).pack(side=tk.LEFT, padx=8, pady=8)

        self.btn_save_roserpina = ctk.CTkButton(
            outer,
            text="Salva Roserpina",
            command=self._save_roserpina_settings,
        )
        self.btn_save_roserpina.pack(anchor="w", padx=12, pady=12)

    def _build_risk_tab(self):
        if self._test_mode:
            self.risk_tree = _DummyTree()
            self.btn_refresh_risk = _DummyButton(self._refresh_runtime_status)
            return

        frame = self.tab_risk
        try:
            frame.grid_rowconfigure(0, weight=1)
            frame.grid_columnconfigure(0, weight=1)
        except Exception:
            pass

        self.risk_tree = ttk.Treeview(
            frame,
            columns=("id", "status", "loss", "exposure", "event", "market", "selection"),
            show="headings",
            height=18,
        )
        for col, text, width in [
            ("id", "Tavolo", 70),
            ("status", "Stato", 120),
            ("loss", "Loss", 100),
            ("exposure", "Exposure", 100),
            ("event", "Event Key", 260),
            ("market", "Market ID", 180),
            ("selection", "Selection ID", 120),
        ]:
            self.risk_tree.heading(col, text=text)
            self.risk_tree.column(col, width=width, anchor="w")
        self.risk_tree.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)

        btns = ctk.CTkFrame(frame, fg_color="transparent")
        btns.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        self.btn_refresh_risk = ctk.CTkButton(
            btns,
            text="Refresh Risk Desk",
            command=self._refresh_runtime_status,
        )
        self.btn_refresh_risk.pack(side=tk.LEFT, padx=6)

    def _build_log_tab(self):
        if self._test_mode:
            self.log_text = _DummyLog()
            return

        frame = self.tab_log
        self.log_text = ctk.CTkTextbox(frame)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

    def _labeled_entry(self, parent, label, variable, width=320, show=None):
        if self._test_mode:
            return
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(row, text=label, width=220, anchor="w").pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkEntry(row, textvariable=variable, width=width, show=show).pack(side=tk.LEFT, padx=8, pady=8)

    # =========================================================
    # SETTINGS LOAD / SAVE
    # =========================================================
    def _load_initial_settings(self):
        try:
            if hasattr(self.settings_service, "load_betfair_config"):
                bf = self.settings_service.load_betfair_config()
                self.bf_username_var.set(getattr(bf, "username", ""))
                self.bf_app_key_var.set(getattr(bf, "app_key", ""))
                self.bf_cert_var.set(getattr(bf, "certificate", ""))
                self.bf_key_var.set(getattr(bf, "private_key", ""))
        except Exception:
            pass

        try:
            if hasattr(self.settings_service, "load_roserpina_config"):
                rs = self.settings_service.load_roserpina_config()
                self.rs_target_var.set(str(getattr(rs, "target_profit_cycle_pct", self.rs_target_var.get())))
                self.rs_max_single_var.set(str(getattr(rs, "max_single_bet_pct", self.rs_max_single_var.get())))
                self.rs_max_total_var.set(str(getattr(rs, "max_total_exposure_pct", self.rs_max_total_var.get())))
                self.rs_max_event_var.set(str(getattr(rs, "max_event_exposure_pct", self.rs_max_event_var.get())))
                self.rs_auto_reset_var.set(str(getattr(rs, "auto_reset_drawdown_pct", self.rs_auto_reset_var.get())))
                self.rs_defense_var.set(str(getattr(rs, "defense_drawdown_pct", self.rs_defense_var.get())))
                self.rs_lockdown_var.set(str(getattr(rs, "lockdown_drawdown_pct", self.rs_lockdown_var.get())))
                self.rs_expansion_profit_var.set(str(getattr(rs, "expansion_profit_pct", self.rs_expansion_profit_var.get())))
                self.rs_expansion_mult_var.set(str(getattr(rs, "expansion_multiplier", self.rs_expansion_mult_var.get())))
                self.rs_defense_mult_var.set(str(getattr(rs, "defense_multiplier", self.rs_defense_mult_var.get())))
                self.rs_table_count_var.set(str(getattr(rs, "table_count", self.rs_table_count_var.get())))
                self.rs_recovery_tables_var.set(str(getattr(rs, "max_recovery_tables", self.rs_recovery_tables_var.get())))
                self.rs_commission_var.set(str(getattr(rs, "commission_pct", self.rs_commission_var.get())))
                self.rs_min_stake_var.set(str(getattr(rs, "min_stake", self.rs_min_stake_var.get())))
                self.rs_max_abs_var.set(str(getattr(rs, "max_stake_abs", self.rs_max_abs_var.get())))
                self.rs_allow_recovery_var.set(bool(getattr(rs, "allow_recovery", self.rs_allow_recovery_var.get())))
                self.rs_anti_dup_var.set(bool(getattr(rs, "anti_duplication_enabled", self.rs_anti_dup_var.get())))
                risk_profile = getattr(rs, "risk_profile", None)
                risk_profile_value = getattr(risk_profile, "value", self.rs_risk_profile_var.get())
                self.rs_risk_profile_var.set(str(risk_profile_value))
        except Exception:
            pass

        self._load_simulation_settings()

    def _load_simulation_settings(self):
        if not hasattr(self.settings_service, "load_simulation_config"):
            return
        try:
            sim = self.settings_service.load_simulation_config()
        except Exception:
            sim = {}

        enabled = bool(sim.get("enabled", True))
        self.simulation_mode_var.set(enabled)
        self.simulation_mode = enabled
        self.sim_label_var.set("SIMULAZIONE" if enabled else "LIVE")
        self.status_broker_var.set("SIMULATION" if enabled else "LIVE")

    def _safe_show_info(self, title: str, msg: str):
        if self._test_mode:
            self._log(f"INFO {title}: {msg}")
            return
        try:
            messagebox.showinfo(title, msg)
        except Exception:
            self._log(f"INFO {title}: {msg}")

    def _safe_show_error(self, title: str, msg: str):
        if self._test_mode:
            self._log(f"ERROR {title}: {msg}")
            return
        try:
            messagebox.showerror(title, msg)
        except Exception:
            self._log(f"ERROR {title}: {msg}")

    def _save_betfair_settings(self):
        try:
            from core.system_state import BetfairConfig

            cfg = BetfairConfig(
                username=self.bf_username_var.get().strip(),
                app_key=self.bf_app_key_var.get().strip(),
                certificate=self.bf_cert_var.get().strip(),
                private_key=self.bf_key_var.get().strip(),
            )
            self.settings_service.save_betfair_config(cfg, password=self.bf_password_var.get())
            self._safe_show_info("OK", "Impostazioni Betfair salvate.")
        except Exception as exc:
            self._safe_show_error("Errore salvataggio Betfair", str(exc))

    def _save_roserpina_settings(self):
        try:
            from core.system_state import RoserpinaConfig, RiskProfile

            cfg = RoserpinaConfig(
                target_profit_cycle_pct=float(self.rs_target_var.get()),
                max_single_bet_pct=float(self.rs_max_single_var.get()),
                max_total_exposure_pct=float(self.rs_max_total_var.get()),
                max_event_exposure_pct=float(self.rs_max_event_var.get()),
                auto_reset_drawdown_pct=float(self.rs_auto_reset_var.get()),
                defense_drawdown_pct=float(self.rs_defense_var.get()),
                lockdown_drawdown_pct=float(self.rs_lockdown_var.get()),
                expansion_profit_pct=float(self.rs_expansion_profit_var.get()),
                expansion_multiplier=float(self.rs_expansion_mult_var.get()),
                defense_multiplier=float(self.rs_defense_mult_var.get()),
                risk_profile=RiskProfile(self.rs_risk_profile_var.get()),
                table_count=int(self.rs_table_count_var.get()),
                max_recovery_tables=int(self.rs_recovery_tables_var.get()),
                allow_recovery=bool(self.rs_allow_recovery_var.get()),
                anti_duplication_enabled=bool(self.rs_anti_dup_var.get()),
                commission_pct=float(self.rs_commission_var.get()),
                min_stake=float(self.rs_min_stake_var.get()),
                max_stake_abs=float(self.rs_max_abs_var.get()),
            )
            self.settings_service.save_roserpina_config(cfg)
            if hasattr(self.runtime, "reload_config"):
                self.runtime.reload_config()
            self._safe_show_info("OK", "Configurazione Roserpina salvata.")
        except Exception as exc:
            self._safe_show_error("Errore salvataggio Roserpina", str(exc))

    # =========================================================
    # LIVE / SIM
    # =========================================================
    def _toggle_simulation_mode(self):
        self._apply_simulation_mode_to_runtime()
        self.sim_label_var.set("SIMULAZIONE" if self.simulation_mode else "LIVE")
        self.status_broker_var.set("SIMULATION" if self.simulation_mode else "LIVE")

        if hasattr(self.settings_service, "save_simulation_config"):
            try:
                current = self.settings_service.load_simulation_config()
            except Exception:
                current = {}
            current["enabled"] = bool(self.simulation_mode)
            try:
                self.settings_service.save_simulation_config(current)
            except Exception:
                pass

        self._log(f"Modalità cambiata: {self.sim_label_var.get()}")
        self._refresh_runtime_status()

    # =========================================================
    # RUNTIME COMMANDS
    # =========================================================
    def _runtime_start(self):
        try:
            result = self.runtime.start(
                password=self.bf_password_var.get() or None,
                simulation_mode=self.simulation_mode,
            )
            self._log(f"START -> {result}")
        except Exception as exc:
            self._log(f"START ERROR -> {exc}")
            self._safe_show_error("Errore avvio", str(exc))
        self._refresh_runtime_status()

    def _runtime_pause(self):
        try:
            result = self.runtime.pause()
            self._log(f"PAUSE -> {result}")
        except Exception as exc:
            self._log(f"PAUSE ERROR -> {exc}")
            self._safe_show_error("Errore pausa", str(exc))
        self._refresh_runtime_status()

    def _runtime_resume(self):
        try:
            result = self.runtime.resume()
            self._log(f"RESUME -> {result}")
        except Exception as exc:
            self._log(f"RESUME ERROR -> {exc}")
            self._safe_show_error("Errore resume", str(exc))
        self._refresh_runtime_status()

    def _runtime_stop(self):
        try:
            result = self.runtime.stop()
            self._log(f"STOP -> {result}")
        except Exception as exc:
            self._log(f"STOP ERROR -> {exc}")
            self._safe_show_error("Errore stop", str(exc))
        self._refresh_runtime_status()

    def _runtime_reset(self):
        try:
            result = self.runtime.reset_cycle()
            self._log(f"RESET -> {result}")
        except Exception as exc:
            self._log(f"RESET ERROR -> {exc}")
            self._safe_show_error("Errore reset", str(exc))
        self._refresh_runtime_status()

    # =========================================================
    # BUS EVENTS
    # =========================================================
    def _wire_bus(self):
        self.bus.subscribe("TELEGRAM_STATUS", self._on_telegram_status)
        self.bus.subscribe("SIGNAL_RECEIVED", self._on_signal_received)
        self.bus.subscribe("SIGNAL_REJECTED", self._on_signal_rejected)
        self.bus.subscribe("SIGNAL_APPROVED", self._on_signal_approved)
        self.bus.subscribe("RUNTIME_STARTED", lambda payload: self.uiq.post(self._refresh_runtime_status))
        self.bus.subscribe("RUNTIME_PAUSED", lambda payload: self.uiq.post(self._refresh_runtime_status))
        self.bus.subscribe("RUNTIME_RESUMED", lambda payload: self.uiq.post(self._refresh_runtime_status))
        self.bus.subscribe("RUNTIME_STOPPED", lambda payload: self.uiq.post(self._refresh_runtime_status))
        self.bus.subscribe("RUNTIME_LOCKDOWN", lambda payload: self.uiq.post(self._refresh_runtime_status))

    def _on_telegram_status(self, payload):
        payload = payload or {}
        self.uiq.post(self._log, f"TELEGRAM_STATUS -> {payload}")
        self.uiq.post(self._update_telegram_status, payload.get("status", "UNKNOWN"), payload.get("message", ""))
        self.uiq.post(self._refresh_runtime_status)

    def _on_signal_received(self, payload):
        self.uiq.post(self._log, f"SIGNAL_RECEIVED -> {payload}")
        self.uiq.post(self._refresh_runtime_status)

    def _on_signal_rejected(self, payload):
        self.uiq.post(self._log, f"SIGNAL_REJECTED -> {payload}")
        self.uiq.post(self._refresh_runtime_status)

    def _on_signal_approved(self, payload):
        self.uiq.post(self._log, f"SIGNAL_APPROVED -> {payload}")
        self.uiq.post(self._refresh_runtime_status)

    def _update_telegram_status(self, status: str, message: str = ""):
        self.telegram_status = str(status or "UNKNOWN")
        self.status_telegram_var.set(self.telegram_status)
        if message:
            self.status_last_error_var.set(str(message))

    # =========================================================
    # STATUS / LOG
    # =========================================================
    def _refresh_runtime_status(self):
        try:
            status = self.runtime.get_status() if hasattr(self.runtime, "get_status") else {}
        except Exception as exc:
            self._log(f"STATUS ERROR -> {exc}")
            status = {}

        broker_status = status.get("broker_status", {}) or {}
        funds = status.get("account_funds", {}) or {}

        try:
            betfair_status = self.betfair_service.status() if hasattr(self.betfair_service, "status") else {}
        except Exception:
            betfair_status = {}

        try:
            telegram_status = self.telegram_service.status() if hasattr(self.telegram_service, "status") else {}
        except Exception:
            telegram_status = {}

        self.status_mode_var.set(str(status.get("mode", "STOPPED")))
        self.status_broker_var.set(
            str(broker_status.get("broker_type", "SIMULATION" if self.simulation_mode else "LIVE"))
        )
        self.status_betfair_var.set("CONNECTED" if betfair_status.get("connected") else "DISCONNECTED")
        self.status_telegram_var.set("LISTENING" if telegram_status.get("connected") else self.telegram_status)
        self.status_bankroll_var.set(str(funds.get("available", status.get("bankroll_current", "0.00"))))
        self.status_drawdown_var.set(str(status.get("drawdown_pct", "0.00")))
        self.status_exposure_var.set(str(funds.get("exposure", status.get("total_exposure", "0.00"))))
        self.status_tables_var.set(str(status.get("active_tables", 0)))
        self.status_last_signal_var.set(str(status.get("last_signal_at", "-")))
        self.status_last_error_var.set(str(status.get("last_error", self.status_last_error_var.get() or "-")))

        if hasattr(self, "risk_tree"):
            try:
                self.risk_tree.delete(*self.risk_tree.get_children())
            except Exception:
                pass

            for table in status.get("tables", []) or []:
                try:
                    self.risk_tree.insert(
                        "",
                        tk.END,
                        values=(
                            table.get("table_id"),
                            table.get("status"),
                            table.get("loss_amount"),
                            table.get("current_exposure"),
                            table.get("current_event_key"),
                            table.get("market_id"),
                            table.get("selection_id"),
                        ),
                    )
                except Exception:
                    continue

    def _log(self, text: str):
        if hasattr(self, "log_text"):
            try:
                self.log_text.insert("end", f"{text}\n")
                self.log_text.see("end")
            except Exception:
                pass

    def _start_polling(self):
        self._refresh_runtime_status()
        self.after(2000, self._start_polling)

    def _on_close(self):
        try:
            if hasattr(self.shutdown, "shutdown"):
                self.shutdown.shutdown()
            elif hasattr(self.shutdown, "run"):
                self.shutdown.run()
        finally:
            try:
                self.destroy()
            except Exception:
                pass


def main():
    app = MiniPickfairGUI()
    app.mainloop()


if __name__ == "__main__":
    main()