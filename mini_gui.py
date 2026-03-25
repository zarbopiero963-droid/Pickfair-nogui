from __future__ import annotations

import tkinter as tk
from tkinter import ttk, messagebox

import customtkinter as ctk

from database import Database
from event_bus import EventBus
from executor_manager import ExecutorManager
from shutdown_manager import ShutdownManager

from services.setting_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController

from controllers.telegram_controller import TelegramController
from telegram_module import TelegramModule
from telegram_tab_ui import TelegramTabUI


class SimpleUIQueue:
    def __init__(self, root: tk.Misc):
        self.root = root

    def post(self, fn, *args, **kwargs):
        self.root.after(0, lambda: fn(*args, **kwargs))


COLORS = {
    "bg_dark": "#111827",
    "bg_panel": "#1f2937",
    "bg_card": "#374151",
    "bg_hover": "#4b5563",
    "border": "#6b7280",
    "text_primary": "#f9fafb",
    "text_secondary": "#d1d5db",
    "text_tertiary": "#9ca3af",
    "success": "#22c55e",
    "error": "#ef4444",
    "loss": "#ef4444",
    "back": "#2563eb",
    "back_hover": "#1d4ed8",
    "button_primary": "#2563eb",
    "button_secondary": "#475569",
    "button_success": "#16a34a",
    "button_danger": "#dc2626",
}
FONTS = {
    "heading": ("Segoe UI", 14, "bold"),
}


class MiniPickfairGUI(ctk.CTk, TelegramModule):
    def __init__(self):
        super().__init__()

        self.title("Pickfair Mini GUI")
        self.geometry("1420x900")

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        self.simulation_mode = True
        self.telegram_status = "STOPPED"

        self._build_core()
        self._build_vars()
        self._build_ui()
        self._load_initial_settings()
        self._wire_bus()
        self._start_polling()

        self.protocol("WM_DELETE_WINDOW", self._on_close)

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

        self.trading_engine = self._create_trading_engine()
        self.runtime = self._create_runtime_controller()

        self.telegram_controller = TelegramController(self)

        self._register_shutdown_hook(
            "telegram_stop",
            self.telegram_service.stop,
            priority=10,
        )
        self._register_shutdown_hook(
            "betfair_disconnect",
            self.betfair_service.disconnect,
            priority=20,
        )
        self._register_shutdown_hook(
            "db_close",
            self.db.close_all_connections,
            priority=30,
        )
        self._register_shutdown_hook(
            "executor_shutdown",
            self.executor.shutdown,
            priority=40,
        )

    def _create_trading_engine(self):
        candidates = [
            {
                "bus": self.bus,
                "db": self.db,
                "client_getter": self.betfair_service.get_client,
                "executor": self.executor,
            },
            {
                "bus": self.bus,
                "db": self.db,
                "client": self.betfair_service.get_client(),
                "executor": self.executor,
            },
            {
                "bus": self.bus,
                "db": self.db,
            },
        ]

        last_exc = None
        for kwargs in candidates:
            try:
                return TradingEngine(**kwargs)
            except TypeError as exc:
                last_exc = exc
                continue

        raise RuntimeError(f"Impossibile inizializzare TradingEngine: {last_exc}")

    def _create_runtime_controller(self):
        candidates = [
            {
                "bus": self.bus,
                "db": self.db,
                "settings_service": self.settings_service,
                "betfair_service": self.betfair_service,
                "telegram_service": self.telegram_service,
                "executor": self.executor,
                "trading_engine": self.trading_engine,
            },
            {
                "bus": self.bus,
                "db": self.db,
                "settings_service": self.settings_service,
                "betfair_service": self.betfair_service,
                "telegram_service": self.telegram_service,
                "executor": self.executor,
            },
            {
                "bus": self.bus,
                "db": self.db,
                "settings_service": self.settings_service,
                "betfair_service": self.betfair_service,
                "telegram_service": self.telegram_service,
            },
        ]

        last_exc = None
        for kwargs in candidates:
            try:
                return RuntimeController(**kwargs)
            except TypeError as exc:
                last_exc = exc
                continue

        raise RuntimeError(f"Impossibile inizializzare RuntimeController: {last_exc}")

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

    # =========================================================
    # TK VARS
    # =========================================================
    def _build_vars(self):
        self.bf_username_var = tk.StringVar()
        self.bf_password_var = tk.StringVar()
        self.bf_app_key_var = tk.StringVar()
        self.bf_cert_var = tk.StringVar()
        self.bf_key_var = tk.StringVar()

        self.rs_target_var = tk.StringVar(value="3.0")
        self.rs_max_single_var = tk.StringVar(value="18.0")
        self.rs_max_total_var = tk.StringVar(value="35.0")
        self.rs_max_event_var = tk.StringVar(value="18.0")
        self.rs_auto_reset_var = tk.StringVar(value="15.0")
        self.rs_defense_var = tk.StringVar(value="7.5")
        self.rs_lockdown_var = tk.StringVar(value="20.0")
        self.rs_expansion_profit_var = tk.StringVar(value="5.0")
        self.rs_expansion_mult_var = tk.StringVar(value="1.10")
        self.rs_defense_mult_var = tk.StringVar(value="0.80")
        self.rs_table_count_var = tk.StringVar(value="5")
        self.rs_recovery_tables_var = tk.StringVar(value="2")
        self.rs_commission_var = tk.StringVar(value="4.5")
        self.rs_min_stake_var = tk.StringVar(value="0.10")
        self.rs_max_abs_var = tk.StringVar(value="10000.0")
        self.rs_allow_recovery_var = tk.BooleanVar(value=True)
        self.rs_anti_dup_var = tk.BooleanVar(value=True)
        self.rs_risk_profile_var = tk.StringVar(value="BALANCED")

        self.simulation_mode_var = tk.BooleanVar(value=True)

        self.status_mode_var = tk.StringVar(value="STOPPED")
        self.status_betfair_var = tk.StringVar(value="DISCONNECTED")
        self.status_telegram_var = tk.StringVar(value="STOPPED")
        self.status_bankroll_var = tk.StringVar(value="0.00")
        self.status_drawdown_var = tk.StringVar(value="0.00")
        self.status_exposure_var = tk.StringVar(value="0.00")
        self.status_tables_var = tk.StringVar(value="0")
        self.status_last_signal_var = tk.StringVar(value="-")
        self.status_last_error_var = tk.StringVar(value="-")
        self.sim_label_var = tk.StringVar(value="SIMULAZIONE")

    # =========================================================
    # UI
    # =========================================================
    def _build_ui(self):
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
        top = ctk.CTkFrame(self, fg_color="transparent")
        top.grid(row=0, column=0, sticky="ew", padx=12, pady=12)
        top.grid_columnconfigure(1, weight=1)

        title = ctk.CTkLabel(
            top,
            text="Pickfair Mini Control Panel",
            font=("Segoe UI", 18, "bold"),
        )
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
        self.live_sim_switch.select()

        self.live_sim_label = ctk.CTkLabel(
            center,
            textvariable=self.sim_label_var,
            font=("Segoe UI", 12, "bold"),
        )
        self.live_sim_label.pack(side=tk.LEFT, padx=10)

    def _build_dashboard_tab(self):
        frame = self.tab_dashboard
        frame.grid_columnconfigure((0, 1, 2, 3), weight=1)

        cards = [
            ("Runtime", self.status_mode_var),
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
            ctk.CTkLabel(
                card,
                text=label,
                font=("Segoe UI", 12),
            ).pack(anchor="w", padx=12, pady=(10, 4))
            ctk.CTkLabel(
                card,
                textvariable=var,
                font=("Segoe UI", 16, "bold"),
            ).pack(anchor="w", padx=12, pady=(0, 10))

        controls = ctk.CTkFrame(frame)
        controls.grid(row=2, column=0, columnspan=4, sticky="ew", padx=8, pady=8)

        ctk.CTkButton(controls, text="Avvia", command=self._runtime_start).pack(side=tk.LEFT, padx=6, pady=10)
        ctk.CTkButton(controls, text="Pausa", command=self._runtime_pause).pack(side=tk.LEFT, padx=6, pady=10)
        ctk.CTkButton(controls, text="Resume", command=self._runtime_resume).pack(side=tk.LEFT, padx=6, pady=10)
        ctk.CTkButton(controls, text="Stop", command=self._runtime_stop).pack(side=tk.LEFT, padx=6, pady=10)
        ctk.CTkButton(controls, text="Reset Ciclo", command=self._runtime_reset).pack(side=tk.LEFT, padx=6, pady=10)
        ctk.CTkButton(controls, text="Refresh", command=self._refresh_runtime_status).pack(side=tk.LEFT, padx=6, pady=10)

        err = ctk.CTkFrame(frame)
        err.grid(row=3, column=0, columnspan=4, sticky="ew", padx=8, pady=8)
        ctk.CTkLabel(err, text="Ultimo Errore", font=("Segoe UI", 12)).pack(anchor="w", padx=12, pady=(10, 4))
        ctk.CTkLabel(
            err,
            textvariable=self.status_last_error_var,
            wraplength=1200,
        ).pack(anchor="w", padx=12, pady=(0, 10))

    def _build_settings_tab(self):
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
        ctk.CTkButton(
            btns,
            text="Salva Impostazioni Betfair",
            command=self._save_betfair_settings,
        ).pack(side=tk.LEFT, padx=6)

    def _build_telegram_tab(self):
        TelegramTabUI(self.tab_telegram, self)

    def _build_roserpina_tab(self):
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
        ctk.CTkCheckBox(
            cb,
            text="Allow Recovery",
            variable=self.rs_allow_recovery_var,
        ).pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkCheckBox(
            cb,
            text="Anti Duplication Enabled",
            variable=self.rs_anti_dup_var,
        ).pack(side=tk.LEFT, padx=8, pady=8)

        ctk.CTkButton(
            outer,
            text="Salva Roserpina",
            command=self._save_roserpina_settings,
        ).pack(anchor="w", padx=12, pady=12)

    def _build_risk_tab(self):
        frame = self.tab_risk
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

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
        ctk.CTkButton(
            btns,
            text="Refresh Risk Desk",
            command=self._refresh_runtime_status,
        ).pack(side=tk.LEFT, padx=6)

    def _build_log_tab(self):
        frame = self.tab_log
        self.log_text = ctk.CTkTextbox(frame)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

    def _labeled_entry(self, parent, label, variable, width=320, show=None):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(row, text=label, width=220, anchor="w").pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkEntry(row, textvariable=variable, width=width, show=show).pack(side=tk.LEFT, padx=8, pady=8)

    # =========================================================
    # SETTINGS LOAD / SAVE
    # =========================================================
    def _load_initial_settings(self):
        bf = self.settings_service.load_betfair_config()
        self.bf_username_var.set(bf.username)
        self.bf_app_key_var.set(bf.app_key)
        self.bf_cert_var.set(bf.certificate)
        self.bf_key_var.set(bf.private_key)

        rs = self.settings_service.load_roserpina_config()
        self.rs_target_var.set(str(rs.target_profit_cycle_pct))
        self.rs_max_single_var.set(str(rs.max_single_bet_pct))
        self.rs_max_total_var.set(str(rs.max_total_exposure_pct))
        self.rs_max_event_var.set(str(rs.max_event_exposure_pct))
        self.rs_auto_reset_var.set(str(rs.auto_reset_drawdown_pct))
        self.rs_defense_var.set(str(rs.defense_drawdown_pct))
        self.rs_lockdown_var.set(str(rs.lockdown_drawdown_pct))
        self.rs_expansion_profit_var.set(str(rs.expansion_profit_pct))
        self.rs_expansion_mult_var.set(str(rs.expansion_multiplier))
        self.rs_defense_mult_var.set(str(rs.defense_multiplier))
        self.rs_table_count_var.set(str(rs.table_count))
        self.rs_recovery_tables_var.set(str(rs.max_recovery_tables))
        self.rs_commission_var.set(str(rs.commission_pct))
        self.rs_min_stake_var.set(str(rs.min_stake))
        self.rs_max_abs_var.set(str(rs.max_stake_abs))
        self.rs_allow_recovery_var.set(bool(rs.allow_recovery))
        self.rs_anti_dup_var.set(bool(rs.anti_duplication_enabled))
        self.rs_risk_profile_var.set(rs.risk_profile.value)

    def _save_betfair_settings(self):
        from core.system_state import BetfairConfig

        cfg = BetfairConfig(
            username=self.bf_username_var.get().strip(),
            app_key=self.bf_app_key_var.get().strip(),
            certificate=self.bf_cert_var.get().strip(),
            private_key=self.bf_key_var.get().strip(),
        )
        self.settings_service.save_betfair_config(
            cfg,
            password=self.bf_password_var.get(),
        )
        messagebox.showinfo("OK", "Impostazioni Betfair salvate.")

    def _save_roserpina_settings(self):
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
        messagebox.showinfo("OK", "Configurazione Roserpina salvata.")

    # =========================================================
    # LIVE / SIM SWITCH
    # =========================================================
    def _toggle_simulation_mode(self):
        self.simulation_mode = bool(self.simulation_mode_var.get())
        self.sim_label_var.set("SIMULAZIONE" if self.simulation_mode else "LIVE")
        self._log(f"Modalità cambiata: {self.sim_label_var.get()}")

    # =========================================================
    # RUNTIME COMMANDS
    # =========================================================
    def _runtime_start(self):
        result = self.runtime.start(password=self.bf_password_var.get() or None)
        self._log(f"START -> {result}")
        self._refresh_runtime_status()

    def _runtime_pause(self):
        result = self.runtime.pause()
        self._log(f"PAUSE -> {result}")
        self._refresh_runtime_status()

    def _runtime_resume(self):
        result = self.runtime.resume()
        self._log(f"RESUME -> {result}")
        self._refresh_runtime_status()

    def _runtime_stop(self):
        result = self.runtime.stop()
        self._log(f"STOP -> {result}")
        self._refresh_runtime_status()

    def _runtime_reset(self):
        result = self.runtime.reset_cycle()
        self._log(f"RESET -> {result}")
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
        self.uiq.post(
            self._update_telegram_status,
            payload.get("status", "UNKNOWN"),
            payload.get("message", ""),
        )
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

    # =========================================================
    # STATUS / LOG
    # =========================================================
    def _refresh_runtime_status(self):
        try:
            status = self.runtime.get_status()
        except Exception as exc:
            self._log(f"STATUS ERROR -> {exc}")
            return

        self.status_mode_var.set(str(status.get("mode", "-")))
        self.status_betfair_var.set(
            "CONNECTED" if self.betfair_service.status().get("connected") else "DISCONNECTED"
        )
        self.status_telegram_var.set(
            "LISTENING" if self.telegram_service.status().get("connected") else "STOPPED"
        )
        self.status_bankroll_var.set(str(status.get("bankroll_current", "0.00")))
        self.status_drawdown_var.set(str(status.get("drawdown_pct", "0.00")))
        self.status_exposure_var.set(str(status.get("total_exposure", "0.00")))
        self.status_tables_var.set(str(status.get("active_tables", 0)))
        self.status_last_signal_var.set(str(status.get("last_signal_at", "-")))
        self.status_last_error_var.set(str(status.get("last_error", "-")))

        self.risk_tree.delete(*self.risk_tree.get_children())
        for table in status.get("tables", []):
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

    def _log(self, text: str):
        self.log_text.insert("end", f"{text}\n")
        self.log_text.see("end")

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
            self.destroy()


def main():
    app = MiniPickfairGUI()
    app.mainloop()


if __name__ == "__main__":
    main()