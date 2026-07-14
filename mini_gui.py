from __future__ import annotations

import logging
import math
import types

_LOGGER = logging.getLogger(__name__)
try:
    import tkinter as tk
    from tkinter import ttk, messagebox
except ModuleNotFoundError:  # pragma: no cover - headless CI fallback
    class _DummyWidget:
        @staticmethod
        def __init__(*args, **kwargs):
            """No-op constructor for headless widget fallback."""
            _ = args, kwargs

        @staticmethod
        def pack(*args, **kwargs):
            """No-op geometry manager fallback."""
            _ = args, kwargs

        @staticmethod
        def grid(*args, **kwargs):
            """No-op grid geometry manager fallback."""
            _ = args, kwargs

        @staticmethod
        def place(*args, **kwargs):
            """No-op place geometry manager fallback."""
            _ = args, kwargs

        @staticmethod
        def configure(*args, **kwargs):
            """Headless GUI compatibility fallback."""
            _ = args, kwargs

        @staticmethod
        def config(*args, **kwargs):
            """Headless GUI compatibility fallback."""
            return _DummyWidget.configure(*args, **kwargs)

        @staticmethod
        def bind(*args, **kwargs):
            """Headless GUI compatibility fallback."""
            _ = args, kwargs

        @staticmethod
        def destroy(*args, **kwargs):
            """Compatibility no-op for headless/test GUI adapters."""
            _ = args, kwargs

    class _DummyVar:
        def __init__(self, value=None):
            """Headless GUI compatibility fallback."""
            self._value = value

        def get(self):
            """Headless GUI compatibility fallback."""
            return self._value

        def set(self, value):
            """Headless GUI compatibility fallback."""
            self._value = value

    tk = types.SimpleNamespace(
        Tk=_DummyWidget,
        Frame=_DummyWidget,
        Label=_DummyWidget,
        Button=_DummyWidget,
        Entry=_DummyWidget,
        Checkbutton=_DummyWidget,
        Text=_DummyWidget,
        StringVar=_DummyVar,
        BooleanVar=_DummyVar,
        IntVar=_DummyVar,
        END="end",
        BOTH="both",
        X="x",
        Y="y",
        LEFT="left",
        RIGHT="right",
        TOP="top",
        BOTTOM="bottom",
    )
    ttk = types.SimpleNamespace(Combobox=_DummyWidget)

    class _MessageBoxFallback:
        @staticmethod
        def showwarning(*_args, **_kwargs):
            """Headless GUI compatibility fallback."""
            _ = _args, _kwargs

        @staticmethod
        def showinfo(*_args, **_kwargs):
            """Headless GUI compatibility fallback."""
            _ = _args, _kwargs

        @staticmethod
        def showerror(*_args, **_kwargs):
            """Headless GUI compatibility fallback."""
            _ = _args, _kwargs

        @staticmethod
        def askyesno(*_args, **_kwargs):
            """Headless GUI compatibility fallback."""
            return True

    messagebox = _MessageBoxFallback()
from typing import Callable

try:
    import customtkinter as ctk
except Exception:  # pragma: no cover
    class _FallbackCTk(tk.Tk):
        pass

    class _FallbackFrame(tk.Frame):
        def __init__(self, master=None, fg_color=None, **kwargs):
            """Headless GUI compatibility fallback."""
            tk.Frame.__init__(self, master, **kwargs)

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
            tk.Label.__init__(
                self,
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
            tk.Button.__init__(self, master, text=text, command=command, **kwargs)

    class _FallbackEntry(tk.Entry):
        def __init__(self, master=None, textvariable=None, width=None, show=None, **kwargs):
            tk.Entry.__init__(self, master, textvariable=textvariable, show=show, **kwargs)

    class _FallbackCheckBox(tk.Checkbutton):
        def __init__(self, master=None, text="", variable=None, **kwargs):
            tk.Checkbutton.__init__(self, master, text=text, variable=variable, **kwargs)

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
            tk.Checkbutton.__init__(
                self,
                master,
                text=text,
                variable=variable,
                command=command,
                onvalue=onvalue,
                offvalue=offvalue,
                **kwargs,
            )

    class _FallbackComboBox(ttk.Combobox):
        def __init__(self, master=None, variable=None, values=None, width=None, **kwargs):
            ttk.Combobox.__init__(self, master, textvariable=variable, values=values or [], width=width, **kwargs)

    class _FallbackText(tk.Text):
        pass

    class _FallbackScrollableFrame(tk.Frame):
        def __init__(self, master=None, **kwargs):
            tk.Frame.__init__(self, master, **kwargs)

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

import trading_config
from services.settings_service import SettingsService
from services.betfair_service import BetfairService
from services.telegram_service import TelegramService

from core.trading_engine import TradingEngine
from core.runtime_controller import RuntimeController
from observability import RuntimeProbe
from safe_mode import get_safe_mode_manager

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
            self.app = app
            self.frame = ctk.CTkFrame(parent)
            if hasattr(self.frame, "pack"):
                self.frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)
            self.label = ctk.CTkLabel(self.frame, text="Telegram UI non disponibile")
            if hasattr(self.label, "pack"):
                self.label.pack(anchor="w", padx=12, pady=12)

try:
    from theme import COLORS, FONTS
except Exception:  # pragma: no cover
    COLORS = {}
    FONTS = {}


from headless_ui_stubs import (  # noqa: E402
    _HeadlessBoolVar,
    _HeadlessStringVar,
    _HeadlessRoot,
    _DummyButton,
    _DummyTree,
    _DummyLog,
)


class SimpleUIQueue:
    def __init__(self, root):
        self.root = root

    def post(self, fn: Callable, *args, **kwargs):
        if hasattr(self.root, "after"):
            self.root.after(0, lambda: fn(*args, **kwargs))
        else:
            fn(*args, **kwargs)


class MiniPickfairGUI(ctk.CTk, TelegramModule):
    def __init__(self, test_mode: bool = False, force_simulation: bool = False):
        self._test_mode = bool(test_mode)
        # Fail-closed from construction (#355): quando True, lo stato persistito
        # execution_mode/live_enabled NON viene mai applicato al runtime — la
        # forzatura a SIMULATION avviene PRIMA della prima sync, cosi' non esiste
        # nemmeno una finestra transitoria in cui il costruttore osservi LIVE.
        self._force_simulation_startup = bool(force_simulation)

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
        # Fail-closed SIM/LIVE (#350, decisione owner "applica entrambe"):
        # la sync di modalita' e' NON confermata finche' il runtime non la
        # accetta; su fallimento il toggle si reverte allo stato confermato,
        # OGNI avvio resta bloccato finche' una sync non riesce e, se lo
        # stato confermato era LIVE, parte l'emergency stop del trading.
        self._mode_sync_failed = False
        self._mode_sync_kill_done = False
        self._start_refused_mode_unconfirmed_total = 0
        self.telegram_status = "STOPPED"

        self._build_core()
        self._build_vars()
        self._build_ui()
        self._load_initial_settings()
        self._wire_bus()
        self._apply_simulation_mode_to_runtime()
        self._status_refresh_inflight = False
        self._status_refresh_pending = False
        self._runtime_commands_inflight: set[str] = set()
        self._runtime_command_rejected_total = 0

        self._refresh_coordinators = {}
        self._wire_refresh_coordinators()

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
        self.safe_mode = get_safe_mode_manager()

        self.trading_engine = TradingEngine(
            bus=self.bus,
            db=self.db,
            client_getter=self.betfair_service.get_client,
            executor=self.executor,
            safe_mode=self.safe_mode,
        )

        self.runtime = RuntimeController(
            bus=self.bus,
            db=self.db,
            settings_service=self.settings_service,
            betfair_service=self.betfair_service,
            telegram_service=self.telegram_service,
            trading_engine=self.trading_engine,
            executor=self.executor,
            safe_mode=self.safe_mode,
        )
        self.trading_engine.runtime_controller = self.runtime
        self.trading_engine.simulation_broker = getattr(self, "simulation_broker", None)
        self.trading_engine.betfair_client = self.betfair_service.get_client()
        self.runtime_probe = RuntimeProbe(
            db=self.db,
            trading_engine=self.trading_engine,
            runtime_controller=self.runtime,
            betfair_service=self.betfair_service,
            safe_mode=self.safe_mode,
            shutdown_manager=self.shutdown,
            telegram_service=self.telegram_service,
            settings_service=self.settings_service,
        )
        self.runtime.runtime_probe = self.runtime_probe
        self.runtime.enforce_probe_readiness_gate = True

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
        desired = bool(self.simulation_mode_var.get())
        if not hasattr(self.runtime, "set_simulation_mode"):
            # Runtime senza sync di modalita': nessuna conferma possibile ne'
            # necessaria; lo stato locale segue l'intento utente.
            self.simulation_mode = desired
            return
        # Snapshot PRIMA del tentativo: kill-switch/allarmi solo sulla
        # TRANSIZIONE verso lo stato failed (CodeRabbit #350) — i retry con
        # sync ancora rotta (es. click ripetuti su AVVIA) non devono
        # ri-sparare emergency_stop/cancel-all ad ogni giro.
        already_unconfirmed = bool(getattr(self, "_mode_sync_failed", False))
        try:
            self.runtime.set_simulation_mode(desired)
        except Exception:
            # Policy owner #350 ("2+guardia", concorde con GPT/Fable/GLM/Codacy):
            # un intento NON confermato dal runtime non deve MAI persistere, ne'
            # nel flag ne' nelle var/label GUI -> rollback all'ultimo stato
            # CONFERMATO (evita split-brain GUI-SIM/runtime-LIVE) e blocco
            # dell'avvio LIVE finche' una sync non riesce (_mode_sync_failed).
            # `except Exception` e' voluto: e' il boundary fail-safe della GUI
            # (niente crash in __init__/toggle); l'errore resta VISIBILE qui
            # sotto (log tab + logger con traceback), mai ingoiato.
            confirmed = bool(getattr(self, "simulation_mode", True))
            self._mode_sync_failed = True
            self.simulation_mode_var.set(confirmed)
            if hasattr(self, "execution_mode_var"):
                self.execution_mode_var.set("SIMULATION" if confirmed else "LIVE")
            self._log(
                "SYNC MODALITA' FALLITA -> rollback allo stato confermato "
                f"({'SIMULATION' if confirmed else 'LIVE'}); ogni avvio bloccato"
            )
            _LOGGER.exception(
                "set_simulation_mode sync failed; rolled back to confirmed=%s",
                "SIMULATION" if confirmed else "LIVE",
            )
            # Decisione owner #350 ("applica entrambe", punto A/Fugu): se
            # l'ultimo stato CONFERMATO e' LIVE, il runtime puo' stare
            # operando denaro reale con modalita' ormai incerta -> kill-switch
            # immediato. emergency_stop e' fail-closed by-design (runtime_controller
            # :1285): LOCKDOWN + cancel-all, resta LOCKED anche se il cancel
            # fallisce, riprende solo con reset_emergency() esplicito.
            # Semantica (CodeRabbit+GPT #350): il kill si RITENTA a ogni
            # fallimento di sync FINCHE' non riesce (_mode_sync_kill_done);
            # dopo il successo non si ripete (niente cancel-all a raffica sui
            # retry). Il flag si ri-arma su sync riuscita.
            if not confirmed:
                if hasattr(self.runtime, "emergency_stop"):
                    if not getattr(self, "_mode_sync_kill_done", False):
                        try:
                            self.runtime.emergency_stop(reason="mode_sync_failed_fail_closed")
                            self._mode_sync_kill_done = True
                            self._log("EMERGENCY STOP inviato: trading LIVE fermato (modalita' incerta)")
                            self._safe_show_error(
                                "EMERGENCY STOP",
                                "Sync modalita' fallita con LIVE attivo: trading fermato "
                                "(LOCKDOWN). Per riprendere serve reset_emergency.",
                            )
                        except Exception:
                            # kill_done resta False -> il kill viene RITENTATO
                            # al prossimo fallimento (GPT: un cancel-all fallito
                            # transitoriamente non va abbandonato). In produzione
                            # emergency_stop LOCKA comunque come PRIMA azione
                            # (_emergency_stopped=True prima di persist/cancel)
                            # e is_live_allowed() blocca ogni submission live.
                            # Allarme operatore, mai silenzioso.
                            _LOGGER.exception("emergency_stop dopo sync fallita anch'esso fallito")
                            self._log("EMERGENCY STOP FALLITO dopo sync fallita -> INTERVENTO MANUALE RICHIESTO")
                            self._safe_show_error(
                                "EMERGENCY STOP FALLITO",
                                "Il runtime potrebbe operare in LIVE con modalita' incerta: "
                                "FERMARE MANUALMENTE il trading (kill switch / chiusura processo).",
                            )
                elif not already_unconfirmed:
                    # Fable/Fugu/GPT (#350): MAI skip silenzioso sul percorso
                    # denaro reale. Ramo puramente DIFENSIVO: il RuntimeController
                    # di produzione espone SEMPRE emergency_stop (invariante
                    # testata in test_live_switch_fail_closed). Se un runtime
                    # alternativo ne fosse privo: allarme critico + best-effort
                    # stop() ordinario — meglio un halt parziale che nessuna
                    # interruzione delle submission.
                    _LOGGER.critical(
                        "runtime privo di emergency_stop con LIVE confermato e sync incerta"
                    )
                    self._log(
                        "ATTENZIONE: runtime senza emergency_stop -> kill automatico "
                        "IMPOSSIBILE, INTERVENTO MANUALE RICHIESTO"
                    )
                    stop_fn = getattr(self.runtime, "stop", None)
                    if callable(stop_fn):
                        try:
                            stop_fn()
                            self._log("STOP ordinario inviato (fallback senza emergency_stop)")
                        except Exception:
                            _LOGGER.exception("stop() di fallback fallito dopo sync incerta")
                            self._log("STOP di fallback FALLITO -> fermare il processo manualmente")
                    self._safe_show_error(
                        "KILL-SWITCH NON DISPONIBILE",
                        "Il runtime non espone emergency_stop e la modalita' e' incerta "
                        "con LIVE confermato: FERMARE MANUALMENTE il trading.",
                    )
            return
        # Sync CONFERMATA dal runtime: solo ora lo stato locale avanza.
        self.simulation_mode = desired
        self._mode_sync_failed = False
        # Re-arm del kill-switch (Fugu/Fable #350): dopo una sync riuscita
        # una FUTURA transizione a sync-fallita con LIVE confermato deve
        # poter sparare di nuovo l'emergency stop.
        self._mode_sync_kill_done = False

    # =========================================================
    # VARIABLES
    # =========================================================
    def _make_string_var(self, value=""):
        if self._test_mode:
            return _HeadlessStringVar(value)
        return tk.StringVar(value=value)

    def _make_bool_var(self, value=False):
        if self._test_mode:
            return _HeadlessBoolVar(value)
        return tk.BooleanVar(value=value)

    def _build_vars(self):
        self.bf_username_var = self._make_string_var()
        self.bf_password_var = self._make_string_var()
        self.bf_app_key_var = self._make_string_var()
        self.bf_cert_var = self._make_string_var()
        self.bf_key_var = self._make_string_var()

        self.rs_target_var = self._make_string_var("5.0")
        self.rs_max_single_var = self._make_string_var("1.0")
        self.rs_max_total_var = self._make_string_var("10.0")
        self.rs_max_event_var = self._make_string_var("2.5")
        self.rs_auto_reset_var = self._make_string_var("5.0")
        self.rs_defense_var = self._make_string_var("15.0")
        self.rs_lockdown_var = self._make_string_var("25.0")
        self.rs_expansion_profit_var = self._make_string_var("10.0")
        self.rs_expansion_mult_var = self._make_string_var("1.2")
        self.rs_defense_mult_var = self._make_string_var("0.8")
        self.rs_table_count_var = self._make_string_var("4")
        self.rs_recovery_tables_var = self._make_string_var("1")
        self.rs_commission_var = self._make_string_var("4.5")
        self.rs_min_stake_var = self._make_string_var("0.10")
        self.rs_max_abs_var = self._make_string_var("10000.0")
        # Hard-stop giornalieri (safety-critical, prerequisiti del gate LIVE).
        # Campo vuoto = "non impostato" (None): il salvataggio NON azzera il
        # valore persistito e il gate LIVE resta fail-closed se non configurato
        # (vedi _parse_hard_stop / _save_roserpina_settings).
        self.rs_max_daily_loss_var = self._make_string_var("")
        self.rs_max_open_exposure_var = self._make_string_var("")
        self.rs_max_drawdown_hard_stop_var = self._make_string_var("")
        # Book % (over-round): soglie avviso/blocco, applicate al submit dutching.
        # Default allineati alle costanti di sistema (no valori hardcoded a parte).
        self.rs_book_warning_var = self._make_string_var(str(trading_config.BOOK_WARNING))
        self.rs_book_block_var = self._make_string_var(str(trading_config.BOOK_BLOCK))
        # Liquidity guard: 2 numerici + 2 toggle, applicati al submit dutching.
        self.rs_liq_multiplier_var = self._make_string_var(str(trading_config.LIQUIDITY_MULTIPLIER))
        self.rs_liq_min_abs_var = self._make_string_var(str(trading_config.MIN_LIQUIDITY_ABSOLUTE))
        self.rs_liq_guard_enabled_var = self._make_bool_var(bool(trading_config.LIQUIDITY_GUARD_ENABLED))
        self.rs_liq_warning_only_var = self._make_bool_var(bool(trading_config.LIQUIDITY_WARNING_ONLY))
        # Quota minima di strategia (floor editabile sopra il minimo Betfair 1.01).
        self.rs_min_price_var = self._make_string_var(str(trading_config.MIN_PRICE))
        # Max Win: cap vincita/payout per gamba + toggle solo-avviso (opt-in).
        self.rs_max_win_var = self._make_string_var(str(trading_config.MAX_WIN))
        self.rs_max_win_warning_only_var = self._make_bool_var(True)
        # Max Stake %: soglia di AVVISO (mai blocco) sull'esposizione dell'operazione.
        self.rs_max_stake_pct_var = self._make_string_var(str(trading_config.MAX_STAKE_PCT * 100))
        # Simulazione: config del broker simulato (gia' enforced in betfair_service).
        self.sim_starting_balance_var = self._make_string_var("1000.0")
        self.sim_partial_fill_var = self._make_bool_var(True)
        self.sim_consume_liq_var = self._make_bool_var(True)
        self.sim_persist_state_var = self._make_bool_var(True)
        # Watchdog anomalie: toggle live (gia' enforced in watchdog_service). Il
        # watchdog e' default-ON: la checkbox parte spuntata (None/non-configurato
        # => ON).
        self.anomaly_enabled_var = self._make_bool_var(True)
        self.anomaly_alerts_enabled_var = self._make_bool_var(False)
        self.anomaly_actions_enabled_var = self._make_bool_var(False)
        # Alert Telegram: routing/soglie notifiche (gia' enforced live in
        # telegram_alerts_service). Il saver dedicato read-merge preserva le credenziali.
        self.alert_enabled_var = self._make_bool_var(False)
        self.alert_chat_id_var = self._make_string_var("")
        self.alert_chat_name_var = self._make_string_var("")
        self.alert_min_severity_var = self._make_string_var("WARNING")
        self.alert_cooldown_var = self._make_string_var("300")
        self.alert_dedup_var = self._make_bool_var(True)
        self.alert_format_rich_var = self._make_bool_var(True)
        self.rs_allow_recovery_var = self._make_bool_var(True)
        self.rs_anti_dup_var = self._make_bool_var(True)
        self.rs_risk_profile_var = self._make_string_var("BALANCED")

        self.simulation_mode_var = self._make_bool_var(True)
        self.execution_mode_var = self._make_string_var("SIMULATION")
        self.live_enabled_var = self._make_bool_var(False)
        self.kill_switch_var = self._make_bool_var(False)
        self.live_requested_mode_var = self._make_string_var("SIMULATION")
        self.live_effective_status_var = self._make_string_var("SAFE_MODE")
        self.live_readiness_level_var = self._make_string_var("UNKNOWN")
        self.live_readiness_blockers_var = self._make_string_var("No readiness data.")
        self.live_control_state_var = self._make_string_var("SIMULATION")
        self.live_last_decision_var = self._make_string_var("N/A")
        self.live_last_reason_var = self._make_string_var("N/A")

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
            self.tab_bet_history = object()
            self.tab_roserpina = object()
            self.tab_simulazione = object()
            self.tab_watchdog = object()
            self.tab_alert = object()
            self.tab_risk_history = object()
            self.tab_risk = object()
            self.tab_log = object()

            self._build_topbar()
            self._build_dashboard_tab()
            self._build_settings_tab()
            self._build_telegram_tab()
            self._build_bet_history_tab()
            self._build_roserpina_tab()
            self._build_simulation_tab()
            self._build_watchdog_tab()
            self._build_alert_tab()
            self._build_risk_tab()
            self._build_risk_desk_history_tab()
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
        self.tab_bet_history = self.tabs.add("Storico Bet")
        self.tab_roserpina = self.tabs.add("Roserpina")
        self.tab_simulazione = self.tabs.add("Simulazione")
        self.tab_watchdog = self.tabs.add("Watchdog")
        self.tab_alert = self.tabs.add("Alert")
        self.tab_risk_history = self.tabs.add("Storico Risk Desk")
        self.tab_risk = self.tabs.add("Risk Desk")
        self.tab_provider = self.tabs.add("Provider")
        self.tab_log = self.tabs.add("Log")

        self._build_dashboard_tab()
        self._build_settings_tab()
        self._build_telegram_tab()
        self._build_bet_history_tab()
        self._build_roserpina_tab()
        self._build_simulation_tab()
        self._build_watchdog_tab()
        self._build_alert_tab()
        self._build_risk_tab()
        self._build_risk_desk_history_tab()
        self._build_provider_tab()
        self._build_log_tab()

    def _build_topbar(self):
        if self._test_mode:
            self.live_sim_switch = object()
            self.live_sim_label = object()
            self.btn_apply_control_plane = _DummyButton(self._apply_execution_control_changes)
            self.btn_refresh_control_plane = _DummyButton(self._refresh_runtime_status)
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

        control = ctk.CTkFrame(top, fg_color="transparent")
        control.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        try:
            control.grid_columnconfigure(7, weight=1)
        except Exception:
            pass

        ctk.CTkLabel(control, text="Requested:").grid(row=0, column=0, sticky="w", padx=(0, 6))
        self.live_requested_mode_label = ctk.CTkLabel(
            control,
            textvariable=self.live_requested_mode_var,
            font=("Segoe UI", 12, "bold"),
        )
        self.live_requested_mode_label.grid(row=0, column=1, sticky="w", padx=(0, 12))

        ctk.CTkLabel(control, text="Execution:").grid(row=0, column=2, sticky="w", padx=(0, 6))
        self.execution_mode_combo = ctk.CTkComboBox(
            control,
            variable=self.execution_mode_var,
            values=["SIMULATION", "LIVE"],
            width=140,
            command=self._on_execution_mode_changed,
        )
        self.execution_mode_combo.grid(row=0, column=3, sticky="w", padx=(0, 12))

        self.live_gate_switch = ctk.CTkSwitch(
            control,
            text="LIVE Gate",
            variable=self.live_enabled_var,
            command=self._on_live_gate_toggled,
        )
        self.live_gate_switch.grid(row=0, column=4, sticky="w", padx=(0, 12))

        self.kill_switch_switch = ctk.CTkSwitch(
            control,
            text="KILL Switch",
            variable=self.kill_switch_var,
            command=self._on_kill_switch_toggled,
            progress_color="#d9534f",
        )
        self.kill_switch_switch.grid(row=0, column=5, sticky="w", padx=(0, 12))

        self.btn_apply_control_plane = ctk.CTkButton(
            control,
            text="Applica Cambiamenti",
            command=self._apply_execution_control_changes,
            width=160,
        )
        self.btn_apply_control_plane.grid(row=0, column=6, sticky="w", padx=(0, 12))

        self.btn_refresh_control_plane = ctk.CTkButton(
            control,
            text="Refresh",
            command=self._refresh_runtime_status,
            width=80,
        )
        self.btn_refresh_control_plane.grid(row=0, column=8, sticky="e")

    def _build_dashboard_tab(self):
        if self._test_mode:
            self.btn_start = _DummyButton(self._runtime_start)
            self.btn_stop = _DummyButton(self._runtime_stop)
            self.btn_pause = _DummyButton(self._runtime_pause)
            self.btn_resume = _DummyButton(self._runtime_resume)
            self.btn_reset = _DummyButton(self._runtime_reset)
            self.btn_emergency = _DummyButton(self._runtime_emergency_stop)
            return

        frame = self.tab_dashboard
        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)

        left = ctk.CTkFrame(frame)
        left.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)

        ctk.CTkLabel(left, text="Runtime Controls", font=("Segoe UI", 16, "bold")).pack(pady=12)

        self.btn_start = ctk.CTkButton(left, text="AVVIA BOT", command=self._runtime_start, height=40, fg_color="#2fa26b")
        self.btn_start.pack(fill=tk.X, padx=20, pady=8)

        self.btn_stop = ctk.CTkButton(left, text="STOP BOT", command=self._runtime_stop, height=40, fg_color="#d9534f")
        self.btn_stop.pack(fill=tk.X, padx=20, pady=8)

        self.btn_pause = ctk.CTkButton(left, text="PAUSA", command=self._runtime_pause, height=40)
        self.btn_pause.pack(fill=tk.X, padx=20, pady=8)

        self.btn_resume = ctk.CTkButton(left, text="RIPRENDI", command=self._runtime_resume, height=40)
        self.btn_resume.pack(fill=tk.X, padx=20, pady=8)

        self.btn_reset = ctk.CTkButton(left, text="RESET CICLO", command=self._runtime_reset, height=40)
        self.btn_reset.pack(fill=tk.X, padx=20, pady=8)

        self.btn_emergency = ctk.CTkButton(
            left,
            text="EMERGENCY STOP",
            command=self._runtime_emergency_stop,
            height=50,
            fg_color="#000000",
            hover_color="#333333",
            text_color="#ff0000",
            font=("Segoe UI", 14, "bold"),
        )
        self.btn_emergency.pack(fill=tk.X, padx=20, pady=(20, 8))

        right = ctk.CTkFrame(frame)
        right.grid(row=0, column=1, sticky="nsew", padx=12, pady=12)

        ctk.CTkLabel(right, text="System Status", font=("Segoe UI", 16, "bold")).pack(pady=12)

        status_grid = ctk.CTkFrame(right, fg_color="transparent")
        status_grid.pack(fill=tk.BOTH, expand=True, padx=20)

        labels = [
            ("Status:", self.status_mode_var),
            ("Broker:", self.status_broker_var),
            ("Betfair:", self.status_betfair_var),
            ("Telegram:", self.status_telegram_var),
            ("Bankroll:", self.status_bankroll_var),
            ("Drawdown:", self.status_drawdown_var),
            ("Exposure:", self.status_exposure_var),
            ("Active Tables:", self.status_tables_var),
            ("Last Signal:", self.status_last_signal_var),
        ]

        for i, (txt, var) in enumerate(labels):
            ctk.CTkLabel(status_grid, text=txt, anchor="w").grid(row=i, column=0, sticky="w", pady=4)
            ctk.CTkLabel(status_grid, textvariable=var, font=("Segoe UI", 12, "bold"), anchor="e").grid(row=i, column=1, sticky="e", pady=4)

        cp = ctk.CTkFrame(right, fg_color="transparent", border_width=1, border_color="#555")
        cp.pack(fill=tk.X, padx=20, pady=12)
        ctk.CTkLabel(cp, text="Live Control Plane", font=("Segoe UI", 12, "bold")).pack(pady=4)

        self.live_readiness_label = ctk.CTkLabel(cp, textvariable=self.live_readiness_level_var, font=("Segoe UI", 14, "bold"))
        self.live_readiness_label.pack()
        ctk.CTkLabel(cp, textvariable=self.live_readiness_blockers_var, wraplength=300).pack(pady=2)

        self.live_effective_status_label = ctk.CTkLabel(cp, textvariable=self.live_effective_status_var, font=("Segoe UI", 16, "bold"))
        self.live_effective_status_label.pack(pady=6)

        ctk.CTkLabel(cp, textvariable=self.live_control_state_var).pack()
        ctk.CTkLabel(cp, textvariable=self.live_last_decision_var, font=("Segoe UI", 10)).pack()
        ctk.CTkLabel(cp, textvariable=self.live_last_reason_var, font=("Segoe UI", 10)).pack()

        err_frame = ctk.CTkFrame(right, fg_color="transparent")
        err_frame.pack(fill=tk.X, padx=20, pady=(10, 0))
        ctk.CTkLabel(err_frame, text="Last Error/Info:").pack(anchor="w")
        ctk.CTkLabel(err_frame, textvariable=self.status_last_error_var, text_color="#d9534f", wraplength=350).pack(anchor="w")

    def _build_settings_tab(self):
        if self._test_mode:
            self.btn_save_bf = _DummyButton(self._save_betfair_settings)
            return

        frame = self.tab_settings
        scroll = ctk.CTkScrollableFrame(frame)
        scroll.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(scroll, text="Betfair Credentials", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=12, pady=8)

        self._labeled_entry(scroll, "Username", self.bf_username_var)
        self._labeled_entry(scroll, "Password", self.bf_password_var, show="*")
        self._labeled_entry(scroll, "App Key", self.bf_app_key_var)
        self._labeled_entry(scroll, "Certificate Path", self.bf_cert_var)
        self._labeled_entry(scroll, "Key Path", self.bf_key_var)

        self.btn_save_bf = ctk.CTkButton(scroll, text="Salva Betfair", command=self._save_betfair_settings)
        self.btn_save_bf.pack(anchor="w", padx=12, pady=12)

    def _build_telegram_tab(self):
        self.telegram_ui = TelegramTabUI(self.tab_telegram, self)

    def _build_roserpina_tab(self):
        if self._test_mode:
            self.btn_save_roserpina = _DummyButton(self._save_roserpina_settings)
            return

        frame = self.tab_roserpina
        outer = ctk.CTkScrollableFrame(frame)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(outer, text="Roserpina Cycle Configuration", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=12, pady=8)

        self._labeled_entry(outer, "Target Profit Cycle %", self.rs_target_var)
        self._labeled_entry(outer, "Max Single Bet %", self.rs_max_single_var)
        self._labeled_entry(outer, "Max Total Exposure %", self.rs_max_total_var)
        self._labeled_entry(outer, "Max Event Exposure %", self.rs_max_event_var)
        self._labeled_entry(outer, "Auto Reset Drawdown %", self.rs_auto_reset_var)
        self._labeled_entry(outer, "Defense Drawdown %", self.rs_defense_var)
        self._labeled_entry(outer, "Lockdown Drawdown %", self.rs_lockdown_var)
        self._labeled_entry(outer, "Expansion Profit %", self.rs_expansion_profit_var)
        self._labeled_entry(outer, "Expansion Multiplier", self.rs_expansion_mult_var)
        self._labeled_entry(outer, "Defense Multiplier", self.rs_defense_mult_var)
        self._labeled_entry(outer, "Table Count", self.rs_table_count_var)
        self._labeled_entry(outer, "Max Recovery Tables", self.rs_recovery_tables_var)
        self._labeled_entry(outer, "Commission %", self.rs_commission_var)
        self._labeled_entry(outer, "Min Stake", self.rs_min_stake_var)
        self._labeled_entry(outer, "Max Stake Assoluto", self.rs_max_abs_var)
        self._labeled_entry(outer, "Hard-stop: Perdita Giornaliera Max (€, vuoto=non impostato)", self.rs_max_daily_loss_var)
        self._labeled_entry(outer, "Hard-stop: Esposizione Aperta Max (€, vuoto=non impostato)", self.rs_max_open_exposure_var)
        self._labeled_entry(outer, "Hard-stop: Drawdown Max % (0-100, vuoto=non impostato)", self.rs_max_drawdown_hard_stop_var)
        self._labeled_entry(outer, "Book Warning % (avviso over-round)", self.rs_book_warning_var)
        self._labeled_entry(outer, "Book Block % (blocca submit se book >= soglia)", self.rs_book_block_var)
        self._labeled_entry(outer, "Liquidity avviso: Moltiplicatore (richiesta = stake x N)", self.rs_liq_multiplier_var)
        self._labeled_entry(outer, "Liquidity avviso: Floor assoluto € (0 = nessun floor)", self.rs_liq_min_abs_var)
        self._labeled_entry(outer, "Quota minima / Min Price (>= 1.02)", self.rs_min_price_var)
        self._labeled_entry(outer, "Max Win € (cap vincita/payout per gamba)", self.rs_max_win_var)
        self._labeled_entry(outer, "Max Stake % (avviso se esposizione operazione > % balance)", self.rs_max_stake_pct_var)

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
        ctk.CTkCheckBox(cb, text="Liquidity Guard attivo", variable=self.rs_liq_guard_enabled_var).pack(side=tk.LEFT, padx=8, pady=8)
        # #383: il guard puo' BLOCCARE il submit se la liquidita' eseguibile e'
        # insufficiente. "Solo avviso" (default opt-in) lo tiene in osservazione;
        # TOGLIERE la spunta arma il BLOCCO reale (money-management).
        ctk.CTkCheckBox(cb, text="Liquidity: solo avviso (⚠ togliere = BLOCCA il submit)", variable=self.rs_liq_warning_only_var).pack(side=tk.LEFT, padx=8, pady=8)
        # G5: il cap Max Win puo' BLOCCARE il submit (dutching + bet manuale) se la
        # vincita/payout potenziale di una gamba supera la soglia. "Solo avviso"
        # (default opt-in) lo tiene in osservazione; TOGLIERE la spunta arma il BLOCCO.
        ctk.CTkCheckBox(cb, text="Max Win: solo avviso (⚠ togliere = BLOCCA il submit)", variable=self.rs_max_win_warning_only_var).pack(side=tk.LEFT, padx=8, pady=8)

        self.btn_save_roserpina = ctk.CTkButton(
            outer,
            text="Salva Roserpina",
            command=self._save_roserpina_settings,
        )
        self.btn_save_roserpina.pack(anchor="w", padx=12, pady=12)

    def _build_simulation_tab(self):
        if self._test_mode:
            self.btn_save_simulation = _DummyButton(self._save_simulation_settings)
            return

        outer = ctk.CTkScrollableFrame(self.tab_simulazione)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(outer, text="Configurazione Simulazione", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=12, pady=8)
        ctk.CTkLabel(
            outer,
            text=(
                "Parametri del broker simulato (attivi solo in modalita' SIMULAZIONE). "
                "La commissione simulata e' bloccata al 4.5% (policy Betfair Italia) e "
                "non e' editabile qui."
            ),
            wraplength=560,
            justify="left",
            anchor="w",
        ).pack(anchor="w", padx=12, pady=(0, 8))

        self._labeled_entry(outer, "Bankroll iniziale simulazione (€)", self.sim_starting_balance_var)

        cb = ctk.CTkFrame(outer)
        cb.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkCheckBox(cb, text="Partial fill abilitato", variable=self.sim_partial_fill_var).pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkCheckBox(cb, text="Consuma liquidita' del book", variable=self.sim_consume_liq_var).pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkCheckBox(cb, text="Persisti stato simulazione", variable=self.sim_persist_state_var).pack(side=tk.LEFT, padx=8, pady=8)

        self.btn_save_simulation = ctk.CTkButton(
            outer,
            text="Salva Simulazione",
            command=self._save_simulation_settings,
        )
        self.btn_save_simulation.pack(anchor="w", padx=12, pady=12)

    def _save_simulation_settings(self):
        if not hasattr(self.settings_service, "save_simulation_config"):
            self._safe_show_error("Errore", "Servizio simulazione non disponibile.")
            return
        try:
            balance = self._parse_sim_balance(self.sim_starting_balance_var.get(), "Bankroll iniziale")
            # FAIL-CLOSED: per preservare i campi NON esposti (commission_pct
            # policy-lock 4.5, simulation.enabled) dobbiamo poter rileggere la
            # config corrente. Se il reload e' assente o fallisce, ABORTIAMO il
            # salvataggio: mai degradare a {} sovrascrivendo i campi protetti con i
            # default (un read error transitorio potrebbe rimettere enabled=True).
            if not hasattr(self.settings_service, "load_simulation_config"):
                raise RuntimeError(
                    "Config simulazione non ricaricabile: salvataggio annullato per "
                    "non sovrascrivere i campi protetti (commission_pct, enabled)."
                )
            loaded = self.settings_service.load_simulation_config()
            # FAIL-CLOSED completo: il reload deve essere un dict che contiene
            # ESPLICITAMENTE i campi protetti (commission_pct policy-lock 4.5,
            # enabled). None / non-dict / vuoto / PARZIALE => non possiamo
            # preservarli, quindi ABORTIAMO (mai degradare a un dict che li
            # riscriverebbe coi default via save_simulation_config.get(...)).
            _locked_keys = ("commission_pct", "enabled")
            if not isinstance(loaded, dict) or any(k not in loaded for k in _locked_keys):
                raise RuntimeError(
                    "Config simulazione corrente incompleta o non valida (campi "
                    "protetti commission_pct/enabled assenti): salvataggio annullato "
                    "per non sovrascriverli con i default."
                )
            cfg = dict(loaded)
            cfg["starting_balance"] = balance
            cfg["partial_fill_enabled"] = bool(self.sim_partial_fill_var.get())
            cfg["consume_liquidity"] = bool(self.sim_consume_liq_var.get())
            cfg["persist_state"] = bool(self.sim_persist_state_var.get())
            self.settings_service.save_simulation_config(cfg)
        except Exception as exc:
            self._safe_show_error("Errore salvataggio Simulazione", str(exc))
            return
        self._safe_show_info("OK", "Configurazione simulazione salvata.")

    def _build_watchdog_tab(self):
        if self._test_mode:
            self.btn_save_watchdog = _DummyButton(self._save_watchdog_settings)
            return

        outer = ctk.CTkScrollableFrame(self.tab_watchdog)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(outer, text="Watchdog Anomalie", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=12, pady=8)
        ctk.CTkLabel(
            outer,
            text=(
                "Toggle LIVE del watchdog anomalie: l'effetto si applica entro ~5s "
                "al tick successivo, senza riavvio. Il watchdog e' ATTIVO di default; "
                "disattivarlo spegne una guardia di safety (viene registrato un "
                "incidente ad ogni tick)."
            ),
            wraplength=560,
            justify="left",
            anchor="w",
        ).pack(anchor="w", padx=12, pady=(0, 8))

        cb = ctk.CTkFrame(outer)
        cb.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkCheckBox(
            cb,
            text="Watchdog anomalie ATTIVO  (⚠ disattivarlo spegne una guardia di safety)",
            variable=self.anomaly_enabled_var,
        ).pack(anchor="w", padx=8, pady=6)
        ctk.CTkCheckBox(
            cb,
            text="Notifiche Telegram delle anomalie",
            variable=self.anomaly_alerts_enabled_var,
        ).pack(anchor="w", padx=8, pady=6)
        ctk.CTkCheckBox(
            cb,
            text="Azioni automatiche su anomalie  (⚠ consente al bot azioni automatiche)",
            variable=self.anomaly_actions_enabled_var,
        ).pack(anchor="w", padx=8, pady=6)

        self.btn_save_watchdog = ctk.CTkButton(
            outer,
            text="Salva Watchdog",
            command=self._save_watchdog_settings,
        )
        self.btn_save_watchdog.pack(anchor="w", padx=12, pady=12)

    def _save_watchdog_settings(self):
        svc = self.settings_service
        if not hasattr(svc, "save_settings"):
            self._safe_show_error("Errore", "Servizio watchdog non disponibile.")
            return
        try:
            # Scrittura ATOMICA dei tre toggle in UN solo save_settings: evita stati
            # parziali del watchdog (GUI vs DB) se una scrittura fallisse a meta'.
            # La coercizione int(bool(...)) rispecchia i metodi save_anomaly_* del
            # service e le chiavi lette da load_anomaly_*. NB: salvando
            # anomaly_enabled si scrive un booleano ESPLICITO, che chiude lo stato
            # tri-state "non configurato = default-ON" (True e None restano entrambi
            # = watchdog attivo; solo togliere la spunta => disattiva).
            svc.save_settings({
                "anomaly_enabled": int(bool(self.anomaly_enabled_var.get())),
                "anomaly_alerts_enabled": int(bool(self.anomaly_alerts_enabled_var.get())),
                "anomaly_actions_enabled": int(bool(self.anomaly_actions_enabled_var.get())),
            })
        except Exception as exc:
            self._safe_show_error("Errore salvataggio Watchdog", str(exc))
            return
        self._safe_show_info("OK", "Configurazione watchdog salvata (effetto entro ~5s).")

    _ALERT_SEVERITIES = ("INFO", "WARNING", "ERROR", "HIGH", "CRITICAL")

    def _build_alert_tab(self):
        if self._test_mode:
            self.btn_save_alert = _DummyButton(self._save_alert_settings)
            return

        outer = ctk.CTkScrollableFrame(self.tab_alert)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(outer, text="Alert Telegram", font=("Segoe UI", 14, "bold")).pack(anchor="w", padx=12, pady=8)
        ctk.CTkLabel(
            outer,
            text=(
                "Routing e soglie delle notifiche di alert su Telegram (distinte "
                "dalle credenziali di sessione nel tab Telegram). Effetto LIVE, "
                "senza riavvio. Disattivarli sopprime anche gli alert di anomalia/"
                "incidente su cui l'operatore fa affidamento."
            ),
            wraplength=560,
            justify="left",
            anchor="w",
        ).pack(anchor="w", padx=12, pady=(0, 8))

        cb = ctk.CTkFrame(outer)
        cb.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkCheckBox(
            cb,
            text="Alert Telegram ATTIVI  (⚠ disattivarli sopprime le notifiche di safety)",
            variable=self.alert_enabled_var,
        ).pack(anchor="w", padx=8, pady=6)
        ctk.CTkCheckBox(cb, text="Deduplica alert ripetuti", variable=self.alert_dedup_var).pack(anchor="w", padx=8, pady=6)
        ctk.CTkCheckBox(cb, text="Formato ricco (dettagli renderizzati)", variable=self.alert_format_rich_var).pack(anchor="w", padx=8, pady=6)

        self._labeled_entry(outer, "Chat ID destinazione (obbligatorio se alert attivi)", self.alert_chat_id_var)
        self._labeled_entry(outer, "Nome chat (facoltativo)", self.alert_chat_name_var)
        self._labeled_entry(outer, "Cooldown anti-spam (secondi, >= 0)", self.alert_cooldown_var)

        sev = ctk.CTkFrame(outer)
        sev.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(sev, text="Severita' minima", width=220, anchor="w").pack(side=tk.LEFT, padx=8, pady=8)
        ctk.CTkComboBox(
            sev,
            variable=self.alert_min_severity_var,
            values=list(self._ALERT_SEVERITIES),
            width=220,
        ).pack(side=tk.LEFT, padx=8, pady=8)

        self.btn_save_alert = ctk.CTkButton(
            outer,
            text="Salva Alert",
            command=self._save_alert_settings,
        )
        self.btn_save_alert.pack(anchor="w", padx=12, pady=12)

    def _save_alert_settings(self):
        svc = self.settings_service
        if not hasattr(svc, "save_telegram_alert_settings"):
            self._safe_show_error("Errore", "Servizio alert non disponibile.")
            return
        try:
            enabled = bool(self.alert_enabled_var.get())
            severity = str(self.alert_min_severity_var.get() or "").strip().upper()
            if severity not in self._ALERT_SEVERITIES:
                raise ValueError(
                    "Severita' minima non valida: usare uno tra "
                    + ", ".join(self._ALERT_SEVERITIES) + "."
                )
            cooldown = self._parse_alert_cooldown(self.alert_cooldown_var.get(), "Cooldown")
            chat_id = str(self.alert_chat_id_var.get() or "").strip()
            # Fail-closed operativo: alert attivi senza destinazione => il service
            # farebbe no-op silenzioso (alerts_chat_id_missing). Blocchiamo il save.
            if enabled and not chat_id:
                raise ValueError("Chat ID obbligatorio quando gli alert sono attivi.")
            svc.save_telegram_alert_settings(
                alerts_enabled=enabled,
                alerts_chat_id=chat_id,
                alerts_chat_name=str(self.alert_chat_name_var.get() or "").strip(),
                min_alert_severity=severity,
                alert_cooldown_sec=cooldown,
                alert_dedup_enabled=bool(self.alert_dedup_var.get()),
                alert_format_rich=bool(self.alert_format_rich_var.get()),
            )
        except Exception as exc:
            self._safe_show_error("Errore salvataggio Alert", str(exc))
            return
        self._safe_show_info("OK", "Configurazione alert salvata (effetto immediato).")

    @staticmethod
    def _parse_alert_cooldown(raw, label):
        """Cooldown alert: intero >= 0.

        Usa int() diretto (NON int(float())): i valori frazionari come "0.5"/"1.9"
        verrebbero troncati silenziosamente a 0/1, accorciando il cooldown anti-spam
        mentre il salvataggio riporta successo. Cosi' vengono rifiutati (come "inf").
        """
        text = (raw or "").strip()
        try:
            val = int(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero intero valido.")
        if val < 0:
            raise ValueError(f"{label}: deve essere >= 0.")
        return val

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
            text="Forza refresh Risk Desk",
            command=lambda: self._on_refresh_event("risk_desk", {}),
        )
        self.btn_refresh_risk.pack(side=tk.LEFT, padx=6)

    def _build_bet_history_tab(self):
        if self._test_mode:
            self.bet_history_tree = _DummyTree()
            self.btn_refresh_bet_history = _DummyButton(self._refresh_bet_history_data)
            return

        frame = self.tab_bet_history
        try:
            frame.grid_rowconfigure(0, weight=1)
            frame.grid_columnconfigure(0, weight=1)
        except Exception:
            pass

        self.bet_history_tree = ttk.Treeview(
            frame,
            columns=("id", "event", "market", "selection", "stake", "price", "status", "outcome", "profit", "timestamp"),
            show="headings",
            height=18,
        )
        for col, text, width in [
            ("id", "ID Bet", 100),
            ("event", "Evento", 200),
            ("market", "Mercato", 150),
            ("selection", "Selezione", 150),
            ("stake", "Stake", 80),
            ("price", "Quota", 80),
            ("status", "Stato", 100),
            ("outcome", "Esito", 100),
            ("profit", "Profitto", 100),
            ("timestamp", "Data/Ora", 150),
        ]:
            self.bet_history_tree.heading(col, text=text)
            self.bet_history_tree.column(col, width=width, anchor="w")
        self.bet_history_tree.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)

        btns = ctk.CTkFrame(frame, fg_color="transparent")
        btns.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        self.btn_refresh_bet_history = ctk.CTkButton(
            btns,
            text="Forza refresh Storico Bet",
            command=lambda: self._on_refresh_event("bet_history", {}),
        )
        self.btn_refresh_bet_history.pack(side=tk.LEFT, padx=6)

    def _build_risk_desk_history_tab(self):
        if self._test_mode:
            self.risk_desk_history_tree = _DummyTree()
            self.btn_refresh_risk_desk_history = _DummyButton(self._refresh_risk_desk_history_data)
            return

        frame = self.tab_risk_history
        try:
            frame.grid_rowconfigure(0, weight=1)
            frame.grid_columnconfigure(0, weight=1)
        except Exception:
            pass

        self.risk_desk_history_tree = ttk.Treeview(
            frame,
            columns=("id", "timestamp", "status", "loss", "exposure", "event", "market", "selection"),
            show="headings",
            height=18,
        )
        for col, text, width in [
            ("id", "ID", 70),
            ("timestamp", "Data/Ora", 150),
            ("status", "Stato", 120),
            ("loss", "Loss", 100),
            ("exposure", "Exposure", 100),
            ("event", "Evento", 200),
            ("market", "Mercato", 150),
            ("selection", "Selezione", 150),
        ]:
            self.risk_desk_history_tree.heading(col, text=text)
            self.risk_desk_history_tree.column(col, width=width, anchor="w")
        self.risk_desk_history_tree.grid(row=0, column=0, sticky="nsew", padx=12, pady=12)

        btns = ctk.CTkFrame(frame, fg_color="transparent")
        btns.grid(row=1, column=0, sticky="ew", padx=12, pady=(0, 12))
        self.btn_refresh_risk_desk_history = ctk.CTkButton(
            btns,
            text="Forza refresh Storico Risk Desk",
            command=lambda: self._on_refresh_event("risk_desk_history", {}),
        )
        self.btn_refresh_risk_desk_history.pack(side=tk.LEFT, padx=6)

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
                self.rs_max_daily_loss_var.set(self._hard_stop_to_str(getattr(rs, "max_daily_loss", None)))
                self.rs_max_open_exposure_var.set(self._hard_stop_to_str(getattr(rs, "max_open_exposure", None)))
                self.rs_max_drawdown_hard_stop_var.set(self._hard_stop_to_str(getattr(rs, "max_drawdown_hard_stop_pct", None)))
                self.rs_book_warning_var.set(str(getattr(rs, "book_warning", trading_config.BOOK_WARNING)))
                self.rs_book_block_var.set(str(getattr(rs, "book_block", trading_config.BOOK_BLOCK)))
                self.rs_liq_multiplier_var.set(str(getattr(rs, "liquidity_multiplier", trading_config.LIQUIDITY_MULTIPLIER)))
                self.rs_liq_min_abs_var.set(str(getattr(rs, "min_liquidity_absolute", trading_config.MIN_LIQUIDITY_ABSOLUTE)))
                self.rs_liq_guard_enabled_var.set(bool(getattr(rs, "liquidity_guard_enabled", True)))
                self.rs_liq_warning_only_var.set(bool(getattr(rs, "liquidity_warning_only", True)))
                self.rs_min_price_var.set(str(getattr(rs, "min_price", trading_config.MIN_PRICE)))
                self.rs_max_win_var.set(str(getattr(rs, "max_win", trading_config.MAX_WIN)))
                self.rs_max_win_warning_only_var.set(bool(getattr(rs, "max_win_warning_only", True)))
                self.rs_max_stake_pct_var.set(str(getattr(rs, "max_stake_pct", trading_config.MAX_STAKE_PCT * 100)))
                self.rs_allow_recovery_var.set(bool(getattr(rs, "allow_recovery", self.rs_allow_recovery_var.get())))
                self.rs_anti_dup_var.set(bool(getattr(rs, "anti_duplication_enabled", self.rs_anti_dup_var.get())))
                risk_profile = getattr(rs, "risk_profile", None)
                risk_profile_value = getattr(risk_profile, "value", self.rs_risk_profile_var.get())
                self.rs_risk_profile_var.set(str(risk_profile_value))
        except Exception:
            pass

        try:
            if hasattr(self.settings_service, "load_simulation_config"):
                sim = self.settings_service.load_simulation_config() or {}
                self.sim_starting_balance_var.set(str(sim.get("starting_balance", 1000.0)))
                self.sim_partial_fill_var.set(bool(sim.get("partial_fill_enabled", True)))
                self.sim_consume_liq_var.set(bool(sim.get("consume_liquidity", True)))
                self.sim_persist_state_var.set(bool(sim.get("persist_state", True)))
        except Exception:
            pass

        try:
            svc = self.settings_service
            if hasattr(svc, "load_anomaly_enabled"):
                # None (non configurato) => watchdog default-ON => checkbox spuntata.
                val = svc.load_anomaly_enabled()
                self.anomaly_enabled_var.set(val is None or bool(val))
            if hasattr(svc, "load_anomaly_alerts_enabled"):
                self.anomaly_alerts_enabled_var.set(bool(svc.load_anomaly_alerts_enabled()))
            if hasattr(svc, "load_anomaly_actions_enabled"):
                self.anomaly_actions_enabled_var.set(bool(svc.load_anomaly_actions_enabled()))
        except Exception:
            pass

        try:
            svc = self.settings_service
            if hasattr(svc, "load_telegram_config_row"):
                row = svc.load_telegram_config_row() or {}
                self.alert_enabled_var.set(bool(row.get("alerts_enabled", False)))
                self.alert_chat_id_var.set(str(row.get("alerts_chat_id") or ""))
                self.alert_chat_name_var.set(str(row.get("alerts_chat_name", "") or ""))
                self.alert_min_severity_var.set(str(row.get("min_alert_severity", "WARNING") or "WARNING").upper())
                self.alert_cooldown_var.set(str(row.get("alert_cooldown_sec", 300)))
                self.alert_dedup_var.set(bool(row.get("alert_dedup_enabled", True)))
                self.alert_format_rich_var.set(bool(row.get("alert_format_rich", True)))
        except Exception:
            pass

        self._load_execution_control_settings(force_simulation=self._force_simulation_startup)

    def _load_execution_control_settings(self, force_simulation: bool = False):
        settings = {
            "execution_mode": "SIMULATION",
            "live_enabled": False,
            "kill_switch": False,
        }
        if hasattr(self.settings_service, "load_execution_settings"):
            try:
                loaded = self.settings_service.load_execution_settings() or {}
                if isinstance(loaded, dict):
                    settings.update(loaded)
            except Exception:
                pass

        execution_mode = str(settings.get("execution_mode", "SIMULATION") or "SIMULATION").strip().upper()
        if execution_mode not in {"SIMULATION", "LIVE"}:
            execution_mode = "SIMULATION"
        live_enabled = bool(settings.get("live_enabled", False))
        kill_switch = bool(settings.get("kill_switch", False))

        self.kill_switch_var.set(kill_switch)
        # #355 fail-closed from construction: se richiesto, forza SIMULATION
        # PRIMA di applicare lo stato persistito. Cosi' un `execution_mode=LIVE`
        # salvato non viene mai sincronizzato (nessuna finestra LIVE transitoria
        # in `__init__`, prima che l'entry point possa forzare SIMULATION). Il
        # kill_switch persistito resta invariato (forzare SIM non lo azzera).
        if force_simulation:
            self._force_simulation_state()
            return

        self.execution_mode_var.set(execution_mode)
        self.live_enabled_var.set(live_enabled)
        self._sync_execution_controls_to_runtime()
        self._refresh_live_control_plane_status({})

    def _force_simulation_state(self) -> None:
        """Porta la sessione a SIMULATION / live-disabled e riallinea sia il
        runtime sia gli status derivati del control-plane (il pannello non deve
        mostrare uno stato LIVE residuo). Direzione sempre verso SIMULATION,
        quindi fail-closed by construction; NON persiste (le preferenze salvate
        su disco restano intatte). Sorgente unica condivisa dal ramo `force`
        del load e da `force_simulation_startup` (no duplicazione/drift)."""
        self.execution_mode_var.set("SIMULATION")
        self.live_enabled_var.set(False)
        self._sync_execution_controls_to_runtime()
        self._refresh_live_control_plane_status({})

    def force_simulation_startup(self) -> None:
        """Forza a SIMULATION a runtime a prescindere dallo stato corrente.

        Un avvio via `python main.py` (o `run_gui()`) NON deve mai partire in
        LIVE: l'entry point lo garantisce gia' costruendo con
        `force_simulation=True` (fail-closed dalla costruzione). Questo metodo
        resta come forzatura runtime esplicita, con la stessa semantica: agisce
        SOLO sulla sessione (non persiste), sempre verso SIMULATION; il gate
        #350 resta la difesa runtime per ogni successiva transizione a LIVE.
        """
        self._force_simulation_state()

    def _save_execution_control_settings(self):
        if not hasattr(self.settings_service, "save_execution_settings"):
            return
        try:
            self.settings_service.save_execution_settings(
                execution_mode=self.execution_mode_var.get(),
                live_enabled=bool(self.live_enabled_var.get()),
                kill_switch=bool(self.kill_switch_var.get()),
            )
        except TypeError:
            self.settings_service.save_execution_settings(
                execution_mode=self.execution_mode_var.get(),
                live_enabled=bool(self.live_enabled_var.get()),
            )
        except Exception:
            pass

    def _sync_execution_controls_to_runtime(self):
        execution_mode = str(self.execution_mode_var.get() or "SIMULATION").strip().upper()
        if execution_mode not in {"SIMULATION", "LIVE"}:
            execution_mode = "SIMULATION"
        is_simulation = execution_mode != "LIVE"
        self.simulation_mode_var.set(is_simulation)
        self._apply_simulation_mode_to_runtime()
        # Rileggi la var DOPO la sync: su fallimento _apply fa rollback allo
        # stato confermato (#350) e le label devono mostrare quello, mai un
        # intento non confermato.
        is_simulation = bool(self.simulation_mode_var.get())
        self.sim_label_var.set("SIMULAZIONE" if is_simulation else "LIVE")
        self.status_broker_var.set("SIMULATION" if is_simulation else "LIVE")

    def _on_execution_mode_changed(self, choice=None):
        if choice is not None:
            self.execution_mode_var.set(str(choice))
        self._sync_execution_controls_to_runtime()
        self._apply_execution_control_changes()

    def _on_live_gate_toggled(self):
        self._apply_execution_control_changes()

    def _on_kill_switch_toggled(self):
        self._apply_execution_control_changes()

    def _apply_execution_control_changes(self):
        self._save_execution_control_settings()
        self._refresh_runtime_status()

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

    @staticmethod
    def _hard_stop_to_str(value):
        """Valore hard-stop -> stringa per la GUI: None => campo vuoto."""
        if value is None:
            return ""
        return str(value)

    @staticmethod
    def _parse_hard_stop(raw, label, *, is_pct=False):
        """Parsa un campo hard-stop dalla GUI in modo FAIL-CLOSED.

        - vuoto => None: il save preserva il valore persistito (non azzera un
          limite di sicurezza gia' configurato) e il gate LIVE resta bloccante
          se il campo non e' impostato;
        - valore presente: DEVE essere numerico, finito e > 0 (per la % anche
          <= 100), coerente con `_validate_live_hard_stop_config` del deploy
          gate (core/runtime_controller). Altrimenti solleva ValueError: il
          salvataggio si interrompe con errore e nessun valore fasullo viene
          scritto (niente 0/negativi che aggirerebbero il gate).
        """
        text = (raw or "").strip()
        if not text:
            return None
        try:
            parsed = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido (oppure lascia vuoto).")
        if not math.isfinite(parsed) or parsed <= 0.0:
            raise ValueError(f"{label}: deve essere un numero finito maggiore di 0.")
        if is_pct and parsed > 100.0:
            raise ValueError(f"{label}: la percentuale non puo' superare 100.")
        return parsed

    @staticmethod
    def _parse_book_pct(raw, label):
        """Parsa una soglia book% dalla GUI: numerica, FINITA e > 0.

        A differenza degli hard-stop, il campo book NON e' opzionale (ha sempre
        un default): un valore vuoto / non numerico / nan / inf / <= 0 e' un
        errore ESPLICITO che interrompe il salvataggio con messaggio chiaro, cosi'
        non si persiste un valore incoerente col gate (che, lato runtime, lo
        scarterebbe col fallback, mostrando pero' in GUI una soglia diversa da
        quella realmente applicata — drift).
        """
        text = (raw or "").strip()
        try:
            val = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido.")
        if not math.isfinite(val) or val <= 0.0:
            raise ValueError(f"{label}: deve essere un numero finito maggiore di 0.")
        return val

    @staticmethod
    def _parse_min_liquidity(raw, label):
        """Floor liquidita' assoluto: numerico, finito, >= 0 (0 = nessun floor).

        Diverso da `_parse_book_pct` (> 0): per il floor liquidita' lo 0 e' un
        valore valido (nessun minimo assoluto).
        """
        text = (raw or "").strip()
        try:
            val = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido.")
        if not math.isfinite(val) or val < 0.0:
            raise ValueError(f"{label}: deve essere un numero finito >= 0.")
        return val

    @staticmethod
    def _parse_min_price(raw, label):
        """Quota minima: numerica, finita, >= 1.02.

        Il minimo realmente raggiungibile e' 1.02: il floor hard del controller
        rifiuta `price <= 1.01` (1.01 e' il minimo Betfair, mai piazzabile qui),
        quindi impostare 1.01 sarebbe identico a 1.02 e fuorviante. La GUI accetta
        quindi solo `>= 1.02`.
        """
        text = (raw or "").strip()
        try:
            val = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido.")
        if not math.isfinite(val) or val < 1.02:
            raise ValueError(f"{label}: deve essere un numero finito >= 1.02.")
        return val

    @staticmethod
    def _parse_max_win(raw, label):
        """Cap Max Win: numerico, finito, > 0 (il cap non e' opzionale, ha default).

        Come `_parse_book_pct`: un valore vuoto / non numerico / nan / inf / <= 0 e'
        un errore ESPLICITO che interrompe il salvataggio, cosi' non si persiste un
        cap incoerente col gate (che a runtime lo scarterebbe col fallback fail-safe,
        mostrando in GUI un valore diverso da quello applicato — drift).
        """
        text = (raw or "").strip()
        try:
            val = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido.")
        if not math.isfinite(val) or val <= 0.0:
            raise ValueError(f"{label}: deve essere un numero finito maggiore di 0.")
        return val

    @staticmethod
    def _parse_sim_balance(raw, label):
        """Bankroll simulazione: numerico, finito, > 0."""
        text = (raw or "").strip()
        try:
            val = float(text)
        except (TypeError, ValueError):
            raise ValueError(f"{label}: inserisci un numero valido.")
        if not math.isfinite(val) or val <= 0.0:
            raise ValueError(f"{label}: deve essere un numero finito maggiore di 0.")
        return val

    def _save_roserpina_settings(self):
        try:
            from core.system_state import RoserpinaConfig, RiskProfile

            book_warning = self._parse_book_pct(self.rs_book_warning_var.get(), "Book Warning %")
            book_block = self._parse_book_pct(self.rs_book_block_var.get(), "Book Block %")
            if book_warning > book_block:
                raise ValueError("Book Warning % non puo' superare Book Block %.")
            liq_multiplier = self._parse_book_pct(self.rs_liq_multiplier_var.get(), "Liquidity Moltiplicatore")
            liq_min_abs = self._parse_min_liquidity(self.rs_liq_min_abs_var.get(), "Liquidity Floor assoluto")
            min_price = self._parse_min_price(self.rs_min_price_var.get(), "Quota minima")
            max_win = self._parse_max_win(self.rs_max_win_var.get(), "Max Win")
            max_stake_pct = self._parse_book_pct(self.rs_max_stake_pct_var.get(), "Max Stake %")

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
                max_daily_loss=self._parse_hard_stop(self.rs_max_daily_loss_var.get(), "Perdita giornaliera max"),
                max_open_exposure=self._parse_hard_stop(self.rs_max_open_exposure_var.get(), "Esposizione aperta max"),
                max_drawdown_hard_stop_pct=self._parse_hard_stop(self.rs_max_drawdown_hard_stop_var.get(), "Drawdown max %", is_pct=True),
                book_warning=book_warning,
                book_block=book_block,
                liquidity_guard_enabled=bool(self.rs_liq_guard_enabled_var.get()),
                liquidity_multiplier=liq_multiplier,
                min_liquidity_absolute=liq_min_abs,
                liquidity_warning_only=bool(self.rs_liq_warning_only_var.get()),
                min_price=min_price,
                max_win=max_win,
                max_win_warning_only=bool(self.rs_max_win_warning_only_var.get()),
                max_stake_pct=max_stake_pct,
            )
            self.settings_service.save_roserpina_config(cfg)
        except Exception as exc:
            # Errore nella COSTRUZIONE/validazione del config o nella PERSISTENZA:
            # il salvataggio non e' avvenuto => errore generico, niente reload.
            self._safe_show_error("Errore salvataggio Roserpina", str(exc))
            return

        # Da qui il config e' PERSISTITO (su DB). Reload runtime e refresh dei
        # campi sono passi POST-salvataggio: un loro errore NON deve essere
        # riportato come "salvataggio fallito" (fuorviante: il dato e' gia' su DB)
        # ne' saltare il refresh dei campi. Il reload runtime e' isolato: se
        # fallisce, la config resta persistita e verra' applicata al prossimo
        # reload/riavvio.
        reload_error = None
        if hasattr(self.runtime, "reload_config"):
            try:
                self.runtime.reload_config()
            except Exception as exc:
                reload_error = exc
        # Riallinea i campi hard-stop al valore REALMENTE persistito: un campo
        # lasciato vuoto (semantica preserve) torna a mostrare il limite
        # conservato, evitando una divergenza UI/stato (un limite attivo ma
        # invisibile). Refresh mirato ai soli hard-stop, per non toccare gli
        # altri tab ne' la logica force-simulation di _load_initial_settings.
        self._refresh_hard_stop_vars()
        if reload_error is not None:
            # Divergenza safety-critical: la config e' su DB ma il runtime NON e'
            # stato riallineato. Il messaggio DEVE essere esplicito che i nuovi
            # limiti (hard-stop/drawdown/esposizione) NON sono attivi a runtime —
            # il bot opera ancora con i limiti PRECEDENTI — cosi' l'operatore non
            # crede erroneamente che i nuovi limiti siano gia' in vigore.
            self._safe_show_error(
                "Reload runtime FALLITO — nuovi limiti NON attivi",
                "La configurazione e' stata SALVATA su DB, ma il reload a runtime e' "
                f"fallito ({reload_error}): il bot sta ancora operando con i limiti "
                "PRECEDENTI, non con quelli appena salvati. Riavvia il bot (o ripeti "
                "il salvataggio) per applicare i nuovi limiti prima di operare in LIVE.",
            )
        else:
            self._safe_show_info("OK", "Configurazione Roserpina salvata.")

    def _refresh_hard_stop_vars(self):
        """Rilegge gli hard-stop persistiti e riallinea i campi GUI (post-save).

        Serve a evitare che un campo lasciato vuoto (preserve) mostri "" mentre
        il limite persistito e' ancora attivo: dopo il salvataggio i campi
        riflettono il valore reale letto da `load_roserpina_config`.
        """
        # Best-effort e COMPLETAMENTE isolato: qualsiasi errore (load DB o .set
        # Tk) non deve propagare nel callback di salvataggio ne' mascherare
        # l'esito del save gia' avvenuto (rilievo Fugu Ultra / Fable 5).
        try:
            rs = self.settings_service.load_roserpina_config()
            self.rs_max_daily_loss_var.set(self._hard_stop_to_str(getattr(rs, "max_daily_loss", None)))
            self.rs_max_open_exposure_var.set(self._hard_stop_to_str(getattr(rs, "max_open_exposure", None)))
            self.rs_max_drawdown_hard_stop_var.set(self._hard_stop_to_str(getattr(rs, "max_drawdown_hard_stop_pct", None)))
        except Exception:
            return

    # =========================================================
    # LIVE / SIM
    # =========================================================
    def _toggle_simulation_mode(self):
        self.execution_mode_var.set("SIMULATION" if bool(self.simulation_mode_var.get()) else "LIVE")
        self._sync_execution_controls_to_runtime()
        self._save_execution_control_settings()
        self._log(f"Modalità cambiata: {self.sim_label_var.get()}")
        self._refresh_runtime_status()

    # =========================================================
    # RUNTIME COMMANDS
    # =========================================================
    def _run_runtime_command_async(self, command_name: str, worker_fn):
        submit_fn = getattr(getattr(self, "executor", None), "submit", None)
        inflight_key = str(command_name or "").strip().upper()
        if inflight_key in self._runtime_commands_inflight:
            self._runtime_command_rejected_total += 1
            self._log(f"{command_name} SKIPPED -> command already in-flight")
            return
        self._runtime_commands_inflight.add(inflight_key)

        def _on_success(result):
            try:
                self._log(f"{command_name} -> {result}")
                self._refresh_runtime_status()
            finally:
                self._runtime_commands_inflight.discard(inflight_key)

        def _on_error(exc: Exception):
            try:
                self._log(f"{command_name} ERROR -> {exc}")
                self._safe_show_error(f"Errore {command_name.lower()}", str(exc))
                self._refresh_runtime_status()
            finally:
                self._runtime_commands_inflight.discard(inflight_key)

        if not callable(submit_fn):
            try:
                _on_success(worker_fn())
            except Exception as exc:
                _on_error(exc)
            return

        try:
            future = submit_fn(f"gui_{command_name.lower()}", worker_fn)
        except Exception as exc:
            _on_error(exc)
            return

        if hasattr(future, "add_done_callback"):
            def _done(fut):
                try:
                    result = fut.result()
                    self.uiq.post(_on_success, result)
                except Exception as exc:
                    self.uiq.post(_on_error, exc)

            future.add_done_callback(_done)
            return

        try:
            _on_success(future)
        except Exception as exc:
            _on_error(exc)

    def _runtime_start(self):
        self._sync_execution_controls_to_runtime()
        execution_mode = str(self.execution_mode_var.get() or "SIMULATION").strip().upper()
        if execution_mode not in {"SIMULATION", "LIVE"}:
            execution_mode = "SIMULATION"

        # Guardia fail-closed (#350, decisione owner "applica entrambe" —
        # punto B/Fugu): con l'ultima sync di modalita' NON confermata lo
        # stato reale del runtime e' incerto -> NESSUN avvio (ne' LIVE ne'
        # SIMULATION). Si sblocca solo con una sync riuscita (la
        # _sync_execution_controls_to_runtime a inizio metodo la ritenta:
        # se il runtime e' guarito, _mode_sync_failed si azzera e si parte).
        if getattr(self, "_mode_sync_failed", False):
            self._start_refused_mode_unconfirmed_total += 1
            self._log(f"START {execution_mode} RIFIUTATO -> sync modalita' non confermata dal runtime")
            self._safe_show_error(
                "Avvio bloccato",
                "La sincronizzazione della modalita' col runtime e' fallita: "
                "ripeti il toggle SIM/LIVE con successo prima di avviare.",
            )
            return

        live_enabled = bool(self.live_enabled_var.get())
        live_readiness_ok = False

        def _work():
            return self.runtime.start(
                password=self.bf_password_var.get() or None,
                simulation_mode=self.simulation_mode,
                execution_mode=execution_mode,
                live_enabled=live_enabled and not bool(self.kill_switch_var.get()),
                live_readiness_ok=live_readiness_ok,
            )

        self._run_runtime_command_async("START", _work)

    def _runtime_pause(self):
        self._run_runtime_command_async("PAUSE", self.runtime.pause)

    def _runtime_resume(self):
        self._run_runtime_command_async("RESUME", self.runtime.resume)

    def _runtime_stop(self):
        self._run_runtime_command_async("STOP", self.runtime.stop)

    def _runtime_reset(self):
        self._run_runtime_command_async("RESET", self.runtime.reset_cycle)

    def _runtime_emergency_stop(self):
        self._run_runtime_command_async(
            "EMERGENCY STOP",
            lambda: self.runtime.emergency_stop(reason="operator_gui_button"),
        )

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

    def _wire_refresh_coordinators(self):
        self._refresh_coordinators["risk_desk"] = {
            "events": [
                "QUICK_BET_ACCEPTED",
                "QUICK_BET_FILLED",
                "QUICK_BET_PARTIAL",
                "QUICK_BET_SUCCESS",
                "QUICK_BET_AMBIGUOUS",
                "QUICK_BET_FAILED",
                "RUNTIME_CLOSE_POSITION",
            ],
            "handler": self._refresh_risk_desk_data,
            "inflight": False,
            "pending": False,
        }
        self._refresh_coordinators["bet_history"] = {
            "events": [
                "QUICK_BET_ACCEPTED",
                "QUICK_BET_FILLED",
                "QUICK_BET_PARTIAL",
                "QUICK_BET_SUCCESS",
                "QUICK_BET_AMBIGUOUS",
                "QUICK_BET_FAILED",
                "RUNTIME_CLOSE_POSITION",
            ],
            "handler": self._refresh_bet_history_data,
            "inflight": False,
            "pending": False,
        }
        self._refresh_coordinators["risk_desk_history"] = {
            "events": [
                "RUNTIME_CLOSE_POSITION",
                "RUNTIME_DAILY_LOSS_BREACH",
            ],
            "handler": self._refresh_risk_desk_history_data,
            "inflight": False,
            "pending": False,
        }

        for coordinator_name, coordinator_config in self._refresh_coordinators.items():
            for event_name in coordinator_config["events"]:
                self.bus.subscribe(event_name, lambda payload, cn=coordinator_name: self._on_refresh_event(cn, payload))

    def _on_refresh_event(self, coordinator_name: str, payload: dict):
        coordinator = self._refresh_coordinators.get(coordinator_name)
        if not coordinator:
            return

        self.uiq.post(self._log, f"Refresh event for {coordinator_name} -> {payload}")

        if coordinator["inflight"]:
            coordinator["pending"] = True
            return

        submit_fn = getattr(getattr(self, "executor", None), "submit", None)
        if not callable(submit_fn):
            coordinator["handler"]()
            return

        coordinator["inflight"] = True
        coordinator["pending"] = False

        try:
            future = submit_fn(f"gui_refresh_{coordinator_name}", coordinator["handler"])
        except Exception as exc:
            coordinator["inflight"] = False
            self.uiq.post(self._log, f"Error submitting refresh for {coordinator_name}: {exc}")
            return

        if not hasattr(future, "add_done_callback"):
            coordinator["inflight"] = False
            coordinator["handler"]()
            return

        def _done(fut):
            try:
                fut.result()
            except Exception as exc:
                self.uiq.post(self._log, f"Error in refresh handler for {coordinator_name}: {exc}")

            def _apply():
                coordinator["inflight"] = False
                if coordinator["pending"]:
                    self._on_refresh_event(coordinator_name, {})

            self.uiq.post(_apply)

        future.add_done_callback(_done)

    def _refresh_risk_desk_data(self):
        self.uiq.post(self._refresh_runtime_status)

    def _refresh_bet_history_data(self):
        self.uiq.post(self._log, "Refreshing Bet History data...")
        try:
            bets = self.db.get_recent_simulation_bets(limit=100)
            def _update_ui():
                if not hasattr(self, "bet_history_tree"): return
                for item in self.bet_history_tree.get_children():
                    self.bet_history_tree.delete(item)
                for bet in bets:
                    self.bet_history_tree.insert(
                        "",
                        "end",
                        values=(
                            bet.get("bet_id", "-"),
                            bet.get("event_name", "-"),
                            bet.get("market_name", "-"),
                            bet.get("runner_name", "-"),
                            f"{bet.get('size', 0.0):.2f}",
                            f"{bet.get('price', 0.0):.2f}",
                            bet.get("status", "-"),
                            "-", # Outcome non diretto in simulation_bets
                            "0.00", # Profit non diretto in simulation_bets
                            bet.get("created_at", "-"),
                        ),
                    )
            self.uiq.post(_update_ui)
        except Exception as e:
            self.uiq.post(self._log, f"Error refreshing bet history: {e}")

    def _refresh_risk_desk_history_data(self):
        self.uiq.post(self._log, "Refreshing Risk Desk History data...")
        try:
            history_entries = self.db.get_risk_position_history(limit=100)
            def _update_ui():
                if not hasattr(self, "risk_desk_history_tree"): return
                for item in self.risk_desk_history_tree.get_children():
                    self.risk_desk_history_tree.delete(item)
                for entry in history_entries:
                    self.risk_desk_history_tree.insert(
                        "",
                        "end",
                        values=(
                            entry.get("id", "-"),
                            entry.get("closed_at", entry.get("created_at", "-")),
                            entry.get("outcome", "-"),
                            f"{entry.get('net_pnl', 0.0):.2f}",
                            f"{entry.get('stake', 0.0):.2f}",
                            entry.get("event_key", "-"),
                            entry.get("market_id", "-"),
                            entry.get("selection_id", "-"),
                        ),
                    )
            self.uiq.post(_update_ui)
        except Exception as e:
            self.uiq.post(self._log, f"Error refreshing risk desk history: {e}")

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
    def _build_runtime_status_snapshot(self) -> dict:
        try:
            status = self.runtime.get_status() if hasattr(self.runtime, "get_status") else {}
        except Exception as exc:
            return {"error": exc, "status": {}}

        try:
            betfair_status = self.betfair_service.status() if hasattr(self.betfair_service, "status") else {}
        except Exception:
            betfair_status = {}

        try:
            telegram_status = self.telegram_service.status() if hasattr(self.telegram_service, "status") else {}
        except Exception:
            telegram_status = {}

        return {
            "status": status or {},
            "betfair_status": betfair_status or {},
            "telegram_status": telegram_status or {},
        }

    def _apply_runtime_status_snapshot(self, snapshot: dict):
        snapshot = dict(snapshot or {})
        if snapshot.get("error"):
            self._log(f"STATUS ERROR -> {snapshot.get('error')}")
        status = snapshot.get("status") or {}
        betfair_status = snapshot.get("betfair_status") or {}
        telegram_status = snapshot.get("telegram_status") or {}

        broker_status = status.get("broker_status", {}) or {}
        funds = status.get("account_funds", {}) or {}

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
        self._refresh_live_control_plane_status(status)

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

    def _build_provider_tab(self):
        if self._test_mode: return
        
        container = ctk.CTkFrame(self.tab_provider, fg_color="transparent")
        container.pack(fill="both", expand=True, padx=12, pady=12)
        
        # 1. Top Bar: Catalog Status (Auto-managed)
        top = ctk.CTkFrame(container, fg_color="transparent")
        top.pack(fill="x", pady=(0, 12))
        
        self.lbl_sync_status = ctk.CTkLabel(top, text="Catalogo Betfair: Sincronizzazione Automatica Attiva", font=("", 12, "italic"))
        self.lbl_sync_status.pack(side="left")
        
        self.lbl_sync_info = ctk.CTkLabel(top, text="-")
        self.lbl_sync_info.pack(side="left", padx=12)
        
        # 2. Main Area: Providers & Aliases
        panes = ctk.CTkFrame(container, fg_color="transparent")
        panes.pack(fill="both", expand=True)
        
        # Left: Providers List
        left = ctk.CTkFrame(panes, width=200)
        left.pack(side="left", fill="y", padx=(0, 12))
        
        ctk.CTkLabel(left, text="Provider", font=("", 14, "bold")).pack(pady=6)
        
        self.provider_list = tk.Listbox(left, bg="#2b2b2b", fg="white", borderwidth=0, highlightthickness=0)
        self.provider_list.pack(fill="both", expand=True, padx=6, pady=6)
        
        # Right: Aliases (Tabs)
        right = ctk.CTkFrame(panes)
        right.pack(side="left", fill="both", expand=True)
        
        self.alias_tabs = ctk.CTkTabview(right)
        self.alias_tabs.pack(fill="both", expand=True)
        
        self.tab_names = self.alias_tabs.add("Nomi Eventi")
        self.tab_markets = self.alias_tabs.add("Mercati")
        self.tab_parsers = self.alias_tabs.add("Parser Avanzati")
        
        self._build_provider_names_tab()
        self._build_provider_markets_tab()
        self._build_provider_parsers_tab()

    def _build_provider_names_tab(self):
        # Treeview per alias nomi eventi
        cols = ("Alias", "Betfair Name", "Country")
        self.name_alias_tree = ttk.Treeview(self.tab_names, columns=cols, show="headings")
        for c in cols: self.name_alias_tree.heading(c, text=c)
        self.name_alias_tree.pack(fill="both", expand=True)

    def _build_provider_markets_tab(self):
        # Treeview per alias mercati
        cols = ("Frase", "Tipo Mercato", "Betfair Name", "Selezione")
        self.market_alias_tree = ttk.Treeview(self.tab_markets, columns=cols, show="headings")
        for c in cols: self.market_alias_tree.heading(c, text=c)
        self.market_alias_tree.pack(fill="both", expand=True)

    def _build_provider_parsers_tab(self):
        # Treeview per parser avanzati
        cols = ("Nome", "Stato", "Tipo")
        self.adv_parser_tree = ttk.Treeview(self.tab_parsers, columns=cols, show="headings")
        for c in cols: self.adv_parser_tree.heading(c, text=c)
        self.adv_parser_tree.pack(fill="both", expand=True)

    def _on_sync_catalog(self):
        from services.catalog_sync_service import CatalogSyncService
        sync_svc = CatalogSyncService(self.db, self.betfair_service.get_client())
        
        def _work():
            sync_svc.run_sync(force=True)
            return self.db.get_sync_meta()
            
        def _done(meta):
            if meta:
                self.lbl_sync_info.configure(text=f"Ultimo Sync: {meta['last_sync_at']} ({meta['events_count']} ev)")
            self._log("Sincronizzazione catalogo completata.")

        self._run_runtime_command_async("SYNC_CATALOG", _work, on_success=_done)

    def _refresh_runtime_status(self):
        if self._status_refresh_inflight:
            self._status_refresh_pending = True
            return

        submit_fn = getattr(getattr(self, "executor", None), "submit", None)
        if not callable(submit_fn):
            self._apply_runtime_status_snapshot(self._build_runtime_status_snapshot())
            return

        self._status_refresh_inflight = True
        self._status_refresh_pending = False

        try:
            future = submit_fn("gui_refresh_runtime_status", self._build_runtime_status_snapshot)
        except Exception as exc:
            self._status_refresh_inflight = False
            self._apply_runtime_status_snapshot({"error": exc, "status": {}})
            return

        if not hasattr(future, "add_done_callback"):
            self._status_refresh_inflight = False
            self._apply_runtime_status_snapshot(future if isinstance(future, dict) else {"status": {}})
            return

        def _done(fut):
            try:
                snapshot = fut.result()
            except Exception as exc:
                snapshot = {"error": exc, "status": {}}

            def _apply():
                self._status_refresh_inflight = False
                self._apply_runtime_status_snapshot(snapshot)
                if self._status_refresh_pending:
                    self._refresh_runtime_status()

            self.uiq.post(_apply)

        future.add_done_callback(_done)

    def _refresh_live_control_plane_status(self, status: dict):
        runtime_status = status or {}
        execution_mode = str(self.execution_mode_var.get() or "SIMULATION").strip().upper()
        if execution_mode not in {"SIMULATION", "LIVE"}:
            execution_mode = "SIMULATION"
        live_enabled = bool(self.live_enabled_var.get())
        kill_switch = bool(self.kill_switch_var.get())
        self.live_requested_mode_var.set(execution_mode)

        readiness = {"level": "UNKNOWN", "ready": False, "blockers": ["READINESS_UNAVAILABLE"]}
        evaluate = getattr(self.runtime, "evaluate_live_readiness", None)
        if callable(evaluate):
            try:
                readiness = evaluate(
                    execution_mode=execution_mode,
                    live_enabled=live_enabled,
                    live_readiness_ok=runtime_status.get("live_readiness_ok"),
                ) or readiness
            except Exception:
                pass

        level = str(readiness.get("level", "UNKNOWN") or "UNKNOWN").strip().upper()
        if level not in {"READY", "DEGRADED", "NOT_READY", "UNKNOWN"}:
            level = "UNKNOWN"
        blockers = readiness.get("blockers")
        if not isinstance(blockers, list):
            blockers = ["READINESS_BLOCKERS_UNKNOWN"]
        blockers = [str(item) for item in blockers if str(item).strip()]

        if kill_switch and "KILL_SWITCH_ACTIVE" not in blockers:
            blockers = list(blockers) + ["KILL_SWITCH_ACTIVE"]
        if execution_mode == "LIVE" and not live_enabled and "LIVE_DISABLED" not in blockers:
            blockers = list(blockers) + ["LIVE_DISABLED"]
        if execution_mode == "LIVE" and level == "UNKNOWN" and "READINESS_UNKNOWN" not in blockers:
            blockers = list(blockers) + ["READINESS_UNKNOWN"]

        effective_status = "UNKNOWN"
        control_state = "Status unavailable"
        decision = "N/A"
        reason_code = "N/A"
        if kill_switch:
            effective_status = "LIVE_BLOCKED"
            control_state = "LIVE blocked by kill switch"
            decision = "NO-GO"
            reason_code = "KILL_SWITCH_ACTIVE"
        elif execution_mode != "LIVE":
            effective_status = "SAFE_MODE"
            control_state = "SIMULATION"
            reason_code = "SIMULATION_MODE"
        elif not live_enabled:
            effective_status = "LIVE_REQUESTED_BLOCKED"
            control_state = "LIVE requested but blocked (gate OFF)"
            decision = "NO-GO"
            reason_code = "LIVE_DISABLED"
        elif blockers:
            effective_status = "LIVE_REQUESTED_BLOCKED"
            control_state = "LIVE requested but blocked"
            decision = "NO-GO"
            reason_code = str(blockers[0])
        elif level == "READY":
            effective_status = "LIVE_ACTIVE"
            control_state = "LIVE active"
            decision = "GO"
            reason_code = "READY"
        else:
            effective_status = "UNKNOWN"
            control_state = "LIVE requested (readiness unknown)"
            decision = "NO-GO"
            reason_code = "READINESS_UNKNOWN"

        if blockers:
            blockers_text = ", ".join(str(item) for item in blockers)
        else:
            blockers_text = "No blockers."

        status_decision = str(runtime_status.get("last_decision", "") or "").strip().upper()
        if status_decision in {"GO", "NO-GO"}:
            decision = status_decision
        status_reason = str(
            runtime_status.get("last_reason_code", runtime_status.get("last_decision_reason", "")) or ""
        ).strip().upper()
        if status_reason:
            reason_code = status_reason

        self._apply_control_plane_colors(effective_status=effective_status, readiness_level=level)

        self.live_readiness_level_var.set(level)
        self.live_readiness_blockers_var.set(f"Blockers: {blockers_text}")
        self.live_control_state_var.set(control_state)
        self.live_effective_status_var.set(effective_status)
        self.live_last_decision_var.set(f"Last decision: {decision}")
        self.live_last_reason_var.set(f"Last reason: {reason_code}")

    def _apply_control_plane_colors(self, *, effective_status: str, readiness_level: str):
        readiness_colors = {
            "READY": "#2fa26b",
            "DEGRADED": "#cc9a06",
            "NOT_READY": "#d9534f",
            "UNKNOWN": "#9aa0a6",
        }
        effective_colors = {
            "SAFE_MODE": "#2fa26b",
            "LIVE_ACTIVE": "#2fa26b",
            "LIVE_REQUESTED_BLOCKED": "#cc9a06",
            "LIVE_BLOCKED": "#d9534f",
            "UNKNOWN": "#9aa0a6",
        }
        try:
            self.live_readiness_label.configure(text_color=readiness_colors.get(readiness_level, "#9aa0a6"))
        except Exception:
            pass
        try:
            self.live_effective_status_label.configure(text_color=effective_colors.get(effective_status, "#9aa0a6"))
        except Exception:
            pass

    def _log(self, text: str):
        if hasattr(self, "log_text"):
            try:
                self.log_text.insert("end", f"{text}\n")
                self.log_text.see("end")
            except Exception:
                pass

    # =========================================================
    # SIGNAL PATTERNS MANAGEMENT (CRUD)
    # =========================================================
    def _refresh_rules_tree(self):
        self.uiq.post(self._log, "Refreshing signal patterns...")
        try:
            patterns = self.db.get_signal_patterns()
            def _update():
                if not hasattr(self, "rules_tree"): return
                for item in self.rules_tree.get_children():
                    self.rules_tree.delete(item)
                for p in patterns:
                    self.rules_tree.insert(
                        "",
                        "end",
                        values=(
                            "✅" if p.get("enabled") else "❌",
                            p.get("label", "-"),
                            p.get("market_type", "MATCH_ODDS"),
                            p.get("bet_side", "BACK"),
                            p.get("selection_template", ""),
                            f"{p.get('min_minute', '')}-{p.get('max_minute', '')}",
                            f"{p.get('min_score', '')}-{p.get('max_score', '')}",
                            "YES" if p.get("live_only") else "NO",
                            p.get("priority", 100),
                            p.get("pattern", ""),
                        ),
                        tags=(str(p.get("id")),)
                    )
            self.uiq.post(_update)
        except Exception as e:
            self.uiq.post(self._log, f"Error refreshing rules: {e}")

    def _add_signal_pattern(self):
        self._edit_signal_pattern_dialog(None)

    def _edit_signal_pattern(self):
        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da modificare")
            return
        pattern_id = int(self.rules_tree.item(selected[0])["tags"][0])
        patterns = self.db.get_signal_patterns()
        pattern = next((p for p in patterns if p["id"] == pattern_id), None)
        if pattern:
            self._edit_signal_pattern_dialog(pattern)

    def _delete_signal_pattern(self):
        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da eliminare")
            return
        if not messagebox.askyesno("Conferma", "Vuoi davvero eliminare questa regola?"):
            return
        pattern_id = int(self.rules_tree.item(selected[0])["tags"][0])
        try:
            self.db.delete_signal_pattern(pattern_id)
            self._refresh_rules_tree()
        except Exception as e:
            messagebox.showerror("Errore", f"Impossibile eliminare la regola: {e}")

    def _toggle_signal_pattern(self):
        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola")
            return
        pattern_id = int(self.rules_tree.item(selected[0])["tags"][0])
        patterns = self.db.get_signal_patterns()
        pattern = next((p for p in patterns if p["id"] == pattern_id), None)
        if pattern:
            try:
                self.db.toggle_signal_pattern(pattern_id, not pattern["enabled"])
                self._refresh_rules_tree()
            except Exception as e:
                messagebox.showerror("Errore", f"Impossibile attivare/disattivare la regola: {e}")

    def _edit_signal_pattern_dialog(self, pattern=None):
        dialog = ctk.CTkToplevel(self)
        dialog.title("Modifica Regola Parser" if pattern else "Nuova Regola Parser")
        dialog.geometry("600x700")
        dialog.grab_set()

        scroll = ctk.CTkScrollableFrame(dialog)
        scroll.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        vars = {
            "label": tk.StringVar(value=pattern["label"] if pattern else ""),
            "pattern": tk.StringVar(value=pattern["pattern"] if pattern else ""),
            "market_type": tk.StringVar(value=pattern["market_type"] if pattern else "MATCH_ODDS"),
            "bet_side": tk.StringVar(value=pattern["bet_side"] if pattern else "BACK"),
            "selection_template": tk.StringVar(value=pattern["selection_template"] if pattern else ""),
            "min_minute": tk.StringVar(value=str(pattern["min_minute"]) if pattern and pattern["min_minute"] is not None else ""),
            "max_minute": tk.StringVar(value=str(pattern["max_minute"]) if pattern and pattern["max_minute"] is not None else ""),
            "min_score": tk.StringVar(value=str(pattern["min_score"]) if pattern and pattern["min_score"] is not None else ""),
            "max_score": tk.StringVar(value=str(pattern["max_score"]) if pattern and pattern["max_score"] is not None else ""),
            "live_only": tk.BooleanVar(value=bool(pattern["live_only"]) if pattern else True),
            "priority": tk.StringVar(value=str(pattern["priority"]) if pattern else "100"),
        }

        self._labeled_entry(scroll, "Nome Regola", vars["label"])
        self._labeled_entry(scroll, "Regex Pattern", vars["pattern"])
        
        m_frame = ctk.CTkFrame(scroll, fg_color="transparent")
        m_frame.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(m_frame, text="Mercato", width=220, anchor="w").pack(side=tk.LEFT, padx=8)
        ctk.CTkComboBox(m_frame, variable=vars["market_type"], values=["MATCH_ODDS", "OVER_UNDER", "BOTH_TEAMS_TO_SCORE"], width=320).pack(side=tk.LEFT, padx=8)

        s_frame = ctk.CTkFrame(scroll, fg_color="transparent")
        s_frame.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkLabel(s_frame, text="Side", width=220, anchor="w").pack(side=tk.LEFT, padx=8)
        ctk.CTkComboBox(s_frame, variable=vars["bet_side"], values=["BACK", "LAY"], width=320).pack(side=tk.LEFT, padx=8)

        self._labeled_entry(scroll, "Selection Template", vars["selection_template"])
        self._labeled_entry(scroll, "Min Minuto", vars["min_minute"])
        self._labeled_entry(scroll, "Max Minuto", vars["max_minute"])
        self._labeled_entry(scroll, "Min Score (Totale)", vars["min_score"])
        self._labeled_entry(scroll, "Max Score (Totale)", vars["max_score"])
        
        l_frame = ctk.CTkFrame(scroll, fg_color="transparent")
        l_frame.pack(fill=tk.X, padx=12, pady=6)
        ctk.CTkCheckBox(l_frame, text="Solo Live", variable=vars["live_only"]).pack(side=tk.LEFT, padx=228)

        self._labeled_entry(scroll, "Priorità", vars["priority"])

        def _save():
            data = {
                "label": vars["label"].get(),
                "pattern": vars["pattern"].get(),
                "market_type": vars["market_type"].get(),
                "bet_side": vars["bet_side"].get(),
                "selection_template": vars["selection_template"].get(),
                "min_minute": int(vars["min_minute"].get()) if vars["min_minute"].get() else None,
                "max_minute": int(vars["max_minute"].get()) if vars["max_minute"].get() else None,
                "min_score": int(vars["min_score"].get()) if vars["min_score"].get() else None,
                "max_score": int(vars["max_score"].get()) if vars["max_score"].get() else None,
                "live_only": 1 if vars["live_only"].get() else 0,
                "priority": int(vars["priority"].get()) if vars["priority"].get() else 100,
            }
            try:
                if pattern:
                    self.db.update_signal_pattern(pattern["id"], **data)
                else:
                    self.db.save_signal_pattern(**data)
                self._refresh_rules_tree()
                dialog.destroy()
            except Exception as e:
                messagebox.showerror("Errore", f"Salvataggio fallito: {e}")

        ctk.CTkButton(scroll, text="Salva Regola", command=_save, fg_color=COLORS.get("button_success", "#2fa26b")).pack(pady=20)

    def _start_polling(self):
        self._refresh_runtime_status()
        self.uiq.post(self._refresh_runtime_status)
        self.after(1000, self._refresh_runtime_status_poller)

    def _refresh_runtime_status_poller(self):
        self.uiq.post(self._refresh_runtime_status)
        self.after(1000, self._refresh_runtime_status_poller)

    def _on_close(self):
        try:
            if hasattr(self.shutdown, "shutdown"):
                self.shutdown.shutdown()
            elif hasattr(self.shutdown, "run"):
                self.shutdown.run()
        except Exception:
            pass
        self.destroy()


def main() -> int:
    """Entry point della Mini GUI (modalita' con display grafico).

    `main.py:run_gui()` importa questa funzione (`from mini_gui import main`):
    la sua assenza rompeva l'avvio GUI con
    `ImportError: cannot import name 'main' from 'mini_gui'`.

    Costruisce `MiniPickfairGUI(force_simulation=True)` (test_mode=False =>
    finestra Tk reale) e avvia il mainloop. `force_simulation=True` garantisce
    il fail-closed **fin dalla costruzione**: lo stato persistito LIVE non viene
    mai sincronizzato al runtime, quindi non esiste nemmeno una finestra LIVE
    transitoria in `__init__`. Questo entry point NON parte mai in LIVE, anche
    se l'ultima sessione aveva salvato LIVE — il gate #350 resta la difesa
    runtime per ogni transizione a LIVE successiva. Richiede un display grafico
    (X11): su un host headless senza $DISPLAY, Tk non puo' aprire la finestra ed
    emette l'errore Tcl standard "no display name and no $DISPLAY environment
    variable" — in quel caso va usata la modalita' `--headless`.
    """
    app = None
    try:
        app = MiniPickfairGUI(force_simulation=True)
        app.mainloop()
        return 0
    except KeyboardInterrupt:
        _LOGGER.info("Arresto Mini GUI richiesto dall'utente")
        return 130
    except Exception as exc:  # pragma: no cover - errori runtime GUI reali
        _LOGGER.exception("Errore fatale nella Mini GUI: %s", exc)
        return 1
    finally:
        if app is not None:
            try:
                app.destroy()
            except Exception:
                pass


if __name__ == "__main__":
    raise SystemExit(main())
