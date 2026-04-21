from __future__ import annotations

import logging
import tkinter as tk
from tkinter import messagebox, simpledialog

import customtkinter as ctk

from theme import COLORS, FONTS
from services.telegram_signal_processor import TelegramSignalProcessor
from services.telegram_bet_resolver import TelegramBetResolver
from observability.sanitizers import sanitize_dict

logger = logging.getLogger(__name__)

# =========================================================
# MERCATI BETFAIR — (codice, etichetta)
# =========================================================
_MARKET_OPTIONS: list[tuple[str, str]] = [
    # 1X2
    ("MATCH_ODDS",          "1X2 — Match Odds"),
    # Over/Under FT
    ("OVER_UNDER_05",       "Over/Under FT 0.5"),
    ("OVER_UNDER_15",       "Over/Under FT 1.5"),
    ("OVER_UNDER_25",       "Over/Under FT 2.5"),
    ("OVER_UNDER_35",       "Over/Under FT 3.5"),
    ("OVER_UNDER_45",       "Over/Under FT 4.5"),
    ("OVER_UNDER_55",       "Over/Under FT 5.5"),
    ("OVER_UNDER_65",       "Over/Under FT 6.5"),
    ("OVER_UNDER_75",       "Over/Under FT 7.5"),
    # Over/Under PT
    ("OVER_UNDER_HT_05",    "Over/Under PT 0.5"),
    ("OVER_UNDER_HT_15",    "Over/Under PT 1.5"),
    ("OVER_UNDER_HT_25",    "Over/Under PT 2.5"),
    ("OVER_UNDER_HT_35",    "Over/Under PT 3.5"),
    ("OVER_UNDER_HT_45",    "Over/Under PT 4.5"),
    # Entrambe segnano
    ("BTTS_YES",            "Entrambe Segnano — Sì"),
    ("BTTS_NO",             "Entrambe Segnano — No"),
    # Risultato Esatto FT
    ("CS_0_0",              "Ris. Esatto 0-0"),
    ("CS_1_0",              "Ris. Esatto 1-0"),
    ("CS_0_1",              "Ris. Esatto 0-1"),
    ("CS_1_1",              "Ris. Esatto 1-1"),
    ("CS_2_0",              "Ris. Esatto 2-0"),
    ("CS_0_2",              "Ris. Esatto 0-2"),
    ("CS_2_1",              "Ris. Esatto 2-1"),
    ("CS_1_2",              "Ris. Esatto 1-2"),
    ("CS_2_2",              "Ris. Esatto 2-2"),
    ("CS_3_0",              "Ris. Esatto 3-0"),
    ("CS_0_3",              "Ris. Esatto 0-3"),
    ("CS_3_1",              "Ris. Esatto 3-1"),
    ("CS_1_3",              "Ris. Esatto 1-3"),
    ("CS_3_2",              "Ris. Esatto 3-2"),
    ("CS_2_3",              "Ris. Esatto 2-3"),
    ("CS_3_3",              "Ris. Esatto 3-3"),
    ("CS_4_0",              "Ris. Esatto 4-0"),
    ("CS_0_4",              "Ris. Esatto 0-4"),
    ("CS_4_1",              "Ris. Esatto 4-1"),
    ("CS_1_4",              "Ris. Esatto 1-4"),
    ("CS_OTHER",            "Ris. Esatto — Altro"),
    # Risultato Esatto PT
    ("CS_HT_0_0",           "Ris. Esatto PT 0-0"),
    ("CS_HT_1_0",           "Ris. Esatto PT 1-0"),
    ("CS_HT_0_1",           "Ris. Esatto PT 0-1"),
    ("CS_HT_1_1",           "Ris. Esatto PT 1-1"),
    ("CS_HT_2_0",           "Ris. Esatto PT 2-0"),
    ("CS_HT_0_2",           "Ris. Esatto PT 0-2"),
    ("CS_HT_2_1",           "Ris. Esatto PT 2-1"),
    ("CS_HT_1_2",           "Ris. Esatto PT 1-2"),
    ("CS_HT_OTHER",         "Ris. Esatto PT — Altro"),
    # Doppia Chance
    ("DOUBLE_CHANCE_1X",    "Doppia Chance — 1X"),
    ("DOUBLE_CHANCE_12",    "Doppia Chance — 12"),
    ("DOUBLE_CHANCE_X2",    "Doppia Chance — X2"),
    # Parziale / Finale
    ("HT_FT_1_1",           "PT/FT — 1/1"),
    ("HT_FT_1_X",           "PT/FT — 1/X"),
    ("HT_FT_1_2",           "PT/FT — 1/2"),
    ("HT_FT_X_1",           "PT/FT — X/1"),
    ("HT_FT_X_X",           "PT/FT — X/X"),
    ("HT_FT_X_2",           "PT/FT — X/2"),
    ("HT_FT_2_1",           "PT/FT — 2/1"),
    ("HT_FT_2_X",           "PT/FT — 2/X"),
    ("HT_FT_2_2",           "PT/FT — 2/2"),
    # Asian Handicap
    ("ASIAN_HC_HOME_05",    "Handicap Casa +0.5"),
    ("ASIAN_HC_AWAY_05",    "Handicap Ospite +0.5"),
    ("ASIAN_HC_HOME_1",     "Handicap Casa +1"),
    ("ASIAN_HC_AWAY_1",     "Handicap Ospite +1"),
    ("ASIAN_HC_HOME_15",    "Handicap Casa +1.5"),
    ("ASIAN_HC_AWAY_15",    "Handicap Ospite +1.5"),
    ("ASIAN_HC_HOME_2",     "Handicap Casa +2"),
    ("ASIAN_HC_AWAY_2",     "Handicap Ospite +2"),
    # Prossimo Gol
    ("NEXT_GOAL",           "Prossimo Gol"),
]

# label → codice per lookup rapido
_MARKET_LABEL_TO_CODE: dict[str, str] = {label: code for code, label in _MARKET_OPTIONS}
_MARKET_CODE_TO_LABEL: dict[str, str] = {code: label for code, label in _MARKET_OPTIONS}
_MARKET_LABELS: list[str] = [label for _, label in _MARKET_OPTIONS]

# =========================================================
# PATTERN PREDEFINITI — (nome → {pattern, market_type})
# =========================================================
_PREDEFINED: dict[str, dict] = {
    "-- Personalizzato --":     {"pattern": "", "market_type": "MATCH_ODDS"},
    "Over 0.5 FT":              {"pattern": r"(?:OVER|O)\s*0[,.]?5\b", "market_type": "OVER_UNDER_05"},
    "Over 1.5 FT":              {"pattern": r"(?:OVER|O)\s*1[,.]?5\b", "market_type": "OVER_UNDER_15"},
    "Over 2.5 FT":              {"pattern": r"(?:OVER|O)\s*2[,.]?5\b", "market_type": "OVER_UNDER_25"},
    "Over 3.5 FT":              {"pattern": r"(?:OVER|O)\s*3[,.]?5\b", "market_type": "OVER_UNDER_35"},
    "Over 4.5 FT":              {"pattern": r"(?:OVER|O)\s*4[,.]?5\b", "market_type": "OVER_UNDER_45"},
    "Under 2.5 FT":             {"pattern": r"(?:UNDER|U)\s*2[,.]?5\b", "market_type": "OVER_UNDER_25"},
    "Under 3.5 FT":             {"pattern": r"(?:UNDER|U)\s*3[,.]?5\b", "market_type": "OVER_UNDER_35"},
    "Over 0.5 PT":              {"pattern": r"(?:OVER|O)\s*0[,.]?5.{0,20}(?:PT|HT|PRIMO)", "market_type": "OVER_UNDER_HT_05"},
    "Over 1.5 PT":              {"pattern": r"(?:OVER|O)\s*1[,.]?5.{0,20}(?:PT|HT|PRIMO)", "market_type": "OVER_UNDER_HT_15"},
    "Over 2.5 PT":              {"pattern": r"(?:OVER|O)\s*2[,.]?5.{0,20}(?:PT|HT|PRIMO)", "market_type": "OVER_UNDER_HT_25"},
    "Next Gol":                 {"pattern": r"NEXT\s*GOL|PROSSIMO\s*GOL", "market_type": "NEXT_GOAL"},
    "Entrambe Segnano Sì":      {"pattern": r"(?:BTTS|ENTRAMBE|BOTH)\s*(?:SI|YES|S[IÌ])\b", "market_type": "BTTS_YES"},
    "Entrambe Segnano No":      {"pattern": r"(?:BTTS|ENTRAMBE|BOTH)\s*NO\b", "market_type": "BTTS_NO"},
    "1X2 — Casa vince":         {"pattern": r"(?:CASA|HOME)\s*(?:VINCE|WIN)|WIN\s*HOME\b", "market_type": "MATCH_ODDS"},
    "1X2 — Pareggio":           {"pattern": r"\b(?:PAREGGIO|DRAW)\b", "market_type": "MATCH_ODDS"},
    "1X2 — Ospite vince":       {"pattern": r"(?:OSPITE|AWAY)\s*(?:VINCE|WIN)|WIN\s*AWAY\b", "market_type": "MATCH_ODDS"},
    "Doppia Chance 1X":         {"pattern": r"\b(?:DOPPIA|DC)\s*1X\b", "market_type": "DOUBLE_CHANCE_1X"},
    "Doppia Chance X2":         {"pattern": r"\b(?:DOPPIA|DC)\s*X2\b", "market_type": "DOUBLE_CHANCE_X2"},
    "Doppia Chance 12":         {"pattern": r"\b(?:DOPPIA|DC)\s*12\b", "market_type": "DOUBLE_CHANCE_12"},
}
_PREDEFINED_NAMES: list[str] = list(_PREDEFINED.keys())


# =========================================================
# DIALOG FORM — Nuova / Modifica Regola di Parsing
# =========================================================
class _PatternDialog(ctk.CTkToplevel):
    """
    Form modale per creare o modificare una regola di parsing.
    result è None se l'utente annulla, altrimenti un dict con tutti i campi.
    """

    def __init__(self, parent, current: dict | None = None):
        super().__init__(parent)
        self.result: dict | None = None
        self._current = current or {}
        self._market_var = tk.StringVar()
        self._predef_var = tk.StringVar(value="-- Personalizzato --")

        self.title("Regola di Parsing")
        self.resizable(False, False)
        self.grab_set()
        self._build()
        self._center(parent)

    def _center(self, parent):
        self.update_idletasks()
        px = parent.winfo_rootx() + parent.winfo_width() // 2
        py = parent.winfo_rooty() + parent.winfo_height() // 2
        w, h = self.winfo_width(), self.winfo_height()
        self.geometry(f"+{px - w // 2}+{py - h // 2}")

    # ----------------------------------------------------------
    def _row(self, parent, label: str, row: int):
        ctk.CTkLabel(
            parent, text=label,
            text_color=COLORS["text_secondary"],
            font=("Segoe UI", 11),
            anchor="w",
        ).grid(row=row, column=0, sticky="w", padx=(10, 6), pady=(6, 0))

    def _entry(self, parent, row: int, var: tk.StringVar, width: int = 320) -> ctk.CTkEntry:
        e = ctk.CTkEntry(
            parent, textvariable=var, width=width,
            fg_color=COLORS["bg_card"], border_color=COLORS["border"],
        )
        e.grid(row=row, column=1, sticky="w", padx=(0, 10), pady=(6, 0))
        return e

    # ----------------------------------------------------------
    def _build(self):
        c = self._current
        pad = {"padx": 10, "pady": 4}

        outer = ctk.CTkFrame(self, fg_color=COLORS["bg_panel"], corner_radius=10)
        outer.pack(fill=tk.BOTH, expand=True, padx=12, pady=12)

        ctk.CTkLabel(
            outer,
            text="Nuova Regola di Parsing" if not c else "Modifica Regola",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).grid(row=0, column=0, columnspan=2, sticky="w", padx=10, pady=(10, 4))

        # Nome
        self._row(outer, "Nome:", 1)
        self._nome_var = tk.StringVar(value=c.get("label", ""))
        self._entry(outer, 1, self._nome_var)

        # Pattern Predefinito
        self._row(outer, "Pattern Predefinito:", 2)
        predef_box = ctk.CTkOptionMenu(
            outer,
            variable=self._predef_var,
            values=_PREDEFINED_NAMES,
            width=320,
            command=self._on_predef_select,
            fg_color=COLORS["bg_card"],
            button_color=COLORS["button_primary"],
        )
        predef_box.grid(row=2, column=1, sticky="w", padx=(0, 10), pady=(6, 0))

        # Pattern Regex
        self._row(outer, "Pattern Regex:", 3)
        self._regex_var = tk.StringVar(value=c.get("pattern", ""))
        self._entry(outer, 3, self._regex_var)

        # Parola Chiave
        self._row(outer, "Parola Chiave:", 4)
        self._keyword_var = tk.StringVar(value=c.get("keyword", ""))
        self._entry(outer, 4, self._keyword_var)
        ctk.CTkLabel(
            outer,
            text="(opzionale — se presente nel msg attiva il pattern)",
            font=("Segoe UI", 9),
            text_color=COLORS["text_tertiary"],
        ).grid(row=5, column=1, sticky="w", padx=(0, 10))

        # Tipo Mercato
        self._row(outer, "Tipo Mercato:", 6)
        current_code = c.get("market_type", "MATCH_ODDS")
        self._market_var.set(_MARKET_CODE_TO_LABEL.get(current_code, _MARKET_LABELS[0]))
        market_box = ctk.CTkOptionMenu(
            outer,
            variable=self._market_var,
            values=_MARKET_LABELS,
            width=320,
            fg_color=COLORS["bg_card"],
            button_color=COLORS["button_primary"],
        )
        market_box.grid(row=6, column=1, sticky="w", padx=(0, 10), pady=(6, 0))

        # Selection Template
        self._row(outer, "Selection Template:", 7)
        self._template_var = tk.StringVar(value=c.get("selection_template", ""))
        self._entry(outer, 7, self._template_var)
        ctk.CTkLabel(
            outer,
            text="Token: {over_line} {total_goals} {home_score} {away_score} {minute}",
            font=("Segoe UI", 9),
            text_color=COLORS["text_tertiary"],
        ).grid(row=8, column=1, sticky="w", padx=(0, 10))

        # Checkboxes: LAY / LIVE / Attiva
        chk_frame = ctk.CTkFrame(outer, fg_color="transparent")
        chk_frame.grid(row=9, column=0, columnspan=2, sticky="w", padx=10, pady=(8, 0))

        self._lay_var = tk.BooleanVar(value=(str(c.get("bet_side", "")).upper() == "LAY"))
        ctk.CTkCheckBox(
            chk_frame, text="LAY",
            variable=self._lay_var,
            fg_color=COLORS["loss"],
            hover_color="#c62828",
            text_color=COLORS["text_primary"],
        ).pack(side=tk.LEFT, padx=(0, 16))

        self._live_var = tk.BooleanVar(value=bool(c.get("live_only", False)))
        ctk.CTkCheckBox(
            chk_frame, text="Solo LIVE",
            variable=self._live_var,
            fg_color=COLORS["back"],
            hover_color=COLORS["back_hover"],
            text_color=COLORS["text_primary"],
        ).pack(side=tk.LEFT, padx=(0, 16))

        self._active_var = tk.BooleanVar(value=bool(c.get("enabled", True)))
        ctk.CTkCheckBox(
            chk_frame, text="Regola Attiva",
            variable=self._active_var,
            fg_color=COLORS["button_success"],
            hover_color="#4caf50",
            text_color=COLORS["text_primary"],
        ).pack(side=tk.LEFT)

        # Filtri numerici
        filters = ctk.CTkFrame(outer, fg_color="transparent")
        filters.grid(row=10, column=0, columnspan=2, sticky="w", padx=10, pady=(8, 0))

        def _lbl(text):
            return ctk.CTkLabel(filters, text=text, text_color=COLORS["text_secondary"], font=("Segoe UI", 11))

        def _num_entry(var, w=55):
            return ctk.CTkEntry(filters, textvariable=var, width=w,
                                fg_color=COLORS["bg_card"], border_color=COLORS["border"])

        self._min_min_var = tk.StringVar(value="" if c.get("min_minute") is None else str(c["min_minute"]))
        self._max_min_var = tk.StringVar(value="" if c.get("max_minute") is None else str(c["max_minute"]))
        self._min_sc_var  = tk.StringVar(value="" if c.get("min_score")  is None else str(c["min_score"]))
        self._max_sc_var  = tk.StringVar(value="" if c.get("max_score")  is None else str(c["max_score"]))
        self._prio_var    = tk.StringVar(value=str(c.get("priority", 100)))

        _lbl("Minuti:").pack(side=tk.LEFT)
        _lbl("da").pack(side=tk.LEFT, padx=(4, 2))
        _num_entry(self._min_min_var).pack(side=tk.LEFT, padx=(0, 4))
        _lbl("a").pack(side=tk.LEFT, padx=(0, 2))
        _num_entry(self._max_min_var).pack(side=tk.LEFT, padx=(0, 16))

        _lbl("Score:").pack(side=tk.LEFT)
        _lbl("da").pack(side=tk.LEFT, padx=(4, 2))
        _num_entry(self._min_sc_var).pack(side=tk.LEFT, padx=(0, 4))
        _lbl("a").pack(side=tk.LEFT, padx=(0, 2))
        _num_entry(self._max_sc_var).pack(side=tk.LEFT, padx=(0, 16))

        _lbl("Priority:").pack(side=tk.LEFT)
        _num_entry(self._prio_var, w=50).pack(side=tk.LEFT, padx=(4, 0))

        # Bottoni
        btn_frame = ctk.CTkFrame(outer, fg_color="transparent")
        btn_frame.grid(row=11, column=0, columnspan=2, pady=(14, 10))

        ctk.CTkButton(
            btn_frame, text="Annulla", width=110,
            fg_color=COLORS["button_secondary"],
            hover_color=COLORS["bg_hover"],
            command=self.destroy,
        ).pack(side=tk.LEFT, padx=8)

        ctk.CTkButton(
            btn_frame, text="Salva", width=110,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            command=self._on_save,
        ).pack(side=tk.LEFT, padx=8)

    # ----------------------------------------------------------
    def _on_predef_select(self, choice: str):
        entry = _PREDEFINED.get(choice, {})
        if entry.get("pattern"):
            self._regex_var.set(entry["pattern"])
        code = entry.get("market_type", "")
        if code and code in _MARKET_CODE_TO_LABEL:
            self._market_var.set(_MARKET_CODE_TO_LABEL[code])

    # ----------------------------------------------------------
    def _parse_int_opt(self, var: tk.StringVar):
        v = var.get().strip()
        if not v:
            return None
        try:
            return int(v)
        except ValueError:
            return None

    def _on_save(self):
        nome = self._nome_var.get().strip()
        regex = self._regex_var.get().strip()
        keyword = self._keyword_var.get().strip()

        if not nome:
            messagebox.showwarning("Attenzione", "Il campo Nome è obbligatorio.", parent=self)
            return
        if not regex and not keyword:
            messagebox.showwarning(
                "Attenzione",
                "Inserisci almeno un Pattern Regex oppure una Parola Chiave.",
                parent=self,
            )
            return

        market_label = self._market_var.get()
        market_code = _MARKET_LABEL_TO_CODE.get(market_label, "MATCH_ODDS")

        self.result = {
            "label":              nome,
            "pattern":            regex,
            "keyword":            keyword,
            "market_type":        market_code,
            "bet_side":           "LAY" if self._lay_var.get() else "BACK",
            "selection_template": self._template_var.get().strip(),
            "live_only":          self._live_var.get(),
            "enabled":            self._active_var.get(),
            "min_minute":         self._parse_int_opt(self._min_min_var),
            "max_minute":         self._parse_int_opt(self._max_min_var),
            "min_score":          self._parse_int_opt(self._min_sc_var),
            "max_score":          self._parse_int_opt(self._max_sc_var),
            "priority":           self._parse_int_opt(self._prio_var) or 100,
        }
        self.destroy()


class TelegramModule:
    """
    Mixin Telegram per MiniPickfairGUI.

    Funzioni:
    - start/stop listener
    - gestione chat monitorate
    - CRUD pattern avanzati
    - gestione segnali Telegram
    - se il segnale ha già market_id/selection_id -> usa TelegramSignalProcessor
    - se NON li ha -> prova a risolvere automaticamente via TelegramBetResolver
    """

    TELEGRAM_BOUNDARY_STAGE = "telegram_ingestion_normalized_v1"
    TELEGRAM_ROUTING_CONTRACT = "telegram_authoritative_routing_v1"
    TELEGRAM_PRIMARY_ROUTE = "SIGNAL_RECEIVED"
    TELEGRAM_COMPAT_FALLBACK_ROUTE = "REQ_QUICK_BET"

    # =========================================================
    # INTERNAL HELPERS
    # =========================================================
    def _get_signal_processor(self):
        processor = getattr(self, "_telegram_signal_processor", None)
        if processor is None:
            processor = TelegramSignalProcessor()
            self._telegram_signal_processor = processor
        return processor

    def _get_bet_resolver(self):
        resolver = getattr(self, "_telegram_bet_resolver", None)
        if resolver is None:
            client_getter = None

            # preferisci il service broker
            if hasattr(self, "betfair_service") and self.betfair_service is not None:
                client_getter = self.betfair_service.get_client
            elif hasattr(self, "runtime") and getattr(self.runtime, "betfair_service", None) is not None:
                client_getter = self.runtime.betfair_service.get_client
            elif hasattr(self, "betfair_client"):
                client_getter = lambda: getattr(self, "betfair_client", None)

            resolver = TelegramBetResolver(client_getter=client_getter)
            self._telegram_bet_resolver = resolver

        return resolver

    def _safe_refresh_telegram_signals_tree(self):
        try:
            if hasattr(self, "_refresh_telegram_signals_tree"):
                self._refresh_telegram_signals_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh signals tree: %s", e)

    def _safe_refresh_telegram_chats_tree(self):
        try:
            if hasattr(self, "_refresh_telegram_chats_tree"):
                self._refresh_telegram_chats_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh chats tree: %s", e)

    def _safe_refresh_rules_tree(self):
        try:
            if hasattr(self, "_refresh_rules_tree"):
                self._refresh_rules_tree()
        except Exception as e:
            logger.exception("[TelegramModule] Errore refresh rules tree: %s", e)

    def _safe_parse_stake(self) -> float:
        try:
            raw = self.tg_auto_stake_var.get()
            if isinstance(raw, str):
                raw = raw.replace(",", ".").strip()
            stake = float(raw)
            return stake if stake > 0 else 1.0
        except Exception:
            return 1.0

    def _safe_parse_int_optional(self, value):
        if value in (None, ""):
            return None
        try:
            return int(str(value).strip())
        except Exception:
            return None

    def _safe_bool_from_text(self, value: str, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "yes", "y", "si", "sì", "on"}

    def _submit_non_blocking(self, task_name: str, worker_fn, on_success, on_error) -> None:
        submit_fn = getattr(getattr(self, "executor", None), "submit", None)
        if not callable(submit_fn):
            try:
                on_success(worker_fn())
            except Exception as exc:
                on_error(exc)
            return

        try:
            future = submit_fn(task_name, worker_fn)
        except Exception as exc:
            on_error(exc)
            return

        if hasattr(future, "add_done_callback"):
            def _done(fut):
                try:
                    result = fut.result()
                    if hasattr(self, "uiq") and self.uiq:
                        self.uiq.post(on_success, result)
                    else:
                        on_success(result)
                except Exception as exc:
                    if hasattr(self, "uiq") and self.uiq:
                        self.uiq.post(on_error, exc)
                    else:
                        on_error(exc)

            future.add_done_callback(_done)
            return

        try:
            on_success(future)
        except Exception as exc:
            on_error(exc)

    def _safe_db_save_received_signal(
        self,
        selection,
        action,
        price,
        stake,
        status,
        signal=None,
    ):
        if not hasattr(self, "db") or self.db is None:
            return
        if not hasattr(self.db, "save_received_signal"):
            return

        try:
            payload = dict(signal or {})
            payload.update(
                {
                    "selection": selection,
                    "action": action,
                    "price": float(price or 0.0),
                    "stake": float(stake or 0.0),
                    "status": str(status or ""),
                }
            )
            self.db.save_received_signal(payload)
        except Exception as e:
            logger.exception("[TelegramModule] Errore save_received_signal status=%s: %s", status, e)

    def _bus_has_subscriber(self, event_name: str) -> bool:
        event_name = str(event_name or "")
        if not event_name:
            return False

        try:
            subscribers = getattr(self.bus, "subscribers", None)
            if isinstance(subscribers, dict):
                return bool(list(subscribers.get(event_name) or []))
        except Exception:
            logger.exception("[TelegramModule] Errore accesso bus.subscribers")

        try:
            private_subscribers = getattr(self.bus, "_subscribers", None)
            if isinstance(private_subscribers, dict):
                lock = getattr(self.bus, "_lock", None)
                if lock is not None and hasattr(lock, "__enter__"):
                    with lock:
                        return bool(list(private_subscribers.get(event_name) or []))
                return bool(list(private_subscribers.get(event_name) or []))
        except Exception:
            logger.exception("[TelegramModule] Errore accesso bus._subscribers")

        return False

    def _publish_order_signal(self, payload: dict) -> str:
        """
        Authoritative Telegram routing boundary.

        Contract:
        1) Telegram payload MUST already be normalized at ingestion boundary.
        2) Primary route is SIGNAL_RECEIVED (RuntimeController gate owner).
        3) REQ_QUICK_BET is explicit compatibility fallback only when runtime
           signal gate is not subscribed.
        """
        payload = dict(payload or {})
        stage = str(payload.get("telegram_boundary_stage") or "").strip()
        if stage != self.TELEGRAM_BOUNDARY_STAGE:
            raise ValueError("TELEGRAM_ROUTING_BOUNDARY_INVALID_STAGE")

        payload["telegram_routing_contract"] = self.TELEGRAM_ROUTING_CONTRACT

        if self._bus_has_subscriber(self.TELEGRAM_PRIMARY_ROUTE):
            payload["telegram_route_target"] = self.TELEGRAM_PRIMARY_ROUTE
            self.bus.publish(self.TELEGRAM_PRIMARY_ROUTE, payload)
            return self.TELEGRAM_PRIMARY_ROUTE

        if self._bus_has_subscriber(self.TELEGRAM_COMPAT_FALLBACK_ROUTE):
            payload["telegram_route_target"] = self.TELEGRAM_COMPAT_FALLBACK_ROUTE
            self.bus.publish(self.TELEGRAM_COMPAT_FALLBACK_ROUTE, payload)
            return self.TELEGRAM_COMPAT_FALLBACK_ROUTE

        raise RuntimeError("TELEGRAM_ROUTING_NO_SUBSCRIBERS")

    def _needs_resolution(self, signal_data: dict) -> bool:
        market_id = signal_data.get("market_id") or signal_data.get("marketId")
        selection_id = signal_data.get("selection_id") or signal_data.get("selectionId")
        return not market_id or not selection_id

    def _resolve_signal_to_payload(self, signal_data: dict, stake: float):
        """
        Se il segnale ha già market_id/selection_id usa il normalizzatore standard.
        Se non li ha, usa il resolver automatico over successivo / linea esplicita.
        """
        processor = self._get_signal_processor()
        normalized_result = processor.normalize_ingestion_signal(signal_data)
        if not normalized_result.get("ok"):
            return None, f"INVALID_SIGNAL:{normalized_result.get('error_code')}"

        normalized_signal = dict(normalized_result.get("normalized_signal") or {})

        if not self._needs_resolution(normalized_signal):
            payload = processor.build_runtime_signal(
                signal=normalized_signal,
                stake=stake,
                simulation_mode=bool(getattr(self, "simulation_mode", False)),
            )
            if payload:
                payload["telegram_boundary_stage"] = normalized_signal.get("boundary_stage")
            return payload, "DIRECT"

        resolver = self._get_bet_resolver()
        resolved = resolver.resolve(normalized_signal, aggressive_best_price=True)
        if not resolved:
            return None, "UNRESOLVED"

        payload = resolved.to_order_payload(
            stake=stake,
            simulation_mode=bool(getattr(self, "simulation_mode", False)),
        )

        payload["raw_signal"] = dict(normalized_signal.get("raw_signal") or {})
        payload["telegram_boundary_stage"] = normalized_signal.get("boundary_stage")
        payload["resolution_mode"] = "AUTO_RESOLVED"
        return payload, "AUTO_RESOLVED"

    # =========================================================
    # START / STOP LISTENER
    # =========================================================
    def _start_telegram_listener(self):
        try:
            settings = self.db.get_telegram_settings() if hasattr(self, "db") else {}
            settings = settings or {}
        except Exception as e:
            logger.exception("[TelegramModule] Errore lettura telegram settings: %s", e)
            settings = {}

        if not settings.get("api_id") or not settings.get("api_hash"):
            messagebox.showwarning(
                "Attenzione",
                "Configura e salva le credenziali Telegram prima di avviare il listener.",
            )
            return

        existing_listener = getattr(self, "telegram_listener", None)
        if existing_listener and getattr(existing_listener, "running", False):
            messagebox.showinfo("Info", "Listener già in esecuzione")
            return

        try:
            from telegram_listener import TelegramListener

            api_id = int(settings["api_id"])
            api_hash = str(settings["api_hash"]).strip()
            session_string = settings.get("session_string")

            listener = TelegramListener(
                api_id=api_id,
                api_hash=api_hash,
                session_string=session_string,
                db=self.db,
            )

            monitored_chats = []
            try:
                chats = self.db.get_telegram_chats() if hasattr(self, "db") else []
                chats = chats or []
            except Exception as e:
                logger.exception("[TelegramModule] Errore lettura telegram chats: %s", e)
                chats = []

            for chat in chats:
                if not chat.get("is_active", True):
                    continue
                try:
                    monitored_chats.append(int(chat["chat_id"]))
                except Exception:
                    logger.warning("[TelegramModule] chat_id non valido ignorato: %s", chat)

            if not monitored_chats:
                messagebox.showwarning(
                    "Attenzione",
                    "Nessuna chat monitorata attiva. Aggiungine almeno una.",
                )
                return

            if hasattr(listener, "set_monitored_chats"):
                listener.set_monitored_chats(monitored_chats)
            if hasattr(listener, "set_database"):
                listener.set_database(self.db)

            if hasattr(listener, "set_callbacks"):
                listener.set_callbacks(
                    on_signal=self._handle_telegram_signal,
                    on_message=None,
                    on_status=lambda st, msg: self.bus.publish(
                        "TELEGRAM_STATUS",
                        {"status": st, "message": msg},
                    ),
                )

            self.telegram_listener = listener

            if hasattr(self.telegram_listener, "start"):
                try:
                    self.telegram_listener.start(monitored_chats=monitored_chats)
                except TypeError:
                    self.telegram_listener.start()

            self.telegram_status = "LISTENING"

            if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
                self.tg_status_label.configure(
                    text=f"Stato: {self.telegram_status}",
                    text_color=COLORS["success"],
                )

            messagebox.showinfo("Successo", "Telegram Listener avviato")

        except Exception as e:
            logger.exception("[TelegramModule] Impossibile avviare listener: %s", e)
            messagebox.showerror("Errore", f"Impossibile avviare listener: {str(e)}")

    def _stop_telegram_listener(self):
        listener = getattr(self, "telegram_listener", None)
        if listener and getattr(listener, "running", False):
            try:
                listener.stop()
            except Exception as e:
                logger.exception("[TelegramModule] Errore stop listener: %s", e)

            self.telegram_status = "STOPPED"

            if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
                self.tg_status_label.configure(
                    text=f"Stato: {self.telegram_status}",
                    text_color=COLORS["error"],
                )

            messagebox.showinfo("Info", "Telegram Listener fermato")

    # =========================================================
    # SIGNAL FLOW
    # =========================================================
    def _handle_telegram_signal(self, signal):
        """
        Listener -> handler -> direct payload or resolver -> publish order signal
        """

        def safe_process_signal():
            processor = self._get_signal_processor()
            normalized_result = processor.normalize_ingestion_signal(signal)
            signal_data = dict(normalized_result.get("normalized_signal") or {})

            if not normalized_result.get("ok"):
                error_code = str(normalized_result.get("error_code") or "INVALID_SIGNAL")
                error_reason = str(normalized_result.get("error_reason") or "invalid telegram signal")
                logger.warning("[TelegramModule] Scarto segnale telegram non valido: %s (%s)", error_code, error_reason)
                self._safe_db_save_received_signal(
                    selection="Unknown",
                    action="BACK",
                    price=0.0,
                    stake=self._safe_parse_stake(),
                    status="ERROR",
                    signal={
                        "error_code": error_code,
                        "error_reason": error_reason,
                        "raw_signal": signal,
                    },
                )
                self._safe_refresh_telegram_signals_tree()
                return

            action = processor.normalize_action(signal_data)
            selection_id = processor.parse_selection_id(signal_data)
            market_id = processor.parse_market_id(signal_data)
            original_price = processor.parse_price(signal_data)
            selection_name = processor.parse_selection_name(signal_data, selection_id)
            stake = self._safe_parse_stake()

            if original_price is None:
                original_price = 0.0

            self._safe_db_save_received_signal(
                selection=selection_name,
                action=action,
                price=original_price,
                stake=stake,
                status="RECEIVED",
                signal=signal_data,
            )
            self._safe_refresh_telegram_signals_tree()

            auto_bet_enabled = bool(
                hasattr(self, "tg_auto_bet_var")
                and self.tg_auto_bet_var is not None
                and self.tg_auto_bet_var.get()
            )
            confirm_enabled = bool(
                hasattr(self, "tg_confirm_var")
                and self.tg_confirm_var is not None
                and self.tg_confirm_var.get()
            )

            if not auto_bet_enabled:
                if confirm_enabled:
                    msg = (
                        f"Segnale ricevuto:\n"
                        f"{selection_name or signal_data.get('event_name', 'Segnale Telegram')}\n"
                        f"Tipo: {action}\n"
                        f"Quota Master: {float(original_price or 0.0):.2f}\n\n"
                        f"Inviare il segnale al runtime?"
                    )
                    if not messagebox.askyesno("Nuovo Segnale Telegram", msg):
                        self._safe_db_save_received_signal(
                            selection=selection_name,
                            action=action,
                            price=original_price,
                            stake=stake,
                            status="IGNORED",
                            signal=signal_data,
                        )
                        self._safe_refresh_telegram_signals_tree()
                        return
                else:
                    self._safe_db_save_received_signal(
                        selection=selection_name,
                        action=action,
                        price=original_price,
                        stake=stake,
                        status="IGNORED",
                        signal=signal_data,
                    )
                    self._safe_refresh_telegram_signals_tree()
                    return

            def _worker():
                payload, resolution_mode = self._resolve_signal_to_payload(signal_data, stake=stake)
                if not payload:
                    return {
                        "ok": False,
                        "status": "ERROR",
                        "selection_name": selection_name,
                        "action": action,
                        "price": original_price,
                        "stake": stake,
                    }

                payload["raw_signal"] = dict(signal_data or {})
                payload["resolution_mode"] = resolution_mode

                logger.info(
                    "[TelegramModule] Inoltro segnale betting (%s): %s",
                    resolution_mode,
                    sanitize_dict(dict(payload or {})),
                )
                route_target = self._publish_order_signal(payload)
                payload["telegram_route_target"] = route_target
                return {
                    "ok": True,
                    "payload": payload,
                    "selection_name": selection_name,
                    "action": action,
                    "price": original_price,
                    "stake": stake,
                }

            def _on_success(result):
                result = dict(result or {})
                if not bool(result.get("ok")):
                    logger.error(
                        "[TelegramModule] Segnale non risolvibile: %s",
                        sanitize_dict(dict(signal_data or {})),
                    )
                    self._safe_db_save_received_signal(
                        selection=result.get("selection_name", selection_name),
                        action=result.get("action", action),
                        price=result.get("price", original_price),
                        stake=result.get("stake", stake),
                        status="ERROR",
                        signal=signal_data,
                    )
                    self._safe_refresh_telegram_signals_tree()
                    return

                payload = dict(result.get("payload") or {})
                self._safe_db_save_received_signal(
                    selection=payload.get("runner_name", selection_name),
                    action=payload.get("bet_type", action),
                    price=payload.get("price", original_price),
                    stake=payload.get("stake", stake),
                    status="SUBMITTED",
                    signal={**signal_data, "resolved_payload": payload},
                )
                self._safe_refresh_telegram_signals_tree()

            def _on_error(exc):
                e = exc
                logger.exception("[TelegramModule] Errore publish ordine: %s", e)
                self._safe_db_save_received_signal(
                    selection=selection_name,
                    action=action,
                    price=original_price,
                    stake=stake,
                    status="ERROR",
                    signal=signal_data,
                )
                self._safe_refresh_telegram_signals_tree()

            self._submit_non_blocking(
                "telegram_signal_resolution",
                _worker,
                _on_success,
                _on_error,
            )

        if hasattr(self, "uiq") and self.uiq:
            self.uiq.post(safe_process_signal)
        else:
            safe_process_signal()

    # =========================================================
    # STATUS
    # =========================================================
    def _update_telegram_status(self, status, message):
        self.telegram_status = status
        color = COLORS["success"] if status == "LISTENING" else COLORS["error"]

        if hasattr(self, "tg_status_label") and self.tg_status_label.winfo_exists():
            self.tg_status_label.configure(
                text=f"Stato: {status} - {message}",
                text_color=color,
            )

    # =========================================================
    # CHATS TREE
    # =========================================================
    def _refresh_telegram_chats_tree(self):
        if not hasattr(self, "tg_chats_tree") or not self.tg_chats_tree.winfo_exists():
            return

        self.tg_chats_tree.delete(*self.tg_chats_tree.get_children())
        chats = self.db.get_telegram_chats()

        for chat in chats:
            state = "Sì" if chat.get("is_active") else "No"
            title = chat.get("title") or chat.get("username") or str(chat.get("chat_id"))
            self.tg_chats_tree.insert(
                "",
                tk.END,
                iid=str(chat["chat_id"]),
                values=(title, state),
            )

    def _remove_telegram_chat(self):
        selected = getattr(self, "tg_chats_tree", None) and self.tg_chats_tree.selection()
        if not selected:
            return

        chats = self.db.get_telegram_chats()
        updated_chats = [c for c in chats if str(c["chat_id"]) not in selected]
        self.db.replace_telegram_chats(updated_chats)
        self._safe_refresh_telegram_chats_tree()

    def _add_selected_available_chats(self):
        selected = getattr(self, "tg_available_tree", None) and self.tg_available_tree.selection()
        if not selected:
            return

        for item_id in selected:
            item = self.tg_available_tree.item(item_id)
            values = item.get("values", [])
            name = values[2] if len(values) > 2 else str(item_id)
            self.db.save_telegram_chat(
                chat_id=item_id,
                title=name,
                is_active=True,
            )

        self._safe_refresh_telegram_chats_tree()
        messagebox.showinfo("Successo", "Chat aggiunte al monitoraggio.")

    # =========================================================
    # ADVANCED SIGNAL PATTERNS
    # =========================================================
    def _open_pattern_form(self, current: dict | None = None) -> dict | None:
        """Apre il form modale _PatternDialog e restituisce il payload, o None se annullato."""
        dlg = _PatternDialog(self, current=current)
        self.wait_window(dlg)
        return dlg.result

    def _minute_range_text(self, rule):
        a = rule.get("min_minute")
        b = rule.get("max_minute")
        if a is None and b is None:
            return "-"
        return f"{'' if a is None else a}-{'' if b is None else b}"

    def _score_range_text(self, rule):
        a = rule.get("min_score")
        b = rule.get("max_score")
        if a is None and b is None:
            return "-"
        return f"{'' if a is None else a}-{'' if b is None else b}"

    def _refresh_rules_tree(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        self.rules_tree.delete(*self.rules_tree.get_children())
        rules = self.db.get_signal_patterns()

        for rule in rules:
            state = "Sì" if rule.get("enabled") else "No"
            self.rules_tree.insert(
                "",
                tk.END,
                iid=str(rule["id"]),
                values=(
                    state,
                    rule.get("label", ""),
                    rule.get("market_type", "MATCH_ODDS"),
                    rule.get("bet_side", ""),
                    rule.get("selection_template", ""),
                    self._minute_range_text(rule),
                    self._score_range_text(rule),
                    "Sì" if rule.get("live_only") else "No",
                    rule.get("priority", 100),
                    rule.get("pattern", ""),
                ),
            )

    def _add_signal_pattern(self):
        payload = self._open_pattern_form()
        if not payload:
            return

        try:
            self.db.save_signal_pattern(
                pattern=payload["pattern"],
                label=payload["label"],
                enabled=payload.get("enabled", True),
                bet_side=payload["bet_side"],
                market_type=payload["market_type"],
                selection_template=payload["selection_template"],
                min_minute=payload["min_minute"],
                max_minute=payload["max_minute"],
                min_score=payload["min_score"],
                max_score=payload["max_score"],
                live_only=payload["live_only"],
                priority=payload["priority"],
                extra={"keyword": payload.get("keyword", "")},
            )
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola aggiunta correttamente.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore add_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile aggiungere la regola: {e}")

    def _edit_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da modificare.")
            return

        pattern_id = selected[0]
        rules = self.db.get_signal_patterns()
        current = next((r for r in rules if str(r["id"]) == str(pattern_id)), None)

        if not current:
            messagebox.showerror("Errore", "Regola non trovata nel database.")
            return

        payload = self._open_pattern_form(current=current)
        if not payload:
            return

        try:
            self.db.update_signal_pattern(
                pattern_id=int(pattern_id),
                pattern=payload["pattern"],
                label=payload["label"],
                enabled=payload.get("enabled", True),
                bet_side=payload["bet_side"],
                market_type=payload["market_type"],
                selection_template=payload["selection_template"],
                min_minute=payload["min_minute"],
                max_minute=payload["max_minute"],
                min_score=payload["min_score"],
                max_score=payload["max_score"],
                live_only=payload["live_only"],
                priority=payload["priority"],
                extra={"keyword": payload.get("keyword", "")},
            )
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola aggiornata correttamente.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore edit_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile aggiornare la regola: {e}")

    def _delete_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da eliminare.")
            return

        pattern_id = selected[0]

        if not messagebox.askyesno("Conferma eliminazione", "Vuoi davvero eliminare la regola selezionata?"):
            return

        try:
            self.db.delete_signal_pattern(int(pattern_id))
            self._safe_refresh_rules_tree()
            messagebox.showinfo("Successo", "Regola eliminata.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore delete_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile eliminare la regola: {e}")

    def _toggle_signal_pattern(self):
        if not hasattr(self, "rules_tree") or not self.rules_tree.winfo_exists():
            return

        selected = self.rules_tree.selection()
        if not selected:
            messagebox.showwarning("Attenzione", "Seleziona una regola da attivare/disattivare.")
            return

        pattern_id = selected[0]

        try:
            new_state = self.db.toggle_signal_pattern(int(pattern_id))
            self._safe_refresh_rules_tree()
            stato_txt = "attivata" if new_state else "disattivata"
            messagebox.showinfo("Successo", f"Regola {stato_txt}.")
        except Exception as e:
            logger.exception("[TelegramModule] Errore toggle_signal_pattern: %s", e)
            messagebox.showerror("Errore", f"Impossibile cambiare stato della regola: {e}")

    # =========================================================
    # SIGNALS TREE
    # =========================================================
    def _refresh_telegram_signals_tree(self):
        if not hasattr(self, "tg_signals_tree") or not self.tg_signals_tree.winfo_exists():
            return

        self.tg_signals_tree.delete(*self.tg_signals_tree.get_children())

        if hasattr(self.db, "get_received_signals"):
            signals = self.db.get_received_signals(limit=50)
            for sig in signals:
                date_str = str(sig.get("received_at", ""))[:16]
                sel = sig.get("selection", "")
                action = sig.get("action", "")
                price = f"{float(sig.get('price', 0) or 0):.2f}"
                stake = f"{float(sig.get('stake', 0) or 0):.2f}"
                status = sig.get("status", "")

                tag = ""
                if status in ("RECEIVED", "SUBMITTED"):
                    tag = "success"
                elif status in ("ERROR", "IGNORED"):
                    tag = "failed"

                self.tg_signals_tree.insert(
                    "",
                    tk.END,
                    values=(date_str, sel, action, price, stake, status),
                    tags=(tag,) if tag else (),
                )
