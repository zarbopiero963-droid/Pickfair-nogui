from __future__ import annotations

__all__ = ["TelegramTabUI"]

import tkinter as tk
from tkinter import ttk

import customtkinter as ctk

from theme import COLORS, FONTS


class TelegramTabUI:
    def __init__(self, parent_frame, app):
        self.parent = parent_frame
        self.app = app
        self.build()

    def _safe_settings(self):
        getter = getattr(self.app.db, "get_telegram_settings", None)
        if callable(getter):
            try:
                return getter() or {}
            except Exception:
                return {}
        return {}

    def build(self):
        main_frame = ctk.CTkFrame(self.parent, fg_color="transparent")
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        left_container = ctk.CTkFrame(main_frame, fg_color="transparent", width=530)
        left_container.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 10))
        left_container.pack_propagate(False)

        left_canvas = tk.Canvas(
            left_container,
            highlightthickness=0,
            bg=COLORS["bg_dark"],
        )
        left_scrollbar = ttk.Scrollbar(
            left_container,
            orient=tk.VERTICAL,
            command=left_canvas.yview,
        )
        left_frame = ctk.CTkFrame(left_canvas, fg_color="transparent")

        left_frame.bind(
            "<Configure>",
            lambda e: left_canvas.configure(scrollregion=left_canvas.bbox("all")),
        )
        left_canvas.create_window((0, 0), window=left_frame, anchor="nw")
        left_canvas.configure(yscrollcommand=left_scrollbar.set)

        left_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        left_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        right_frame = ctk.CTkFrame(main_frame, fg_color="transparent")
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)

        settings = self._safe_settings()

        config_frame = ctk.CTkFrame(left_frame, fg_color=COLORS["bg_panel"], corner_radius=8)
        config_frame.pack(fill=tk.X, pady=(0, 5), padx=5)

        ctk.CTkLabel(
            config_frame,
            text="Configurazione Telegram",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(10, 5))

        ctk.CTkLabel(
            config_frame,
            text="Ottieni API ID e Hash su my.telegram.org",
            font=("Segoe UI", 8),
            text_color=COLORS["text_tertiary"],
        ).pack(anchor=tk.W, padx=10)

        self.app.tg_api_id_var = tk.StringVar(value=settings.get("api_id", ""))
        self.app.tg_api_hash_var = tk.StringVar(value=settings.get("api_hash", ""))
        self.app.tg_phone_var = tk.StringVar(value=settings.get("phone_number", ""))
        self.app.tg_auto_stake_var = tk.StringVar(value=str(settings.get("auto_stake", "1.0")))
        self.app.tg_auto_bet_var = tk.BooleanVar(value=bool(settings.get("auto_bet", False)))
        self.app.tg_confirm_var = tk.BooleanVar(value=bool(settings.get("require_confirmation", True)))
        self.app.tg_code_var = tk.StringVar()
        self.app.tg_2fa_var = tk.StringVar()

        ctk.CTkLabel(config_frame, text="API ID:", text_color=COLORS["text_secondary"]).pack(anchor=tk.W, padx=10, pady=(5, 0))
        ctk.CTkEntry(
            config_frame,
            textvariable=self.app.tg_api_id_var,
            width=200,
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(anchor=tk.W, padx=10)

        ctk.CTkLabel(config_frame, text="API Hash:", text_color=COLORS["text_secondary"]).pack(anchor=tk.W, padx=10, pady=(5, 0))
        ctk.CTkEntry(
            config_frame,
            textvariable=self.app.tg_api_hash_var,
            width=260,
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(anchor=tk.W, padx=10)

        ctk.CTkLabel(config_frame, text="Numero di Telefono (+39...)", text_color=COLORS["text_secondary"]).pack(anchor=tk.W, padx=10, pady=(5, 0))
        ctk.CTkEntry(
            config_frame,
            textvariable=self.app.tg_phone_var,
            width=180,
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(anchor=tk.W, padx=10)

        ctk.CTkLabel(config_frame, text="Stake Automatico (EUR)", text_color=COLORS["text_secondary"]).pack(anchor=tk.W, padx=10, pady=(5, 0))
        ctk.CTkEntry(
            config_frame,
            textvariable=self.app.tg_auto_stake_var,
            width=80,
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(anchor=tk.W, padx=10)

        ctk.CTkCheckBox(
            config_frame,
            text="Piazza automaticamente",
            variable=self.app.tg_auto_bet_var,
            fg_color=COLORS["back"],
            hover_color=COLORS["back_hover"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(5, 0))

        ctk.CTkCheckBox(
            config_frame,
            text="Richiedi conferma (solo se auto OFF)",
            variable=self.app.tg_confirm_var,
            fg_color=COLORS["back"],
            hover_color=COLORS["back_hover"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10)

        auth_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        auth_frame.pack(fill=tk.X, padx=10, pady=(5, 0))

        ctk.CTkLabel(auth_frame, text="Codice:", text_color=COLORS["text_secondary"]).pack(side=tk.LEFT)
        ctk.CTkEntry(
            auth_frame,
            textvariable=self.app.tg_code_var,
            width=70,
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(side=tk.LEFT, padx=4)

        ctk.CTkLabel(auth_frame, text="2FA:", text_color=COLORS["text_secondary"]).pack(side=tk.LEFT, padx=(10, 0))
        ctk.CTkEntry(
            auth_frame,
            textvariable=self.app.tg_2fa_var,
            width=80,
            show="*",
            fg_color=COLORS["bg_card"],
            border_color=COLORS["border"],
        ).pack(side=tk.LEFT, padx=4)

        ctk.CTkButton(
            auth_frame,
            text="Invia Codice",
            command=self.app.telegram_controller.send_code,
            fg_color=COLORS["button_secondary"],
            hover_color=COLORS["bg_hover"],
            corner_radius=6,
            width=90,
        ).pack(side=tk.LEFT, padx=5)

        ctk.CTkButton(
            auth_frame,
            text="Verifica",
            command=self.app.telegram_controller.verify_code,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            corner_radius=6,
            width=70,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            auth_frame,
            text="Reset Sessione",
            command=self.app.telegram_controller.reset_session,
            fg_color=COLORS["button_danger"],
            hover_color="#c62828",
            corner_radius=6,
            width=100,
        ).pack(side=tk.LEFT, padx=5)

        self.app.tg_status_label = ctk.CTkLabel(
            config_frame,
            text=f"Stato: {getattr(self.app, 'telegram_status', 'STOPPED')}",
            text_color=COLORS["text_secondary"],
        )
        self.app.tg_status_label.pack(anchor=tk.W, padx=10, pady=5)

        btn_frame = ctk.CTkFrame(config_frame, fg_color="transparent")
        btn_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        ctk.CTkButton(
            btn_frame,
            text="Salva",
            command=self.app.telegram_controller.save_settings,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            corner_radius=6,
            width=80,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            btn_frame,
            text="Avvia Listener",
            command=self.app._start_telegram_listener,
            fg_color=COLORS["button_success"],
            hover_color="#4caf50",
            corner_radius=6,
            width=100,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            btn_frame,
            text="Ferma",
            command=self.app._stop_telegram_listener,
            fg_color=COLORS["button_danger"],
            hover_color="#c62828",
            corner_radius=6,
            width=70,
        ).pack(side=tk.LEFT, padx=2)

        chats_frame = ctk.CTkFrame(left_frame, fg_color=COLORS["bg_panel"], corner_radius=8)
        chats_frame.pack(fill=tk.X, pady=(0, 5), padx=5)

        ctk.CTkLabel(
            chats_frame,
            text="Chat Monitorate",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(10, 5))

        chat_btn_frame = ctk.CTkFrame(chats_frame, fg_color="transparent")
        chat_btn_frame.pack(fill=tk.X, padx=10, pady=(0, 5))

        ctk.CTkButton(
            chat_btn_frame,
            text="Rimuovi",
            command=self.app._remove_telegram_chat,
            fg_color=COLORS["button_danger"],
            hover_color="#c62828",
            corner_radius=6,
            width=80,
        ).pack(side=tk.LEFT, padx=2)

        self.app.tg_chats_tree = ttk.Treeview(
            chats_frame,
            columns=("name", "enabled"),
            show="headings",
            height=4,
        )
        self.app.tg_chats_tree.heading("name", text="Nome Chat")
        self.app.tg_chats_tree.heading("enabled", text="Attivo")
        self.app.tg_chats_tree.column("name", width=220)
        self.app.tg_chats_tree.column("enabled", width=60)
        self.app.tg_chats_tree.pack(fill=tk.X, padx=10, pady=(0, 10))

        if hasattr(self.app, "_refresh_telegram_chats_tree"):
            self.app._refresh_telegram_chats_tree()

        available_frame = ctk.CTkFrame(left_frame, fg_color=COLORS["bg_panel"], corner_radius=8)
        available_frame.pack(fill=tk.X, pady=(0, 5), padx=5)

        ctk.CTkLabel(
            available_frame,
            text="Chat Disponibili da Telegram",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(10, 5))

        avail_btn_frame = ctk.CTkFrame(available_frame, fg_color="transparent")
        avail_btn_frame.pack(fill=tk.X, padx=10, pady=(0, 5))

        ctk.CTkButton(
            avail_btn_frame,
            text="Carica/Aggiorna Chat",
            command=self.app.telegram_controller.load_dialogs,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            corner_radius=6,
            width=140,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            avail_btn_frame,
            text="Aggiungi Selezionate",
            command=self.app._add_selected_available_chats,
            fg_color=COLORS["button_success"],
            hover_color="#4caf50",
            corner_radius=6,
            width=140,
        ).pack(side=tk.LEFT, padx=2)

        self.app.tg_available_status = ctk.CTkLabel(
            avail_btn_frame,
            text="",
            text_color=COLORS["text_secondary"],
        )
        self.app.tg_available_status.pack(side=tk.RIGHT, padx=5)

        avail_tree_container = ctk.CTkFrame(available_frame, fg_color="transparent")
        avail_tree_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.app.tg_available_tree = ttk.Treeview(
            avail_tree_container,
            columns=("select", "type", "name"),
            show="headings",
            height=8,
            selectmode="extended",
        )
        self.app.tg_available_tree.heading("select", text="")
        self.app.tg_available_tree.heading("type", text="Tipo")
        self.app.tg_available_tree.heading("name", text="Nome")
        self.app.tg_available_tree.column("select", width=30)
        self.app.tg_available_tree.column("type", width=60)
        self.app.tg_available_tree.column("name", width=220)

        avail_scroll = ttk.Scrollbar(avail_tree_container, orient=tk.VERTICAL, command=self.app.tg_available_tree.yview)
        self.app.tg_available_tree.configure(yscrollcommand=avail_scroll.set)
        self.app.tg_available_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        avail_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.app.available_chats_data = []

        rules_frame = ctk.CTkFrame(left_frame, fg_color=COLORS["bg_panel"], corner_radius=8)
        rules_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 5), padx=5)

        ctk.CTkLabel(
            rules_frame,
            text="Regole di Parsing",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(10, 5))

        ctk.CTkLabel(
            rules_frame,
            text="Regex + market/side/template + filtri minuto/score/live/priority",
            font=("Segoe UI", 8),
            text_color=COLORS["text_tertiary"],
        ).pack(anchor=tk.W, padx=10)

        rules_btn_frame = ctk.CTkFrame(rules_frame, fg_color="transparent")
        rules_btn_frame.pack(fill=tk.X, padx=10, pady=(5, 5))

        ctk.CTkButton(
            rules_btn_frame,
            text="Aggiungi",
            command=self.app._add_signal_pattern,
            fg_color=COLORS["button_success"],
            hover_color="#4caf50",
            corner_radius=6,
            width=80,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            rules_btn_frame,
            text="Modifica",
            command=self.app._edit_signal_pattern,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            corner_radius=6,
            width=80,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            rules_btn_frame,
            text="Elimina",
            command=self.app._delete_signal_pattern,
            fg_color=COLORS["button_danger"],
            hover_color="#c62828",
            corner_radius=6,
            width=80,
        ).pack(side=tk.LEFT, padx=2)

        ctk.CTkButton(
            rules_btn_frame,
            text="Attiva/Disattiva",
            command=self.app._toggle_signal_pattern,
            fg_color=COLORS["button_secondary"],
            hover_color=COLORS["bg_hover"],
            corner_radius=6,
            width=120,
        ).pack(side=tk.LEFT, padx=2)

        rules_tree_container = ctk.CTkFrame(rules_frame, fg_color="transparent")
        rules_tree_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.app.rules_tree = ttk.Treeview(
            rules_tree_container,
            columns=(
                "enabled",
                "name",
                "market",
                "side",
                "template",
                "minute",
                "score",
                "live",
                "priority",
                "pattern",
            ),
            show="headings",
            height=10,
        )
        self.app.rules_tree.heading("enabled", text="ON")
        self.app.rules_tree.heading("name", text="Nome")
        self.app.rules_tree.heading("market", text="Mercato")
        self.app.rules_tree.heading("side", text="Side")
        self.app.rules_tree.heading("template", text="Template")
        self.app.rules_tree.heading("minute", text="Minuti")
        self.app.rules_tree.heading("score", text="Score")
        self.app.rules_tree.heading("live", text="Live")
        self.app.rules_tree.heading("priority", text="Prio")
        self.app.rules_tree.heading("pattern", text="Pattern")

        self.app.rules_tree.column("enabled", width=40, anchor="center")
        self.app.rules_tree.column("name", width=110)
        self.app.rules_tree.column("market", width=110)
        self.app.rules_tree.column("side", width=55, anchor="center")
        self.app.rules_tree.column("template", width=140)
        self.app.rules_tree.column("minute", width=70, anchor="center")
        self.app.rules_tree.column("score", width=70, anchor="center")
        self.app.rules_tree.column("live", width=50, anchor="center")
        self.app.rules_tree.column("priority", width=50, anchor="center")
        self.app.rules_tree.column("pattern", width=220)

        rules_scroll = ttk.Scrollbar(rules_tree_container, orient=tk.VERTICAL, command=self.app.rules_tree.yview)
        self.app.rules_tree.configure(yscrollcommand=rules_scroll.set)
        self.app.rules_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        rules_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        if hasattr(self.app, "_refresh_rules_tree"):
            self.app._refresh_rules_tree()

        signals_frame = ctk.CTkFrame(right_frame, fg_color=COLORS["bg_panel"], corner_radius=8)
        signals_frame.pack(fill=tk.BOTH, expand=True)

        ctk.CTkLabel(
            signals_frame,
            text="Segnali Ricevuti",
            font=FONTS["heading"],
            text_color=COLORS["text_primary"],
        ).pack(anchor=tk.W, padx=10, pady=(10, 5))

        signals_tree_container = ctk.CTkFrame(signals_frame, fg_color="transparent")
        signals_tree_container.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.app.tg_signals_tree = ttk.Treeview(
            signals_tree_container,
            columns=("data", "selezione", "tipo", "quota", "stake", "stato"),
            show="headings",
            height=15,
        )
        self.app.tg_signals_tree.heading("data", text="Data")
        self.app.tg_signals_tree.heading("selezione", text="Selezione")
        self.app.tg_signals_tree.heading("tipo", text="Tipo")
        self.app.tg_signals_tree.heading("quota", text="Quota")
        self.app.tg_signals_tree.heading("stake", text="Stake")
        self.app.tg_signals_tree.heading("stato", text="Stato")
        self.app.tg_signals_tree.column("data", width=110)
        self.app.tg_signals_tree.column("selezione", width=180)
        self.app.tg_signals_tree.column("tipo", width=60)
        self.app.tg_signals_tree.column("quota", width=60)
        self.app.tg_signals_tree.column("stake", width=60)
        self.app.tg_signals_tree.column("stato", width=100)

        self.app.tg_signals_tree.tag_configure("success", foreground=COLORS["success"])
        self.app.tg_signals_tree.tag_configure("failed", foreground=COLORS["loss"])

        scrollbar = ttk.Scrollbar(signals_tree_container, orient=tk.VERTICAL, command=self.app.tg_signals_tree.yview)
        self.app.tg_signals_tree.configure(yscrollcommand=scrollbar.set)
        self.app.tg_signals_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        ctk.CTkButton(
            signals_frame,
            text="Aggiorna Segnali",
            command=self.app._refresh_telegram_signals_tree,
            fg_color=COLORS["button_primary"],
            hover_color=COLORS["back_hover"],
            corner_radius=6,
        ).pack(pady=10)

        if hasattr(self.app, "_refresh_telegram_signals_tree"):
            self.app._refresh_telegram_signals_tree()