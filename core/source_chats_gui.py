"""PR-13b/13c: vista customtkinter (sottile) dell'editor delle sorgenti multi-chat.

Tutta la logica sta nel controller `source_editor.SourceEditor` (testato in CI) e in
`source_manager` (validazione); qui ci sono SOLO i widget. La finestra si apre da un
pulsante nella GUI principale (`app.App`). Permette di aggiungere/rimuovere sorgenti
`source_chats` (nome, chat_id, attiva, modalità PRE/LIVE, provider) e di assegnare a
ciascuna un **Parser Personalizzato** dedicato (override `parser_by_chat`, PR-13c),
salvando in `config.json` senza editare il file a mano.

Testi UI localizzati via `i18n.tr` (#343 slice 4e) — SOLO la chrome di display:
titolo, hint, intestazioni colonne, bottoni, messaggi di stato GUI-composti. Restano
in italiano (FUORI SCOPE, per non toccare logica safety-critical o contratti di test):
- la sentinella `(predefinito)` (`_NO_PARSER_BASE`): è usata in confronti di
  UGUAGLIANZA (`_effective_parser_name`, `_save`) e come chiave di override, NON è
  semplice testo;
- l'helper puro `_translations_chip_text` (chip «Nomi ✓ · Mercati —»): asserito
  VERBATIM in più test CI e vocabolario condiviso con `config_summary_gui`;
- i messaggi d'errore/warning bubblati da `editor.apply()` (layer di dominio).

NB: questo modulo non è testato in CI (richiede un display). La logica che usa è
coperta da `tests/unit/test_source_editor.py`. Verifica manuale su Windows.
"""

import customtkinter as ctk

from . import config_store, config_summary, custom_parser, gui_utils, i18n
from .source_editor import SourceEditor, _clean_names

# Chip «Traduzioni» per canale (#293 slice 6): mostra se il parser del canale ha mappature
# nomi/mercati RISOLTE attive. Colori theme-aware (light, dark): verde se almeno una attiva.
_CHIP_ON_COLOR = ("#2e7d32", "#66bb6a")
_CHIP_OFF_COLOR = "gray"


def _effective_parser_name(selected: str, no_parser_sentinel: str, global_parser: str) -> str:
    """Parser effettivo per una riga di Chat sorgenti: l'override SELEZIONATO se presente,
    altrimenti — se «(predefinito)» (sentinella «nessun override») — il parser GLOBALE
    (`active_parser`). Puro/testabile: è l'unica logica di branch specifica del chip (CodeRabbit
    #340). Il globale viene rifilato; nessun override e nessun globale → "" (nessun parser)."""
    name = "" if selected == no_parser_sentinel else selected
    return name or str(global_parser or "").strip()


def _translations_chip_text(names_active: bool, markets_active: bool) -> str:
    """Testo del chip «Traduzioni» di un canale (#293 slice 6): «Nomi ✓ · Mercati ✓» a seconda
    che il parser del canale abbia mappature nomi/mercati **risolte** attive; «—» dove nessuna.
    Puro/testabile: non dipende dalla GUI."""
    nomi = "Nomi ✓" if names_active else "Nomi —"
    mercati = "Mercati ✓" if markets_active else "Mercati —"
    return f"{nomi} · {mercati}"


def _parsers_summary(names, no_parser_text: str) -> str:
    """PR-2: testo riassuntivo della LISTA di parser di una riga per il display (label).
    Lista vuota → `no_parser_text` («(predefinito)» = usa il globale). Uno o più nomi →
    numerati in ordine di priorità, es. «1. A · 2. B». Riusa `_clean_names` (fonte unica di
    strip+dedup, così le due logiche non divergono — CodeRabbit #391). Puro/testabile."""
    clean = _clean_names(names)
    if not clean:
        return no_parser_text
    return " · ".join(f"{i}. {n}" for i, n in enumerate(clean, 1))

# Etichetta base della voce "nessun override per-chat" (= "" in parser_by_chat): la
# chat usa il parser GLOBALE (`active_parser`, via resolve_parser_name), da cui
# "(predefinito)". NON significa "nessun parser": se un parser globale è attivo, la chat
# è comunque parsata da quello (Codex). Se nemmeno il globale è impostato, allora — col
# parser automatico P.Bet disattivato (CP-09b) — la chat non viene processata.
_NO_PARSER_BASE = "(predefinito)"


def _none_sentinel(names) -> str:
    """Sentinella "nessuno" GARANTITA diversa da ogni nome di parser reale: se per
    assurdo un parser si chiama "(predefinito)", aggiunge spazi finché è unica. Evita la
    collisione che renderebbe ambiguo "nessun override" vs il parser omonimo (Codex)."""
    existing = set(names or [])
    label = _NO_PARSER_BASE
    while label in existing:
        label += " "
    return label


class SourceChatsPanel(ctk.CTkFrame):
    """Pannello editor delle sorgenti multi-chat — incassabile in finestra standalone
    (`SourceChatsWindow`) o come scheda della finestra "🧰 Strumenti".

    `on_saved(new_cfg)`: callback opzionale chiamata dopo un salvataggio riuscito,
    così la GUI principale può aggiornare la propria config in memoria."""

    @staticmethod
    def _no_parser_label(real_names, override_values=()) -> str:
        """Etichetta-sentinella "nessun override" GARANTITA diversa dai nomi reali dei parser
        E dai valori di override attualmente tenuti dalle righe — inclusi quelli "danglanti"
        verso un parser CANCELLATO. Così la sentinella non collide mai col valore di una riga:
        un override verso un parser rimosso (es. un parser chiamato "(predefinito)") NON viene
        scambiato per "nessun override" e azzerato al salvataggio (Codex #97). I valori vuoti
        (= nessun override) sono ignorati. Logica pura, testata in CI."""
        avoid = list(real_names) + [str(v).strip() for v in override_values if str(v).strip()]
        return _none_sentinel(avoid)

    def __init__(self, master=None, on_saved=None):
        super().__init__(master)
        self._on_saved = on_saved
        # Snapshot config per i chip «Traduzioni» (#293 slice 6): profili di mappatura esistenti
        # + parser globale. Aggiornato in refresh()/refresh_options().
        self._cfg = config_store.load_config(config_store.CONFIG_FILE)
        self._editor = SourceEditor(self._cfg)
        self._modes = self._editor.mode_options()
        # Nomi reali dei parser + sentinella "nessuno" unica (non collide mai con un nome
        # reale NÉ con un override tenuto da una riga, anche danglante). Menu: sentinella, poi i nomi.
        self._parser_names = self._editor.parser_options()
        self._no_parser = self._no_parser_label(
            self._parser_names, [s.get("parser", "") for s in self._editor.sources])
        self._rows = []   # widget refs per sorgente
        self._build_ui()
        for src in self._editor.sources:
            self._add_row(src)

    def refresh(self):
        """Ricarica editor e righe dalla config su disco.

        Da chiamare quando la config cambia da FUORI questo pannello (es. un profilo
        applicato nella stessa finestra "🧰 Strumenti"): senza, un Salva successivo
        riscriverebbe le `source_chats` STANTIE sopra il profilo appena caricato,
        indebolendo il filtro chat (Codex P1). Le modifiche non salvate vengono scartate:
        il profilo appena caricato è la nuova verità."""
        self._cfg = config_store.load_config(config_store.CONFIG_FILE)
        self._editor = SourceEditor(self._cfg)
        self._modes = self._editor.mode_options()
        self._parser_names = self._editor.parser_options()
        self._no_parser = self._no_parser_label(
            self._parser_names, [s.get("parser", "") for s in self._editor.sources])
        for refs in self._rows:
            refs["frame"].destroy()
        self._rows = []
        for src in self._editor.sources:
            self._add_row(src)

    def refresh_options(self):
        """Aggiorna SOLO le liste-opzioni dei dropdown (i parser disponibili) sulle righe
        esistenti, SENZA ricostruirle: le modifiche in corso restano. Chiamato quando questa
        scheda torna attiva nella hub, così un parser appena creato nella scheda Parser
        compare subito nel menu Parser di ogni riga (Codex). Best-effort."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
            editor = SourceEditor(cfg)
        except Exception:               # noqa: BLE001 — config illeggibile: niente refresh
            return
        self._cfg = cfg                 # snapshot fresco per i chip «Traduzioni» (#293 slice 6)
        old_no_parser = self._no_parser
        self._parser_names = editor.parser_options()
        # Nuova sentinella unica vs nomi reali E override tenuti dalle righe (valore != vecchia
        # sentinella). Così se un parser reale omonimo alla sentinella viene CANCELLATO mentre
        # una riga lo tiene come override, la sentinella NON collassa sul suo valore e quell'override
        # non viene azzerato al salvataggio (resta fail-closed verso il parser mancante, Codex #97).
        self._no_parser = self._no_parser_label(
            self._parser_names,
            [refs["parser"].get() for refs in self._rows if refs["parser"].get() != old_no_parser])
        for refs in self._rows:
            var = refs["parser"]
            # Se il sentinella "nessun override" è cambiato (es. è stato creato un parser
            # con lo stesso nome del vecchio sentinella, e `_none_sentinel` lo disambigua),
            # migra le righe che erano su "no override" al NUOVO sentinella: senza, un Save
            # le salverebbe come override al parser reale omonimo, cambiando in silenzio il
            # parser usato per quelle chat (Codex).
            if old_no_parser != self._no_parser and var.get() == old_no_parser:
                var.set(self._no_parser)
            # PR-2: aggiorna il testo del bottone lista-parser (la sentinella «(predefinito)»
            # per la lista vuota può essere cambiata → il riassunto va rifatto).
            btn = refs.get("parser_btn")
            if btn is not None:
                btn.configure(text=_parsers_summary(refs.get("parsers", []), self._no_parser))
            self._update_row_chip(refs)     # rifletti mappature/parser aggiornati nel chip

    # ── costruzione UI ─────────────────────────────────────────────────────
    def _build_ui(self):
        ctk.CTkLabel(
            self, text=i18n.tr("📡  Chat sorgenti (multi-chat)"),
            font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=12, pady=(10, 2))
        ctk.CTkLabel(
            self, text=i18n.tr("Ogni sorgente è una chat/canale da cui accettare segnali. "
                               "chat_id obbligatorio e univoco; una sorgente disattivata "
                               "viene ignorata."),
            font=ctk.CTkFont(size=11), text_color="gray", wraplength=860,
            anchor="w", justify="left").pack(anchor="w", padx=12, pady=(0, 6))

        # Intestazione colonne (i titoli sono display: i18n.tr al momento della
        # costruzione della tupla, così l'anti-drift AST li riconosce come costanti).
        head = ctk.CTkFrame(self, fg_color="transparent")
        head.pack(fill="x", padx=12)
        for text, w in ((i18n.tr("Attiva"), 60), (i18n.tr("Nome"), 180),
                        (i18n.tr("Chat ID"), 160), (i18n.tr("Modalità"), 100),
                        (i18n.tr("Provider"), 150), (i18n.tr("Parser"), 160),
                        (i18n.tr("Traduzioni"), 150), ("", 40)):
            ctk.CTkLabel(head, text=text, width=w, anchor="w",
                         font=ctk.CTkFont(size=11, weight="bold")).pack(side="left", padx=3)

        self._rows_frame = ctk.CTkScrollableFrame(self, height=320)
        self._rows_frame.pack(fill="both", expand=True, padx=12, pady=6)

        btns = ctk.CTkFrame(self, fg_color="transparent")
        btns.pack(fill="x", padx=12, pady=(0, 4))
        ctk.CTkButton(btns, text=i18n.tr("➕  Aggiungi sorgente"), width=180,
                      command=lambda: self._add_row()).pack(side="left", padx=4)
        ctk.CTkButton(btns, text=i18n.tr("💾  Salva"), width=140, fg_color="#2e7d32",
                      hover_color="#1b5e20", command=self._save).pack(side="right", padx=4)

        self._status = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=11),
                                    text_color="gray", wraplength=860, anchor="w", justify="left")
        self._status.pack(fill="x", padx=12, pady=(0, 10))

    def _add_row(self, source: dict = None):
        source = source or {}
        row = ctk.CTkFrame(self._rows_frame, fg_color="transparent")
        row.pack(fill="x", pady=2)
        enabled = ctk.BooleanVar(value=bool(source.get("enabled", True)))
        ctk.CTkCheckBox(row, text="", width=60, variable=enabled).pack(side="left", padx=3)
        name = ctk.CTkEntry(row, width=180)
        name.insert(0, str(source.get("name", "")))
        name.pack(side="left", padx=3)
        chat_id = ctk.CTkEntry(row, width=160)
        chat_id.insert(0, str(source.get("chat_id", "")))
        chat_id.pack(side="left", padx=3)
        mode = ctk.StringVar(value=source.get("mode", self._modes[0] if self._modes else "PRE"))
        ctk.CTkOptionMenu(row, width=100, values=self._modes, variable=mode).pack(side="left", padx=3)
        provider = ctk.CTkEntry(row, width=150)
        provider.insert(0, str(source.get("provider", "")))
        provider.pack(side="left", padx=3)
        # PR-2: LISTA ordinata di parser per la chat. La lista vive in `refs["parsers"]`; un
        # bottone col riassunto («1. A · 2. B» oppure «(predefinito)») apre l'editor ordinabile.
        # `refs["parser"]` (Var) resta il PRIMARIO (primo della lista, o la sentinella se vuota):
        # lo usano il chip «Traduzioni» e `refresh_options` (retro-compat, test CI su quegli helper).
        parsers = _clean_names(source.get("parsers")
                               or ([source.get("parser")] if source.get("parser") else []))
        parser = ctk.StringVar(value=(parsers[0] if parsers else self._no_parser))
        parser_btn = ctk.CTkButton(row, width=160, anchor="w",
                                   text=_parsers_summary(parsers, self._no_parser))
        parser_btn.pack(side="left", padx=3)
        # Chip «Traduzioni» (#293 slice 6): mostra a colpo d'occhio se il parser PRIMARIO di
        # questa chat ha mappature nomi/mercati RISOLTE attive. Read-only, si aggiorna al cambio.
        trad_chip = ctk.CTkLabel(row, width=150, anchor="w", font=ctk.CTkFont(size=11))
        trad_chip.pack(side="left", padx=3)
        refs = {"frame": row, "enabled": enabled, "name": name,
                "chat_id": chat_id, "mode": mode, "provider": provider, "parser": parser,
                "parsers": list(parsers), "parser_btn": parser_btn,
                "parser_menu": None, "trad_chip": trad_chip}
        # Il bottone apre l'editor della lista ordinata di parser per QUESTA riga.
        parser_btn.configure(command=lambda r=refs: self._open_parser_list_editor(r))
        self._update_row_chip(refs)
        ctk.CTkButton(row, text="✕", width=40, fg_color="#c62828", hover_color="#7f0000",
                      command=lambda r=refs: self._remove_row(r)).pack(side="left", padx=3)
        self._rows.append(refs)

    def _update_row_chip(self, refs):
        """Aggiorna il chip «Traduzioni» della riga in base al parser selezionato: l'override
        per-chat se presente, altrimenti il parser GLOBALE (voce «(predefinito)»). Read-only:
        legge lo snapshot `self._cfg`. `parser_translation_flags` è fail-safe (parser mancante/
        invalido → nessuna traduzione), quindi non serve un try qui."""
        # «(predefinito)» (sentinella) = nessun override → parser globale (helper puro).
        effective = _effective_parser_name(
            refs["parser"].get(), self._no_parser, self._cfg.get("active_parser", ""))
        names_active, markets_active = config_summary.parser_translation_flags(
            self._cfg, effective, parsers_dir=custom_parser.default_parsers_dir())
        chip = refs.get("trad_chip")
        if chip is not None:
            active = names_active or markets_active
            chip.configure(text=_translations_chip_text(names_active, markets_active),
                           text_color=(_CHIP_ON_COLOR if active else _CHIP_OFF_COLOR))

    def _open_parser_list_editor(self, refs):
        """Apre il popup per gestire la LISTA ordinata di parser di questa riga (PR-2)."""
        _ParserListDialog(self, list(self._parser_names), list(refs.get("parsers", [])),
                          lambda names, r=refs: self._set_parser_list(r, names))

    def _set_parser_list(self, refs, names):
        """Applica la nuova lista di parser a una riga: aggiorna `refs["parsers"]`, il primario
        `refs["parser"]` (per il chip), il testo del bottone e il chip «Traduzioni»."""
        clean = _clean_names(names)
        refs["parsers"] = clean
        refs["parser"].set(clean[0] if clean else self._no_parser)
        btn = refs.get("parser_btn")
        if btn is not None:
            btn.configure(text=_parsers_summary(clean, self._no_parser))
        self._update_row_chip(refs)

    def _remove_row(self, refs):
        refs["frame"].destroy()
        self._rows.remove(refs)

    # ── salvataggio ────────────────────────────────────────────────────────
    def _save(self):
        # Ricostruisce l'editor dallo stato corrente dei widget (niente sync per-campo).
        editor = SourceEditor()
        for r in self._rows:
            # PR-2: la LISTA ordinata di parser della riga (`refs["parsers"]`). Lista vuota =
            # nessun override → usa il parser globale (comportamento «(predefinito)» di prima).
            editor.add_source(name=r["name"].get(), chat_id=r["chat_id"].get(),
                              enabled=r["enabled"].get(), mode=r["mode"].get(),
                              provider=r["provider"].get(),
                              parsers=list(r.get("parsers", [])))
        cfg = config_store.load_config(config_store.CONFIG_FILE)
        new_cfg, errors, warnings = editor.apply(cfg)
        if errors:
            # Gli `errors` vengono dal layer di dominio (`editor.apply`, IT): fuori
            # scope i18n di questa slice; qui si localizza solo la coda GUI-composta.
            self._status.configure(
                text="❌ " + "  ·  ".join(errors) + "\n"
                     + i18n.tr("Niente salvato: correggi gli errori."),
                text_color="#ef5350")
            return
        # Esito reale della persistenza (A1): se la scrittura su disco fallisce NON si
        # deve mostrare "Salvate". Queste sorgenti definiscono le chat ascoltate: un falso
        # "salvato" le farebbe sparire al riavvio cambiando di nascosto il filtro chat.
        _, ok = config_store.save_config(new_cfg, config_store.CONFIG_FILE)
        if not ok:
            self._status.configure(
                text=i18n.tr("❌ Salvataggio su disco FALLITO: sorgenti NON salvate "
                             "(andrebbero perse al riavvio). Controlla permessi/spazio "
                             "del file config."),
                text_color="#ef5350")
            return
        if self._on_saved:
            self._on_saved(new_cfg)
        msg = i18n.tr("✅ Salvate {n} sorgenti in config.json.").format(n=len(self._rows))
        if warnings:
            # `warnings` dal layer di dominio (IT): fuori scope; solo il prefisso è chrome.
            msg += "\n⚠️ " + "  ·  ".join(warnings)
        self._status.configure(text=msg, text_color="#66bb6a")


class _ParserListDialog(ctk.CTkToplevel):
    """PR-2: popup per gestire la LISTA ORDINATA di parser di una chat.

    Mostra i parser scelti in ordine di priorità (↑/↓ per riordinare, ✕ per togliere) e una
    tendina + «➕ Aggiungi parser» per aggiungerne. Al salvataggio richiama `on_ok(nuova_lista)`
    (la vista aggiorna la riga; la persistenza passa comunque dal controller testato). Modulo
    non testato in CI (richiede display): la logica di ordine/dedup vive in `_clean_names`."""

    def __init__(self, master, available, current, on_ok):
        super().__init__(master)
        self.title(i18n.tr("Parser della chat (in ordine di priorità)"))
        gui_utils.fit_to_screen(self, 480, 440, 380, 340)
        # MODALE (CodeRabbit #391): il genitore non deve poter salvare finché questo editor è
        # aperto, altrimenti la riga verrebbe persistita con stato parser stantio. `transient`
        # + `grab_set` costringono a chiudere/salvare qui prima di agire sul genitore.
        try:
            self.transient(master)
            self.grab_set()
        except Exception:               # noqa: BLE001 — best-effort (test headless / stub Tk)
            pass
        self._available = list(available)
        self._names = _clean_names(current)
        self._on_ok = on_ok
        ctk.CTkLabel(
            self, text=i18n.tr("Il messaggio va a ogni parser in ordine; scattano TUTTI quelli "
                               "le cui condizioni combaciano (una riga CSV per parser che scatta)."),
            wraplength=440, justify="left", font=ctk.CTkFont(size=11),
            text_color="gray", anchor="w").pack(anchor="w", padx=12, pady=(10, 6))
        self._list_frame = ctk.CTkScrollableFrame(self, height=220)
        self._list_frame.pack(fill="both", expand=True, padx=12, pady=6)
        add = ctk.CTkFrame(self, fg_color="transparent")
        add.pack(fill="x", padx=12, pady=4)
        # Tendina «aggiungi»: mostra SOLO i parser non ancora nella lista (CodeRabbit #391),
        # così scegliere un già-aggiunto non risulta un no-op silenzioso.
        self._add_var = ctk.StringVar(value="")
        self._add_menu = ctk.CTkOptionMenu(add, width=250, values=[""], variable=self._add_var)
        self._add_menu.pack(side="left", padx=4)
        ctk.CTkButton(add, text=i18n.tr("➕ Aggiungi parser"), width=160,
                      command=self._add).pack(side="left", padx=4)
        ctk.CTkButton(self, text=i18n.tr("💾  Salva"), fg_color="#2e7d32",
                      hover_color="#1b5e20", command=self._ok).pack(side="right", padx=12, pady=8)
        self._render()

    def _remaining(self) -> list:
        """Parser disponibili non ancora nella lista (per la tendina «aggiungi»)."""
        return [n for n in self._available if n not in self._names]

    def _refresh_add_menu(self):
        """Aggiorna valori/selezione della tendina «aggiungi» sui parser ancora disponibili."""
        remaining = self._remaining()
        menu = getattr(self, "_add_menu", None)
        if menu is not None:
            menu.configure(values=(remaining or [""]))
        self._add_var.set(remaining[0] if remaining else "")

    def _render(self):
        self._refresh_add_menu()            # tieni la tendina «aggiungi» in sync con la lista
        for w in list(self._list_frame.winfo_children()):
            w.destroy()
        if not self._names:
            ctk.CTkLabel(self._list_frame, text=i18n.tr("Nessun parser: la chat usa il parser "
                         "globale (predefinito)."), text_color="gray",
                         font=ctk.CTkFont(size=11), anchor="w").pack(anchor="w", padx=6, pady=6)
            return
        for i, name in enumerate(self._names):
            r = ctk.CTkFrame(self._list_frame, fg_color="transparent")
            r.pack(fill="x", pady=2)
            ctk.CTkLabel(r, text=f"{i + 1}.", width=28, anchor="w").pack(side="left", padx=2)
            ctk.CTkLabel(r, text=name, anchor="w").pack(side="left", padx=4, fill="x", expand=True)
            ctk.CTkButton(r, text="↑", width=34,
                          command=lambda idx=i: self._move(idx, -1)).pack(side="left", padx=1)
            ctk.CTkButton(r, text="↓", width=34,
                          command=lambda idx=i: self._move(idx, 1)).pack(side="left", padx=1)
            ctk.CTkButton(r, text="✕", width=34, fg_color="#c62828", hover_color="#7f0000",
                          command=lambda idx=i: self._remove(idx)).pack(side="left", padx=1)

    def _add(self):
        name = str(self._add_var.get() or "").strip()
        if name and name not in self._names:
            self._names.append(name)
            self._render()

    def _remove(self, idx):
        if 0 <= idx < len(self._names):
            del self._names[idx]
            self._render()

    def _move(self, idx, delta):
        j = idx + delta
        if 0 <= idx < len(self._names) and 0 <= j < len(self._names):
            self._names[idx], self._names[j] = self._names[j], self._names[idx]
            self._render()

    def _ok(self):
        self._on_ok(_clean_names(self._names))
        self.destroy()


class SourceChatsWindow(ctk.CTkToplevel):
    """Finestra standalone che ospita `SourceChatsPanel` a tutta finestra.

    Mantenuta per compatibilità; la stessa `SourceChatsPanel` vive anche come scheda
    della finestra "🧰 Strumenti"."""

    def __init__(self, master=None, on_saved=None):
        super().__init__(master)
        self.title(i18n.tr("Chat sorgenti (multi-chat)"))
        gui_utils.fit_to_screen(self, 1080, 560, 820, 460)
        SourceChatsPanel(self, on_saved=on_saved).pack(fill="both", expand=True)
