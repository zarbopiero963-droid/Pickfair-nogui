"""Finestra di gestione dell'anagrafica Provider.

Permette di **vedere**, **aggiungere** e **rimuovere** i nomi Provider salvati
(`config.json` → chiave `providers`), usati nel Parser Personalizzato (colonna
`Provider`, menu a tendina). Finora l'anagrafica si poteva solo *aggiungere* dal
builder ("➕ Provider"); qui c'è anche la **rimozione** e una vista d'insieme,
senza dover aprire il builder.

Tutta la logica sta in `provider_store` (funzioni pure, testate in CI): qui ci
sono SOLO i widget e la persistenza (`config_store.save_config`). Come per la
finestra Sorgenti, ogni azione persiste subito e chiama `on_saved(new_cfg)`, così
la GUI principale aggiorna la config in memoria e un successivo "Salva Config" /
"Avvia" non riscrive il file perdendo i provider (stesso pattern anti-stale).

Testi UI localizzati via `i18n.tr` (#343 slice 4c): l'italiano è il riferimento,
la lingua attiva è quella scelta all'avvio; i messaggi con variabili passano dal
template tradotto (`.format(...)`), così la chiave di catalogo resta stabile.

NB: questo modulo non è testato in CI (richiede un display). La logica che usa è
coperta da `tests/unit/test_provider_store.py`. Verifica manuale su Windows.
"""

import customtkinter as ctk

from . import config_store, gui_utils, i18n, provider_store


class ProviderPanel(ctk.CTkFrame):
    """Pannello dell'anagrafica Provider — incassabile sia in una finestra standalone
    (`ProviderWindow`) sia come scheda della finestra "🧰 Strumenti".

    `on_saved(new_cfg)`: callback opzionale chiamata dopo ogni salvataggio
    riuscito, così la GUI principale aggiorna la propria config in memoria."""

    def __init__(self, master=None, on_saved=None):
        super().__init__(master)
        self._on_saved = on_saved
        self._build_ui()
        self._reload()

    def refresh(self):
        """Ricarica la lista dei provider dal disco.

        Usato quando la config cambia da FUORI questo pannello (es. l'applicazione di
        un profilo nella stessa finestra "🧰 Strumenti" può sostituire la lista
        `providers`): senza, il pannello mostrerebbe nomi stantii (Codex)."""
        self._reload()

    # ── costruzione UI ─────────────────────────────────────────────────────
    def _build_ui(self):
        ctk.CTkLabel(
            self, text=i18n.tr("📇  Anagrafica Provider"),
            font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=12, pady=(10, 2))
        ctk.CTkLabel(
            self, text=i18n.tr("Nomi Provider riutilizzabili nel Parser Personalizzato "
                               "(colonna Provider). Valgono per tutti i parser."),
            font=ctk.CTkFont(size=11), text_color="gray", wraplength=480,
            anchor="w", justify="left").pack(anchor="w", padx=12, pady=(0, 6))

        # Riga di inserimento: nome + Aggiungi.
        add = ctk.CTkFrame(self, fg_color="transparent")
        add.pack(fill="x", padx=12, pady=(0, 6))
        self._name_entry = ctk.CTkEntry(add, placeholder_text=i18n.tr("Nome del nuovo Provider"))
        self._name_entry.pack(side="left", fill="x", expand=True, padx=(0, 6))
        self._name_entry.bind("<Return>", lambda _e: self._add())
        ctk.CTkButton(add, text=i18n.tr("➕  Aggiungi"), width=120, fg_color="#2e7d32",
                      hover_color="#1b5e20", command=self._add).pack(side="left")

        self._rows_frame = ctk.CTkScrollableFrame(self, height=360,
                                                  label_text=i18n.tr("Provider salvati"))
        self._rows_frame.pack(fill="both", expand=True, padx=12, pady=6)

        self._status = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=11),
                                    text_color="gray", wraplength=480, anchor="w", justify="left")
        self._status.pack(fill="x", padx=12, pady=(0, 10))

    # ── stato corrente ─────────────────────────────────────────────────────
    def _load_names(self) -> list:
        """Nomi provider salvati (best-effort: config illeggibile → lista vuota)."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            return []
        return provider_store.provider_names(cfg)

    def _reload(self):
        """Ridisegna la lista dei provider dallo stato corrente su disco."""
        for child in self._rows_frame.winfo_children():
            child.destroy()
        names = self._load_names()
        if not names:
            ctk.CTkLabel(self._rows_frame, text=i18n.tr("Nessun provider salvato."),
                         text_color="gray").pack(anchor="w", padx=6, pady=4)
            return
        for name in names:
            row = ctk.CTkFrame(self._rows_frame, fg_color="transparent")
            row.pack(fill="x", pady=2)
            ctk.CTkLabel(row, text=name, anchor="w").pack(side="left", fill="x",
                                                          expand=True, padx=6)
            ctk.CTkButton(row, text=i18n.tr("🗑  Rimuovi"), width=110, fg_color="#c62828",
                          hover_color="#7f0000",
                          command=lambda n=name: self._remove(n)).pack(side="right", padx=3)

    # ── azioni (persistono subito, come il builder) ─────────────────────────
    def _persist(self, cfg: dict, ok_msg: str, fail_msg: str):
        """Salva `cfg`, sincronizza la GUI principale (on_saved) e ridisegna.

        Mostra l'esito REALE della scrittura su disco: un salvataggio fallito non
        deve apparire come riuscito (i provider andrebbero persi al riavvio)."""
        saved, ok = config_store.save_config(cfg, config_store.CONFIG_FILE)
        if ok and callable(self._on_saved):
            self._on_saved(saved)
        self._reload()
        self._status.configure(text=ok_msg if ok else fail_msg,
                               text_color="#66bb6a" if ok else "#ef5350")

    def _add(self):
        """Aggiunge il nome digitato all'anagrafica (dedup case-insensitive)."""
        name = (self._name_entry.get() or "").strip()
        if not name:
            self._status.configure(text=i18n.tr("⛔ Nome vuoto: provider non aggiunto."),
                                   text_color="#ef5350")
            return
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception as exc:                 # noqa: BLE001
            self._status.configure(
                text=i18n.tr("❌ Config illeggibile: {exc}").format(exc=exc),
                text_color="#ef5350")
            return
        if name.casefold() in {n.casefold() for n in provider_store.provider_names(cfg)}:
            self._status.configure(
                text=i18n.tr("ℹ️ «{name}» è già nell'anagrafica.").format(name=name),
                text_color="gray")
            return
        cfg = provider_store.add_provider(cfg, name)
        self._name_entry.delete(0, "end")
        self._persist(
            cfg,
            ok_msg=i18n.tr("➕ Provider «{name}» salvato.").format(name=name),
            fail_msg=i18n.tr(
                "❌ Salvataggio FALLITO: «{name}» non salvato (andrebbe perso al riavvio). "
                "Controlla permessi/spazio del file config.").format(name=name))

    def _remove(self, name: str):
        """Rimuove `name` dall'anagrafica (confronto case-insensitive)."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception as exc:                 # noqa: BLE001
            self._status.configure(
                text=i18n.tr("❌ Config illeggibile: {exc}").format(exc=exc),
                text_color="#ef5350")
            return
        cfg = provider_store.remove_provider(cfg, name)
        self._persist(
            cfg,
            ok_msg=i18n.tr("🗑 Provider «{name}» rimosso.").format(name=name),
            fail_msg=i18n.tr(
                "❌ Salvataggio FALLITO: «{name}» non rimosso (ricomparirebbe al riavvio). "
                "Controlla permessi/spazio del file config.").format(name=name))


class ProviderWindow(ctk.CTkToplevel):
    """Finestra standalone che ospita `ProviderPanel` a tutta finestra.

    Mantenuta per compatibilità (apertura come finestra separata); la stessa
    `ProviderPanel` vive anche come scheda della finestra "🧰 Strumenti"."""

    def __init__(self, master=None, on_saved=None):
        super().__init__(master)
        self.title(i18n.tr("Anagrafica Provider"))
        gui_utils.fit_to_screen(self, 520, 520, 460, 420)
        ProviderPanel(self, on_saved=on_saved).pack(fill="both", expand=True)
