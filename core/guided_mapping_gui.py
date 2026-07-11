"""Pannello «🌳 Mapping guidato» — albero Sport → Competizione → Squadre → nome canale (Fase 3).

Sotto-scheda di «🧰 Strumenti → Mapping». Guida l'utente: sceglie **Sport**, poi una
**Competizione** (dai dati Betfair sincronizzati sul PC), vede le **Squadre** di quella
competizione e, accanto a ciascuna, scrive «come la chiama il canale Telegram». Al salvataggio
le associazioni finiscono nel **profilo `name_mappings`** scelto (consumato dal parser), riusando
`name_mapping_store.set_entries` (stessa pulizia/validazione della GUI classica).

Divisione delle responsabilità (come le altre GUI del repo):
- la logica pura (competizioni/squadre da Betfair, fusione righe) sta in
  `betfair.guided_mapping` (testata in CI);
- qui ci sono SOLO widget + persistenza (`config_store.save_config`) + il pattern anti-stale
  (`on_saved(new_cfg)`);
- la lettura del dizionario Betfair è **fail-fast** durante una sync: i provider passati
  dall'app sollevano `DictionaryBusy` (probe non bloccante sul lock del DB) e il pannello mostra
  «sync in corso» invece di freezare il thread Tk (come il viewer, #175).

NB: modulo non testato in CI (richiede display); la logica sottostante è coperta da
`tests/unit/test_betfair_guided_mapping.py`. Verifica manuale su Windows.
"""

import customtkinter as ctk

from . import config_store, name_mapping_store, sports
from .betfair.dictionary_viewer import Debouncer, DictionaryBusy
from .betfair.guided_mapping import (
    competition_labels,
    existing_aliases_for_teams,
    merge_team_aliases,
)

# Voce placeholder delle tendine.
_NO_PROFILE = "(nessun profilo)"
_NO_COMP = "(scegli lo sport)"
# Tetto di righe-squadra renderizzate in un colpo (come il viewer, Fase 2): evita il freeze su
# competizioni molto popolose (es. tornei tennis con centinaia di partecipanti). Il MODELLO tiene
# tutte le squadre (una StringVar ciascuna, nessun widget); qui si limita solo il RENDER, e la
# casella «Filtra» restringe. NON è un cap sui dati salvati.
_TEAM_RENDER_CAP = 500
# Debounce della casella «Filtra squadre» (come il viewer #184 M12): una raffica di keystroke
# collassa in un solo ridisegno a fine digitazione.
_FILTER_DEBOUNCE_MS = 250


class GuidedMappingPanel(ctk.CTkFrame):
    """Pannello del Mapping guidato Betfair → nome canale.

    Provider iniettati dall'app (sola lettura del dizionario Betfair, **fail-fast** su
    `DictionaryBusy` durante una sync):
    - `competitions_provider(sport) -> list[{"competition_id","name"}]`;
    - `teams_provider(competition_id) -> list[str]` (nomi squadra della competizione).
    `on_saved(new_cfg)`: callback dopo ogni salvataggio riuscito (anti-stale della GUI principale).
    """

    def __init__(self, master=None, *, competitions_provider=None, teams_provider=None,
                 on_saved=None):
        super().__init__(master)
        self._competitions_provider = competitions_provider
        self._teams_provider = teams_provider
        self._on_saved = on_saved
        self._current = None                 # profilo selezionato
        self._comp_by_label = {}             # {label univoca tendina: competition_id}
        self._team_vars = {}                 # {nome_squadra: StringVar(alias)} — MODELLO, tutte le squadre
        self._filter_debouncer = Debouncer(
            _FILTER_DEBOUNCE_MS, self._render_team_rows,
            schedule=self.after, cancel=self.after_cancel)
        self._build_ui()
        self._reload_profiles(select_first=True)
        # Precarica le competizioni per lo sport di default (già selezionato nella tendina): il
        # `command` del menu Sport scatta solo su cambio manuale, quindi senza questa chiamata la
        # tendina Competizione resterebbe sul placeholder finché l'utente non ritocca lo Sport
        # (CodeRabbit #389). Best-effort: `_on_sport_change` gestisce da sé DB assente/sync in corso.
        self._on_sport_change()

    def destroy(self):
        """Teardown: annulla un eventuale ridisegno in debounce PRIMA di distruggere i widget
        (come il viewer): un timer `after` pendente scatterebbe altrimenti contro widget distrutti."""
        deb = getattr(self, "_filter_debouncer", None)
        if deb is not None:
            deb.cancel_pending()
        super().destroy()

    def refresh(self):
        """Aggiorna SOLO l'elenco profili dalla config su disco (anti-stale dei nomi profilo dopo
        rename/delete fatti in un'altra sotto-scheda). **Non ri-precompila gli alias e non tocca le
        squadre**: un refresh esterno (es. salvataggio in «⚽ Calcio» che propaga `on_saved`) NON deve
        azzerare gli alias digitati e non ancora salvati nell'albero guidato (Fable #389)."""
        cfg = self._load_cfg()
        names = name_mapping_store.profile_names(cfg) if cfg is not None else []
        self._profile_menu.configure(values=names or [_NO_PROFILE])
        if self._current not in names:          # profilo eliminato altrove → deseleziona (senza clobber)
            self._current = None
            self._profile_var.set(_NO_PROFILE)

    # ── costruzione UI ─────────────────────────────────────────────────────────
    def _build_ui(self):
        ctk.CTkLabel(
            self, text="🌳  Mapping guidato (Betfair → nome canale)",
            font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=12, pady=(10, 2))
        ctk.CTkLabel(
            self, text="Scegli Sport → Competizione: compaiono le squadre dai dati Betfair "
                       "sincronizzati. Accanto a ogni squadra scrivi «come la chiama il canale» e "
                       "salva nel profilo. Serve un sync Betfair recente.",
            font=ctk.CTkFont(size=11), text_color="gray", wraplength=760,
            anchor="w", justify="left").pack(anchor="w", padx=12, pady=(0, 6))

        # Riga profilo (destinazione del salvataggio).
        prof = ctk.CTkFrame(self, fg_color="transparent")
        prof.pack(fill="x", padx=12, pady=(0, 4))
        ctk.CTkLabel(prof, text="Profilo:").pack(side="left", padx=(6, 4))
        self._profile_var = ctk.StringVar(value=_NO_PROFILE)
        self._profile_menu = ctk.CTkOptionMenu(
            prof, variable=self._profile_var, values=[_NO_PROFILE], width=220,
            command=self._on_profile_change)
        self._profile_menu.pack(side="left", padx=4)
        ctk.CTkButton(prof, text="🆕 Nuovo", width=84,
                      command=self._new_profile).pack(side="left", padx=3)

        # Riga sport + competizione.
        pick = ctk.CTkFrame(self, fg_color="transparent")
        pick.pack(fill="x", padx=12, pady=(0, 4))
        ctk.CTkLabel(pick, text="Sport:").pack(side="left", padx=(6, 4))
        self._sport_var = ctk.StringVar(value=sports.SPORTS[0])
        ctk.CTkOptionMenu(pick, variable=self._sport_var, width=150, values=list(sports.SPORTS),
                          command=lambda _v: self._on_sport_change()).pack(side="left", padx=4)
        ctk.CTkLabel(pick, text="Competizione:").pack(side="left", padx=(12, 4))
        self._comp_var = ctk.StringVar(value=_NO_COMP)
        self._comp_menu = ctk.CTkOptionMenu(pick, variable=self._comp_var, width=240,
                                            values=[_NO_COMP], command=lambda _v: self._load_teams())
        self._comp_menu.pack(side="left", padx=4)

        # Riga filtro squadre.
        fbar = ctk.CTkFrame(self, fg_color="transparent")
        fbar.pack(fill="x", padx=12, pady=(0, 4))
        ctk.CTkLabel(fbar, text="Filtra squadre:").pack(side="left", padx=(6, 4))
        self._filter_var = ctk.StringVar(value="")
        entry = ctk.CTkEntry(fbar, textvariable=self._filter_var, width=240,
                             placeholder_text="parte del nome squadra…")
        entry.pack(side="left", padx=4)
        entry.bind("<KeyRelease>", self._on_filter_key)
        ctk.CTkButton(fbar, text="Pulisci", width=80,
                      command=self._clear_filter).pack(side="left", padx=3)

        # Intestazione tabella squadre.
        head = ctk.CTkFrame(self, fg_color="transparent")
        head.pack(fill="x", padx=12, pady=(4, 0))
        ctk.CTkLabel(head, text="Squadra Betfair", width=300, anchor="w",
                     font=ctk.CTkFont(size=11, weight="bold")).pack(side="left", padx=3)
        ctk.CTkLabel(head, text="Come la chiama il canale", width=300, anchor="w",
                     font=ctk.CTkFont(size=11, weight="bold")).pack(side="left", padx=3)

        self._rows_frame = ctk.CTkScrollableFrame(self, height=340, label_text="Squadre")
        self._rows_frame.pack(fill="both", expand=True, padx=12, pady=6)

        actions = ctk.CTkFrame(self, fg_color="transparent")
        actions.pack(fill="x", padx=12, pady=(0, 4))
        ctk.CTkButton(actions, text="💾 Salva nel profilo", width=170, fg_color="#2e7d32",
                      hover_color="#1b5e20", command=self._save).pack(side="left", padx=3)

        self._status = ctk.CTkLabel(self, text="", font=ctk.CTkFont(size=11),
                                    text_color="gray", wraplength=760, anchor="w", justify="left")
        self._status.pack(fill="x", padx=12, pady=(0, 10))

    # ── config / profili ───────────────────────────────────────────────────────
    def _load_cfg(self):
        try:
            return config_store.load_config(config_store.CONFIG_FILE)
        except Exception as exc:                 # noqa: BLE001 — fallback con messaggio
            self._status.configure(text=f"❌ Config illeggibile: {exc}", text_color="#ef5350")
            return None

    def _reload_profiles(self, select=None, select_first=False, prefill=True):
        """Ricarica la tendina profili e seleziona il target. `prefill=True` ri-precompila gli alias
        dal profilo scelto (init/switch deliberato); `prefill=False` preserva gli alias digitati (es.
        dopo «🆕 Nuovo»: le squadre appena mappate a mano non devono sparire prima di salvarle)."""
        cfg = self._load_cfg()
        names = name_mapping_store.profile_names(cfg) if cfg is not None else []
        self._profile_menu.configure(values=names or [_NO_PROFILE])
        if select and select in names:
            target = select
        elif self._current in names:
            target = self._current
        elif select_first and names:
            target = names[0]
        else:
            target = None
        self._current = target
        self._profile_var.set(target or _NO_PROFILE)
        if prefill:
            # switch/init deliberato: mostra gli alias già salvati del profilo scelto.
            self._prefill_aliases()
            self._render_team_rows()

    def _on_profile_change(self, value):
        self._current = value if value != _NO_PROFILE else None
        self._prefill_aliases()
        self._render_team_rows()

    def _new_profile(self):
        dialog = ctk.CTkInputDialog(text="Nome del nuovo profilo:", title="Nuovo profilo")
        name = (dialog.get_input() or "").strip()
        if not name:
            self._status.configure(text="⛔ Profilo non creato (nome vuoto).", text_color="#ef5350")
            return
        cfg = self._load_cfg()
        if cfg is None:
            return
        if name in name_mapping_store.profile_names(cfg):
            self._status.configure(text=f"ℹ️ Il profilo «{name}» esiste già.", text_color="gray")
            return
        cfg = name_mapping_store.add_profile(cfg, name)
        saved, ok = config_store.save_config(cfg, config_store.CONFIG_FILE)
        if ok and callable(self._on_saved):
            self._on_saved(saved)
        if ok:
            # prefill=False: se avevi già digitato alias, «Nuovo» seleziona il profilo vuoto SENZA
            # azzerarli, così puoi salvarli subito nel profilo appena creato.
            self._reload_profiles(select=name, prefill=False)
        self._status.configure(
            text=f"🆕 Profilo «{name}» creato." if ok
            else f"❌ Salvataggio FALLITO: «{name}» non creato.",
            text_color="#66bb6a" if ok else "#ef5350")

    # ── sport / competizioni / squadre ─────────────────────────────────────────
    def _selected_sport(self):
        return self._sport_var.get()

    def _on_sport_change(self):
        """Nuovo sport: ricarica la tendina competizioni (fail-fast su sync in corso)."""
        self._comp_by_label = {}
        try:
            comps = (self._competitions_provider(self._selected_sport())
                     if callable(self._competitions_provider) else [])
        except DictionaryBusy:
            self._comp_menu.configure(values=[_NO_COMP])
            self._comp_var.set(_NO_COMP)
            self._team_vars = {}
            self._render_team_rows()
            self._status.configure(
                text="⏳ Dizionario in aggiornamento (sync Betfair in corso): riprova tra poco.",
                text_color="#ffa726")
            return
        except Exception:   # noqa: BLE001 — best-effort: DB assente/illeggibile → nessuna competizione
            comps = []
        # Label UNIVOCHE (Fable/Fugu #389): competizioni omonime avrebbero la stessa voce e
        # selezionerebbero sempre la prima → id sbagliato. `competition_labels` disambigua.
        pairs = competition_labels(comps)
        self._comp_by_label = dict(pairs)
        labels = [lbl for lbl, _ in pairs] or [_NO_COMP]
        self._comp_menu.configure(values=labels)
        self._comp_var.set(labels[0])
        # svuota le squadre finché non si (ri)carica la competizione selezionata.
        self._team_vars = {}
        self._render_team_rows()
        if comps:
            self._load_teams()
        else:
            self._status.configure(
                text=f"ℹ️ Nessuna competizione per «{self._selected_sport()}». "
                     "Fai un sync Betfair, poi riprova.", text_color="gray")

    def _selected_competition_id(self):
        return self._comp_by_label.get(self._comp_var.get())

    def _load_teams(self):
        """Carica le squadre della competizione selezionata nel MODELLO (una StringVar ciascuna),
        pre-compila gli alias già salvati, e ridisegna."""
        comp_id = self._selected_competition_id()
        if not comp_id:
            self._team_vars = {}
            self._render_team_rows()
            return
        try:
            teams = (self._teams_provider(comp_id) if callable(self._teams_provider) else [])
        except DictionaryBusy:
            self._team_vars = {}
            self._render_team_rows()
            self._status.configure(
                text="⏳ Dizionario in aggiornamento (sync Betfair in corso): riprova tra poco.",
                text_color="#ffa726")
            return
        except Exception:   # noqa: BLE001 — best-effort: DB assente/illeggibile → nessuna squadra
            teams = []
        self._team_vars = {t: ctk.StringVar(value="") for t in teams}
        self._prefill_aliases()
        self._render_team_rows()
        if not teams:
            self._status.configure(
                text="ℹ️ Nessuna squadra per questa competizione (nessun evento sincronizzato). "
                     "Fai un sync Betfair, poi riprova.", text_color="gray")
        else:
            self._status.configure(
                text=f"{len(teams)} squadre. Scrivi l'alias del canale e premi «Salva nel profilo».",
                text_color="gray")

    def _prefill_aliases(self):
        """Pre-compila gli alias delle squadre correnti con quelli GIÀ salvati nel profilo scelto
        (mapping per-sport: la stessa squadra mostra il suo alias in qualunque competizione). Le
        squadre senza mapping restano vuote. Fondamentale per la correttezza del salvataggio: senza,
        ri-salvare una competizione azzererebbe i mapping di squadre condivise con altre competizioni."""
        if not self._team_vars:
            return
        cfg = self._load_cfg()
        entries = (name_mapping_store.get_entries(cfg, self._current)
                   if (cfg is not None and self._current) else [])
        aliases = existing_aliases_for_teams(entries, self._selected_sport(),
                                             list(self._team_vars.keys()))
        for team, var in self._team_vars.items():
            var.set(aliases.get(team, ""))

    # ── filtro / render ────────────────────────────────────────────────────────
    def _on_filter_key(self, event):
        if getattr(event, "keysym", "") in ("Return", "KP_Enter"):
            self._filter_debouncer.cancel_pending()
            self._render_team_rows()
            return
        self._filter_debouncer.trigger()

    def _clear_filter(self):
        self._filter_debouncer.cancel_pending()
        self._filter_var.set("")
        self._render_team_rows()

    def _render_team_rows(self):
        """Ridisegna le righe-squadra filtrate (cap `_TEAM_RENDER_CAP`). Le StringVar del MODELLO
        NON vengono ricreate qui, così il testo digitato sopravvive a filtro/ridisegno."""
        for child in self._rows_frame.winfo_children():
            child.destroy()
        if not self._team_vars:
            ctk.CTkLabel(self._rows_frame,
                         text="Scegli Sport e Competizione per vedere le squadre.",
                         text_color="gray").pack(anchor="w", padx=6, pady=4)
            return
        needle = self._filter_var.get().strip().casefold()
        teams = [t for t in self._team_vars if not needle or needle in t.casefold()]
        shown = teams[:_TEAM_RENDER_CAP]
        for team in shown:
            row = ctk.CTkFrame(self._rows_frame, fg_color="transparent")
            row.pack(fill="x", pady=2)
            ctk.CTkLabel(row, text=team, width=300, anchor="w").pack(side="left", padx=3)
            ctk.CTkEntry(row, width=300, textvariable=self._team_vars[team],
                         placeholder_text="come la chiama il canale…").pack(side="left", padx=3)
        if len(teams) > _TEAM_RENDER_CAP:
            ctk.CTkLabel(
                self._rows_frame,
                text=f"… mostrate {_TEAM_RENDER_CAP} di {len(teams)} squadre: usa «Filtra» per "
                     "restringere (gli alias già scritti restano salvati anche se non visibili).",
                text_color="#ffa726", wraplength=560, anchor="w", justify="left").pack(
                anchor="w", padx=6, pady=(4, 2))

    # ── salvataggio ────────────────────────────────────────────────────────────
    def _save(self):
        if not self._current:
            self._status.configure(
                text="⛔ Nessun profilo selezionato: crea o scegli un profilo di destinazione.",
                text_color="#ef5350")
            return
        if not self._team_vars:
            self._status.configure(text="⛔ Nessuna squadra caricata da salvare.",
                                   text_color="#ef5350")
            return
        cfg = self._load_cfg()
        if cfg is None:
            return
        team_aliases = {team: var.get() for team, var in self._team_vars.items()}
        existing = name_mapping_store.get_entries(cfg, self._current)
        merged = merge_team_aliases(existing, self._selected_sport(), team_aliases)
        cfg = name_mapping_store.set_entries(cfg, self._current, merged)
        saved, ok = config_store.save_config(cfg, config_store.CONFIG_FILE)
        if ok:
            if callable(self._on_saved):
                self._on_saved(saved)
            n_written = sum(1 for a in team_aliases.values() if str(a or "").strip())
            n_total = len(name_mapping_store.get_entries(cfg, self._current))
            self._status.configure(
                text=f"💾 Salvato nel profilo «{self._current}»: {n_written} squadre mappate in "
                     f"questa competizione ({n_total} righe totali nel profilo).",
                text_color="#66bb6a")
        else:
            self._status.configure(
                text=f"❌ Salvataggio FALLITO: «{self._current}» non salvato (andrebbe perso al "
                     "riavvio). Controlla permessi/spazio del file config.",
                text_color="#ef5350")
