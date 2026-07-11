"""Scheda «🧹 Nomi Betfair» — ripulitura manuale dei nomi squadra permanenti (#282 PR 11-bis).

I nomi squadra raccolti dalla sync Betfair (`betfair_known_teams`, #319) sono **permanenti**:
il mark-and-sweep non li tocca, quindi crescono nel tempo e possono restare nomi obsoleti/
errati (squadre retrocesse/rinominate). Questa scheda li **sfoglia per sport** e permette di
**eliminarli** uno per uno — l'unico modo per togliere un nome permanente.

Come le altre viste sul dizionario Betfair, è **fail-fast** durante una sync (probe non
bloccante: mostra «⏳ sync in corso» invece di congelare la GUI) e best-effort (DB assente →
avviso, nessun crash). Tutta la logica di lettura/eliminazione (busy-guard incluso) vive in
`App` (callback iniettati); qui ci sono solo widget/wiring, non testati in CI (serve un
display): la logica esercitabile è testata a parte.
"""

import customtkinter as ctk

from . import sports
from .betfair.dictionary_viewer import DictionaryBusy

# Voce «tutti gli sport» del filtro (= nessun filtro).
_SPORT_ALL = "(tutti gli sport)"
_COL_SPORT_WIDTH = 130
_COL_NAME_WIDTH = 300


class KnownTeamsPanel(ctk.CTkFrame):
    """Pannello di ripulitura dei nomi squadra permanenti.

    `teams_provider(sport=None)` → lista di dict `{sport, normalized_name, display_name, …}`
    (può sollevare `DictionaryBusy` se una sync è in corso). `delete_team(sport,
    normalized_name)` → elimina un nome (idem `DictionaryBusy`). Entrambi opzionali: se
    assenti, il pannello avvisa invece di operare."""

    def __init__(self, master=None, teams_provider=None, delete_team=None):
        super().__init__(master)
        self._teams_provider = teams_provider
        self._delete_team = delete_team
        self._sport = ctk.StringVar(value=_SPORT_ALL)
        self._build_ui()
        self._refresh()

    # ── costruzione UI ────────────────────────────────────────────────────────
    def _build_ui(self):
        ctk.CTkLabel(
            self, text="🧹  Nomi Betfair noti (permanenti) — ripulitura",
            font=ctk.CTkFont(size=16, weight="bold")).pack(anchor="w", padx=12, pady=(10, 2))
        ctk.CTkLabel(
            self, text="Nomi squadra raccolti dalla sync Betfair, conservati per sempre. "
                       "Elimina qui quelli obsoleti/errati (es. squadre retrocesse).",
            font=ctk.CTkFont(size=11), text_color="gray", wraplength=720,
            anchor="w", justify="left").pack(anchor="w", padx=12, pady=(0, 6))

        bar = ctk.CTkFrame(self)
        bar.pack(fill="x", padx=12, pady=6)
        ctk.CTkLabel(bar, text="Sport").pack(side="left", padx=(8, 4))
        ctk.CTkOptionMenu(bar, variable=self._sport, width=180,
                          values=[_SPORT_ALL, *sports.SPORTS],
                          command=lambda _v: self._refresh()).pack(side="left", padx=4)
        ctk.CTkButton(bar, text="🔄 Aggiorna", width=110,
                      command=self._refresh).pack(side="left", padx=4)

        self._counts = ctk.CTkLabel(self, text="", anchor="w")
        self._counts.pack(fill="x", padx=14, pady=(0, 4))
        self._rows_frame = ctk.CTkScrollableFrame(self, height=400, label_text="Nomi noti")
        self._rows_frame.pack(fill="both", expand=True, padx=12, pady=6)

    # ── dati ──────────────────────────────────────────────────────────────────
    def _selected_sport(self):
        s = self._sport.get()
        return None if s == _SPORT_ALL else s

    def _clear_rows(self):
        for w in self._rows_frame.winfo_children():
            w.destroy()

    def _refresh(self):
        """Ricarica l'elenco dei nomi noti (best-effort, fail-fast durante una sync)."""
        self._clear_rows()
        if not callable(self._teams_provider):
            self._counts.configure(
                text="⛔ Dizionario Betfair non disponibile: sincronizza da «🔵 Betfair Sync».")
            return
        try:
            teams = self._teams_provider(self._selected_sport()) or []
        except DictionaryBusy:
            self._counts.configure(
                text="⏳ Sincronizzazione Betfair in corso: riprova tra poco.")
            return
        except Exception as exc:                 # noqa: BLE001 — best-effort, niente crash GUI
            self._counts.configure(text=f"⚠️ Errore lettura nomi: {type(exc).__name__}")
            return
        self._counts.configure(text=f"{len(teams)} nomi noti.")
        for team in teams:
            self._append_row(team)

    def _append_row(self, team):
        sport = str((team or {}).get("sport") or "")
        name = str((team or {}).get("display_name") or "")
        norm = str((team or {}).get("normalized_name") or "")
        row = ctk.CTkFrame(self._rows_frame, fg_color="transparent")
        row.pack(fill="x", pady=1)
        ctk.CTkLabel(row, text=sport, width=_COL_SPORT_WIDTH, anchor="w").pack(side="left", padx=3)
        ctk.CTkLabel(row, text=name, width=_COL_NAME_WIDTH, anchor="w").pack(side="left", padx=3)
        ctk.CTkButton(row, text="🗑 Elimina", width=100, fg_color="#c62828",
                      hover_color="#7f0000",
                      command=lambda s=sport, n=norm: self._on_delete(s, n)).pack(side="left", padx=3)

    def _on_delete(self, sport, normalized_name):
        """Elimina un nome permanente e ricarica. Fail-fast durante una sync; best-effort."""
        if not callable(self._delete_team):
            self._counts.configure(text="⛔ Eliminazione non disponibile.")
            return
        try:
            ok = self._delete_team(sport, normalized_name)
        except DictionaryBusy:
            self._counts.configure(
                text="⏳ Sincronizzazione Betfair in corso: riprova tra poco.")
            return
        except Exception as exc:                 # noqa: BLE001 — best-effort, niente crash GUI
            self._counts.configure(text=f"⚠️ Eliminazione fallita: {type(exc).__name__}")
            return
        if not ok:
            # DB non disponibile (best-effort → False): niente refresh «pulito» che nasconde
            # il no-op — avvisa che nulla è stato eliminato (CodeRabbit/GPT/Fable #322).
            self._counts.configure(
                text="⚠️ Eliminazione non riuscita: dizionario Betfair non disponibile.")
            return
        self._refresh()
