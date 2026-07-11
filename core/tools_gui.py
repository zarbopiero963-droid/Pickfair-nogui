"""Finestra hub "🧰 Strumenti": raccoglie gli strumenti del bridge in schede.

Parte della consolidazione GUI (roadmap, Tappa 1): invece di N finestre separate
aperte da N pulsanti, gli strumenti vivono come schede di un'unica finestra.

`ToolsWindow` è DISACCOPPIATA dai singoli strumenti: riceve una lista di
`(titolo_scheda, factory)`, dove `factory(parent)` costruisce il pannello dentro la
scheda. Così questa finestra non conosce le callback/gli store dei singoli strumenti
— li cabla chi la apre (la GUI principale, che ha la config viva). Aggiungere uno
strumento = aggiungere una voce alla lista, senza toccare questa classe.

NB: modulo GUI, non testato in CI (richiede un display). La logica dei singoli
strumenti è coperta dai rispettivi test unitari. Verifica manuale su Windows.
"""

import customtkinter as ctk

from . import gui_utils

# Information architecture dell'hub Strumenti (#293 slice 4): gli strumenti sono raggruppati
# PER FLUSSO in 4 gruppi. L'ordine di questa struttura è l'ordine delle schede; il numero del
# gruppo (①..④) prefissa il titolo di ogni scheda, così l'appartenenza è visibile a colpo
# d'occhio pur restando un `CTkTabview` piatto (primo passo incrementale, scelto col
# proprietario). Fonte UNICA della IA: ordine e prefissi non possono divergere tra codice e test.
TOOL_GROUPS = (
    ("①", "Sorgenti", ("sources", "provider")),
    ("②", "Lettura messaggi", ("parser", "mapping")),
    ("③", "Betfair", ("betfair", "dictionary", "journal", "known_teams")),
    ("④", "Impostazioni", ("profiles", "summary")),
)

# Prefissi di gruppo (①..④), derivati da TOOL_GROUPS: usati per riconoscere/rimuovere il
# prefisso dal titolo di una scheda quando si risolve un `initial` passato come titolo base.
_GROUP_PREFIXES = frozenset(prefix for prefix, _name, _keys in TOOL_GROUPS)

# Etichetta base (icona + nome) di ogni strumento, SENZA il prefisso di gruppo.
TOOL_TITLES = {
    "sources": "📡 Chat sorgenti",
    "provider": "📇 Provider",
    "parser": "🧩 Parser",
    "mapping": "🗺️ Mapping",
    "betfair": "🔵 Betfair Sync",
    "dictionary": "📖 Dizionario Betfair",
    "journal": "📒 Diario",
    "known_teams": "🧹 Nomi Betfair",
    "profiles": "📁 Profili",
    "summary": "📋 Riepilogo",
}


def build_tool_panels(factories: dict) -> list:
    """Costruisce la lista ordinata `(titolo, factory)` delle schede dell'hub, raggruppate per
    flusso secondo `TOOL_GROUPS`. `factories` mappa la chiave-strumento → `factory(parent)`. Il
    titolo è prefissato col numero del gruppo (es. «① 📡 Chat sorgenti»). Logica **pura**
    (nessun widget), così l'ordine/prefissi/completezza sono testabili headless. Solleva
    `KeyError` se manca la factory di uno strumento previsto: fail-fast, nessuna scheda persa in
    silenzio dopo un riordino errato."""
    panels = []
    for prefix, _name, keys in TOOL_GROUPS:
        for key in keys:
            panels.append((f"{prefix} {TOOL_TITLES[key]}", factories[key]))
    return panels


class ToolsWindow(ctk.CTkToplevel):
    """Finestra a schede che ospita i pannelli-strumento.

    Args:
        master: finestra padre.
        panels: lista di `(titolo, factory)`; `factory(parent)` ritorna un widget
            (tipicamente un `CTkFrame`) da mostrare nella scheda.
        initial: titolo della scheda da selezionare all'apertura (opzionale).
        title: titolo della finestra.
    """

    def __init__(self, master=None, panels=None, initial=None, title="🧰 Strumenti"):
        super().__init__(master)
        self.title(title)
        # Larghezza default 1140 (era 1040): la scheda Mapping ha righe con 5 colonne +
        # elimina (Country|Betfair|Provider|Sport|Tipo|🗑 ≈ 1032 px) e lo scroll è solo
        # verticale; a 1040 Tipo/elimina venivano tagliati nel tab Strumenti (Codex #178 §2).
        gui_utils.fit_to_screen(self, 1140, 720, 780, 480)
        # `command`: a ogni cambio scheda si aggiornano le liste-opzioni del pannello
        # mostrato (vedi `_on_tab_change`), così le scelte derivate dal config restano
        # fresche senza riaprire la finestra (Codex).
        self._tabs = ctk.CTkTabview(self, command=self._on_tab_change)
        self._tabs.pack(fill="both", expand=True, padx=8, pady=8)
        self._panels = {}      # titolo scheda → pannello vivo (per refresh_options)
        for tab_title, factory in (panels or []):
            container = self._tabs.add(tab_title)
            try:
                panel = factory(container)
                panel.pack(fill="both", expand=True, padx=4, pady=4)
                self._panels[tab_title] = panel
            except Exception as exc:        # noqa: BLE001 — isolamento per-scheda
                # Un pannello che fallisce la COSTRUZIONE (es. cartella profili illeggibile
                # → OSError da list_profiles) non deve impedire l'apertura degli ALTRI
                # strumenti: prima della consolidazione erano finestre separate, quindi un
                # guasto non bloccava gli altri. Qui si preserva quell'isolamento mostrando
                # l'errore NELLA sua scheda e proseguendo con le altre (Codex).
                ctk.CTkLabel(
                    container,
                    text=f"⚠️ Impossibile aprire questo strumento:\n{exc}",
                    text_color="#ef5350", wraplength=600, justify="left",
                    anchor="w").pack(padx=12, pady=12, fill="x")
        self.select_tab(initial)

    def _on_tab_change(self):
        """Al cambio scheda, aggiorna le liste-opzioni del pannello mostrato se le supporta
        (`refresh_options`), così provider/parser/profili modificati in un'altra scheda si
        riflettono subito, senza scartare le modifiche in corso. Best-effort: un refresh
        fallito non rompe il cambio scheda (Codex)."""
        panel = self._panels.get(self._tabs.get())
        if panel is not None and hasattr(panel, "refresh_options"):
            try:
                panel.refresh_options()
            except Exception:               # noqa: BLE001 — refresh best-effort
                pass

    def select_tab(self, title):
        """Seleziona la scheda `title` (no-op se vuoto o titolo non trovato).

        Usata sia all'apertura sia quando si riapre la hub già viva su un'altra scheda
        (vedi `App._open_tools`: una sola finestra hub, si cambia scheda). Accetta sia il
        titolo COMPLETO della scheda (col prefisso di gruppo, es. «② 🧩 Parser») sia il titolo
        BASE senza prefisso (es. «🧩 Parser»): dal #293 slice 4 i titoli portano un prefisso
        ①..④, così un chiamante che non lo conosce apre comunque la scheda giusta invece di
        fallire in silenzio (robustezza del parametro `initial`, review #338)."""
        target = self._resolve_tab_title(title)
        if target is None:
            return
        try:
            self._tabs.set(target)
        except Exception:                   # noqa: BLE001 — widget distrutto/titolo invalido: resta la scheda corrente
            pass

    def _resolve_tab_title(self, title):
        """Titolo di scheda REALE corrispondente a `title`, oppure ``None`` se nessuna
        corrisponde. Match esatto; in mancanza, la scheda il cui titolo, tolto il solo prefisso
        di gruppo «①..④ », **coincide** col valore richiesto (titolo base). Match preciso: «Parser»
        da solo NON matcha «② 🧩 Parser». Logica **pura** (legge solo `self._panels`), testabile
        headless. Vuoto → ``None``."""
        if not title:
            return None
        if title in self._panels:
            return title
        for name in self._panels:
            # Rimuovi il solo prefisso di gruppo «①..④ » e confronta col titolo base richiesto:
            # match preciso (niente suffix-match ambiguo su una parola qualsiasi).
            base = name.split(" ", 1)[1] if name[:1] in _GROUP_PREFIXES and " " in name else name
            if base == title:
                return name
        return None
