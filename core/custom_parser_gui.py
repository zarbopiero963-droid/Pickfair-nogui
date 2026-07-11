"""CP-06: vista customtkinter (sottile) del costruttore di Parser Personalizzati.

Tutta la logica sta nel controller `parser_builder.ParserBuilder` (testato in CI);
qui ci sono SOLO i widget. La finestra si apre da un pulsante nella GUI principale
(`app.App`). Per regola: target, "Inizia dopo", "Finisce prima di", valore fisso,
trasformazione, value-map, obbligatorio. In più: aggiungi/rimuovi/sposta regola,
salva/carica, e **test-live** su un messaggio incollato.

Testi UI localizzati via `i18n.tr` (#343 slice 4g): l'italiano è il riferimento,
la lingua attiva è quella scelta all'avvio; i messaggi con variabili passano dal
template tradotto (`.format(...)`), così la chiave di catalogo resta stabile. Si
wrappa SOLO il chrome puro (titolo, etichette, bottoni, header, messaggi di
stato): NON gli interruttori «MultiMarket (più mercati)»/«MultiSelection (più
selezioni)» (le loro label raddoppiano da semantica di configurazione) né i
VALORI delle tendine (Modalità/Sport/Mercato/Trasformazione/Value-map, che sono
chiavi di config), né `title="Provider"` (confrontato come `rule.target`).

NB: questo modulo non è testato in CI (richiede un display). La logica che usa è
coperta da `tests/unit/test_parser_builder.py`. Verifica manuale su Windows.
"""

import customtkinter as ctk

from . import (
    config_store,
    gui_utils,
    i18n,
    market_mapping_store,
    name_mapping_store,
    parser_diagnostics,
    provider_store,
    sports,
)
from .custom_parser import Condition, MultiRowRule
from .parser_builder import ParserBuilder

# #283 PR 13: le righe MarketType/MarketName/SelectionName della tabella regole hanno una
# tendina EDITABILE in «Valore fisso», popolata dai valori permanenti del dizionario Betfair
# (harvest PR 12), filtrata per lo sport del parser. Mappa colonna CSV → chiave del provider.
_BETFAIR_TERM_TARGETS = {
    "MarketType": "market_types",
    "MarketName": "market_names",
    "SelectionName": "selection_names",
}

# Colori dell'indicatore «🔗 Traduzioni attive» (#293): verde theme-aware se almeno un profilo è
# selezionato, grigio se nessuno. Tuple CustomTkinter (light, dark).
_TRANSLATION_ON_COLOR = ("#2e7d32", "#66bb6a")
_TRANSLATION_OFF_COLOR = "gray"


def _translations_status_text(count: int) -> str:
    """Testo dell'indicatore «🔗 Traduzioni attive per questo parser» (#293): ``✓ N attive`` se
    almeno un profilo di mappatura è selezionato, ``— nessuna`` se nessuno. Puro/testabile: non
    dipende dalla GUI."""
    if count <= 0:
        return i18n.tr("— nessuna")
    return (i18n.tr("✓ 1 attiva") if count == 1
            else i18n.tr("✓ {count} attive").format(count=count))


# Colonne della tabella regole: (label, larghezza, avanzata?). #293 «densità parser»: di
# default il Parser mostra solo le colonne ESSENZIALI; le colonne «avanzate» Trasformazione e
# Value-map sono nascoste finché non si attiva il toggle «Avanzate». Fonte unica dell'ordine e
# della larghezza delle colonne dell'intestazione.
_RULE_COLUMNS = (
    ("Colonna", 150, False),
    ("Inizia dopo", 150, False),
    ("Finisce prima", 150, False),
    ("Valore fisso", 130, False),
    ("Trasformazione", 150, True),
    ("Value-map", 150, True),
    ("Obblig.", 60, False),
    ("", 70, False),
)


def _visible_rule_columns(show_advanced: bool):
    """Colonne ``(label, larghezza)`` da mostrare nell'intestazione della tabella regole:
    tutte se ``show_advanced``, altrimenti SENZA le colonne avanzate (Trasformazione/Value-map).
    Puro/testabile headless: la densità di default (#293) non dipende dalla GUI."""
    return [(label, w) for label, w, advanced in _RULE_COLUMNS
            if show_advanced or not advanced]


class CustomParserPanel(ctk.CTkFrame):
    """Pannello del costruttore di Parser Personalizzati — incassabile in finestra
    standalone (`CustomParserWindow`) o come scheda "🧩 Parser" di "🧰 Strumenti".
    `on_message` opzionale non usato: l'anteprima è interna (test-live)."""

    # Etichetta-sentinella quando non c'è nessun parser salvato.
    _NONE_SAVED = "(nessuno)"
    # Voce della tendina Modalità per il sentinella "" (parser legacy = eredita la
    # modalità globale). Mostrarla evita di convertire "" in NAME_ONLY salvando (Codex).
    _MODE_INHERIT = "(eredita globale)"
    # Voce della tendina Sport per il sentinella "" (parser agnostico, PR-P9).
    _SPORT_UNSPECIFIED = "(non specificato)"

    def _mode_to_label(self, mode: str) -> str:
        return self._MODE_INHERIT if not mode else mode

    def _label_to_mode(self, label: str) -> str:
        return "" if label == self._MODE_INHERIT else label

    def _sport_to_label(self, sport: str) -> str:
        return self._SPORT_UNSPECIFIED if not sport else sport

    def _label_to_sport(self, label: str) -> str:
        return "" if label == self._SPORT_UNSPECIFIED else label

    def _on_sport_change(self, _value=None):
        """Sport cambiato dal menu: aggiorna il builder (canonicalizzazione/fail-safe in
        `set_sport`). Non tocca le regole né l'obbligatorietà (lo sport non cambia le colonne).
        Aggiorna però le tendine MarketType/MarketName/SelectionName ai termini del nuovo sport."""
        self.builder.set_sport(self._label_to_sport(self._sport_var.get()))
        self._refresh_term_combos()

    # ── tendine Betfair MarketType/MarketName/SelectionName (#283 PR 13) ─────
    def _fetch_market_terms(self) -> dict:
        """Legge i valori permanenti mercato/selezione dal dizionario Betfair per lo sport
        CORRENTE del parser (best-effort). Provider assente/`DictionaryBusy` (sync in corso)/
        errore → liste vuote: nessun suggerimento ora, ma la tendina resta editabile (testo
        libero preservato, nessun blocco)."""
        empty = {"market_types": [], "market_names": [], "selection_names": []}
        if not callable(self._market_terms_provider):
            return dict(empty)
        from .betfair.dictionary_viewer import DictionaryBusy
        sport = self._label_to_sport(self._sport_var.get()) or None   # "" agnostico → tutti
        try:
            return self._market_terms_provider(sport) or dict(empty)
        except DictionaryBusy:
            return dict(empty)          # sync in corso: nessun suggerimento (niente freeze)
        except Exception:               # noqa: BLE001 — best-effort: nessun suggerimento
            return dict(empty)

    def _term_values(self, target: str) -> list:
        """Valori suggeriti (per lo sport corrente) per la tendina di `target`, dalla cache."""
        return list(self._market_terms.get(_BETFAIR_TERM_TARGETS.get(target, ""), []))

    def _refresh_term_combos(self):
        """Ricarica la cache dei termini e aggiorna i `values` delle tendine
        MarketType/MarketName/SelectionName **preservando** il valore digitato/selezionato
        (anche se non (ancora) sincronizzato: resta in lista)."""
        self._market_terms = self._fetch_market_terms()
        for refs in self._rows:
            combo = refs.get("term_combo")
            if combo is None:
                continue
            cur = refs["fixed_value"].get()
            vals = ["", *self._term_values(refs["target"])]
            if cur and cur not in vals:
                vals.append(cur)
            combo.configure(values=vals)

    # ── anagrafica Provider (PR-5) ─────────────────────────────────────────
    @staticmethod
    def _load_providers() -> list:
        """Nomi provider salvati (best-effort: una config illeggibile → lista vuota)."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            return []
        return provider_store.provider_names(cfg)

    def _add_provider(self):
        """Chiede un nome, lo salva nell'anagrafica (config) e aggiorna le tendine
        Provider. Persistenza indipendente come per la finestra Sorgenti."""
        dialog = ctk.CTkInputDialog(text=i18n.tr("Nome del nuovo Provider:"), title="Provider")
        name = (dialog.get_input() or "").strip()
        if not name:
            self._result.configure(text=i18n.tr("⛔ Provider non aggiunto (nome vuoto)."))
            return
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
            cfg = provider_store.add_provider(cfg, name)
            saved, ok = config_store.save_config(cfg, config_store.CONFIG_FILE)
        except Exception as exc:                 # noqa: BLE001
            self._result.configure(text=i18n.tr("❌ Errore salvataggio provider: {exc}").format(exc=exc))
            return
        # Sincronizza la config in memoria della GUI principale (no perdita provider).
        if ok and callable(self._on_saved):
            self._on_saved(saved)
        self._providers = provider_store.provider_names(saved if ok else cfg)
        self._sync_to_builder()                  # non perdere le modifiche correnti
        self._reload_rows_from_builder()         # ridisegna con la tendina aggiornata
        self._result.configure(
            text=i18n.tr("➕ Provider «{name}» salvato.").format(name=name) if ok
            else i18n.tr("⚠️ Provider «{name}» aggiunto solo in memoria (salvataggio fallito).").format(name=name))

    # ── mappatura nomi squadra (profili) ───────────────────────────────────
    @staticmethod
    def _load_name_mapping_profiles() -> list:
        """Nomi dei profili di mappatura salvati (best-effort: config illeggibile → [])."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            return []
        return name_mapping_store.profile_names(cfg)

    def _reload_profile_checks(self, use_builder=False):
        """Ridisegna le checkbox dei profili. La selezione spuntata viene da
        `builder.name_mapping_profiles` (su `use_builder`, es. caricamento di un parser)
        oppure dalle checkbox correnti (refresh dopo aver modificato il dizionario).

        Mostra i profili **esistenti** in config e, in più, i profili **selezionati ma non
        più esistenti** (rinominati/eliminati nel dizionario) come voci ⚠ **fantasma**:
        restano una checkbox così l'utente PUÒ togliere la spunta, e `_unresolved_selected`
        le intercetta per bloccare save/preview finché non sono risolte — niente
        riferimenti morti riscritti in silenzio né blocchi senza via d'uscita (Codex)."""
        if use_builder or not self._profile_checks:
            selected = list(self.builder.name_mapping_profiles)
        else:
            selected = self._selected_profiles()
        selected_set = set(selected)
        for child in self._profiles_box.winfo_children():
            child.destroy()
        self._profile_checks = {}
        existing = list(self._load_name_mapping_profiles())
        self._existing_profiles = set(existing)
        missing = [n for n in selected if n not in self._existing_profiles]
        names = existing + [n for n in missing if n not in existing]
        if not names:
            ctk.CTkLabel(self._profiles_box, text="(nessun profilo)",
                         text_color="gray").pack(side="left", padx=4)
            self._update_translations_status()   # indicatore ✓/— (#293)
            return
        for name in names:
            present = name in self._existing_profiles
            var = ctk.BooleanVar(value=name in selected_set)
            kw = {} if present else {"text_color": "#ffa726"}   # ⚠ profilo mancante
            # `command` (#293): al toggle aggiorna l'indicatore «🔗 Traduzioni attive».
            ctk.CTkCheckBox(self._profiles_box, text=name if present else f"⚠ {name}",
                            variable=var, width=20, command=self._update_translations_status,
                            **kw).pack(side="left", padx=4)
            self._profile_checks[name] = var
        self._update_translations_status()   # indicatore ✓/— (#293)

    def _selected_profiles(self) -> list:
        """Profili spuntati, **preservando l'ordine** scelto nel parser: l'ordine dei
        profili è significativo (in `resolve_team` vince la prima corrispondenza), quindi
        i profili già presenti in `builder.name_mapping_profiles` mantengono la loro
        posizione; gli eventuali profili appena spuntati si aggiungono in coda (ordine di
        visualizzazione). Così aprire e ri-salvare un parser con profili ['B','A'] NON li
        riordina alfabeticamente cambiando la precedenza (Codex P1). I profili mancanti
        (⚠) selezionati restano inclusi: hanno una checkbox e `_unresolved_selected` li
        blocca finché non sono risolti."""
        checked = {name for name, var in self._profile_checks.items() if var.get()}
        ordered = [n for n in self.builder.name_mapping_profiles if n in checked]
        ordered += [n for n in self._profile_checks if n in checked and n not in ordered]
        return ordered

    def _unresolved_selected(self) -> list:
        """Profili selezionati che non corrispondono a un profilo ESISTENTE (voci ⚠
        fantasma). Finché ce ne sono spuntati, `_save`/`_test` si bloccano: una mappatura
        richiesta ma non risolvibile non deve diventare in silenzio «nessuna mappatura»."""
        return [n for n in self._selected_profiles() if n not in self._existing_profiles]

    @staticmethod
    def _resolve_mapping_profiles(defn):
        """Righe dei profili del parser risolte dalla config (per l'anteprima), o None
        se il parser non usa la mappatura. Best-effort: config illeggibile → lista vuota
        (mappatura richiesta ma irrisolvibile → l'anteprima farà fail-closed)."""
        if not defn.name_mapping_profiles:
            return None
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            cfg = {}
        return name_mapping_store.entries_for_profiles(cfg, defn.name_mapping_profiles)

    def _open_name_mapping(self):
        """Apre il Dizionario nomi (finestra separata). Alla chiusura/salvataggio
        ricarica le checkbox dei profili così le nuove voci compaiono subito."""
        self._sync_to_builder()              # non perdere selezione/separatore correnti
        from .name_mapping_gui import NameMappingWindow

        def _on_saved(new_cfg):
            if callable(self._on_saved):
                self._on_saved(new_cfg)
            self._reload_profile_checks()

        win = NameMappingWindow(self, on_saved=_on_saved)
        win.focus()

    # ── mappatura mercati (profili) ────────────────────────────────────────
    # Speculare alla mappatura nomi sopra, ma su `market_mapping_profiles` /
    # `market_mapping_store` (config `market_mappings`). Metodi PARALLELI dedicati (non un
    # refactor di quelli nomi) per non rischiare regressioni sul path nomi già collaudato.
    @staticmethod
    def _load_market_mapping_profiles() -> list:
        """Nomi dei profili mercati salvati (best-effort: config illeggibile → [])."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            return []
        return market_mapping_store.profile_names(cfg)

    def _reload_market_profile_checks(self, use_builder=False):
        """Ridisegna le checkbox dei profili MERCATI. La selezione viene da
        `builder.market_mapping_profiles` (su `use_builder`) o dalle checkbox correnti.

        Come per i nomi, mostra i profili esistenti + i selezionati ma non più esistenti
        come voci ⚠ **fantasma**: restano una checkbox da togliere e `_unresolved_market_selected`
        le intercetta per bloccare save/preview, così un profilo mercati rinominato/eliminato
        non viene riscritto stantio nel parser (→ MARKET_MAPPING_MISSING, Codex P2)."""
        if use_builder or not self._market_profile_checks:
            selected = list(self.builder.market_mapping_profiles)
        else:
            selected = self._selected_market_profiles()
        selected_set = set(selected)
        for child in self._market_profiles_box.winfo_children():
            child.destroy()
        self._market_profile_checks = {}
        existing = list(self._load_market_mapping_profiles())
        self._existing_market_profiles = set(existing)
        missing = [n for n in selected if n not in self._existing_market_profiles]
        names = existing + [n for n in missing if n not in existing]
        if not names:
            ctk.CTkLabel(self._market_profiles_box, text="(nessun profilo)",
                         text_color="gray").pack(side="left", padx=4)
            self._update_translations_status()   # indicatore ✓/— (#293)
            return
        for name in names:
            present = name in self._existing_market_profiles
            var = ctk.BooleanVar(value=name in selected_set)
            kw = {} if present else {"text_color": "#ffa726"}   # ⚠ profilo mancante
            # `command` (#293): al toggle aggiorna l'indicatore «🔗 Traduzioni attive».
            ctk.CTkCheckBox(self._market_profiles_box, text=name if present else f"⚠ {name}",
                            variable=var, width=20, command=self._update_translations_status,
                            **kw).pack(side="left", padx=4)
            self._market_profile_checks[name] = var
        self._update_translations_status()   # indicatore ✓/— (#293)

    def _set_translation_status(self, lbl, count: int) -> None:
        """Aggiorna un'etichetta indicatore «🔗 Traduzioni attive» (#293): testo `✓ N attive`/
        `— nessuna` (via `_translations_status_text`) e colore verde/grigio. No-op difensivo se
        l'etichetta non è ancora costruita."""
        if lbl is not None:
            lbl.configure(text=_translations_status_text(count),
                          text_color=(_TRANSLATION_ON_COLOR if count > 0 else _TRANSLATION_OFF_COLOR))

    def _update_translations_status(self) -> None:
        """Ricalcola gli indicatori ✓/— (Nomi/Mercati) dai profili selezionati (#293). Chiamato
        al reload delle checkbox e a ogni toggle. Difensivo sull'ordine di costruzione: aggiorna
        un indicatore solo se la sua etichetta E il suo dizionario di checkbox esistono già.

        Conta SOLO i profili selezionati **e risolti** (esistenti): un profilo fantasma ⚠
        selezionato NON è una traduzione realmente attiva (è bloccato da `_unresolved_*` e non
        applica alcuna mappatura), quindi non gonfia il conteggio dell'indicatore (Fable #336)."""
        # `getattr(..., set())` sui set «risolti» come per le etichette/dizionari: se un ordine di
        # costruzione o un reload parziale non li ha ancora inizializzati, si conta 0 (nessuna
        # traduzione attiva) invece di sollevare `AttributeError` al toggle (GPT/GLM/Fable #336).
        if self.__dict__.get("_nm_status_lbl") is not None and "_profile_checks" in self.__dict__:
            existing = getattr(self, "_existing_profiles", set())
            active = [p for p in self._selected_profiles() if p in existing]
            self._set_translation_status(self._nm_status_lbl, len(active))
        if self.__dict__.get("_mm_status_lbl") is not None and "_market_profile_checks" in self.__dict__:
            existing_m = getattr(self, "_existing_market_profiles", set())
            active_m = [p for p in self._selected_market_profiles() if p in existing_m]
            self._set_translation_status(self._mm_status_lbl, len(active_m))

    def _selected_market_profiles(self) -> list:
        """Profili mercati spuntati, preservando l'ordine scelto nel parser (come i nomi):
        i profili già in `builder.market_mapping_profiles` mantengono la posizione, i nuovi
        si aggiungono in coda. I profili ⚠ mancanti selezionati restano inclusi (li blocca
        `_unresolved_market_selected`)."""
        checked = {name for name, var in self._market_profile_checks.items() if var.get()}
        ordered = [n for n in self.builder.market_mapping_profiles if n in checked]
        ordered += [n for n in self._market_profile_checks if n in checked and n not in ordered]
        return ordered

    def _unresolved_market_selected(self) -> list:
        """Profili mercati selezionati che non esistono più (voci ⚠ fantasma): finché ce ne
        sono spuntati, `_save`/`_test` si bloccano — una mappatura mercati richiesta ma non
        risolvibile non deve diventare in silenzio «nessuna mappatura»."""
        return [n for n in self._selected_market_profiles()
                if n not in self._existing_market_profiles]

    @staticmethod
    def _resolve_market_mapping_profiles(defn):
        """Voci dei profili mercati del parser risolte dalla config (per l'anteprima), o
        None se il parser non usa la mappatura mercati. Best-effort: config illeggibile →
        lista vuota.

        Semantica `None` vs `[]` (Sourcery): per i mercati la distinzione NON è significativa
        a valle. Il runtime (`custom_pipeline.build_validated_row`) attiva l'hook in base a
        `defn.market_mapping_profiles` (i NOMI dei profili scelti nel parser) e coalizza
        l'argomento con `market_mapping_profiles or []`: quindi `None` e `[]` producono lo
        stesso percorso (`resolve_market(text, [])` → stato "none"). Il fail-closed dei
        mercati è «nessuna frase combacia **e** nessun mercato dalle regole-colonna →
        `MARKET_MAPPING_MISSING`» (mode-aware), valido in entrambi i casi. È un'asimmetria
        VOLUTA rispetto ai nomi: i nomi DEVONO essere tradotti (config illeggibile ⇒
        `MAPPING_MISSING`, fail-closed), i mercati invece ripiegano sulla regola-colonna
        quando nessuna frase combacia (precedenza D1). `entries_for_profiles` ignora da sé i
        profili assenti, quindi una config illeggibile non scrive mai un mercato a caso."""
        if not defn.market_mapping_profiles:
            return None
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:                       # noqa: BLE001 — fallback sicuro
            cfg = {}
        return market_mapping_store.entries_for_profiles(cfg, defn.market_mapping_profiles)

    def _open_market_mapping(self):
        """Apre il Dizionario mercati (finestra separata). Alla chiusura/salvataggio
        ricarica le checkbox dei profili mercati così le nuove voci compaiono subito."""
        self._sync_to_builder()              # non perdere selezione corrente
        from .name_mapping_gui import MarketMappingWindow

        def _on_saved(new_cfg):
            if callable(self._on_saved):
                self._on_saved(new_cfg)
            self._reload_market_profile_checks()

        win = MarketMappingWindow(self, on_saved=_on_saved)
        win.focus()

    def __init__(self, master=None, builder: ParserBuilder = None, provider: str = "",
                 global_mode: str = "", on_saved=None, id_resolver_factory=None,
                 market_terms_provider=None):
        super().__init__(master)
        is_new = builder is None
        self.builder = builder or ParserBuilder()
        self._provider = provider
        # Provider OPZIONALE dei valori permanenti mercato/selezione del dizionario Betfair
        # (#283 PR 13): `callable(sport) -> {"market_types", "market_names", "selection_names"}`
        # (l'app passa `App._known_market_terms`). Popola le tendine editabili delle righe
        # MarketType/MarketName/SelectionName. Assente/fallisce → nessun suggerimento, testo
        # libero preservato (nessuna regressione fail-closed).
        self._market_terms_provider = market_terms_provider
        self._market_terms = {}   # cache per lo sport corrente (riempita al reload righe)
        # Factory OPZIONALE del dizionario Betfair per l'anteprima (#192, Codex): un callable
        # `() -> id_resolver | None` (l'app passa `App._betfair_id_resolver`). Serve a rendere
        # «Prova messaggio» EQUIVALENTE al runtime per i parser ID_ONLY che risolvono gli ID dal
        # dizionario; se assente/fallisce, l'anteprima resta conservativa/fail-closed (invariato).
        self._id_resolver_factory = id_resolver_factory
        # Callback opzionale: dopo aver salvato l'anagrafica Provider su config.json,
        # sincronizza la config in memoria della GUI principale (vedi `app`), così un
        # successivo Salva/Avvia non riscrive il file perdendo i provider (Codex).
        self._on_saved = on_saved
        # Modalità globale (config `recognition_mode`): usata SOLO per l'anteprima di un
        # parser legacy a eredità ("" ): così "Prova messaggio" combacia col runtime (Codex).
        self._global_mode = global_mode
        self._rows = []  # widget refs per regola
        self._saved_map = {}  # etichetta menu → path file parser
        # #293 «densità parser»: colonne avanzate (Trasformazione/Value-map) nascoste di default;
        # il toggle «Avanzate» le mostra. Interruttore letto da _add_row/_populate_rules_header.
        self._show_advanced = False

        self._transforms = self.builder.transform_options()
        self._value_maps = self.builder.value_map_options(include_dizionario=True)
        self._modes = self.builder.mode_options()
        self._providers = self._load_providers()   # anagrafica Provider (PR-5)

        # Parser NUOVO: applica l'auto-Obblig. della modalità di default UNA volta (per i
        # parser caricati invece si preservano i flag salvati: niente set_mode al reload).
        # `apply_mode_defaults` crea PRIMA le 14 colonne e POI allinea la modalità: senza,
        # set_mode su un builder senza regole non marcherebbe i campi come Obblig. (Codex #72).
        if is_new and self.builder.mode:
            self.builder.apply_mode_defaults(self.builder.mode)
        self._build_ui()
        self._reload_rows_from_builder()
        self._refresh_saved()

    def refresh_options(self):
        """Aggiorna le LISTE-OPZIONI derivate dal config SENZA toccare il parser in
        costruzione (preserva le modifiche): provider del menu colonna Provider, modalità
        globale per l'anteprima, checkbox dei profili di mappatura (le spunte restano).

        Chiamato quando questa scheda torna attiva nella hub "🧰 Strumenti": un provider o
        un profilo aggiunto/rimosso in un'altra scheda, o un cambio profilo, si riflette
        subito senza riaprire Strumenti (Codex). Best-effort: config illeggibile → no-op."""
        try:
            cfg = config_store.load_config(config_store.CONFIG_FILE)
        except Exception:               # noqa: BLE001 — config illeggibile: niente refresh
            return
        self._providers = provider_store.provider_names(cfg)
        self._global_mode = str(cfg.get("recognition_mode", "")).strip()
        # Anche il provider per l'anteprima: `_test()` passa `self._provider` a
        # test_message()/diagnose(); senza aggiornarlo, un parser con colonna Provider non
        # fissa verrebbe provato col provider VECCHIO dopo un cambio profilo (Codex).
        self._provider = str(cfg.get("provider", "")).strip()
        for refs in self._rows:
            menu = refs.get("provider_menu")
            if menu is not None:
                cur = refs["fixed_value"].get()
                vals = ["", *self._providers]
                if cur and cur not in vals:
                    vals.append(cur)        # preserva la selezione anche se rimossa dall'anagrafica
                menu.configure(values=vals)
        self._reload_profile_checks()       # ricarica i profili mapping dal disco (spunte preservate)
        self._reload_market_profile_checks()  # idem per i profili mercati
        self._refresh_term_combos()         # #283 PR 13: una sync nel frattempo può aver aggiunto termini

    # ── costruzione UI ─────────────────────────────────────────────────────
    def _build_ui(self):
        # TUTTA la finestra dentro UN solo contenitore scrollabile: il contenuto è alto
        # (griglia 14 colonne + area test + diagnostica) e prima, su schermi piccoli, la
        # parte bassa finiva fuori finestra senza modo di raggiungerla. Con lo scroll
        # esterno ogni sezione resta sempre raggiungibile. Le sezioni interne che prima
        # avevano uno scroll proprio (griglia regole, tabella diagnostica) diventano frame
        # semplici: lo scorrimento lo gestisce SOLO questo contenitore (niente scroll
        # verticali annidati, che si rubavano la rotellina a vicenda).
        outer = ctk.CTkScrollableFrame(self)
        outer.pack(fill="both", expand=True)
        self._outer = outer

        top = ctk.CTkFrame(outer, fg_color="transparent")
        top.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(top, text=i18n.tr("Nome parser:")).pack(side="left", padx=6)
        self._name_var = ctk.StringVar(value=self.builder.name)
        ctk.CTkEntry(top, textvariable=self._name_var, width=240).pack(side="left", padx=6)
        ctk.CTkLabel(top, text=i18n.tr("Modalità:")).pack(side="left", padx=6)
        # Modalità DEL PARSER (per-parser): scegliendola, i campi di riconoscimento del
        # set diventano obbligatori da soli (set_mode → auto-Obblig.). La voce
        # "(eredita globale)" rappresenta "" (parser legacy): usa la modalità globale.
        self._mode_var = ctk.StringVar(value=self._mode_to_label(self.builder.mode))
        ctk.CTkOptionMenu(top, variable=self._mode_var,
                          values=[self._MODE_INHERIT, *self._modes], width=160,
                          command=self._on_mode_change).pack(side="left", padx=6)
        # Sport DEL PARSER (PR-P9): Calcio/Tennis/Basket/Rugby Union o "(non specificato)"
        # (= agnostico). Non cambia le colonne CSV; nelle PR successive restringe la
        # risoluzione degli ID Betfair all'event_type_id corretto.
        ctk.CTkLabel(top, text=i18n.tr("Sport:")).pack(side="left", padx=6)
        self._sport_var = ctk.StringVar(value=self._sport_to_label(self.builder.sport))
        ctk.CTkOptionMenu(top, variable=self._sport_var,
                          values=[self._sport_to_label(s) for s in self.builder.sport_options()],
                          width=150, command=self._on_sport_change).pack(side="left", padx=6)
        # Anagrafica Provider (PR-5): aggiungi un nome riusabile nella tendina della
        # colonna Provider (sotto). I provider salvati valgono per tutti i parser.
        ctk.CTkButton(top, text=i18n.tr("➕ Provider"), width=110,
                      command=self._add_provider).pack(side="left", padx=6)

        # gestione parser salvati: lista + nuovo / carica / duplica / elimina
        manage = ctk.CTkFrame(outer, fg_color="transparent")
        manage.pack(fill="x", padx=10, pady=(0, 6))
        ctk.CTkLabel(manage, text=i18n.tr("Parser salvati:")).pack(side="left", padx=6)
        self._saved_var = ctk.StringVar(value=self._NONE_SAVED)
        self._saved_menu = ctk.CTkOptionMenu(
            manage, variable=self._saved_var, values=[self._NONE_SAVED], width=220)
        self._saved_menu.pack(side="left", padx=6)
        ctk.CTkButton(manage, text=i18n.tr("🆕 Nuovo"), width=90, command=self._new).pack(side="left", padx=3)
        ctk.CTkButton(manage, text=i18n.tr("📂 Carica"), width=90, command=self._load_selected).pack(side="left", padx=3)
        ctk.CTkButton(manage, text=i18n.tr("📑 Duplica"), width=90, command=self._duplicate_selected).pack(side="left", padx=3)
        ctk.CTkButton(manage, text=i18n.tr("🗑 Elimina"), width=90, fg_color="#7f0000",
                      command=self._delete_selected).pack(side="left", padx=3)

        # Catalogo XTrader (B2): scegli Mercato → Selezione (solo NON dinamici) e
        # inseriscili come regole FISSE, senza digitare i nomi canonici a mano.
        cat = ctk.CTkFrame(outer, fg_color="transparent")
        cat.pack(fill="x", padx=10, pady=(0, 6))
        ctk.CTkLabel(cat, text=i18n.tr("Catalogo XTrader:")).pack(side="left", padx=6)
        self._markets = self.builder.market_options()
        self._cat_market = ctk.StringVar(value=self._markets[0] if self._markets else "")
        self._cat_market_menu = ctk.CTkOptionMenu(
            cat, variable=self._cat_market, values=self._markets or [""],
            width=240, command=self._on_market_change)
        self._cat_market_menu.pack(side="left", padx=4)
        self._cat_selection = ctk.StringVar(value="")
        self._cat_selection_menu = ctk.CTkOptionMenu(
            cat, variable=self._cat_selection, values=[""], width=240)
        self._cat_selection_menu.pack(side="left", padx=4)
        ctk.CTkButton(cat, text=i18n.tr("➕ Inserisci regole fisse"), width=180,
                      command=self._insert_fixed_market).pack(side="left", padx=4)
        self._refresh_selection_menu()   # popola le selezioni del mercato iniziale

        # «🔗 Traduzioni attive per questo parser» (#293): raggruppa le mappature Nomi + Mercati
        # (già presenti) sotto un unico riquadro etichettato, con un indicatore di stato ✓/— per
        # tipo (✓ N attive = profili selezionati, — nessuna = nessuno). Nessun cambio funzionale:
        # le checkbox profili e i pulsanti «apri Dizionario» restano quelli di prima; solo la
        # presentazione cambia (le mappature vivono accanto al parser, dove si accendono).
        trad = ctk.CTkFrame(outer)
        trad.pack(fill="x", padx=10, pady=(2, 6))
        ctk.CTkLabel(trad, text=i18n.tr("🔗 Traduzioni attive per questo parser"),
                     font=ctk.CTkFont(size=12, weight="bold")).pack(anchor="w", padx=8, pady=(4, 0))

        # Mappatura nomi squadra: separatore casa/trasferta del canale + profili
        # (checkbox multi-selezione) che traducono l'EventName provider → Betfair/XTrader.
        nm = ctk.CTkFrame(trad, fg_color="transparent")
        nm.pack(fill="x", padx=6, pady=(0, 4))
        ctk.CTkLabel(nm, text=i18n.tr("Nomi squadra · separatore:")).pack(side="left", padx=6)
        self._separator_var = ctk.StringVar(value=self.builder.team_separator)
        ctk.CTkEntry(nm, textvariable=self._separator_var, width=70,
                     placeholder_text="v").pack(side="left", padx=2)
        ctk.CTkButton(nm, text=i18n.tr("🗺️ Dizionario nomi"), width=160,
                      command=self._open_name_mapping).pack(side="left", padx=6)
        self._nm_status_lbl = ctk.CTkLabel(nm, text=i18n.tr("— nessuna"), width=92, anchor="w")
        self._nm_status_lbl.pack(side="left", padx=(8, 2))
        self._profiles_box = ctk.CTkScrollableFrame(nm, height=42, orientation="horizontal")
        self._profiles_box.pack(side="left", fill="x", expand=True, padx=4)
        self._profile_checks = {}        # nome profilo → BooleanVar
        self._existing_profiles = set()  # profili realmente presenti in config (non ⚠)
        self._reload_profile_checks()

        # Mappatura mercati a frase: profili (checkbox multi-selezione) che traducono una
        # frase-mercato del messaggio nel Mercato/Selezione XTrader (market_mapping_store).
        mm = ctk.CTkFrame(trad, fg_color="transparent")
        mm.pack(fill="x", padx=6, pady=(0, 6))
        ctk.CTkLabel(mm, text=i18n.tr("Mercati:")).pack(side="left", padx=6)
        ctk.CTkButton(mm, text=i18n.tr("🎯 Dizionario mercati"), width=170,
                      command=self._open_market_mapping).pack(side="left", padx=6)
        self._mm_status_lbl = ctk.CTkLabel(mm, text=i18n.tr("— nessuna"), width=92, anchor="w")
        self._mm_status_lbl.pack(side="left", padx=(8, 2))
        self._market_profiles_box = ctk.CTkScrollableFrame(mm, height=42, orientation="horizontal")
        self._market_profiles_box.pack(side="left", fill="x", expand=True, padx=4)
        self._market_profile_checks = {}        # nome profilo mercati → BooleanVar
        self._existing_market_profiles = set()  # profili mercati realmente in config (non ⚠)
        self._reload_market_profile_checks()

        # Output multi-riga (#192): un solo messaggio → più righe CSV. Due interruttori
        # indipendenti (MultiMarket = più mercati diversi della stessa partita; MultiSelection
        # = più selezioni dello stesso mercato) + righe dinamiche [+]/[Rimuovi]. Spento =
        # single-row come prima. La logica (round-trip, anteprima) sta nel controller, testata
        # in CI; qui SOLO i widget. Il banner sotto avvisa quando entrambi attivi (righe
        # separate, non cartesiane).
        self._build_multi_section(outer)

        # Condizioni di gate (PR-1): il parser scatta SOLO se il messaggio soddisfa le
        # condizioni (contiene/NON contiene ⟨testo⟩; modo E/O). Solo widget; lo stato vive nel
        # ParserBuilder (round-trip testato in CI). Serve a far agire un parser solo sui
        # messaggi pertinenti (filtro fail-closed), es. «un mercato diverso per scenario».
        self._build_conditions_section(outer)

        # Toggle «Avanzate» (#293 densità parser): mostra/nasconde le colonne Trasformazione e
        # Value-map, tenendo di default la tabella più leggibile (solo colonne essenziali).
        adv_bar = ctk.CTkFrame(outer, fg_color="transparent")
        adv_bar.pack(fill="x", padx=10, pady=(4, 0))
        self._advanced_var = ctk.BooleanVar(value=self._show_advanced)
        ctk.CTkCheckBox(adv_bar, text=i18n.tr("⚙️ Avanzate (Trasformazione · Value-map)"),
                        variable=self._advanced_var,
                        command=self._on_toggle_advanced).pack(side="left", padx=2)

        # intestazione colonne — le colonne avanzate compaiono solo in modalità «Avanzate».
        self._rules_head = ctk.CTkFrame(outer, fg_color="transparent")
        self._rules_head.pack(fill="x", padx=10)
        self._populate_rules_header()

        self._rows_frame = ctk.CTkFrame(outer, fg_color="transparent")
        self._rows_frame.pack(fill="x", padx=10, pady=6)

        actions = ctk.CTkFrame(outer, fg_color="transparent")
        actions.pack(fill="x", padx=10, pady=4)
        ctk.CTkButton(actions, text=i18n.tr("💾 Salva"), command=self._save).pack(side="left", padx=4)
        ctk.CTkButton(actions, text=i18n.tr("🧪 Prova messaggio"), command=self._test).pack(side="left", padx=4)
        # Tester multiplo (#311 §3.2): N messaggi reali separati da righe «---».
        ctk.CTkButton(actions, text=i18n.tr("🧪🧪 Prova più messaggi (separati da ---)"),
                      command=self._test_batch).pack(side="left", padx=4)
        ctk.CTkButton(actions, text=i18n.tr("📋 Copia diagnostica"), command=self._copy_diag).pack(side="left", padx=4)

        # test-live
        test = ctk.CTkFrame(outer, fg_color="transparent")
        test.pack(fill="x", padx=10, pady=6)
        ctk.CTkLabel(test, text=i18n.tr("Messaggio di prova:")).pack(anchor="w", padx=6)
        self._msg_box = ctk.CTkTextbox(test, height=120)
        self._msg_box.pack(fill="x", padx=6, pady=4)
        self._result = ctk.CTkLabel(test, text="", anchor="w", justify="left")
        self._result.pack(fill="x", padx=6, pady=4)
        # Anteprima multi-riga (#192): TABELLA con UNA riga per ogni riga CSV generata
        # (base, oppure le righe MultiMarket/MultiSelection), col verdetto per-riga. Resta
        # vuota finché non si preme «Prova messaggio».
        ctk.CTkLabel(test, text=i18n.tr("Anteprima righe generate (#192):")).pack(anchor="w", padx=6)
        self._preview_table = ctk.CTkFrame(test, fg_color="transparent")
        self._preview_table.pack(fill="x", padx=6, pady=(0, 4))
        # Larghezze colonne della tabella anteprima (px), in ordine.
        self._preview_cols = (("#", 30), ("Tipo", 90), ("Esito", 70),
                              ("Riga CSV (campi valorizzati)", 560))
        # Diagnostica per-campo (CP-08b): TABELLA — perché "Non pronto", colonna per colonna.
        ctk.CTkLabel(test, text=i18n.tr("Diagnostica (una riga per colonna):")).pack(anchor="w", padx=6)
        self._diag_table = ctk.CTkFrame(test, fg_color="transparent")
        self._diag_table.pack(fill="x", padx=6, pady=(0, 4))
        # Larghezze colonne della tabella diagnostica (px), in ordine.
        self._diag_cols = (("Colonna", 110), ("Stato", 64), ("Motivo", 280),
                           ("Inizia dopo", 120), ("Finisce prima", 120), ("Valore estratto", 170))
        self._last_report = ""   # testo per "Copia diagnostica"

    # ── output multi-riga (#192): MultiMarket / MultiSelection ──────────────
    # Campi editabili di UNA riga multi (attributo MultiRowRule → etichetta, larghezza px).
    # Un campo vuoto EREDITA dalla riga base; per MultiSelection di norma basta «Selezione».
    _MULTI_FIELDS = (
        ("market_type", "Tipo mercato", 150),
        ("market_name", "Mercato", 160),
        ("selection_name", "Selezione", 150),
        ("price", "Quota", 70),
        ("bet_type", "BetType", 90),
        ("handicap", "Handicap", 90),
    )
    # Campi delle sole righe SELEZIONE (#325 slice 2): in più i delimitatori «Inizia dopo/
    # Finisce prima» dell'estrazione dinamica dei risultati esatti (Selezione VUOTA +
    # delimitatori su mercato CORRECT_SCORE/HALF_TIME_SCORE → una riga per punteggio).
    # NON esposti sulle righe MERCATO (lì sono solo misconfigurazione: il runtime li ignora
    # per design, gate #341) — dove restano preservati come campi nascosti (Codex P1).
    _MULTI_SELECTION_FIELDS = _MULTI_FIELDS + (
        ("start_after", "Inizia dopo", 110),
        ("end_before", "Finisce prima", 110),
    )
    # Guard anti-rientranza di `_reload_multi_from_builder` (Fable/Fugu #348): default di
    # CLASSE (non attributo d'istanza creato altrove) così `_refresh_multi_warnings` può
    # leggerlo sempre senza passare dal `__getattr__` ricorsivo di Tk su attributo mancante
    # (stessa trappola del lock di istanza, #346).
    _multi_reloading = False

    def _build_multi_section(self, outer):
        """Sezione output multi-riga: due interruttori + due liste di righe dinamiche.
        Solo widget; lo stato vive nel `ParserBuilder` (round-trip/anteprima testati in CI)."""
        sec = ctk.CTkFrame(outer)
        sec.pack(fill="x", padx=10, pady=(8, 4))
        ctk.CTkLabel(sec, text=i18n.tr("Output multi-riga (un messaggio → più righe CSV)"),
                     font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=8, pady=(6, 2))

        # MultiMarket: più mercati diversi della stessa partita.
        mk = ctk.CTkFrame(sec, fg_color="transparent")
        mk.pack(fill="x", padx=8, pady=2)
        self._multi_market_var = ctk.BooleanVar(value=bool(self.builder.multi_market_enabled))
        ctk.CTkCheckBox(mk, text="MultiMarket (più mercati)", variable=self._multi_market_var,
                        command=self._on_multi_toggle).pack(side="left", padx=4)
        ctk.CTkButton(mk, text=i18n.tr("➕ Aggiungi mercato"), width=160,
                      command=self._add_multi_market_clicked).pack(side="left", padx=6)
        self._multi_markets_box = ctk.CTkFrame(sec, fg_color="transparent")
        self._multi_markets_box.pack(fill="x", padx=8, pady=(0, 4))
        self._multi_market_rows = []     # refs per riga MultiMarket

        # MultiSelection: più selezioni dello stesso mercato (eredita il mercato dalla base).
        ms = ctk.CTkFrame(sec, fg_color="transparent")
        ms.pack(fill="x", padx=8, pady=2)
        self._multi_selection_var = ctk.BooleanVar(value=bool(self.builder.multi_selection_enabled))
        ctk.CTkCheckBox(ms, text="MultiSelection (più selezioni)",
                        variable=self._multi_selection_var,
                        command=self._on_multi_toggle).pack(side="left", padx=4)
        ctk.CTkButton(ms, text=i18n.tr("➕ Aggiungi selezione"), width=160,
                      command=self._add_multi_selection_clicked).pack(side="left", padx=6)
        self._multi_selections_box = ctk.CTkFrame(sec, fg_color="transparent")
        self._multi_selections_box.pack(fill="x", padx=8, pady=(0, 4))
        self._multi_selection_rows = []  # refs per riga MultiSelection
        # Hint estrazione dinamica (#325): spiega la combinazione che attiva una-riga-per-risultato.
        ctk.CTkLabel(
            sec, anchor="w", justify="left", font=ctk.CTkFont(size=10),
            text=("💡 Selezione VUOTA + «Inizia dopo/Finisce prima» = estrazione dinamica dei "
                  "risultati esatti dal messaggio (una riga per punteggio «N - N»; solo mercati "
                  "CORRECT_SCORE / HALF_TIME_SCORE).")).pack(fill="x", padx=8, pady=(0, 2))

        # Banner avvisi (es. entrambi attivi → righe separate, non cartesiane).
        self._multi_warn = ctk.CTkLabel(sec, text="", anchor="w", justify="left",
                                        text_color="#ffa726")
        self._multi_warn.pack(fill="x", padx=8, pady=(0, 6))

    def _add_multi_row_widget(self, container, refs_list, rule, fields=None):
        """Disegna UNA riga multi editabile (campi `fields` + abilitata + Rimuovi).

        `fields` = tupla colonne della riga: `_MULTI_FIELDS` (default, righe MERCATO) o
        `_MULTI_SELECTION_FIELDS` (righe SELEZIONE, con i delimitatori #325)."""
        fields = fields or self._MULTI_FIELDS
        row = ctk.CTkFrame(container, fg_color="transparent")
        row.pack(fill="x", pady=2)
        # Conserva la regola SORGENTE: i campi NON esposti nella GUI di QUESTA riga
        # (min_price/max_price/points sempre; start_after/end_before sulle righe MERCATO)
        # vanno PRESERVATI al salvataggio, altrimenti aprire+salvare un parser azzererebbe
        # in silenzio quei vincoli per-riga cambiando le righe CSV emesse (Codex P1).
        # `_multi_rule_from_refs` riparte da una copia di questa regola e applica SOLO i
        # campi in `_fields`.
        refs = {"frame": row, "_rule": rule, "_fields": fields}
        for attr, label, w in fields:
            cell = ctk.CTkFrame(row, fg_color="transparent")
            cell.pack(side="left", padx=2)
            ctk.CTkLabel(cell, text=label, width=w, anchor="w",
                         font=ctk.CTkFont(size=10)).pack(anchor="w")
            entry = ctk.CTkEntry(cell, width=w)
            entry.insert(0, getattr(rule, attr, "") or "")
            entry.pack()
            # Il banner avvisi ora ha avvisi PER-RIGA che dipendono dal testo digitato
            # (Selezione fissa + delimitatori, mercato non-punteggio): si aggiorna quando
            # l'utente lascia il campo, non solo su aggiungi/rimuovi/toggle.
            entry.bind("<FocusOut>", lambda _e: self._refresh_multi_warnings())
            refs[attr] = entry
        refs["enabled"] = ctk.BooleanVar(value=bool(getattr(rule, "enabled", True)))
        ctk.CTkCheckBox(row, text=i18n.tr("Attiva"), variable=refs["enabled"], width=40,
                        command=self._refresh_multi_warnings).pack(
            side="left", padx=6)
        ctk.CTkButton(row, text=i18n.tr("🗑 Rimuovi"), width=90, fg_color="#7f0000",
                      command=lambda: self._remove_multi_row(refs_list, refs)).pack(
                          side="left", padx=4)
        refs_list.append(refs)

    def _remove_multi_row(self, refs_list, refs):
        """Toglie una riga multi (widget + ref) e aggiorna il banner avvisi."""
        try:
            refs_list.remove(refs)
        except ValueError:
            pass
        refs["frame"].destroy()
        self._refresh_multi_warnings()

    def _add_multi_market_clicked(self):
        """[+] mercato: spunta MultiMarket (se serve) e aggiunge una riga vuota."""
        self._multi_market_var.set(True)
        self._add_multi_row_widget(self._multi_markets_box, self._multi_market_rows,
                                   MultiRowRule())
        self._refresh_multi_warnings()

    def _add_multi_selection_clicked(self):
        """[+] selezione: spunta MultiSelection (se serve) e aggiunge una riga vuota
        (con i campi delimitatori #325)."""
        self._multi_selection_var.set(True)
        self._add_multi_row_widget(self._multi_selections_box, self._multi_selection_rows,
                                   MultiRowRule(), fields=self._MULTI_SELECTION_FIELDS)
        self._refresh_multi_warnings()

    def _on_multi_toggle(self):
        """Interruttore MultiMarket/MultiSelection cambiato: aggiorna solo il banner avvisi
        (lo stato si legge dai widget in `_sync_to_builder`)."""
        self._refresh_multi_warnings()

    def _reload_multi_from_builder(self):
        """Ridisegna interruttori + righe multi dal builder (caricamento parser/nuovo).

        Le liste refs vengono SVUOTATE prima di distruggere i frame (CodeRabbit #348):
        distruggere un entry che ha il focus può far scattare il suo `<FocusOut>` →
        `_refresh_multi_warnings` in modo rientrante; con le liste già vuote il sync non
        legge widget mezzi distrutti né resuscita righe stantie (stesso ordine di
        `_remove_multi_row`: prima via dalla lista, poi destroy).

        In più, per TUTTA la durata del reload il flag `_multi_reloading` rende
        `_refresh_multi_warnings` un no-op (Fable #348): senza il guard, un refresh
        rientrante durante i destroy vedrebbe le liste GUI vuote e SOVRASCRIVEREBBE
        `builder.multi_markets/multi_selections` con `[]` PRIMA dei loop di ricostruzione
        qui sotto (che iterano proprio su quelle liste del builder) → perdita silenziosa
        delle righe multi al caricamento di un parser."""
        self._multi_reloading = True
        try:
            self._multi_market_var.set(bool(self.builder.multi_market_enabled))
            self._multi_selection_var.set(bool(self.builder.multi_selection_enabled))
            old_market_rows, self._multi_market_rows = self._multi_market_rows, []
            old_selection_rows, self._multi_selection_rows = self._multi_selection_rows, []
            for refs in old_market_rows:
                refs["frame"].destroy()
            for refs in old_selection_rows:
                refs["frame"].destroy()
            for rule in self.builder.multi_markets:
                self._add_multi_row_widget(self._multi_markets_box, self._multi_market_rows, rule)
            for rule in self.builder.multi_selections:
                self._add_multi_row_widget(self._multi_selections_box, self._multi_selection_rows,
                                           rule, fields=self._MULTI_SELECTION_FIELDS)
        finally:
            self._multi_reloading = False
        self._refresh_multi_warnings()

    def _sync_multi_to_builder(self):
        """Riporta interruttori + righe multi nei campi del builder (per save/anteprima)."""
        self.builder.multi_market_enabled = bool(self._multi_market_var.get())
        self.builder.multi_selection_enabled = bool(self._multi_selection_var.get())
        self.builder.multi_markets = [self._multi_rule_from_refs(r) for r in self._multi_market_rows]
        self.builder.multi_selections = [
            self._multi_rule_from_refs(r) for r in self._multi_selection_rows]

    def _multi_rule_from_refs(self, refs) -> "MultiRowRule":
        """Ricostruisce la `MultiRowRule` da una riga: parte dalla regola SORGENTE e applica
        SOLO gli override visibili della riga (`refs["_fields"]`: `_MULTI_FIELDS` per i
        mercati, `_MULTI_SELECTION_FIELDS` — coi delimitatori #325 — per le selezioni) +
        Attiva, preservando i campi non esposti (logica in
        `ParserBuilder.merge_multi_rule_overrides`, testata in CI; Codex P1)."""
        fields = refs.get("_fields") or self._MULTI_FIELDS
        # I delimitatori NON vengono strippati (stesso contratto della griglia base,
        # `_sync_to_builder`): «\n» o uno spazio sono delimitatori LEGITTIMI di estrazione —
        # uno strip li cancellerebbe in silenzio a ogni apri+salva.
        overrides = {attr: (refs[attr].get() if attr in ("start_after", "end_before")
                            else refs[attr].get().strip())
                     for attr, _, _ in fields}
        return ParserBuilder.merge_multi_rule_overrides(
            refs.get("_rule") or MultiRowRule(), overrides,
            enabled=bool(refs["enabled"].get()))

    def _refresh_multi_warnings(self):
        """Aggiorna il banner avvisi dal controller (sincronizza prima i widget).

        No-op durante `_reload_multi_from_builder` (Fable/Fugu #348): un `<FocusOut>`
        rientrante mentre le righe vengono distrutte/ricostruite farebbe scrivere nel
        builder le liste GUI transitoriamente vuote, perdendo le righe multi del parser
        appena caricato. Il reload chiama comunque questo metodo alla fine, a flag basso."""
        if self._multi_reloading:
            return
        self._sync_multi_to_builder()
        warnings = self.builder.multi_warnings()
        self._multi_warn.configure(text=("\n".join(f"⚠ {w}" for w in warnings)) if warnings else "")

    # ── condizioni di gate (PR-1): il parser scatta SOLO se il messaggio le soddisfa ──
    # Etichette GUI ⇄ valori-modello. Il modello usa `negate` (bool) e `conditions_mode`
    # ("all"/"any"); qui mostriamo testo italiano leggibile. Le tendine offrono SOLO valori
    # validi, quindi la mappatura è totale (nessun ramo «ignoto» possibile da GUI).
    _COND_CONTAINS = "contiene"
    _COND_NOT_CONTAINS = "NON contiene"
    _COND_MODE_ALL = "TUTTE (E)"
    _COND_MODE_ANY = "una qualsiasi (O)"

    def _cond_mode_to_label(self, mode: str) -> str:
        """Modello ("all"/"any") → etichetta GUI. Fallback fail-closed su E (più restrittivo)."""
        return self._COND_MODE_ANY if str(mode) == "any" else self._COND_MODE_ALL

    def _cond_label_to_mode(self, label: str) -> str:
        """Etichetta GUI → modello. Fallback fail-closed su "all" (più restrittivo)."""
        return "any" if label == self._COND_MODE_ANY else "all"

    def _build_conditions_section(self, outer):
        """Sezione «Condizioni di gate»: modo E/O + righe dinamiche [contiene/NON contiene +
        testo + Rimuovi]. Solo widget; lo stato vive nel `ParserBuilder` (round-trip e gate
        testati in CI). Nessuna condizione = nessun filtro (comportamento invariato)."""
        sec = ctk.CTkFrame(outer)
        sec.pack(fill="x", padx=10, pady=(8, 4))
        ctk.CTkLabel(sec, text=i18n.tr("Condizioni di gate (il parser scatta solo se il messaggio le soddisfa)"),
                     font=ctk.CTkFont(weight="bold")).pack(anchor="w", padx=8, pady=(6, 2))

        # Barra: modo E/O + pulsante «Aggiungi condizione».
        bar = ctk.CTkFrame(sec, fg_color="transparent")
        bar.pack(fill="x", padx=8, pady=2)
        ctk.CTkLabel(bar, text=i18n.tr("Soddisfa:")).pack(side="left", padx=(4, 2))
        self._cond_mode_var = ctk.StringVar(
            value=self._cond_mode_to_label(getattr(self.builder, "conditions_mode", "all")))
        ctk.CTkOptionMenu(bar, variable=self._cond_mode_var, width=150,
                          values=[self._COND_MODE_ALL, self._COND_MODE_ANY]).pack(side="left", padx=4)
        ctk.CTkButton(bar, text=i18n.tr("➕ Aggiungi condizione"), width=170,
                      command=self._add_condition_clicked).pack(side="left", padx=6)

        self._conditions_box = ctk.CTkFrame(sec, fg_color="transparent")
        self._conditions_box.pack(fill="x", padx=8, pady=(0, 4))
        self._condition_rows = []        # refs per riga condizione

        ctk.CTkLabel(
            sec, anchor="w", justify="left", font=ctk.CTkFont(size=10),
            text=i18n.tr("💡 «contiene»/«NON contiene» un testo; confronto senza maiuscole e "
                         "tollerante agli spazi. Nessuna condizione = nessun filtro. Righe a "
                         "testo vuoto sono ignorate.")).pack(fill="x", padx=8, pady=(0, 6))

    def _add_condition_row_widget(self, cond):
        """Disegna UNA riga condizione: [tendina contiene/NON contiene] [testo] [🗑 Rimuovi]."""
        row = ctk.CTkFrame(self._conditions_box, fg_color="transparent")
        row.pack(fill="x", pady=2)
        refs = {"frame": row}
        refs["kind"] = ctk.StringVar(
            value=self._COND_NOT_CONTAINS if bool(getattr(cond, "negate", False))
            else self._COND_CONTAINS)
        ctk.CTkOptionMenu(row, variable=refs["kind"], width=140,
                          values=[self._COND_CONTAINS, self._COND_NOT_CONTAINS]).pack(side="left", padx=2)
        entry = ctk.CTkEntry(row, width=360, placeholder_text=i18n.tr("testo da cercare nel messaggio"))
        entry.insert(0, getattr(cond, "text", "") or "")
        entry.pack(side="left", padx=4)
        refs["text"] = entry
        ctk.CTkButton(row, text=i18n.tr("🗑 Rimuovi"), width=90, fg_color="#7f0000",
                      command=lambda: self._remove_condition_row(refs)).pack(side="left", padx=4)
        self._condition_rows.append(refs)

    def _add_condition_clicked(self):
        """[+] condizione: aggiunge una riga vuota (contiene, testo vuoto)."""
        self._add_condition_row_widget(Condition())

    def _remove_condition_row(self, refs):
        """Toglie una riga condizione (widget + ref)."""
        try:
            self._condition_rows.remove(refs)
        except ValueError:
            pass
        refs["frame"].destroy()

    def _reload_conditions_from_builder(self):
        """Ridisegna modo + righe condizioni dal builder (caricamento parser/nuovo).

        Stesso ordine anti-rientranza di `_reload_multi_from_builder`: prima si SVUOTA la
        lista refs, poi si distruggono i frame, così un eventuale evento su un widget in
        distruzione non legge righe mezze morte."""
        self._cond_mode_var.set(
            self._cond_mode_to_label(getattr(self.builder, "conditions_mode", "all")))
        old_rows, self._condition_rows = self._condition_rows, []
        for refs in old_rows:
            refs["frame"].destroy()
        for cond in getattr(self.builder, "conditions", []) or []:
            self._add_condition_row_widget(cond)

    def _sync_conditions_to_builder(self):
        """Riporta modo + righe condizioni nel builder. Le righe a testo vuoto vengono
        SCARTATE qui (non solo ignorate a runtime): così `validate_parser_def` non segnala
        «testo vuoto» per una riga lasciata a metà, e il file salvato resta pulito."""
        self.builder.conditions_mode = self._cond_label_to_mode(self._cond_mode_var.get())
        conds = []
        for refs in self._condition_rows:
            text = refs["text"].get().strip()
            if not text:
                continue
            conds.append(Condition(text=text, negate=(refs["kind"].get() == self._COND_NOT_CONTAINS)))
        self.builder.conditions = conds

    # ── colonne avanzate / toggle «Avanzate» (#293 densità parser) ─────────
    def _populate_rules_header(self):
        """(Ri)costruisce l'intestazione delle colonne, mostrando le colonne avanzate
        (Trasformazione/Value-map) solo se `self._show_advanced`. Usa la fonte unica
        `_visible_rule_columns`; distrugge le label precedenti (no `winfo_children`, così
        resta costruibile anche con widget stubbati in test)."""
        for lbl in getattr(self, "_rules_head_labels", []):
            lbl.destroy()
        self._rules_head_labels = []
        for label, w in _visible_rule_columns(self._show_advanced):
            lbl = ctk.CTkLabel(self._rules_head, text=label, width=w, anchor="w")
            lbl.pack(side="left", padx=2)
            self._rules_head_labels.append(lbl)

    def _on_toggle_advanced(self):
        """Toggle «Avanzate»: mostra/nasconde le colonne Trasformazione/Value-map e ricostruisce
        intestazione + righe. Prima sincronizza i widget nel builder (come `_on_mode_change`),
        così un'eventuale modifica in corso non va persa; i dati `rule.transform`/`value_map`
        restano invariati anche a colonne nascoste (si nascondono, non si cancellano)."""
        self._show_advanced = bool(self._advanced_var.get())
        self._sync_to_builder()
        self._populate_rules_header()
        self._reload_rows_from_builder()

    # ── righe regola ──────────────────────────────────────────────────────
    def _add_row(self, rule):
        """Una riga = UNA colonna del contratto (griglia fissa a 14, PR-4): la colonna
        è una Label fissa (non più una tendina), così l'ordine resta quello del contratto
        e non si possono creare doppioni o dimenticare colonne."""
        row = ctk.CTkFrame(self._rows_frame, fg_color="transparent")
        row.pack(fill="x", pady=2)
        refs = {"target": rule.target}     # colonna FISSA: stringa, non un widget
        ctk.CTkLabel(row, text=rule.target, width=150, anchor="w").pack(side="left", padx=2)
        refs["start_after"] = ctk.CTkEntry(row, width=150)
        refs["end_before"] = ctk.CTkEntry(row, width=150)
        refs["start_after"].insert(0, rule.start_after)
        refs["end_before"].insert(0, rule.end_before)
        refs["start_after"].pack(side="left", padx=2)
        refs["end_before"].pack(side="left", padx=2)
        # Valore fisso: per la colonna Provider è un MENU dall'anagrafica (PR-5), così si
        # sceglie un provider salvato invece di digitarlo; per le altre colonne è testo.
        # In entrambi i casi `_sync_to_builder` legge `.get()` (StringVar o Entry).
        if rule.target == "Provider":
            refs["fixed_value"] = ctk.StringVar(value=rule.fixed_value)
            vals = ["", *self._providers]
            if rule.fixed_value and rule.fixed_value not in vals:
                vals.append(rule.fixed_value)   # preserva un provider non (più) in anagrafica
            provider_menu = ctk.CTkOptionMenu(row, variable=refs["fixed_value"], width=130,
                                              values=vals)
            provider_menu.pack(side="left", padx=2)
            refs["provider_menu"] = provider_menu   # per refresh_options (hub)
        elif rule.target in _BETFAIR_TERM_TARGETS:
            # #283 PR 13: tendina EDITABILE coi valori permanenti Betfair (per sport). È una
            # CTkComboBox (non OptionMenu): suggerisce i valori sincronizzati MA il testo libero
            # resta digitabile → nessuna regressione fail-closed (un valore valido non ancora
            # harvestato è comunque inseribile). `.get()` sul StringVar è letto da _sync_to_builder.
            refs["fixed_value"] = ctk.StringVar(value=rule.fixed_value)
            vals = ["", *self._term_values(rule.target)]
            if rule.fixed_value and rule.fixed_value not in vals:
                vals.append(rule.fixed_value)   # preserva un valore non (ancora) sincronizzato
            combo = ctk.CTkComboBox(row, variable=refs["fixed_value"], width=130, values=vals)
            combo.pack(side="left", padx=2)
            refs["term_combo"] = combo          # per _refresh_term_combos (cambio sport / hub)
        else:
            refs["fixed_value"] = ctk.CTkEntry(row, width=130)
            refs["fixed_value"].insert(0, rule.fixed_value)
            refs["fixed_value"].pack(side="left", padx=2)
        # Colonne avanzate (#293 densità parser): i StringVar esistono SEMPRE — così
        # `_sync_to_builder` continua a leggere e conservare `rule.transform`/`rule.value_map`
        # anche a colonne nascoste (nessuna perdita di dati) — ma i menu si mostrano solo in
        # modalità «Avanzate», per tenere la tabella leggibile di default.
        refs["transform"] = ctk.StringVar(value=rule.transform)
        refs["value_map"] = ctk.StringVar(value=rule.value_map)
        if getattr(self, "_show_advanced", False):
            ctk.CTkOptionMenu(row, variable=refs["transform"], values=self._transforms,
                              width=150).pack(side="left", padx=2)
            ctk.CTkOptionMenu(row, variable=refs["value_map"], values=self._value_maps,
                              width=150).pack(side="left", padx=2)
        refs["required"] = ctk.BooleanVar(value=bool(rule.required))
        ctk.CTkCheckBox(row, text="", variable=refs["required"], width=40).pack(side="left", padx=2)
        refs["frame"] = row
        self._rows.append(refs)

    def _reload_rows_from_builder(self):
        for r in list(self._rows):
            r["frame"].destroy()
        self._rows = []
        # Griglia fissa: garantisce una riga per ognuna delle 14 colonne, in ordine.
        self.builder.ensure_all_columns()
        # NB: NON si chiama qui `set_mode` — il reload deve solo RENDERIZZARE i flag
        # correnti, preservando i required salvati a mano di un parser caricato (Codex).
        # L'auto-Obblig. si applica su azione esplicita (_on_mode_change) o su parser nuovo.
        self._mode_var.set(self._mode_to_label(self.builder.mode))
        # Sport (PR-P9): ripristina la tendina dal builder (incl. "" agnostico).
        self._sport_var.set(self._sport_to_label(self.builder.sport))
        # #283 PR 13: rileggi i termini Betfair per lo sport (appena ripristinato) PRIMA di
        # costruire le righe, così le tendine MarketType/MarketName/SelectionName nascono coi
        # valori giusti. Best-effort (sync in corso / DB assente → nessun suggerimento).
        self._market_terms = self._fetch_market_terms()
        # Mappatura nomi: ripristina separatore + checkbox profili dal builder.
        self._separator_var.set(self.builder.team_separator)
        self._reload_profile_checks(use_builder=True)
        # Mappatura mercati: checkbox profili mercati dal builder.
        self._reload_market_profile_checks(use_builder=True)
        # Output multi-riga (#192): interruttori + righe dal builder.
        self._reload_multi_from_builder()
        # Condizioni di gate (PR-1): modo E/O + righe dal builder.
        self._reload_conditions_from_builder()
        for rule in self.builder.rules:
            self._add_row(rule)

    def _on_mode_change(self, _value=None):
        """Modalità cambiata dal menu. Se concreta → set_mode (auto-Obblig. add-only);
        se "(eredita globale)" → mode "" (nessuna auto-Obblig.: la decide il globale a
        runtime). Poi ricarica le righe."""
        self._sync_to_builder()
        mode = self._label_to_mode(self._mode_var.get())
        if mode:
            self.builder.set_mode(mode)
        else:
            self.builder.mode = ""
        self._reload_rows_from_builder()

    def _sync_to_builder(self):
        """Riporta i valori dei widget nel controller (colonne fisse + Modalità).
        La modalità è preservata com'è (incl. "" = eredita globale): non si normalizza,
        così aprire/salvare un parser legacy non lo converte a NAME_ONLY (Codex)."""
        self.builder.name = self._name_var.get().strip()
        self.builder.mode = self._label_to_mode(self._mode_var.get())
        # Sport (PR-P9): canonicalizza un valore NOTO (case-insensitive) ma **preserva**
        # un valore ignoto (es. parser caricato con sport corrotto a mano) invece di
        # azzerarlo a "" qui: così `validate_parser_def` può emettere "Sport non valido"
        # (fail-closed) e il salvataggio è bloccato, invece di convertirlo in silenzio in
        # agnostico perdendo lo scope sport (Codex). La tendina offre solo valori validi,
        # quindi un valore ignoto può arrivare SOLO da un file manomesso.
        _sport_raw = self._label_to_sport(self._sport_var.get())
        self.builder.sport = sports.normalize_sport(_sport_raw) or _sport_raw
        # Mappatura nomi: separatore (testo libero) + profili spuntati.
        self.builder.team_separator = self._separator_var.get().strip()
        self.builder.name_mapping_profiles = self._selected_profiles()
        # Mappatura mercati: profili mercati spuntati.
        self.builder.market_mapping_profiles = self._selected_market_profiles()
        # Output multi-riga (#192): interruttori + righe MultiMarket/MultiSelection.
        self._sync_multi_to_builder()
        # Condizioni di gate (PR-1): modo E/O + righe (le vuote scartate).
        self._sync_conditions_to_builder()
        self.builder.rules = []
        for refs in self._rows:
            self.builder.add_rule(
                target=refs["target"],          # stringa fissa (Label)
                start_after=refs["start_after"].get(),
                end_before=refs["end_before"].get(),
                fixed_value=refs["fixed_value"].get(),
                transform=refs["transform"].get(),
                value_map=refs["value_map"].get(),
                required=bool(refs["required"].get()),
            )

    # ── azioni ────────────────────────────────────────────────────────────
    def _save(self):
        # Rinfresca le checkbox dal config: una modifica del dizionario fatta altrove
        # (es. rinomina dal pulsante della finestra principale) deve riflettersi qui,
        # così un profilo mancante diventa ⚠ e blocca il salvataggio invece di riscrivere
        # un riferimento morto nel parser (Codex).
        self._reload_profile_checks()
        self._reload_market_profile_checks()   # idem per i profili mercati (Codex P2)
        self._sync_to_builder()
        unresolved = self._unresolved_selected()
        if unresolved:
            self._result.configure(
                text=i18n.tr("⛔ Non salvato: profili di mappatura nomi mancanti ({names}). "
                             "Ricreali nel «Dizionario nomi» o togli la spunta prima di salvare.")
                     .format(names=', '.join(unresolved)))
            return
        unresolved_mkt = self._unresolved_market_selected()
        if unresolved_mkt:
            # Stesso fail-closed dei nomi: un profilo mercati rinominato/eliminato non deve
            # essere riscritto stantio nel parser (→ MARKET_MAPPING_MISSING a runtime, Codex P2).
            self._result.configure(
                text=i18n.tr("⛔ Non salvato: profili di mappatura mercati mancanti ({names}). "
                             "Ricreali nel «Dizionario mercati» o togli la spunta prima di salvare.")
                     .format(names=', '.join(unresolved_mkt)))
            return
        errors = self.builder.errors()
        if errors:
            self._result.configure(text=i18n.tr("❌ Non salvato:\n- ") + "\n- ".join(errors))
            return
        try:
            path = self.builder.save()
        except (OSError, ValueError) as exc:
            self._result.configure(text=i18n.tr("❌ Errore salvataggio: {exc}").format(exc=exc))
            return
        self._refresh_saved()
        self._result.configure(text=i18n.tr("💾 Salvato in {path}").format(path=path))

    # ── catalogo XTrader (B2) ───────────────────────────────────────────────
    def _on_market_change(self, _value=None):
        """Cambiato il mercato → rinfresca la tendina delle selezioni (solo fisse)."""
        self._refresh_selection_menu()

    def _refresh_selection_menu(self):
        market = self._cat_market.get()
        sels = self.builder.selection_options(market) if market else []
        self._cat_selection_menu.configure(values=sels or [""])
        self._cat_selection.set(sels[0] if sels else "")

    def _insert_fixed_market(self):
        """Inserisce Mercato+Selezione scelti come regole FISSE (MarketType/MarketName/
        SelectionName), preservando le altre regole già impostate nei widget."""
        market = self._cat_market.get()
        selection = self._cat_selection.get()
        self._sync_to_builder()          # cattura le regole correnti dai widget
        try:
            self.builder.set_fixed_market(market, selection)
        except ValueError as exc:
            self._result.configure(text=f"⛔ {exc}")
            return
        self._reload_rows_from_builder()
        self._result.configure(text=i18n.tr("➕ Regole fisse inserite: {market} · {selection}").format(market=market, selection=selection))

    # ── gestione parser salvati (lista / nuovo / carica / duplica / elimina) ─
    def _refresh_saved(self):
        """Ricarica la tendina dei parser salvati dalla cartella utente."""
        items = ParserBuilder.saved_parsers()
        self._saved_map = {it["name"]: it["path"] for it in items}
        labels = list(self._saved_map) or [self._NONE_SAVED]
        self._saved_menu.configure(values=labels)
        # Mantieni la selezione se ancora valida, altrimenti vai sul primo.
        if self._saved_var.get() not in labels:
            self._saved_var.set(labels[0])

    def _selected_path(self):
        """Path del parser selezionato, o None se non c'è selezione valida."""
        return self._saved_map.get(self._saved_var.get())

    def _new(self):
        """Svuota il costruttore per un nuovo parser (non tocca i file salvati)."""
        self.builder = ParserBuilder()
        # Parser nuovo: crea le 14 colonne e POI applica l'auto-Obblig. della modalità di
        # default una volta (set_mode da solo, senza regole, non marcherebbe nulla — Codex #72).
        if self.builder.mode:
            self.builder.apply_mode_defaults(self.builder.mode)
        self._name_var.set("")
        self._reload_rows_from_builder()
        self._result.configure(text=i18n.tr("🆕 Nuovo parser (non ancora salvato)."))

    def _load_selected(self):
        path = self._selected_path()
        if not path:
            self._result.configure(text=i18n.tr("⛔ Nessun parser selezionato."))
            return
        try:
            self.builder = ParserBuilder.load(path)
        except (OSError, ValueError) as exc:
            self._result.configure(text=i18n.tr("❌ Errore caricamento: {exc}").format(exc=exc))
            return
        self._name_var.set(self.builder.name)
        self._reload_rows_from_builder()
        self._result.configure(text=i18n.tr("📂 Caricato {name!r}.").format(name=self.builder.name))

    def _duplicate_selected(self):
        path = self._selected_path()
        if not path:
            self._result.configure(text=i18n.tr("⛔ Nessun parser selezionato."))
            return
        src_name = self._saved_var.get()
        dialog = ctk.CTkInputDialog(
            text=i18n.tr("Nuovo nome per la copia di {src!r}:").format(src=src_name),
            title=i18n.tr("Duplica parser"))
        new_name = (dialog.get_input() or "").strip()
        if not new_name:
            self._result.configure(text=i18n.tr("⛔ Duplica annullata (nome vuoto)."))
            return
        try:
            ParserBuilder.duplicate_saved(path, new_name)
        except (OSError, ValueError) as exc:
            self._result.configure(text=i18n.tr("❌ Errore duplica: {exc}").format(exc=exc))
            return
        self._refresh_saved()
        self._saved_var.set(new_name if new_name in self._saved_map else self._saved_var.get())
        self._result.configure(text=i18n.tr("📑 Duplicato in {new_name!r}.").format(new_name=new_name))

    def _delete_selected(self):
        name = self._saved_var.get()
        if name == self._NONE_SAVED or name not in self._saved_map:
            self._result.configure(text=i18n.tr("⛔ Nessun parser selezionato."))
            return
        try:
            removed = ParserBuilder.delete_saved(name)
        except OSError as exc:
            # Permessi / filesystem: mostra un errore pulito invece di crashare il
            # callback (stesso pattern di _save/_load/_duplicate_selected).
            self._result.configure(text=i18n.tr("❌ Errore eliminazione: {exc}").format(exc=exc))
            return
        self._refresh_saved()
        self._result.configure(
            text=i18n.tr("🗑 Eliminato {name!r}.").format(name=name) if removed
            else i18n.tr("⛔ {name!r} non trovato.").format(name=name))

    def _preview_id_resolver(self):
        """Resolver ID Betfair per l'anteprima (#192, Codex), best-effort/fail-open.

        Se l'app ha fornito la factory `id_resolver_factory` (`() -> id_resolver | None`,
        tipicamente `App._betfair_id_resolver`), invocala e ritorna il resolver così
        «Prova messaggio» risolve gli ID come il runtime per i parser ID_ONLY che li
        prendono dal dizionario. Qualsiasi assenza/eccezione → `None`: l'anteprima resta
        conservativa/fail-closed (comportamento storico), MAI un crash della GUI e MAI
        un effetto sul runtime reale."""
        factory = self._id_resolver_factory
        if factory is None:
            return None
        try:
            return factory()
        except Exception:
            return None

    def _test(self):
        self._reload_profile_checks()   # rifletti modifiche al dizionario fatte altrove (Codex)
        self._reload_market_profile_checks()
        self._sync_to_builder()
        # Riallinea il banner avvisi multi allo stato appena sincronizzato: gli avvisi per-riga
        # (#325 follow-up) dipendono anche dai campi della griglia base (MarketType fisso) e dai
        # profili mercati, che non passano dai refresh su FocusOut delle sole righe multi.
        self._refresh_multi_warnings()
        unresolved = self._unresolved_selected()
        if unresolved:
            # Mappatura richiesta ma con profili non risolvibili: non mostrare un'anteprima
            # fuorviante (col rischio di EventName grezzo). Blocca con spiegazione (Codex).
            self._result.configure(
                text=f"⛔ Non pronto: profili di mappatura nomi mancanti ({', '.join(unresolved)}). "
                     "Ricreali nel «Dizionario nomi» o togli la spunta.")
            return
        unresolved_mkt = self._unresolved_market_selected()
        if unresolved_mkt:
            self._result.configure(
                text=f"⛔ Non pronto: profili di mappatura mercati mancanti ({', '.join(unresolved_mkt)}). "
                     "Ricreali nel «Dizionario mercati» o togli la spunta.")
            return
        message = self._msg_box.get("1.0", "end").rstrip("\n")
        # Modalità EFFETTIVA per l'anteprima: quella scelta; se "(eredita globale)" ("")
        # usa la modalità globale, così "Prova messaggio" combacia col runtime (Codex).
        mode = self._label_to_mode(self._mode_var.get()) or self._global_mode
        # Verdetto sintetico + diagnostica per-campo (CP-08b). Il verdetto usa la
        # diagnostica (`diag.placeable`/`diag.status`), che include ANCHE il gate di
        # contenuto: così "Prova messaggio" non dice "Pronto" per un parser che il
        # runtime scarterebbe come NO_CONTENT_MATCH (Codex).
        # Quota richiesta sì/no = riga Price del parser (unico comando). Lo stesso
        # valore guida sia il verdetto sintetico sia la diagnostica per-campo, così
        # "Prova messaggio" combacia col runtime.
        defn = self.builder.to_def()
        require_price = defn.price_required()
        # Mappatura nomi: risolvi i profili selezionati dalla config, così l'anteprima
        # traduce l'EventName come il runtime (e fa fail-closed se non mappabile) invece
        # di mostrare un falso "Pronto" col nome grezzo.
        name_mapping_profiles = self._resolve_mapping_profiles(defn)
        # Mappatura mercati: risolvi i profili mercati dalla config così l'anteprima imposta
        # Mercato/Selezione come il runtime (o fa fail-closed con MARKET_MAPPING_MISSING).
        market_mapping_profiles = self._resolve_market_mapping_profiles(defn)
        # Dizionario Betfair per l'anteprima (#192, Codex): best-effort, così «Prova messaggio»
        # risolve gli ID come il runtime per i parser ID_ONLY dizionario-dipendenti (o resta
        # conservativa se il resolver non è disponibile).
        id_resolver = self._preview_id_resolver()
        res = self.builder.test_message(message, provider=self._provider, mode=mode,
                                        require_price=require_price,
                                        name_mapping_profiles=name_mapping_profiles,
                                        market_mapping_profiles=market_mapping_profiles,
                                        id_resolver=id_resolver)
        diag = parser_diagnostics.diagnose(
            defn, message, provider=self._provider, mode=mode, require_price=require_price,
            name_mapping_profiles=name_mapping_profiles,
            market_mapping_profiles=market_mapping_profiles, id_resolver=id_resolver)
        # Anteprima multi-riga (#192): tutte le righe generate (base o MultiMarket/
        # MultiSelection), col verdetto per-riga. Stesso motore del runtime.
        preview = self.builder.preview_rows(
            message, provider=self._provider, mode=mode, require_price=require_price,
            name_mapping_profiles=name_mapping_profiles,
            market_mapping_profiles=market_mapping_profiles, id_resolver=id_resolver)
        # Verdetto sintetico (logica pura testata in CI: `ParserBuilder.test_verdict`).
        # Precedenza: errori STRUTTURALI del parser (che Save rifiuterebbe) → «Non salvabile»,
        # così l'anteprima non dice «Pronto» per una definizione non salvabile (Codex #19);
        # poi output multi-riga (verdetto sulle RIGHE GENERATE, non sulla sola base, Codex P2);
        # infine single-row col motivo e i campi mancanti — sia il gate parser sia i campi di
        # RICONOSCIMENTO del validator (INVALID_MISSING_FIELDS), così si sa QUALE colonna aggiungere.
        # `content_ok`: esito del gate di contenuto whole-message (in `diag.message_error`),
        # così il verdetto multi-riga onora NO_CONTENT_MATCH come il runtime (Codex, #192).
        self._result.configure(text=ParserBuilder.test_verdict(
            self.builder.errors(), preview,
            diag_placeable=diag.placeable, diag_status=diag.status,
            res_row=res.row, res_missing_required=res.missing_required,
            res_detail=res.detail, content_ok=not diag.message_error))
        self._last_report = parser_diagnostics.format_report(diag)
        self._render_diag_table(parser_diagnostics.diagnostic_table(diag, defn))
        self._render_preview_table(preview)

    _MULTI_KIND_LABEL = {"base": "Base", "market": "Mercato", "selection": "Selezione"}

    def _render_preview_table(self, preview_rows):
        """Disegna la tabella anteprima multi-riga (#192) da `PreviewRow` già pronte
        (`ParserBuilder.preview_rows`, testata in CI): qui solo widget. Verde = riga
        piazzabile, rosso = scartata (col motivo `status`)."""
        for child in self._preview_table.winfo_children():
            child.destroy()

        def add_cells(values, *, header=False, color=None):
            row = ctk.CTkFrame(self._preview_table, fg_color="transparent")
            row.pack(fill="x", pady=1)
            font = ctk.CTkFont(size=11, weight="bold" if header else "normal")
            for (txt, (_, w)) in zip(values, self._preview_cols):
                ctk.CTkLabel(row, text=txt, width=w, anchor="w", justify="left",
                             wraplength=w - 6, font=font, text_color=color).pack(side="left", padx=2)

        add_cells([c for c, _ in self._preview_cols], header=True)
        if not preview_rows:
            add_cells(["", "", "", "(nessuna riga)"], color="gray")
            return
        for pr in preview_rows:
            kind = self._MULTI_KIND_LABEL.get(pr.kind, pr.kind)
            esito = "✅" if pr.placeable else f"⛔ {pr.status}"
            add_cells([str(pr.index + 1), kind, esito, pr.summary],
                      color=None if pr.placeable else "#ef5350")

    def _test_batch(self):
        """Tester multiplo (#311 §3.2): valuta OGNI messaggio incollato nel box (separati
        da righe «---») con la STESSA pipeline read-only di «Prova messaggio» — verdetto
        col motivo esatto + anteprima righe CSV per messaggio. Solo lettura: nessuna
        scrittura del CSV operativo. Input risolti come in `_test` (profili, resolver)."""
        self._reload_profile_checks()
        self._reload_market_profile_checks()
        self._sync_to_builder()
        self._refresh_multi_warnings()
        unresolved = self._unresolved_selected()
        if unresolved:
            self._result.configure(
                text=f"⛔ Non pronto: profili di mappatura nomi mancanti ({', '.join(unresolved)}). "
                     "Ricreali nel «Dizionario nomi» o togli la spunta.")
            return
        unresolved_mkt = self._unresolved_market_selected()
        if unresolved_mkt:
            self._result.configure(
                text=f"⛔ Non pronto: profili di mappatura mercati mancanti ({', '.join(unresolved_mkt)}). "
                     "Ricreali nel «Dizionario mercati» o togli la spunta.")
            return
        text = self._msg_box.get("1.0", "end").rstrip("\n")
        mode = self._label_to_mode(self._mode_var.get()) or self._global_mode
        defn = self.builder.to_def()
        reports, skipped = self.builder.batch_report(
            text, provider=self._provider, mode=mode,
            require_price=defn.price_required(),
            name_mapping_profiles=self._resolve_mapping_profiles(defn),
            market_mapping_profiles=self._resolve_market_mapping_profiles(defn),
            id_resolver=self._preview_id_resolver())
        if not reports:
            self._result.configure(
                text=i18n.tr("⛔ Nessun messaggio: incolla uno o più messaggi separati da una "
                             "riga «---»."))
            return
        ok = sum(1 for r in reports if r.ok)
        extra = (f" · ⚠ mostrati i primi {len(reports)} (altri {skipped} oltre il tetto)"
                 if skipped else "")
        self._result.configure(
            text=f"{'✅' if ok == len(reports) else '⚠'} Messaggi validi: "
                 f"{ok}/{len(reports)}{extra}")
        self._render_batch_table(reports)

    def _render_batch_table(self, reports):
        """Tabella del tester multiplo nell'area anteprima: per ogni messaggio una riga
        di intestazione (n° · esito · verdetto col motivo) e sotto le sue righe CSV."""
        for child in self._preview_table.winfo_children():
            child.destroy()

        def add_cells(values, *, header=False, color=None):
            row = ctk.CTkFrame(self._preview_table, fg_color="transparent")
            row.pack(fill="x", pady=1)
            font = ctk.CTkFont(size=11, weight="bold" if header else "normal")
            for (txt, (_, w)) in zip(values, self._preview_cols):
                ctk.CTkLabel(row, text=txt, width=w, anchor="w", justify="left",
                             wraplength=w - 6, font=font, text_color=color).pack(
                                 side="left", padx=2)

        add_cells([c for c, _ in self._preview_cols], header=True)
        for rep in reports:
            add_cells([f"M{rep.index + 1}", "Messaggio",
                       "✅" if rep.ok else "⛔", f"{rep.first_line} → {rep.verdict}"],
                      header=True, color=None if rep.ok else "#ef5350")
            for pr in rep.rows:
                kind = self._MULTI_KIND_LABEL.get(pr.kind, pr.kind)
                esito = "✅" if pr.placeable else f"⛔ {pr.status}"
                add_cells([str(pr.index + 1), kind, esito, pr.summary],
                          color=None if pr.placeable else "#ef5350")

    def _render_diag_table(self, rows):
        """Disegna la tabella diagnostica da righe già pronte (logica in
        `parser_diagnostics.diagnostic_table`): qui solo widget."""
        for child in self._diag_table.winfo_children():
            child.destroy()

        def add_cells(values, *, header=False, color=None):
            row = ctk.CTkFrame(self._diag_table, fg_color="transparent")
            row.pack(fill="x", pady=1)
            font = ctk.CTkFont(size=11, weight="bold" if header else "normal")
            for (txt, (_, w)) in zip(values, self._diag_cols):
                ctk.CTkLabel(row, text=txt, width=w, anchor="w", justify="left",
                             wraplength=w - 6, font=font, text_color=color).pack(side="left", padx=2)

        add_cells([c for c, _ in self._diag_cols], header=True)
        for r in rows:
            target = r.target if (r.required or r.banner) else f"{r.target}  (opz)"
            add_cells([target, r.status, r.reason, r.start_after, r.end_before, r.extracted],
                      color=None if r.ok else "#ef5350")

    def _copy_diag(self):
        """Copia l'ultimo report di diagnostica negli appunti (per incollarlo)."""
        if not self._last_report:
            self._result.configure(text=i18n.tr("⛔ Premi prima «Prova messaggio»."))
            return
        try:
            self.clipboard_clear()
            self.clipboard_append(self._last_report)
        except Exception:                       # noqa: BLE001 — clipboard non disponibile
            self._result.configure(text=i18n.tr("❌ Copia non riuscita (appunti non disponibili)."))
            return
        self._result.configure(text=i18n.tr("📋 Diagnostica copiata negli appunti."))


class CustomParserWindow(ctk.CTkToplevel):
    """Finestra standalone che ospita `CustomParserPanel` a tutta finestra.

    Mantenuta per compatibilità; la stessa `CustomParserPanel` vive anche come scheda
    "🧩 Parser" della finestra "🧰 Strumenti"."""

    def __init__(self, master=None, builder: ParserBuilder = None, provider: str = "",
                 global_mode: str = "", on_saved=None, id_resolver_factory=None,
                 market_terms_provider=None):
        super().__init__(master)
        self.title(i18n.tr("Parser Personalizzato"))
        gui_utils.fit_to_screen(self, 1024, 720, 760, 480)
        CustomParserPanel(self, builder=builder, provider=provider,
                          global_mode=global_mode, on_saved=on_saved,
                          id_resolver_factory=id_resolver_factory,
                          market_terms_provider=market_terms_provider).pack(
                              fill="both", expand=True)
