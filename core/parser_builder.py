"""CP-06: controller del costruttore di Parser Personalizzati (senza GUI).

Tutta la logica del costruttore vive qui, separata dai widget customtkinter
(vista sottile in `custom_parser_gui.py`), così è interamente testabile in CI:
gestione regole (aggiungi/aggiorna/rimuovi/sposta), opzioni dei menu a tendina,
validazione, salvataggio/caricamento e **test-live** di un messaggio.

Riusa i moduli già testati: `custom_parser` (modello/validazione/persistenza),
`value_maps`/`transforms` (opzioni a tendina), `custom_pipeline` (test-live),
`recognition` (modalità).
"""

import json
import os
from dataclasses import dataclass, field

from . import (
    csv_writer,
    custom_parser,
    dizionario,
    parser_diagnostics,
    recognition,
    sports,
    transforms,
    validator,
    value_maps,
)
from .custom_parser import Condition, CustomParserDef, FieldRule, MultiRowRule
from .custom_pipeline import both_multi_active, build_validated_row, build_validated_rows
# Gate dei mercati-punteggio dell'estrazione dinamica (#325/#341): importato dal runtime
# (nome interno, stesso package) perché è la FONTE UNICA di verità — duplicare il set qui
# farebbe divergere gli avvisi GUI dal comportamento reale della pipeline.
from .custom_pipeline import _DYNAMIC_SCORE_MARKETS as DYNAMIC_SCORE_MARKETS


@dataclass
class PreviewRow:
    """Una riga dell'anteprima multi-riga (#192, PR2) per la GUI «Prova messaggio».

    Dati GIÀ pronti per il rendering (la vista è sottile): `kind` distingue la riga base
    dalle righe MultiMarket/MultiSelection generate, `placeable`/`status` riflettono il
    verdetto del runtime per QUELLA riga (una riga non piazzabile non blocca le altre),
    `summary` è il riepilogo «Colonna=valore» dei campi non vuoti, `row` è la riga CSV
    completa (14 colonne)."""

    index: int
    kind: str                                       # "base" | "market" | "selection"
    placeable: bool
    status: str
    missing_required: "list[str]" = field(default_factory=list)
    row: "dict[str, str]" = field(default_factory=dict)
    summary: str = ""


def _static_base_market_type(defn: CustomParserDef) -> "str | None":
    """`MarketType` della riga BASE se determinabile STATICAMENTE (senza messaggio), altrimenti
    `None` (= ignoto). Serve agli avvisi multi (#325 follow-up): un avviso «mercato non-punteggio»
    emesso su un mercato noto solo a runtime sarebbe un falso allarme, quindi su ignoto si tace.

    Ignoto quando: il parser usa la mappatura mercati a frase (`market_mapping_profiles` — a
    runtime può sovrascrivere il MarketType, D1 «il dizionario vince»), oppure la regola
    MarketType estrae dal messaggio o applica transform/value-map (valore noto solo a runtime).
    Statico = il `fixed_value` (strippato) della regola, o `""` se la regola manca/è vuota."""
    if getattr(defn, "market_mapping_profiles", None):
        return None
    rule = next((r for r in defn.rules if r.target == "MarketType"), None)
    if rule is None:
        return ""
    if rule.has_extraction() or rule.transform or rule.value_map:
        return None
    return (rule.fixed_value or "").strip()


# Tester multiplo (#311 §3.2): separatore esplicito fra i messaggi incollati (una riga
# che contiene solo questo) e tetto fail-safe di messaggi valutati per invocazione.
MESSAGE_SEPARATOR = "---"
MAX_BATCH_MESSAGES = 50


@dataclass
class BatchMessageReport:
    """Esito di UN messaggio del tester multiplo (#311 §3.2), pronto per la GUI:
    `verdict` è lo STESSO verdetto sintetico di «Prova messaggio» (motivo esatto
    incluso), `rows` le `PreviewRow` generate (anteprima CSV per-riga)."""

    index: int
    first_line: str
    ok: bool
    verdict: str
    rows: "list" = field(default_factory=list)


class ParserBuilder:
    """Stato e operazioni del costruttore. Nessun widget: solo dati e logica."""

    def __init__(self, defn: CustomParserDef = None):
        if defn is None:
            self.name = ""
            self.description = ""
            self.mode = recognition.DEFAULT_MODE
            # Sport del parser (PR-P9): "" = non specificato (agnostico). Preservato nel
            # round-trip come gli altri campi, così load+save/duplica non lo azzera.
            self.sport = ""
            self.rules = []
            # Mappatura nomi squadra (name_mapping_store): vanno preservati nel
            # round-trip del builder, altrimenti load+save/duplica azzererebbe la
            # mappatura in silenzio (live scriverebbe l'EventName provider grezzo).
            self.name_mapping_profiles = []
            self.team_separator = ""
            # Mappatura mercati a frase (market_mapping_store): preservata nel round-trip
            # del builder come i profili nomi, così load+save/duplica non l'azzera in silenzio.
            self.market_mapping_profiles = []
            # Output multi-riga (#192): flag + righe MultiMarket/MultiSelection. Default
            # spento/vuoto = single-row come prima.
            self.multi_market_enabled = False
            self.multi_selection_enabled = False
            self.multi_markets = []
            self.multi_selections = []
            # Condizioni di gate (PR-1): righe contiene/NON contiene + modo E/O. Vuote =
            # nessun gate aggiuntivo. Preservate nel round-trip come gli altri campi.
            self.conditions = []
            self.conditions_mode = "all"
        else:
            self.name = defn.name
            self.description = defn.description
            # Preserva la modalità COM'È, incl. "" (legacy = eredita il globale): NON
            # normalizzare "" → NAME_ONLY, altrimenti aprire/salvare/duplicare un parser
            # legacy ne scriverebbe NAME_ONLY perdendo l'ereditarietà (Codex).
            self.mode = getattr(defn, "mode", recognition.DEFAULT_MODE)
            # Sport COM'È (incl. "" agnostico): `getattr` tollera def costruite prima del campo.
            self.sport = getattr(defn, "sport", "") or ""
            self.rules = [FieldRule.from_dict(r.to_dict()) for r in defn.rules]  # copia
            # Campi mappatura nomi: copiati (lista nuova) così il builder non perde i
            # profili/separatore di un parser caricato (Codex). `getattr` per tollerare
            # def costruite prima dei campi.
            self.name_mapping_profiles = list(getattr(defn, "name_mapping_profiles", []) or [])
            self.team_separator = getattr(defn, "team_separator", "") or ""
            self.market_mapping_profiles = list(getattr(defn, "market_mapping_profiles", []) or [])
            # Output multi-riga (#192): copia profonda delle righe (lista nuova + copia di
            # ogni MultiRowRule) così il builder non condivide oggetti col def caricato e un
            # load+save/duplica non perde la config multi. `getattr` tollera def pre-#192.
            self.multi_market_enabled = bool(getattr(defn, "multi_market_enabled", False))
            self.multi_selection_enabled = bool(getattr(defn, "multi_selection_enabled", False))
            self.multi_markets = [MultiRowRule.from_dict(r.to_dict())
                                  for r in getattr(defn, "multi_markets", []) or []]
            self.multi_selections = [MultiRowRule.from_dict(r.to_dict())
                                     for r in getattr(defn, "multi_selections", []) or []]
            # Condizioni di gate (PR-1): copia (lista nuova + copia di ogni Condition) così il
            # builder non condivide oggetti col def caricato e load+save/duplica non perde il
            # gate. `getattr` tollera def costruite prima della feature.
            self.conditions = [Condition.from_dict(c.to_dict())
                               for c in getattr(defn, "conditions", []) or []]
            self.conditions_mode = getattr(defn, "conditions_mode", "all") or "all"

    # ── opzioni per i menu a tendina della GUI ─────────────────────────────
    def target_options(self) -> list:
        return list(custom_parser.VALID_TARGETS)

    def transform_options(self) -> list:
        # "" = nessuna trasformazione.
        return [""] + transforms.available_transforms()

    def value_map_options(self, include_dizionario: bool = True, rows=None) -> list:
        # "" = nessuna value-map.
        return [""] + value_maps.available_value_maps(include_dizionario=include_dizionario, rows=rows)

    def mode_options(self) -> list:
        return list(recognition.VALID_MODES)

    def sport_options(self) -> list:
        """Opzioni della tendina Sport: "" (= non specificato/agnostico) + gli sport
        supportati (PR-P9). L'ordine segue `sports.SPORTS` (Calcio/Tennis/Basket/Rugby)."""
        return [""] + list(sports.SPORTS)

    def set_sport(self, sport: str) -> None:
        """Imposta lo sport del parser: canonicalizza un valore noto (case-insensitive);
        vuoto/None/ignoto → "" (non specificato, agnostico)."""
        self.sport = sports.normalize_sport(sport) or ""

    # ── catalogo XTrader: Mercato → Selezione FISSI (B2) ───────────────────
    def market_options(self, rows=None) -> list:
        """MarketName selezionabili come valore **fisso** per la tendina Mercato del
        catalogo: esclude i mercati **dinamici** (MarketName con placeholder squadra,
        es. handicap `"{HOME_TEAM} +1"`), che non sono valori fissi sicuri."""
        return dizionario.market_names(rows=rows, fixed_only=True)

    def selection_options(self, market: str, rows=None) -> list:
        """SelectionName **non dinamici** del mercato dato, per la tendina Selezione.
        Esclude le selezioni con placeholder squadra (vanno risolte a runtime da
        Home/Away, quindi non usabili come valore fisso)."""
        return [s["SelectionName"]
                for s in dizionario.selections_for_market(market, rows)
                if not s["dynamic"] and s["SelectionName"]]

    def set_fixed_market(self, market: str, selection: str, rows=None) -> None:
        """Imposta Mercato+Selezione **fissi** dal catalogo XTrader (B2): crea/aggiorna
        le regole `MarketType`, `MarketName`, `SelectionName` coi valori canonici scelti
        (`fixed_value`), azzerando estrazione/transform/value-map così il valore resta
        ESATTAMENTE quello del catalogo. Non tocca le altre regole.

        CSV-safe: l'input è confrontato in modo case/spazio-insensitive col catalogo ma
        nel CSV si persistono **sempre i nomi CANONICI** del dizionario (non l'input
        grezzo), così un `"esito finale"` non diventa una riga non-canonica che romperebbe
        il match XTrader. `ValueError` se il mercato non è nel catalogo (fixed-only) o la
        selezione non è tra quelle **non dinamiche** del mercato."""
        market_key = str(market or "").strip().casefold()
        selection_key = str(selection or "").strip().casefold()
        # Risolve il nome CANONICO del mercato (solo fixed-only: niente dinamici).
        canonical_market = next(
            (m for m in self.market_options(rows=rows)
             if m.strip().casefold() == market_key), None)
        if not canonical_market:
            raise ValueError(f"Mercato non nel catalogo XTrader: {market!r}")
        canonical_selection = next(
            (s for s in self.selection_options(canonical_market, rows)
             if s.strip().casefold() == selection_key), None)
        if not canonical_selection:
            raise ValueError(
                f"Selezione non valida o dinamica per {market!r}: {selection!r}")
        market_type = dizionario.market_type_for_name(canonical_market, rows)
        for target, value in (("MarketType", market_type),
                              ("MarketName", canonical_market),
                              ("SelectionName", canonical_selection)):
            self._upsert_fixed_rule(target, value)

    def _upsert_fixed_rule(self, target: str, value: str) -> None:
        """Imposta una regola a valore FISSO per `target`: aggiorna quella esistente (o
        ne aggiunge una nuova), azzerando i campi di estrazione/traduzione così resta un
        valore costante. Evita target duplicati (vietati dalla validazione)."""
        for rule in self.rules:
            if rule.target == target:
                rule.fixed_value = value
                rule.start_after = rule.end_before = rule.transform = rule.value_map = ""
                return
        self.rules.append(FieldRule(target=target, fixed_value=value))

    # ── gestione regole ────────────────────────────────────────────────────
    def add_rule(self, target: str = "EventName", **kwargs) -> FieldRule:
        rule = FieldRule(target=target, **kwargs)
        self.rules.append(rule)
        return rule

    def update_rule(self, index: int, **kwargs) -> None:
        rule = self.rules[index]
        for key, value in kwargs.items():
            if not hasattr(rule, key):
                raise AttributeError(f"FieldRule non ha il campo {key!r}")
            setattr(rule, key, value)

    def remove_rule(self, index: int) -> None:
        del self.rules[index]

    def move_rule(self, index: int, delta: int) -> int:
        """Sposta la regola di `delta` posizioni (clamp ai bordi). Ritorna il
        nuovo indice."""
        new_index = max(0, min(len(self.rules) - 1, index + delta))
        if new_index != index:
            self.rules.insert(new_index, self.rules.pop(index))
        return new_index

    # ── righe multi-output (#192): MultiMarket / MultiSelection ─────────────
    # Speculari alle regole-colonna ma su `multi_markets`/`multi_selections`. Ogni riga è
    # un `MultiRowRule` (override dei campi mercato/selezione; vuoto = eredita la base).
    def add_multi_market(self, **kwargs) -> MultiRowRule:
        """Aggiunge una riga MultiMarket (un mercato diverso della stessa partita) e la
        ritorna. I kwargs sono i campi di `MultiRowRule` (market_type, market_name,
        selection_name, price, bet_type, handicap, points, …)."""
        rule = MultiRowRule(**kwargs)
        self.multi_markets.append(rule)
        return rule

    def add_multi_selection(self, **kwargs) -> MultiRowRule:
        """Aggiunge una riga MultiSelection (un'altra selezione dello stesso mercato) e la
        ritorna. Tipicamente basta `selection_name`: gli altri campi ereditano dalla base."""
        rule = MultiRowRule(**kwargs)
        self.multi_selections.append(rule)
        return rule

    def remove_multi_market(self, index: int) -> None:
        del self.multi_markets[index]

    def remove_multi_selection(self, index: int) -> None:
        del self.multi_selections[index]

    def multi_warnings(self) -> list:
        """Avvisi NON bloccanti sulla config multi (#192), per la GUI. Non sono errori di
        `validate_parser_def` (il modello è valido): segnalano comportamenti che l'utente
        deve conoscere prima di salvare/avviare —

        - MultiMarket E MultiSelection attivi insieme → righe SEPARATE (prima i mercati, poi
          le selezioni sul mercato base), MAI il prodotto cartesiano;
        - una modalità attiva senza righe abilitate → nessuna riga extra generata;
        - (#325 follow-up, review #341/#344) per ogni riga SELEZIONE attiva coi delimitatori:
          Selezione fissa impostata → i delimitatori sono IGNORATI dal runtime (config ambigua);
          Selezione vuota ma mercato effettivo NON-punteggio (quando determinabile staticamente:
          override della riga, o base a valore fisso senza mappatura mercati) → l'estrazione
          dinamica NON si attiva e la riga resta FISSA ereditando la Selezione base (gate #341).
          Mercato noto solo a runtime → nessun avviso (mai falsi allarmi)."""
        warnings = []
        defn = self.to_def()
        if both_multi_active(defn):
            warnings.append(
                "MultiMarket e MultiSelection sono attivi insieme: verranno generate righe "
                "SEPARATE (prima i mercati, poi le selezioni), non combinazioni automatiche.")
        if self.multi_market_enabled and not defn.active_multi_markets():
            warnings.append("MultiMarket è attivo ma nessuna riga mercato è abilitata: "
                            "nessuna riga extra verrà generata.")
        if self.multi_selection_enabled and not defn.active_multi_selections():
            warnings.append("MultiSelection è attivo ma nessuna riga selezione è abilitata: "
                            "nessuna riga extra verrà generata.")
        warnings.extend(self._dynamic_selection_warnings(defn))
        return warnings

    @staticmethod
    def _dynamic_selection_warnings(defn: CustomParserDef) -> list:
        """Avvisi per-riga sulle SELEZIONI attive coi delimitatori (#325 follow-up). Specchia
        ESATTAMENTE la detection del runtime (`custom_pipeline._is_dynamic_selection`: Selezione
        vuota + almeno un delimitatore non-blank + mercato effettivo in `DYNAMIC_SCORE_MARKETS`,
        confronto esatto), così l'avviso non può divergere dal comportamento reale. L'indice
        della riga è la posizione nella lista della GUI (1-based, incluse le righe disattivate,
        così l'utente la ritrova a colpo d'occhio)."""
        if not defn.multi_selection_enabled:
            return []
        out = []
        static_market = _static_base_market_type(defn)
        score_markets = ", ".join(sorted(DYNAMIC_SCORE_MARKETS))   # calcolato una volta (Sourcery)
        for pos, rule in enumerate(defn.multi_selections, start=1):
            if not rule.enabled:
                continue
            # Come il runtime: un delimitatore di soli spazi NON attiva l'estrazione dinamica.
            has_delims = bool(str(rule.start_after or "").strip()
                              or str(rule.end_before or "").strip())
            if not has_delims:
                continue
            if str(rule.selection_name or "").strip():
                out.append(
                    f"Riga selezione {pos}: c'è una Selezione fissa, quindi i delimitatori "
                    "«Inizia dopo»/«Finisce prima» verranno IGNORATI. Per l'estrazione "
                    "dinamica dei punteggi lascia la Selezione vuota.")
                continue
            market = str(rule.market_type or "").strip() or static_market
            if market is not None and market not in DYNAMIC_SCORE_MARKETS:
                shown = market or "(vuoto)"
                out.append(
                    f"Riga selezione {pos}: estrazione dinamica dei punteggi INATTIVA — il "
                    f"mercato effettivo {shown} non è un mercato-punteggio "
                    f"({score_markets}): la riga resta FISSA ed "
                    "eredita la Selezione della riga base.")
        return out

    # ── modello / validazione ──────────────────────────────────────────────
    def to_def(self) -> CustomParserDef:
        return CustomParserDef(
            name=self.name, description=self.description, mode=self.mode,
            sport=self.sport,
            name_mapping_profiles=list(self.name_mapping_profiles),
            team_separator=self.team_separator,
            market_mapping_profiles=list(self.market_mapping_profiles),
            rules=list(self.rules),
            # Output multi-riga (#192): inoltrati al modello così save/preview/round-trip
            # riflettono la config multi. Liste NUOVE (no aliasing col builder).
            multi_market_enabled=bool(self.multi_market_enabled),
            multi_selection_enabled=bool(self.multi_selection_enabled),
            multi_markets=list(self.multi_markets),
            multi_selections=list(self.multi_selections),
            # Condizioni di gate (PR-1): copia PROFONDA (nuova lista + nuova Condition per
            # elemento), simmetrica alla deep-copy di `__init__` — così il def prodotto non
            # condivide oggetti `Condition` col builder e una mutazione successiva del builder
            # non tocca il def già salvato (nota Fable #390: allinea le due direzioni del round-trip).
            conditions=[Condition.from_dict(c.to_dict()) for c in self.conditions],
            conditions_mode=self.conditions_mode)

    # ── Modalità di riconoscimento (per-parser) ────────────────────────────
    def set_mode(self, mode: str) -> None:
        """Imposta la Modalità del parser e **allinea** l'obbligatorietà dei SOLI campi di
        riconoscimento al suo set (auto-Obblig.): i campi del set diventano `required=True`,
        gli ALTRI campi di riconoscimento `required=False`. Così selezionando una modalità
        i required risultano sempre coerenti con essa (cambiando NAME↔ID non restano
        required "stantii", Codex). `BOTH` → nessun campo di riconoscimento forzato (basta
        un set). Price/BetType/Provider NON sono toccati (non dipendono dalla modalità).

        Va invocata SOLO su azione esplicita dell'utente (scelta modalità) o su parser
        NUOVO — MAI al semplice reload/apertura, altrimenti rilasserebbe i required salvati
        a mano di un parser esistente (per quello la GUI non la chiama in `_reload`)."""
        self.mode = recognition.normalize_mode(mode)
        required = set(recognition.required_targets(self.mode))
        for rule in self.rules:
            if rule.target in recognition.RECOGNITION_FIELDS:
                rule.required = rule.target in required

    def apply_mode_defaults(self, mode: str) -> None:
        """Prepara un parser **nuovo** per `mode`: garantisce le 14 colonne E POI allinea
        l'auto-Obblig. (`set_mode`). L'ordine conta: chiamare `set_mode` su un builder ancora
        SENZA regole (parser nuovo/«Nuovo», prima che la griglia crei le righe) non marcherebbe
        nulla, così i campi del set della modalità resterebbero facoltativi (Codex #72). Da usare
        SOLO su parser nuovo — non al reload di uno esistente (non deve rilassare i required
        salvati a mano)."""
        self.ensure_all_columns()
        self.set_mode(mode)

    def ensure_all_columns(self) -> None:
        """Garantisce una riga per OGNI colonna del contratto (14), nell'ordine di
        `VALID_TARGETS`: le colonne non ancora presenti sono aggiunte come regole
        vuote (nessun valore → colonna CSV vuota se non configurata). Serve alla GUI a
        righe fisse: l'utente compila/lascia vuota ciascuna colonna senza aggiungerle
        a mano. Mantiene le regole esistenti (valori/Obblig.), solo riordinate.

        Le regole DUPLICATE (stessa colonna, es. da un JSON manomesso) NON vengono
        droppate: la PRIMA occorrenza va nella griglia, le altre restano in CODA così
        `validate_parser_def` le segnala e il salvataggio è bloccato — invece di perdere
        in silenzio un'estrazione e persistere una definizione alterata (Codex #72)."""
        first_by_target = {}
        for r in self.rules:
            first_by_target.setdefault(r.target, r)
        ordered = [first_by_target.get(target) or FieldRule(target=target)
                   for target in custom_parser.VALID_TARGETS]
        placed = {id(r) for r in ordered}
        # In coda: i duplicati di colonne standard e i target non-standard (preservati, non persi).
        ordered.extend(r for r in self.rules if id(r) not in placed)
        self.rules = ordered

    def errors(self) -> list:
        return custom_parser.validate_parser_def(self.to_def())

    def is_valid(self) -> bool:
        return not self.errors()

    # ── persistenza ─────────────────────────────────────────────────────────
    def save(self, dir_path: str = None) -> str:
        """Salva il parser corrente (valida prima; solleva ValueError se invalido)."""
        return custom_parser.save_parser(self.to_def(), dir_path)

    @classmethod
    def load(cls, path: str) -> "ParserBuilder":
        return cls(custom_parser.load_parser(path))

    @staticmethod
    def list_saved(dir_path: str = None) -> list:
        return custom_parser.list_parser_files(dir_path)

    # ── gestione dei parser salvati (per la GUI: lista/carica/duplica/elimina) ─
    @staticmethod
    def saved_parsers(dir_path: str = None) -> list:
        """Elenco dei parser salvati come `[{"name", "path"}]`, ordinato per nome
        (case-insensitive). `name` è il nome **dentro** il file; se un file è
        illeggibile/corrotto si usa il nome del file (stem) come fallback, senza
        far fallire l'intera lista (un parser rotto non deve nascondere gli altri)."""
        items = []
        for path in custom_parser.list_parser_files(dir_path):
            try:
                name = custom_parser.load_parser(path).name
            except (OSError, ValueError, json.JSONDecodeError):
                name = os.path.splitext(os.path.basename(path))[0]
            items.append({"name": name, "path": path})
        items.sort(key=lambda it: it["name"].lower())
        return items

    @staticmethod
    def delete_saved(name: str, dir_path: str = None) -> bool:
        """Elimina un parser salvato per nome. Ritorna `True` se rimosso."""
        return custom_parser.delete_parser(name, dir_path)

    @staticmethod
    def duplicate_saved(src_path: str, new_name: str, dir_path: str = None) -> str:
        """Duplica un parser salvato sotto `new_name` e salva la copia.

        Una duplica crea un parser **nuovo**: se esiste già un file per `new_name`
        viene rifiutata con `ValueError`, così non si sovrascrive in silenzio un
        parser esistente (`save_parser` con lo stesso nome sarebbe invece un
        *update*). Ritorna il path della copia; l'originale non è modificato."""
        new_name = str(new_name).strip()
        if os.path.exists(custom_parser.parser_path(new_name, dir_path)):
            raise ValueError(
                f"Esiste già un parser con nome {new_name!r}: scegli un altro nome.")
        builder = ParserBuilder.load(src_path)
        builder.name = new_name
        return builder.save(dir_path)

    # ── test-live ────────────────────────────────────────────────────────────
    def test_message(self, message: str, *, provider: str = "",
                     mode: str = None, require_price: bool = None,
                     name_mapping_profiles=None, market_mapping_profiles=None,
                     id_resolver=None):
        """Applica il parser corrente a un messaggio e ritorna il `PipelineResult`
        (status + riga + piazzabilità), per l'anteprima del costruttore. La modalità
        usata è quella DEL PARSER (`self.mode`) salvo override esplicito.

        `require_price` di default (None) deriva dalla riga Price del parser
        (`price_required()`): l'anteprima riflette così l'unico comando della quota,
        coerente col runtime.

        `name_mapping_profiles` (righe dei profili risolte da config) è inoltrato al
        pipeline: se il parser usa la mappatura nomi, l'anteprima traduce l'EventName
        come il runtime (o fa fail-closed con MAPPING_MISSING). `market_mapping_profiles`
        (voci dei profili mercati risolte da config) è inoltrato allo stesso modo: se il
        parser usa la mappatura mercati, l'anteprima imposta Mercato/Selezione come il
        runtime (o fa fail-closed con MARKET_MAPPING_MISSING).

        `id_resolver` (#192, Codex): opzionale, il dizionario Betfair locale per l'arricchimento
        ID. Passandolo, l'anteprima risolve gli ID come il runtime; senza, resta conservativa
        (vedi `preview_rows`)."""
        defn = self.to_def()
        if require_price is None:
            require_price = defn.price_required()
        return build_validated_row(defn, message, provider=provider,
                                   mode=self.mode if mode is None else mode,
                                   require_price=require_price,
                                   name_mapping_profiles=name_mapping_profiles,
                                   market_mapping_profiles=market_mapping_profiles,
                                   id_resolver=id_resolver)

    @staticmethod
    def merge_multi_rule_overrides(base: MultiRowRule, overrides: dict,
                                   *, enabled: bool) -> MultiRowRule:
        """Applica gli override VISIBILI della GUI su una COPIA della riga multi `base`,
        PRESERVANDO i campi non esposti (start_after/end_before/min_price/max_price/points):
        salvare un parser caricato dalla GUI non deve azzerarli in silenzio, perché sono
        consumati dagli override multi-riga del runtime e cambiano le righe CSV emesse
        (Codex P1). `overrides` mappa attributo→valore (stringhe già strippate); `enabled`
        è lo stato della casella «Attiva». Logica pura, testata in CI."""
        rule = MultiRowRule.from_dict(base.to_dict())   # copia: non muta la sorgente
        for key, val in overrides.items():
            setattr(rule, key, val)
        rule.enabled = bool(enabled)
        return rule

    @staticmethod
    def test_verdict(errors: list, preview_rows: list, *, diag_placeable: bool,
                     diag_status: str, res_row: dict, res_missing_required: list,
                     res_detail, content_ok: bool = True) -> str:
        """Verdetto sintetico di «Prova messaggio» (single + multi-riga). Logica pura, CI.

        Precedenza (Codex #19):
        1. **Errori STRUTTURALI** del parser (`errors()` = `validate_parser_def`): un parser
           che Save rifiuterebbe NON deve mai risultare «Pronto», anche se per caso la
           pipeline produce una riga (es. `fixed_value` + delimitatori sullo stesso campo).
           Si mostra l'errore invece della piazzabilità.
        2. **Output multi-riga** attivo → gate di contenuto (`content_ok`) POI `preview_summary`.
        3. **Single-row**: «Pronto» se piazzabile; altrimenti «Non pronto» col motivo e i
           campi mancanti — sia il gate parser (`missing_required`) sia i campi di
           RICONOSCIMENTO mancanti (in `res_detail` quando lo status è INVALID_MISSING_FIELDS),
           così l'anteprima dice QUALE colonna aggiungere. Il `detail` di altri stati (es.
           la tupla di INVALID_PRICE_BOUNDS) NON è trattato come «mancanti».

        `content_ok` (#192, Codex): esito del gate di contenuto del runtime
        (`custom_parser_engine.matches_message`, whole-message). Il runtime
        (`signal_router.resolve_row`) scarta con `NO_CONTENT_MATCH` un parser che non estrae
        NULLA dal messaggio (solo valori fissi) **anche se** le righe generate sarebbero
        piazzabili. Il verdetto single-row lo onora già via `diag_placeable` (vedi `diagnose`);
        per il **multi-riga** va onorato QUI, altrimenti «Prova messaggio» direbbe «✅ Pronto ·
        N righe» per un parser che il runtime non scriverebbe (over-promise). Default `True`
        preserva il comportamento dei chiamanti che non lo passano.

        Il gate rispetta l'ORDINE del runtime (Codex): `matches_message` è valutato SOLO se esiste
        almeno una riga piazzabile; con ZERO righe piazzabili il router ritorna lo status di
        validazione reale, quindi qui si ripiega su `preview_summary` (che elenca gli status delle
        righe scartate) invece di mascherare il vero errore con `NO_CONTENT_MATCH`."""
        if errors:
            return "⛔ Non salvabile: " + "; ".join(errors)
        if any(getattr(p, "kind", "base") != "base" for p in preview_rows):
            # Gate di contenuto come il runtime (signal_router): un parser a soli valori fissi
            # è piazzabile su qualsiasi testo ma verrebbe scartato con NO_CONTENT_MATCH. Non
            # mostrare «Pronto» in quel caso, coerentemente col verdetto single-row (Codex).
            # ORDINE come il runtime (Codex): matches_message è controllato SOLO dopo aver trovato
            # almeno una riga piazzabile; se ZERO righe sono piazzabili il router ritorna lo status
            # di validazione REALE (non NO_CONTENT_MATCH). Applicare il gate qui solo se esiste
            # una riga piazzabile, così l'anteprima non MASCHERA il vero errore bloccante.
            if any(p.placeable for p in preview_rows) and not content_ok:
                # Riusa il token di stato condiviso (`parser_diagnostics`, stessa fonte di
                # `diag.message_error` da cui deriva `content_ok`) così il messaggio resta
                # allineato allo status del runtime, senza letterale divergente (CodeRabbit/Sourcery).
                return (f"⛔ Non pronto ({parser_diagnostics.NO_CONTENT_MATCH}) · "
                        "nessun contenuto estratto dal messaggio")
            return ParserBuilder.preview_summary(preview_rows)
        if diag_placeable:
            # Decimali nel formato della lingua CSV corrente (#342, follow-up #344): l'anteprima
            # mostra i valori COME usciranno nel file (IT/ES virgola, EN punto), non il canonico
            # interno col punto — altrimenti l'operatore vede «1.85» e il file avrà «1,85».
            shown = csv_writer.localize_row(res_row)
            riga = ", ".join(f"{k}={v}" for k, v in shown.items() if v != "")
            return f"✅ Pronto · {riga}"
        missing = list(res_missing_required or [])
        if (not missing and diag_status == validator.INVALID_MISSING_FIELDS
                and isinstance(res_detail, (list, tuple))):
            missing = [str(x) for x in res_detail]
        extra = f" · mancanti: {', '.join(missing)}" if missing else ""
        return f"⛔ Non pronto ({diag_status}){extra}"

    @staticmethod
    def preview_summary(preview_rows: list) -> str:
        """Verdetto sintetico per «Prova messaggio» quando l'output MULTI-RIGA è attivo (#192):
        si basa sulle RIGHE GENERATE, non sulla sola riga base. Necessario perché in un parser
        MultiMarket la base può mancare di MarketType/SelectionName di proposito (li fornisce
        ogni riga mercato): il verdetto single-row direbbe «Non pronto» mentre le righe generate
        sono valide e il runtime le scriverebbe (Codex P2). Logica pura, testata in CI."""
        total = len(preview_rows)
        if total == 0:
            return "⛔ Nessuna riga generata."
        placeable = sum(1 for p in preview_rows if p.placeable)
        if placeable == total:
            return f"✅ Pronto · {total} righe generate, tutte piazzabili."
        if placeable == 0:
            statuses = ", ".join(sorted({p.status for p in preview_rows}))
            return f"⛔ Nessuna delle {total} righe è piazzabile ({statuses})."
        return f"⚠ {placeable}/{total} righe piazzabili (le altre verranno scartate)."

    def preview_rows(self, message: str, *, provider: str = "",
                     mode: str = None, require_price: bool = None,
                     name_mapping_profiles=None, market_mapping_profiles=None,
                     id_resolver=None) -> list:
        """Anteprima MULTI-RIGA (#192, PR2): applica il parser e ritorna una lista di
        `PreviewRow` GIÀ pronte per la tabella della GUI «Prova messaggio».

        Usa lo STESSO motore del runtime (`custom_pipeline.build_validated_rows`): ogni riga porta
        il suo verdetto (`placeable`/`status`) e una riga non piazzabile NON blocca le altre. Quando
        MultiMarket/MultiSelection sono disattivati ritorna UNA sola riga `kind="base"` (identico al
        single-row). Quando sono attivi, le righe MultiMarket (`kind="market"`) precedono quelle
        MultiSelection (`kind="selection"`), nello stesso ordine generato dal motore.

        `id_resolver` (#192, Codex): il dizionario Betfair locale per l'arricchimento ID. È
        **opzionale** e va passato dal chiamante (la GUI inoltra il resolver dell'app quando il
        dizionario è disponibile) perché il builder, da solo, non ha accesso al DB Betfair. **Senza
        resolver l'anteprima è CONSERVATIVA**: un parser `ID_ONLY` che si affida al dizionario per
        `MarketId`/`SelectionId` (lasciati vuoti) appare **non pronto** in anteprima anche se a
        runtime — con il dizionario — verrebbe risolto e scritto. Fail-closed (mai il contrario:
        l'anteprima non mostra piazzabile ciò che a runtime sarebbe scartato).

        Logica pura e testabile in CI (la GUI fa solo da vista): vedi `test_parser_builder`."""
        defn = self.to_def()
        if require_price is None:
            require_price = defn.price_required()
        eff_mode = self.mode if mode is None else mode
        results = build_validated_rows(
            defn, message, provider=provider, mode=eff_mode, require_price=require_price,
            name_mapping_profiles=name_mapping_profiles,
            market_mapping_profiles=market_mapping_profiles, id_resolver=id_resolver)
        n_markets = len(defn.active_multi_markets())
        n_selections = len(defn.active_multi_selections())
        multi_active = bool(n_markets or n_selections)
        # Lingua CSV catturata UNA volta per anteprima (#342, follow-up #344): il `summary` mostra
        # i decimali COME usciranno nel file (IT/ES virgola, EN punto). `row` resta CANONICO col
        # punto (è il dato, non la vista: chi lo consuma non deve dipendere dalla lingua).
        lang = csv_writer.get_csv_language()
        out = []
        for i, res in enumerate(results):
            if not multi_active:
                kind = "base"
            elif i < n_markets:
                kind = "market"
            else:
                kind = "selection"
            shown = csv_writer.localize_row(res.row, lang)
            summary = ", ".join(f"{k}={v}" for k, v in shown.items() if v != "")
            out.append(PreviewRow(
                index=i, kind=kind, placeable=res.placeable, status=res.status,
                missing_required=list(res.missing_required), row=dict(res.row),
                summary=summary))
        return out

    # ── Tester multiplo (#311 §3.2): N messaggi reali in un colpo solo ──────
    @staticmethod
    def split_messages(text) -> list:
        """Divide il testo incollato in MESSAGGI sul separatore ESPLICITO: una riga che
        contiene solo ``---`` (`MESSAGE_SEPARATOR`, spazi ai bordi tollerati). Nessuna
        euristica (i messaggi Telegram sono multi-linea e possono contenere righe vuote:
        indovinare i confini darebbe verdetti fuorvianti). Blocchi vuoti scartati; ogni
        messaggio è strippato dei soli whitespace ai bordi."""
        blocks, current = [], []
        for line in str(text or "").splitlines():
            if line.strip() == MESSAGE_SEPARATOR:
                blocks.append("\n".join(current))
                current = []
            else:
                current.append(line)
        blocks.append("\n".join(current))
        return [b.strip() for b in blocks if b.strip()]

    def batch_report(self, text, *, provider: str = "", mode: str = None,
                     require_price: bool = None, name_mapping_profiles=None,
                     market_mapping_profiles=None, id_resolver=None):
        """Report del tester multiplo (#311 §3.2): per OGNI messaggio del testo (separati
        da righe ``---``) il verdetto sintetico (valido/scartato col MOTIVO esatto — è lo
        stesso `test_verdict` del singolo «Prova messaggio», quindi status + campi
        mancanti) e l'anteprima delle righe CSV generate (`preview_rows`, stessa pipeline
        read-only del runtime: NESSUNA scrittura). Ritorna `(reports, skipped)`:
        `reports` = lista di `BatchMessageReport`, `skipped` = messaggi oltre il tetto
        `MAX_BATCH_MESSAGES` (fail-safe anti-paste gigante: il thread GUI non si
        congela; la GUI segnala il taglio, mai silenzioso)."""
        messages = self.split_messages(text)
        skipped = max(0, len(messages) - MAX_BATCH_MESSAGES)
        errors = self.errors()          # strutturali: indipendenti dal messaggio
        defn = self.to_def()
        if require_price is None:
            require_price = defn.price_required()
        eff_mode = self.mode if mode is None else mode
        reports = []
        for i, msg in enumerate(messages[:MAX_BATCH_MESSAGES]):
            try:
                reports.append(self._single_report(
                    i, msg, errors, defn, provider=provider, mode=eff_mode,
                    require_price=require_price,
                    name_mapping_profiles=name_mapping_profiles,
                    market_mapping_profiles=market_mapping_profiles,
                    id_resolver=id_resolver))
            except Exception as exc:   # noqa: BLE001 — isolamento PER-MESSAGGIO (CodeRabbit
                # #350): un messaggio patologico non deve abortire il batch nascondendo gli
                # altri report; l'errore resta VISIBILE nel verdetto di quel messaggio.
                first = (msg.splitlines() or [""])[0][:80]
                reports.append(BatchMessageReport(
                    index=i, first_line=first, ok=False,
                    verdict=f"❌ Errore interno su questo messaggio: {exc}", rows=[]))
        return reports, skipped

    def _single_report(self, i, msg, errors, defn, *, provider, mode,
                       require_price, name_mapping_profiles,
                       market_mapping_profiles, id_resolver):
        """Report di UN messaggio del batch (#311 §3.2): stessa pipeline del singolo."""
        res = self.test_message(msg, provider=provider, mode=mode,
                                require_price=require_price,
                                name_mapping_profiles=name_mapping_profiles,
                                market_mapping_profiles=market_mapping_profiles,
                                id_resolver=id_resolver)
        diag = parser_diagnostics.diagnose(
            defn, msg, provider=provider, mode=mode, require_price=require_price,
            name_mapping_profiles=name_mapping_profiles,
            market_mapping_profiles=market_mapping_profiles, id_resolver=id_resolver)
        rows = self.preview_rows(msg, provider=provider, mode=mode,
                                 require_price=require_price,
                                 name_mapping_profiles=name_mapping_profiles,
                                 market_mapping_profiles=market_mapping_profiles,
                                 id_resolver=id_resolver)
        verdict = ParserBuilder.test_verdict(
            errors, rows, diag_placeable=diag.placeable, diag_status=diag.status,
            res_row=res.row, res_missing_required=res.missing_required,
            res_detail=res.detail, content_ok=not diag.message_error)
        first = (msg.splitlines() or [""])[0][:80]
        return BatchMessageReport(index=i, first_line=first,
                                  ok=verdict.startswith("✅"), verdict=verdict, rows=rows)
